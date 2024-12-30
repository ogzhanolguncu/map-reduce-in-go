package distributed

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ogzhanolguncu/map-reduce-in-go/map_reduce"
	"github.com/stretchr/testify/require"
)

// TestState manages shared test state and provides isolation
type TestState struct {
	t            *testing.T
	tempDir      string
	coordAddress string
	coordinator  *Coordinator
	ctx          context.Context
	cancel       context.CancelFunc
	workers      []*Worker
	wg           sync.WaitGroup
}

func newTestState(t *testing.T) *TestState {
	// Create unique temp directory
	tempDir, err := os.MkdirTemp("", fmt.Sprintf("mr-test-%d-*", time.Now().UnixNano()))
	require.NoError(t, err)

	// Get random available port
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()

	coordAddress := fmt.Sprintf("localhost:%d", port)
	ctx, cancel := context.WithCancel(context.Background())

	return &TestState{
		t:            t,
		tempDir:      tempDir,
		coordAddress: coordAddress,
		ctx:          ctx,
		cancel:       cancel,
	}
}

func (ts *TestState) cleanup() {
	// Cancel context to stop all operations
	ts.cancel()

	// Clean up coordinator if it exists
	if ts.coordinator != nil {
		ts.coordinator.Cleanup()
	}

	// Clean up workers
	for _, w := range ts.workers {
		if w != nil {
			// Give workers time to shutdown gracefully
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Wait for all goroutines to complete
	ts.wg.Wait()

	// Remove temp directory
	if ts.tempDir != "" {
		os.RemoveAll(ts.tempDir)
	}
}

func createTestFile(dir, name, content string) (string, error) {
	filepath := filepath.Join(dir, name)
	return filepath, os.WriteFile(filepath, []byte(content), 0644)
}

func TestBasicMapReduceWorkflow(t *testing.T) {
	ts := newTestState(t)
	defer ts.cleanup()

	// Create test input file
	testFile, err := createTestFile(ts.tempDir, "test.txt",
		"hello world\nthis is a test\nhello test\nworld hello\n")
	require.NoError(t, err)

	// Create and start coordinator
	ts.coordinator = NewCoordinator(2, []string{testFile}, ts.tempDir)
	err = ts.coordinator.Start(ts.coordAddress)
	require.NoError(t, err)

	// Create completion channel
	completionCh := make(chan struct{})
	errorCh := make(chan error, 2)

	// Monitor completion
	ts.wg.Add(1)
	go func() {
		defer ts.wg.Done()
		for {
			select {
			case <-ts.ctx.Done():
				return
			default:
				if ts.coordinator.IsComplete() {
					close(completionCh)
					return
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	// Start workers
	for i := 0; i < 2; i++ {
		worker := NewWorker(&map_reduce.WordCountMapper{}, &map_reduce.WordCountReducer{})
		ts.workers = append(ts.workers, worker)

		ts.wg.Add(1)
		go func(w *Worker, workerNum int) {
			defer ts.wg.Done()
			// Add small delay between worker starts
			time.Sleep(time.Duration(workerNum*100) * time.Millisecond)

			if err := w.Register(ts.coordAddress, ts.ctx); err != nil {
				if err != ErrCleanShutdown && err != context.Canceled {
					errorCh <- fmt.Errorf("worker %d error: %v", workerNum, err)
				}
			}
		}(worker, i)
	}

	// Wait for completion or error
	select {
	case <-completionCh:
		// Verify results
		results := ts.coordinator.GetResults()
		require.NotEmpty(t, results)

		expectedWords := map[string]string{
			"hello": "3",
			"world": "2",
			"test":  "2",
		}

		for word, expectedCount := range expectedWords {
			actualCount, exists := results[word]
			require.True(t, exists, "Word '%s' not found in results", word)
			require.Equal(t, expectedCount, actualCount,
				"Incorrect count for word '%s'", word)
		}

	case err := <-errorCh:
		t.Fatalf("Worker error: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("Test timed out")
	}
}
