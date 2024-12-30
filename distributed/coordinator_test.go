package distributed

import (
	"context"
	"testing"
	"time"

	"github.com/ogzhanolguncu/map-reduce-in-go/map_reduce"
	"github.com/stretchr/testify/require"
)

// func TestBasicMapReduce(t *testing.T) {
// 	// Create context with timeout
// 	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
// 	defer cancel()
//
// 	// Create coordinator
// 	coord := NewCoordinator(2,
// 		[]string{"../pg-being_ernest.txt", "../pg-dorian_gray.txt", "../pg-frankenstein.txt"},
// 		"/tmp/mr-test")
//
// 	// Start coordinator with error handling
// 	err := coord.Start("localhost:1234")
// 	require.NoError(t, err)
// 	defer coord.Cleanup()
//
// 	// Ensure coordinator is ready before starting workers
// 	time.Sleep(100 * time.Millisecond)
//
// 	// Create and start workers
// 	w1 := NewWorker(&map_reduce.WordCountMapper{}, &map_reduce.WordCountReducer{})
// 	w2 := NewWorker(&map_reduce.WordCountMapper{}, &map_reduce.WordCountReducer{})
//
// 	// Start workers with error handling
// 	errCh := make(chan error, 2)
// 	go func() {
// 		errCh <- w1.Register("localhost:1234", ctx)
// 	}()
// 	go func() {
// 		errCh <- w2.Register("localhost:1234", ctx)
// 	}()
//
// 	// Wait for job completion or timeout
// 	select {
// 	case <-ctx.Done():
// 		t.Fatal("Test timed out")
// 	case err := <-errCh:
// 		require.NoError(t, err)
// 	}
//
// 	// Get and verify results
// 	results := coord.GetResults()
// 	require.NotEmpty(t, results)
//
// 	// Test common words that should appear in all books
// 	commonWords := []string{"the", "and", "of", "to", "in"}
// 	for _, word := range commonWords {
// 		require.Contains(t, results, word, "Common word '%s' not found in results", word)
// 		count, err := strconv.Atoi(results[word])
// 		require.NoError(t, err)
// 		require.Greater(t, count, 0, "Word '%s' has count of 0", word)
// 	}
//
// 	// Verify no unexpected errors from workers
// 	select {
// 	case err := <-errCh:
// 		require.NoError(t, err)
// 	default:
// 	}
// }

func TestWorkerHeartbeat(t *testing.T) {
	// Create coordinator
	coord := NewCoordinator(2, []string{"../pg-being_ernest.txt"}, "/tmp/mr-test")
	coord.healthCheckInterval = 3 * time.Second
	coord.maxHeartbeatDelay = 5 * time.Second

	// Start coordinator
	err := coord.Start("localhost:1234")
	require.NoError(t, err)
	defer coord.Cleanup()

	// Ensure coordinator is ready
	time.Sleep(500 * time.Millisecond)

	// Create worker
	w1 := NewWorker(&map_reduce.WordCountMapper{}, &map_reduce.WordCountReducer{})

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Start worker in goroutine
	errCh := make(chan error, 1)
	go func() {
		errCh <- w1.Register("localhost:1234", ctx)
	}()

	// Wait longer for worker to register and establish heartbeat
	time.Sleep(1 * time.Second)

	// Verify worker is active
	require.True(t, coord.hasActiveWorkers(), "Worker should be marked as active")

	// Check worker info in coordinator
	coord.mu.Lock()
	var worker *WorkerInfo
	for _, w := range coord.workers {
		worker = w
		break
	}
	coord.mu.Unlock()

	require.NotNil(t, worker, "Worker should be registered")
	require.True(t, worker.active, "Worker should be active")
	require.False(t, worker.lastHeartbeat.IsZero(), "Worker should have sent heartbeat")

	// Simulate worker failure by canceling context
	cancel()

	// Wait for health check to detect failure
	time.Sleep(6 * time.Second)

	// Verify worker is marked as inactive
	require.False(t, coord.hasActiveWorkers(), "Worker should be marked as inactive after failure")

	// Check if worker exited properly
	select {
	case err := <-errCh:
		require.Error(t, err, "Worker should exit with error")
	case <-time.After(time.Second):
		t.Fatal("Worker failed to exit")
	}
}

// func TestTaskProgressReporting(t *testing.T) {
// 	// Create coordinator
// 	coord := NewCoordinator(2, []string{"../pg-being_ernest.txt"}, "/tmp/mr-test")
//
// 	// Start coordinator
// 	err := coord.Start("localhost:1235")
// 	require.NoError(t, err)
// 	defer coord.Cleanup()
//
// 	// Create worker
// 	w1 := NewWorker(&map_reduce.WordCountMapper{}, &map_reduce.WordCountReducer{})
//
// 	// Start worker with longer timeout
// 	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
// 	defer cancel()
//
// 	// Start worker in goroutine
// 	go func() {
// 		w1.Register("localhost:1235", ctx)
// 	}()
//
// 	// Wait for task to be assigned and processed
// 	time.Sleep(2 * time.Second)
//
// 	// Check progress reports
// 	coord.mu.Lock()
// 	var lastProgress float64
// 	var lastTaskID int
// 	for _, worker := range coord.workers {
// 		lastProgress = worker.status.TaskProgress
// 		lastTaskID = worker.status.CurrentTaskID
// 	}
// 	coord.mu.Unlock()
//
// 	require.Greater(t, lastProgress, float64(0), "Worker should report non-zero progress")
// 	require.GreaterOrEqual(t, lastTaskID, 0, "Worker should report valid task ID")
// }
//
// func TestWorkerHealthCheckTimeout(t *testing.T) {
// 	// Create coordinator with very short timeouts for testing
// 	coord := NewCoordinator(2, []string{"../pg-being_ernest.txt"}, "/tmp/mr-test")
// 	coord.healthCheckInterval = 50 * time.Millisecond
// 	coord.maxHeartbeatDelay = 100 * time.Millisecond
//
// 	// Start coordinator
// 	err := coord.Start("localhost:1236")
// 	require.NoError(t, err)
// 	defer coord.Cleanup()
//
// 	// Create and register worker
// 	w1 := NewWorker(&map_reduce.WordCountMapper{}, &map_reduce.WordCountReducer{})
//
// 	ctx, cancel := context.WithCancel(context.Background())
// 	go func() {
// 		w1.Register("localhost:1236", ctx)
// 	}()
//
// 	// Wait for worker to register
// 	time.Sleep(200 * time.Millisecond)
// 	require.True(t, coord.hasActiveWorkers(), "Worker should be active initially")
//
// 	// Cancel worker context to stop heartbeats
// 	cancel()
//
// 	// Wait for health check to detect timeout
// 	time.Sleep(200 * time.Millisecond)
// 	require.False(t, coord.hasActiveWorkers(), "Worker should be marked inactive after timeout")
//
// 	// Verify task reassignment
// 	coord.mu.Lock()
// 	var worker *WorkerInfo
// 	for _, w := range coord.workers {
// 		worker = w
// 		break
// 	}
// 	coord.mu.Unlock()
//
// 	require.NotNil(t, worker)
// 	require.False(t, worker.active, "Worker should be marked inactive")
// }
