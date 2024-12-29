package distributed

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/ogzhanolguncu/map-reduce-in-go/map_reduce"
	"github.com/stretchr/testify/require"
)

func TestBasicMapReduce(t *testing.T) {
	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create coordinator
	coord := NewCoordinator(2,
		[]string{"../pg-being_ernest.txt", "../pg-dorian_gray.txt", "../pg-frankenstein.txt"},
		"/tmp/mr-test")

	// Start coordinator with error handling
	err := coord.Start("localhost:1234")
	require.NoError(t, err)
	defer coord.Cleanup()

	// Ensure coordinator is ready before starting workers
	time.Sleep(100 * time.Millisecond)

	// Create and start workers
	w1 := NewWorker(&map_reduce.WordCountMapper{}, &map_reduce.WordCountReducer{})
	w2 := NewWorker(&map_reduce.WordCountMapper{}, &map_reduce.WordCountReducer{})

	// Start workers with error handling
	errCh := make(chan error, 2)
	go func() {
		errCh <- w1.Register("localhost:1234", ctx)
	}()
	go func() {
		errCh <- w2.Register("localhost:1234", ctx)
	}()

	// Wait for job completion or timeout
	select {
	case <-ctx.Done():
		t.Fatal("Test timed out")
	case err := <-errCh:
		require.NoError(t, err)
	}

	// Get and verify results
	results := coord.GetResults()
	require.NotEmpty(t, results)

	// Test common words that should appear in all books
	commonWords := []string{"the", "and", "of", "to", "in"}
	for _, word := range commonWords {
		require.Contains(t, results, word, "Common word '%s' not found in results", word)
		count, err := strconv.Atoi(results[word])
		require.NoError(t, err)
		require.Greater(t, count, 0, "Word '%s' has count of 0", word)
	}

	// Verify no unexpected errors from workers
	select {
	case err := <-errCh:
		require.NoError(t, err)
	default:
	}
}
