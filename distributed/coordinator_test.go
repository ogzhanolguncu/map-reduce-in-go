package distributed

import (
	"testing"
	"time"

	"github.com/ogzhanolguncu/map-reduce-in-go/map_reduce"
	"github.com/stretchr/testify/require"
)

func TestBasicMapReduce(t *testing.T) {
	coord := NewCoordinator(5,
		[]string{"../pg-being_ernest.txt", "../pg-dorian_gray.txt", "../pg-frankenstein.txt"},
		"/tmp/mr-test")

	go coord.Start("localhost:1234")
	time.Sleep(100 * time.Millisecond)

	w1 := NewWorker(&map_reduce.WordCountMapper{}, &map_reduce.WordCountReducer{})
	w2 := NewWorker(&map_reduce.WordCountMapper{}, &map_reduce.WordCountReducer{})

	err := w1.Register("localhost:1234")
	require.NoError(t, err)
	err = w2.Register("localhost:1234")
	require.NoError(t, err)

	time.Sleep(2 * time.Second)

	results := coord.GetResults()
	require.NotEmpty(t, results)

	// Test common words that should appear in both books
	require.Contains(t, results, "the")
	require.Contains(t, results, "and")
}
