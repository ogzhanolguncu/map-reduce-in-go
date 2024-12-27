package map_reduce

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunner_Run(t *testing.T) {
	runner := NewRunner(&WordCountMapper{}, &WordCountReducer{})
	inputs := map[string]string{
		"file1.txt": "hello world",
		"file2.txt": "hello mapreduce",
	}

	got, err := runner.Run(inputs)
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"hello":     "2",
		"world":     "1",
		"mapreduce": "1",
	}, got)
}
