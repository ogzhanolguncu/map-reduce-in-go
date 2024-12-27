package map_reduce

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWordCountMapper(t *testing.T) {
	m := &WordCountMapper{}
	input := "the quick brown fox"

	got, err := m.Map("test.txt", input)
	require.NoError(t, err)
	require.Equal(t, []KeyValue{
		{"the", "1"},
		{"quick", "1"},
		{"brown", "1"},
		{"fox", "1"},
	}, got)
}

func TestWordCountReducer(t *testing.T) {
	r := &WordCountReducer{}

	tests := []struct {
		name   string
		key    string
		want   string
		values []string
	}{
		{
			name:   "single value",
			key:    "fox",
			values: []string{"1"},
			want:   "1",
		},
		{
			name:   "multiple values",
			key:    "the",
			values: []string{"1", "1", "1"},
			want:   "3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := r.Reduce(tt.key, tt.values)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
