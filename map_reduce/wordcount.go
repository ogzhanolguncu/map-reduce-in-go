package map_reduce

import (
	"strconv"
	"strings"
)

type WordCountMapper struct{}

func (m *WordCountMapper) Map(filename string, contents string) ([]KeyValue, error) {
	var kvs []KeyValue
	words := strings.Fields(contents)

	for _, word := range words {
		word = strings.ToLower(strings.Trim(word, ".,!?\"':;()"))
		if word != "" {
			kvs = append(kvs, KeyValue{Key: word, Value: "1"})
		}
	}

	return kvs, nil
}

type WordCountReducer struct{}

func (r *WordCountReducer) Reduce(key string, values []string) (string, error) {
	if len(values) == 0 {
		return "0", nil
	}
	count := len(values)
	return strconv.Itoa(count), nil
}
