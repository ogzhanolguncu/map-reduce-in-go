package map_reduce

import (
	"fmt"
	"sync"
)

type Runner struct {
	mapper  Mapper
	reducer Reducer
	mu      sync.Mutex
}

func NewRunner(m Mapper, r Reducer) *Runner {
	return &Runner{
		mapper:  m,
		reducer: r,
	}
}

func (r *Runner) Run(inputs map[string]string) (map[string]string, error) {
	var mappedKVs []KeyValue
	for fileName, contents := range inputs {
		kvs, err := r.mapper.Map(fileName, contents)
		if err != nil {
			return nil, fmt.Errorf("mapping error: %w", err)
		}
		mappedKVs = append(mappedKVs, kvs...)
	}

	groups := make(map[string][]string)
	for _, kv := range mappedKVs {
		groups[kv.Key] = append(groups[kv.Key], kv.Value)
	}

	results := make(map[string]string)
	for key, values := range groups {
		result, err := r.reducer.Reduce(key, values)
		if err != nil {
			return nil, fmt.Errorf("reduce error for key %s: %w", key, err)
		}
		results[key] = result
	}

	return results, nil
}
