package map_reduce

type KeyValue struct {
	Key   string
	Value string
}

type Mapper interface {
	Map(filename string, contents string) ([]KeyValue, error)
}

type Reducer interface {
	Reduce(key string, values []string) (string, error)
}
