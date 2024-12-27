package distributed

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/rpc"
	"os"
	"sort"
	"time"

	. "github.com/ogzhanolguncu/map-reduce-in-go/map_reduce"
)

type Worker struct {
	mapper    Mapper
	reducer   Reducer
	workerID  string
	masterURL string
}

func NewWorker(m Mapper, r Reducer) *Worker {
	return &Worker{
		mapper:  m,
		reducer: r,
	}
}

func (w *Worker) Register(masterURL string) error {
	w.masterURL = masterURL
	client, err := rpc.Dial("tcp", masterURL)
	if err != nil {
		return fmt.Errorf("failed to connect to coordinator: %w", err)
	}
	defer client.Close()

	args := &RegisterArgs{WorkerAddr: w.masterURL}
	reply := &RegisterReply{}

	err = client.Call("Coordinator.Register", args, reply)
	if err != nil {
		return fmt.Errorf("failed to register: %w", err)
	}

	w.workerID = reply.WorkerID
	return w.Start()
}

func (w *Worker) Start() error {
	client, err := rpc.Dial("tcp", w.masterURL)
	if err != nil {
		return fmt.Errorf("failed to connect to coordinator: %w", err)
	}
	defer client.Close()

	for {
		args := &GetTaskArgs{WorkerID: w.workerID}
		reply := &GetTaskReply{}

		err := client.Call("Coordinator.GetTask", args, reply)
		if err != nil {
			if err.Error() == "all tasks completed" {
				return nil // Graceful exit when work is done
			}
			return fmt.Errorf("failed to get task: %w", err)
		}

		var results map[string]string

		switch reply.Type {
		case MapTask:
			err = w.executeMapTask(reply.TaskID, reply.Input, reply.NReduce)
		case ReduceTask:
			results = make(map[string]string)
			err = w.executeReduceTask(reply.TaskID, reply.MapID, results)
		}

		completeArgs := &TaskCompleteArgs{
			WorkerID: w.workerID,
			TaskID:   reply.TaskID,
			Success:  err == nil,
			Results:  results,
		}

		if err != nil {
			completeArgs.Error = err.Error()
		}

		completeReply := &TaskCompleteReply{}
		if err := client.Call("Coordinator.TaskComplete", completeArgs, completeReply); err != nil {
			return fmt.Errorf("failed to report completion: %w", err)
		}

		time.Sleep(time.Second)
	}
}

func (w *Worker) executeMapTask(mapID int, input string, nReduce int) error {
	// Read the input file
	content, err := os.ReadFile(input)
	if err != nil {
		return err
	}

	// Map input to KV pairs
	kvs, err := w.mapper.Map(input, string(content))
	if err != nil {
		return err
	}

	// Create intermediate files
	files := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)

	for i := 0; i < nReduce; i++ {
		filename := fmt.Sprintf("mr-%d-%d", mapID, i)
		files[i], err = os.Create(filename)
		if err != nil {
			return err
		}
		encoders[i] = json.NewEncoder(files[i])
	}

	// Write KVs to files
	for _, kv := range kvs {
		reduceTask := ihash(kv.Key) % nReduce
		if err := encoders[reduceTask].Encode(&kv); err != nil {
			return err
		}
	}

	// Close files
	for _, f := range files {
		f.Close()
	}

	return nil
}

// ihash returns a hash value for a key
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func (w *Worker) executeReduceTask(reduceID int, mapTasks int, results map[string]string) error {
	// Read all intermediate files for this reduce task
	var kvs []KeyValue

	for mapID := 0; mapID < mapTasks; mapID++ {
		filename := fmt.Sprintf("mr-%d-%d", mapID, reduceID)
		file, err := os.Open(filename)
		if err != nil {
			continue // Skip missing files
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
		file.Close()
	}

	// Group KVs by key
	grouped := make(map[string][]string)
	for _, kv := range kvs {
		grouped[kv.Key] = append(grouped[kv.Key], kv.Value)
	}

	// Sort keys for deterministic output
	var keys []string
	for k := range grouped {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Create output file
	outputFile := fmt.Sprintf("mr-out-%d", reduceID)
	f, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer f.Close()

	// Process each key and write results
	for _, k := range keys {
		result, err := w.reducer.Reduce(k, grouped[k])
		if err != nil {
			return err
		}
		results[k] = result
		fmt.Fprintf(f, "%v\t%v\n", k, result)
	}
	return nil
}
