package distributed

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"

	. "github.com/ogzhanolguncu/map-reduce-in-go/map_reduce"
)

type Worker struct {
	mapper    Mapper
	reducer   Reducer
	workerID  string
	masterURL string
	interDir  string
}

func NewWorker(m Mapper, r Reducer) *Worker {
	return &Worker{
		mapper:  m,
		reducer: r,
	}
}

func (w *Worker) Register(masterURL string, ctx context.Context) error {
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
	return w.Start(ctx)
}

func (w *Worker) Start(ctx context.Context) error {
	// Create RPC client with timeout
	client, err := rpc.Dial("tcp", w.masterURL)
	if err != nil {
		return fmt.Errorf("failed to connect to coordinator: %w", err)
	}
	defer client.Close()

	for {
		select {
		case <-ctx.Done():
			log.Printf("Worker %s shutting down...", w.workerID)
			return nil
		default:
			// Continue with task processing
		}

		args := &GetTaskArgs{WorkerID: w.workerID}
		reply := &GetTaskReply{}

		// Add timeout for RPC calls
		callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		call := client.Go("Coordinator.GetTask", args, reply, nil)

		select {
		case <-callCtx.Done():
			cancel()
			return fmt.Errorf("RPC timeout")
		case result := <-call.Done:
			cancel()
			if result.Error != nil {
				return fmt.Errorf("failed to get task: %w", result.Error)
			}
		}

		if reply.JobComplete {
			log.Printf("Worker '%s' is done", w.workerID)
			return nil
		}

		var results map[string]string
		var taskErr error

		// Get interDir from coordinator
		w.interDir = reply.InterDir

		switch reply.Type {
		case MapTask:
			taskErr = w.executeMapTask(reply.TaskID, reply.Input, reply.NReduce)
		case ReduceTask:
			results = make(map[string]string)
			taskErr = w.executeReduceTask(reply.TaskID, reply.MapID, results)
		}

		completeArgs := &TaskCompleteArgs{
			WorkerID: w.workerID,
			TaskID:   reply.TaskID,
			Success:  taskErr == nil,
			Results:  results,
		}

		if taskErr != nil {
			completeArgs.Error = taskErr.Error()
		}

		completeReply := &TaskCompleteReply{}
		if err := client.Call("Coordinator.TaskComplete", completeArgs, completeReply); err != nil {
			return fmt.Errorf("failed to report completion: %w", err)
		}

		// Add backoff between tasks
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Second):
			continue
		}
	}
}

func (w *Worker) executeMapTask(mapID int, input string, nReduce int) error {
	if err := os.MkdirAll(w.interDir, 0755); err != nil {
		log.Printf("Failed to create directory: %v", err)
		return fmt.Errorf("failed to create intermediate directory: %w", err)
	}

	// Verify directory exists
	if _, err := os.Stat(w.interDir); os.IsNotExist(err) {
		return fmt.Errorf("directory creation failed")
	}

	log.Printf("Directory %s successfully created", w.interDir)

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
		filename := filepath.Join(w.interDir, fmt.Sprintf("mr-%d-%d", mapID, i))
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
	log.Printf("Starting reduce task %d, mapTasks: %d", reduceID, mapTasks)
	// Read all intermediate files for this reduce task
	var kvs []KeyValue

	for mapID := 0; mapID < mapTasks; mapID++ {
		filename := filepath.Join(w.interDir, fmt.Sprintf("mr-%d-%d", mapID, reduceID))
		file, err := os.Open(filename)
		if err != nil {
			log.Printf("Error opening %s: %v", filename, err)
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
	log.Printf("Reduce %d: Found %d KV pairs", reduceID, len(kvs))

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

	log.Printf("Completed reduce task %d, wrote to %s", reduceID, outputFile)
	return nil
}
