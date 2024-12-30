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
	"strings"
	"time"

	. "github.com/ogzhanolguncu/map-reduce-in-go/map_reduce"
)

type Worker struct {
	mapper        Mapper
	reducer       Reducer
	workerID      string
	masterURL     string
	interDir      string
	lastError     string
	currentTaskID int
	taskProgress  float64
}

func NewWorker(m Mapper, r Reducer) *Worker {
	return &Worker{
		mapper:  m,
		reducer: r,
	}
}

var ErrCleanShutdown = fmt.Errorf("clean shutdown")

func (w *Worker) Register(masterURL string, ctx context.Context) error {
	log.Printf("Attempting to register with coordinator at %s", masterURL)
	w.masterURL = masterURL

	// Create RPC client
	client, err := rpc.Dial("tcp", masterURL)
	if err != nil {
		return fmt.Errorf("failed to connect to coordinator: %w", err)
	}
	defer client.Close()

	// Register with coordinator
	args := &RegisterArgs{WorkerAddr: w.masterURL}
	reply := &RegisterReply{}

	log.Printf("Sending registration request...")
	err = client.Call("Coordinator.Register", args, reply)
	if err != nil {
		return fmt.Errorf("failed to register: %w", err)
	}

	w.workerID = reply.WorkerID
	log.Printf("Successfully registered with coordinator. Assigned ID: %s", w.workerID)

	// Create a new client for long-running operations
	taskClient, err := rpc.Dial("tcp", masterURL)
	if err != nil {
		return fmt.Errorf("failed to create task client: %w", err)
	}
	defer taskClient.Close()

	// Start heartbeat and task processing
	errCh := make(chan error, 2)

	// Start heartbeat goroutine
	go func() {
		if err := w.startHeartbeat(ctx, taskClient); err == ErrCleanShutdown {
			errCh <- nil
		} else {
			errCh <- err
		}
	}()

	// Start task processing
	go func() {
		if err := w.processTasks(ctx, taskClient); err == ErrCleanShutdown {
			errCh <- nil
		} else {
			errCh <- err
		}
	}()

	// Wait for either completion or error
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		if err == nil {
			log.Printf("Worker completed successfully")
			return nil
		}
		return fmt.Errorf("worker error: %w", err)
	}
}

func (w *Worker) executeMapTask(mapID int, input string, nReduce int) error {
	w.currentTaskID = mapID
	w.taskProgress = 0.0
	w.lastError = ""
	defer func() {
		w.currentTaskID = 0
		w.taskProgress = 0.0
	}()

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
	w.taskProgress = 0.3

	// Map input to KV pairs
	kvs, err := w.mapper.Map(input, string(content))
	if err != nil {
		return err
	}
	w.taskProgress = 0.6

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
	w.taskProgress = 1

	return nil
}

// ihash returns a hash value for a key
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func (w *Worker) executeReduceTask(reduceID int, mapTasks int, results map[string]string) error {
	w.currentTaskID = reduceID
	w.taskProgress = 0.0
	w.lastError = ""
	defer func() {
		w.currentTaskID = 0
		w.taskProgress = 0.0
	}()

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
	w.taskProgress = 0.3 // After reading intermediate files
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
	w.taskProgress = 0.6 // After reading intermediate files

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

	w.taskProgress = 1 // After reading intermediate files
	log.Printf("Completed reduce task %d, wrote to %s", reduceID, outputFile)
	return nil
}

func (w *Worker) startHeartbeat(ctx context.Context, client *rpc.Client) error {
	log.Printf("Worker %s starting heartbeat routine", w.workerID)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("Worker %s heartbeat stopped: context done", w.workerID)
			return ctx.Err()
		case <-ticker.C:
			args := &HeartbeatArgs{
				WorkerID: w.workerID,
				Status:   w.getStatus(),
			}
			reply := &HeartbeatReply{}

			if err := client.Call("Coordinator.Heartbeat", args, reply); err != nil {
				if !reply.ShouldContinue {
					log.Printf("Worker %s gracefully disconnecting", w.workerID)
					return ErrCleanShutdown
				}
				log.Printf("Worker %s heartbeat failed: %v", w.workerID, err)
				return fmt.Errorf("heartbeat failed: %w", err)
			}

			if !reply.ShouldContinue {
				log.Printf("Worker %s received shutdown signal", w.workerID)
				return ErrCleanShutdown
			}
		}
	}
}

func (w *Worker) recordError(err error) {
	if err != nil {
		w.lastError = err.Error()
	}
}

func (w *Worker) processTasks(ctx context.Context, client *rpc.Client) error {
	for {
		select {
		case <-ctx.Done():
			log.Printf("Worker %s context cancelled, stopping task processing", w.workerID)
			return nil
		default:
		}

		args := &GetTaskArgs{WorkerID: w.workerID}
		reply := &GetTaskReply{}

		// Add timeout for RPC calls
		callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		call := client.Go("Coordinator.GetTask", args, reply, nil)

		select {
		case <-callCtx.Done():
			cancel()
			if ctx.Err() != nil {
				// Context cancelled, clean exit
				return nil
			}
			// If we get a timeout after job completion, exit cleanly
			if w.taskProgress == 1.0 {
				return nil
			}
			return fmt.Errorf("RPC timeout")
		case result := <-call.Done:
			cancel()
			if result.Error != nil {
				// Check for common shutdown conditions
				if strings.Contains(result.Error.Error(), "connection refused") ||
					strings.Contains(result.Error.Error(), "connection reset by peer") ||
					strings.Contains(result.Error.Error(), "EOF") {
					log.Printf("Coordinator appears to have shut down, exiting cleanly")
					return nil
				}
				return fmt.Errorf("failed to get task: %w", result.Error)
			}
		}

		if reply.JobComplete {
			log.Printf("Worker %s: job complete signal received", w.workerID)
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

func (w *Worker) getStatus() WorkerStatus {
	return WorkerStatus{
		CurrentTaskID: w.currentTaskID,
		TaskProgress:  w.taskProgress,
		LastError:     w.lastError,
		Timestamp:     time.Now(),
		MemoryUsage:   0,
		CPUUsage:      0,
	}
}
