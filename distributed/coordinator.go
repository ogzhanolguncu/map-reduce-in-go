package distributed

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
)

type TaskState int

const (
	TaskIdle TaskState = iota
	TaskInProgress
	TaskCompleted
)

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
)

type Task struct {
	StartTime time.Time
	Input     string
	Workers   []string
	ID        int
	Type      TaskType
	State     TaskState
	Attempts  int
}

type WorkerInfo struct {
	id      string
	address string
	active  bool
}

type Coordinator struct {
	workers    map[string]*WorkerInfo
	results    map[string]string
	interDir   string
	inputFiles []string
	tasks      []*Task
	nReduce    int
	mu         sync.Mutex
}

func NewCoordinator(nReduce int, files []string, interDir string) *Coordinator {
	c := &Coordinator{
		nReduce:    nReduce,
		inputFiles: files,
		interDir:   interDir,
		workers:    make(map[string]*WorkerInfo),
		results:    make(map[string]string),
	}

	// Initialize map tasks
	for i, f := range files {
		c.tasks = append(c.tasks, &Task{
			ID:    i,
			Type:  MapTask,
			State: TaskIdle,
			Input: f,
		})
	}
	return c
}

func (c *Coordinator) Start(address string) error {
	// Register RPC methods
	rpc.Register(c)

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to start RPC server: %w", err)
	}

	go func() {
		c.checkTaskTimeouts()
	}()

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Printf("Accept error: %v", err)
				continue
			}
			go rpc.ServeConn(conn)
		}
	}()

	return nil
}

func (c *Coordinator) GetResults() map[string]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.results
}

func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	wId, err := uuid.NewRandom()
	if err != nil {
		return fmt.Errorf("Something went wrong when generating UUID %w", err)
	}

	c.workers[wId.String()] = &WorkerInfo{
		id:      wId.String(),
		address: args.WorkerAddr,
		active:  true,
	}

	reply.WorkerID = wId.String()
	return nil
}

const (
	taskTimeout = 10 * time.Second
	maxAttempts = 3
)

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	worker, exists := c.workers[args.WorkerID]
	if !exists || !worker.active {
		return fmt.Errorf("invalid or inactive worker")
	}

	// Start reduce phase if map phase complete
	if c.allMapTasksComplete() && len(c.tasks) == len(c.inputFiles) {
		c.initializeReduceTasks()
	}

	for _, task := range c.tasks {
		if task.State == TaskIdle {
			task.State = TaskInProgress
			task.Workers = append(task.Workers, args.WorkerID)
			task.Attempts++
			task.StartTime = time.Now()

			reply.TaskID = task.ID
			reply.Type = task.Type
			reply.Input = task.Input
			reply.NReduce = c.nReduce
			reply.MapID = len(c.inputFiles) // Only needed for reduce tasks

			return nil
		}
	}

	if c.allTasksComplete() {
		return fmt.Errorf("all tasks completed")
	}

	if c.allTasksAtMaxAttempts() {
		return fmt.Errorf("all remaining tasks failed max attemps")
	}

	return nil
}

func (c *Coordinator) TaskComplete(args *TaskCompleteArgs, reply *TaskCompleteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	task := c.tasks[args.TaskID]
	if args.Success {
		task.State = TaskCompleted
		if task.Type == ReduceTask {
			for k, v := range args.Results {
				c.results[k] = v
			}
		}
	} else {
		if task.Attempts >= maxAttempts {
			log.Printf("Task %d failed after %d attempts", args.TaskID, task.Attempts)
			return fmt.Errorf("task failed all attempts")
		}
		task.State = TaskIdle
	}
	return nil
}

func (c *Coordinator) allTasksComplete() bool {
	for _, task := range c.tasks {
		if task.State != TaskCompleted {
			return false
		}
	}
	return true
}

func (c *Coordinator) allTasksAtMaxAttempts() bool {
	for _, task := range c.tasks {
		if task.State != TaskCompleted && task.Attempts < maxAttempts {
			return false
		}
	}
	return true
}

func (c *Coordinator) allMapTasksComplete() bool {
	for _, task := range c.tasks {
		if task.Type == MapTask && task.State != TaskCompleted {
			return false
		}
	}
	return true
}

func (c *Coordinator) initializeReduceTasks() {
	c.tasks = nil

	for i := 0; i < c.nReduce; i++ {
		c.tasks = append(c.tasks, &Task{
			ID:    i,
			Type:  ReduceTask,
			State: TaskIdle,
			Input: strconv.Itoa(i), // Partition number
		})
	}
}

func (c *Coordinator) checkTaskTimeouts() {
	ticker := time.NewTicker(time.Second)
	for range ticker.C {
		c.mu.Lock()
		now := time.Now()

		for _, task := range c.tasks {
			if task.State == TaskInProgress && now.Sub(task.StartTime) > taskTimeout {
				task.State = TaskIdle
				task.Attempts++
				log.Printf("Task %d timed out, attemps: %d", task.ID, task.Attempts)
			}
		}
		c.mu.Unlock()
	}
}
