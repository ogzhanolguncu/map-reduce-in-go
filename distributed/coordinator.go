package distributed

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"

	"github.com/google/uuid"
)

type WorkerInfo struct {
	id      string
	address string
	active  bool
}

type Coordinator struct {
	workers     map[string]*WorkerInfo
	results     map[string]string
	taskTracker *TaskTracker
	interDir    string
	inputFiles  []string
	nReduce     int
	mu          sync.Mutex
}

func NewCoordinator(nReduce int, files []string, interDir string) *Coordinator {
	if interDir == "" {
		interDir = fmt.Sprintf("/tmp/mr-%s", uuid.New().String())
		os.MkdirAll(interDir, 0755)
	}

	c := &Coordinator{
		inputFiles:  files,
		interDir:    interDir,
		workers:     make(map[string]*WorkerInfo),
		results:     make(map[string]string),
		taskTracker: NewTaskTracker(nReduce),
	}

	c.taskTracker.InitMapTasks(files)

	return c
}

func (c *Coordinator) Start(address string) error {
	rpc.Register(c)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to start RPC server: %w", err)
	}

	go c.taskTracker.CheckTimeouts()

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

func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	wId, err := uuid.NewRandom()
	if err != nil {
		return fmt.Errorf("failed generating UUID: %w", err)
	}

	c.workers[wId.String()] = &WorkerInfo{
		id:      wId.String(),
		address: args.WorkerAddr,
		active:  true,
	}

	reply.WorkerID = wId.String()
	return nil
}

func (c *Coordinator) GetResults() map[string]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.results
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// First check if all reduce tasks are complete
	if c.taskTracker.hasStartedReduce && c.taskTracker.IsReducePhaseDone() {
		reply.JobComplete = true
		return nil
	}

	// Then check map phase and transition if needed
	if c.taskTracker.IsMapPhaseDone() && !c.taskTracker.hasStartedReduce {
		if err := c.taskTracker.TransitionToReducePhase(); err != nil {
			return err
		}
	}

	task, err := c.taskTracker.AssignTask(args.WorkerID)
	if err != nil {
		return err
	}
	if task == nil {
		return fmt.Errorf("no tasks available")
	}
	reply.TaskID = task.ID
	reply.Type = task.Type
	reply.Input = task.Input
	reply.NReduce = c.taskTracker.nReduce
	reply.MapID = len(c.inputFiles)
	reply.InterDir = c.interDir

	return nil
}

func (c *Coordinator) TaskComplete(args *TaskCompleteArgs, reply *TaskCompleteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	log.Printf("TaskComplete called for task %d, success: %v", args.TaskID, args.Success)

	if args.Success {
		if err := c.taskTracker.MarkComplete(args.TaskID); err != nil {
			log.Printf("Error marking task complete: %v", err)
			return err
		}
		if args.Results != nil {
			for k, v := range args.Results {
				c.results[k] = v
			}
		}
	} else {
		log.Printf("Task %d failed: %s", args.TaskID, args.Error)
		// Consider task reassignment logic here
	}

	return nil
}
