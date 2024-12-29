package distributed

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
)

type WorkerInfo struct {
	id      string
	address string
	active  bool
}

type Coordinator struct {
	listener    net.Listener
	workers     map[string]*WorkerInfo
	results     map[string]string
	taskTracker *TaskTracker
	shutdown    chan struct{}
	interDir    string
	inputFiles  []string
	wg          sync.WaitGroup
	nReduce     int
	mu          sync.Mutex
	done        bool
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
		shutdown:    make(chan struct{}),
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
	c.listener = listener

	// Start straggler detection
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if c.isJobComplete() {
					close(c.shutdown)
					return
				}
				c.taskTracker.checkForStragglers()
			case <-c.shutdown:
				return
			}
		}
	}()

	// Start timeout checker
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				c.taskTracker.CheckTimeouts()
			case <-c.shutdown:
				return
			}
		}
	}()

	// Accept connections
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				select {
				case <-c.shutdown:
					return // Normal shutdown
				default:
					log.Printf("Accept error: %v", err)
				}
				continue
			}

			c.wg.Add(1)
			go func(conn net.Conn) {
				defer c.wg.Done()
				rpc.ServeConn(conn)
			}(conn)
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

	task, exists := c.taskTracker.tasks[args.TaskID]
	if !exists {
		return fmt.Errorf("task %d not found", args.TaskID)
	}

	// Check if this is a replica completion
	if _, isReplica := task.Metadata.Replicas[args.WorkerID]; isReplica {
		if args.Success {
			// If main task isn't completed yet, mark it complete
			if task.State != TaskCompleted {
				task.State = TaskCompleted
				// Store results
				if args.Results != nil {
					for k, v := range args.Results {
						c.results[k] = v
					}
				}
				// Cancel other replicas
				c.taskTracker.cancelReplicas(args.TaskID, args.WorkerID)
			}
		} else {
			// Handle replica failure
			delete(task.Metadata.Replicas, args.WorkerID)
			task.Metadata.ReplicaCount--
			log.Printf("Replica failed for task %d by worker %s: %s",
				args.TaskID, args.WorkerID, args.Error)
		}
		return nil
	}

	// Handle regular (non-replica) task completion
	if args.Success {
		if err := c.taskTracker.MarkComplete(args.TaskID); err != nil {
			return err
		}
		if args.Results != nil {
			for k, v := range args.Results {
				c.results[k] = v
			}
		}
	} else {
		log.Printf("Task %d failed: %s", args.TaskID, args.Error)
		return c.taskTracker.ReassignFailedTask(args.TaskID, args.WorkerID)
	}

	return nil
}

func (c *Coordinator) Cleanup() {
	// Signal shutdown
	select {
	case <-c.shutdown:
		// Already closed
	default:
		close(c.shutdown)
	}

	// Close listener to stop accepting new connections
	if c.listener != nil {
		c.listener.Close()
	}

	// Wait for all goroutines to finish
	c.wg.Wait()

	// Clean up intermediate files
	if err := os.RemoveAll(c.interDir); err != nil {
		log.Printf("Error cleaning up intermediate directory: %v", err)
	}
}

// Add method to check if coordinator has any active workers
func (c *Coordinator) hasActiveWorkers() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, worker := range c.workers {
		if worker.active {
			return true
		}
	}
	return false
}

func (c *Coordinator) isJobComplete() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.taskTracker.hasStartedReduce {
		return false
	}

	isDone := c.taskTracker.IsReducePhaseDone()
	if isDone && !c.done {
		c.done = true
		// Only log once when transitioning to done state
		log.Println("All tasks completed, initiating shutdown...")
	}

	return isDone
}

func (c *Coordinator) IsComplete() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.taskTracker.hasStartedReduce {
		return false
	}

	return c.taskTracker.IsReducePhaseDone()
}
