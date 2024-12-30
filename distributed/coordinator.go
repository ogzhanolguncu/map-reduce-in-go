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
	lastHeartbeat time.Time
	id            string
	address       string
	status        WorkerStatus
	active        bool
}

type Coordinator struct {
	listener            net.Listener
	workers             map[string]*WorkerInfo
	results             map[string]string
	taskTracker         *TaskTracker
	shutdown            chan struct{}
	interDir            string
	inputFiles          []string
	wg                  sync.WaitGroup
	nReduce             int
	healthCheckInterval time.Duration
	maxHeartbeatDelay   time.Duration
	mu                  sync.Mutex
	done                bool
	isShuttingDown      bool
}

func NewCoordinator(nReduce int, files []string, interDir string) *Coordinator {
	if interDir == "" {
		interDir = fmt.Sprintf("/tmp/mr-%s", uuid.New().String())
		os.MkdirAll(interDir, 0755)
	}

	c := &Coordinator{
		inputFiles:          files,
		interDir:            interDir,
		workers:             make(map[string]*WorkerInfo),
		results:             make(map[string]string),
		taskTracker:         NewTaskTracker(nReduce),
		shutdown:            make(chan struct{}),
		healthCheckInterval: 5 * time.Second,
		maxHeartbeatDelay:   10 * time.Second,
	}

	c.startHealthCheck()
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
					c.initiateShutdown()
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

	// Start worker health checker
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(c.healthCheckInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				c.checkWorkerHealth()
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
		id:            wId.String(),
		address:       args.WorkerAddr,
		active:        true,
		lastHeartbeat: time.Now(),
		status: WorkerStatus{
			CurrentTaskID: 0,
			TaskProgress:  0,
			LastError:     "",
			Timestamp:     time.Now(),
		},
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
		log.Printf("All reduce tasks completed for worker %s", args.WorkerID)
		return nil
	}

	// Check and transition to reduce phase if map phase is done
	if c.taskTracker.IsMapPhaseDone() && !c.taskTracker.hasStartedReduce {
		if err := c.taskTracker.TransitionToReducePhase(); err != nil {
			log.Printf("Error transitioning to reduce phase: %v", err)
			return err
		}
	}

	// Attempt to assign a task
	task, err := c.taskTracker.AssignTask(args.WorkerID)
	if err != nil {
		log.Printf("Task assignment error for worker %s: %v", args.WorkerID, err)
		return err
	}

	// If no task is available right now, but not job complete
	if task == nil {
		// Check if we're just waiting for tasks to complete
		if c.taskTracker.hasStartedReduce {
			reply.JobComplete = c.taskTracker.IsReducePhaseDone()
		}
		log.Printf("No tasks available for worker %s at the moment", args.WorkerID)
		return nil
	}

	// Populate task details
	reply.TaskID = task.ID
	reply.Type = task.Type
	reply.Input = task.Input
	reply.NReduce = c.taskTracker.nReduce
	reply.MapID = len(c.inputFiles)
	reply.InterDir = c.interDir

	log.Printf("Assigned task %d of type %v to worker %s",
		task.ID, task.Type, args.WorkerID)

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
	// Initiate shutdown if not already done
	c.initiateShutdown()

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

func (c *Coordinator) recordHeartbeat(workerID string, status WorkerStatus) {
	c.mu.Lock()
	defer c.mu.Unlock()

	worker, exists := c.workers[workerID]
	if !exists {
		return
	}

	worker.lastHeartbeat = time.Now()
	worker.status = status
}

func (c *Coordinator) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.isShuttingDown {
		reply.ShouldContinue = false
		return nil
	}

	worker, exists := c.workers[args.WorkerID]
	if !exists {
		return fmt.Errorf("unknown worker")
	}

	worker.lastHeartbeat = time.Now()
	worker.status = args.Status
	worker.active = true

	reply.ShouldContinue = !c.isShuttingDown
	return nil
}

func (c *Coordinator) IsComplete() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	log.Printf("Checking job completion. Reduce started: %v", c.taskTracker.hasStartedReduce)

	if !c.taskTracker.hasStartedReduce {
		log.Println("Reduce phase not started yet")
		return false
	}

	// Detailed logging of task states
	for id, task := range c.taskTracker.tasks {
		log.Printf("Task %d: Type=%s, State=%s", id, task.Type, task.State)
	}

	return c.taskTracker.IsReducePhaseDone()
}

func (c *Coordinator) checkWorkerHealth() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	log.Printf("Checking worker health. Current workers count: %d", len(c.workers))

	for id, worker := range c.workers {
		if !worker.active {
			continue
		}

		timeSinceHeartbeat := now.Sub(worker.lastHeartbeat)
		log.Printf("Worker %s: Time since last heartbeat: %v", id, timeSinceHeartbeat)

		if timeSinceHeartbeat > c.maxHeartbeatDelay {
			log.Printf("Worker %s missed heartbeat (delay: %v), marking as inactive",
				id, timeSinceHeartbeat)
			worker.active = false

			if worker.status.CurrentTaskID != 0 {
				if err := c.taskTracker.ReassignFailedTask(worker.status.CurrentTaskID, id); err != nil {
					log.Printf("Failed to reassign task from worker %s: %v", id, err)
				}
			}
		}
	}
}

func (c *Coordinator) startHealthCheck() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(c.healthCheckInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				c.checkWorkerHealth()
			case <-c.shutdown:
				return
			}
		}
	}()
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

	// Explicitly check all reduce tasks
	allTasksCompleted := true
	for _, task := range c.taskTracker.tasks {
		if task.Type == ReduceTask && task.State != TaskCompleted {
			allTasksCompleted = false
			break
		}
	}

	if allTasksCompleted && !c.done {
		c.done = true
		log.Println("All tasks completed, initiating shutdown...")

		go func() {
			c.initiateShutdown()
		}()
	}

	return allTasksCompleted
}

func (c *Coordinator) initiateShutdown() {
	c.mu.Lock()
	if !c.isShuttingDown {
		c.isShuttingDown = true
		close(c.shutdown)
		// Immediately close the listener to stop accepting new connections
		if c.listener != nil {
			c.listener.Close()
		}
		// Force close any existing connections
		for _, worker := range c.workers {
			worker.active = false
		}
	}
	c.mu.Unlock()

	// Give a short grace period for cleanup
	time.Sleep(100 * time.Millisecond)

	// Force exit after timeout
	go func() {
		time.Sleep(5 * time.Second)
		log.Println("Forcing exit due to shutdown timeout")
		os.Exit(0)
	}()
}
