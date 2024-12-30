package distributed

import (
	"fmt"
	"log"
	"sync"
	"time"
)

const (
	maxAttempts          = 3
	defaultTimeout       = 10 * time.Second
	replicationThreshold = 1.5 // Replicate if task takes 1.5x longer than average
	minTaskForStats      = 3   // Need at least tasks to calculate meaningful stats
)

type TaskState int

const (
	TaskIdle TaskState = iota
	TaskInProgress
	TaskCompleted
)

func (s TaskState) String() string {
	switch s {
	case TaskIdle:
		return "Idle"
	case TaskInProgress:
		return "InProgress"
	case TaskCompleted:
		return "Completed"
	default:
		return fmt.Sprintf("Unknown(%d)", int(s))
	}
}

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
)

func (t TaskType) String() string {
	switch t {
	case MapTask:
		return "Map"
	case ReduceTask:
		return "Reduce"
	default:
		return fmt.Sprintf("Unknown(%d)", int(t))
	}
}

type TaskMetadata struct {
	StartTime     time.Time
	FailedWorkers map[string]int
	Replicas      map[string]*TaskReplica
	LastWorker    string
	Attempts      int
	ReplicaCount  int
}

type TaskReplica struct {
	StartTime time.Time
	WorkerID  string
	State     TaskState
}

type Task struct {
	Input    string
	Metadata TaskMetadata
	ID       int
	Type     TaskType
	State    TaskState
}

type TaskTracker struct {
	tasks            map[int]*Task
	mu               sync.RWMutex
	nReduce          int
	timeout          time.Duration
	hasStartedReduce bool
}

func NewTaskTracker(nReduce int) *TaskTracker {
	return &TaskTracker{
		tasks:   make(map[int]*Task),
		nReduce: nReduce,
		timeout: defaultTimeout,
	}
}

func (t *TaskTracker) AssignTask(workerID string) (*Task, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	log.Printf("Attempting to assign task. Current tasks: %+v", t.tasks)

	for _, task := range t.tasks {
		if task.State == TaskIdle {
			task.State = TaskInProgress
			task.Metadata.StartTime = time.Now()
			task.Metadata.LastWorker = workerID
			task.Metadata.Attempts++
			return task, nil
		}
	}

	log.Printf("No idle tasks found")
	return nil, nil
}

func (t *TaskTracker) CheckTimeouts() {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	for _, task := range t.tasks {
		if task.State == TaskInProgress && now.Sub(task.Metadata.StartTime) > t.timeout {
			if task.Metadata.Attempts < maxAttempts {
				task.State = TaskIdle
			}
		}
	}
}

func (t *TaskTracker) MarkComplete(taskID int) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	task, exists := t.tasks[taskID]
	if !exists {
		log.Printf("Task %d not found", taskID)
		return fmt.Errorf("task %d not found", taskID)
	}

	log.Printf("Marking task %d as completed. Previous state: %v", taskID, task.State)
	task.State = TaskCompleted
	return nil
}

func (t *TaskTracker) IsMapPhaseDone() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	log.Printf("Checking map phase completion. Total tasks: %d", len(t.tasks))
	for id, task := range t.tasks {
		log.Printf("Task %d: Type=%s, State=%s, Input=%s",
			id, task.Type, task.State, task.Input)
		if task.Type == MapTask && task.State != TaskCompleted {
			return false
		}
	}
	return true
}

func (t *TaskTracker) IsReducePhaseDone() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for _, task := range t.tasks {
		log.Printf("Task %d is not complete: %s", task.ID, task.State)
		if task.Type == ReduceTask && task.State != TaskCompleted {
			return false
		}
	}
	return true
}

func (t *TaskTracker) InitReduceTasks(nMap int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.tasks = make(map[int]*Task)
	for i := 0; i < t.nReduce; i++ {
		t.tasks[i] = &Task{
			ID:    i,
			Type:  ReduceTask,
			State: TaskIdle,
			Input: fmt.Sprintf("%d", i),
		}
	}
}

func (t *TaskTracker) InitMapTasks(files []string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.tasks = make(map[int]*Task)
	for i, file := range files {
		t.tasks[i] = &Task{
			ID:    i,
			Type:  MapTask,
			State: TaskIdle,
			Input: file,
			Metadata: TaskMetadata{
				FailedWorkers: make(map[string]int),
				Replicas:      make(map[string]*TaskReplica),
				ReplicaCount:  0,
			},
		}
	}
}

func (t *TaskTracker) TransitionToReducePhase() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.isMapPhaseDoneNoLock() {
		return fmt.Errorf("map phase not complete")
	}

	t.tasks = make(map[int]*Task)
	for i := 0; i < t.nReduce; i++ {
		t.tasks[i] = &Task{
			ID:    i,
			Type:  ReduceTask,
			State: TaskIdle,
			Input: fmt.Sprintf("%d", i),
			Metadata: TaskMetadata{
				FailedWorkers: make(map[string]int),
			},
		}
	}

	t.hasStartedReduce = true
	return nil
}

func (t *TaskTracker) isMapPhaseDoneNoLock() bool {
	for _, task := range t.tasks {
		if task.Type == MapTask && task.State != TaskCompleted {
			return false
		}
	}
	return true
}

func (t *TaskTracker) ReassignFailedTask(taskID int, workerID string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	task, exists := t.tasks[taskID]
	if !exists {
		return fmt.Errorf("task %d not found", taskID)
	}

	// Update failure count for worker
	task.Metadata.FailedWorkers[workerID]++

	// Reset task state if under max attempts
	if task.Metadata.Attempts < maxAttempts {
		task.State = TaskIdle
		task.Metadata.StartTime = time.Time{}
		task.Metadata.LastWorker = ""
		task.Metadata.Attempts++ // Add this
		return nil
	}

	return fmt.Errorf("task %d exceeded max attempts", taskID)
}

func (t *TaskTracker) checkForStragglers() {
	t.mu.Lock()
	defer t.mu.Unlock()

	var completionTimes []float64
	log.Printf("Starting straggler detection...")

	// Debug completed tasks
	for _, task := range t.tasks {
		if task.State == TaskCompleted {
			duration := time.Since(task.Metadata.StartTime)
			completionTimes = append(completionTimes, duration.Seconds())
			log.Printf("Completed task duration: %.2f seconds", duration.Seconds())
		}
	}

	// Debug completion times
	log.Printf("Number of completion times: %d (need %d)", len(completionTimes), minTaskForStats)
	if len(completionTimes) < minTaskForStats {
		log.Printf("Not enough completed tasks for statistics")
		return
	}

	avgTime := calculateAverage(completionTimes)
	log.Printf("Average completion time: %.2f seconds", avgTime)

	// Debug in-progress tasks
	for _, task := range t.tasks {
		if task.State == TaskInProgress {
			currentDuration := time.Since(task.Metadata.StartTime).Seconds()
			threshold := avgTime * replicationThreshold
			log.Printf("In-progress task duration: %.2f seconds (threshold: %.2f)",
				currentDuration, threshold)

			if currentDuration > threshold && task.Metadata.ReplicaCount == 0 {
				log.Printf("Marking task for replication")
				t.replicateTask(task)
			}
		}
	}
}

func (t *TaskTracker) replicateTask(task *Task) error {
	if task.State == TaskCompleted || task.Metadata.ReplicaCount >= 2 {
		return nil
	}

	if task.Metadata.Replicas == nil {
		task.Metadata.Replicas = make(map[string]*TaskReplica)
	}

	task.Metadata.ReplicaCount++
	return nil
}

func (t *TaskTracker) cancelReplicas(taskID int, successfulWorkerID string) {
	task := t.tasks[taskID]
	// Cancel all replicas except the successful one
	for workerID, replica := range task.Metadata.Replicas {
		if workerID != successfulWorkerID {
			replica.State = TaskIdle
		}
	}
}

func calculateAverage(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}

	var sum float64
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}
