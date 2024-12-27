package distributed

import (
	"fmt"
	"log"
	"sync"
	"time"
)

const (
	maxAttempts    = 3
	defaultTimeout = 10 * time.Second
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

type TaskMetadata struct {
	StartTime     time.Time
	FailedWorkers map[string]int
	LastWorker    string
	Attempts      int
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

	for id, task := range t.tasks {
		log.Printf("Examining task %d: State=%v, Type=%v", id, task.State, task.Type)
		if task.State == TaskIdle {
			task.State = TaskInProgress
			task.Metadata.StartTime = time.Now()
			task.Metadata.LastWorker = workerID
			task.Metadata.Attempts++
			log.Printf("Assigned task %d to worker %s", id, workerID)
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

	log.Printf("Checking map phase completion. Tasks: %+v", t.tasks)
	for _, task := range t.tasks {
		log.Printf("Task %d: Type=%d, State=%d", task.ID, task.Type, task.State)
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
