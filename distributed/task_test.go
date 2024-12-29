package distributed

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTaskTrackerFailureHandling(t *testing.T) {
	tracker := NewTaskTracker(2)
	tracker.InitMapTasks([]string{"test_input.txt"})

	worker1 := "worker1"
	task, err := tracker.AssignTask(worker1)
	require.NoError(t, err)
	require.NotNil(t, task)

	// Only need maxAttempts-1 more attempts since one was used in AssignTask
	for i := 0; i < maxAttempts-1; i++ {
		err = tracker.ReassignFailedTask(task.ID, worker1)
		require.NoError(t, err)
	}

	// This should error as we've hit max attempts
	err = tracker.ReassignFailedTask(task.ID, worker1)
	require.Error(t, err)
}

func TestWorkerFailureTracking(t *testing.T) {
	tracker := NewTaskTracker(2)
	tracker.InitMapTasks([]string{"test_input.txt"})

	worker1 := "worker1"
	worker2 := "worker2"

	// Assign and fail task multiple times
	task, _ := tracker.AssignTask(worker1)
	tracker.ReassignFailedTask(task.ID, worker1)

	task, _ = tracker.AssignTask(worker2)
	tracker.ReassignFailedTask(task.ID, worker2)

	// Verify failure counts
	require.Equal(t, 1, tracker.tasks[task.ID].Metadata.FailedWorkers[worker1])
	require.Equal(t, 1, tracker.tasks[task.ID].Metadata.FailedWorkers[worker2])
}

func TestMultipleWorkerFailures(t *testing.T) {
	tracker := NewTaskTracker(2)
	tracker.InitMapTasks([]string{"test_input.txt"})

	task, _ := tracker.AssignTask("worker1")

	// Reset attempts since we just want to test multiple worker failures
	tracker.tasks[task.ID].Metadata.Attempts = 1

	// Test two worker failures (staying under maxAttempts=3)
	workers := []string{"worker2", "worker3"}
	for _, worker := range workers {
		err := tracker.ReassignFailedTask(task.ID, worker)
		require.NoError(t, err)
		require.Equal(t, 1, tracker.tasks[task.ID].Metadata.FailedWorkers[worker])
	}
}

func TestCheckTimeouts(t *testing.T) {
	tracker := NewTaskTracker(2)
	tracker.timeout = 1 * time.Millisecond
	tracker.InitMapTasks([]string{"test_input.txt"})

	task, _ := tracker.AssignTask("worker1")
	time.Sleep(2 * time.Millisecond)

	tracker.CheckTimeouts()
	require.Equal(t, TaskIdle, tracker.tasks[task.ID].State)
}

func TestStragglerDetection(t *testing.T) {
	tracker := NewTaskTracker(2)
	tracker.timeout = 1 * time.Second
	tracker.InitMapTasks([]string{"test1.txt", "test2.txt", "test3.txt", "test4.txt"})

	task1, _ := tracker.AssignTask("worker1")
	task2, _ := tracker.AssignTask("worker2")
	task3, _ := tracker.AssignTask("worker3")

	task1.Metadata.StartTime = time.Now().Add(-1 * time.Second)
	task2.Metadata.StartTime = time.Now().Add(-1 * time.Second)
	task3.Metadata.StartTime = time.Now().Add(-1 * time.Second)
	tracker.MarkComplete(task1.ID)
	tracker.MarkComplete(task2.ID)
	tracker.MarkComplete(task3.ID)

	// Make the last task take longer
	task4, _ := tracker.AssignTask("worker4")
	task4.Metadata.StartTime = time.Now().Add(-3 * time.Second)

	// Check for stragglers
	tracker.checkForStragglers()

	// Verify the slow task was marked for replication
	require.Equal(t, 1, task4.Metadata.ReplicaCount, "Slow task should be marked for replication")
}

func TestReplicaCompletion(t *testing.T) {
	coord := NewCoordinator(2, []string{"test1.txt"}, "")

	// Create a completed task for statistics
	comp1 := &Task{
		ID:    1,
		State: TaskCompleted,
		Metadata: TaskMetadata{
			StartTime: time.Now().Add(-1 * time.Second),
			Replicas:  make(map[string]*TaskReplica),
		},
	}
	coord.taskTracker.tasks[1] = comp1

	// Start task with first worker
	w1 := "worker1"
	getTaskArgs := &GetTaskArgs{WorkerID: w1}
	getTaskReply := &GetTaskReply{}
	err := coord.GetTask(getTaskArgs, getTaskReply)
	require.NoError(t, err)

	// Simulate task taking too long and getting replicated
	task := coord.taskTracker.tasks[getTaskReply.TaskID]
	task.Metadata.StartTime = time.Now().Add(-2 * time.Second)

	// Manually create a replica since checkForStragglers needs more stats
	task.Metadata.Replicas = make(map[string]*TaskReplica)
	task.Metadata.Replicas[w1] = &TaskReplica{
		StartTime: task.Metadata.StartTime,
		WorkerID:  w1,
		State:     TaskInProgress,
	}
	task.Metadata.ReplicaCount = 1

	// Assign replica to second worker
	w2 := "worker2"
	task.Metadata.Replicas[w2] = &TaskReplica{
		StartTime: time.Now(),
		WorkerID:  w2,
		State:     TaskInProgress,
	}

	// Complete the replica first
	completeArgs := &TaskCompleteArgs{
		WorkerID: w2,
		TaskID:   getTaskReply.TaskID,
		Success:  true,
		Results:  map[string]string{"test": "value"},
	}
	completeReply := &TaskCompleteReply{}
	err = coord.TaskComplete(completeArgs, completeReply)
	require.NoError(t, err)

	// Verify task state
	task = coord.taskTracker.tasks[getTaskReply.TaskID]
	require.Equal(t, TaskCompleted, task.State, "Task should be marked as completed")

	// Verify replicas were cancelled
	for workerID, replica := range task.Metadata.Replicas {
		if workerID != w2 {
			require.Equal(t, TaskIdle, replica.State, "Other replicas should be cancelled")
		}
	}
}

func TestReplicaFailure(t *testing.T) {
	coord := NewCoordinator(2, []string{"test1.txt"}, "")

	// Set up initial task and replica
	task := &Task{
		ID:    0,
		State: TaskInProgress,
		Metadata: TaskMetadata{
			Replicas:     make(map[string]*TaskReplica),
			ReplicaCount: 1,
		},
	}
	coord.taskTracker.tasks[0] = task

	// Add a replica
	task.Metadata.Replicas["worker2"] = &TaskReplica{
		StartTime: time.Now(),
		WorkerID:  "worker2",
		State:     TaskInProgress,
	}

	// Fail the replica
	completeArgs := &TaskCompleteArgs{
		WorkerID: "worker2",
		TaskID:   0,
		Success:  false,
		Error:    "replica failed",
	}
	completeReply := &TaskCompleteReply{}
	err := coord.TaskComplete(completeArgs, completeReply)
	require.NoError(t, err)

	// Verify replica was removed and count decreased
	task = coord.taskTracker.tasks[0]
	require.Equal(t, 0, task.Metadata.ReplicaCount, "Replica count should be decremented")
	require.NotContains(t, task.Metadata.Replicas, "worker2", "Failed replica should be removed")
}
