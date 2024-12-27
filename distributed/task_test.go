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
