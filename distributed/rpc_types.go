package distributed

import "time"

type HeartbeatArgs struct {
	WorkerID string
	Status   WorkerStatus
}

type HeartbeatReply struct {
	ShouldContinue bool
}

type WorkerStatus struct {
	Timestamp     time.Time
	LastError     string
	CurrentTaskID int
	TaskProgress  float64
	MemoryUsage   uint64
	CPUUsage      float64
}

type RegisterArgs struct {
	WorkerID   string
	WorkerAddr string
}

type RegisterReply struct {
	WorkerID string
}

type GetTaskArgs struct {
	WorkerID string
}

type GetTaskReply struct {
	Input       string
	InterDir    string
	TaskID      int
	Type        TaskType
	NReduce     int
	MapID       int
	JobComplete bool
}

type TaskCompleteArgs struct {
	Results  map[string]string
	WorkerID string
	Error    string
	TaskID   int
	Success  bool
}

type TaskCompleteReply struct{}
