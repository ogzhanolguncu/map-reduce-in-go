package distributed

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
	TaskID      int
	Type        TaskType
	NReduce     int
	MapID       int
	JobComplete bool
	InterDir    string
}

type TaskCompleteArgs struct {
	Results  map[string]string
	WorkerID string
	Error    string
	TaskID   int
	Success  bool
}

type TaskCompleteReply struct{}
