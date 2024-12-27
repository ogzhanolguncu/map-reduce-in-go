package distributed

type RegisterArgs struct {
	WorkerAddr string
}

type RegisterReply struct {
	WorkerID string
}

type GetTaskArgs struct {
	WorkerID string
}

type GetTaskReply struct {
	TaskID  int
	Type    TaskType
	Input   string
	NReduce int // For map tasks
	MapID   int // For reduce tasks
}

type TaskCompleteArgs struct {
	WorkerID string
	TaskID   int
	Success  bool
	Error    string
	Results  map[string]string // For reduce task results
}

type TaskCompleteReply struct{}
