package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type Status int

const (
	MapTask Status = iota
	ReduceTask
	Wait
	MapDone
	ReduceDone
	Exit
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Worker RPC args
type WorkerArgs struct {
	Status         Status
	IsFirstCall    bool
	InterFileNames []string
	WorkerId       int
}

// Worker RPC reply
type WorkerReply struct {
	Status   Status
	WorkerId int
	TaskId   int
	Files    []string
	NReduce  int
}
