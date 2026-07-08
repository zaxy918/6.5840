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

const (
	MapTask    string = "MapTask"
	ReduceTask string = "ReduceTask"
	Wait       string = "Wait"
	MapDone    string = "MapDone"
	ReduceDone string = "ReduceDone"
	Exit       string = "Exit"
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
	Status         string
	InterFileNames []string
}

// Worker RPC reply
type WorkerReply struct {
	Status  string
	TaskId  int
	Files   []string
	NReduce int
}
