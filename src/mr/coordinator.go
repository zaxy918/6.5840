package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	//TODO Your definitions here.
	mMapper           int
	rReducer          int
	allMapDone        bool
	aliveWorker       int
	mu                sync.Mutex
	inputFile         []string
	inputIdx          int
	intermediateFiles []string
	nReduce           int
}

// TODO Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Assign(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	status := args.Status
	switch status {
	case Wait:
		// TODO File part
		//*Debug
		fmt.Println("In Wait case")
		// Mappers haven't done
		if !c.allMapDone {
			//*Debug
			fmt.Println("In !c.allMapDone")
			if c.inputIdx < len(c.inputFile) {
				//*Debug
				fmt.Println("assign mapper")
				c.mMapper++
				c.aliveWorker++
				*reply = WorkerReply{MapTask, c.mMapper, []string{c.inputFile[c.inputIdx]}, c.nReduce}
				fmt.Println("Reply in coordinator", reply)
				c.inputIdx++
			} else {
				*reply = WorkerReply{Status: Wait}
			}
		} else { //TODO All mappers done
			//* Debug
			*reply = WorkerReply{Status: Exit}
		}
	case MapDone:
		c.mMapper--
		c.intermediateFiles = append(c.intermediateFiles, args.InterFileNames...)
		// All mappers done
		if c.mMapper == 0 {
			c.allMapDone = true
			//* Debug code
			for _, filename := range c.intermediateFiles {
				fmt.Println("Remove", filename)
				os.Remove(filename)
			}
			os.Remove("./tmp")
			os.Exit(0)
		}
		*reply = WorkerReply{Status: Wait}
		//TODO
	case ReduceDone:
	default:
		return errors.New("invalid task status")
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
	//*Debug
	fmt.Println("coordinator rpc server running")
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	//TODO Your code here.
	if c.allMapDone {
		ret = true
	}
	//*Debug
	fmt.Println("Running in done")
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{inputFile: files, nReduce: nReduce}
	// *Debug
	fmt.Println("create coordinator")
	c.server(sockname)
	return &c
}
