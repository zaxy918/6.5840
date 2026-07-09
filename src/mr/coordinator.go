package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
)

type Coordinator struct {
	mMapper           int
	mapperCnt         int
	rReducer          int
	allMapDone        bool
	aliveWorker       int
	mu                sync.Mutex
	inputFile         []string
	inputIdx          int
	intermediateFiles map[int][]string
	interFileCnt      int
	nReduce           int
}

// TODO Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Assign(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	status := args.Status
	if args.IsFirstCall {
		c.aliveWorker++
	}
	switch status {
	case Wait:
		// Mappers haven't done
		if !c.allMapDone {
			if c.inputIdx < len(c.inputFile) {
				// Have files to assign
				//fmt.Printf("Assign a map task with taskId=%v\n", c.mMapper)
				*reply = WorkerReply{MapTask, c.mMapper, []string{c.inputFile[c.inputIdx]}, c.nReduce}
				c.mMapper++
				c.mapperCnt++
				c.inputIdx++
			} else {
				// No files to assign
				//fmt.Println("No file to map, just wait")
				*reply = WorkerReply{Status: Wait}
			}
		} else if c.interFileCnt > 0 {
			//.fmt.Printf("Assign a reduce task with taskId=%v\n", c.rReducer)
			reduceId := c.rReducer
			interFileNames := c.intermediateFiles[reduceId]
			c.interFileCnt -= len(c.intermediateFiles[reduceId])
			*reply = WorkerReply{ReduceTask, reduceId, interFileNames, c.nReduce}
			c.rReducer++
		} else {
			//fmt.Println("Kill worker")
			c.aliveWorker--
			*reply = WorkerReply{Status: Exit}
			if c.aliveWorker == 0 {
				//fmt.Println("All worker killed, clean inter files")
				for _, filenames := range c.intermediateFiles {
					for _, filename := range filenames {
						os.Remove(fmt.Sprintf("/tmp/%v", filename))
					}
				}
				//fmt.Println("All inter files cleaned, goodbye!")
			}
		}
	case MapDone:
		//fmt.Println("Maptask done, store inter file location")
		c.mapperCnt--
		c.interFileCnt += len(args.InterFileNames)
		for _, filename := range args.InterFileNames {
			parts := strings.Split(filename, "-")
			reduceId, err := strconv.Atoi(parts[len(parts)-1])
			if err != nil {
				log.Fatal(err, "in MapDone")
			}
			c.intermediateFiles[reduceId] = append(c.intermediateFiles[reduceId], filename)
		}
		//fmt.Println("Inter file stored")
		// All mappers done
		// This is the last mapper and all files are handled
		if c.mapperCnt == 0 && c.inputIdx == len(c.inputFile) {
			//fmt.Println("All map work done")
			c.allMapDone = true
		}
		*reply = WorkerReply{Status: Wait}
	case ReduceDone:
		//fmt.Println("One reduce task done")
		*reply = WorkerReply{Status: Wait}
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
	//*Debug
	//fmt.Println("Start server")
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.aliveWorker == 0 && c.inputIdx == len(c.inputFile) {
		return true
	}
	return false
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{inputFile: files, nReduce: nReduce, intermediateFiles: make(map[int][]string)}
	// *Debug
	//fmt.Println("create coordinator")
	c.server(sockname)
	return &c
}
