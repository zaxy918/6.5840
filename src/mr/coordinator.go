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
	"time"
)

const (
	MAX_WORKER  = 10
	EXPIRE_TIME = 10 * time.Second
)

type WorkerEntry struct {
	status   Status
	workerId int
	files    []string
	deadLine time.Time
	taskId   int
}

type Task struct {
	taskId int
	status Status
	files  []string
}

type Coordinator struct {
	mu                sync.Mutex
	cond              *sync.Cond
	workerId          int
	allMapDone        bool
	allReduceDone     bool
	interFileCnt      int
	nReduce           int
	aliveWorker       int
	taskId            int
	mapperCnt         int
	reducerCnt        int
	inputFile         []string
	intermediateFiles map[int][]string
	workerList        []WorkerEntry
	failTaskFiles     []Task
}

func (c *Coordinator) replyAndSetWorkerList(worker *WorkerEntry, reply *WorkerReply) {
	c.workerList[worker.workerId] = *worker
	*reply = WorkerReply{worker.status, worker.workerId, worker.taskId, worker.files, c.nReduce}
}

func (c *Coordinator) Assign(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	worker := WorkerEntry{status: args.Status, workerId: args.WorkerId}
	if args.IsFirstCall {
		worker.workerId = c.workerId
		worker.taskId = -1
		c.workerList = append(c.workerList, worker)
		c.workerId++
		c.aliveWorker++
	}
	if c.workerList[worker.workerId].status == Exit {
		worker = WorkerEntry{Exit, worker.workerId, nil, time.Now().Add(EXPIRE_TIME), -1}
		c.replyAndSetWorkerList(&worker, reply)
		return nil
	}
	switch worker.status {
	case Wait:
		switch {
		case len(c.failTaskFiles) > 0:
			task := c.failTaskFiles[0]
			//*debug
			// fmt.Printf("Worker%v continue failed task%v\n", worker.workerId, task.taskId)
			expireTime := time.Now().Add(EXPIRE_TIME)
			worker = WorkerEntry{task.status, worker.workerId, task.files, expireTime, task.taskId}
			c.replyAndSetWorkerList(&worker, reply)
			c.failTaskFiles = c.failTaskFiles[1:]
		case !c.allMapDone && len(c.inputFile) > 0:
			// Have files to assign
			//*debug
			// fmt.Printf("Assign a map task with taskId=%v\n", c.taskId)
			expireTime := time.Now().Add(EXPIRE_TIME)
			worker = WorkerEntry{MapTask, worker.workerId, []string{c.inputFile[0]}, expireTime, c.taskId}
			c.replyAndSetWorkerList(&worker, reply)
			c.inputFile = c.inputFile[1:]
			c.mapperCnt++
			c.taskId++
			//*debug
			// fmt.Println(reply)
			// No files to assign
		case c.interFileCnt > 0 && c.allMapDone:
			//*debug
			// fmt.Printf("Assign a reduce task with taskId=%v\n", c.taskId)
			interFileNames := c.intermediateFiles[c.taskId]
			expireTime := time.Now().Add(EXPIRE_TIME)
			worker = WorkerEntry{ReduceTask, worker.workerId, interFileNames, expireTime, c.taskId}
			c.replyAndSetWorkerList(&worker, reply)
			c.interFileCnt -= len(c.intermediateFiles[c.taskId])
			c.taskId++
			c.reducerCnt++
		case c.allReduceDone:
			c.cond.Broadcast()
			//*debug
			// fmt.Println("Kill worker")
			c.aliveWorker--
			worker.status = Exit
			c.replyAndSetWorkerList(&worker, reply)
			if c.aliveWorker == 0 {
				//*debug
				// fmt.Println("All worker killed, clean inter files")
				for _, filenames := range c.intermediateFiles {
					for _, filename := range filenames {
						os.Remove(fmt.Sprintf("/tmp/%v", filename))
					}
				}
				//*debug
				// fmt.Println("All inter files cleaned, goodbye!")
			}
		default:
			//*debug
			// fmt.Println("No work, just wait")
			c.cond.Wait()
			expireTime := time.Now().Add(EXPIRE_TIME)
			worker.deadLine = expireTime
			c.replyAndSetWorkerList(&worker, reply)
		}
	case MapDone:
		//*debug
		// fmt.Println("Maptask done, store inter file location")
		c.mapperCnt--
		c.interFileCnt += len(args.InterFileNames)
		for _, filename := range args.InterFileNames {
			parts := strings.Split(filename, "-")
			reducerId, err := strconv.Atoi(parts[len(parts)-1])
			if err != nil {
				log.Fatal(err, "in MapDone")
			}
			c.intermediateFiles[reducerId] = append(c.intermediateFiles[reducerId], filename)
		}
		//*debug
		// fmt.Println("Inter file stored")
		// All mappers done
		// This is the last mapper and all files are handled
		if c.mapperCnt == 0 && len(c.inputFile) == 0 && len(c.failTaskFiles) == 0 {
			//*debug
			// fmt.Println("All map work done")
			c.allMapDone = true
			c.taskId = 0
			c.cond.Broadcast()
		}
		expireTime := time.Now().Add(EXPIRE_TIME)
		worker = WorkerEntry{Wait, worker.workerId, nil, expireTime, -1}
		c.replyAndSetWorkerList(&worker, reply)
	case ReduceDone:
		c.reducerCnt--
		if c.reducerCnt == 0 && c.interFileCnt == 0 && len(c.failTaskFiles) == 0 {
			c.allReduceDone = true
		}
		//*debug
		// fmt.Println("One reduce task done")
		expireTime := time.Now().Add(EXPIRE_TIME)
		worker = WorkerEntry{Wait, worker.workerId, nil, expireTime, -1}
		c.replyAndSetWorkerList(&worker, reply)
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
	//*debug
	// fmt.Println("Start server")
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.aliveWorker == 0 && c.allReduceDone {
		//*debug
		// fmt.Println("Coordinator done")
		return true
	}
	return false
}

func (c *Coordinator) checkWorkerStatus() {
	for {
		time.Sleep(time.Second)
		c.mu.Lock()
		for _, worker := range c.workerList {
			//*debug
			// fmt.Printf("The worker %v\n", worker)
			if worker.status != Wait && time.Now().After(worker.deadLine) && worker.status != Exit {
				//*debug
				// fmt.Printf("Worker%v died with task%v\n", worker.workerId, worker.taskId)
				c.failTaskFiles = append(c.failTaskFiles, Task{worker.taskId, worker.status, worker.files})
				c.workerList[worker.workerId].status = Exit
				c.aliveWorker--
				c.cond.Broadcast()
			}
		}
		c.mu.Unlock()
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		inputFile:         files,
		nReduce:           nReduce,
		intermediateFiles: make(map[int][]string),
	}
	c.cond = sync.NewCond(&c.mu)
	//*debug
	// fmt.Println("create coordinator")
	c.server(sockname)
	go c.checkWorkerStatus()
	return &c
}
