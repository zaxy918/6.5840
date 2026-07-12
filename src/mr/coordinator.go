package mr

import (
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "6.5840/config"
)

// The task expire after 10 seconds from the time it was asigned
func getExpireTime() time.Time { return time.Now().Add(time.Second * 10) }

// For coordinator to keep track on the status of workers
type WorkerEntry struct {
	status   Status
	workerId int       // Assigned when the worker do its first rpc
	files    []string  // The files the worker was assigned, inputfiles or intermediate files
	deadLine time.Time // When the worker should be considered as died
	taskId   int       // Assigned when the worker is assigned a task
}

// To record a failed task
type Task struct {
	taskId int      // Defined when the task was first assigned, corresponding with the taskId in WorkerEntry
	status Status   // Reduce task or map task
	files  []string // Input files or intermediate files
}

type Coordinator struct {
	mu                sync.Mutex
	cond              *sync.Cond
	workerId          int // Auto-increment
	allMapDone        bool
	allReduceDone     bool
	interFileCnt      int // To record how many intermediate files can be assigned
	nReduce           int
	aliveWorker       int // Count the worker that still aslive
	taskId            int // Auto-increment, and be initialized as 0 in both map phase and reduce phase
	mapperCnt         int // Count the mappers
	reducerCnt        int // Count the reducers
	inputFile         []string
	intermediateFiles map[int][]string
	workerList        []WorkerEntry
	failTask          []Task
}

func (c *Coordinator) replyAndSetWorkerList(worker *WorkerEntry, reply *WorkerReply) {
	c.workerList[worker.workerId] = *worker
	*reply = WorkerReply{worker.status, worker.workerId, worker.taskId, worker.files, c.nReduce}
}

func (c *Coordinator) Assign(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	worker := WorkerEntry{status: args.Status, workerId: args.WorkerId}
	// When the worker first do rpc, something should be initialized
	if args.IsFirstCall {
		worker.workerId = c.workerId
		worker.taskId = -1
		c.workerList = append(c.workerList, worker)
		c.workerId++
		c.aliveWorker++
	}
	// The worker now is marked dead
	if c.workerList[worker.workerId].status == Exit {
		worker = WorkerEntry{Exit, worker.workerId, nil, getExpireTime(), -1}
		c.replyAndSetWorkerList(&worker, reply)
		return nil
	}
	// The main logic of assigning tasks depending on the status that the worker send in rpc args
	switch worker.status {
	case Wait:
		switch {
		// Some files has failed
		case len(c.failTask) > 0:
			// Take the task out
			task := c.failTask[0]
			c.failTask = c.failTask[1:]
			// Assign to worker
			slog.Debug("Continue failed task", "worker_id", worker.workerId, "task_id", task.taskId)
			worker = WorkerEntry{task.status, worker.workerId, task.files, getExpireTime(), task.taskId}
			c.replyAndSetWorkerList(&worker, reply)
		// Map phase
		case !c.allMapDone && len(c.inputFile) > 0:
			slog.Debug("Assign map task", "task_id", c.taskId, "worker_id", worker.workerId)
			// Take the input file out
			fiilename := c.inputFile[0]
			c.inputFile = c.inputFile[1:]
			// Assign to worker
			worker = WorkerEntry{MapTask, worker.workerId, []string{fiilename}, getExpireTime(), c.taskId}
			c.replyAndSetWorkerList(&worker, reply)
			// Some state update
			c.mapperCnt++
			c.taskId++
		// Reduce phase
		case c.interFileCnt > 0 && c.allMapDone:
			slog.Debug("Assign reduce task", "task_id", c.taskId, "worker_id", worker.workerId)
			// Take the intermediate files out
			interFileNames := c.intermediateFiles[c.taskId]
			c.interFileCnt -= len(c.intermediateFiles[c.taskId])
			worker = WorkerEntry{ReduceTask, worker.workerId, interFileNames, getExpireTime(), c.taskId}
			c.replyAndSetWorkerList(&worker, reply)
			// Some state update
			c.taskId++
			c.reducerCnt++
		case c.allReduceDone:
			// All work done, wake the waiting worker to exit
			c.cond.Broadcast()
			slog.Debug("Kill worker")
			// All worker exit, clean the intermediate files
			if c.aliveWorker == 0 {
				slog.Debug("All worker killed, clean inter files")
				for _, filenames := range c.intermediateFiles {
					for _, filename := range filenames {
						os.Remove(fmt.Sprintf("/tmp/%v", filename))
					}
				}
				slog.Debug("All inter files cleaned, goodbye!")
			}
			// Tell the worker to exit
			worker.status = Exit
			c.replyAndSetWorkerList(&worker, reply)
			// State update
			c.aliveWorker--
		// No task to assign
		default:
			slog.Debug("No work, just wait")
			// Wait for all work done, some task fail, or all map done
			c.cond.Wait()
			// Reply to worker to start a new rpc
			worker.deadLine = getExpireTime()
			c.replyAndSetWorkerList(&worker, reply)
		}
	case MapDone:
		slog.Debug("Map task done, store intermediate file locations", "worker_id", worker.workerId)
		// Store the intermediate files
		for _, filename := range args.InterFileNames {
			parts := strings.Split(filename, "-")
			reducerId, err := strconv.Atoi(parts[len(parts)-1])
			if err != nil {
				log.Fatal(err, "in MapDone")
			}
			c.intermediateFiles[reducerId] = append(c.intermediateFiles[reducerId], filename)
		}
		slog.Debug("Stored intermediate file locations", "worker_id", worker.workerId)
		// Some state update
		c.mapperCnt--
		c.interFileCnt += len(args.InterFileNames)
		// All mappers done
		if c.mapperCnt == 0 && len(c.inputFile) == 0 && len(c.failTask) == 0 {
			slog.Debug("All map work done")
			c.allMapDone = true
			c.taskId = 0
			c.cond.Broadcast()
		}
		// Reply
		worker = WorkerEntry{Wait, worker.workerId, nil, getExpireTime(), -1}
		c.replyAndSetWorkerList(&worker, reply)
	case ReduceDone:
		c.reducerCnt--
		if c.reducerCnt == 0 && c.interFileCnt == 0 && len(c.failTask) == 0 {
			c.allReduceDone = true
		}
		slog.Debug("Reduce task done", "worker_id", worker.workerId)
		// Reply
		worker = WorkerEntry{Wait, worker.workerId, nil, getExpireTime(), -1}
		c.replyAndSetWorkerList(&worker, reply)
	default:
		return errors.New("invalid task status")
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	slog.Debug("Start rpc server")
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.aliveWorker == 0 && c.allReduceDone {
		slog.Debug("Coordinator done")
		return true
	}
	return false
}

// Try to find if some worker is dead
func (c *Coordinator) checkWorkerStatus() {
	for {
		time.Sleep(time.Second)
		c.mu.Lock()
		for _, worker := range c.workerList {
			slog.Debug("Check worker", "worker", worker)
			if worker.status != Wait && time.Now().After(worker.deadLine) && worker.status != Exit {
				slog.Debug("Worker died", "worker_id", worker.workerId, "task_id", worker.taskId)
				// Add fail task to list
				c.failTask = append(c.failTask, Task{worker.taskId, worker.status, worker.files})
				c.workerList[worker.workerId].status = Exit
				c.aliveWorker--
				// Tell the waiting worker to have task
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
	c.server(sockname)
	go c.checkWorkerStatus()
	slog.Debug("Create coordinator")
	return &c
}
