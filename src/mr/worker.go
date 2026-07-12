package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"log/slog"
	"net/rpc"
	"os"
	"sort"

	_ "6.5840/config"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// Implements for the sort.Interface
type KVList []KeyValue

func (kv KVList) Len() int           { return len(kv) }
func (kv KVList) Swap(i, j int)      { kv[i], kv[j] = kv[j], kv[i] }
func (kv KVList) Less(i, j int) bool { return kv[i].Key < kv[i].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator
var nReduce int

func writeIntermediate(kvs []KeyValue, taskId int) []string {
	// Store all kvs in a map to avoid open the file repeatedly
	fileToContent := make(map[string]([]KeyValue))
	for _, kv := range kvs {
		reducerId := ihash(kv.Key) % nReduce
		filename := fmt.Sprintf("mr-%v-%v", taskId, reducerId)
		fileToContent[filename] = append(fileToContent[filename], kv)
	}
	interFileNames := []string{}
	for filename, kvs := range fileToContent {
		interFileNames = append(interFileNames, filename)
		fTemp, err := os.CreateTemp("", "mr-tmp-*")
		tempPath := fTemp.Name()
		if err != nil {
			log.Fatal(err, "in temp file create")
		}
		enc := json.NewEncoder(fTemp)
		if err := enc.Encode(kvs); err != nil {
			log.Fatalf(err.Error())
		}
		fTemp.Close()
		err = os.Rename(tempPath, fmt.Sprintf("/tmp/%v", filename))
		if err != nil {
			log.Fatal(err)
		}
	}
	return interFileNames
}

func readFromInterFiles(interFileNames []string) (allkvs []KeyValue) {
	for _, filename := range interFileNames {
		f, err := os.Open(fmt.Sprintf("/tmp/%v", filename))
		if err != nil {
			log.Fatal(err, "in readFromInterFiles")
		}
		var kvs []KeyValue
		dec := json.NewDecoder(f)
		if err = dec.Decode(&kvs); err != nil {
			log.Fatal(err, "in decode")
		}
		allkvs = append(allkvs, kvs...)
	}
	return
}

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname
	args := WorkerArgs{Status: Wait, IsFirstCall: true}
	// Waiting for the coordinator to assign tasks
	for {
		reply := WorkerReply{}
		ok := call("Coordinator.Assign", &args, &reply)
		if !ok {
			log.Fatalf("RPC call failure")
		} else {
			if reply.Status == Wait {
				args = WorkerArgs{Status: Wait, WorkerId: reply.WorkerId}
				continue
			}
			// Do the task
			files := reply.Files
			nReduce = reply.NReduce
			switch reply.Status {
			case MapTask:
				slog.Debug("Start map task", "worker_id", reply.WorkerId, "task_id", reply.TaskId)
				// Iterate all files
				kvs := []KeyValue{}
				for _, filename := range files {
					content, err := os.ReadFile(filename)
					if err != nil {
						log.Fatalf("Read file fail: %v", filename)
						args = WorkerArgs{Status: Wait}
						continue
					}
					kvs = append(kvs, mapf(filename, string(content))...)
				}
				// Generate temporary file for reduce worker
				interFileNames := writeIntermediate(kvs, reply.TaskId)
				args = WorkerArgs{Status: MapDone, InterFileNames: interFileNames, WorkerId: reply.WorkerId}
				slog.Debug("Map task done", "worker_id", reply.WorkerId, "task_id", reply.TaskId)
			case ReduceTask:
				slog.Debug("Start reduce task", "worker_id", reply.WorkerId, "task_id", reply.TaskId)
				// Read files
				interFileNames := reply.Files
				reducerId := reply.TaskId
				kvs := readFromInterFiles(interFileNames)
				// Sort it
				sort.Sort(KVList(kvs))
				// Construct the args for reducef
				keyToAllValues := make(map[string][]string)
				for _, kv := range kvs {
					keyToAllValues[kv.Key] = append(keyToAllValues[kv.Key], kv.Value)
				}
				oFile, err := os.Create(fmt.Sprintf("mr-out-%v", reducerId))
				if err != nil {
					log.Fatalf("Create file %v fail\n", oFile)
				}
				// Do the reducef
				for k, vs := range keyToAllValues {
					res := reducef(k, vs)
					fmt.Fprintf(oFile, "%v %v\n", k, res)
				}
				args = WorkerArgs{Status: ReduceDone, WorkerId: reply.WorkerId}
				slog.Debug("Reduce task done", "worker_id", reply.WorkerId, "task_id", reply.TaskId)
			case Exit:
				os.Exit(0)
			}
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	// the "Coordinator.Example" tells the
// 	// receiving server that we'd like to call
// 	// the Example() method of struct Coordinator.
// 	ok := call("Coordinator.Example", &args, &reply)
// 	if ok {
// 		// reply.Y should be 100.
// 		fmt.Printf("reply.Y %v\n", reply.Y)
// 	} else {
// 		fmt.Printf("call failed!\n")
// 	}
// }

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args any, reply any) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
