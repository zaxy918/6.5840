package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator
var status string
var taskId int
var nReduce int

func writeIntermediate(kvs []KeyValue) []string {
	// Write all kvs to file to avoid open the file repeatedly
	fileToContent := make(map[string]([]KeyValue))
	for _, kv := range kvs {
		reducerId := ihash(kv.Key) % nReduce
		filename := fmt.Sprintf("mr-%v-%v", taskId, reducerId)
		fileToContent[filename] = append(fileToContent[filename], kv)
	}
	interFileNames := []string{}
	for filename, kvs := range fileToContent {
		interFileNames = append(interFileNames, filename)
		fTemp, err := os.CreateTemp("", filename)
		// *Debug
		fmt.Println("Create temp file %v", fTemp.Name())
		defer fTemp.Close()
		if err != nil {
			log.Fatalf(err.Error())
		}
		enc := json.NewEncoder(fTemp)
		if err := enc.Encode(kvs); err != nil {
			log.Fatalf(err.Error())
		}
	}
	return interFileNames
}

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname
	status = Wait
	args := WorkerArgs{Status: status}
	// Waiting for the coordinator to assign tasks
	for {
		reply := WorkerReply{}
		ok := call("Coordinator.Assign", &args, &reply)
		if !ok {
			log.Fatalf("RPC call failure")
		} else {
			fmt.Println(reply)
			// Run the task
			status = reply.Status
			taskId = reply.TaskId
			files := reply.Files
			nReduce = reply.NReduce
			switch status {
			case MapTask:
				// Iterate all files
				kvs := []KeyValue{}
				for _, filename := range files {
					content, err := os.ReadFile(filename)
					if err != nil {
						log.Fatalf("Read file fail: %v", filename)
						status = Wait
						continue
					}
					kvs = append(kvs, mapf(filename, string(content))...)
				}
				// Generate temporary file for reduce worker
				interFileNames := writeIntermediate(kvs)
				args = WorkerArgs{MapDone, interFileNames}
			case ReduceTask:
				//TODO Reduce task
			case Exit:
				os.Exit(0)
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
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
