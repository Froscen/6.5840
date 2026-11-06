package mr

import (
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"time"
	"sort"
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

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	for {
		var args GetTaskArgs
		var reply GetTaskReply

		ok := call("Coordinator.GetTask", &args, &reply)
		if !ok {
			time.Sleep(time.Second)
			continue
		}
		switch reply.TaskType {
		case MapTask:
			context, err := os.ReadFile(reply.Filename)
			if err != nil {
				//
				log.Printf("Worker %d: read file %q failed: %v", os.Getpid(), reply.Filename, err)
				continue
			}
			kva := mapf(reply.Filename, string(context))
			partitions := make([][]KeyValue, reply.NReduce)

			for _, kv := range kva {
				partition := ihash(kv.Key) % reply.NReduce
				partitions[partition] = append(partitions[partition], kv)
			}

			mapFailed := false
			for i := range partitions {
				partitionFailed := false

				filename := "mr-" + strconv.Itoa(reply.TaskID) + "-" + strconv.Itoa(i)

				file, err := os.Create(filename)
				if err != nil {
					partitionFailed = true
					mapFailed = true
					continue
				}

				enc := gob.NewEncoder(file)

				for _, kv := range partitions[i] {
					err := enc.Encode(kv)
					if err != nil {
						mapFailed = true
						partitionFailed = true
						break
					}
				}

				err = file.Close()
				if err != nil {
					partitionFailed = true
					mapFailed = true
				}

				if partitionFailed {
					err = os.Remove(filename)
				}
			}

			if !mapFailed {
				var ReportArgs ReportTaskArgs
				var ReportReply ReportTaskReply
				ReportArgs.TaskID = reply.TaskID
				ReportArgs.TaskType = reply.TaskType
				ReportArgs.Generation = reply.Generation

				ok = call("Coordinator.ReportTask", &ReportArgs, &ReportReply)
			}
		case ReduceTask:
			var intermediate []KeyValue
			reduceFailed := false

			for i := 0; i < reply.NMap; i++ {
				filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.TaskID)

 				file, err := os.Open(filename)
				if err != nil {
					//
					log.Printf("Worker %d: open intermediate file %q failed: %v", os.Getpid(), filename, err)
					reduceFailed = true
					continue
				}

				dec := gob.NewDecoder(file)

				for {
					var kv KeyValue
					err := dec.Decode(&kv)
					if err == io.EOF {
						break
					}
					if err != nil {
						reduceFailed = true
						break
					}
					intermediate = append(intermediate, kv)
				}
				err = file.Close()
				if err != nil {
					reduceFailed = true
				}
			}

			if !reduceFailed {
				sort.Slice(intermediate, func(i, j int) bool {
					return intermediate[i].Key < intermediate[j].Key
				})

				oname := "mr-out-" + strconv.Itoa(reply.TaskID)
				ofile, err := os.Create(oname)
				if err != nil {
					reduceFailed = true
				} else {
					i := 0
					for i < len(intermediate) {
						j := i + 1
						for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
							j++
						}
						values := []string{}
						for k := i; k < j; k++ {
							values = append(values, intermediate[k].Value)
						}
						output := reducef(intermediate[i].Key, values)
						_, err := fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
						if err != nil {
							reduceFailed = true
							break
						}

						i = j
					}
					err = ofile.Close()
					if err != nil {
						reduceFailed = true
					}
				}
 			}

			if !reduceFailed {
				var ReportArgs ReportTaskArgs
				var ReportReply ReportTaskReply
				ReportArgs.TaskID = reply.TaskID
				ReportArgs.TaskType = reply.TaskType
				ReportArgs.Generation = reply.Generation

				ok = call("Coordinator.ReportTask", &ReportArgs, &ReportReply)
			}
		case WaitTask:
			time.Sleep(time.Second)
			continue
		case ExitTask:
			return
		}
	}
	// Your worker implementation here.

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
		// log.Fatal("dialing:", err)
		log.Printf("dialing failed: %v", err)
		return false
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
