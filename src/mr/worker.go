package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		needWordReply := NeedWorkReply{}
		ok := call("Coordinator.NeedWork", &NeedWorkArgs{}, &needWordReply)
		if !ok {
			// Coordinator finish its work
			break
		}
		if needWordReply.T.Type == Map {
			filename := needWordReply.T.Filename
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			intermediate := make([][]KeyValue, needWordReply.ReduceCnt)
			for _, kv := range kva {
				reduceTask := ihash(kv.Key) % needWordReply.ReduceCnt
				intermediate[reduceTask] = append(intermediate[reduceTask], kv)
			}
			for i := 0; i < needWordReply.ReduceCnt; i++ {
				ofilename := fmt.Sprintf("mr-%d-%d", needWordReply.T.TaskId, i)
				// ofile, _ := os.Create(ofilename)
				tf, _ := os.CreateTemp("./", ofilename)
				enc := json.NewEncoder(tf)
				for _, kv := range intermediate[i] {
					enc.Encode(&kv)
				}
				tf.Close()
				os.Rename(tf.Name(), ofilename)
			}
		} else if needWordReply.T.Type == Reduce {
			// find all files corresponding to this reduce task
			var filenames []string
			files, err := os.ReadDir(".")
			if err != nil {
				log.Fatalf("cannot read current directory")
			}
			for _, file := range files {
				if file.IsDir() {
					continue
				}
				filename := file.Name()
				prefix := "mr-"
				suffix := fmt.Sprintf("-%d", needWordReply.T.ReduceId)
				if strings.HasPrefix(filename, prefix) && strings.HasSuffix(filename, suffix) {
					filenames = append(filenames, filename)
				}
			}

			// do reduce job
			var kva []KeyValue
			for _, filename := range filenames {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}

			// copy from mrsequential.go
			sort.Sort(ByKey(kva))
			oname := fmt.Sprintf("mr-out-%d", needWordReply.T.ReduceId)
			ofile, _ := os.Create(oname)
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
				i = j
			}
			ofile.Close()
		} else {
			// unknown task type
			log.Fatalf("unknown task type: %v", needWordReply.T.Type)
		}

		// make FinishWork RPC call
		call("Coordinator.FinishWork", &FinishWorkArgs{TaskId: needWordReply.T.TaskId}, &FinishWorkReply{})
	}
	// log.Printf("Worker: %v exit", os.Getpid())
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
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
