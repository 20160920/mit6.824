package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)
import "log"
import "hash/fnv"
import "net/rpc"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (b ByKey) Len() int           { return len(b) }
func (b ByKey) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b ByKey) Less(i, j int) bool { return b[i].Key < b[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := Args{}
		reply := Reply{}
		call("Coordinator.HandleTask", &args, &reply)

		switch reply.TaskType {
		case Map:
			performMap(reply.MapFile, reply.TaskNum, reply.ReduceTasks, mapf)
		case Reduce:
			performReduce(reply.TaskNum, reply.MapTasks, reducef)
		case Done:
			os.Exit(0)
		default:
			fmt.Errorf("bad task type: %s", reply.TaskType)
		}

	}
	// uncomment to send the Example RPC to the coordinator.
	CallExample()

}

func performMap(filename string, taskNum int, reduceTaskNum int, mapf func(string, string) []KeyValue) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open  %s", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot red %s", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	tmpFiles, tmpFilenames := []*os.File{}, []string{}
	encoders := []*json.Encoder{}

	for r := 0; r < reduceTaskNum; r++ {
		tmpFile, err := ioutil.TempFile("", "")
		if err != nil {
			log.Fatalf("connot open tmpFile")
		}
		tmpFiles = append(tmpFiles, tmpFile)
		tmpFilename := tmpFile.Name()
		tmpFilenames = append(tmpFilenames, tmpFilename)
		enc := json.NewEncoder(tmpFile)
		encoders = append(encoders, enc)
	}

	for _, kv := range kva {
		r := ihash(kv.Key) % reduceTaskNum
		encoders[r].Encode(&kv)
	}

	for _, f := range tmpFiles {
		f.Close()
	}

	for r := 0; r < reduceTaskNum; r++ {
		finalizeIntermediateFile(tmpFilenames[r], taskNum, r)
	}
}

func getIntermediateFile(mapTaskNum int, redTaskNum int) string {
	return fmt.Sprintf("mr-%d-%d", mapTaskNum, redTaskNum)
}

func finalizeIntermediateFile(tmpFile string, mapTaskNum int, redTaskNum int) {
	finalFile := getIntermediateFile(mapTaskNum, redTaskNum)
	os.Rename(tmpFile, finalFile)
}

func performReduce(taskNum int, mapTaskNum int, reducef func(string, []string) string) {
	kva := []KeyValue{}
	for i := 0; i < taskNum; i++ {
		filename := getIntermediateFile(i, taskNum)
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
		file.Close()
	}
	sort.Sort(ByKey(kva))

	tmpFile, err := ioutil.TempFile("", "")
	if err != nil {
		log.Fatalf("cannot open tmpfile")
	}
	tmpFilename := tmpFile.Name()

	keyBegin := 0
	for keyBegin < len(kva) {
		keyEnd := keyBegin + 1
		for keyEnd < len(kva) && kva[keyEnd].Key == kva[keyBegin].Key {
			keyEnd++
		}
		values := []string{}
		for k := keyBegin; k < keyEnd; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[keyBegin].Key, values)

		fmt.Fprintf(tmpFile, "%v %v\n", kva[keyBegin].Key, output)

		keyBegin = keyEnd
	}
	finalizeReduceFile(tmpFilename, taskNum)
}

func finalizeReduceFile(tmpFilename string, taskNum int) {
	finalFile := fmt.Sprintf("mr-out-%d", taskNum)
	os.Rename(tmpFilename, finalFile)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func requestTask() (Reply, bool) {
	args := Args{}
	reply := Reply{}
	ok := call("Coordinator.DispatchTask", &args, &reply)
	return reply, ok
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
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
