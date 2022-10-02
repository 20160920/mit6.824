package mr

import (
	"fmt"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var mu sync.Mutex

type Task struct {
	TaskType   TaskType
	InputFile  []string
	TaskId     int
	ReducerNum int
}

type Coordinator struct {
	// Your definitions here.
	UID     int
	Map     chan *Task
	Reducer chan *Task

	ReducerNum int
	MapNum     int

	Status Condition
	isDone bool
}

func (c *Coordinator) generateTaskId() int {
	res := c.UID
	c.UID++
	return res
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	fmt.Println("Done!!!!!!")
	return c.Status == AllDone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		UID:        0,
		Map:        make(chan *Task, len(files)),
		Reducer:    make(chan *Task, nReduce),
		ReducerNum: nReduce,
		MapNum:     len(files),
	}

	c.makeMapJobs(files)

	c.server()

	return &c
}

func (c *Coordinator) makeMapJobs(files []string) {
	for _, v := range files {
		id := c.generateTaskId()
		task := Task{
			TaskId:     id,
			TaskType:   Map,
			InputFile:  []string{v},
			ReducerNum: c.ReducerNum,
		}
		c.Map <- &task
	}
	fmt.Println("making map tasks completed")
}
