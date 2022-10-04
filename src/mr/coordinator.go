package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var mu sync.Mutex

type Task struct {
	TaskType   TaskType
	InputFile  []string
	TaskId     int
	ReducerNum int
}

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex

	cond *sync.Cond

	mapFiles    []string
	mapTasks    int
	reduceTasks int

	mapTaskFinished    []bool
	mapTaskIssued      []time.Time
	reduceTaskFinished []bool
	reduceTaskIssued   []time.Time

	isDone bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) HandleTask(args *Task, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.ReduceTasks = c.reduceTasks
	reply.MapTasks = c.mapTasks

	for {
		mapDone := true
		for m, done := range c.mapTaskFinished {
			if !done {
				if c.mapTaskIssued[m].IsZero() || time.Since(c.mapTaskIssued[m]).Seconds() > 10 {
					reply.TaskType = Map
					reply.TaskNum = m
					reply.MapFile = c.mapFiles[m]
					c.mapTaskIssued[m] = time.Now()
				} else {
					mapDone = false
				}
			}
		}
		if !mapDone {
			c.cond.Wait()
		} else {
			break
		}
	}

	for {
		redDone := true
		for r, done := range c.reduceTaskFinished {
			if !done {
				if c.reduceTaskIssued[r].IsZero() || time.Since(c.reduceTaskIssued[r]).Seconds() > 10 {
					reply.TaskType = Reduce
					reply.TaskNum = r
					c.reduceTaskIssued[r] = time.Now()
				} else {
					redDone = false
				}
			}
		}
		if !redDone {

		} else {
			break
		}
	}
	reply.TaskType = Done
	c.isDone = true
	return nil
}

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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.isDone
}

//
// create a Coordinator
// main/mrcoordinator.go calls this function
// nReduce is the number of reduce tasks to use
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.cond = sync.NewCond(&c.mu)

	c.mapFiles = files
	c.mapTasks = len(files)
	c.mapTaskFinished = make([]bool, len(files))
	c.mapTaskIssued = make([]time.Time, nReduce)

	c.reduceTasks = nReduce
	c.reduceTaskFinished = make([]bool, nReduce)
	c.reduceTaskIssued = make([]time.Time, nReduce)

	go func() {
		for {
			c.mu.Lock()
			c.cond.Broadcast()
			c.mu.Unlock()
			time.Sleep(time.Second)
		}
	}()
	c.server()
	return &c
}

func (c *Coordinator) HandleFinishedTask(args *FinishedTaskArgs, reply *FinishedTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.TaskType {
	case Map:
		c.mapTaskFinished[args.TaskNum] = true
	case Reduce:
		c.reduceTaskFinished[args.TaskNum] = true
	default:
		log.Fatalf("bad finished task: %d", args.TaskType)
	}
	c.cond.Broadcast()
	return nil
}
