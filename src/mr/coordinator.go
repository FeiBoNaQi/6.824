package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	state            []int8
	location         []string
	startTime        []time.Time
	NReduce          int
	NMap             int
	coordinatorState int8
	mutex            sync.Mutex
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

func (c *Coordinator) Communicate(args *CommunicateArgs, reply *CommunicateReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// receive information from the workers
	if args.TaskNumber != -1 {
		c.state[args.TaskNumber] = completed
		c.location[args.TaskNumber] = args.Location
	}
	// iterate over every input file  === iterate over every map task
	taskCompleted := 0
	timeOutTask := -1
	if c.coordinatorState == mapTask {
		for i := 0; i < len(c.state)-c.NReduce; i++ {
			if c.state[i] == idle {
				reply.TaskNumber = i
				reply.Location = os.Args[1+i]
				reply.Task = mapTask
				reply.NReduce = c.NReduce
				c.startTime[i] = time.Now()
				c.state[i] = inProgress
				return nil
			} else if c.state[i] == completed {
				taskCompleted++
			} else if c.state[i] == inProgress {
				elapsed := time.Since(c.startTime[i])
				if elapsed > timeSlot*000000000 {
					c.state[i] = timeout
					timeOutTask = i
				}
			} else if c.state[i] == timeout {
				timeOutTask = i
			}
		}
		// if no task is idle, run the timeout task
		if timeOutTask != -1 {
			reply.TaskNumber = timeOutTask
			reply.Location = os.Args[1+timeOutTask]
			reply.Task = mapTask
			reply.NReduce = c.NReduce
			c.startTime[timeOutTask] = time.Now()
			c.state[timeOutTask] = inProgress
			return nil
		}
		// if no task timeout and still some task is running, reply with idle task
		if taskCompleted != len(c.state)-c.NReduce {
			reply.Task = idleTask
			return nil
		}
		// if every map task is completed, coordinator goes into reduce state
		c.coordinatorState = reduceTask
	}
	// coordinator in reduce state
	if c.coordinatorState == reduceTask {
		taskCompleted = 0
		for i := len(c.state) - c.NReduce; i < len(c.state); i++ {
			if c.state[i] == idle {
				reply.TaskNumber = i
				reply.Location = fmt.Sprintf("%v", i-len(c.state)+c.NReduce)
				reply.Task = reduceTask
				reply.NReduce = c.NReduce
				reply.NMap = c.NMap
				c.startTime[i] = time.Now()
				c.state[i] = inProgress
				return nil
			} else if c.state[i] == completed {
				taskCompleted++
			} else if c.state[i] == inProgress {
				elapsed := time.Since(c.startTime[i])
				if elapsed > timeSlot*000000000 {
					c.state[i] = timeout
					timeOutTask = i
				}
			} else if c.state[i] == timeout {
				timeOutTask = i
			}
		}
		// if no task is idle, run the timeout task
		if timeOutTask != -1 {
			reply.TaskNumber = timeOutTask
			reply.Location = fmt.Sprintf("%v", timeOutTask-len(c.state)+c.NReduce)
			reply.Task = reduceTask
			reply.NReduce = c.NReduce
			reply.NMap = c.NMap
			c.startTime[timeOutTask] = time.Now()
			c.state[timeOutTask] = inProgress
			return nil
		}
		// if no task timeout and still some task is running, reply with idle task
		if taskCompleted != c.NReduce {
			reply.Task = idleTask
			return nil
		}
		c.coordinatorState = exitTask

	}
	if c.coordinatorState == exitTask {
		reply.Task = exitTask
	}
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
	ret := false

	// Your code here.
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.coordinatorState == exitTask {
		ret = true
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.NReduce = nReduce
	c.NMap = len(os.Args) - 1
	c.state = make([]int8, nReduce+len(os.Args)-1)
	c.location = make([]string, nReduce+len(os.Args)-1)
	c.startTime = make([]time.Time, nReduce+len(os.Args)-1)
	c.coordinatorState = mapTask
	c.server()
	return &c
}

const (
	// const represent task states
	idle       = 0
	inProgress = 1
	completed  = 2
	timeout    = 3

	// timeout period seconds
	timeSlot = 10

	// map and reduce function definitions
	idleTask   = 0
	mapTask    = 1
	reduceTask = 2
	exitTask   = 3
)
