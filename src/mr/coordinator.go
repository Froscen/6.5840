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

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
)

type Task struct {
	TaskID     int
	TaskType   TaskType
	State      TaskState
	StartTime  time.Time
	Filename   string
	Generation int
}

type Coordinator struct {
	// Your definitions here.
	mu          sync.Mutex
	files       []string
	nReduce     int
	mapTasks    []Task
	reduceTasks []Task
}

// Your code here -- RPC handlers for the worker to call.

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
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()

	ret = c.allReduceTaskCompleted()
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nReduce = nReduce

	// Your code here.
	for i, filename := range files {
		task := Task{
			TaskID:   i,
			TaskType: MapTask,
			State:    Idle,
			Filename: filename,
		}
		c.mapTasks = append(c.mapTasks, task)
	}
	for i := 0; i < nReduce; i++ {
		task := Task{
			TaskID:   i,
			TaskType: ReduceTask,
			State:    Idle,
		}
		c.reduceTasks = append(c.reduceTasks, task)
	}

	c.server(sockname)
	return &c
}

func (c *Coordinator) allMapTaskCompleted() bool {
	for _, task := range c.mapTasks {
		if task.State != Completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) allReduceTaskCompleted() bool {
	for _, task := range c.reduceTasks {
		if task.State != Completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.TaskType = WaitTask
	if !c.allMapTaskCompleted() {
		found := false
		for i, task := range c.mapTasks {
			if task.State == InProgress && time.Since(task.StartTime) > 10*time.Second {
				c.mapTasks[i].State = Idle
			}
			if c.mapTasks[i].State == Idle {
				c.mapTasks[i].Generation++
				reply.Generation = c.mapTasks[i].Generation
				reply.TaskID = task.TaskID
				reply.TaskType = task.TaskType
				reply.Filename = task.Filename
				reply.NReduce = c.nReduce

				c.mapTasks[i].State = InProgress
				c.mapTasks[i].StartTime = time.Now()
				found = true
				break
			}
		}
		if !found {
			reply.TaskType = WaitTask
		}
	} else {
		found := false
		for i, task := range c.reduceTasks {
			if task.State == InProgress && time.Since(task.StartTime) > 10*time.Second {
				c.reduceTasks[i].State = Idle
			}
			if c.reduceTasks[i].State == Idle {
				c.reduceTasks[i].Generation++
				reply.Generation = c.reduceTasks[i].Generation
				reply.TaskID = task.TaskID
				reply.TaskType = task.TaskType
				reply.NMap = len(c.mapTasks)

				c.reduceTasks[i].State = InProgress
				c.reduceTasks[i].StartTime = time.Now()
				found = true
				break
			}
		}
		if !found {
			if c.allReduceTaskCompleted() {
				reply.TaskType = ExitTask
			} else {
				reply.TaskType = WaitTask
			}
		}
	}
	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.TaskType == MapTask {
		for i := range c.mapTasks {
			if c.mapTasks[i].TaskID == args.TaskID && c.mapTasks[i].Generation == args.Generation && c.mapTasks[i].State == InProgress {
				c.mapTasks[i].State = Completed
				break
			}
		}
	} else if args.TaskType == ReduceTask {
		for i := range c.reduceTasks {
			if c.reduceTasks[i].TaskID == args.TaskID && c.reduceTasks[i].Generation == args.Generation && c.reduceTasks[i].State == InProgress {
				c.reduceTasks[i].State = Completed
				break
			}
		}
	}
	return nil
}
