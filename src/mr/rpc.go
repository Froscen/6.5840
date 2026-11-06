package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type GetTaskArgs struct {}

type GetTaskReply struct {
	TaskID int
	TaskType TaskType
	Filename string
	NReduce int
	NMap int
	Generation int
}

type ReportTaskReply struct {}

type ReportTaskArgs struct {
	TaskID int
	TaskType TaskType
	Generation int
}