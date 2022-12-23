package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

type Task struct {
	Type     int
	Filename string
	TaskId   int
	ReduceId int
}

const (
	Map = iota
	Reduce
)

func NewMapTask(filename string, taskId int) Task {
	return Task{
		Type:     Map,
		Filename: filename,
		TaskId:   taskId,
	}
}

func NewReduceTask(reduceId, taskId int) Task {
	return Task{
		Type:     Reduce,
		TaskId:   taskId,
		ReduceId: reduceId,
	}
}

type NeedWorkArgs struct {
}

type NeedWorkReply struct {
	T         Task
	ReduceCnt int
}

type FinishWorkArgs struct {
	TaskId int
}

type FinishWorkReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
