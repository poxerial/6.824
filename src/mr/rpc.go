package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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

const (
	Request      = 1
	MapFinish    = 2
	ReduceFinish = 3
	Fail         = 4
	IsMap        = 5
	IsReduce     = 6
	Exit         = 7
	Wait         = 8
)

// Add your RPC definitions here.
type Args struct {
	Wid         int
	RequestType int
	MapResult   string
}

type Reply struct {
	Wid      int
	TaskType int
	NReduce  int
	Task     string
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
