package mr

import (
	"log"
	"math"
	"math/big"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	crand "crypto/rand"
)

type workerStatus struct {
	startTime  time.Time
	workerType int
	name       string
}

type Coordinator struct {
	// Your definitions here.
	workers      map[int]workerStatus
	workers_lock sync.Mutex

	inputfiles      map[string]status
	inputfiles_lock sync.Mutex

	imm      map[string][]string
	imm_lock sync.Mutex

	reduces      map[string]status
	reduces_lock sync.Mutex
	nReduce      int

	global sync.Mutex
}

type status int

const (
	Todo  = 0
	Doing = 1
)

// need additional lock
func (c *Coordinator) gen_wid() (wid int) {
	for {
		rr, _ := crand.Int(crand.Reader, big.NewInt(math.MaxInt))
		wid = int(rr.Int64())

		_, repeat := c.workers[wid]
		if !repeat {
			break
		}
	}
	return
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Handle(args *Args, reply *Reply) error {
	if c.Done() {
		reply.TaskType = Exit
		return nil
	}

	global_lock_avai := c.global.TryLock()
	if !global_lock_avai {
		reply.TaskType = Fail
		return nil
	}
	c.global.Unlock()

	reply.NReduce = c.nReduce

	c.workers_lock.Lock()
	defer c.workers_lock.Unlock()
	_, ok := c.workers[args.Wid]
	if args.Wid == -1 || !ok {
		reply.Wid = c.gen_wid()
		args.RequestType = Request
		c.workers[reply.Wid] = workerStatus{}
	} else {
		reply.Wid = args.Wid
	}

	if args.RequestType == Fail {
		_, ok := c.workers[args.Wid]
		if !ok {
			return nil
		}
		delete(c.workers, args.Wid)
	}

	if args.RequestType == MapFinish {

		c.inputfiles_lock.Lock()
		delete(c.inputfiles, c.workers[args.Wid].name)
		c.inputfiles_lock.Unlock()

		c.imm_lock.Lock()
		defer c.imm_lock.Unlock()
		for i, val := range strings.Split(args.MapResult, ":") {
			if val != "" {
				c.imm[strconv.Itoa(i)] = append(c.imm[strconv.Itoa(i)], val)
			}
		}
	}

	if args.RequestType == ReduceFinish {
		c.reduces_lock.Lock()
		defer c.reduces_lock.Unlock()
		delete(c.reduces, c.workers[args.Wid].name)
	}

	c.inputfiles_lock.Lock()
	defer c.inputfiles_lock.Unlock()
	if len(c.inputfiles) > 0 {
		for k, v := range c.inputfiles {
			if v == Todo {
				reply.TaskType = IsMap
				reply.Task = k
				c.inputfiles[k] = Doing
				c.workers[reply.Wid] = workerStatus{time.Now(), IsMap, k}
				return nil
			}
		}
	} else {
		if args.RequestType != ReduceFinish {
			c.reduces_lock.Lock()
			defer c.reduces_lock.Unlock()
		}
		for k, v := range c.reduces {
			if v == Todo {
				var err error
				reply.Wid, err = strconv.Atoi(k)
				if err != nil {
					reply.TaskType = Fail
					return err
				}
				reply.TaskType = IsReduce
				reply.Task = strings.Join(c.imm[k], ":")
				c.reduces[k] = Doing
				c.workers[reply.Wid] = workerStatus{time.Now(), IsReduce, k}
				return nil
			}
		}
	}

	reply.TaskType = Wait
	reply.Task = ""
	delete(c.workers, args.Wid)
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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
	// for i := 0; i < runtime.NumCPU(); i++ {
	go http.Serve(l, nil)
	// }
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	c.reduces_lock.Lock()
	if len(c.reduces) == 0 {
		ret = true
	}
	c.reduces_lock.Unlock()

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.workers = make(map[int]workerStatus)
	c.inputfiles = make(map[string]status)
	c.imm = make(map[string][]string)
	c.reduces = make(map[string]status)

	// Your code here.
	for _, file := range files {
		c.inputfiles[file] = Todo
	}

	c.nReduce = nReduce
	for i := 0; i < nReduce; i++ {
		c.reduces[strconv.Itoa(i)] = Todo
		c.workers[i] = workerStatus{}
	}

	go func() {
		for !c.Done() {
			c.workers_lock.Lock()
			c.global.Lock()
			now := time.Now()
			for k, v := range c.workers {
				if v.startTime.IsZero() {
					continue
				}
				if now.Sub(v.startTime) > 10*time.Second {
					if v.workerType == IsMap {
						c.inputfiles_lock.Lock()
						_, ok := c.inputfiles[v.name]
						if ok {
							c.inputfiles[v.name] = Todo
						}
						c.inputfiles_lock.Unlock()
					} else {
						c.reduces_lock.Lock()
						_, ok := c.reduces[v.name]
						if ok {
							c.reduces[v.name] = Todo
						}
						c.reduces_lock.Unlock()
					}
					delete(c.workers, k)
				}
			}
			c.global.Unlock()
			c.workers_lock.Unlock()
			time.Sleep(1 * time.Second)
		}
	}()

	c.server()
	return &c
}
