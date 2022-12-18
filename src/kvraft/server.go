package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	commitHandleSleepMs = 10
	sendRPCSleepMs      = 50
)

const (
	OpGet = iota
	OpPut
	OpAppend
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType int
	Key    string
	Value  string
}

type KVServer struct {
	mu      raft.DLock
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	// initialized with sync.Mutex
	// also used to ensure the sequential consistency
	// between requestQueue and applyCh
	cond *sync.Cond
	// condition variable
	requestOp *Op
	// if false, requestResult is error message instead of result
	requestSuccess bool
	requestResult  string

	requestQueue      []*Op
	db                map[string]string
	clerk_m_commandID map[int]int
}

func (kv *KVServer) startOp(op Op) (requestSuccess bool, requestResult string) {
	_, _, isLeader := kv.rf.Start(op)
	kv.cond.L.Lock()
	if !isLeader {
		kv.cond.L.Unlock()
		requestSuccess = false
		requestResult = ErrWrongLeader
		return
	}
	kv.requestQueue = append(kv.requestQueue, &op)
	for kv.requestOp != &op {
		raft.DLog(raft.CondInfo, kv.me, unsafe.Pointer(&op), "start waiting.")
		kv.cond.Wait()
		raft.DLog(raft.CondInfo, kv.me, unsafe.Pointer(&op), "receive broadcast with &op", unsafe.Pointer(kv.requestOp))
	}
	raft.DLog(raft.CondInfo, kv.me, unsafe.Pointer(&op), "awakes.")
	requestSuccess = kv.requestSuccess
	requestResult = kv.requestResult
	kv.cond.L.Unlock()
	return
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	raft.DLog(raft.ServerInfo, kv.me, "receives Get", args)
	defer raft.DLog(raft.ServerInfo, kv.me, "finishes Get", args, "and reply", reply)

	op := Op{}
	op.OpType = OpGet
	op.Key = args.Key

	requestSuccess, requestResult := kv.startOp(op)

	if !requestSuccess {
		reply.Err = requestResult
		return
	}
	reply.Err = OK
	reply.Value = requestResult
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	raft.DLog(raft.ServerInfo, kv.me, "receives PutAppend", args)
	defer raft.DLog(raft.ServerInfo, kv.me, "reply PutAppend", reply)

	op := Op{}
	if args.Op == "Put" {
		op.OpType = OpPut
	} else {
		op.OpType = OpAppend
	}
	op.Key = args.Key
	op.Value = args.Value

	requestSuccess, requestResult := kv.startOp(op)

	if !requestSuccess {
		reply.Err = requestResult
		return
	}
	reply.Err = OK
}

func (kv *KVServer) apply(op Op) (success bool, result string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if op.OpType == OpAppend {
		_, ok := kv.db[op.Key]
		if !ok {
			kv.db[op.Key] = ""
		}
		kv.db[op.Key] = kv.db[op.Key] + op.Value
	} else if op.OpType == OpPut {
		kv.db[op.Key] = op.Value
	} else {
		raft.Assert(op.OpType == OpGet, "op.OpType must be OpPut OpAppend OpGet.")
		val, ok := kv.db[op.Key]
		if !ok {
			result = string(ErrNoKey)
			return
		}
		result = val
	}
	success = true
	return
}

func (kv *KVServer) handleCommit() {
	kill := make(chan bool)
	go func() {
		for !kv.killed() {
			time.Sleep(time.Millisecond * 100)
		}
		kill <- true
	} ()
	for {
		select {
		case msg := <-kv.applyCh:
			go func() {
			committed, ok := msg.Command.(Op)
			raft.Assert(ok, "command field of msg in applyCh must be type Op.")
			success, result := kv.apply(committed)

			kv.cond.L.Lock()
			var op *Op
			empty := len(kv.requestQueue) == 0
			if !empty {
				op = kv.requestQueue[0]
			}
			if empty {
				kv.cond.L.Unlock()
				return 
			} else if *op != committed {
				for i := 1; i < len(kv.requestQueue); i++ {
					if kv.requestQueue[i] == op {
						for j := 0; j < i; j++ {
							kv.requestSuccess = false
							kv.requestResult = ErrApplyFailed
							kv.requestOp = kv.requestQueue[j]
							kv.cond.Broadcast()
						}
						kv.requestQueue = kv.requestQueue[i+1:]
						kv.requestSuccess = success
						kv.requestResult = result
						kv.requestOp = op
						raft.DLog(raft.CondInfo, kv.me, "send broadcast", unsafe.Pointer(kv.requestOp))
						kv.cond.Broadcast()
					}
				}
			} else {
				kv.requestQueue = kv.requestQueue[1:]
				kv.requestSuccess = success
				kv.requestResult = result
				kv.requestOp = op
				raft.DLog(raft.CondInfo, kv.me, "send broadcast", unsafe.Pointer(kv.requestOp))
				kv.cond.Broadcast()
			}
			kv.cond.L.Unlock()
		} ()
		case <-kill:
			return
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	mu := sync.Mutex{}
	kv.cond = sync.NewCond(&mu)
	kv.db = make(map[string]string)

	go kv.handleCommit()

	return kv
}
