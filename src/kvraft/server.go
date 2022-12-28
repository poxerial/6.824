package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

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
	commitHandleSleepMs  = 10
	electionMs           = 50
	OpTimeOutMs          = 100
	unsuccessfulMaxTimes = 3
)

const (
	OpEmpty = iota
	OpGet
	OpPut
	OpAppend
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType    int
	Key       string
	Value     string
	ClerkID   int
	CommandID int
	Term      int
}

type KVServer struct {
	mu      raft.DLock
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	// Your definitions here.

	// initialized with sync.Mutex
	// used to synchronize request from Clerk and message from applyCh
	cond *sync.Cond

	// term of most recent message from rf
	term      int
	requestOp Op

	// if false, requestResult is error message instead of result
	requestSuccess bool
	requestResult  string
	requestCh      chan request
	requestApplyCh chan bool

	// database
	db map[string]string
	// trace the increment of CommandID of different Clerks
	clerkMCommandID map[int]int
}

type request struct {
	ch chan result
	op Op
}

type result struct {
	val     string
	success bool
}

func (kv *KVServer) handleRequest() {
	raft.DLog(raft.ServerInfo, kv.me, "handleRequest, GoID", raft.GoID())

	kill := make(chan bool)
	go func() {
		for !kv.killed() {
			time.Sleep(raft.HeartBeatMs * time.Millisecond)
		}
		kill <- true
	}()

	for {
		select {
		case req := <-kv.requestCh:
			term, isLeader := kv.rf.GetState()

			kv.cond.L.Lock()
			if !isLeader || term != kv.term || req.op.Term != term {
				kv.cond.L.Unlock()
				result := result{
					val:     ErrWrongLeader,
					success: false,
				}
				req.ch <- result
				continue
			} else if req.op.CommandID == -1 {
				kv.cond.L.Unlock()
				result := result{
					val:     ErrReGet,
					success: false,
				}
				req.ch <- result
				continue
			}

			kv.requestOp = req.op
			raft.DLog(raft.ServerInfo, kv.me, "push", req.op)

			kv.rf.Start(req.op)

			// wait for apply
			kv.cond.Wait()

			kv.requestOp = Op{}
			raft.DLog(raft.ServerInfo, kv.me, "pull", req.op)

			kv.cond.L.Unlock()

			result := result{
				val:     kv.requestResult,
				success: kv.requestSuccess,
			}
			req.ch <- result
		case <-kill:
			return
		}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	raft.DLog(raft.ServerInfo, kv.me, "GoID:", raft.GoID(), "receives Get", args)
	defer raft.DLog(raft.ServerInfo, kv.me, "finishes Get", args, "and reply", reply)

	term, isLeader := kv.rf.GetState()
	reply.Term = term
	if !isLeader {
		reply.Err = Err(ErrWrongLeader)
		return
	}

	op := Op{}
	op.OpType = OpGet
	op.Key = args.Key
	op.CommandID = args.CommandID
	op.ClerkID = args.ClerkID
	op.Term = term

	resultCh := make(chan result)
	request := request{
		op: op,
		ch: resultCh,
	}
	kv.requestCh <- request

	result := <-resultCh

	if !result.success {
		reply.Err = Err(result.val)
		return
	}
	reply.Err = OK
	reply.Value = result.val
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	raft.DLog(raft.ServerInfo, kv.me, "GoId:", raft.GoID(), "receives PutAppend", args)
	defer raft.DLog(raft.ServerInfo, kv.me, "reply PutAppend", args, "with", reply)

	term, isLeader := kv.rf.GetState()
	reply.Term = term
	if !isLeader {
		reply.Err = Err(ErrWrongLeader)
		return
	}

	op := Op{}
	if args.Op == "Put" {
		op.OpType = OpPut
	} else {
		op.OpType = OpAppend
	}
	op.Key = args.Key
	op.Value = args.Value
	op.ClerkID = args.ClerkID
	op.CommandID = args.CommandID
	op.Term = term

	resultCh := make(chan result)
	request := request{
		op: op,
		ch: resultCh,
	}
	kv.requestCh <- request

	result := <-resultCh

	if !result.success {
		reply.Err = Err(result.val)
	} else {
		reply.Err = OK
	}
}

func (kv *KVServer) apply(op Op) (success bool, result string) {

	defer func() {
		if success == false && len(result) == 0 {
			panic("")
		}
	}()

	commandID, ok := kv.clerkMCommandID[op.ClerkID]
	if !ok || op.CommandID > commandID {
		raft.Assert(commandID+1 == op.CommandID || !ok, "command out of order clerkID:", op.ClerkID,
			"last commandID", commandID, "new commandID", op.CommandID)
		kv.clerkMCommandID[op.ClerkID] = op.CommandID
	} else {
		if op.OpType == OpGet {
			// avoid reading stale data
			success = false
			result = ErrReGet
			raft.DLog(raft.ServerInfo, kv.me, ErrReGet, "op:", op, "clerkID:", op.ClerkID,
				"last commandID", commandID)
		} else {
			success = true
		}
		return
	}

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
			result = ErrNoKey
			success = false
			return
		} else {
			result = val
		}
	}
	success = true
	return
}

func (kv *KVServer) checkSnapshot(lastCommandIndex int) {
	if kv.persister.RaftStateSize() >= kv.maxraftstate && kv.maxraftstate != -1 {
		writer := new(bytes.Buffer)
		encoder := labgob.NewEncoder(writer)

		err1 := encoder.Encode(kv.db)
		err2 := encoder.Encode(kv.clerkMCommandID)

		raft.Assert(err1 == nil && err2 == nil, "Failed to encode snapshot:", err1, err2)

		data := writer.Bytes()
		kv.rf.Snapshot(lastCommandIndex, data)
	}
}

func (kv *KVServer) readSnapshot(data []byte) {
	writer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(writer)

	//db := make(map[string]string)
	//clerkMCommandID := make(map[int]int)

	err1 := decoder.Decode(&kv.db)
	err2 := decoder.Decode(&kv.clerkMCommandID)
	raft.Assert(err1 == nil && err2 == nil, "Failed to decode snapshot:", err1, err2)

}

func (kv *KVServer) handleLeaderTransit() {
	raft.DLog(raft.ServerInfo, kv.me, "handleLeaderTransit", raft.GoID())

	for !kv.killed() {
		term, isLeader := kv.rf.GetState()
		kv.cond.L.Lock()
		termChanged := term != kv.term
		if !isLeader || termChanged {
			if kv.requestOp.OpType != OpEmpty && kv.requestOp.Term != term {
				if kv.requestOp.OpType != OpEmpty {
					raft.DLog(raft.ServerInfo, kv.me, "kill op:", kv.requestOp)
					kv.requestSuccess = false
					kv.requestResult = ErrWrongLeader
					kv.cond.Signal()
				}
			}
			if termChanged {
				kv.term = term
			}
		}
		kv.cond.L.Unlock()
		time.Sleep(raft.HeartBeatMs / 2 * time.Millisecond)
	}
}

func (kv *KVServer) handleApply() {
	raft.DLog(raft.ServerInfo, kv.me, "handleApply()", raft.GoID())

	kill := make(chan bool)
	go func() {
		for !kv.killed() {
			time.Sleep(time.Millisecond * 100)
		}
		kill <- true
	}()

	for {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				kv.applyCommandMessage(msg)
			} else if msg.SnapshotValid {
				kv.applyInstallSnapshot(msg)
			}
		case <-kill:
			return
		}
	}
}

func (kv *KVServer) applyCommandMessage(msg raft.ApplyMsg) {
	raft.Assert(msg.CommandValid)

	committed, ok := msg.Command.(Op)
	raft.Assert(ok, "command field of msg in applyCh must be type Op.")
	success, result := kv.apply(committed)

	kv.checkSnapshot(msg.CommandIndex)

	var resultLog string
	if len(result) > 20 {
		resultLog = result[len(result)-20 : len(result)]
	} else {
		resultLog = result
	}
	raft.DLog(raft.ServerInfo, kv.me, "apply", committed, "index:", msg.CommandIndex,
		"result:", resultLog)

	kv.cond.L.Lock()
	empty := kv.requestOp.OpType == OpEmpty
	if empty {
		kv.cond.L.Unlock()
		return
	} else if kv.requestOp == committed {
		kv.requestResult = result
		kv.requestSuccess = success
		kv.cond.Signal()
	} else {
		term, isLeader := kv.rf.GetState()
		raft.Assert(!isLeader || term != committed.Term,
			"requestOp", kv.requestOp, "mismatch with committed", committed)
		if !isLeader {
			kv.requestSuccess = false
			kv.requestResult = ErrWrongLeader
			kv.cond.Signal()
		}
	}
	kv.cond.L.Unlock()
}

func (kv *KVServer) applyInstallSnapshot(msg raft.ApplyMsg) {
	raft.Assert(msg.SnapshotValid)

	kv.readSnapshot(msg.Snapshot)
	raft.DLog(raft.ServerInfo, kv.me, "apply snapshot at index", msg.SnapshotIndex)
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
	kv.persister = persister

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	mu := raft.DLock{}
	kv.cond = sync.NewCond(&mu)
	kv.db = make(map[string]string)
	kv.clerkMCommandID = make(map[int]int)

	snapshot := persister.ReadSnapshot()
	if len(snapshot) != 0 {
		kv.readSnapshot(snapshot)
	}

	kv.requestCh = make(chan request)
	kv.requestApplyCh = make(chan bool)

	go kv.handleApply()
	go kv.handleLeaderTransit()
	go kv.handleRequest()

	return kv
}
