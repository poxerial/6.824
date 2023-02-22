package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

const (
	OpEmpty = iota
	OpGet
	OpPut
	OpAppend

	OpMigrate
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

	shardID int
	shard   Shard
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	persister       raft.Persister
	sm              *shardctrler.Clerk
	availableShards map[int]int
	migrateHistory  map[int]int
	dead            int32

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

	config shardctrler.Config

	shards map[int]Shard
}

type request struct {
	ch chan result
	op Op
}

type result struct {
	val     string
	success bool
}

type Shard struct {
	// database
	db map[string]string
	// trace the increment of CommandID of different Clerks
	clerkMCommandID map[int]int
}

func (kv *ShardKV) handleRequest() {
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
				res := result{
					val:     ErrWrongLeader,
					success: false,
				}
				req.ch <- res
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

			res := result{
				val:     kv.requestResult,
				success: kv.requestSuccess,
			}
			req.ch <- res
		case <-kill:
			return
		}
	}
}

func (kv *ShardKV) keyAvailable(key string) bool {
	kv.cond.L.Lock()
	defer kv.cond.L.Unlock()
	_, ok := kv.availableShards[key2shard(key)]
	return ok
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	raft.DLog(raft.ServerInfo, kv.me, "GoID:", raft.GoID(), "receives Get", args)
	defer raft.DLog(raft.ServerInfo, kv.me, "finishes Get", args, "and reply", reply)

	term, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = Err(ErrWrongLeader)
		return
	}

	if !kv.keyAvailable(args.Key) {
		reply.Err = ErrWrongGroup
		return
	}

	op := Op{}
	op.OpType = OpGet
	op.Key = args.Key
	op.CommandID = args.CommandID
	op.ClerkID = args.ClerkID
	op.Term = term

	resultCh := make(chan result)
	req := request{
		op: op,
		ch: resultCh,
	}
	kv.requestCh <- req

	res := <-resultCh

	if !res.success {
		reply.Err = Err(res.val)
		return
	}
	reply.Err = OK
	reply.Value = res.val
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	raft.DLog(raft.ServerInfo, kv.me, "GoId:", raft.GoID(), "receives PutAppend", args)
	defer raft.DLog(raft.ServerInfo, kv.me, "reply PutAppend", args, "with", reply)

	term, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = Err(ErrWrongLeader)
		return
	}

	if !kv.keyAvailable(args.Key) {
		reply.Err = ErrWrongGroup
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
	req := request{
		op: op,
		ch: resultCh,
	}
	kv.requestCh <- req

	res := <-resultCh

	if !res.success {
		reply.Err = Err(res.val)
	} else {
		reply.Err = OK
	}
}

func (kv *ShardKV) MigrateShard(args MigrateShardArgs, reply *MigrateShardReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Success = false
		return
	}

	kv.mu.Lock()
	kv.cond.L.Lock()
	configNum, ok := kv.availableShards[args.Shard]
	if !ok || configNum != args.ConfigNum {
		kv.mu.Unlock()
		kv.cond.L.Unlock()
		reply.Success = false
		return
	}
	kv.cond.L.Unlock()

	if !args.SendContent {
		kv.mu.Unlock()
		reply.Success = true
		return
	}

	shard := kv.shards[args.Shard]

	reply.Shard = Shard{
		db:              shard.db,
		clerkMCommandID: shard.clerkMCommandID,
	}

	kv.mu.Unlock()

	reply.Success = true
	return
}

func (kv *ShardKV) apply(op Op) (success bool, result string) {
	if !kv.keyAvailable(op.Key) {
		success = false
		result = ErrWrongGroup
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	shard := key2shard(op.Key)
	db := kv.shards[shard].db

	commandID, ok := kv.shards[shard].clerkMCommandID[op.ClerkID]
	if !ok || op.CommandID > commandID {
		raft.Assert(commandID+1 == op.CommandID || !ok, "command out of order clerkID:", op.ClerkID,
			"last commandID", commandID, "new commandID", op.CommandID)
		kv.shards[shard].clerkMCommandID[op.ClerkID] = op.CommandID
	} else if op.OpType != OpGet {
		success = true
	}

	if op.OpType == OpAppend {
		_, ok := db[op.Key]
		if !ok {
			db[op.Key] = ""
		}
		db[op.Key] = db[op.Key] + op.Value
	} else if op.OpType == OpPut {
		db[op.Key] = op.Value
	} else {
		raft.Assert(op.OpType == OpGet, "op.OpType must be OpPut OpAppend OpGet.")
		val, ok := db[op.Key]
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

func (kv *ShardKV) checkSnapshot(lastCommandIndex int) {
	if kv.persister.RaftStateSize() >= kv.maxraftstate && kv.maxraftstate != -1 {
		writer := new(bytes.Buffer)
		encoder := labgob.NewEncoder(writer)

		err1 := encoder.Encode(kv.shards)
		err2 := encoder.Encode(kv.config)

		raft.Assert(err1 == nil && err2 == nil, "Failed to encode snapshot:", err1, err2)

		data := writer.Bytes()
		kv.rf.Snapshot(lastCommandIndex, data)
	}
}

func (kv *ShardKV) readSnapshot(data []byte) {
	writer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(writer)

	//db := make(map[string]string)
	//clerkMCommandID := make(map[int]int)

	err1 := decoder.Decode(&kv.shards)
	err2 := decoder.Decode(&kv.config)
	raft.Assert(err1 == nil && err2 == nil, "Failed to decode snapshot:", err1, err2)

}

func (kv *ShardKV) handleLeaderTransit() {
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

func (kv *ShardKV) handleConfigTransit() {
	raft.DLog(raft.ServerInfo, kv.me, "handleConfigTransit", raft.GoID())

	for !kv.killed() {
		newConfig := kv.sm.Query(-1)

		kv.mu.Lock()
		kv.cond.L.Lock()
		if newConfig.Num > kv.config.Num {

			if newConfig.Num != kv.config.Num+1 {
				newConfig = kv.sm.Query(kv.config.Num + 1)
			}

			raft.Assert(newConfig.Num == kv.config.Num+1)

			moveInShards := make(map[int]int)
			moveOutShards := make(map[int]int)

			for shard, gid := range newConfig.Shards {
				if gid == kv.gid {
					_, ok := kv.availableShards[shard]
					if !ok {
						moveInShards[shard] = kv.config.Shards[shard]
					}
				}
			}

			for shard := range kv.availableShards {
				gid := newConfig.Shards[shard]
				if gid != kv.gid {
					moveOutShards[shard] = gid
				}
				delete(kv.availableShards, shard)
			}

			kv.mu.Unlock()
			kv.cond.L.Unlock()

		} else {
			kv.mu.Unlock()
			kv.cond.L.Unlock()
		}
	}

}

func (kv *ShardKV) moveInAll(shards map[int]int, newConfig *shardctrler.Config) {
	for shard, gid := range shards {
		go kv.moveIn(shard, gid, newConfig)
	}
}

func (kv *ShardKV) moveIn(shard int, gid int, newConfig *shardctrler.Config) {
	args := MigrateShardArgs{
		Shard:       shard,
		SendContent: true,
		ConfigNum:   newConfig.Num - 1,
	}

	reply := MigrateShardReply{}

	for !reply.Success {

		servers, ok := newConfig.Groups[gid]
		if ok {
			for _, server := range servers {
				reply = MigrateShardReply{}
				srv := kv.make_end(server)
				srv.Call("ShardKV.MigrateShard", args, &reply)

				if reply.Success {
					op := Op{
						OpType:  OpMigrate,
						shardID: shard,
						shard:   reply.Shard,
					}
					kv.rf.Start(op)

					return
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (kv *ShardKV) moveOutAll(shards map[int]int, newConfig *shardctrler.Config) {
	for shard, gid := range shards {
		kv.moveIn(shard, gid, newConfig)
	}
}

func (kv *ShardKV) moveOut(shard int, gid int, newConfig *shardctrler.Config) {
	args := MigrateShardArgs{
		Shard:       shard,
		SendContent: false,
		ConfigNum:   newConfig.Num,
	}

	reply := MigrateShardReply{}

	for !reply.Success {

		servers, ok := newConfig.Groups[gid]
		if ok {
			for _, server := range servers {
				reply = MigrateShardReply{}
				srv := kv.make_end(server)
				srv.Call("ShardKV.MigrateShard", args, &reply)

				if reply.Success {
					return
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (kv *ShardKV) waitMoveInApplied(shards map[int]int, configNum int) {
	for {
		kv.cond.L.Lock()

		success := true
		for shard := range shards {
			result, ok := kv.availableShards[shard]
			if !ok || result != configNum {
				success = false
			}
		}

		if success {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (kv *ShardKV) waitMoveOutAcknowledged(shards map[int]int) {

}

func (kv *ShardKV) handleApply() {
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

func (kv *ShardKV) applyCommandMessage(msg raft.ApplyMsg) {
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

	if committed.OpType == OpMigrate {
		return
	}

	kv.cond.L.Lock()
	empty := kv.requestOp.OpType == OpEmpty
	if empty {
		kv.cond.L.Unlock()
		return
	} else if kv.requestOp.ClerkID == committed.ClerkID && kv.requestOp.CommandID == committed.CommandID {
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

func (kv *ShardKV) applyInstallSnapshot(msg raft.ApplyMsg) {
	raft.Assert(msg.SnapshotValid)

	kv.readSnapshot(msg.Snapshot)
	raft.DLog(raft.ServerInfo, kv.me, "apply snapshot at index", msg.SnapshotIndex)
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.sm = shardctrler.MakeClerk(ctrlers)

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	return kv
}
