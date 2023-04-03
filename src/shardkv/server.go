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

	OpAlterAvailableShards
	OpAlterConfig
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

	AvailableShards []int

	Shard  map[int]Shard
	Config shardctrler.Config
}

type ShardKV struct {
	mu           raft.DLock
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastApplyIndex atomic.Int32

	persister       *raft.Persister
	sm              *shardctrler.Clerk
	availableShards map[int]int
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
	DB map[string]string
	// trace the increment of CommandID of different Clerks
	ClerkMCommandID map[int]int
}

func (kv *ShardKV) setKill(kill chan bool) {
	for {
		select {
		case <-kill:
			return
		default:
			time.Sleep(100 * time.Millisecond)
			if kv.killed() {
				kill <- true
				return
			}
		}
	}
}

func copyShard(src *Shard, dest *Shard) {
	dest.DB = make(map[string]string)
	dest.ClerkMCommandID = make(map[int]int)

	for key, val := range src.DB {
		dest.DB[key] = val
	}
	for key, val := range src.ClerkMCommandID {
		dest.ClerkMCommandID[key] = val
	}
}

func (kv *ShardKV) handleRequest() {
	raft.DLog(raft.ServerInfo, kv.gid, kv.me, "handleRequest, GoID", raft.GoID())

	kill := make(chan bool, 2)
	defer func() {
		kill <- true
	}()
	go kv.setKill(kill)
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
			raft.DLog(raft.ServerInfo, kv.gid, kv.me, "push", req.op)

			kv.rf.Start(req.op)

			// wait for apply
			kv.cond.Wait()

			kv.requestOp = Op{}
			raft.DLog(raft.ServerInfo, kv.gid, kv.me, "pull", req.op)

			res := result{
				val:     kv.requestResult,
				success: kv.requestSuccess,
			}

			kv.cond.L.Unlock()

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
	raft.DLog(raft.ServerInfo, kv.gid, kv.me, "GoID:", raft.GoID(), "receives Get", args)
	defer raft.DLog(raft.ServerInfo, kv.gid, kv.me, "finishes Get", args, "and reply", reply)

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
	raft.DLog(raft.ServerInfo, kv.gid, kv.me, "GoId:", raft.GoID(), "receives PutAppend", args)
	defer raft.DLog(raft.ServerInfo, kv.gid, kv.me, "reply PutAppend", args, "with", reply)

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
	defer func() {
		if reply.Success {
			raft.DLog(raft.ServerInfo, kv.gid, kv.me, "receive MigrateShard", args, "reply", reply)
		}
	}()

	kv.mu.Lock()
	kv.cond.L.Lock()
	shard, hasShard := kv.shards[args.Shard]
	currConfigNum := kv.config.Num
	_, shard_available := kv.availableShards[args.Shard]
	kv.cond.L.Unlock()
	kv.mu.Unlock()

	// ask if this peer's configNum >= args.ConfigNum
	if !args.SendContent {
		reply.Success = currConfigNum >= args.ConfigNum
		return
	}

	// ask for unavailable shard
	// if send available shard, the consistency will be broken
	if !hasShard || shard_available {
		reply.Success = false
		return
	}

	reply.Shard = shard
	reply.Success = true
	return
}

func (kv *ShardKV) apply(op *Op) (success bool, result string) {
	if op.OpType == OpAlterConfig {
		kv.applyAlterConfig(op)
		success = true
		result = OK
		return
	} else if op.OpType == OpAlterAvailableShards {
		kv.applyAlterAvailableShards(op)
		success = true
		result = OK
		return
	}

	if !kv.keyAvailable(op.Key) {
		success = false
		result = ErrWrongGroup
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	shard := key2shard(op.Key)
	db := kv.shards[shard].DB

	commandID, ok := kv.shards[shard].ClerkMCommandID[op.ClerkID]
	if !ok || op.CommandID > commandID {
		raft.Assert(commandID+1 == op.CommandID || !ok, "command out of order clerkID:", op.ClerkID,
			"last commandID", commandID, "new commandID", op.CommandID)
		kv.shards[shard].ClerkMCommandID[op.ClerkID] = op.CommandID
	} else if op.OpType != OpGet {
		success = true
		return
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

		kv.mu.Lock()
		kv.cond.L.Lock()
		err1 := encoder.Encode(kv.shards)
		err2 := encoder.Encode(kv.config)
		err3 := encoder.Encode(kv.availableShards)
		kv.mu.Unlock()
		kv.cond.L.Unlock()

		raft.Assert(err1 == nil && err2 == nil && err3 == nil, "Failed to encode snapshot:", err1, err2, err3)

		data := writer.Bytes()
		kv.rf.Snapshot(lastCommandIndex, data)
	} else {
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) readSnapshot(data []byte) {
	writer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(writer)

	//db := make(map[string]string)
	//ClerkMCommandID := make(map[int]int)

	kv.mu.Lock()
	kv.cond.L.Lock()
	kv.lastApplyIndex.Store(-1)
	err1 := decoder.Decode(&kv.shards)
	err2 := decoder.Decode(&kv.config)
	err3 := decoder.Decode(&kv.availableShards)
	kv.cond.L.Unlock()
	kv.mu.Unlock()
	raft.Assert(err1 == nil && err2 == nil, "Failed to decode snapshot:", err1, err2, err3)
}

func (kv *ShardKV) handleLeaderTransit() {
	raft.DLog(raft.ServerInfo, kv.gid, kv.me, "handleLeaderTransit", raft.GoID())

	for !kv.killed() {
		term, isLeader := kv.rf.GetState()
		kv.cond.L.Lock()
		termChanged := term != kv.term
		if !isLeader || termChanged {
			if kv.requestOp.OpType != OpEmpty && kv.requestOp.Term != term {
				if kv.requestOp.OpType != OpEmpty {
					raft.DLog(raft.ServerInfo, kv.gid, kv.me, "kill op:", kv.requestOp)
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

func (kv *ShardKV) alterAvailableShards(shards []int, configNum int) {
	kill := make(chan bool, 2)
	defer func() {
		kill <- true
	}()
	go kv.setKill(kill)

	for {
		term, isLeader := kv.rf.GetState()
		if !isLeader || kv.killed() {
			return
		}
		op := Op{
			OpType:          OpAlterAvailableShards,
			Term:            term,
			AvailableShards: shards,
			Config: shardctrler.Config{
				Num: configNum,
			},
		}
		ch := make(chan result)
		r := request{
			ch: ch,
			op: op,
		}
		kv.requestCh <- r

		select {
		case result := <-ch:
			if result.success {
				raft.DLog(raft.ServerInfo, kv.gid, kv.me, "alter availableShards to", shards)
				return
			} else {
				_, isLeader = kv.rf.GetState()
				if !isLeader {
					return
				}
			}
		case <-kill:
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (kv *ShardKV) alterConfig(shards map[int]Shard, config shardctrler.Config) {
	kill := make(chan bool, 2)
	defer func() {
		kill <- true
	}()
	go kv.setKill(kill)

	for {
		term, isLeader := kv.rf.GetState()
		if !isLeader || kv.killed() {
			return
		}
		op := Op{
			OpType: OpAlterConfig,
			Term:   term,
			Config: config,
			Shard:  shards,
		}
		ch := make(chan result)
		r := request{
			ch: ch,
			op: op,
		}
		kv.requestCh <- r

		select {
		case result := <-ch:
			if result.success {
				raft.DLog(raft.ServerInfo, kv.gid, kv.me, "alter config to", config.Num)
				return
			} else {
				_, isLeader = kv.rf.GetState()
				if !isLeader {
					return
				}
			}
		case <-kill:
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (kv *ShardKV) handleConfigTransit() {
	raft.DLog(raft.ServerInfo, kv.gid, kv.me, "handleConfigTransit", raft.GoID())

	for !kv.killed() {
		newConfig := kv.sm.Query(-1)

		kv.mu.Lock()
		kv.cond.L.Lock()
		_, isLeader := kv.rf.GetState()
		if newConfig.Num > kv.config.Num && isLeader {
			kv.cond.L.Unlock()

			if newConfig.Num != kv.config.Num+1 {
				newConfig = kv.sm.Query(kv.config.Num + 1)
			}

			raft.DLog(raft.ServerInfo, raft.GoID(), kv.me, "begin handleConfigTransit at config", newConfig.Num)
			raft.Assert(newConfig.Num == kv.config.Num+1)

			oldConfig := kv.config

			moveInShards := make(map[int]int)
			moveOutShards := make(map[int]int)

			for shard, gid := range newConfig.Shards {
				if gid == kv.gid {
					oldGid := oldConfig.Shards[shard]
					if oldGid != kv.gid {
						moveInShards[shard] = oldConfig.Shards[shard]
					}
				}
			}

			kv.mu.Unlock()

			availableShards := make([]int, 0)

			for shard, oldGid := range oldConfig.Shards {
				if oldGid == kv.gid {
					gid := newConfig.Shards[shard]
					if gid != kv.gid {
						moveOutShards[shard] = gid
					} else {
						availableShards = append(availableShards, shard)
					}
				}
			}

			kv.alterAvailableShards(availableShards, newConfig.Num)

			shards := make(map[int]Shard)
			if newConfig.Num != 1 {
				shards = kv.getAllMoveInShards(moveInShards, &oldConfig)
				kv.moveOutAll(moveOutShards, &newConfig)
			} else {
				for shard := range moveInShards {
					shards[shard] = Shard{
						DB:              make(map[string]string),
						ClerkMCommandID: make(map[int]int),
					}
				}
			}
			kv.alterConfig(shards, newConfig)
		} else {
			kv.mu.Unlock()
			kv.cond.L.Unlock()
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (kv *ShardKV) getAllMoveInShards(shards map[int]int, oldConfig *shardctrler.Config) (result map[int]Shard) {
	result = make(map[int]Shard)
	if len(shards) == 0 {
		return result
	}
	cond := sync.NewCond(&sync.Mutex{})

	for shard := range shards {
		go kv.getMoveInShard(shard, oldConfig, cond, result)
	}

	for {
		cond.L.Lock()
		cond.Wait()
		if len(shards) == len(result) {
			return result
		}
		cond.L.Unlock()
	}
}

func (kv *ShardKV) getMoveInShard(shard int, oldConfig *shardctrler.Config, cond *sync.Cond, shards map[int]Shard) {
	kv.mu.Lock()
	args := MigrateShardArgs{
		Shard:       shard,
		SendContent: true,
		ConfigNum:   oldConfig.Num,
	}
	gid := oldConfig.Shards[shard]
	servers, ok := oldConfig.Groups[gid]
	kv.mu.Unlock()

	reply := MigrateShardReply{}

	for !reply.Success {
		_, isLeader := kv.rf.GetState()
		if !isLeader || kv.killed() {
			return
		}

		if ok {
			for _, server := range servers {
				reply = MigrateShardReply{}
				srv := kv.make_end(server)
				srv.Call("ShardKV.MigrateShard", args, &reply)

				if reply.Success {
					cond.L.Lock()
					shards[shard] = reply.Shard
					cond.L.Unlock()
					cond.Signal()
					return
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (kv *ShardKV) moveOutAll(shards map[int]int, newConfig *shardctrler.Config) {
	wg := sync.WaitGroup{}
	wg.Add(len(shards))
	for shard, gid := range shards {
		go kv.moveOut(shard, gid, newConfig, &wg)
	}
	wg.Wait()
}

func (kv *ShardKV) moveOut(shard int, gid int, newConfig *shardctrler.Config, wg *sync.WaitGroup) {
	defer wg.Done()

	args := MigrateShardArgs{
		Shard:       shard,
		SendContent: false,
		ConfigNum:   newConfig.Num,
	}

	servers, ok := newConfig.Groups[gid]
	raft.Assert(ok, "OK")
	numSuccess := 0
	cond := sync.NewCond(&sync.Mutex{})
	numServers := len(servers)
	for _, server := range servers {
		go func(_server string) {
			reply := MigrateShardReply{}
			cond.L.Lock()
			for numSuccess <= numServers/2 || !kv.killed() {
				cond.L.Unlock()

				_, isLeader := kv.rf.GetState()
				if !isLeader {
					cond.Signal()
					return
				}

				reply = MigrateShardReply{}
				srv := kv.make_end(_server)
				srv.Call("ShardKV.MigrateShard", args, &reply)

				if reply.Success {
					cond.L.Lock()
					numSuccess++
					if numSuccess > numServers/2 {
						cond.Signal()
					}
					cond.L.Unlock()
					return
				}
				time.Sleep(10 * time.Millisecond)
				cond.L.Lock()
			}
			if numSuccess > numServers/2 {
				cond.Signal()
			}
			cond.L.Unlock()
		}(server)
	}
	cond.L.Lock()
	cond.Wait()

	_, isLeader := kv.rf.GetState()
	if !isLeader || kv.killed() {
		cond.L.Unlock()
		return
	}

	// the majority of group members have the shard
	raft.Assert(numSuccess > numServers/2)
	cond.L.Unlock()
}

func (kv *ShardKV) handleApply() {
	raft.DLog(raft.ServerInfo, kv.gid, kv.me, "handleApply", raft.GoID())

	kill := make(chan bool, 2)
	defer func() {
		kill <- true
	}()
	go kv.setKill(kill)

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

func (kv *ShardKV) applyAlterConfig(committed *Op) {
	copiedShards := make(map[int]Shard)
	done := make(chan bool, 1)
	go func() {
		for id, shard := range committed.Shard {
			newShard := Shard{}
			copyShard(&shard, &newShard)
			copiedShards[id] = newShard
		}
		done <- true
	}()

	kv.mu.Lock()
	kv.cond.L.Lock()
	if kv.config.Num >= committed.Config.Num {
		kv.requestSuccess = true
		kv.mu.Unlock()
		kv.cond.L.Unlock()
		return
	}
	kv.cond.L.Unlock()
	kv.mu.Unlock()

	<-done

	kv.mu.Lock()
	kv.cond.L.Lock()
	kv.config = committed.Config
	for id, shard := range copiedShards {
		kv.shards[id] = shard
	}
	for id := range kv.shards {
		if committed.Config.Shards[id] != kv.gid {
			delete(kv.shards, id)
		}
	}
	kv.availableShards = make(map[int]int)
	for shardID, gid := range committed.Config.Shards {
		if gid == kv.gid {
			kv.availableShards[shardID] = committed.Config.Num
		}
	}
	kv.mu.Unlock()
	kv.cond.L.Unlock()
}

func (kv *ShardKV) applyAlterAvailableShards(committed *Op) {
	kv.mu.Lock()
	kv.cond.L.Lock()
	if kv.config.Num >= committed.Config.Num {
		kv.requestSuccess = true
		kv.mu.Unlock()
		kv.cond.L.Unlock()
		return
	}
	kv.cond.L.Unlock()
	kv.mu.Unlock()

	kv.cond.L.Lock()
	kv.availableShards = make(map[int]int)
	for _, shardID := range committed.AvailableShards {
		kv.availableShards[shardID] = committed.Config.Num
	}
	kv.cond.L.Unlock()
}

func (kv *ShardKV) applyCommandMessage(msg raft.ApplyMsg) {

	raft.Assert(msg.CommandValid)

	committed, ok := msg.Command.(Op)
	raft.Assert(ok, "command field of msg in applyCh must be type Op.")
	success, result := kv.apply(&committed)

	kv.checkSnapshot(msg.CommandIndex)

	var resultLog string
	if len(result) > 20 {
		resultLog = result[len(result)-20 : len(result)]
	} else {
		resultLog = result
	}
	raft.DLog(raft.ServerInfo, kv.gid, kv.me, "apply", committed, "index:", msg.CommandIndex,
		"result:", resultLog)

	if committed.OpType == OpAlterConfig {
		kv.applyAlterConfig(&committed)
	}

	kv.cond.L.Lock()
	empty := kv.requestOp.OpType == OpEmpty
	term, _ := kv.rf.GetState()
	if empty || committed.Term != term {
		kv.cond.L.Unlock()
		return
	} else if kv.requestOp.ClerkID == committed.ClerkID && kv.requestOp.CommandID == committed.CommandID {
		kv.requestResult = result
		kv.requestSuccess = success
		kv.cond.Signal()
	} else {
		term, isLeader := kv.rf.GetState()
		raft.Assert(!isLeader || term != committed.Term || committed.OpType >= OpAlterAvailableShards,
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
	raft.DLog(raft.ServerInfo, kv.gid, kv.me, "apply snapshot at index", msg.SnapshotIndex)
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
	kv.persister = persister

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.requestCh = make(chan request)
	kv.cond = sync.NewCond(&raft.DLock{})

	kv.rf = raft.Make(servers, gid*10+me, persister, kv.applyCh)
	kv.availableShards = make(map[int]int)
	kv.shards = make(map[int]Shard)

	snapshot := persister.ReadSnapshot()
	if len(snapshot) != 0 {
		kv.readSnapshot(snapshot)
	}

	go kv.handleLeaderTransit()
	go kv.handleApply()
	go kv.handleRequest()
	go kv.handleConfigTransit()

	return kv
}
