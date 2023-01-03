package shardctrler

import (
	"6.824/raft"
	"math"
	"sync/atomic"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	// Your data here.
	requestOp  Op
	requestErr Err
	requestCh  chan request
	term       int
	cond       *sync.Cond
	// trace the increment of CommandID of different Clerks
	clerkMCommandID map[int]int

	configs []Config // indexed by config num
}

const (
	commitHandleSleepMs  = 10
	electionMs           = 50
	OpTimeOutMs          = 100
	unsuccessfulMaxTimes = 3
)

const (
	OpEmpty = iota
	OpJoin
	OpLeave
	OpMove
	OpQuery
)

type Op struct {
	// Your data here.
	OpType    int
	Term      int
	NewConfig Config
	CommandID int
	ClerkID   int
}

type request struct {
	ch        chan Err
	OpType    int
	Args      interface{}
	Term      int
	CommandID int
	ClerkID   int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	term, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	ch := make(chan Err)

	sc.requestCh <- request{
		ch:        ch,
		OpType:    OpJoin,
		Args:      *args,
		Term:      term,
		CommandID: args.CommandID,
		ClerkID:   args.ClerkID,
	}

	err := <-ch
	reply.Err = err
	if err == ErrWrongLeader {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	term, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	ch := make(chan Err)

	sc.requestCh <- request{
		ch:        ch,
		OpType:    OpLeave,
		Args:      *args,
		Term:      term,
		CommandID: args.CommandID,
		ClerkID:   args.ClerkID,
	}

	err := <-ch
	reply.Err = err
	if err == ErrWrongLeader {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	term, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	ch := make(chan Err)

	sc.requestCh <- request{
		ch:        ch,
		OpType:    OpMove,
		Args:      *args,
		Term:      term,
		CommandID: args.CommandID,
		ClerkID:   args.ClerkID,
	}

	err := <-ch
	reply.Err = err
	if err == ErrWrongLeader {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	term, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	ch := make(chan Err)

	sc.requestCh <- request{
		ch:        ch,
		OpType:    OpQuery,
		Args:      *args,
		Term:      term,
		CommandID: args.CommandID,
		ClerkID:   args.ClerkID,
	}

	err := <-ch
	reply.Err = err
	if err == ErrWrongLeader {
		reply.WrongLeader = true
	} else if err == OK {
		sc.mu.Lock()
		if args.Num != -1 && args.Num < len(sc.configs) {
			reply.Config = sc.configs[args.Num]
		} else {
			reply.Config = sc.configs[len(sc.configs)-1]
		}
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) handleRequest() {
	raft.DLog(raft.CtrlInfo, sc.me, "handleRequest, GoID", raft.GoID())

	kill := make(chan bool)
	go func() {
		for !sc.killed() {
			time.Sleep(raft.HeartBeatMs * time.Millisecond)
		}
		kill <- true
	}()

	for {
		select {
		case req := <-sc.requestCh:
			term, isLeader := sc.rf.GetState()

			raft.DLog(raft.CtrlInfo, sc.me, "receives request", req)

			sc.cond.L.Lock()
			if !isLeader || term != sc.term || req.Term != term {
				sc.cond.L.Unlock()
				req.ch <- ErrWrongLeader
				continue
			} else if req.CommandID == -1 {
				sc.cond.L.Unlock()
				req.ch <- OK
				continue
			}

			newConfig := Config{}
			if req.OpType == OpJoin {
				args := req.Args.(JoinArgs)
				newConfig = sc.join(args)
			} else if req.OpType == OpLeave {
				args := req.Args.(LeaveArgs)
				newConfig = sc.leave(args)
			} else if req.OpType == OpMove {
				args := req.Args.(MoveArgs)
				newConfig = sc.move(args)
			} else {
				newConfig.Num = -1
			}

			op := Op{
				Term:      req.Term,
				CommandID: req.CommandID,
				ClerkID:   req.ClerkID,
				NewConfig: newConfig,
				OpType:    req.OpType,
			}

			sc.requestOp = op
			raft.DLog(raft.CtrlInfo, sc.me, "push", op)

			sc.rf.Start(op)

			// wait for apply
			sc.cond.Wait()

			sc.requestOp = Op{}
			raft.DLog(raft.CtrlInfo, sc.me, "pull", op)

			sc.cond.L.Unlock()

			req.ch <- sc.requestErr
		case <-kill:
			return
		}
	}
}

func (sc *ShardCtrler) handleLeaderTransit() {
	raft.DLog(raft.CtrlInfo, sc.me, "handleLeaderTransit", raft.GoID())

	for !sc.killed() {
		term, isLeader := sc.rf.GetState()
		sc.cond.L.Lock()
		termChanged := term != sc.term
		if !isLeader || termChanged {
			if sc.requestOp.OpType != OpEmpty && sc.requestOp.Term != term {
				if sc.requestOp.OpType != OpEmpty {
					raft.DLog(raft.CtrlInfo, sc.me, "kill op:", sc.requestOp)
					sc.requestErr = ErrWrongLeader
					sc.cond.Signal()
				}
			}
			if termChanged {
				sc.term = term
			}
		}
		sc.cond.L.Unlock()
		time.Sleep(raft.HeartBeatMs / 2 * time.Millisecond)
	}
}

func (sc *ShardCtrler) handleApply() {
	raft.DLog(raft.CtrlInfo, sc.me, "handleApply()", raft.GoID())

	kill := make(chan bool)
	go func() {
		for !sc.killed() {
			time.Sleep(time.Millisecond * 100)
		}
		kill <- true
	}()

	for {
		select {
		case msg := <-sc.applyCh:
			sc.applyCommandMessage(msg)
		case <-kill:
			return
		}
	}
}

func (sc *ShardCtrler) applyCommandMessage(msg raft.ApplyMsg) {
	raft.Assert(msg.CommandValid)

	committed, ok := msg.Command.(Op)
	raft.Assert(ok, "command field of msg in applyCh must be type Op.")
	err := sc.apply(committed)

	raft.DLog(raft.CtrlInfo, sc.me, "apply", committed, "index:", msg.CommandIndex,
		"error:", err)

	sc.cond.L.Lock()
	empty := sc.requestOp.OpType == OpEmpty
	if empty {
		sc.cond.L.Unlock()
		return
	} else if sc.requestOp.NewConfig.Num == committed.NewConfig.Num {
		sc.requestErr = err
		sc.cond.Signal()
	} else {
		term, isLeader := sc.rf.GetState()
		raft.Assert(!isLeader || term != committed.Term,
			"requestOp", sc.requestOp, "mismatch with committed", committed)
		if !isLeader {
			sc.requestErr = ErrWrongLeader
			sc.cond.Signal()
		}
	}
	sc.cond.L.Unlock()
}

func (sc *ShardCtrler) apply(op Op) Err {
	commandID, ok := sc.clerkMCommandID[op.ClerkID]
	if !ok || op.CommandID > commandID {
		raft.Assert(commandID+1 == op.CommandID || !ok, "command out of order clerkID:", op.ClerkID,
			"last commandID", commandID, "new commandID", op.CommandID)
		sc.clerkMCommandID[op.ClerkID] = op.CommandID
	} else {
		return OK
	}

	if op.NewConfig.Num != -1 {
		sc.mu.Lock()
		sc.configs = append(sc.configs, op.NewConfig)
		sc.mu.Unlock()
	}
	return OK
}

func (sc *ShardCtrler) join(args JoinArgs) Config {
	newConfig := sc.configs[len(sc.configs)-1]
	newConfig.Num++
	newConfig.Groups = make(map[int][]string)

	for key, val := range sc.configs[len(sc.configs)-1].Groups {
		newConfig.Groups[key] = val
	}

	gidMshards := make(map[int][]int)
	for shard, gid := range newConfig.Shards {
		_, ok := gidMshards[gid]
		if ok {
			gidMshards[gid] = append(gidMshards[gid], shard)
		} else {
			gidMshards[gid] = make([]int, 1)
			gidMshards[gid][0] = shard
		}
	}

	for key, val := range args.Servers {
		newConfig.Groups[key] = val
		gidMshards[key] = make([]int, 0)
	}

	for !isBalance(gidMshards) {
		gid := nextGroup(gidMshards)
		destGid := minShardsGroup(gidMshards)

		shard := gidMshards[gid][0]
		newConfig.Shards[shard] = destGid
		gidMshards[gid] = gidMshards[gid][1:]
		gidMshards[destGid] = append(gidMshards[destGid], shard)
	}
	return newConfig
}

func (sc *ShardCtrler) leave(args LeaveArgs) Config {
	newConfig := sc.configs[len(sc.configs)-1]
	newConfig.Num++
	newConfig.Groups = make(map[int][]string)

	for key, val := range sc.configs[len(sc.configs)-1].Groups {
		newConfig.Groups[key] = val
	}

	for gid := range newConfig.Groups {
		for _, leaveGid := range args.GIDs {
			if gid == leaveGid {
				delete(newConfig.Groups, gid)
			}
		}
	}

	for i, gid := range newConfig.Shards {
		for _, leaveGid := range args.GIDs {
			if gid == leaveGid {
				newConfig.Shards[i] = 0
			}
		}
	}

	gidMshards := make(map[int][]int)
	for shard, gid := range newConfig.Shards {
		_, ok := gidMshards[gid]
		if ok {
			gidMshards[gid] = append(gidMshards[gid], shard)
		} else {
			gidMshards[gid] = make([]int, 1)
			gidMshards[gid][0] = shard
		}
	}

	for gid := range newConfig.Groups {
		_, ok := gidMshards[gid]
		if !ok {
			gidMshards[gid] = make([]int, 0)
		}
	}

	for !isBalance(gidMshards) {
		gid := nextGroup(gidMshards)
		destGid := minShardsGroup(gidMshards)

		shard := gidMshards[gid][0]
		newConfig.Shards[shard] = destGid
		gidMshards[gid] = gidMshards[gid][1:]
		gidMshards[destGid] = append(gidMshards[destGid], shard)
	}

	return newConfig
}

func (sc *ShardCtrler) move(args MoveArgs) Config {
	newConfig := sc.configs[len(sc.configs)-1]
	newConfig.Num++
	newConfig.Groups = make(map[int][]string)

	for key, val := range sc.configs[len(sc.configs)-1].Groups {
		newConfig.Groups[key] = val
	}

	newConfig.Shards[args.Shard] = args.GID
	return newConfig
}

func nextGroup(m map[int][]int) int {
	_, ok := m[0]
	if ok && len(m[0]) > 0 {
		return 0
	}

	max := 0
	maxKey := 0
	for key, val := range m {
		if len(val) > max {
			maxKey = key
			max = len(val)
		}
	}
	return maxKey
}

func minShardsGroup(m map[int][]int) int {
	min := math.MaxInt
	minKey := 0
	for key, val := range m {
		if key != 0 && len(val) < min {
			min = len(val)
			minKey = key
		}
	}
	return minKey
}

func isBalance(m map[int][]int) bool {
	_, ok := m[0]
	if len(m) == 1 {
		return true
	}
	if ok && len(m[0]) > 0 {
		return false
	}

	max, min := -1, math.MaxInt
	for gid, val := range m {
		if gid != 0 {
			if len(val) > max {
				max = len(val)
			}
			if len(val) < min {
				min = len(val)
			}
		}
	}
	return (max - min) <= 1
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}

func (sc *ShardCtrler) killed() bool {
	if atomic.LoadInt32(&sc.dead) == 1 {
		return true
	}
	return false
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.requestCh = make(chan request)
	mu := sync.Mutex{}
	sc.cond = sync.NewCond(&mu)
	sc.clerkMCommandID = make(map[int]int)

	go sc.handleRequest()
	go sc.handleLeaderTransit()
	go sc.handleApply()

	return sc
}
