package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
	"6.824/raft"
	"sync"
)
import "time"
import "crypto/rand"
import "math/big"

var clerkNum = 0

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	mu              sync.Mutex
	lastLeaderIndex int
	commandID       int
	ID              int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.ID = clerkNum
	clerkNum++
	return ck
}

// need additional lock
func (ck *Clerk) findOutLeader() {
	raft.DLog(raft.CtrlInfo, "Clerk", ck.ID, "GoID", raft.GoID(), "starts to find leader.")

	mu := sync.Mutex{}
	wg := sync.WaitGroup{}
	leaders := make([]int, 1)
	for i := range ck.servers {
		wg.Add(1)
		args := QueryArgs{
			Num: 0,
		}
		go func(server int) {
			defer wg.Done()

			success := false
			reply := QueryReply{}
			sendTimes := 0
			for !success {
				if sendTimes > unsuccessfulMaxTimes {
					return
				}

				reply := QueryReply{}
				success = ck.servers[server].Call("ShardCtrler.Query", &args, &reply)

				sendTimes++
			}
			if !reply.WrongLeader {
				mu.Lock()
				defer mu.Unlock()
				leaders = append(leaders, server)
			}
		}(i)
	}
	wg.Wait()

	if len(leaders) == 0 {
		time.Sleep(electionMs)
		ck.findOutLeader()
	} else {
		ck.lastLeaderIndex = leaders[0]
	}
}

func (ck *Clerk) sendRPC(method string, args interface{}, reply interface{}) bool {
	leaderIndex := ck.lastLeaderIndex

	raft.DLog(raft.CtrlInfo, "clerk", ck.ID, "starts to send", method, "to", leaderIndex, "args:", args)
	startTime := time.Now()

	ret := ck.servers[leaderIndex].Call(method, args, reply)
	duration := time.Now().Sub(startTime)
	raft.DLog(raft.CtrlInfo, "clerk", ck.ID, "finish", method, "duration:", duration, "args", args, "reply", reply)
	return ret
}

func (ck *Clerk) Query(num int) Config {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := &QueryArgs{}
	// Your code here.
	args.ClerkID = ck.ID
	args.CommandID = ck.commandID
	ck.commandID++

	args.Num = num
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := &JoinArgs{}
	// Your code here.
	args.ClerkID = ck.ID
	args.CommandID = ck.commandID
	ck.commandID++
	args.Servers = servers

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := &LeaveArgs{}
	// Your code here.
	args.ClerkID = ck.ID
	args.CommandID = ck.commandID
	ck.commandID++
	args.GIDs = gids

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := &MoveArgs{}
	// Your code here.
	args.ClerkID = ck.ID
	args.CommandID = ck.commandID
	ck.commandID++
	args.Shard = shard
	args.GID = gid

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
