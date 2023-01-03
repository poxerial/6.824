package kvraft

import (
	"crypto/rand"
	"log"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
	"6.824/raft"
)

var clerkNum = 0

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	// You'll have to add code here
	ck.ID = clerkNum
	clerkNum++
	return ck
}

func (ck *Clerk) sendRPC(method string, args interface{}, reply interface{}) bool {
	leaderIndex := ck.lastLeaderIndex

	raft.DLog(raft.ClientInfo, "clerk", ck.ID, "starts to send", method, "to", leaderIndex, "args:", args)
	startTime := time.Now()

	ret := ck.servers[leaderIndex].Call(method, args, reply)
	duration := time.Now().Sub(startTime)
	raft.DLog(raft.ClientInfo, "clerk", ck.ID, "finish", method, "duration:", duration, "args", args, "reply", reply)
	return ret

}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()

	args := GetArgs{}
	args.Key = key
	args.ClerkID = ck.ID

	args.CommandID = ck.commandID
	ck.commandID++

	reply := GetReply{}
	success := ck.sendRPC("KVServer.Get", &args, &reply)
	lastUnsuccessful := false
	unsuccessfulCount := 1
	for (!success || reply.Err != OK) && reply.Err != ErrNoKey {
		if !success {
			if lastUnsuccessful {
				// if unsuccessfulCount hits unsuccessfulMaxTimes, the corresponding leader may have crashed
				unsuccessfulCount++
				if unsuccessfulCount >= unsuccessfulMaxTimes {
					reply.Err = ErrWrongLeader
				}
			} else {
				lastUnsuccessful = true
			}
		} else {
			lastUnsuccessful = false
			unsuccessfulCount = 1
		}

		if reply.Err == ErrWrongLeader || reply.Err == ErrTimeOut {
			ck.findOutLeader()
		} else if reply.Err == ErrReGet {
			args.CommandID = ck.commandID
			ck.commandID++
		}

		reply = GetReply{}
		success = ck.sendRPC("KVServer.Get", &args, &reply)
	}

	if reply.Err == OK {
		return reply.Value
	}
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	raft.DLog(raft.ClientInfo, ck.ID, "receives", op, "key:", key, "value:", value)

	ck.mu.Lock()
	defer ck.mu.Unlock()

	args := PutAppendArgs{}
	args.Key = key
	args.Op = op
	args.Value = value
	args.ClerkID = ck.ID

	args.CommandID = ck.commandID
	ck.commandID++

	reply := PutAppendReply{}
	success := ck.sendRPC("KVServer.PutAppend", &args, &reply)

	unsuccessfulCount := 1
	lastUnsuccessful := false
	for !success || reply.Err != OK {
		if !success {
			if lastUnsuccessful {
				// if unsuccessfulCount hits unsuccessfulMaxTimes, the corresponding leader may have crashed
				unsuccessfulCount++
				if unsuccessfulCount >= unsuccessfulMaxTimes {
					reply.Err = ErrWrongLeader
				}
			} else {
				lastUnsuccessful = true
			}
		} else {
			lastUnsuccessful = false
			unsuccessfulCount = 1
		}

		if reply.Err == ErrWrongLeader || reply.Err == ErrTimeOut {
			ck.findOutLeader()
		}
		reply = PutAppendReply{}
		success = ck.sendRPC("KVServer.PutAppend", &args, &reply)
	}
}

// need additional lock
func (ck *Clerk) findOutLeader() {
	raft.DLog(raft.ClientInfo, "Clerk", ck.ID, "GoID", raft.GoID(), "starts to find leader.")

	mu := sync.Mutex{}
	wg := sync.WaitGroup{}
	leaders := make(map[int]int)
	for i := range ck.servers {
		wg.Add(1)
		args := GetArgs{
			Key:       "0",
			ClerkID:   ck.ID,
			CommandID: -1,
		}
		go func(server int) {
			defer wg.Done()

			success := false
			reply := GetReply{}
			sendTimes := 0
			for !success {
				if sendTimes > unsuccessfulMaxTimes {
					return
				}

				reply = GetReply{}
				success = ck.servers[server].Call("KVServer.Get", &args, &reply)

				sendTimes++
			}
			if reply.Err != ErrWrongLeader {
				mu.Lock()
				defer mu.Unlock()
				lastLeader, ok := leaders[reply.Term]
				if ok {
					log.Fatal("multiple leaders in same term:",
						lastLeader, "and", server, "are all at term", reply.Term)
				}
				leaders[reply.Term] = server
			}
		}(i)
	}
	wg.Wait()

	term := -1
	leader := -1
	for t, l := range leaders {
		if t > term {
			leader = l
			term = t
		}
	}

	if term == -1 {
		time.Sleep(electionMs)
		ck.findOutLeader()
	} else {
		ck.lastLeaderIndex = leader
		raft.DLog(raft.ClientInfo, ck.ID, "finds leader", leader)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
