package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
	"6.824/raft"
)

var clerkNum = 0

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu              sync.Mutex
	lastLeaderIndex atomic.Int32
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
	leaderIndex := ck.lastLeaderIndex.Load()

	raft.DLog(raft.RPCInfo, "clerk starts to send", method, "to leader", leaderIndex, "args:", args)
	startTime := time.Now()

	ret := ck.servers[leaderIndex].Call(method, args, reply)
	duration := time.Now().Sub(startTime)
	raft.DLog(raft.RPCInfo, "clerk finish", method, "duration:", duration, "args", args, "reply", reply)
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
	args := GetArgs{}
	args.Key = key
	args.Clerk = ck.ID

	ck.mu.Lock()
	args.ID = ck.commandID
	ck.commandID++
	ck.mu.Unlock()

	reply := GetReply{}

	success := ck.sendRPC("KVServer.Get", &args, &reply)
	for (!success || reply.Err != OK) && reply.Err != ErrNoKey {
		time.Sleep(sendRPCSleepMs * time.Millisecond)
		if reply.Err == ErrWrongLeader {
			ck.lastLeaderIndex.Store(int32(nrand() % int64(len(ck.servers))))
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
	args := PutAppendArgs{}
	args.Key = key
	args.Op = op
	args.Value = value
	reply := PutAppendReply{}

	success := ck.sendRPC("KVServer.PutAppend", &args, &reply)
	for success && reply.Err == ErrWrongLeader {
		ck.lastLeaderIndex.Store(int32(nrand() % int64(len(ck.servers))))
		success = ck.sendRPC("KVServer.PutAppend", &args, &reply)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
