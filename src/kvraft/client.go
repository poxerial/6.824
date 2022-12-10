package kvraft

import (
	"6.824/labrpc"
	"6.824/raft"
	"sync/atomic"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeaderIndex atomic.Int32
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
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) sendRPC(method string, args interface{}, reply interface{}) bool {
	raft.DLog(raft.RPCInfo, "clerk starts to send", method, "args", args)
	defer raft.DLog(raft.RPCInfo, "clerk finish", method, "args", args, "reply", reply)

	leaderIndex := ck.lastLeaderIndex.Load()

	return ck.servers[leaderIndex].Call(method, args, reply)
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
	reply := GetReply{}

	success := ck.sendRPC("KVServer.Get", &args, &reply)
	for success && reply.Err == ErrWrongLeader {
		ck.lastLeaderIndex.Store(int32(nrand() % int64(len(ck.servers))))
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
