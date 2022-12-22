package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrApplyFailed = "ErrApplyFailed"
	ErrReGet       = "ErrReGet"
	ErrTimeOut     = "ErrTimeOut"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"

	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkID   int
	CommandID int
}

type PutAppendReply struct {
	Err  Err
	Term int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkID   int
	CommandID int
}

type GetReply struct {
	Err   Err
	Value string
	Term  int
}
