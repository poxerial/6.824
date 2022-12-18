package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrApplyFailed = "ApplyFailed"
)

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"

	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Clerk int
	ID    int32
}

type PutAppendReply struct {
	Err string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Clerk int
	ID    int
}

type GetReply struct {
	Err   string
	Value string
}
