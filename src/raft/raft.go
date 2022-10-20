package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer

//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"log"
	"math/rand"
	"os"
	"runtime"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	voteSuccessRatio      = 0.5
	maxTickerPeriod       = 500
	maxVoteDuration       = 500
	minVoteDuration       = 0
	voteCallSleepDuration = 50
	tickerSleepDuration   = 200
	heartBeatPeriod       = 100
	maxRPCRetryDuration   = 500
)

var tmp *log.Logger

// initialize logger
func init() {
	tmpIO, err := os.OpenFile("tmp.txt", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalf("Can't open file.")
	}
	tmp = log.New(tmpIO, "", log.LstdFlags)
	runtime.SetFinalizer(tmp, func(logger *log.Logger) {
		err := tmpIO.Close()
		if err != nil {
			log.Fatalf("Can't close file.")
		}
	})
}

const (
	Follower int = iota
	Candidate
	Leader
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type persistent struct {
	votedFor    int
	currentTerm int
	logEntries  []string
}

type leaderStatus struct {
	nextIndex  []int
	matchIndex []int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persisted state
	ps persistent

	// volatile state
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine
	leaderStat  *leaderStatus

	status   int
	lastTime time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	term = rf.ps.currentTerm
	isleader = rf.status == Leader

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status == Follower && rf.ps.votedFor == -1 {
		reply.VoteGranted = args.Term >= rf.ps.currentTerm
	} else {
		reply.VoteGranted = args.Term > rf.ps.currentTerm
	}
	if args.Term > rf.ps.currentTerm {
		rf.status = Follower
		rf.ps.currentTerm = args.Term
	}
	if reply.VoteGranted {
		rf.ps.votedFor = args.CandidateId
	}
	rf.lastTime = time.Now()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      [][]string
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(args.Entries) == 0 {
		if rf.status == Follower {
			reply.Success = args.Term >= rf.ps.currentTerm
		} else {
			reply.Success = args.Term > rf.ps.currentTerm
		}
		if args.Term > rf.ps.currentTerm {
			rf.ps.currentTerm = args.Term
			rf.ps.votedFor = -1
			rf.status = Follower
		}
	} else {
		// TODO
	}
	rf.lastTime = time.Now()
	reply.Term = rf.ps.currentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	index := rf.commitIndex
	term := rf.ps.currentTerm
	isLeader := rf.status == Leader

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) vote(tickerSleepDuration time.Duration) {

	rf.mu.Lock()
	rf.lastTime = time.Now()
	lastTime := rf.lastTime
	rf.status = Candidate
	rf.ps.currentTerm++
	term := rf.ps.currentTerm
	me := rf.me
	totalNum := len(rf.peers)
	rf.mu.Unlock()

	log.Println(me, "start vote, time duration:", tickerSleepDuration)

	var voteGrantedNum atomic.Int32
	voteGrantedNum.Store(1)
	var callNum atomic.Int32
	var voteFail atomic.Bool

	for i := 0; i < totalNum; i++ {
		if i != me {
			go func(server int) {
				args := RequestVoteArgs{term, me, 0, 0}
				reply := RequestVoteReply{}
				if !rf.sendRPC(server, "Raft.RequestVote", &args, &reply) {
					return
				}
				if reply.VoteGranted {
					voteGrantedNum.Add(1)
				} else {
					voteFail.Store(true)
					rf.mu.Lock()
					rf.ps.currentTerm = reply.Term
					rf.ps.votedFor = -1
					rf.mu.Unlock()
				}
				callNum.Add(1)
			}(i)
		}
	}

	for !voteFail.Load() && time.Now().Sub(lastTime) < tickerSleepDuration {
		rf.mu.Lock()
		if float32(voteGrantedNum.Load())/float32(totalNum) >= voteSuccessRatio && rf.status == Candidate {
			terminate := time.Now().Sub(rf.lastTime) > tickerSleepDuration
			if !terminate {
				rf.initializeLeader()
				rf.mu.Unlock()
				log.Println("new leader", rf.me)
				return
			}
		} else {
			// log.Println("election failed, ratio:", float32(voteGrantedNum.Load())/float32(totalNum))
		}
		rf.mu.Unlock()
		time.Sleep(voteCallSleepDuration * time.Millisecond)
	}
	// fail
	// log.Println("election failed,", "callNum:", callNum.Load(), "voteGrantedNum:", voteGrantedNum.Load(),
	//	"voteFail:", voteFail.Load(), "total:", totalNum)
	rf.mu.Lock()
	rf.status = Follower
	rf.lastTime = time.Now()
	rf.mu.Unlock()
}

// need additional lock
func (rf *Raft) initializeLeader() {
	rf.status = Leader

	// TODO
	// rf.leaderStat

	go rf.heartBeat()
}

func (rf *Raft) sendRPC(server int, method string, args interface{}, reply interface{}) bool {
	ok := false
	begin := time.Now()
	for !ok {
		rf.mu.RLock()
		peer := rf.peers[server]
		rf.mu.RUnlock()
		ok = peer.Call(method, args, reply)
		if time.Now().Sub(begin) > maxRPCRetryDuration*time.Millisecond {
			tmp.Println(rf.me, "failed to connect", server, "method:", method, "args:", args)
			return false
		}
	}
	tmp.Println(rf.me, "connect to", server, "method", method, "args:", args, "reply:", reply)
	return true
}

func (rf *Raft) heartBeat() {
	rf.mu.RLock()
	term := rf.ps.currentTerm
	rf.mu.RUnlock()

	var terminate atomic.Bool
	for rf.killed() == false && !terminate.Load() {
		for i := range rf.peers {
			go func(server int) {
				if server != rf.me {
					args := AppendEntriesArgs{Entries: nil, Term: term}
					reply := AppendEntriesReply{}
					if !rf.sendRPC(server, "Raft.AppendEntries", &args, &reply) {
						return
					}
					if !reply.Success {
						terminate.Store(true)
						rf.mu.Lock()
						if rf.status == Leader {
							rf.status = Follower
							rf.ps.currentTerm = reply.Term
							rf.ps.votedFor = -1
							log.Println("term", term, "leader transit to follower with term", reply.Term)
						}
						rf.mu.Unlock()
					}
				}
			}(i)
		}
		sleepDuration := heartBeatPeriod * time.Millisecond
		if sleepDuration > 0 {
			time.Sleep(sleepDuration)
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.RLock()
		lastTime := rf.lastTime
		rf.mu.RUnlock()

		if time.Now().Sub(lastTime) > maxTickerPeriod*time.Millisecond {
			r := rand.Intn(maxVoteDuration - minVoteDuration)
			sleepTime := time.Duration(r+minVoteDuration) * time.Millisecond
			go rf.vote(sleepTime)
			time.Sleep(sleepTime)
		}
		time.Sleep(tickerSleepDuration * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.status = Follower
	rf.leaderStat = nil
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastTime = time.Now()
	rf.ps.currentTerm = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
