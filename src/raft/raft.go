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
	"math/rand"

	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	voteSuccessRatio   = 0.5
	EntryCommitRatio   = 0.5
	maxTickerMs        = 200
	maxVoteMs          = 300
	minVoteMs          = 100
	voteCallSleepMs    = 50
	tickerSleepMs      = 100
	heartBeatMs        = 100
	maxRPCRetryMs      = 100
	appendEntryRetryMs = 0
	appendEntrySleepMs = 200
)

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
	logEntries  []Entry
}

type leaderStatus struct {
	entriesSend []int
	nextIndex   []int
	matchIndex  []int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        DLock               // Lock to protect shared access to this peer's state
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

	msgCh chan ApplyMsg
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
	if len(rf.ps.logEntries)-1 > args.LastLogIndex ||
		len(rf.ps.logEntries)-1 >= args.LastLogIndex && rf.ps.logEntries[args.LastLogIndex].Term != args.LastLogTerm {
		reply.VoteGranted = false
	} else {
		if rf.status == Follower && rf.ps.votedFor == -1 {
			reply.VoteGranted = args.Term >= rf.ps.currentTerm
		} else {
			reply.VoteGranted = args.Term > rf.ps.currentTerm
		}
	}
	if args.Term > rf.ps.currentTerm {
		rf.status = Follower
		rf.ps.currentTerm = args.Term
	}
	if reply.VoteGranted {
		rf.ps.votedFor = args.CandidateId
	}
	rf.lastTime = time.Now()
	DLog(FollowerInfo, rf.me, "receive Request Vote and reply", reply)
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
	Entries      []Entry
	LeaderCommit int
}

type Entry struct {
	Term    int
	Command interface{}
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) apply(index int) {
	for rf.lastApplied < index {
		// Apply(rf.ps.logEntries[rf.lastApplied + 1]) // apply command to state machine

		msg := ApplyMsg{Command: rf.ps.logEntries[rf.lastApplied+1].Command, CommandIndex: rf.lastApplied + 1, CommandValid: true}
		rf.msgCh <- msg
		DLog(ApplyInfo, rf.me, "send ApplyMsg", msg)

		rf.lastApplied++
	}
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
		if rf.ps.currentTerm > args.Term ||
			args.PrevLogIndex >= len(rf.ps.logEntries) ||
			rf.ps.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm {
			DLog(FollowerInfo, rf.me, "refuse AppendEntries", "log len:", len(rf.ps.logEntries),
				"last entry term", rf.ps.logEntries[len(rf.ps.logEntries)-1].Term)
			reply.Success = false
			reply.Term = rf.ps.currentTerm
		} else {
			reply.Success = true
			for i := range args.Entries {
				if i+args.PrevLogIndex+1 >= len(rf.ps.logEntries) {
					rf.ps.logEntries = append(rf.ps.logEntries, args.Entries[i])
				} else {
					rf.ps.logEntries[i+args.PrevLogIndex+1] = args.Entries[i]
				}
			}
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		lastNewEntryIndex := len(rf.ps.logEntries) - 1
		if args.LeaderCommit > lastNewEntryIndex {
			rf.commitIndex = lastNewEntryIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.apply(rf.commitIndex)
	}
	rf.lastTime = time.Now()
	reply.Term = rf.ps.currentTerm
	DLog(FollowerInfo, rf.me, "receive AppendEntries from", args.LeaderId, "and reply", reply)
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
	if rf.killed() {
		return 0, 0, false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader := rf.status == Leader

	// Your code here (2B).
	if isLeader {
		entry := Entry{rf.ps.currentTerm, command}
		rf.ps.logEntries = append(rf.ps.logEntries, entry)
		rf.leaderStat.entriesSend = append(rf.leaderStat.entriesSend, 1)
		DLog(UserInfo, "leader", rf.me, "receive command", command)
	} else {
		DLog(UserInfo, rf.me, "(not leader)", "receive command", command)
	}

	index := len(rf.ps.logEntries) - 1
	term := rf.ps.currentTerm

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

	// truncate log to commit index
	// as these logs may never be committed
	rf.ps.logEntries = rf.ps.logEntries[:rf.commitIndex+1]

	rf.lastTime = time.Now()
	rf.status = Candidate
	rf.ps.currentTerm++
	term := rf.ps.currentTerm
	me := rf.me
	lastTime := rf.lastTime
	totalNum := len(rf.peers)
	index := len(rf.ps.logEntries) - 1
	lastLogTerm := rf.ps.logEntries[index].Term
	rf.mu.Unlock()

	DLog(CandidateInfo, me, "start vote, time duration:", tickerSleepDuration)

	var voteGrantedNum atomic.Int32
	voteGrantedNum.Store(1)
	var callNum atomic.Int32
	var voteFail atomic.Bool

	for i := 0; i < totalNum; i++ {
		if i != me {
			go func(server int) {
				args := RequestVoteArgs{term, me, index, lastLogTerm}
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
				DLog(LeaderInfo, "new leader", me, "term", term)
				return
			}
		} else {
			DLog(CandidateInfo, rf.me, "election failed, ratio:", float32(voteGrantedNum.Load())/float32(totalNum))
		}
		rf.mu.Unlock()
		time.Sleep(voteCallSleepMs * time.Millisecond)
	}
	rf.mu.Lock()
	// fail
	DLog(CandidateInfo, rf.me, "election failed,", "callNum:", callNum.Load(), "voteGrantedNum:", voteGrantedNum.Load(),
		"voteFail:", voteFail.Load(), "total:", totalNum)
	rf.status = Follower
	rf.lastTime = time.Now()
	rf.mu.Unlock()
}

// need additional lock
func (rf *Raft) initializeLeader() {
	rf.status = Leader
	DLog(LeaderInfo, "Leader", rf.me, "initializing.")

	// TODO
	// rf.leaderStat
	rf.leaderStat = &leaderStatus{}
	rf.leaderStat.nextIndex = make([]int, len(rf.peers))
	rf.leaderStat.matchIndex = make([]int, len(rf.peers))
	rf.leaderStat.entriesSend = make([]int, len(rf.ps.logEntries))
	index := len(rf.ps.logEntries)
	for i := range rf.peers {
		rf.leaderStat.nextIndex[i] = index
		rf.leaderStat.matchIndex[i] = 0
	}

	leaderID := rf.me
	term := rf.ps.currentTerm

	for i := range rf.peers {
		if i != leaderID {
			rf.setAppendEntries(i, term, leaderID)
		}
	}
}

func (rf *Raft) setAppendEntries(i int, term int, leaderID int) {
	go func(server int) {
		for !rf.killed() {
			start := time.Now()

			toSend := false
			nextIndex := 0
			prevLogIndex := 0
			prevLogTerm := -1
			var entries []Entry

			rf.mu.Lock()
			rf.lastTime = start
			if rf.status != Leader {
				rf.mu.Unlock()
				return
			}
			leaderCommit := rf.commitIndex

			if rf.leaderStat.matchIndex[server]+1 < len(rf.ps.logEntries) &&
				rf.leaderStat.nextIndex[server] < len(rf.ps.logEntries) {
				toSend = true
				nextIndex = rf.leaderStat.nextIndex[server]
				tmpEntries := rf.ps.logEntries[nextIndex:]
				entries = make([]Entry, len(tmpEntries))
				copy(entries, tmpEntries)
				prevLogIndex = nextIndex - 1
				Assert(prevLogIndex >= 0, "prevLogTerm can't be less than zero")
				prevLogTerm = rf.ps.logEntries[prevLogIndex].Term
			}
			rf.mu.Unlock()

			args := AppendEntriesArgs{term, leaderID, prevLogIndex,
				prevLogTerm, entries, leaderCommit}
			reply := AppendEntriesReply{}
			connectionOk := rf.sendRPC(server, "Raft.AppendEntries", &args, &reply)

			if !connectionOk {
				// connection failed
			} else if toSend {
				rf.mu.Lock()
				if reply.Success {
					for index := prevLogIndex + 1; index < prevLogIndex+1+len(entries); index++ {
						rf.leaderStat.entriesSend[index]++
						if index == rf.commitIndex+1 &&
							float32(rf.leaderStat.entriesSend[index])/float32(len(rf.peers)) > EntryCommitRatio {
							rf.commitIndex++
							rf.apply(index)
						}
					}
					rf.leaderStat.nextIndex[server] += len(entries)
					rf.leaderStat.matchIndex[server] = rf.leaderStat.nextIndex[server] - 1
					DLog(LeaderInfo, rf.me, "success send appendEntries to", server)
				} else {
					DLog(LeaderInfo, rf.me, "fail to send appendEntries to", server)
					if reply.Term > rf.ps.currentTerm {
						rf.status = Follower
					} else {
						rf.leaderStat.nextIndex[server]--
						Assert(rf.leaderStat.nextIndex[server] >= 1, "nextIndex can't be less than 1")

						// if send AppendEntries failed, retry immediately
						rf.mu.Unlock()
						continue
					}
				}
				rf.mu.Unlock()
			} else {
				// heart beat
				if !reply.Success && reply.Term > term {
					rf.mu.Lock()
					if rf.status == Leader {
						rf.status = Follower
						rf.ps.currentTerm = reply.Term
						rf.ps.votedFor = -1
						DLog(LeaderInfo, "term", term, "leader", rf.me, "transit to follower from", server, "with term", reply.Term)
					}
					rf.mu.Unlock()
				} else {
					rf.mu.RLock()
					DLog(LeaderInfo, rf.me, "success send heartbeat to", server)
					rf.mu.RUnlock()
				}
			}

			end := time.Now()
			diff := end.Sub(start).Milliseconds()
			sleepTime := time.Duration(heartBeatMs-diff) * time.Millisecond

			if sleepTime > 0 {
				time.Sleep(sleepTime)
			}
		}
	}(i)
}

func (rf *Raft) sendRPC(server int, method string, args interface{}, reply interface{}, indefiniteRetry_opt ...bool) bool {
	ok := false
	indefiniteRetry := false
	if len(indefiniteRetry_opt) > 0 {
		indefiniteRetry = indefiniteRetry_opt[0]
	}
	begin := time.Now()

	rf.mu.Lock()
	peer := rf.peers[server]
	me := rf.me
	rf.mu.Unlock()

	for !ok {
		ok = peer.Call(method, args, reply)
		if !indefiniteRetry && time.Now().Sub(begin) > maxRPCRetryMs*time.Millisecond {
			DLog(RPCInfo, me, "failed to connect", server, "method:", method, "args:", args)
			return false
		}
	}

	DLog(RPCInfo, me, "connect to", server, "method", method, "args:", args, "reply:", reply)
	return true
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		lastTime := rf.lastTime
		DLog(TickerInfo, rf.me, "ticks with last time:", lastTime)
		rf.mu.Unlock()

		if time.Now().Sub(lastTime) > maxTickerMs*time.Millisecond {
			r := rand.Intn(maxVoteMs - minVoteMs)
			sleepTime := time.Duration(r+minVoteMs) * time.Millisecond
			go rf.vote(sleepTime)
			time.Sleep(sleepTime)
		} else {
			time.Sleep(tickerSleepMs * time.Millisecond)
		}
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
	rf.ps.logEntries = append(rf.ps.logEntries, Entry{})

	rf.msgCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
