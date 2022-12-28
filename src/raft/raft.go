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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	voteSuccessRatio = 0.5
	EntryCommitRatio = 0.5
	maxTickerMs      = 130
	tickerSleepMs    = 10
	maxVoteMs        = 100
	minVoteMs        = 55
	voteCallSleepMs  = 5
	HeartBeatMs      = 100
	RPCTimeOutMs     = 50
	RPCMAXRetryTimes = 3
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

type Entry struct {
	Term    int
	Command interface{}
}

type persistentState struct {
	votedFor    int
	currentTerm int
	logEntries  []Entry

	// For 2D:
	lastTerm  int
	lastIndex int
}

type leaderState struct {
	entriesSend     []int
	allSendIndex    int
	nextIndex       []int
	matchIndex      []int
	lastConnectTime []time.Time
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu      DLock      // Lock to protect shared access to this peer's state
	applyMu sync.Mutex // Lock to prevent InstallSnapshot when sending apply message
	// Lock order: applyMu -> mu
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state
	ps persistentState

	// volatile state
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine
	leaderStat  *leaderState

	// other state
	status   int
	lastTime time.Time
	applyCh  chan bool

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
// need RLock
func (rf *Raft) persist(snapshot ...[]byte) {
	// Your code here (2C).
	Assert(len(snapshot) < 2, "persist receives 1 arg at most.")
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	logs := make([]Entry, len(rf.ps.logEntries))
	copy(logs, rf.ps.logEntries)
	term := rf.ps.currentTerm
	voteFor := rf.ps.votedFor
	lastIndex := rf.ps.lastIndex
	lastTerm := rf.ps.lastTerm

	err1 := e.Encode(logs)
	err2 := e.Encode(term)
	err3 := e.Encode(voteFor)
	err4 := e.Encode(lastIndex)
	err5 := e.Encode(lastTerm)

	if err1 != nil || err2 != nil || err3 != nil || err4 != nil || err5 != nil {
		panic("Failed to encode persistent state.")
	}

	data := w.Bytes()
	if len(snapshot) == 1 {
		rf.persister.SaveStateAndSnapshot(data, snapshot[0])
	} else {
		rf.persister.SaveRaftState(data)
	}
	DLog(PersistInfo, rf.me, "saves persist state.")
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var logs []Entry
	var term int
	var voteFor int
	var lastIndex int
	var lastTerm int
	if d.Decode(&logs) != nil || d.Decode(&term) != nil || d.Decode(&voteFor) != nil ||
		d.Decode(&lastIndex) != nil || d.Decode(&lastTerm) != nil {
		panic("Failed to decode persistent state.")
	} else {
		rf.ps.logEntries = make([]Entry, len(logs))
		copy(rf.ps.logEntries, logs)
		rf.ps.currentTerm = term
		rf.ps.votedFor = voteFor
		rf.ps.lastIndex = lastIndex
		rf.ps.lastTerm = lastTerm
	}
	if rf.ps.lastIndex >= 0 {
		rf.lastApplied = rf.ps.lastIndex
		rf.commitIndex = rf.ps.lastIndex
	}
	DLog(PersistInfo, rf.me, "reads persist state.")
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
	rf.mu.Lock()
	Assert(index <= rf.commitIndex, "snapshot includes uncommitted entry.")
	DLog(SnapshotInfo, rf.me, "start to snapshot:", index, "/", rf.commitIndex)
	if rf.ps.lastIndex >= index {
		rf.mu.Unlock()
		return
	}
	rf.ps.lastTerm = rf.ps.logEntries[index-rf.ps.lastIndex-1].Term

	if index-rf.ps.lastIndex < len(rf.ps.logEntries) {
		rf.ps.logEntries = rf.ps.logEntries[index-rf.ps.lastIndex:]
		if rf.status == Leader {
			rf.leaderStat.entriesSend = rf.leaderStat.entriesSend[index-rf.ps.lastIndex:]
		}
	} else {
		rf.ps.logEntries = make([]Entry, 0)
		if rf.status == Leader {
			rf.leaderStat.entriesSend = make([]int, 0)
		}
	}
	rf.ps.lastIndex = index
	me := rf.me

	DLog(SnapshotInfo, me, "starts to snapshot, last included index", index)
	rf.persist(snapshot)
	DLog(SnapshotInfo, me, "snapshots successfully")
	rf.mu.Unlock()
}

type InstallSnapshotArgs struct {
	Term int
	//LeaderId  int
	LastIndex int
	LastTerm  int
	//Offset    int
	Data []byte
	//Done      bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.applyMu.Lock()
	rf.mu.Lock()
	defer rf.applyMu.Unlock()
	defer rf.mu.Unlock()

	rf.lastTime = time.Now()
	if args.Term < rf.ps.currentTerm {
		reply.Term = rf.ps.currentTerm
		return
	} else if args.Term > rf.ps.currentTerm {
		if rf.status == Leader {
			rf.transitToFollower(args.Term)
		} else {
			rf.ps.currentTerm = args.Term
			rf.ps.votedFor = -1
			rf.status = Follower
		}
	}
	reply.Term = rf.ps.currentTerm

	if args.LastIndex <= rf.ps.lastIndex {
		return
	}

	if len(rf.ps.logEntries)+rf.ps.lastIndex+1 > args.LastIndex+1 && rf.ps.lastIndex+1 < args.LastIndex+1 &&
		rf.ps.logEntries[args.LastIndex-rf.ps.lastIndex-1].Term == args.LastTerm {
		rf.ps.logEntries = rf.ps.logEntries[args.LastIndex-rf.ps.lastIndex:]
	} else {
		rf.ps.logEntries = make([]Entry, 0)
	}

	rf.ps.lastIndex = args.LastIndex
	rf.ps.lastTerm = args.LastTerm
	rf.lastApplied = args.LastIndex
	rf.commitIndex = args.LastIndex

	rf.persist(args.Data)

	if args.Data == nil {
		return
	}

	msg := ApplyMsg{}
	msg.SnapshotValid = true
	msg.Snapshot = make([]byte, len(args.Data))
	copy(msg.Snapshot, args.Data)
	msg.SnapshotTerm = args.LastTerm
	msg.SnapshotIndex = args.LastIndex

	DLog(SnapshotInfo, rf.me, "install snapshot with index", msg.SnapshotIndex, "term", msg.SnapshotTerm)
	rf.msgCh <- msg
}

// need additional lock
func (rf *Raft) sendInstallSnapshot(server int) {
	args := InstallSnapshotArgs{}
	args.Term = rf.ps.currentTerm
	args.LastTerm = rf.ps.lastTerm
	args.LastIndex = rf.ps.lastIndex

	// rf.persister.snapshot may be written in Raft.persist()
	args.Data = rf.persister.snapshot

	reply := InstallSnapshotReply{}

	rf.mu.Unlock()
	rf.sendRPC(server, "Raft.InstallSnapshot", &args, &reply)
	rf.mu.Lock()

	if reply.Term > rf.ps.currentTerm {
		rf.transitToFollower(reply.Term)
	}
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
	psChanged := false
	defer func() {
		if psChanged {
			rf.persist()
		}
	}()

	if args.Term > rf.ps.currentTerm {
		if rf.status == Leader {
			rf.transitToFollower(args.Term)
		} else {
			rf.status = Follower
			rf.ps.currentTerm = args.Term
			rf.ps.votedFor = -1
			psChanged = true
		}
	}
	reply.Term = rf.ps.currentTerm

	var lastTerm, lastIndex int
	if len(rf.ps.logEntries) == 0 {
		lastTerm = rf.ps.lastTerm
		lastIndex = rf.ps.lastIndex
	} else {
		lastIndex = len(rf.ps.logEntries) + rf.ps.lastIndex
		lastTerm = rf.ps.logEntries[len(rf.ps.logEntries)-1].Term
	}

	if args.LastLogTerm > lastTerm ||
		args.LastLogTerm == lastTerm &&
			args.LastLogIndex >= lastIndex {
		rf.lastTime = time.Now()
		if (rf.ps.votedFor == -1 || rf.ps.votedFor == args.CandidateId) &&
			args.Term == rf.ps.currentTerm {
			reply.VoteGranted = true
			rf.status = Follower
			rf.ps.votedFor = args.CandidateId
			psChanged = true
		}
	}

	DLog(FollowerInfo, rf.me, "receive Request Vote", args, "and reply", reply)
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// optimize the number of rejected AppendEntries RPCs
	LastTerm  int
	LastIndex int
}

func (rf *Raft) setApply() {
	rf.applyCh = make(chan bool)
	deadCh := make(chan bool, 1)
	go func() {
		for !rf.killed() {
			time.Sleep(HeartBeatMs * time.Millisecond)
		}
		deadCh <- true
	}()
	go func() {
		DLog(ApplyInfo, rf.me, "GoID:", GoID())
		for {
			select {
			case <-rf.applyCh:
				rf.applyMu.Lock()
				rf.mu.Lock()
				if rf.lastApplied == rf.commitIndex {
					rf.applyMu.Unlock()
					rf.mu.Unlock()
					continue
				}

				DLog(ApplyInfo, rf.me, "start to apply, lastApplied", rf.lastApplied, "commitIndex", rf.commitIndex)
				for rf.lastApplied < rf.commitIndex {

					msg := ApplyMsg{}
					msg.CommandValid = true
					msg.CommandIndex = rf.lastApplied + 1
					msg.Command = rf.ps.logEntries[rf.lastApplied-rf.ps.lastIndex].Command

					rf.mu.Unlock()
					DLog(ApplyInfo, rf.me, "apply", msg)
					rf.msgCh <- msg
					DLog(ApplyInfo, rf.me, "finish apply", msg)
					rf.mu.Lock()

					rf.lastApplied++

					if rf.commitIndex > len(rf.ps.logEntries)+rf.ps.lastIndex {
						rf.commitIndex = len(rf.ps.logEntries) + rf.ps.lastIndex
					}
				}
				index := rf.commitIndex
				rf.mu.Unlock()
				rf.applyMu.Unlock()

				DLog(ApplyInfo, rf.me, "finish apply to", index)
			case <-deadCh:
				return
			}
		}

	}()
}

func (rf *Raft) apply() {
	rf.mu.Unlock()
	rf.applyCh <- true
	rf.mu.Lock()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	psChanged := false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		if psChanged {
			rf.persist()
		}
	}()

	if args.Term > rf.ps.currentTerm {
		if rf.status == Leader {
			rf.transitToFollower(args.Term)
		} else {
			rf.ps.currentTerm = args.Term
			rf.ps.votedFor = -1
			rf.status = Follower
			psChanged = true
		}
	}
	reply.Term = rf.ps.currentTerm
	if args.Term < rf.ps.currentTerm {
		return
	}
	if rf.status == Leader {
		Assert(rf.ps.currentTerm != args.Term, "there is only one leader in the same term.")
		return
	}

	// reply.Success
	prevLogIndex := args.PrevLogIndex - rf.ps.lastIndex - 1
	if prevLogIndex < -1 {
		// if prevLog has been discarded, ask for InstallSnapshot
		reply.LastIndex = -1
		return
	}
	if prevLogIndex == -1 {
		reply.Success = rf.ps.lastTerm == args.PrevLogTerm
	} else if prevLogIndex >= len(rf.ps.logEntries) ||
		rf.ps.logEntries[prevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		DLog(FollowerInfo, rf.me, "rejects AppendEntries", "at", args.PrevLogIndex)
		if prevLogIndex < len(rf.ps.logEntries) {
			DLog(FollowerInfo, "conflict term", rf.ps.logEntries[args.PrevLogIndex-rf.ps.lastIndex-1].Term)

			// optimize the number of rejected AppendEntries RPCs
			reply.LastTerm = rf.ps.logEntries[prevLogIndex].Term
			reply.LastIndex = prevLogIndex
			for reply.LastIndex > 2 && rf.ps.logEntries[reply.LastIndex-1].Term == reply.LastTerm {
				reply.LastIndex--
			}
			rf.ps.logEntries = rf.ps.logEntries[:prevLogIndex]
			psChanged = true
			reply.LastIndex += rf.ps.lastIndex + 1
		} else {
			DLog(FollowerInfo, "logs length", len(rf.ps.logEntries)+rf.ps.lastIndex)
			reply.LastIndex = len(rf.ps.logEntries) + rf.ps.lastIndex + 1
			reply.LastTerm = -1
		}
	} else {
		reply.Success = true
	}

	if reply.Success {
		for i := range args.Entries {
			if i+prevLogIndex+1 >= len(rf.ps.logEntries) {
				rf.ps.logEntries = append(rf.ps.logEntries, args.Entries[i])
			} else {
				rf.ps.logEntries[i+prevLogIndex+1] = args.Entries[i]
			}
			psChanged = true
		}
	}
	DLog(FollowerInfo, rf.me, "receive AppendEntries", args, "from", args.LeaderId, "and reply", reply)

	if reply.Success && args.LeaderCommit > rf.commitIndex {
		lastNewEntryIndex := len(rf.ps.logEntries) + rf.ps.lastIndex
		if args.LeaderCommit > lastNewEntryIndex {
			rf.commitIndex = lastNewEntryIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.apply()
	}

	rf.lastTime = time.Now()
	reply.Term = rf.ps.currentTerm
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

	isLeader := rf.status == Leader

	// Your code here (2B).
	if isLeader {
		entry := Entry{rf.ps.currentTerm, command}
		rf.ps.logEntries = append(rf.ps.logEntries, entry)
		rf.leaderStat.entriesSend = append(rf.leaderStat.entriesSend, 1)
		rf.broadcastAppendEntries(false)
		DLog(UserInfo, "leader", rf.me, "receive command", command)
	} else {
		// DLog(UserInfo, rf.me, "(not leader)", "receive command", command)
	}

	index := len(rf.ps.logEntries) + rf.ps.lastIndex
	term := rf.ps.currentTerm

	if isLeader {
		// rf.ps has changed
		rf.persist()
	}
	rf.mu.Unlock()

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

func (rf *Raft) vote(voteMaxTime time.Duration) {
	// avoid starting vote simultaneously
	time.Sleep(time.Duration(rand.Int()%10) * time.Millisecond)

	rf.mu.Lock()
	if time.Since(rf.lastTime) < time.Duration(maxTickerMs)*time.Millisecond {
		rf.mu.Unlock()
		return
	}
	rf.status = Candidate
	rf.ps.currentTerm++
	rf.ps.votedFor = rf.me
	term := rf.ps.currentTerm
	me := rf.me
	totalNum := len(rf.peers)
	index := len(rf.ps.logEntries) + rf.ps.lastIndex
	var lastLogTerm int
	if len(rf.ps.logEntries) == 0 {
		lastLogTerm = rf.ps.lastTerm
	} else {
		lastLogTerm = rf.ps.logEntries[len(rf.ps.logEntries)-1].Term
	}

	rf.persist()
	rf.mu.Unlock()

	DLog(CandidateInfo, me, "start vote, time duration:", voteMaxTime)
	start := time.Now()

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
					if reply.Term > rf.ps.currentTerm {
						rf.ps.currentTerm = reply.Term
						rf.ps.votedFor = -1
						if rf.status == Leader {
							rf.transitToFollower(rf.ps.currentTerm)
						}
					}
					rf.persist()
					rf.mu.Unlock()
				}
				callNum.Add(1)
			}(i)
		}
	}

	for !voteFail.Load() && time.Since(start) < voteMaxTime {
		rf.mu.Lock()
		if float32(voteGrantedNum.Load())/float32(totalNum) >= voteSuccessRatio && rf.status == Candidate {
			terminate := time.Since(start) > voteMaxTime
			if !terminate {
				rf.initializeLeader()
				rf.mu.Unlock()
				DLog(LeaderInfo, "new leader", me, "term", term)
				return
			}
		}
		rf.mu.Unlock()
		time.Sleep(voteCallSleepMs * time.Millisecond)
	}
	rf.mu.Lock()
	// fail
	DLog(CandidateInfo, rf.me, "election failed,", "callNum:", callNum.Load(), "voteGrantedNum:", voteGrantedNum.Load(),
		"voteFail:", voteFail.Load(), "total:", totalNum)
	rf.status = Follower
	now := time.Now()
	diff := now.Sub(start)
	var sleepMs int64
	if voteMaxTime.Milliseconds() > diff.Milliseconds() {
		sleepMs = voteMaxTime.Milliseconds() - diff.Milliseconds()
	}
	rf.mu.Unlock()
	if sleepMs > 0 {
		time.Sleep(time.Duration(sleepMs) * time.Millisecond)
	}
}

// need additional lock
func (rf *Raft) transitToFollower(term int) {
	rf.status = Follower
	rf.ps.currentTerm = term
	rf.ps.votedFor = -1

	rf.persist()

	DLog(LeaderInfo, rf.me, "transit to follower with term", term)
}

// need additional lock
func (rf *Raft) initializeLeader() {
	rf.status = Leader
	DLog(LeaderInfo, "Leader", rf.me, "initializing.")

	// initialize leaderStat
	rf.leaderStat = &leaderState{}
	rf.leaderStat.nextIndex = make([]int, len(rf.peers))
	rf.leaderStat.matchIndex = make([]int, len(rf.peers))
	rf.leaderStat.entriesSend = make([]int, len(rf.ps.logEntries))
	rf.leaderStat.lastConnectTime = make([]time.Time, len(rf.peers))
	for i := range rf.leaderStat.entriesSend {
		rf.leaderStat.entriesSend[i] = 1
	}
	for i := range rf.peers {
		rf.leaderStat.nextIndex[i] = len(rf.ps.logEntries) + rf.ps.lastIndex + 1
		rf.leaderStat.matchIndex[i] = rf.ps.lastIndex
	}

	leaderID := rf.me

	for i := range rf.peers {
		if i != leaderID {
			rf.setHeartbeat(i, leaderID)
		}
	}
}

// need additional lock
func (rf *Raft) checkCommit() {
	toCommit := false
	if rf.status != Leader {
		return
	}
	for len(rf.ps.logEntries) > rf.commitIndex-rf.ps.lastIndex &&
		float32(rf.leaderStat.entriesSend[rf.commitIndex-rf.ps.lastIndex])/float32(len(rf.peers)) > EntryCommitRatio {
		toCommit = true
		rf.commitIndex++
	}
	if toCommit && rf.ps.logEntries[rf.commitIndex-rf.ps.lastIndex-1].Term == rf.ps.currentTerm {
		DLog(ApplyInfo, GoID(), "leader", rf.me, "start apply", "last applied:", rf.lastApplied, "commitIndex", rf.commitIndex)
		rf.apply()
		rf.broadcastAppendEntries(false)
	}
}

// need additional lock
func (rf *Raft) updateEntriesSend(server int, index int) {
	if index <= rf.ps.lastIndex {
		index = rf.ps.lastIndex + 1
	}
	for ; index <= rf.leaderStat.matchIndex[server]; index++ {
		rf.leaderStat.entriesSend[index-rf.ps.lastIndex-1]++

		if rf.leaderStat.entriesSend[index-rf.ps.lastIndex-1] == len(rf.peers) {
			rf.leaderStat.allSendIndex = index
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, leaderID int, notHeartBeat bool) {
	psChanged := false
	toSend := false
	var entries []Entry

	rf.mu.Lock()
	if rf.status != Leader {
		rf.mu.Unlock()
		return
	}
	rf.leaderStat.lastConnectTime[server] = time.Now()
	term := rf.ps.currentTerm
	leaderCommit := rf.commitIndex
	nextIndex := rf.leaderStat.nextIndex[server]
	prevLogIndex := nextIndex - 1
	Assert(prevLogIndex >= 0, "prevLogIndex can't be less than zero")
	var prevLogTerm int
	if prevLogIndex == rf.ps.lastIndex {
		prevLogTerm = rf.ps.lastTerm
	} else if prevLogIndex > rf.ps.lastIndex {
		prevLogTerm = rf.ps.logEntries[prevLogIndex-rf.ps.lastIndex-1].Term
	} else {
		rf.leaderStat.nextIndex[server] = rf.ps.lastIndex + 1
		rf.leaderStat.matchIndex[server] = rf.ps.lastIndex
		DLog(SnapshotInfo, "send install snapshot before append entries")
		rf.sendInstallSnapshot(server)
		rf.mu.Unlock()
		rf.sendAppendEntries(server, leaderID, true)
		return
	}
	if rf.leaderStat.matchIndex[server]-rf.ps.lastIndex < len(rf.ps.logEntries) &&
		rf.leaderStat.nextIndex[server]-rf.ps.lastIndex-1 < len(rf.ps.logEntries) {
		toSend = true
		tmpEntries := rf.ps.logEntries[nextIndex-rf.ps.lastIndex-1:]
		entries = make([]Entry, len(tmpEntries))
		copy(entries, tmpEntries)
	}
	rf.mu.Unlock()

	if !toSend && notHeartBeat {
		return
	}

	args := AppendEntriesArgs{term, leaderID, prevLogIndex,
		prevLogTerm, entries, leaderCommit}
	reply := AppendEntriesReply{}
	connectionOk := rf.sendRPC(server, "Raft.AppendEntries", &args, &reply)

	if !connectionOk {
		// connection failed
		// DLog(LeaderInfo, leaderID, "can't connect to", server)
	} else {
		rf.mu.Lock()
		rf.leaderStat.lastConnectTime[server] = time.Now()
		if reply.Success {
			if toSend {
				// not heart beat
				nextIndex = args.PrevLogIndex + len(args.Entries) + 1
				if nextIndex > rf.leaderStat.nextIndex[server] {
					rf.leaderStat.nextIndex[server] = nextIndex
				}

				matchIndex := nextIndex - 1;
				if matchIndex > rf.leaderStat.matchIndex[server] {
					index := rf.leaderStat.matchIndex[server] + 1
					rf.leaderStat.matchIndex[server] = matchIndex
					rf.updateEntriesSend(server, index)
					rf.checkCommit()
				}

				DLog(LeaderInfo, leaderID, "success send AppendEntries to", server,
					"nextIndex:", rf.leaderStat.nextIndex[server],
					"matchIndex:", rf.leaderStat.matchIndex[server])
			} else {
				DLog(LeaderInfo, leaderID, "success send heartbeat to", server)
			}
		} else {
			DLog(LeaderInfo, leaderID, "fail to send AppendEntries/heartbeat to", server)
			if reply.Term > rf.ps.currentTerm {
				if rf.status == Leader {
					rf.transitToFollower(reply.Term)
				}
				rf.ps.currentTerm = reply.Term
				rf.ps.votedFor = -1
				psChanged = true
			} else if args.Term == reply.Term {
				// follower log at prevLogIndex is not consistent with leader's
				if reply.LastTerm == -1 {
					rf.leaderStat.nextIndex[server] = reply.LastIndex
				} else if reply.LastIndex == -1 {
					lastIndex := rf.ps.lastIndex
					if rf.ps.lastIndex == -1 {
						// no snapshot to send
						rf.mu.Unlock()
						return
					}

					DLog(SnapshotInfo, "send install snapshot because reply.LastIndex == -1")
					rf.sendInstallSnapshot(server)
					rf.leaderStat.nextIndex[server] = lastIndex + 1
					rf.leaderStat.matchIndex[server] = lastIndex

					rf.mu.Unlock()

					rf.sendAppendEntries(server, leaderID, true)
					return
				} else {
					for rf.leaderStat.nextIndex[server]-1-rf.ps.lastIndex-1 >= 0 && rf.ps.logEntries[rf.leaderStat.nextIndex[server]-1-rf.ps.lastIndex-1].Term >= reply.LastTerm {
						rf.leaderStat.nextIndex[server]--
					}
					if rf.leaderStat.nextIndex[server]-1-rf.ps.lastIndex+1 < 0 {
						DLog(SnapshotInfo, "entries out of leader's rf.ps.logEntries")
						rf.sendInstallSnapshot(server)
						rf.leaderStat.nextIndex[server] = rf.ps.lastIndex + 1
						rf.leaderStat.matchIndex[server] = rf.ps.lastIndex
						rf.mu.Unlock()
						rf.sendAppendEntries(server, leaderID, true)
						return
					}
					if rf.leaderStat.nextIndex[server]-1 > reply.LastIndex {
						rf.leaderStat.nextIndex[server] = reply.LastIndex
					}
				}
				Assert(rf.leaderStat.nextIndex[server] >= 1, "nextIndex can't be less than 1")

				// if fail to send AppendEntries, retry immediately
				rf.mu.Unlock()
				rf.sendAppendEntries(server, leaderID, notHeartBeat)
				return
			}
		}
		if psChanged {
			rf.persist()
		}
		rf.mu.Unlock()
	}

}

func (rf *Raft) broadcastAppendEntries(notHeartBeat bool) {
	DLog(LeaderInfo, GoID(), "start to broadcast.")
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.sendAppendEntries(i, rf.me, notHeartBeat)
		}
	}
}

func (rf *Raft) setHeartbeat(i int, leaderID int) {
	go func(server int) {
		for !rf.killed() {
			rf.mu.RLock()
			isLeader := rf.status == Leader
			lastConnectTime := rf.leaderStat.lastConnectTime[i]
			rf.mu.RUnlock()

			if !isLeader {
				return
			}

			if time.Since(lastConnectTime).Milliseconds() > HeartBeatMs {
				rf.sendAppendEntries(i, leaderID, false)
			}

			sleepTimeMs := HeartBeatMs - time.Since(lastConnectTime).Milliseconds()

			if sleepTimeMs > 0 {
				time.Sleep(time.Duration(sleepTimeMs) * time.Millisecond)
			}
		}
	}(i)
}

func (rf *Raft) sendRPC(server int, method string, args interface{}, reply interface{}) bool {

	rf.mu.RLock()
	peer := rf.peers[server]
	me := rf.me
	rf.mu.RUnlock()

	start := time.Now()

	sendCH := make(chan bool, 1)
	go func() {
		ok := false

		for i := 0; i < RPCMAXRetryTimes && !ok; i++ {
			ok = peer.Call(method, args, reply)
		}
		sendCH <- ok
	}()

	returnCH := make(chan bool, 1)
	go func() {
		time.Sleep(RPCTimeOutMs * time.Millisecond)
		returnCH <- true
	}()

	var ok bool
	for {
		select {
		case ok = <-sendCH:
			if !ok {
				// DLog(RPCInfo, me, "failed to connect", server, "time cost:", time.Since(start).Milliseconds(), "ms, method:", method, "args:", args)
			} else {
				DLog(RPCInfo, me, "connect to", server, "method", method, "time cost", time.Since(start).Milliseconds(), "ms args:", args, "reply:", reply)
			}
			return ok
		case <-returnCH:
			DLog(RPCInfo, me, "failed to connect", server, "time cost:", "method:", method, time.Since(start).Milliseconds(), "ms, args:", args)
			return false
		}
	}

}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.RLock()
		lastTime := rf.lastTime
		status := rf.status
		rf.mu.RUnlock()
		DLog(TickerInfo, rf.me, "ticks with last time:", lastTime)

		if time.Now().Sub(lastTime) > maxTickerMs*time.Millisecond && status == Follower {
			r := rand.Intn(maxVoteMs - minVoteMs)
			sleepTime := time.Duration(r+minVoteMs) * time.Millisecond
			rf.vote(sleepTime)
			rf.mu.Lock()
			rf.lastTime = time.Now()
			rf.mu.Unlock()
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
	rf.lastTime = time.Unix(0, 0)
	rf.ps.currentTerm = 0
	rf.ps.logEntries = append(rf.ps.logEntries, Entry{})
	rf.ps.lastIndex = -1

	rf.msgCh = applyCh
	rf.setApply()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
