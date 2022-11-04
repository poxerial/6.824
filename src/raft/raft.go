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
	"6.824/labgob"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	voteSuccessRatio    = 0.5
	EntryCommitRatio    = 0.5
	maxTickerMs         = 300
	tickerSleepMs       = 150
	maxVoteMs           = 300
	minVoteMs           = 0
	voteCallSleepMs     = 20
	heartBeatMs         = 100
	RPCTimeOutMs        = 50
	RPCSleepMs          = 10
	applySleepMs        = 20
	maxAppendEntriesLen = 30
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
	entriesSend  []int
	allSendIndex int
	nextIndex    []int
	matchIndex   []int
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

	// persistent state
	ps persistentState

	// volatile state
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine
	leaderStat  *leaderState

	// other state
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
func (rf *Raft) persist(snapshot ...[]byte) {
	// Your code here (2C).
	Assert(len(snapshot) < 2, "persist receives 1 arg at most.")
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	rf.mu.RLock()
	logs := make([]Entry, len(rf.ps.logEntries))
	copy(logs, rf.ps.logEntries)
	term := rf.ps.currentTerm
	voteFor := rf.ps.votedFor
	lastIndex := rf.ps.lastIndex
	lastTerm := rf.ps.lastTerm
	rf.mu.RUnlock()

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
	rf.mu.Unlock()

	DLog(SnapshotInfo, me, "starts to snapshot, last included index", index)
	rf.persist(snapshot)
	DLog(SnapshotInfo, me, "snapshots successfully")
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
	rf.mu.Lock()
	rf.lastTime = time.Now()
	if args.Term < rf.ps.currentTerm {
		reply.Term = rf.ps.currentTerm
		rf.mu.Unlock()
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

	if len(rf.ps.logEntries)+rf.ps.lastIndex+1 > args.LastIndex+1 && rf.ps.lastIndex+1 < args.LastIndex+1 &&
		rf.ps.logEntries[args.LastIndex].Term == args.LastTerm {
		rf.ps.logEntries = rf.ps.logEntries[args.LastIndex+1:]
	} else {
		rf.ps.logEntries = make([]Entry, 0)
		rf.ps.lastTerm = args.LastTerm
		rf.ps.lastIndex = args.LastIndex
	}

	if rf.lastApplied < rf.ps.lastIndex {
		rf.lastApplied = rf.ps.lastIndex
		rf.commitIndex = rf.ps.lastIndex
	}

	rf.mu.Unlock()

	if args.Data == nil {
		rf.persist()
		return
	}

	rf.persist(args.Data)

	msg := ApplyMsg{}
	msg.SnapshotValid = true
	msg.Snapshot = make([]byte, len(args.Data))
	copy(msg.Snapshot, args.Data)
	msg.SnapshotTerm = args.LastTerm
	msg.SnapshotIndex = args.LastIndex

	DLog(SnapshotInfo, rf.me, "install snapshot with index", msg.SnapshotIndex, "term", msg.SnapshotTerm)
	rf.msgCh <- msg
}

func (rf *Raft) sendInstallSnapshot(server int) {
	rf.mu.RLock()
	args := InstallSnapshotArgs{}
	args.Term = rf.ps.currentTerm
	args.LastTerm = rf.ps.lastTerm
	args.LastIndex = rf.ps.lastIndex
	rf.mu.RUnlock()

	// rf.persister.snapshot may be written in Raft.persist()
	rf.persister.mu.Lock()
	args.Data = rf.persister.snapshot
	rf.persister.mu.Unlock()

	reply := InstallSnapshotReply{}
	rf.sendRPC(server, "Raft.InstallSnapshot", &args, &reply)

	rf.mu.Lock()
	if reply.Term > rf.ps.currentTerm {
		rf.transitToFollower(reply.Term)
	}
	rf.mu.Unlock()
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
	psChanged := false
	rf.mu.Lock()

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

	var lastTerm, lastIndex int
	if len(rf.ps.logEntries) == 0 {
		lastTerm = rf.ps.lastTerm
		lastIndex = rf.ps.lastIndex
	} else {
		lastIndex = len(rf.ps.logEntries) + rf.ps.lastIndex
		lastTerm = rf.ps.logEntries[len(rf.ps.logEntries)-1].Term
	}

	if rf.status == Follower &&
		(rf.ps.votedFor == -1 || rf.ps.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastTerm ||
			args.LastLogTerm == lastTerm &&
				args.LastLogIndex >= lastIndex) {
		reply.VoteGranted = true
		rf.ps.votedFor = args.CandidateId
		psChanged = true
	}

	rf.lastTime = time.Now()
	rf.mu.Unlock()
	if psChanged {
		rf.persist()
	}
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

// need additional lock
func (rf *Raft) apply(index int) {
	msgs := make([]ApplyMsg, 0)
	rf.commitIndex = index
	DLog(ApplyInfo, "lastApplied", rf.lastApplied, "commitIndex", rf.commitIndex)
	for rf.lastApplied < rf.commitIndex {

		msg := ApplyMsg{}
		msg.CommandValid = true
		msg.CommandIndex = rf.lastApplied + 1
		msg.Command = rf.ps.logEntries[rf.lastApplied-rf.ps.lastIndex].Command

		msgs = append(msgs, msg)

		rf.lastApplied++
	}

	me := rf.me
	rf.mu.Unlock()
	for _, msg := range msgs {
		DLog(ApplyInfo, me, "start", msg)
		rf.msgCh <- msg
		DLog(ApplyInfo, me, "finish", msg)
	}
	rf.mu.Lock()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	psChanged := false
	defer func() {
		if psChanged {
			rf.persist()
		}
	}()
	rf.mu.Lock()
	defer rf.mu.Unlock()

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
		reply.Success = false
		return
	}
	if rf.status == Leader {
		Assert(reply.Term > args.Term, "there is only one leader in the same term.")
		return
	}

	// reply.Success
	prevLogIndex := args.PrevLogIndex - rf.ps.lastIndex - 1
	// if prevLog has been discarded, ask for InstallSnapshot
	if prevLogIndex < -1 {
		// ask for InstallSnapshot
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
		DLog(ApplyInfo, "follower", rf.me, "begin apply")
		rf.apply(rf.commitIndex)
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
		DLog(UserInfo, "leader", rf.me, "receive command", command)
	} else {
		// DLog(UserInfo, rf.me, "(not leader)", "receive command", command)
	}

	index := len(rf.ps.logEntries) + rf.ps.lastIndex
	term := rf.ps.currentTerm

	rf.mu.Unlock()

	if isLeader {
		// rf.ps has changed
		rf.persist()
	}

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
	rf.status = Candidate
	rf.ps.currentTerm++
	rf.ps.votedFor = rf.me
	term := rf.ps.currentTerm
	me := rf.me
	lastTime := rf.lastTime
	totalNum := len(rf.peers)
	index := len(rf.ps.logEntries) + rf.ps.lastIndex
	var lastLogTerm int
	if len(rf.ps.logEntries) == 0 {
		lastLogTerm = rf.ps.lastTerm
	} else {
		lastLogTerm = rf.ps.logEntries[len(rf.ps.logEntries)-1].Term
	}
	rf.mu.Unlock()

	rf.persist()

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
					if reply.Term > rf.ps.currentTerm {
						rf.ps.currentTerm = reply.Term
						rf.ps.votedFor = -1
					}
					rf.mu.Unlock()
					rf.persist()
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
func (rf *Raft) transitToFollower(term int) {
	rf.status = Follower
	rf.ps.currentTerm = term
	rf.ps.votedFor = -1

	rf.mu.Unlock()
	rf.persist()
	rf.mu.Lock()

	DLog(rf.me, "transit to follower with term", term)
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
			rf.setAppendEntries(i, leaderID)
		}
	}
	rf.setCommit()
}

func (rf *Raft) setCommit() {
	go func() {
		for !rf.killed() {
			start := time.Now()

			toCommit := false
			rf.mu.Lock()
			if rf.status != Leader {
				rf.mu.Unlock()
				return
			}
			index := rf.commitIndex
			for len(rf.ps.logEntries) > index-rf.ps.lastIndex &&
				float32(rf.leaderStat.entriesSend[index-rf.ps.lastIndex])/float32(len(rf.peers)) > EntryCommitRatio {
				toCommit = true
				index++
			}
			if toCommit && rf.ps.logEntries[index-rf.ps.lastIndex-1].Term == rf.ps.currentTerm {
				DLog(ApplyInfo, "leader", rf.me, "start apply.")
				rf.apply(index)
			}
			rf.mu.Unlock()

			end := time.Now()
			diff := end.Sub(start)
			sleepMs := applySleepMs - diff.Milliseconds()
			if sleepMs > 0 {
				time.Sleep(time.Duration(sleepMs) * time.Millisecond)
			}
		}
	}()
}

// need additional lock
func (rf *Raft) updateEntriesSend(server int, index int) {
	if index <= rf.ps.lastIndex {
		index = rf.ps.lastIndex + 1
	}
	for ; index < rf.leaderStat.nextIndex[server]; index++ {
		rf.leaderStat.entriesSend[index-rf.ps.lastIndex-1]++

		if rf.leaderStat.entriesSend[index-rf.ps.lastIndex-1] == len(rf.peers) {
			rf.leaderStat.allSendIndex = index
		}
	}
}

func (rf *Raft) setAppendEntries(i int, leaderID int) {
	go func(server int) {
		for !rf.killed() {
			psChanged := false
			start := time.Now()

			toSend := false
			var entries []Entry

			rf.mu.RLock()
			if rf.status != Leader {
				rf.mu.RUnlock()
				return
			}
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
				rf.mu.RUnlock()
				rf.sendInstallSnapshot(server)
				continue
			}
			if rf.leaderStat.matchIndex[server]-rf.ps.lastIndex < len(rf.ps.logEntries) &&
				rf.leaderStat.nextIndex[server]-rf.ps.lastIndex-1 < len(rf.ps.logEntries) {
				toSend = true
				tmpEntries := rf.ps.logEntries[nextIndex-rf.ps.lastIndex-1:]
				entries = make([]Entry, len(tmpEntries))
				copy(entries, tmpEntries)
			}
			rf.mu.RUnlock()

			args := AppendEntriesArgs{term, leaderID, prevLogIndex,
				prevLogTerm, entries, leaderCommit}
			reply := AppendEntriesReply{}
			connectionOk := rf.sendRPC(server, "Raft.AppendEntries", &args, &reply)

			if !connectionOk {
				// connection failed
				// DLog(LeaderInfo, leaderID, "can't connect to", server)
			} else {
				rf.mu.Lock()
				if reply.Success {
					if toSend {
						// not heart beat
						rf.leaderStat.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
						index := rf.leaderStat.matchIndex[server] + 1
						rf.leaderStat.matchIndex[server] = rf.leaderStat.nextIndex[server] - 1
						rf.updateEntriesSend(server, index)
						DLog(LeaderInfo, leaderID, "success send AppendEntries to", server)
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
							rf.mu.Unlock()
							rf.sendInstallSnapshot(server)
							rf.leaderStat.nextIndex[server] = lastIndex + 1
							rf.leaderStat.matchIndex[server] = lastIndex
							continue
						} else {
							for rf.leaderStat.nextIndex[server]-1-rf.ps.lastIndex-1 >= 0 && rf.ps.logEntries[rf.leaderStat.nextIndex[server]-1-rf.ps.lastIndex-1].Term >= reply.LastTerm {
								rf.leaderStat.nextIndex[server]--
							}
							if rf.leaderStat.nextIndex[server]-1-rf.ps.lastIndex-1 < 0 {
								rf.mu.Unlock()
								rf.sendInstallSnapshot(server)
								rf.leaderStat.nextIndex[server] = rf.ps.lastIndex + 1
								rf.leaderStat.matchIndex[server] = rf.ps.lastIndex
								continue
							}
							if rf.leaderStat.nextIndex[server]-1 > reply.LastIndex {
								rf.leaderStat.nextIndex[server] = reply.LastIndex
							}
						}
						Assert(rf.leaderStat.nextIndex[server] >= 1, "nextIndex can't be less than 1")

						// if fail to send AppendEntries, retry immediately
						rf.mu.Unlock()
						continue
					}
				}
				rf.mu.Unlock()
			}
			if psChanged {
				rf.persist()
			}

			end := time.Now()
			diff := end.Sub(start).Milliseconds()
			sleepTimeMs := heartBeatMs - diff

			if sleepTimeMs > 0 {
				time.Sleep(time.Duration(sleepTimeMs) * time.Millisecond)
			}
		}
	}(i)
}

func (rf *Raft) sendRPC(server int, method string, args interface{}, reply interface{}) bool {

	start := time.Now()
	rf.mu.RLock()
	peer := rf.peers[server]
	me := rf.me
	rf.mu.RUnlock()

	ch := make(chan bool)
	go func() {
		ch <- peer.Call(method, args, reply)
	}()

	var ok bool
	for {
		select {
		case ok = <-ch:
			if !ok {
				// DLog(RPCInfo, me, "failed to connect", server, "method:", method, "args:", args)
			} else {
				DLog(RPCInfo, me, "connect to", server, "method", method, "args:", args, "reply:", reply)
			}
			return ok
		default:
			time.Sleep(RPCSleepMs * time.Millisecond)
			if time.Now().Sub(start).Milliseconds() > RPCTimeOutMs {
				//DLog(RPCInfo, me, "failed to connect", server, "method:", method, "args:", args)
				return false
			}
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
		rf.mu.Lock()
		lastTime := rf.lastTime
		DLog(TickerInfo, rf.me, "ticks with last time:", lastTime)
		status := rf.status
		rf.mu.Unlock()

		if time.Now().Sub(lastTime) > maxTickerMs*time.Millisecond && status == Follower {
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
	rf.ps.lastIndex = -1

	rf.msgCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
