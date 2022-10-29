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
	voteSuccessRatio   = 0.5
	EntryCommitRatio   = 0.5
	maxTickerMs        = 300
	tickerSleepMs      = 100
	maxVoteMs          = 300
	minVoteMs          = 0
	voteCallSleepMs    = 20
	heartBeatMs        = 100
	RPCTimeOutMs       = 50
	RPCSleepMs         = 10
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

type Entry struct {
	Term    int
	Command interface{}
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	rf.mu.RLock()
	logs := make([]Entry, len(rf.ps.logEntries))
	copy(logs, rf.ps.logEntries)
	term := rf.ps.currentTerm
	voteFor := rf.ps.votedFor
	rf.mu.RUnlock()

	err1 := e.Encode(logs)
	err2 := e.Encode(term)
	err3 := e.Encode(voteFor)

	if err1 != nil || err2 != nil || err3 != nil {
		panic("Failed to encode persistent state.")
	}

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	if d.Decode(&logs) != nil || d.Decode(&term) != nil || d.Decode(&voteFor) != nil {
		panic("Failed to decode persistent state.")
	} else {
		rf.ps.logEntries = make([]Entry, len(logs))
		copy(rf.ps.logEntries, logs)
		rf.ps.currentTerm = term
		rf.ps.votedFor = voteFor
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
		rf.status = Follower
		rf.ps.currentTerm = args.Term
		rf.ps.votedFor = -1
		psChanged = true
	}

	if rf.status == Follower && (rf.ps.votedFor == -1 || rf.ps.votedFor == args.CandidateId) &&
		(args.LastLogTerm > rf.ps.logEntries[len(rf.ps.logEntries)-1].Term ||
			args.LastLogTerm == rf.ps.logEntries[len(rf.ps.logEntries)-1].Term &&
				args.LastLogIndex >= len(rf.ps.logEntries)-1) {
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
	for rf.lastApplied < index {
		// Apply(rf.ps.logEntries[rf.lastApplied + 1]) // apply command to state machine

		msg := ApplyMsg{Command: rf.ps.logEntries[rf.lastApplied+1].Command, CommandIndex: rf.lastApplied + 1, CommandValid: true}
		rf.msgCh <- msg
		DLog(ApplyInfo, rf.me, "send ApplyMsg", msg)

		rf.lastApplied++
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	psChanged := false
	rf.mu.Lock()

	if args.Term > rf.ps.currentTerm {
		rf.ps.currentTerm = args.Term
		rf.ps.votedFor = -1
		rf.status = Follower
		psChanged = true
	}
	reply.Term = rf.ps.currentTerm
	if rf.status == Leader {
		Assert(reply.Term > args.Term, "there is only one leader in the same term.")
		rf.mu.Unlock()
		return
	}

	if rf.ps.currentTerm > args.Term ||
		args.PrevLogIndex >= len(rf.ps.logEntries) ||
		rf.ps.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm {
		DLog(FollowerInfo, rf.me, "refuse AppendEntries", "at", args.PrevLogIndex)
		if args.PrevLogIndex < len(rf.ps.logEntries) {
			DLog(FollowerInfo, "conflict term", rf.ps.logEntries[args.PrevLogIndex].Term)
			reply.LastTerm = rf.ps.logEntries[args.PrevLogIndex].Term
			reply.LastIndex = args.PrevLogIndex
			for reply.LastIndex > 2 && rf.ps.logEntries[reply.LastIndex-1].Term == reply.LastTerm {
				reply.LastIndex--
			}
		} else {
			DLog(FollowerInfo, "logs length", len(rf.ps.logEntries))
			reply.LastIndex = len(rf.ps.logEntries)
			reply.LastTerm = -1
		}
		reply.Success = false

		if args.PrevLogIndex < len(rf.ps.logEntries) &&
			rf.ps.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm {
			rf.ps.logEntries = rf.ps.logEntries[:args.PrevLogIndex-1]
			psChanged = true
		}
	} else {
		reply.Success = true
	}

	if len(args.Entries) > 0 {
		if reply.Success {
			for i := range args.Entries {
				if i+args.PrevLogIndex+1 >= len(rf.ps.logEntries) {
					rf.ps.logEntries = append(rf.ps.logEntries, args.Entries[i])
				} else {
					rf.ps.logEntries[i+args.PrevLogIndex+1] = args.Entries[i]
				}
				psChanged = true
			}
		}
	}
	DLog(FollowerInfo, rf.me, "receive AppendEntries", args, "from", args.LeaderId, "and reply", reply)

	if reply.Success && args.LeaderCommit > rf.commitIndex {
		lastNewEntryIndex := len(rf.ps.logEntries) - 1
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
	rf.mu.Unlock()

	if psChanged {
		rf.persist()
	}
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

	index := len(rf.ps.logEntries) - 1
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
	index := len(rf.ps.logEntries) - 1
	lastLogTerm := rf.ps.logEntries[index].Term
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
func (rf *Raft) initializeLeader() {
	rf.status = Leader
	DLog(LeaderInfo, "Leader", rf.me, "initializing.")

	// initialize leaderStat
	rf.leaderStat = &leaderStatus{}
	rf.leaderStat.nextIndex = make([]int, len(rf.peers))
	rf.leaderStat.matchIndex = make([]int, len(rf.peers))
	rf.leaderStat.entriesSend = make([]int, len(rf.ps.logEntries))
	for i := range rf.leaderStat.entriesSend {
		rf.leaderStat.entriesSend[i] = 1
	}
	for i := range rf.peers {
		rf.leaderStat.nextIndex[i] = len(rf.ps.logEntries)
		rf.leaderStat.matchIndex[i] = 0
	}

	leaderID := rf.me

	for i := range rf.peers {
		if i != leaderID {
			rf.setAppendEntries(i, leaderID)
		}
	}
}

// need additional lock
func (rf *Raft) checkCommit(server int, index int) {
	for ; index < rf.leaderStat.nextIndex[server]; index++ {
		rf.leaderStat.entriesSend[index]++
		if index > rf.commitIndex && rf.ps.logEntries[index].Term == rf.ps.currentTerm &&
			float32(rf.leaderStat.entriesSend[index])/float32(len(rf.peers)) > EntryCommitRatio {
			toCommit := true
			for j := rf.commitIndex + 1; j < index; j++ {
				if float32(rf.leaderStat.entriesSend[j])/float32(len(rf.peers)) <= EntryCommitRatio {
					toCommit = false
				}
			}
			if toCommit {
				rf.commitIndex = index
				DLog(ApplyInfo, "leader", rf.me, "start apply")
				rf.apply(rf.commitIndex)
			}
		}
	}
}

func (rf *Raft) setAppendEntries(i int, leaderID int) {
	go func(server int) {
		for !rf.killed() {
			psChanged := false
			start := time.Now()

			toSend := false
			nextIndex := 0
			prevLogIndex := 0
			prevLogTerm := -1
			var entries []Entry

			rf.mu.RLock()
			if rf.status != Leader {
				rf.mu.RUnlock()
				return
			}
			term := rf.ps.currentTerm
			leaderCommit := rf.commitIndex
			nextIndex = rf.leaderStat.nextIndex[server]
			prevLogIndex = nextIndex - 1
			prevLogTerm = rf.ps.logEntries[prevLogIndex].Term

			if rf.leaderStat.matchIndex[server]+1 < len(rf.ps.logEntries) &&
				rf.leaderStat.nextIndex[server] < len(rf.ps.logEntries) {
				toSend = true
				tmpEntries := rf.ps.logEntries[nextIndex:]
				entries = make([]Entry, len(tmpEntries))
				copy(entries, tmpEntries)
				Assert(prevLogIndex >= 0, "prevLogTerm can't be less than zero")
			}
			rf.mu.RUnlock()

			args := AppendEntriesArgs{term, leaderID, prevLogIndex,
				prevLogTerm, entries, leaderCommit}
			reply := AppendEntriesReply{}
			connectionOk := rf.sendRPC(server, "Raft.AppendEntries", &args, &reply)

			if !connectionOk {
				// connection failed
				// DLog(LeaderInfo, leaderID, "can't connect to", server)
				// retry immediately
				continue
			} else {
				rf.mu.Lock()
				if reply.Success {
					if toSend {
						// not heart beat
						rf.leaderStat.nextIndex[server] += len(entries)
						index := rf.leaderStat.matchIndex[server] + 1
						rf.checkCommit(server, index)
						rf.leaderStat.matchIndex[server] = index - 1
						DLog(LeaderInfo, leaderID, "success send AppendEntries to", server)
					} else {
						DLog(LeaderInfo, leaderID, "success send heartbeat to", server)
					}
				} else {
					DLog(LeaderInfo, leaderID, "fail to send AppendEntries/heartbeat to", server)
					if reply.Term > rf.ps.currentTerm {
						if rf.status == Leader {
							rf.status = Follower
							DLog(LeaderInfo, "term", term, "leader", rf.me,
								"transit to follower from", server, "with term", reply.Term)
							rf.lastTime = time.Now()
						}
						rf.ps.currentTerm = reply.Term
						rf.ps.votedFor = -1
						psChanged = true
					} else if args.Term == reply.Term {
						// follower log at PrevLogIndex is not consistent with leader's
						if reply.LastTerm == -1 {
							rf.leaderStat.nextIndex[server] = reply.LastIndex
						} else {
							for rf.ps.logEntries[rf.leaderStat.nextIndex[server]-1].Term >= reply.LastTerm {
								rf.leaderStat.nextIndex[server]--
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

	rf.msgCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
