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
	//	"bytes"

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/prettydebug"
)

//
// as each Raft peer becomes aware that Successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int  //persist state
	votedFor    int  //persist state
	heartbeat   bool // true means receive heart beat
	state       string
	// 2B log repelication
	commitIndex   int
	lastApplied   int
	nextIndex     []int // index 1 point to first log, index 0 kept as a starting point for logic comparison, index 0 has term 0
	matchIndex    []int
	logTerm       []int // every server as a log term history //persist state
	cond          sync.Cond
	log           []interface{} //persist state
	applyCh       chan ApplyMsg
	snapshotIndex int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.state == leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logTerm)
	e.Encode(rf.log)
	e.Encode(rf.snapshotIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logTerm []int
	var log []interface{}
	var snapshotIndex int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logTerm) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&snapshotIndex) != nil {
		prettydebug.Debug(prettydebug.DError, "S%d failed reload persistent state", rf.me)
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logTerm = logTerm
		rf.log = log
		rf.snapshotIndex = snapshotIndex
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
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
	defer rf.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logTerm[index-rf.snapshotIndex:])
	e.Encode(rf.log[index-rf.snapshotIndex:])
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
	rf.logTerm = rf.logTerm[index-rf.snapshotIndex:]
	rf.log = rf.log[index-rf.snapshotIndex:]
	rf.snapshotIndex = index // every thing before index is removed from the log, but snapshot include terms in index
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateID int
	// 2B log repelication, leader election must examine log
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		prettydebug.Debug(prettydebug.DVote, "S%d Vote, refuse %d, Vote for %d", rf.me, args.CandidateID, rf.votedFor)
	} else { //args.Term >= rf.currentTerm
		if args.CandidateID == rf.votedFor || rf.votedFor == -1 || args.Term > rf.currentTerm {
			if args.LastLogTerm > rf.logTerm[len(rf.logTerm)-1] { // request more up to date
				reply.VoteGranted = true
				rf.state = follower
				rf.cond.Broadcast()
				rf.heartbeat = true
				rf.votedFor = args.CandidateID
				rf.persist()
				prettydebug.Debug(prettydebug.DVote, "S%d Vote, Vote for %d", rf.me, rf.votedFor)
			} else if args.LastLogTerm == rf.logTerm[len(rf.logTerm)-1] {
				if args.LastLogIndex >= len(rf.log)-1+rf.snapshotIndex { // request more up to date
					reply.VoteGranted = true
					rf.state = follower
					rf.cond.Broadcast()
					rf.heartbeat = true
					rf.votedFor = args.CandidateID
					rf.persist()
					prettydebug.Debug(prettydebug.DVote, "S%d Vote, Vote for %d", rf.me, rf.votedFor)
				} else { // request outdated, same log term, but request index is lower
					reply.VoteGranted = false
					prettydebug.Debug(prettydebug.DVote, "S%d Vote, same term, I have longer index", rf.me)
				}
			} else { // request outdated, request log term is lower
				prettydebug.Debug(prettydebug.DVote, "S%d Vote, I have longer term", rf.me)
				reply.VoteGranted = false
			}
		} else { // request term is same as server, but server already vote for someone else
			prettydebug.Debug(prettydebug.DVote, "S%d Vote, I vote for %d, term %d", rf.me, rf.votedFor, rf.currentTerm)
			reply.VoteGranted = false
		}
		reply.Term = args.Term
		if rf.currentTerm < args.Term {
			rf.currentTerm = args.Term
			rf.state = follower
			rf.cond.Broadcast()
			rf.persist()
		}

	}
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	// 2A heart beat implementation
	Term      int
	LearderID int
	// 2B log repelication
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []interface{}
	Terms        []int
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// MisMatchTerm           int //if log replication failed, return the term of the failed entry, if entry don't exist, return last term
	MisMatchTermStartIndex int //if log replication failed, return first index whose term failed the rpc, if entry don't exit, return first entry of the last term
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a < b {
		return b
	}
	return a
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if rf.currentTerm <= args.Term {
		if len(args.Entries) == 0 { // handle ping
			rf.currentTerm = args.Term
			rf.heartbeat = true
			rf.votedFor = args.LearderID // receive ping, only leader send ping message
			rf.persist()
			rf.state = follower
			rf.cond.Broadcast()
			reply.Term = args.Term
			reply.Success = true
			if args.LeaderCommit > rf.commitIndex {
				if (len(rf.logTerm)+rf.snapshotIndex >= args.PrevLogIndex+1) && (rf.logTerm[args.PrevLogIndex-rf.snapshotIndex] == args.PrevLogTerm) {
					rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex)
					rf.applyLog()
				}
			}
			prettydebug.Debug(prettydebug.DClient, "S%d is follower, leader: %d, term: %d", rf.me, args.LearderID, rf.currentTerm)
		} else { // handle log replication
			rf.currentTerm = args.Term
			rf.heartbeat = true
			rf.votedFor = args.LearderID // receive log replication, only leader send log replication message
			rf.persist()
			rf.state = follower
			rf.cond.Broadcast()
			reply.Term = args.Term
			// PrevLogIndex outbound existing log
			if len(rf.logTerm)+rf.snapshotIndex < args.PrevLogIndex+1 {
				reply.Success = false
				// reply.MisMatchTerm = rf.logTerm[len(rf.logTerm)-1]
				// for i := len(rf.logTerm) - 1; i > 0; i-- {
				// 	if rf.logTerm[i] != reply.MisMatchTerm {
				// 		reply.MisMatchTermStartIndex = i + 1
				// 		break
				// 	}
				// }
				reply.MisMatchTermStartIndex = len(rf.logTerm) + rf.snapshotIndex
				prettydebug.Debug(prettydebug.DClient, "S%d PrevLogIndex outbound existing log, log number: %d, rpc PrevLogIndex: %d, term: %d", rf.me, len(rf.logTerm), args.PrevLogIndex, rf.currentTerm)
			} else if rf.logTerm[args.PrevLogIndex-rf.snapshotIndex] != args.PrevLogTerm { // PrevLogTerm don't match
				reply.Success = false
				MisMatchTerm := rf.logTerm[args.PrevLogIndex-rf.snapshotIndex]
				for i := args.PrevLogIndex - 1 - rf.snapshotIndex; i >= 0; i-- {
					if rf.logTerm[i] != MisMatchTerm {
						reply.MisMatchTermStartIndex = i + 1
						break
					}
				}
				prettydebug.Debug(prettydebug.DClient, "S%d PrevLogIndex PrevLogTerm don't match, log term: %d, rpc PrevLogTerm: %d, current term: %d", rf.me, rf.logTerm[args.PrevLogIndex-rf.snapshotIndex], args.PrevLogTerm, rf.currentTerm)
				rf.log = rf.log[:args.PrevLogIndex-rf.snapshotIndex]         // this naturally excludes previous index
				rf.logTerm = rf.logTerm[:args.PrevLogIndex-rf.snapshotIndex] // this naturally excludes previous index
				rf.persist()
			} else { // every thing before match, append the new entries
				reply.Success = true
				rf.log = append(rf.log[:args.PrevLogIndex+1-rf.snapshotIndex], args.Entries...)
				rf.logTerm = append(rf.logTerm[:args.PrevLogIndex+1-rf.snapshotIndex], args.Terms...)
				rf.persist()
				prettydebug.Debug(prettydebug.DLog, "S%d appending log entries: %d", rf.me, args.PrevLogIndex+1)
				if args.LeaderCommit > rf.commitIndex {
					rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
					rf.applyLog()
				}
			}
		}
	} else { // outdated message
		reply.Success = false
		reply.Term = rf.currentTerm
		prettydebug.Debug(prettydebug.DLog, "S%d is refusing outdated appendEntries rpc from %d, term: %d", rf.me, args.LearderID, rf.currentTerm)
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	LastIncludedLog   interface{}
	Data              []byte
}

type InstallSnapshotReply struct {
	Term      int
	InstallOK bool
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm { //Reply immediately if term < currentTerm
		reply.Term = rf.currentTerm
		reply.InstallOK = false
		return
	}
	if args.LastIncludedIndex <= rf.snapshotIndex { //leader's snapshot lag behind servers
		reply.Term = rf.currentTerm
		reply.InstallOK = true
		return
	}
	if args.LastIncludedIndex > len(rf.log)-1+rf.snapshotIndex { //leader's snapshot contain every log server has, discard every log
		rf.log = rf.log[:0]
		rf.logTerm = rf.logTerm[:0]
		rf.log = append(rf.log, args.LastIncludedLog)
		rf.logTerm = append(rf.logTerm, args.LastIncludedTerm)
	} else if rf.logTerm[args.LastIncludedIndex-rf.snapshotIndex] == args.LastIncludedTerm { //snapshot information match, retain every log behind snapshot
		rf.log = rf.log[args.LastIncludedIndex-rf.snapshotIndex:]
		rf.logTerm = rf.logTerm[args.LastIncludedIndex-rf.snapshotIndex:]
	} else { //server's log lag behind leader's snapshot, discard every log
		rf.log = rf.log[:0]
		rf.logTerm = rf.logTerm[:0]
		rf.log = append(rf.log, args.LastIncludedLog)
		rf.logTerm = append(rf.logTerm, args.LastIncludedTerm)
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	data := w.Bytes()
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logTerm)
	e.Encode(rf.log)
	rf.persister.SaveStateAndSnapshot(data, args.Data)
	rf.snapshotIndex = args.LastIncludedIndex // every thing before index is removed from the log, but snapshot include terms in index
	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)
	reply.Term = rf.currentTerm
	reply.InstallOK = true
	prettydebug.Debug(prettydebug.DSnap, "S%d install snapshotIndex:%d, commit index:%d, lastApplied:%d", rf.me, rf.snapshotIndex, rf.commitIndex, rf.lastApplied)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// @server index of the server receive snapshot
func (rf *Raft) sendSnapshot(server int, term int, lastIncludedIndex int, LastIncludedTerm int, lastIncludedLog interface{}, data []byte) {
	args := InstallSnapshotArgs{
		Term:              term,
		LeaderId:          rf.me,
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  LastIncludedTerm,
		LastIncludedLog:   lastIncludedLog,
		Data:              data,
	}
	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(server, &args, &reply)
	if ok { // send snapshot sucessful
		rf.mu.Lock()
		if reply.InstallOK { //server install snapshot sucessful
			rf.matchIndex[server] = max(lastIncludedIndex, rf.matchIndex[server])
			rf.nextIndex[server] = rf.matchIndex[server] + 1
		} else { // server install snapshot failed
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.persist()
				rf.state = follower
				rf.cond.Broadcast()
				prettydebug.Debug(prettydebug.DClient, "S%d is follower, term: %d", rf.me, rf.currentTerm)
			}
		}
		rf.mu.Unlock()
	} else {
		prettydebug.Debug(prettydebug.DWarn, "S%d send snapshot to S%d failed, term: %d", rf.me, server, term)
	}
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// if raft server is not group leader, return immediately
	if rf.state != leader {
		isLeader = false
		return index, term, isLeader
	}
	// append the arrival command into leader's log entry and return immediately, signal the cond variable
	rf.log = append(rf.log, command)
	rf.logTerm = append(rf.logTerm, rf.currentTerm)
	rf.persist()
	index = len(rf.log) - 1 + rf.snapshotIndex
	term = rf.currentTerm
	isLeader = true
	rf.cond.Broadcast()
	return index, term, isLeader
}

// must run with rf.mu mutex held
func (rf *Raft) checkLeadTerm(electedTerm int) bool {
	// no longer being the leader, return
	if rf.state != leader {
		prettydebug.Debug(prettydebug.DLog2, "S%d only leader can send log entries", rf.me)
		return false
	}
	// if current term change from elected term, quit sending logs
	if rf.currentTerm != electedTerm {
		prettydebug.Debug(prettydebug.DLog2, "S%d current term: %d, elected term: %d, stop sending log entries", rf.me, rf.currentTerm, electedTerm)
		return false
	}
	return true
}

func (rf *Raft) sendLogEntries(electedTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !rf.checkLeadTerm(electedTerm) {
		return
	}
	// for every peer, start a go routine to send the new log
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(i int, t int) {
			for {
				rf.mu.Lock()
				// every thing commited, sleep in cond variable
				for rf.nextIndex[i] == len(rf.log)+rf.snapshotIndex && rf.state == leader {
					rf.cond.Wait()
				}
				if !rf.checkLeadTerm(electedTerm) {
					rf.mu.Unlock()
					return
				}
				if rf.nextIndex[i]-1-rf.snapshotIndex < 0 { // leader's snapshot goes beyond server next log entry
					rf.sendSnapshot(i, t, rf.snapshotIndex, rf.logTerm[0], rf.log[0], rf.persister.ReadSnapshot())
					continue
				}
				replay := AppendEntriesReply{}
				args := AppendEntriesArgs{
					Term:         t,
					LearderID:    rf.me,
					PrevLogIndex: rf.nextIndex[i] - 1,
					PrevLogTerm:  rf.logTerm[rf.nextIndex[i]-1-rf.snapshotIndex],
					Entries:      rf.log[rf.nextIndex[i]-rf.snapshotIndex:],
					Terms:        rf.logTerm[rf.nextIndex[i]-rf.snapshotIndex:],
					LeaderCommit: rf.commitIndex,
				}
				prettydebug.Debug(prettydebug.DLog, "S%d sends log(%d-%d) to s%d, currurent term: %d", rf.me, rf.nextIndex[i], rf.nextIndex[i]+len(args.Entries)-1, i, t)
				rf.mu.Unlock() // sendAppendEntries is blocking rpc, release the lock
				ok := rf.sendAppendEntries(i, &args, &replay)
				// to do
				// use heart beat to decide if we should send next ping message
				if ok {
					rf.mu.Lock()
					if replay.Term > rf.currentTerm { // there exist server with higher term, change to follower
						rf.currentTerm = replay.Term
						rf.votedFor = -1
						rf.persist()
						rf.state = follower
						rf.cond.Broadcast()
						prettydebug.Debug(prettydebug.DClient, "S%d is follower, term: %d", rf.me, rf.currentTerm)
					} else if replay.Term == rf.currentTerm { //handle rpc reply
						if replay.Success { // append entry successful
							rf.matchIndex[i] = rf.nextIndex[i] + len(args.Entries) - 1
							rf.nextIndex[i] = rf.matchIndex[i] + 1
							prettydebug.Debug(prettydebug.DClient, "S%d server%d,match index: %d, commit index: %d, term: %d", rf.me, i, rf.matchIndex[i], rf.commitIndex, rf.currentTerm)

							// loop through peers to check if commitIndex need update
							if rf.matchIndex[i] > rf.commitIndex && rf.logTerm[rf.matchIndex[i]-rf.snapshotIndex] == rf.currentTerm {
								count := 0
								for peerIndex := range rf.peers {
									if rf.matchIndex[peerIndex] >= rf.matchIndex[i] {
										count++
									}
								}
								if count >= (len(rf.peers)-1)/2 {
									rf.commitIndex = rf.matchIndex[i]
									rf.applyLog()
								}
							}
						} else { // append entry failed
							rf.nextIndex[i] = replay.MisMatchTermStartIndex
						}
					} else if replay.Term < electedTerm {
						panic("reply term can not be smaller than current term")
					}
					// if the reply is no higher, simply release the lock, those reply means noting to leader
					rf.mu.Unlock()
				} else {
					prettydebug.Debug(prettydebug.DWarn, "S%d send sendLogEntries to S%d failed, term: %d", rf.me, i, electedTerm)
					time.Sleep(110 * time.Millisecond)
				}
			}
		}(index, electedTerm)
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// to do
// if the server is not selected as leader
// the cond.Wait() will blocking forever
// which wates resouces, check how many server rejected
func (rf *Raft) runElection(electionTerm int) bool {
	rf.mu.Lock()
	length := len(rf.peers)
	voted := 1
	var mu_voted sync.Mutex
	cond := sync.NewCond(&mu_voted)
	if rf.currentTerm == electionTerm {
		// for every peer, start a seperate go routine to ask for votes
		for index := range rf.peers {
			// do not send rpc to candidate server itself
			if index == rf.me {
				continue
			}
			go func(i int, t int, m int, lli int, llt int) bool {
				var backoff time.Duration = 1
				for {
					replay := RequestVoteReply{}
					args := RequestVoteArgs{
						Term:         t,
						CandidateID:  m,
						LastLogIndex: lli,
						LastLogTerm:  llt,
					}

					rf.mu.Lock() //check again that code is still electing
					if rf.currentTerm != t || rf.state != candidate {
						cond.Signal()
						rf.mu.Unlock()
						return false
					}
					rf.mu.Unlock()

					ok := rf.sendRequestVote(i, &args, &replay)
					if ok {
						if replay.VoteGranted {
							mu_voted.Lock()
							voted = voted + 1
							prettydebug.Debug(prettydebug.DVote, "S%d receive votes from server%d, term: %d", m, i, t)
							mu_voted.Unlock()
							cond.Signal()
							return true
						} else if replay.Term > t {
							// entering follower state
							rf.mu.Lock()
							if replay.Term > rf.currentTerm {
								rf.state = follower
								rf.cond.Broadcast()
								rf.currentTerm = replay.Term
								rf.votedFor = -1 // election was ended by a higher term, but we do not know who the leader is
								rf.persist()
							}
							rf.mu.Unlock()
							cond.Signal()
							return false
						} else if replay.Term == t {
							// peer's vote already gives to others
							cond.Signal()
							return false
						}
					} else {
						time.Sleep(backoff * 10 * time.Millisecond)
						backoff = backoff * 2
						if backoff > 16 {
							backoff = 16
							cond.Signal()
						}
					}
				}
			}(index, electionTerm, rf.me, len(rf.log)-1+rf.snapshotIndex, rf.logTerm[len(rf.log)-1-rf.snapshotIndex])
		}
	}
	rf.mu.Unlock()
	mu_voted.Lock()
	count := 0
	for voted < (length+1)/2 {
		if count == len(rf.peers)-1 {
			mu_voted.Unlock()
			prettydebug.Debug(prettydebug.DVote, "S%d election failed, election term: %d", rf.me, electionTerm)
			return false
		}
		cond.Wait()
		count++
		rf.mu.Lock()
		if rf.currentTerm > electionTerm {
			rf.mu.Unlock()
			mu_voted.Unlock()
			prettydebug.Debug(prettydebug.DVote, "S%d election failed, election term: %d", rf.me, electionTerm)
			return false
		}
		rf.mu.Unlock()
	}
	mu_voted.Unlock()
	// entering leader state
	rf.mu.Lock()
	rf.state = leader
	rf.currentTerm = electionTerm
	rf.votedFor = rf.me //leader vote for itself
	rf.persist()
	rf.nextIndex = make([]int, len(rf.peers)) //initialize nextindex and matchIndex
	rf.matchIndex = make([]int, len(rf.peers))
	for index := range rf.nextIndex {
		rf.nextIndex[index] = len(rf.log) + rf.snapshotIndex
	}
	prettydebug.Debug(prettydebug.DLeader, "S%d becomes Leader, currurent term: %d", rf.me, electionTerm)
	go rf.sendHeartBeat(electionTerm)
	go rf.sendLogEntries(electionTerm)
	rf.mu.Unlock()
	return true
}

func (rf *Raft) sendHeartBeat(electedTerm int) {
	rf.mu.Lock()
	for rf.currentTerm == electedTerm && rf.state == leader {
		for index := range rf.peers {
			// do not send rpc to candidate server itself
			if index == rf.me {
				continue
			}
			go func(i int, t int, m int) {
				rf.mu.Lock()
				if rf.state == leader && electedTerm == rf.currentTerm {
					replay := AppendEntriesReply{}
					args := AppendEntriesArgs{
						Term:         t,
						LearderID:    rf.me,
						PrevLogIndex: rf.nextIndex[i] - 1,
						LeaderCommit: rf.commitIndex,
					}
					if rf.nextIndex[i]-1-rf.snapshotIndex > 0 {
						args.PrevLogTerm = rf.logTerm[rf.nextIndex[i]-1-rf.snapshotIndex]
					} else {
						args.PrevLogTerm = -1 // leader's snapshot goes beyond server next log entry
						go rf.sendSnapshot(i, t, rf.snapshotIndex, rf.logTerm[0], rf.log[0], rf.persister.ReadSnapshot())
					}
					rf.mu.Unlock()
					prettydebug.Debug(prettydebug.DLeader, "S%d ping s%d, currurent term: %d", rf.me, i, t)
					ok := rf.sendAppendEntries(i, &args, &replay)
					if ok {
						rf.mu.Lock()
						if replay.Term > rf.currentTerm {
							// there exist server with higher term, change to follower
							rf.currentTerm = replay.Term
							rf.votedFor = -1
							rf.persist()
							rf.state = follower
							rf.cond.Broadcast()
							prettydebug.Debug(prettydebug.DClient, "S%d is follower, term: %d", rf.me, rf.currentTerm)
						}
						// if the reply is no higher, simply release the lock, those reply means noting to leader
						rf.mu.Unlock()
					} else {
						prettydebug.Debug(prettydebug.DWarn, "S%d send heartbeat to S%d failed, term: %d", rf.me, i, electedTerm)
					}
				} else {
					rf.mu.Unlock()
				}
			}(index, electedTerm, rf.me)
		}
		rf.mu.Unlock()
		time.Sleep(110 * time.Millisecond)
		rf.mu.Lock()
	}
	rf.mu.Unlock()
}

// must run with mutex held
func (rf *Raft) applyLog() {
	for rf.commitIndex > rf.lastApplied {
		appMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied+1-rf.snapshotIndex],
			CommandIndex: rf.lastApplied + 1, // command index start from 1 same as actual log start from 1
		}
		rf.lastApplied = rf.lastApplied + 1
		rf.mu.Unlock()
		rf.applyCh <- appMsg
		prettydebug.Debug(prettydebug.DLog, "S%d send log: %d to applyCh", rf.me, appMsg.Command)
		rf.mu.Lock()
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	//protect rf.dead, rf.state and rf.heartbeat
	rf.mu.Lock()
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		if !rf.heartbeat && rf.state != leader {
			prettydebug.Debug(prettydebug.DTimer, "S%d Timer, election timeout, currurent term: %d", rf.me, rf.currentTerm)
			// entering candidate state
			rf.state = candidate
			rf.currentTerm = rf.currentTerm + 1
			rf.votedFor = rf.me
			rf.persist()
			go rf.runElection(rf.currentTerm)
		}
		rf.applyLog()
		rf.heartbeat = false
		rf.mu.Unlock()
		time.Sleep(time.Duration(150+300*rand.Float32()) * time.Millisecond)
		rf.mu.Lock()
	}
	rf.mu.Unlock()
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rand.Seed(time.Now().Unix())
	rf.mu.Lock()
	rf.currentTerm = 0 // start from term 0, after election it will be term 1, term 0 is reserved for the first default committed log
	rf.votedFor = -1
	rf.heartbeat = true
	rf.state = follower
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.cond = *sync.NewCond(&rf.mu)
	rf.logTerm = make([]int, 0, 100)
	rf.log = make([]interface{}, 0, 100)
	rf.logTerm = append(rf.logTerm, 0) // logTerm[0] default to 0
	rf.log = append(rf.log, 0)         // log[0] default to 0
	rf.applyCh = applyCh
	rf.snapshotIndex = 0
	rf.mu.Unlock()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

const (
	follower  string = "follower"
	candidate string = "candidate"
	leader    string = "leader"
)
