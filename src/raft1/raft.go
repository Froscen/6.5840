package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	"bytes"
	"math/rand"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state           Role
	currentTerm     int
	votedFor        int
	lastHeartbeat   time.Time
	electionTimeout time.Duration

	log         []LogEntry
	commitIndex int

	nextIndex  []int
	matchIndex []int

	votes int

	lastHeartbeatSend time.Time
	HeartbeatTimeout  time.Duration

	lastApplied int
	applyCh     chan raftapi.ApplyMsg

	snapshotIndex int
	snapshotTerm  int

	pendingSnapshot *raftapi.ApplyMsg
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotRequest struct {
	term   int
	args   InstallSnapshotArgs
	server int
}

type InstallSnapshotReply struct {
	Term int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	rf.persistWithSnapshot(rf.persister.ReadSnapshot())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var snapshotIndex int
	var snapshotTerm int

	if err := d.Decode(&currentTerm); err != nil {
		panic(err)
	}
	if err := d.Decode(&votedFor); err != nil {
		panic(err)
	}
	if err := d.Decode(&log); err != nil {
		panic(err)
	}
	if err := d.Decode(&snapshotIndex); err != nil {
		panic(err)
	}
	if err := d.Decode(&snapshotTerm); err != nil {
		panic(err)
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.snapshotIndex = snapshotIndex
	rf.snapshotTerm = snapshotTerm
}

func (rf *Raft) persistWithSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(rf.currentTerm); err != nil {
		panic(err)
	}
	if err := e.Encode(rf.votedFor); err != nil {
		panic(err)
	}
	if err := e.Encode(rf.log); err != nil {
		panic(err)
	}
	if err := e.Encode(rf.snapshotIndex); err != nil {
		panic(err)
	}
	if err := e.Encode(rf.snapshotTerm); err != nil {
		panic(err)
	}

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.snapshotIndex || index > rf.lastApplied {
		return
	}

	term, ok := rf.termAt(index)
	if !ok {
		return
	}

	sliceIndex := index - rf.snapshotIndex
	log := append([]LogEntry(nil), rf.log[sliceIndex:]...)
	log[0].Command = nil
	log[0].Term = term

	rf.log = log
	rf.snapshotIndex = index
	rf.snapshotTerm = term

	rf.persistWithSnapshot(snapshot)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type LogEntry struct {
	Command interface{}
	Term    int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

type AppendEntriesRequest struct {
	server int
	args   AppendEntriesArgs
	term   int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	stateChanged := false

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.resetElectionTimer()

		reply.Term = rf.currentTerm

		stateChanged = true
	}

	lastLogIndex := rf.lastLogIndex()
	lastLogTerm, _ := rf.termAt(lastLogIndex)

	isCandidateLogUpToDate := false
	if args.LastLogTerm > lastLogTerm {
		isCandidateLogUpToDate = true
	} else if args.LastLogTerm == lastLogTerm {
		if args.LastLogIndex >= lastLogIndex {
			isCandidateLogUpToDate = true
		}
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && isCandidateLogUpToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		stateChanged = true
		rf.resetElectionTimer()
	}

	if stateChanged {
		rf.persist()
	}
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

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()

	if rf.state != Leader {
		rf.mu.Unlock()
		return -1, -1, false
	}

	index = rf.lastLogIndex() + 1
	term = rf.currentTerm
	rf.log = append(rf.log, LogEntry{
		Command: command,
		Term:    term,
	})
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1

	rf.persist()

	rf.updateCommitIndex()
	rf.mu.Unlock()

	rf.broadcastHeartbeat()

	return index, term, isLeader
}

func (rf *Raft) ticker() {
	for true {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		state := rf.state
		sendHeartbeat := false
		startElection := false
		// pause for a random amount of time between 50 and 350
		// milliseconds.

		switch state {
		case Leader:
			if time.Since(rf.lastHeartbeatSend) >= rf.HeartbeatTimeout {
				rf.lastHeartbeatSend = time.Now()
				sendHeartbeat = true
			}
		case Candidate, Follower:
			if time.Since(rf.lastHeartbeat) >= rf.electionTimeout {
				startElection = true
			}
		}
		rf.mu.Unlock()

		if sendHeartbeat {
			rf.broadcastHeartbeat()
		}
		if startElection {
			rf.startElection()
		}

		time.Sleep(10 * time.Millisecond)
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{
		peers:             peers,
		persister:         persister,
		me:                me,
		lastHeartbeatSend: time.Now(),
		HeartbeatTimeout:  100 * time.Millisecond,
		state:             Follower,
		currentTerm:       0,
		votedFor:          -1,
		log:               []LogEntry{{Term: 0}},
		commitIndex:       0,
		lastApplied:       0,
		applyCh:           applyCh,
		electionTimeout:   time.Duration(300+rand.Intn(300)) * time.Millisecond,
		lastHeartbeat:     time.Now(),
		snapshotIndex:     0,
		snapshotTerm:      0,
	}

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}

func (rf *Raft) startElection() {
	rf.mu.Lock()

	if rf.state == Leader || time.Since(rf.lastHeartbeat) < rf.electionTimeout {
		rf.mu.Unlock()
		return
	}

	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	votes := 1
	rf.resetElectionTimer()
	rf.persist()

	term := rf.currentTerm
	lastLogIndex := rf.lastLogIndex()
	lastLogTerm, _ := rf.termAt(lastLogIndex)

	if votes >= len(rf.peers)/2+1 && rf.state == Candidate && rf.currentTerm == term {
		rf.state = Leader

		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))

		for i := range rf.peers {
			rf.nextIndex[i] = rf.lastLogIndex() + 1
			rf.matchIndex[i] = 0
		}

		rf.matchIndex[rf.me] = rf.lastLogIndex()

		rf.mu.Unlock()
		rf.broadcastHeartbeat()
		return
	}

	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			var reply RequestVoteReply
			ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
			if !ok {
				return
			}
			becameLeader := false

			rf.mu.Lock()

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				rf.resetElectionTimer()
				rf.persist()
			}

			if rf.state != Candidate || rf.currentTerm != term {
				rf.mu.Unlock()
				return
			}

			if reply.Term == rf.currentTerm && reply.VoteGranted {
				votes++

				if votes >= len(rf.peers)/2+1 && rf.state == Candidate && rf.currentTerm == term {
					rf.state = Leader
					becameLeader = true
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))

					for i := range rf.peers {
						rf.nextIndex[i] = rf.lastLogIndex() + 1
						rf.matchIndex[i] = 0
					}

					rf.matchIndex[rf.me] = rf.lastLogIndex()

				}
			}
			rf.mu.Unlock()

			if becameLeader {
				rf.broadcastHeartbeat()
			}

		}(i)
	}
}

func (rf *Raft) resetElectionTimer() {
	rf.lastHeartbeat = time.Now()
	rf.electionTimeout = time.Duration(300+rand.Intn(300)) * time.Millisecond
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, term int) {
	var reply AppendEntriesReply
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.resetElectionTimer()
		rf.persist()
		return
	}

	if rf.state != Leader || rf.currentTerm != term {
		return
	}

	if reply.Success {
		newMatchIndex := args.PrevLogIndex + len(args.Entries)

		if newMatchIndex > rf.matchIndex[server] {
			rf.matchIndex[server] = newMatchIndex
		}
		rf.nextIndex[server] = rf.matchIndex[server] + 1

		rf.updateCommitIndex()
	} else {
		if rf.nextIndex[server] != args.PrevLogIndex+1 {
			return
		}

		if reply.ConflictTerm == -1 {
			rf.nextIndex[server] = reply.ConflictIndex
		} else {
			found := false
			for i := rf.lastLogIndex(); i >= rf.snapshotIndex; i-- {
				logTerm, ok := rf.termAt(i)
				if ok && logTerm == reply.ConflictTerm {
					found = true
					rf.nextIndex[server] = i + 1
					break
				}
			}

			if !found {
				rf.nextIndex[server] = reply.ConflictIndex
			}
		}

		if rf.nextIndex[server] < rf.matchIndex[server]+1 {
			rf.nextIndex[server] = rf.matchIndex[server] + 1
		}
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args InstallSnapshotArgs, term int) {
	var reply InstallSnapshotReply
	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.resetElectionTimer()
		rf.persist()
		return
	}
	if rf.state != Leader || rf.currentTerm != term {
		return
	}

	rf.matchIndex[server] = max(rf.matchIndex[server], args.LastIncludedIndex)
	rf.nextIndex[server] = rf.matchIndex[server] + 1

	rf.updateCommitIndex()
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	termChanged := false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		termChanged = true
	}
	rf.state = Follower
	rf.resetElectionTimer()

	if args.LastIncludedIndex <= rf.lastApplied {
		reply.Term = rf.currentTerm
		if termChanged {
			rf.persist()
		}
		rf.mu.Unlock()
		return
	}

	reply.Term = rf.currentTerm

	lastIncludedTerm, ok := rf.termAt(args.LastIncludedIndex)
	if ok && lastIncludedTerm == args.LastIncludedTerm {
		sliceIndex := args.LastIncludedIndex - rf.snapshotIndex
		rf.log = append([]LogEntry(nil), rf.log[sliceIndex:]...)
		rf.log[0].Command = nil
		rf.log[0].Term = args.LastIncludedTerm
	} else {
		rf.log = []LogEntry{
			{
				Command: nil,
				Term:    args.LastIncludedTerm,
			},
		}
	}

	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotTerm = args.LastIncludedTerm
	rf.commitIndex = max(args.LastIncludedIndex, rf.commitIndex)
	rf.lastApplied = max(args.LastIncludedIndex, rf.lastApplied)

	msg := raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.pendingSnapshot = &msg

	rf.persistWithSnapshot(args.Data)
	rf.mu.Unlock()
}

func (rf *Raft) broadcastHeartbeat() {
	appendRequests := make([]AppendEntriesRequest, 0)
	snapshotRequests := make([]InstallSnapshotRequest, 0)

	rf.mu.Lock()

	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	term := rf.currentTerm

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		if rf.nextIndex[i] <= rf.snapshotIndex {
			args := InstallSnapshotArgs{
				Term:              term,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.snapshotIndex,
				LastIncludedTerm:  rf.snapshotTerm,
				Data:              rf.persister.ReadSnapshot(),
			}

			snapshotRequests = append(snapshotRequests, InstallSnapshotRequest{
				server: i,
				args:   args,
				term:   term,
			})
			continue
		}

		if rf.nextIndex[i] > rf.lastLogIndex()+1 {
			continue
		}
		prevLogIndex := rf.nextIndex[i] - 1
		prevLogTerm, ok := rf.termAt(prevLogIndex)
		if !ok {
			continue
		}

		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      append([]LogEntry(nil), rf.log[rf.nextIndex[i]-rf.snapshotIndex:]...),
			LeaderCommit: rf.commitIndex,
		}

		appendRequests = append(appendRequests, AppendEntriesRequest{
			server: i,
			args:   args,
			term:   term,
		})
	}
	rf.mu.Unlock()

	for _, req := range snapshotRequests {
		go rf.sendInstallSnapshot(req.server, req.args, req.term)
	}
	for _, req := range appendRequests {
		go rf.sendAppendEntries(req.server, req.args, req.term)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	stateChanged := false

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		stateChanged = true
	}

	rf.state = Follower
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm

	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.lastLogIndex() + 1
		if stateChanged {
			rf.persist()
		}
		return
	}

	if args.PrevLogIndex < rf.snapshotIndex {
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.snapshotIndex + 1
		if stateChanged {
			rf.persist()
		}
		return
	}

	prevLogTerm, ok := rf.termAt(args.PrevLogIndex)
	if !ok {
		if stateChanged {
			rf.persist()
		}
		return
	}

	if args.PrevLogTerm != prevLogTerm {
		reply.ConflictTerm = prevLogTerm
		reply.ConflictIndex = rf.snapshotIndex

		for i := args.PrevLogIndex; i >= rf.snapshotIndex; i-- {
			term, _ := rf.termAt(i)
			if term != reply.ConflictTerm {
				reply.ConflictIndex = i + 1
				break
			}
		}
		if stateChanged {
			rf.persist()
		}
		return
	}

	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		sliceIndex := index - rf.snapshotIndex

		if sliceIndex >= len(rf.log) {
			rf.log = append(rf.log, args.Entries[i:]...)
			stateChanged = true
			break
		}

		if rf.log[sliceIndex].Term == entry.Term {
			continue
		}

		rf.log = rf.log[:sliceIndex]
		rf.log = append(rf.log, args.Entries[i:]...)
		stateChanged = true
		break
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
	}

	if stateChanged {
		rf.persist()
	}

	reply.Success = true
}

func (rf *Raft) updateCommitIndex() {
	for n := rf.lastLogIndex(); n > rf.commitIndex; n-- {
		term, _ := rf.termAt(n)
		if term == rf.currentTerm {
			count := 0
			for i := range rf.peers {
				if rf.matchIndex[i] >= n {
					count++
				}
			}
			if count >= len(rf.peers)/2+1 {
				rf.commitIndex = n
				break
			}
		}
	}
}

func (rf *Raft) applier() {
	for {
		rf.mu.Lock()

		if rf.pendingSnapshot != nil {
			msg := *rf.pendingSnapshot
			rf.pendingSnapshot = nil

			rf.mu.Unlock()
			rf.applyCh <- msg
			continue
		}

		if rf.lastApplied < rf.commitIndex {
			if rf.lastApplied < rf.snapshotIndex {
				rf.lastApplied = rf.snapshotIndex
			}
			rf.lastApplied++
			index := rf.lastApplied
			sliceIndex := index - rf.snapshotIndex
			command := rf.log[sliceIndex].Command

			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      command,
				CommandIndex: index,
			}

			rf.mu.Unlock()
			rf.applyCh <- msg
		} else {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (rf *Raft) lastLogIndex() int {
	return rf.snapshotIndex + len(rf.log) - 1
}

func (rf *Raft) termAt(index int) (int, bool) {
	if index == rf.snapshotIndex {
		return rf.snapshotTerm, true
	} else if index > rf.snapshotIndex && index <= rf.lastLogIndex() {
		return rf.log[index-rf.snapshotIndex].Term, true
	}

	return 0, false
}
