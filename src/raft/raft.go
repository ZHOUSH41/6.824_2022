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
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
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
	applyCh   chan ApplyMsg
	applyCond *sync.Cond
	state     NodeState

	// persist state on all servers
	currentTerm int
	votedFor    int
	logs        []Entry // the first entry is a dummy entry which contains LastSnapshotTerm, LastSnapshotIndex and nil Command

	// volatile state on all servers
	commitIndex int //
	lastApplied int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	electionTime time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("{Node %v} state %v", rf.me, rf.state)
	return rf.currentTerm, rf.state == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	rf.persister.SaveRaftState(rf.encodeState())
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	return w.Bytes()
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
	var logs []Entry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		DPrintf("{Node %v} restores persisted state failed", rf.me)
	}
	rf.currentTerm, rf.votedFor, rf.logs = currentTerm, votedFor, logs
	// TODO: lastApplied, commitIndex, 对2D 日志压缩来说必要的, first log是dummy log
	rf.lastApplied, rf.commitIndex = rf.logs[0].Index, rf.logs[0].Index
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("{Node %v} service calls CondInstallSnapshot with lastIncludedTerm %v and lastIncludedIndex %v to check whether snapshot is still valid in term %v", rf.me, lastIncludedTerm, lastIncludedIndex, rf.currentTerm)

	// outdated snapshot
	if lastIncludedIndex <= rf.commitIndex {
		DPrintf("{Node %v} rejects the snapshot which lastIncludedIndex is %v because commitIndex %v is larger", rf.me, lastIncludedIndex, rf.commitIndex)
		return false
	}
	if lastIncludedIndex > rf.getLastLogL().Index {
		rf.logs = make([]Entry, 1)
	} else {
		rf.logs = shrinkEntriesArrayL(rf.logs[lastIncludedIndex-rf.getFirstLogL().Index:])
		rf.logs[0].Command = nil
	}
	// update dummy entry with lastIncludedTerm and lastIncludedIndex
	rf.logs[0].Term, rf.logs[0].Index = lastIncludedTerm, lastIncludedIndex
	rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex

	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after accepting the snapshot which lastIncludedTerm is %v, lastIncludedIndex is %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLogL(), rf.getLastLogL(), lastIncludedTerm, lastIncludedIndex)

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
	snapshotIndex := rf.getFirstLogL().Index
	if index <= snapshotIndex {
		DPrintf("{Node %v} rejects replacing log with snapshotIndex %v as current snapshotIndex %v is larger in term %v", rf.me, index, snapshotIndex, rf.currentTerm)
		return
	}
	rf.logs = shrinkEntriesArrayL(rf.logs[index-snapshotIndex:])
	rf.logs[0].Command = nil
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after replacing log with snapshotIndex %v as old snapshotIndex %v is smaller", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLogL(), rf.getLastLogL(), index, snapshotIndex)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// rules for all servers
	if args.Term > rf.currentTerm {
		rf.setNewTermL(args.Term)
	}

	uptoDate := rf.isLogUpToDateL(args.LastLogTerm, args.LastLogIndex)
	reply.Term = rf.currentTerm
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && uptoDate {
		reply.VoteGranted = true
		// 投票之后不能再投了
		rf.votedFor = args.CandidateId
		// 重置选举超时时间
		rf.resetElectionTimer()
		return
	}
	reply.VoteGranted = false
}

func (rf *Raft) setNewTermL(term int) {
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = -1
	rf.persist()
}

// invoke by vote RPC
// return true if vote RPC params is up to date to grant vote
func (rf *Raft) isLogUpToDateL(term, index int) bool {
	lastLog := rf.getLastLogL()
	return term > lastLog.Term || (term == lastLog.Term && index >= lastLog.Index)
}

func (rf *Raft) getLastLogL() Entry {
	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) getFirstLogL() Entry {
	return rf.logs[0]
}

func (rf *Raft) matchLogL(term int, index int) bool {
	return index <= rf.getLastLogL().Index && rf.logs[index-rf.getFirstLogL().Index].Term == term
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	}

	newLog := rf.appendNewEntryL(command)
	DPrintf("{Node %v} receives a new command[%v] to replicate in term %v", rf.me, newLog, rf.currentTerm)
	rf.appendEntriesL(false)
	return newLog.Index, newLog.Term, true
	// Your code here (2B).

}

func (rf *Raft) appendNewEntryL(command interface{}) Entry {
	lastLog := rf.getLastLogL()
	newLog := Entry{rf.currentTerm, lastLog.Index + 1, command}
	rf.logs = append(rf.logs, newLog)
	rf.matchIndex[rf.me], rf.nextIndex[rf.me] = newLog.Index, newLog.Index+1
	rf.persist()
	return newLog
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		if rf.state == Leader {
			rf.appendEntriesL(true)
		}
		if time.Now().After(rf.electionTime) {
			rf.leaderElectionL()
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(125) * time.Millisecond)
	}
}

func (rf *Raft) apply() {
	rf.applyCond.Signal()
	DPrintf("{Node %v}: invoke rf.applyCond.Signal()", rf.me)
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		// 如果没有就一直等待
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		firstIndex, commitIndex, lastApplied := rf.getFirstLogL().Index, rf.commitIndex, rf.lastApplied
		entris := make([]Entry, commitIndex-lastApplied)
		copy(entris, rf.logs[lastApplied+1-firstIndex:commitIndex+1-firstIndex])
		rf.mu.Unlock()
		for _, entry := range entris {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
		}
		rf.mu.Lock()
		DPrintf("{Node %v} applies entries %v-%v in term %v", rf.me, rf.lastApplied, commitIndex, rf.currentTerm)
		rf.lastApplied = Max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}
}

func (rf *Raft) appendEntriesL(heartbeat bool) {
	lastLog := rf.getLastLogL()
	for peer := range rf.peers {
		if peer == rf.me {
			rf.resetElectionTimer()
			continue
		}
		prevLogIndex := rf.nextIndex[peer] - 1
		if prevLogIndex < rf.getFirstLogL().Index {
			// snapshot can catch up
			firstLog := rf.getFirstLogL()
			args := InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: firstLog.Index,
				LastIncludedTerm:  firstLog.Term,
				Data:              rf.persister.ReadSnapshot(),
			}
			go rf.leaderSendSnapShot(peer, &args)
		}
		// rules for leader 3
		if lastLog.Index >= rf.nextIndex[peer] || heartbeat {
			firstIndex := rf.getFirstLogL().Index
			if prevLogIndex <= firstIndex {
				prevLogIndex = firstIndex
			}
			entries := make([]Entry, len(rf.logs[prevLogIndex+1-firstIndex:]))
			copy(entries, rf.logs[prevLogIndex+1-firstIndex:])
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  rf.logs[prevLogIndex-firstIndex].Term,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			go rf.leaderSendEntries(peer, &args)
		}
	}
}

func (rf *Raft) leaderSendSnapShot(peer int, args *InstallSnapshotArgs) {
	var reply InstallSnapshotReply
	ok := rf.sendInstallSnapshot(peer, args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == Leader && rf.currentTerm == args.Term {
		if reply.Term > rf.currentTerm {
			rf.setNewTermL(reply.Term)
		} else {
			rf.matchIndex[peer], rf.nextIndex[peer] = args.LastIncludedIndex, args.LastIncludedIndex+1
		}
	}
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after handling InstallSnapshotResponse %v for InstallSnapshotRequest %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLogL(), rf.getLastLogL(), args, reply)
}

func (rf *Raft) leaderSendEntries(peer int, args *AppendEntriesArgs) {
	var reply AppendEntriesReply
	ok := rf.sendAppendEntries(peer, args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.setNewTermL(reply.Term)
		return
	}

	if rf.state == Leader && args.Term == rf.currentTerm {
		// rules for leader 3.1
		if reply.Success {
			rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1
			rf.advanceCommitIndexForLeaderL()
		} else {
			if reply.Term > rf.currentTerm {
				rf.setNewTermL(args.Term)
			} else if reply.Term == rf.currentTerm {
				rf.nextIndex[peer] = reply.ConflictIndex
				if reply.ConflictTerm != -1 {
					firstIndex := rf.getFirstLogL().Index
					for i := args.PrevLogIndex; i >= firstIndex; i-- {
						if rf.logs[i-firstIndex].Term == reply.ConflictTerm {
							rf.nextIndex[peer] = i + 1
							break
						}
					}
				}
			}
		}
	}
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after handling AppendEntriesResponse %v for AppendEntriesRequest %v",
		rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLogL(), rf.getLastLogL(), reply, args)
}

func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)
	return ok
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing AppendEntriesRequest %v and reply AppendEntriesResponse %v",
		rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLogL(), rf.getLastLogL(), args, reply)

	// append entries rpc rules 1
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// rules for all servers rules 2
	if args.Term > rf.currentTerm {
		rf.setNewTermL(args.Term)
	}
	// candidate rule 3
	rf.state = Follower

	rf.resetElectionTimer()

	// append entries rpc rules 2
	if args.PrevLogIndex < rf.getFirstLogL().Index {
		reply.Success = false
		reply.Term = rf.currentTerm
		DPrintf("{Node %v} receives unexpected AppendEntriesRequest %v from {Node %v} because prevLogIndex %v < firstLogIndex %v",
			rf.me, args, args.LeaderId, args.PrevLogIndex, rf.getFirstLogL().Index)
		return
	}

	// append entries rpc rules 2
	if !rf.matchLogL(args.PrevLogTerm, args.PrevLogIndex) {
		reply.Success = false
		reply.Term = rf.currentTerm
		lastIndex := rf.getLastLogL().Index
		if lastIndex < args.PrevLogIndex {
			reply.ConflictTerm = -1
			reply.ConflictIndex = lastIndex + 1
		} else {
			firstIndex := rf.getFirstLogL().Index
			reply.ConflictTerm = rf.logs[args.PrevLogIndex-firstIndex].Term
			index := args.PrevLogIndex - 1
			for index >= firstIndex && rf.logs[index-firstIndex].Term == reply.ConflictTerm {
				index--
			}
			reply.ConflictIndex = index
		}
		return
	}

	// append entries rpc rules 4
	firstIndex := rf.getFirstLogL().Index
	for index, entry := range args.Entries {
		if entry.Index-firstIndex >= len(rf.logs) || rf.logs[entry.Index-firstIndex].Term != entry.Term {
			rf.logs = shrinkEntriesArrayL(append(rf.logs[:entry.Index-firstIndex], args.Entries[index:]...))
			break
		}
	}

	// append entries rpc rules 5
	rf.advanceCommitIndexForFollowerL(args.LeaderCommit)
	reply.Term, reply.Success = rf.currentTerm, true
}

func (rf *Raft) sendInstallSnapshot(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[peer].Call("Raft.InstallSnapshot", args, reply)
}

// install snapshot handler
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing InstallSnapshotRequest %v and reply InstallSnapshotResponse %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLogL(), rf.getLastLogL(), args, reply)

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.setNewTermL(args.Term)
	}

	rf.resetElectionTimer()

	// outdated snapshot
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
}

func (rf *Raft) leaderElectionL() {
	// rules for servers at for followers
	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	args := rf.genVoteArgsL()
	DPrintf("{Node %v} starts election with RequestVoteRequest %v", rf.me, args)
	rf.resetElectionTimer()
	rf.persist()
	votes := 1
	for peer := range rf.peers {
		if peer != rf.me {
			go func(peer int) {
				reply := new(RequestVoteReply)
				if rf.sendRequestVote(peer, args, reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					DPrintf("{Node %v} receives RequestVoteResponse %v from {Node %v} after sending RequestVoteRequest %v in term %v", rf.me, reply, peer, args, rf.currentTerm)
					// DPrintf("{Node %v} state %v term %v", rf.me, rf.state, rf.currentTerm)
					if rf.currentTerm == args.Term && rf.state == Candidate {
						// DPrintf("join")
						if reply.VoteGranted {
							votes += 1
							// DPrintf("{Node %v} votes %v", rf.me, votes)
							if votes > len(rf.peers)/2 {
								DPrintf("{Node %v} receives majority votes in term %v", rf.me, rf.currentTerm)
								rf.becomeLeaderL()
								rf.persist()
							}
						} else if reply.Term > rf.currentTerm {
							DPrintf("{Node %v} finds a new leader {Node %v} with term %v and steps down in term %v", rf.me, peer, reply.Term, rf.currentTerm)
							rf.setNewTermL(reply.Term)
							rf.persist()
						}
					}
				}
			}(peer)
		}
	}
}

func (rf *Raft) becomeLeaderL() {
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.getLastLogL().Index + 1
		rf.matchIndex[i] = 0
	}
	rf.state = Leader
	rf.resetElectionTimer()
	rf.appendEntriesL(true)
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
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.state = Follower
	rf.currentTerm = 0
	rf.logs = make([]Entry, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	rf.resetElectionTimer()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	lastLog := rf.getLastLogL()
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index+1
	}
	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()
	return rf
}

func (rf *Raft) resetElectionTimer() {
	t := time.Now()
	electionTimeOut := time.Duration(650+rand.Intn(650)) * time.Millisecond
	rf.electionTime = t.Add(electionTimeOut)
}

func (rf *Raft) advanceCommitIndexForFollowerL(leaderCommit int) {
	newCommitIndex := Min(leaderCommit, rf.getLastLogL().Index)
	if newCommitIndex > rf.commitIndex {
		DPrintf("{Node %d} advance commitIndex from %d to %d with leaderCommit %d in term %d", rf.me, rf.commitIndex, newCommitIndex, leaderCommit, rf.currentTerm)
		rf.commitIndex = newCommitIndex
		rf.apply()
	}
}

func (rf *Raft) advanceCommitIndexForLeaderL() {
	// leader rule 4
	if rf.state != Leader {
		return
	}

	lastLog := rf.getLastLogL()
	firtLog := rf.getFirstLogL()
	for n := rf.commitIndex + 1; n <= lastLog.Index; n++ {
		if rf.logs[n-firtLog.Index].Term != rf.currentTerm {
			continue
		}
		cnt := 1
		for serverId := 0; serverId < len(rf.peers); serverId++ {
			if rf.matchIndex[serverId] >= n && serverId != rf.me {
				cnt++
			}
			if cnt > len(rf.peers)/2 {
				rf.commitIndex = n
				DPrintf("{Node %v} advance commitIndex from %d to %d with matchIndex %v in term %d", rf.me, rf.commitIndex, n, rf.matchIndex, rf.currentTerm)
				rf.apply()
				break
			}
		}
	}
}
