// Package raft implements the Raft consensus algorithm as described in the paper "In Search of an Understandable Consensus Algorithm" by Diego Ongaro and John Ousterhout. The Raft algorithm is designed to manage a replicated log across a cluster of servers, ensuring that all servers agree on the same sequence of log entries, even in the presence of failures.
package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"bytes"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"

	// debug

	_ "6.5840/config"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// Raft is a single Raft peer.
type Raft struct {
	mu        sync.Mutex            // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd   // RPC end points of all peers
	persister *tester.Persister     // Object to hold this peer's persisted state
	applyCh   chan raftapi.ApplyMsg // Channel to send newly committed log entries to the service (or tester)

	me          int   // this peer's index into peers[]
	currentTerm int   // The term server has seen for the last time
	votedFor    int   // Candidate id that received vote
	leaderID    int   // The current leader's id
	state       State // The state of the server: Follower, Candidate or Leader
	log         []LogEntry
	commitIndex int   // Index of the highest log entry known to be committed
	lastApplied int   // Index of the last entry applied to the state machine
	nextIndex   []int // For each server, index of the next log entry to send to that server
	matchIndex  []int // For each server, index of the highest log entry known to be replicated on server
	heartBeatCh chan int
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) lastLogTerm() int {
	return rf.log[rf.lastLogIndex()].Term
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logEntries []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logEntries) != nil {
		slog.Error("PERSIST", "PEER", rf.me, "EVENT", "READ_PERSIST_FAILED")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logEntries
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
func (rf *Raft) sendRequestVote(server int, electionTerm int, votes chan bool, args *RequestVoteArgs, reply *RequestVoteReply) {
	if ok := rf.peers[server].Call("Raft.RequestVote", args, reply); ok {
		if reply.Term == electionTerm {
			votes <- reply.VoteGranted
		}
	}
}

// The election timeout is 500 ~ 800ms
func electionTimeout() time.Duration {
	return time.Duration((500 + rand.Int63()%300)) * time.Millisecond
}

func (rf *Raft) sendRequestVoteAll(term int, votes chan bool) {
	for server := range rf.peers {
		if server != rf.me {
			slog.Debug(
				"VOTE",
				"PEER", rf.me,
				"EVENT", "REQUEST_VOTE",
				"TO", server,
				"TERM", term,
			)
			args := RequestVoteArgs{
				Term:         term,
				CandidateID:  rf.me,
				LastLogIndex: rf.lastLogIndex(),
				LastLogTerm:  rf.lastLogTerm(),
			}
			reply := RequestVoteReply{}
			go rf.sendRequestVote(server, term, votes, &args, &reply)
		}
	}
}

func (rf *Raft) beCandidate() int {
	electionTerm := rf.currentTerm + 1
	rf.currentTerm = electionTerm
	rf.votedFor = rf.me
	rf.state = Candidate
	rf.persist()
	return electionTerm
}

func (rf *Raft) beLeader() {
	slog.Debug(
		"VOTE",
		"PEER", rf.me,
		"EVENT", "BE_LEADER",
		"TERM", rf.currentTerm,
	)
	rf.state = Leader
	rf.leaderID = rf.me
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = rf.lastLogIndex() + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	// Start heartbeat goroutine
	go rf.heartBeat()
}

func (rf *Raft) waitVotes(electionTerm int, votes chan bool) State {
	cntYes := 1
	cntNo := 0
	voteTimeout := time.NewTimer(time.Second)
	defer voteTimeout.Stop()
	for cntYes+cntNo < len(rf.peers) {
		rf.mu.Lock()
		if rf.state == Candidate && rf.currentTerm == electionTerm {
			rf.mu.Unlock()
			select {
			case vote := <-votes:
				if vote {
					cntYes++
				} else {
					cntNo++
				}
				if cntYes > len(rf.peers)/2 {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.state == Candidate && rf.currentTerm == electionTerm {
						rf.beLeader()
						return Leader
					} else {
						slog.Debug(
							"VOTE",
							"PEER", rf.me,
							"EVENT", "ELECTION_WRONG_TERM",
							"TERM", rf.currentTerm,
						)
						return Follower
					}
				} else if cntNo > len(rf.peers)/2 {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					slog.Debug(
						"VOTE",
						"PEER", rf.me,
						"EVENT", "ELECTION_LOST",
						"TERM", rf.currentTerm,
					)
					rf.beFollower(rf.currentTerm)
					return Follower
				}
			case <-voteTimeout.C:
				rf.mu.Lock()
				defer rf.mu.Unlock()
				slog.Debug(
					"TIME",
					"PEER", rf.me,
					"EVENT", "ELECTION_TIMEOUT",
					"TERM", rf.currentTerm,
				)
				rf.beFollower(rf.currentTerm)
				return Follower
			}
		} else {
			rf.mu.Unlock()
			return Follower
		}
	}
	return Follower
}

func (rf *Raft) election() State {
	rf.mu.Lock()
	slog.Debug(
		"VOTE",
		"PEER", rf.me,
		"EVENT", "ELECTION_START",
		"TERM", rf.currentTerm+1,
	)
	votes := make(chan bool, len(rf.peers))
	electionTerm := rf.beCandidate()
	rf.sendRequestVoteAll(electionTerm, votes)
	rf.mu.Unlock()
	return rf.waitVotes(electionTerm, votes)
}

func (rf *Raft) sendAppendAll(term int) {
	for server := range rf.peers {
		if server != rf.me {
			args := AppendEntriesArgs{
				Term:         term,
				LeaderID:     rf.me,
				Entries:      append([]LogEntry{}, rf.log[rf.nextIndex[server]:]...),
				PrevLogIndex: rf.nextIndex[server] - 1,
				PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
				LeaderCommit: rf.commitIndex,
			}
			reply := AppendEntriesReply{}
			slog.Debug(
				"APPEND",
				"PEER", rf.me,
				"EVENT", "APPEND_ENTRIES",
				"TO", server,
			)
			go rf.sendAppendEntries(server, &args, &reply)
		}
	}
}

func (rf *Raft) heartBeat() {
	rf.mu.Lock()
	leaderTerm := rf.currentTerm
	rf.mu.Unlock()
	for {
		rf.mu.Lock()
		if rf.state == Leader && rf.currentTerm == leaderTerm {
			rf.sendAppendAll(leaderTerm)
			rf.mu.Unlock()
			// Do later
			time.Sleep(150 * time.Millisecond)
		} else {
			rf.mu.Unlock()
			go rf.ticker()
			return
		}
	}
}

func (rf *Raft) ticker() {
	timer := time.NewTimer(electionTimeout())
	defer timer.Stop()
	for {
	Loop:
		for {
			select {
			case leaderID := <-rf.heartBeatCh:
				slog.Debug(
					"APPEND",
					"PEER", rf.me,
					"EVENT", "HEARTBEAT_RECEIVED",
					"FROM", leaderID,
				)
				break Loop
			case <-timer.C:
				if rf.election() == Leader {
					return
				}
				break Loop
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(electionTimeout())
	}
}

func (rf *Raft) beFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.persist()
	slog.Debug(
		"STEP_DOWN",
		"PEER", rf.me,
		"EVENT", "STEP_DOWN",
		"TERM", rf.currentTerm,
	)
}

func (rf *Raft) retryBack(server int, args *AppendEntriesArgs) {
	slog.Debug(
		"APPEND",
		"PEER", rf.me,
		"EVENT", "APPEND_ENTRIES_RETRY",
		"FOR", server,
	)
	for index := args.PrevLogIndex - 1; index >= 0; index-- {
		if index == 0 {
			rf.nextIndex[server] = 1
			break
		}
		if rf.log[index].Term != rf.log[index+1].Term {
			rf.nextIndex[server] = index
			break
		}
	}
	args.PrevLogIndex = rf.nextIndex[server] - 1
	args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	args.Entries = append([]LogEntry{}, rf.log[rf.nextIndex[server]:]...)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	for {
		if ok := rf.peers[server].Call("Raft.AppendEntries", args, reply); ok {
			rf.mu.Lock()
			if rf.state == Leader && rf.currentTerm == args.Term {
				if reply.Success {
					defer rf.mu.Unlock()
					// Update nextIndex and matchIndex for the server
					rf.nextIndex[server] = max(args.PrevLogIndex+len(args.Entries)+1, rf.nextIndex[server])
					rf.matchIndex[server] = max(rf.matchIndex[server], rf.nextIndex[server]-1)
					return
				} else if reply.Term > args.Term {
					defer rf.mu.Unlock()
					rf.beFollower(reply.Term)
					return
				} else {
					rf.retryBack(server, args)
					rf.mu.Unlock()
				}
			} else {
				rf.mu.Unlock()
				return
			}
		} else {
			return
		}
	}
}

func (rf *Raft) applyLogEntries(start, end int) {
	if start <= end {
		for i := start; i <= end; i++ {
			rf.lastApplied = i
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			}
		}
		rf.commitIndex = end
	}
}

func (rf *Raft) waitMajorityAgreement(index, term int) {
	var cntYes int
	for {
		rf.mu.Lock()
		if rf.currentTerm == term && rf.state == Leader {
			cntYes = 1
			for server := range rf.peers {
				if server != rf.me && rf.matchIndex[server] >= index {
					cntYes++
				}
			}
			rf.mu.Unlock()
			if cntYes > len(rf.peers)/2 {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				slog.Debug(
					"AGREEMENT",
					"PEER", rf.me,
					"EVENT", "AGREEMENT_REACHED",
					"INDEX", index,
					"TERM", term,
				)
				rf.applyLogEntries(rf.commitIndex+1, index)
				return
			}
			time.Sleep(50 * time.Millisecond)
		} else {
			rf.mu.Unlock()
			return
		}
	}
}

func (rf *Raft) startAgreement(index, term int) {
	rf.mu.Lock()
	slog.Debug(
		"AGREEMENT",
		"PEER", rf.me,
		"EVENT", "START_AGREEMENT",
		"INDEX", index,
		"TERM", term,
	)
	rf.sendAppendAll(term)
	rf.mu.Unlock()
	rf.waitMajorityAgreement(index, term)
}

// GetState returns the current term and whether this server
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.state == Leader
	return rf.currentTerm, isLeader
}

// Start is called by the service using Raft (e.g. a k/v server) to start agreement on the next command to be appended to Raft's log. if this server isn't the leader, returns false. otherwise start the agreement and return immediately. there is no guarantee that this command will ever be committed to the Raft log, since the leader may fail or lose an election.
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
func (rf *Raft) Start(command any) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader {
		rf.log = append(rf.log, LogEntry{
			Command: command,
			Term:    rf.currentTerm,
		})
		rf.persist()
		index := rf.lastLogIndex()
		term := rf.currentTerm
		go rf.startAgreement(index, term)
		return index, term, true
	} else {
		return -1, rf.currentTerm, false
	}
}

// PersistBytes returns the number of bytes in Raft's persisted log. This function is useful for monitoring the size of the log and ensuring that it does not grow too large, which could impact performance and resource usage.
// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// Snapshot is called by the service to tell Raft that it has created a snapshot that has all information up to and including index. Raft should now trim its log as much as possible. This function is used to manage the size of the log and ensure that it does not grow too large, which could impact performance and resource usage.
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
}

// Make must return quickly, so it should start goroutines
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	slog.Debug(
		"INIT",
		"PEER", me,
		"EVENT", "PEER_INIT",
	)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.heartBeatCh = make(chan int, 1)
	rf.state = Follower
	rf.log = make([]LogEntry, 1) // log starts at index 1
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
