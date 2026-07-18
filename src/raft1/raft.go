package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"

	// debug
	"log/slog"

	_ "6.5840/config"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state

	me          int   // this peer's index into peers[]
	currentTerm int   // The term server has seen for the last time
	votedFor    int   // Candidate id that received vote
	leaderId    int   // The current leader's id
	state       State // The state of the server: Follower, Candidate or Leader
	log         []LogEntry
	heartBeatCh chan int
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.log)
}

func (rf *Raft) lastLogTerm() int {
	if len(rf.log) > 0 {
		return rf.log[len(rf.log)-1].Term
	}
	return 0
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := false
	if rf.state == Leader {
		isLeader = true
	}
	return rf.currentTerm, isLeader
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
		slog.Debug("received vote reply",
			"peer_id", rf.me,
			"target_peer", server,
			"local_term", electionTerm,
			"reply_term", reply.Term,
			"vote_granted", reply.VoteGranted,
		)
		if reply.Term == electionTerm {
			slog.Debug("vote reply is still valid for this term",
				"peer_id", rf.me,
				"target_peer", server,
				"vote_granted", reply.VoteGranted,
				"election_term", electionTerm,
			)
			votes <- reply.VoteGranted
		}
	}
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
func (rf *Raft) Start(command any) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// The election timeout is 300 ~ 450ms
func electionTimeout() time.Duration {
	return time.Duration((300 + rand.Int63()%150)) * time.Millisecond
}

func (rf *Raft) election() State {
	rf.mu.Lock()
	slog.Debug("starting election",
		"peer_id", rf.me,
		"term", rf.currentTerm,
		"leader_id", rf.leaderId,
	)
	electionTerm := rf.currentTerm + 1
	rf.currentTerm = electionTerm
	rf.votedFor = rf.me
	rf.state = Candidate
	slog.Debug("sending RequestVote RPCs in parallel",
		"peer_id", rf.me,
		"candidate_id", rf.me,
		"term", rf.currentTerm,
		"target_peers", len(rf.peers)-1,
	)
	votes := make(chan bool, len(rf.peers))
	for server := range rf.peers {
		if server != rf.me {
			args := RequestVoteArgs{
				Term:         electionTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.lastLogIndex(),
				LastLogTerm:  rf.lastLogTerm()}
			reply := RequestVoteReply{}
			go rf.sendRequestVote(server, electionTerm, votes, &args, &reply)
		}
	}
	rf.mu.Unlock()
	cntYes := 1
	cntNo := 0
	voteTimeout := time.NewTimer(500 * time.Millisecond)
	defer voteTimeout.Stop()
	for cntYes+cntNo < len(rf.peers) {
		rf.mu.Lock()
		if rf.state == Candidate && rf.currentTerm == electionTerm {
			rf.mu.Unlock()
			select {
			case vote := <-votes:
				slog.Debug("received vote",
					"peer_id", rf.me,
					"term", electionTerm,
					"vote_granted", vote,
				)
				if vote {
					cntYes++
				} else {
					cntNo++
				}
				if cntYes > len(rf.peers)/2 {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.state == Candidate && rf.currentTerm == electionTerm {
						slog.Debug("election won",
							"peer_id", rf.me,
							"term", rf.currentTerm,
							"leader_id", rf.currentTerm,
							"votes_yes", cntYes,
							"votes_no", cntNo,
						)
						rf.state = Leader
						rf.leaderId = rf.me
						// Start heartbeat goroutine
						go rf.heartBeat()
						return Leader
					} else {
						slog.Debug("election won but state changed; stepping down",
							"peer_id", rf.me,
							"term", rf.currentTerm,
							"leader_id", rf.leaderId,
							"votes_yes", cntYes,
							"votes_no", cntNo,
						)
						return Follower
					}
				} else if cntNo > len(rf.peers)/2 {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					slog.Debug("election lost",
						"peer_id", rf.me,
						"term", rf.currentTerm,
						"leader_id", rf.leaderId,
						"votes_yes", cntYes,
						"votes_no", cntNo,
					)
					rf.state = Follower
					return Follower
				}
			case <-voteTimeout.C:
				rf.mu.Lock()
				defer rf.mu.Unlock()
				slog.Debug("election timed out",
					"peer_id", rf.me,
					"term", rf.currentTerm,
					"leader_id", rf.leaderId,
				)
				rf.state = Follower
				return Follower
			}
		} else {
			rf.mu.Unlock()
			return Follower
		}
	}
	return Follower
}

func (rf *Raft) sendAppendEntries(server int, shutDown chan int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if ok := rf.peers[server].Call("Raft.AppendEntries", args, reply); ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		slog.Debug("received AppendEntries reply",
			"peer_id", rf.me,
			"target_peer", server,
			"reply_term", reply.Term,
			"reply_success", reply.Success,
			"local_term", args.Term,
		)
		if reply.Term > rf.currentTerm {
			slog.Debug("AppendEntries reply came from a newer term; stepping down",
				"peer_id", rf.me,
				"target_peer", server,
				"reply_term", reply.Term,
			)
			rf.state = Follower
			rf.currentTerm = reply.Term
			shutDown <- reply.Term
		} else if !reply.Success {
			//TODO: Handle the case when AppendEntries fails due to log inconsistency
			slog.Debug("AppendEntries failed",
				"peer_id", rf.me,
				"target_peer", server,
				"reply_term", reply.Term,
			)
		}
	}
}

func (rf *Raft) heartBeat() {
	timer := time.NewTimer(150 * time.Millisecond)
	rf.mu.Lock()
	leaderTerm := rf.currentTerm
	rf.mu.Unlock()
	for {
		rf.mu.Lock()
		if rf.state == Leader && rf.currentTerm == leaderTerm {
			slog.Debug("broadcasting heartbeat to peers",
				"peer_id", rf.me,
				"term", rf.currentTerm,
				"leader_id", rf.me,
			)
			rf.mu.Unlock()
			shutDown := make(chan int, len(rf.peers))
			for server := range rf.peers {
				if rf.me != server {
					args := AppendEntriesArgs{
						Term:     rf.currentTerm,
						LeaderId: rf.me,
						Entries:  nil}
					reply := AppendEntriesReply{}
					go rf.sendAppendEntries(server, shutDown, &args, &reply)
				}
			}
			// Do later
			timer.Reset(150 * time.Millisecond)
			select {
			case <-shutDown:
				slog.Debug("heartbeat loop received a newer-term notification; switching back to ticker",
					"peer_id", rf.me,
				)
				go rf.ticker()
				return
			case <-timer.C:
			}
		} else {
			slog.Debug("heartbeat loop exiting; no longer leader or term changed",
				"peer_id", rf.me,
				"term", rf.currentTerm,
			)
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
			case leaderId := <-rf.heartBeatCh:
				slog.Debug("received heartbeat from leader; staying follower",
					"peer_id", rf.me,
					"leader_id", leaderId,
				)
				break Loop
			case <-timer.C:
				slog.Debug("election timeout elapsed",
					"peer_id", rf.me,
				)
				if rf.election() == Leader {
					slog.Debug("election won; starting heartbeat loop",
						"peer_id", rf.me,
					)
					return
				}
				slog.Debug("election lost or timed out; staying follower",
					"peer_id", rf.me,
				)
				break Loop
			}
		}
		timer.Reset(electionTimeout())
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
func Make(peers []*labrpc.ClientEnd, me int, persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	slog.Debug("initializing raft peer",
		"peer_id", me,
		"peers", len(peers),
		"state", "follower",
	)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.heartBeatCh = make(chan int)
	rf.state = Follower
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
