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
	mu        sync.Mutex            // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd   // RPC end points of all peers
	persister *tester.Persister     // Object to hold this peer's persisted state
	applyCh   chan raftapi.ApplyMsg // Channel to send newly committed log entries to the service (or tester)

	me          int   // this peer's index into peers[]
	currentTerm int   // The term server has seen for the last time
	votedFor    int   // Candidate id that received vote
	leaderId    int   // The current leader's id
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

// The election timeout is 450 ~ 600ms
func electionTimeout() time.Duration {
	return time.Duration((450 + rand.Int63()%150)) * time.Millisecond
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
						rf.nextIndex = make([]int, len(rf.peers))
						for i := range rf.peers {
							rf.nextIndex[i] = rf.lastLogIndex() + 1
						}
						rf.matchIndex = make([]int, len(rf.peers))
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

func (rf *Raft) heartBeat() {
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
			for server := range rf.peers {
				if rf.me != server {
					args := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						LeaderCommit: rf.commitIndex,
						PrevLogIndex: rf.nextIndex[server] - 1,
						PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
						Entries:      append([]LogEntry{}, rf.log[rf.nextIndex[server]:]...),
					}
					reply := AppendEntriesReply{}
					go rf.sendAppendEntries(server, &args, &reply)
				}
			}
			rf.mu.Unlock()
			// Do later
			time.Sleep(150 * time.Millisecond)
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
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(electionTimeout())
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	for {
		if ok := rf.peers[server].Call("Raft.AppendEntries", args, reply); ok {
			rf.mu.Lock()
			if rf.state == Leader && rf.currentTerm == args.Term {
				if reply.Success {
					defer rf.mu.Unlock()
					slog.Debug("AppendEntries succeeded; updating nextIndex and matchIndex",
						"peer_id", rf.me,
						"target_peer", server,
						"next_index", rf.nextIndex[server],
						"match_index", rf.matchIndex[server],
					)
					rf.nextIndex[server] = max(args.PrevLogIndex+len(args.Entries)+1, rf.nextIndex[server])
					rf.matchIndex[server] = max(rf.matchIndex[server], rf.nextIndex[server]-1)
					return
				} else if reply.Term > args.Term {
					defer rf.mu.Unlock()
					slog.Debug("AppendEntries failed due to higher term; stepping down",
						"peer_id", rf.me,
						"target_peer", server,
						"reply_term", reply.Term,
						"local_term", rf.currentTerm,
					)
					rf.state = Follower
					rf.currentTerm = reply.Term
					return
				} else {
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
					slog.Debug("AppendEntries failed; decrementing nextIndex and retrying",
						"peer_id", rf.me,
						"target_peer", server,
						"next_index_before", rf.nextIndex[server],
						"local_term", rf.currentTerm,
						"entries_len", len(args.Entries),
					)
					rf.mu.Unlock()
				}
			} else {
				rf.mu.Unlock()
				return
			}
		} else {
			slog.Debug("AppendEntries RPC failed to send; retrying",
				"peer_id", rf.me,
				"target_peer", server,
				"local_term", args.Term,
			)
			return
		}
	}

}

func (rf *Raft) startAgreement(index, term int) {
	rf.mu.Lock()
	slog.Debug("starting agreement on new log entry",
		"peer_id", rf.me,
		"term", term,
		"leader_id", rf.me,
	)
	for server := range rf.peers {
		if server != rf.me {
			args := AppendEntriesArgs{
				Term:         term,
				LeaderId:     rf.me,
				Entries:      append([]LogEntry{}, rf.log[rf.nextIndex[server]:]...),
				PrevLogIndex: rf.nextIndex[server] - 1,
				PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
				LeaderCommit: rf.commitIndex,
			}
			reply := AppendEntriesReply{}
			slog.Debug("sending AppendEntries RPC to peer",
				"peer_id", rf.me,
				"target_peer", server,
				"term", term,
				"leader_id", rf.me,
				"prev_log_index", args.PrevLogIndex,
				"prev_log_term", args.PrevLogTerm,
				"leader_commit", args.LeaderCommit,
			)
			go rf.sendAppendEntries(server, &args, &reply)
		}
	}
	rf.mu.Unlock()
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
				slog.Debug("majority of peers have agreed; committing log entry",
					"peer_id", rf.me,
					"term", term,
					"leader_id", rf.me,
					"log_index", index,
				)
				if index > rf.commitIndex {
					for i := rf.commitIndex + 1; i <= index; i++ {
						rf.lastApplied = i
						rf.applyCh <- raftapi.ApplyMsg{
							CommandValid: true,
							Command:      rf.log[i].Command,
							CommandIndex: i,
						}
					}
					rf.commitIndex = index
				}
				return
			}
			time.Sleep(50 * time.Millisecond)
		} else {
			slog.Debug("agreement loop exiting; no longer leader or term changed",
				"peer_id", rf.me,
				"term", rf.currentTerm,
			)
			rf.mu.Unlock()
			return
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
		index := rf.lastLogIndex()
		term := rf.currentTerm
		go rf.startAgreement(index, term)
		return index, term, true
	} else {
		return -1, rf.currentTerm, false
	}
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
