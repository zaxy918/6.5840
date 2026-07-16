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
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *tester.Persister   // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	currentTerm int                 // The term server has seen for the last time
	votedFor    int                 // Candidate id that received vote
	leaderId    int                 // The current leader's id
	state       State               // The state of the server: Follower, Candidate or Leader
	heartBeatCh chan int
}

func (rf *Raft) getState() State {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

func (rf *Raft) getCurTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}

func (rf *Raft) setState(state State) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = state
}

func (rf *Raft) setCurTerm(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	isLeader := false
	if rf.getState() == Leader {
		isLeader = true
	}
	return rf.getCurTerm(), isLeader
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

type RequestVoteArgs struct {
	Term        int // Candidate's term
	CandidateId int
}

type RequestVoteReply struct {
	Term        int  // currentTerm
	VoteGranted bool // true if the cnadidate received vote
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
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
func (rf *Raft) sendRequestVote(server int, votes chan bool, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if ok := rf.peers[server].Call("Raft.RequestVote", args, reply); ok {
		// RPC success
		if reply.Term == rf.getCurTerm() {
			// Get voted from the peer with the current term
			votes <- reply.VoteGranted
		} else {
			//TODO Not valid vote
		}
	} else {
		//TODO RPC failure
	}
	return true
}

type AppendEntriesArgs struct {
	// 3A
	Term     int // Leader's term
	LeaderId int
}

type AppendEntriesReply struct {
	// 3A
	Term    int // currentTerm
	Success bool
}

// Handle the RPC call from the leader
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Not the new leader
	if rf.getCurTerm() < args.Term {
		reply.Success = false
		return
	}
	slog.Debug("AppendEntries from leader", "LeaderId", args.LeaderId)
	*reply = AppendEntriesReply{Term: rf.currentTerm, Success: true}
	rf.heartBeatCh <- args.LeaderId
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

// Make this Raft a RPC server
func (rf *Raft) serve() {
	srv := labrpc.MakeServer()
	svc := labrpc.MakeService(rf)
	srv.AddService(svc)
}

// The election timeout is 300 ~ 450ms
func electionTimeout() time.Duration {
	return time.Duration((300 + rand.Int63()%150)) * time.Millisecond
}

func (rf *Raft) election() bool {
	// Update state
	rf.setCurTerm(rf.getCurTerm() + 1)
	rf.setState(Candidate)
	// Vote for itself
	rf.votedFor = rf.me
	// Issues RequestVote PRCS in parallel
	args := RequestVoteArgs{Term: rf.getCurTerm(), CandidateId: rf.me}
	reply := RequestVoteReply{}
	votes := make(chan bool, len(rf.peers))
	for server := range rf.peers {
		go rf.sendRequestVote(server, votes, &args, &reply)
	}
	// Count the votes
	cntYes := 0
	cntNo := 0
	voteTimeout := time.NewTimer(5 * time.Second)
	for {
		select {
		case vote := <-votes:
			// Whether it gets voted
			if vote {
				cntYes++
			} else {
				cntNo++
			}
			// Whether it wins the election
			if cntYes > len(rf.peers)/2 {
				return true
			} else if cntNo > len(rf.peers)/2 {
				return false
			}
		case <-rf.heartBeatCh:
			// A new leader is already elected
			return false
		case <-voteTimeout.C:
			// Haven't finish vote within 5 seconds
			return false
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if ok := rf.peers[server].Call("Raft.AppendEntries", args, reply); ok {
		//TODO RPC success
	} else {
		//TODO RPC failure
	}
}

func (rf *Raft) heartBeat() {
	for {
		// Send heart beat to peers in parallel
		for server := range rf.peers {
			if rf.me != server {
				args := AppendEntriesArgs{Term: rf.getCurTerm(), LeaderId: rf.me}
				reply := AppendEntriesReply{}
				go rf.sendAppendEntries(server, &args, &reply)
			}
		}
		// Do later
		time.Sleep(150 * time.Millisecond)
	}
}

func (rf *Raft) ticker() {
	timer := time.NewTimer(electionTimeout())
	defer timer.Stop()
	for {
	Loop:
		for {
			select {
			case <-rf.heartBeatCh:
				// Already heard heart beat  from the leader
				break Loop
			case <-timer.C:
				// Reach the election timeout, begin an election
				if rf.election() {
					// Win the election
					rf.leaderId = rf.me
					rf.setState(Leader)
					slog.Debug("Close heart beat chan", "leader", rf.me)
					close(rf.heartBeatCh)
					go rf.heartBeat()
					return
				} else {
					// Lose the election
					rf.setState(Follower)
					break Loop
				}
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
	slog.Debug("A Raft peer is making:", "id", me)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.setState(Follower)
	rf.heartBeatCh = make(chan int)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Start RPC service
	go rf.serve()

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
