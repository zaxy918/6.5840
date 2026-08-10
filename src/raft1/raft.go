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

	"log/slog"
	"sync"
	"time"

	//	"6.5840/labgob"

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
	cond      *sync.Cond            // Condition variable to signal when the state changes
	peers     []*labrpc.ClientEnd   // RPC end points of all peers
	persister *tester.Persister     // Object to hold this peer's persisted state
	applyCh   chan raftapi.ApplyMsg // Channel to send newly committed log entries to the service (or tester)

	me              int   // this peer's index into peers[]
	currentTerm     int   // The term server has seen for the last time
	votedFor        int   // Candidate id that received vote
	leaderID        int   // The current leader's id
	state           State // The state of the server: Follower, Candidate or Leader
	log             []LogEntry
	commitIndex     int   // Index of the highest log entry known to be committed
	lastApplied     int   // Index of the last entry applied to the state machine
	nextIndex       []int // For each server, index of the next log entry to send to that server
	matchIndex      []int // For each server, index of the highest log entry known to be replicated on server
	heartBeatCh     chan int
	index0          int               // For snapshots
	snapshot        []byte            // The last snapshot
	pendingSnapshot *raftapi.ApplyMsg // The pending snapshot
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
	rf.cond = sync.NewCond(&rf.mu)
	rf.log = make([]LogEntry, 1) // log starts at index 1
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.commitIndex = rf.index0
	rf.lastApplied = rf.index0
	rf.snapshot = persister.ReadSnapshot()
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()
	return rf
}
