package raft

import (
	"log/slog"
	"math/rand"
	"time"
)

type RequestVoteArgs struct {
	Term         int // Candidate's term
	CandidateID  int
	LastLogIndex int // Index of the candidate's last log entry
	LastLogTerm  int // Term of the candidate's last log entry
}

type RequestVoteReply struct {
	Term        int  // currentTerm
	VoteGranted bool // true if the cnadidate received vote
}

// The election timeout is 500 ~ 800ms
func electionTimeout() time.Duration {
	return time.Duration((500 + rand.Int63()%300)) * time.Millisecond
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// old candidate
	if args.Term < rf.currentTerm || rf.votedFor != -1 && rf.currentTerm == args.Term {
		slog.Debug(
			"VOTE",
			"PEER", rf.me,
			"EVENT", "STALE_CANDIDATE",
		)
		*reply = RequestVoteReply{Term: rf.currentTerm, VoteGranted: false}
		return
	}
	// if candidate up to date
	upToDate :=
		args.LastLogTerm > rf.lastLogTerm() ||
			(args.LastLogTerm == rf.lastLogTerm() &&
				args.LastLogIndex >= rf.lastLogIndex())
	rf.votedFor = -1
	rf.beFollower(args.Term)
	// not up to date
	if !upToDate {
		*reply = RequestVoteReply{Term: rf.currentTerm, VoteGranted: false}
		return
	}
	// vote and update the vote status
	slog.Debug(
		"VOTE",
		"PEER", rf.me,
		"EVENT", "GRANT_VOTE",
		"TO", args.CandidateID,
	)
	select {
	case rf.heartBeatCh <- args.CandidateID:
	default:
	}
	rf.votedFor = args.CandidateID
	rf.persist()
	*reply = RequestVoteReply{Term: rf.currentTerm, VoteGranted: true}
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

func (rf *Raft) sendRequestVote(server int, votes chan bool, args *RequestVoteArgs, reply *RequestVoteReply) {
	if ok := rf.peers[server].Call("Raft.RequestVote", args, reply); !ok {
		return
	}
	votes <- reply.VoteGranted
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
			go rf.sendRequestVote(server, votes, &args, &reply)
		}
	}
}
