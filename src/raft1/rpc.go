package raft

import (
	"log/slog"

	"6.5840/raftapi"
)

type LogEntry struct {
	Term    int
	Command any
}

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

type AppendEntriesArgs struct {
	Term         int        // Leader's term
	LeaderID     int        // So follower can redirect clients
	PrevLogIndex int        // Index of log entry immediately preceding new ones
	PrevLogTerm  int        // Term of prevLogIndex entry
	Entries      []LogEntry // Log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // Leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int // currentTerm
	Success bool
}

type InstallSnapshotArgs struct {
	Term              int    // Leader's term
	LeaderID          int    // So follower can redirect clients
	LastIncludedIndex int    // Index of the last log entry to be installed
	LastIncludedTerm  int    // Term of the last log entry to be installed
	Data              []byte // Snapshot data
}

type InstallSnapshotReply struct {
	Term int // currentTerm
}

/*
 * The caller is a old candidate or a new candidate. The receiver could be in any state: follower, candidate, or leader.
 *
 * If the caller is a old candidate, reject the request and return the current term.
 *
 * If the caller is a new candidate, accept the request and update the current term and votedFor.
 * If the receiver is not a follower, change the state to follower.
 * If the receiver is a follower, accept the request and return success.
 */
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm || rf.votedFor != -1 && rf.currentTerm == args.Term {
		slog.Debug(
			"VOTE",
			"PEER", rf.me,
			"EVENT", "STALE_CANDIDATE",
		)
		*reply = RequestVoteReply{Term: rf.currentTerm, VoteGranted: false}
		return
	}
	upToDate :=
		args.LastLogTerm > rf.lastLogTerm() ||
			(args.LastLogTerm == rf.lastLogTerm() &&
				args.LastLogIndex >= rf.lastLogIndex())
	rf.votedFor = -1
	rf.beFollower(args.Term)
	if upToDate {
		slog.Debug(
			"VOTE",
			"PEER", rf.me,
			"EVENT", "GRANT_VOTE",
			"TO", args.CandidateID,
		)
		rf.votedFor = args.CandidateID
		rf.persist()
		select {
		case rf.heartBeatCh <- args.CandidateID:
		default:
		}
		*reply = RequestVoteReply{Term: rf.currentTerm, VoteGranted: true}
	} else {
		slog.Debug(
			"VOTE",
			"PEER", rf.me,
			"EVENT", "REJECT_VOTE",
			"REASON", "NOT_UP_TO_DATE",
		)
		*reply = RequestVoteReply{Term: rf.currentTerm, VoteGranted: false}
	}
}

/*
 * Two kinds of leader could send this RPC: a old leader or a new leader.
 * The receiver could be in any state: follower, candidate, or leader.
 *
 * If the caller is an old leader, reject the request and return the current term.
 *
 * If the caller is a new leader, accept the request and update the current term and leader id.
 * If the receiver is not a follower, change the state to follower.
 * If the receiver is a follower, accept the request and return success.
 */
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term >= rf.currentTerm {
		select {
		case rf.heartBeatCh <- args.LeaderID:
		default:
		}
		if rf.state != Follower {
			rf.beFollower(args.Term)
			rf.leaderID = args.LeaderID
			*reply = AppendEntriesReply{Term: rf.currentTerm, Success: false}
		} else {
			rf.currentTerm = args.Term
			rf.persist()
			rf.leaderID = args.LeaderID
			if args.PrevLogIndex > rf.lastLogIndex() || rf.Log(args.PrevLogIndex).Term != args.PrevLogTerm {
				slog.Debug(
					"APPEND",
					"PEER", rf.me,
					"EVENT", "SHOULD_RETRY",
				)
				*reply = AppendEntriesReply{Term: rf.currentTerm, Success: false}
			} else {
				slog.Debug(
					"APPEND",
					"PEER", rf.me,
					"EVENT", "APPENDING",
				)
				if len(args.Entries) > 0 {
					for i, entry := range args.Entries {
						if args.PrevLogIndex+i+1 > rf.lastLogIndex() || rf.Log(args.PrevLogIndex+i+1).Term != entry.Term {
							rf.log = append(rf.Logs(rf.index0, args.PrevLogIndex+i+1), args.Entries[i:]...)
						}
					}
				}
				rf.persist()
				*reply = AppendEntriesReply{Term: rf.currentTerm, Success: true}
				rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
				rf.cond.Broadcast()
			}
		}
	} else {
		slog.Debug(
			"APPEND",
			"PEER", rf.me,
			"EVENT", "OLD_LEADER",
		)
		*reply = AppendEntriesReply{Term: rf.currentTerm, Success: false}
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	slog.Debug(
		"SNAPSHOT",
		"PEER", rf.me,
		"EVENT", "INSTALLING",
	)
	// old leader
	if args.Term < rf.currentTerm {
		*reply = InstallSnapshotReply{Term: rf.currentTerm}
		return
	}
	// new leader rpc
	select {
	case rf.heartBeatCh <- args.LeaderID:
	default:
	}
	rf.beFollower(args.Term)
	// old snapshot
	if args.LastIncludedIndex <= rf.commitIndex || args.LastIncludedIndex <= rf.index0 {
		*reply = InstallSnapshotReply{Term: rf.currentTerm}
		return
	}
	if args.LastIncludedIndex <= rf.lastLogIndex() && args.LastIncludedTerm == rf.Log(args.LastIncludedIndex).Term {
		rf.log = append([]LogEntry{}, rf.Logs(args.LastIncludedIndex, rf.lastLogIndex()+1)...)
	} else {
		rf.log = []LogEntry{
			{Term: args.LastIncludedTerm},
		}
	}
	rf.index0 = args.LastIncludedIndex
	rf.snapshot = append([]byte{}, args.Data...)
	rf.commitIndex = args.LastIncludedIndex
	rf.pendingSnapshot = &raftapi.ApplyMsg{
		SnapshotValid: true,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
		Snapshot:      append([]byte{}, args.Data...),
	}
	rf.persist()
	*reply = InstallSnapshotReply{Term: rf.currentTerm}
	rf.cond.Broadcast()
}
