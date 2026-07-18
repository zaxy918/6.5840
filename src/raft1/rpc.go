package raft

import "log/slog"

type LogEntry struct {
	Term int
	Cmd  string
}

type RequestVoteArgs struct {
	Term         int // Candidate's term
	CandidateId  int
	LastLogIndex int // Index of the candidate's last log entry
	LastLogTerm  int // Term of the candidate's last log entry
}

type RequestVoteReply struct {
	Term        int  // currentTerm
	VoteGranted bool // true if the cnadidate received vote
}

// shouldGrantVote returns true if the candidate has the newest state
func (rf *Raft) shouldGrantVote(args *RequestVoteArgs) bool {
	if args.Term <= rf.currentTerm {
		return false
	}
	if args.LastLogTerm < func() int {
		if len(rf.log) > 0 {
			return rf.log[len(rf.log)-1].Term
		}
		return 0
	}() {
		return false
	}
	if args.LastLogIndex < len(rf.log) {
		return false
	}
	return true
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
	slog.Debug("handling RequestVote RPC",
		"peer_id", rf.me,
		"local_term", rf.currentTerm,
		"candidate_id", args.CandidateId,
		"candidate_term", args.Term,
		"candidate_last_log_index", args.LastLogIndex,
		"candidate_last_log_term", args.LastLogTerm,
	)
	if rf.shouldGrantVote(args) {
		slog.Debug("granting vote to candidate",
			"peer_id", rf.me,
			"candidate_id", args.CandidateId,
			"term", rf.currentTerm,
		)
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		if rf.state != Follower {
			slog.Debug("the local state is not follower, changing to follower",
				"peer_id", rf.me,
				"candidate_id", args.CandidateId,
				"term", rf.currentTerm,
			)
			rf.state = Follower
		}
		*reply = RequestVoteReply{Term: rf.currentTerm, VoteGranted: true}
	} else {
		slog.Debug("rejecting vote request",
			"peer_id", rf.me,
			"candidate_id", args.CandidateId,
			"candidate_term", args.Term,
			"local_term", rf.currentTerm,
			"candidate_last_log_index", args.LastLogIndex,
			"candidate_last_log_term", args.LastLogTerm,
		)
		*reply = RequestVoteReply{Term: rf.currentTerm, VoteGranted: false}
	}
	// switch {
	// case args.Term <= rf.getCurTerm():
	// 	slog.Debug("rejecting vote request because the candidate term is stale",
	// 		"peer_id", rf.me,
	// 		"candidate_id", args.CandidateId,
	// 		"candidate_term", args.Term,
	// 		"local_term", rf.getCurTerm(),
	// 	)
	// 	*reply = RequestVoteReply{Term: rf.getCurTerm(), VoteGranted: false}
	// case args.LastLogTerm < rf.getLastLogTerm():
	// 	slog.Debug("rejecting vote request because the candidate log is older",
	// 		"peer_id", rf.me,
	// 		"candidate_id", args.CandidateId,
	// 		"candidate_last_log_term", args.LastLogTerm,
	// 		"local_last_log_term", rf.getLastLogTerm(),
	// 	)
	// 	*reply = RequestVoteReply{Term: rf.getCurTerm(), VoteGranted: false}
	// case args.LastLogIndex < rf.getLastLogIndex():
	// 	slog.Debug("rejecting vote request because the candidate log is shorter",
	// 		"peer_id", rf.me,
	// 		"candidate_id", args.CandidateId,
	// 		"candidate_last_log_index", args.LastLogIndex,
	// 		"local_last_log_index", rf.getLastLogIndex(),
	// 	)
	// 	*reply = RequestVoteReply{Term: rf.getCurTerm(), VoteGranted: false}
	// default:
	// 	rf.setVotedFor(args.CandidateId)
	// 	rf.setCurTerm(args.Term)
	// 	rf.setState(Follower)
	// 	slog.Debug("granting vote to candidate",
	// 		"peer_id", rf.me,
	// 		"candidate_id", args.CandidateId,
	// 		"term", rf.getCurTerm(),
	// 	)
	// 	*reply = RequestVoteReply{Term: rf.getCurTerm(), VoteGranted: true}
	// }
}

type AppendEntriesArgs struct {
	Term         int        // Leader's term
	LeaderId     int        // So follower can redirect clients
	PrevLogIndex int        // Index of log entry immediately preceding new ones
	PrevLogTerm  int        // Term of prevLogIndex entry
	Entries      []LogEntry // Log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // Leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int // currentTerm
	Success bool
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
	slog.Debug("handling AppendEntries RPC",
		"peer_id", rf.me,
		"local_term", rf.currentTerm,
		"state", rf.state,
		"leader_id", args.LeaderId,
		"leader_term", args.Term,
	)
	if args.Term >= rf.currentTerm {
		slog.Debug("AppendEntries from a new leader",
			"peer_id", rf.me,
			"leader_id", args.LeaderId,
			"leader_term", args.Term,
			"local_term", rf.currentTerm,
		)
		if rf.state != Follower {
			slog.Debug("the local state is not follower, changing to follower",
				"peer_id", rf.me,
				"leader_id", args.LeaderId,
				"leader_term", args.Term,
				"local_term", rf.currentTerm,
			)
			rf.currentTerm = args.Term
			rf.leaderId = args.LeaderId
			rf.state = Follower
			*reply = AppendEntriesReply{Term: rf.currentTerm, Success: false}
		} else {
			select {
			case rf.heartBeatCh <- args.LeaderId:
			default:
			}
			slog.Debug("AppendEntries from a new leader, the local state is follower, accepting the request",
				"peer_id", rf.me,
				"leader_id", args.LeaderId,
				"leader_term", args.Term,
				"local_term", rf.currentTerm,
			)
			// TODO: Append the entries to the log
			rf.currentTerm = args.Term
			rf.leaderId = args.LeaderId
			rf.state = Follower
			if len(args.Entries) > 0 {
			}
			*reply = AppendEntriesReply{Term: rf.currentTerm, Success: true}
		}
	} else {
		slog.Debug("AppendEntries from an old leader",
			"peer_id", rf.me,
			"leader_id", args.LeaderId,
			"leader_term", args.Term,
			"local_term", rf.currentTerm,
		)
		*reply = AppendEntriesReply{Term: rf.currentTerm, Success: false}
	}
}
