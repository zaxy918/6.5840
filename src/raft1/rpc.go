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

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	slog.Debug("handling RequestVote RPC",
		"peer_id", rf.me,
		"state", rf.stateName(),
		"local_term", rf.getCurTerm(),
		"candidate_id", args.CandidateId,
		"candidate_term", args.Term,
		"candidate_last_log_index", args.LastLogIndex,
		"candidate_last_log_term", args.LastLogTerm,
		"local_last_log_index", rf.getLastLogIndex(),
		"local_last_log_term", rf.getLastLogTerm(),
	)
	switch {
	case args.Term <= rf.getCurTerm():
		slog.Debug("rejecting vote request because the candidate term is stale",
			"peer_id", rf.me,
			"candidate_id", args.CandidateId,
			"candidate_term", args.Term,
			"local_term", rf.getCurTerm(),
		)
		*reply = RequestVoteReply{Term: rf.getCurTerm(), VoteGranted: false}
	case args.LastLogTerm < rf.getLastLogTerm():
		slog.Debug("rejecting vote request because the candidate log is older",
			"peer_id", rf.me,
			"candidate_id", args.CandidateId,
			"candidate_last_log_term", args.LastLogTerm,
			"local_last_log_term", rf.getLastLogTerm(),
		)
		*reply = RequestVoteReply{Term: rf.getCurTerm(), VoteGranted: false}
	case args.LastLogIndex < rf.getLastLogIndex():
		slog.Debug("rejecting vote request because the candidate log is shorter",
			"peer_id", rf.me,
			"candidate_id", args.CandidateId,
			"candidate_last_log_index", args.LastLogIndex,
			"local_last_log_index", rf.getLastLogIndex(),
		)
		*reply = RequestVoteReply{Term: rf.getCurTerm(), VoteGranted: false}
	default:
		rf.setVotedFor(args.CandidateId)
		rf.setCurTerm(args.Term)
		rf.setState(Follower)
		slog.Debug("granting vote to candidate",
			"peer_id", rf.me,
			"candidate_id", args.CandidateId,
			"term", rf.getCurTerm(),
		)
		*reply = RequestVoteReply{Term: rf.getCurTerm(), VoteGranted: true}
	}
}

type AppendEntriesArgs struct {
	Term     int // Leader's term
	LeaderId int
	Entries  []LogEntry
}

type AppendEntriesReply struct {
	Term    int // currentTerm
	Success bool
}

// Handle the RPC call from the leader
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.getCurTerm() > args.Term {
		slog.Debug("rejecting AppendEntries because the leader is behind",
			"peer_id", rf.me,
			"leader_id", args.LeaderId,
			"leader_term", args.Term,
			"local_term", rf.getCurTerm(),
		)
		reply.Term = rf.getCurTerm()
		reply.Success = false
		return
	} else if rf.getState() == Leader {
		slog.Debug("rejecting AppendEntries because the peer is a old leader",
			"peer_id", rf.me,
			"leader_id", args.LeaderId,
			"leader_term", args.Term,
			"local_term", rf.getCurTerm(),
		)
		rf.setLeaderId(args.LeaderId)
		reply.Term = rf.getCurTerm()
		reply.Success = false
		return
	}
	slog.Debug("handling AppendEntries RPC",
		"peer_id", rf.me,
		"leader_id", args.LeaderId,
		"leader_term", args.Term,
		"local_term", rf.getCurTerm(),
		"state", rf.stateName(),
		"entry_count", len(args.Entries),
	)
	rf.heartBeatCh <- args.LeaderId
	rf.setCurTerm(args.Term)
	rf.setLeaderId(args.LeaderId)
	*reply = AppendEntriesReply{Term: rf.getCurTerm(), Success: true}
}
