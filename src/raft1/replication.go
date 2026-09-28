package raft

import (
	"log/slog"
)

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

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// old leader
	if args.Term < rf.currentTerm {
		*reply = AppendEntriesReply{Term: rf.currentTerm, Success: false}
		return
	}
	// update heartbeat
	select {
	case rf.heartBeatCh <- args.LeaderID:
	default:
	}
	// become follower if not already
	if rf.state != Follower {
		rf.beFollower(args.Term)
		rf.leaderID = args.LeaderID
		*reply = AppendEntriesReply{Term: rf.currentTerm, Success: false}
		return
	}
	// update term and leaderID
	rf.currentTerm = args.Term
	rf.leaderID = args.LeaderID
	rf.persist()
	// not matching, retry
	if args.PrevLogIndex > rf.lastLogIndex() || rf.Log(args.PrevLogIndex).Term != args.PrevLogTerm {
		slog.Debug(
			"APPEND",
			"PEER", rf.me,
			"EVENT", "SHOULD_RETRY",
		)
		*reply = AppendEntriesReply{Term: rf.currentTerm, Success: false}
		return
	}
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

func (rf *Raft) replicator(server int) {
	for range rf.replicateCh[server] {
		for rf.replicate(server) {
		}
	}
}

func (rf *Raft) replicate(server int) bool {
	rf.mu.Lock()
	// not leader
	if rf.state != Leader {
		rf.mu.Unlock()
		return false
	}
	// too-left-behind
	if rf.nextIndex[server] <= rf.index0 {
		args := InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderID:          rf.me,
			LastIncludedIndex: rf.index0,
			LastIncludedTerm:  rf.Log(rf.index0).Term,
			Data:              append([]byte{}, rf.snapshot...),
		}
		rf.mu.Unlock()
		return rf.sendInstallSnapshot(server, &args, &InstallSnapshotReply{})
	}
	// normal append entries
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		Entries:      append([]LogEntry{}, rf.Logs(rf.nextIndex[server], rf.lastLogIndex()+1)...),
		PrevLogIndex: rf.nextIndex[server] - 1,
		PrevLogTerm:  rf.Log(rf.nextIndex[server] - 1).Term,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	return rf.sendAppendEntries(server, &args, &AppendEntriesReply{})
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// Send the append entries request
	if !rf.peers[server].Call("Raft.AppendEntries", args, reply) {
		return true
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !rf.isCurrentLeader(args.Term, reply.Term) {
		return false
	}
	// append not success
	if !reply.Success {
		rf.retryBack(server, args)
		return true
	}
	// Update nextIndex and matchIndex for the server
	rf.nextIndex[server] = max(args.PrevLogIndex+len(args.Entries)+1, rf.nextIndex[server])
	rf.matchIndex[server] = max(rf.matchIndex[server], rf.nextIndex[server]-1)
	rf.advanceCommitIndex()
	return rf.nextIndex[server] <= rf.lastLogIndex()
}

func (rf *Raft) notifyAllReplicators() {
	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}
		select {
		case rf.replicateCh[server] <- struct{}{}:
		default:
		}
	}
}

func (rf *Raft) retryBack(server int, args *AppendEntriesArgs) {
	slog.Debug(
		"APPEND",
		"PEER", rf.me,
		"EVENT", "APPEND_ENTRIES_RETRY",
		"FOR", server,
	)
	// too-left-behind
	if rf.nextIndex[server] <= rf.index0+1 {
		rf.nextIndex[server] = rf.index0
		return
	}
	// back off by one term
	for index := args.PrevLogIndex - 1; index >= rf.index0; index-- {
		if index == rf.index0 {
			rf.nextIndex[server] = rf.index0 + 1
			break
		}
		if rf.Log(index).Term != rf.Log(index+1).Term {
			rf.nextIndex[server] = index + 1
			break
		}
	}
	// update args
	args.PrevLogIndex = rf.nextIndex[server] - 1
	args.PrevLogTerm = rf.Log(args.PrevLogIndex).Term
	args.Entries = append([]LogEntry{}, rf.Logs(rf.nextIndex[server], rf.lastLogIndex()+1)...)
}

func (rf *Raft) advanceCommitIndex() {
	if rf.state != Leader {
		return
	}
	for index := rf.lastLogIndex(); index > rf.commitIndex; index-- {
		cnt := 1
		for server := range rf.peers {
			if server != rf.me && rf.matchIndex[server] >= index && rf.Log(index).Term == rf.currentTerm {
				cnt++
			}
		}
		if cnt > len(rf.peers)/2 {
			slog.Debug(
				"AGREEMENT",
				"PEER", rf.me,
				"EVENT", "ADVANCE_COMMIT_INDEX",
				"INDEX", index,
				"TERM", rf.currentTerm,
			)
			rf.commitIndex = index
			rf.cond.Broadcast()
			return
		}
	}
}
