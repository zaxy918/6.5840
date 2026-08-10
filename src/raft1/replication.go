package raft

import (
	"log/slog"
	"time"
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

func (rf *Raft) sendAppendAll(term int) {
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		if rf.nextIndex[server] > rf.index0 {
			args := AppendEntriesArgs{
				Term:         term,
				LeaderID:     rf.me,
				Entries:      append([]LogEntry{}, rf.Logs(rf.nextIndex[server], rf.lastLogIndex()+1)...),
				PrevLogIndex: rf.nextIndex[server] - 1,
				PrevLogTerm:  rf.Log(rf.nextIndex[server] - 1).Term,
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
		} else {
			args := InstallSnapshotArgs{
				Term:              term,
				LeaderID:          rf.me,
				LastIncludedIndex: rf.index0,
				LastIncludedTerm:  rf.Log(rf.index0).Term,
				Data:              append([]byte{}, rf.snapshot...),
			}
			reply := InstallSnapshotReply{}
			slog.Debug(
				"SNAPSHOT",
				"PEER", rf.me,
				"EVENT", "INSTALL_SNAPSHOT",
				"TO", server,
			)
			go rf.sendInstallSnapshot(server, &args, &reply)
		}
	}
}

func (rf *Raft) retryBack(server int, args *AppendEntriesArgs) bool {
	slog.Debug(
		"APPEND",
		"PEER", rf.me,
		"EVENT", "APPEND_ENTRIES_RETRY",
		"FOR", server,
	)
	if rf.nextIndex[server] <= rf.index0+1 {
		rf.nextIndex[server] = rf.index0
		return false
	}
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
	args.PrevLogIndex = rf.nextIndex[server] - 1
	args.PrevLogTerm = rf.Log(args.PrevLogIndex).Term
	args.Entries = append([]LogEntry{}, rf.Logs(rf.nextIndex[server], rf.lastLogIndex()+1)...)
	return true
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
					*reply = AppendEntriesReply{}
					if !rf.retryBack(server, args) {
						rf.mu.Unlock()
						return
					}
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
				if index > rf.commitIndex {
					rf.commitIndex = index
					rf.cond.Broadcast()
				}
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
