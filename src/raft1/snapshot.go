package raft

import (
	"log/slog"

	"6.5840/raftapi"
)

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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply); !ok {
		slog.Debug(
			"SNAPSHOT",
			"PEER", rf.me,
			"EVENT", "INSTALL_SNAPSHOT_RPC_FAILURE",
			"TO", server,
		)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.beFollower(reply.Term)
		return
	}
	if rf.state != Leader || rf.currentTerm != args.Term {
		return
	}
	rf.matchIndex[server] = max(rf.matchIndex[server], args.LastIncludedIndex)
	rf.nextIndex[server] = max(rf.nextIndex[server], args.LastIncludedIndex+1)
}
