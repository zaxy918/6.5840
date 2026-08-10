package raft

import "6.5840/raftapi"

func (rf *Raft) applier() {
	for {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex && rf.pendingSnapshot == nil {
			rf.cond.Wait()
		}
		if rf.pendingSnapshot != nil {
			msg := *rf.pendingSnapshot
			rf.lastApplied = msg.SnapshotIndex
			rf.pendingSnapshot = nil
			rf.mu.Unlock()
			rf.applyCh <- msg
			continue
		}
		start := rf.lastApplied + 1
		end := rf.commitIndex
		entries := append([]LogEntry{}, rf.Logs(start, end+1)...)
		rf.lastApplied = end
		rf.mu.Unlock()
		for i, entry := range entries {
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: start + i,
			}
		}
	}
}
