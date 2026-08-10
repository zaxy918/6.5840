package raft

import (
	"log/slog"
)

// GetState returns the current term and whether this server
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.state == Leader
	return rf.currentTerm, isLeader
}

// Start is called by the service using Raft (e.g. a k/v server) to start agreement on the next command to be appended to Raft's log. if this server isn't the leader, returns false. otherwise start the agreement and return immediately. there is no guarantee that this command will ever be committed to the Raft log, since the leader may fail or lose an election.
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader {
		rf.log = append(rf.log, LogEntry{
			Command: command,
			Term:    rf.currentTerm,
		})
		rf.persist()
		index := rf.lastLogIndex()
		term := rf.currentTerm
		go rf.startAgreement(index, term)
		return index, term, true
	} else {
		return -1, rf.currentTerm, false
	}
}

// PersistBytes returns the number of bytes in Raft's persisted log. This function is useful for monitoring the size of the log and ensuring that it does not grow too large, which could impact performance and resource usage.
// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// Snapshot is called by the service to tell Raft that it has created a snapshot that has all information up to and including index. Raft should now trim its log as much as possible. This function is used to manage the size of the log and ensure that it does not grow too large, which could impact performance and resource usage.
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	slog.Debug(
		"SNAPSHOT",
		"PEER", rf.me,
		"EVENT", "SNAPSHOT_CREATED",
		"INDEX", index,
		"RF_INDEX", rf.index0,
	)
	if index <= rf.index0 || index > rf.commitIndex {
		return
	}
	rf.snapshot = append([]byte{}, snapshot...)
	rf.log = append([]LogEntry{}, rf.Logs(index, rf.lastLogIndex()+1)...)
	rf.index0 = index
	rf.persist()
}
