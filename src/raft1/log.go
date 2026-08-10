package raft

type LogEntry struct {
	Term    int
	Command any
}

func (rf *Raft) lastLogIndex() int {
	return rf.index0 + len(rf.log) - 1
}

func (rf *Raft) lastLogTerm() int {
	return rf.log[rf.lastLogIndex()-rf.index0].Term
}

func (rf *Raft) Logs(start, end int) []LogEntry {
	return rf.log[start-rf.index0 : end-rf.index0]
}

func (rf *Raft) Log(index int) LogEntry {
	return rf.log[index-rf.index0]
}
