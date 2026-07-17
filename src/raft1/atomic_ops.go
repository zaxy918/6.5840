package raft

func (rf *Raft) getState() State {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

func (rf *Raft) getCurTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}

func (rf *Raft) getLastLogIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return len(rf.log)
}

func (rf *Raft) getLastLogTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(rf.log) > 0 {
		return rf.log[len(rf.log)-1].Term
	}
	return 0
}

func (rf *Raft) getLeaderId() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.leaderId
}

func (rf *Raft) setState(state State) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = state
}

func (rf *Raft) setCurTerm(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = term
}

func (rf *Raft) setVotedFor(candidateId int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = candidateId
}

func (rf *Raft) setLeaderId(leaderId int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.leaderId = leaderId
}
