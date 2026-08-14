package rsm

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Op struct {
	Me      int
	ID      uint64
	Request any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine

	opID     atomic.Uint64
	opResChs map[int]chan applyResult
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		opResChs:     make(map[int]chan applyResult),
	}
	go rsm.Reader()
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	term, isLeader := rsm.rf.GetState()
	// not leader
	if !isLeader {
		slog.Debug(
			"SUBMIT",
			"PEER", rsm.me,
			"EVENT", "NOT_LEADER",
		)
		return rpc.ErrWrongLeader, nil
	}
	// construct op
	op := Op{
		Me:      rsm.me,
		ID:      rsm.opID.Add(1),
		Request: req,
	}
	// start raft agreement
	slog.Debug(
		"SUBMIT",
		"PEER", rsm.me,
		"EVENT", "START",
		"OP", op.ID,
	)
	// start the operation
	rsm.mu.Lock()
	index, nterm, isLeader := rsm.rf.Start(op)
	if nterm != term || !isLeader {
		slog.Debug(
			"SUBMIT",
			"PEER", rsm.me,
			"EVENT", "LEADER_CHANGE",
			"OP", op.ID,
		)
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	// create a channel for the result
	resCh := make(chan applyResult, 1)
	rsm.opResChs[index] = resCh
	rsm.mu.Unlock()
	// wait for the result
	return rsm.waitApply(term, op, resCh)
}

func (rsm *RSM) waitApply(term int, op Op, resCh chan applyResult) (rpc.Err, any) {
	timer := time.NewTimer(500 * time.Millisecond)
	defer timer.Stop()
	for {
		select {
		case res := <-resCh:
			if res.op.ID != op.ID || res.op.Me != op.Me {
				slog.Debug(
					"SUBMIT",
					"PEER", rsm.me,
					"EVENT", "WRONG_OP",
					"OP", op.ID,
					"RES", res.op.ID,
				)
				return rpc.ErrWrongLeader, nil
			}
			slog.Debug(
				"SUBMIT",
				"PEER", rsm.me,
				"EVENT", "SUBMIT_SUCCESS",
				"OP", op.ID,
			)
			return rpc.OK, res.value
		case <-timer.C:
			timer.Reset(500 * time.Millisecond)
			if nterm, isLeader := rsm.rf.GetState(); nterm != term || !isLeader {
				slog.Debug(
					"SUBMIT",
					"PEER", rsm.me,
					"EVENT", "LEADER_CHANGE",
					"OP", op.ID,
				)
				return rpc.ErrWrongLeader, nil
			}
		}
	}
}
