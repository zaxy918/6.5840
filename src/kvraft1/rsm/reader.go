package rsm

import "log/slog"

type applyResult struct {
	op    Op
	value any
}

func (rsm *RSM) reader() {
	for msg := range rsm.applyCh {
		if msg.SnapshotValid {
			slog.Debug(
				"READ",
				"PEER", rsm.me,
				"EVENT", "SNAPSHOT",
			)
			rsm.mu.Lock()
			if msg.SnapshotIndex > rsm.lastAppliedIndex {
				rsm.lastAppliedIndex = msg.SnapshotIndex
				rsm.sm.Restore(msg.Snapshot)
			}
			rsm.mu.Unlock()
			continue
		}
		// invalid commands
		if !msg.CommandValid {
			continue
		}
		slog.Debug(
			"READ",
			"PEER", rsm.me,
			"EVENT", "DO_OP",
			"OP", msg.Command.(Op).ID,
		)
		rsm.mu.Lock()
		if msg.CommandIndex <= rsm.lastAppliedIndex {
			rsm.mu.Unlock()
			continue
		}
		res := rsm.sm.DoOp(msg.Command.(Op).Request)
		rsm.lastAppliedIndex = msg.CommandIndex
		if resCh, ok := rsm.opResChs[msg.CommandIndex]; ok {
			delete(rsm.opResChs, msg.CommandIndex)
			rsm.mu.Unlock()
			resCh <- applyResult{
				op:    msg.Command.(Op),
				value: res,
			}
			continue
		}
		rsm.mu.Unlock()
	}
}
