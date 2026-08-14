package rsm

import "log/slog"

type applyResult struct {
	op    Op
	value any
}

func (rsm *RSM) Reader() {
	for msg := range rsm.applyCh {
		// invalid commands
		if !msg.CommandValid {
			slog.Debug(
				"READ",
				"PEER", rsm.me,
				"EVENT", "INVALID_COMMAND",
				"OP", msg.Command.(Op).ID,
			)
			continue
		}
		slog.Debug(
			"READ",
			"PEER", rsm.me,
			"EVENT", "DO_OP",
			"OP", msg.Command.(Op).ID,
		)
		res := rsm.sm.DoOp(msg.Command.(Op).Request)
		rsm.mu.Lock()
		if resCh, ok := rsm.opResChs[msg.CommandIndex]; ok {
			delete(rsm.opResChs, msg.CommandIndex)
			rsm.mu.Unlock()
			resCh <- applyResult{op: msg.Command.(Op), value: res}
			continue
		}
		rsm.mu.Unlock()
	}
}
