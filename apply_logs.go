package traft

import (
	"context"
)

func (r *raftNode) listenApplyLogsRequest(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.applyC:
			r.applyLogs()
		}
	}
}

func (r *raftNode) applyLogs() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lastApplied >= r.commitIndex {
		return
	}

	for i := 1; i < len(r.walogs); i++ {
		if r.walogs[i].Index > r.commitIndex {
			break
		}
		_ = r.stateMachine.ApplyLog(r.walogs[i].Data)
		r.lastApplied++
	}
	r.cutoffLogsByIndex(r.lastApplied)
}
