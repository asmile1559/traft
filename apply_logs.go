package traft

import (
	"context"
)

const (
	MaxApplyLogs = 100
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

	entries, err := r.extractLogEntries(r.lastApplied+1, r.commitIndex)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.Index > r.commitIndex {
			break
		}
		_ = r.stateMachine.ApplyLog(entry.Data)
		r.lastApplied++
	}
	if Debug {
		if r.lastApplied-r.walogs[0].Index >= 10 {
			r.cutoffLogsByIndex(r.lastApplied)
		}
	} else {
		if r.lastApplied-r.walogs[0].Index >= MaxApplyLogs {
			r.cutoffLogsByIndex(r.lastApplied)
		}
	}
	//r.cutoffLogsByIndex(r.lastApplied)
}
