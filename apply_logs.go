package traft

import (
	"context"
	"fmt"
)

const (
	MaxApplyLogs = 10
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
	r.logger.Debug(fmt.Sprintf("Extracted log entries: %v", entries))
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
	r.logger.Debug(fmt.Sprintf("Last applied entries index: %d", r.lastApplied))
	if Debug {
		if r.lastApplied-r.walogs[0].Index >= 3 {
			r.logger.Debug("Compacting logs")
			_ = r.compactLog()
			//r.cutoffLogsByIndex(r.lastApplied)
		}
	} else {
		if r.lastApplied-r.walogs[0].Index >= MaxApplyLogs {
			r.logger.Debug("Compacting logs")
			_ = r.compactLog()
			//r.cutoffLogsByIndex(r.lastApplied)
		}
	}
	//r.cutoffLogsByIndex(r.lastApplied)
}
