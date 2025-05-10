package traft

import (
	"context"
	raftpb "github.com/asmile1559/traft/internal/apis/raft"
)

func (r *raftNode) applyStateMachine(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.applyC:
			r.applyLogToStateMachine()
		}
	}
}

func (r *raftNode) applyLogToStateMachine() {

	r.mu.RLock()
	if r.lastApplied >= r.commitIndex {
		r.mu.RUnlock()
		return
	}

	begin, _ := r.logOffset(r.lastApplied + 1)
	end, _ := r.logOffset(r.commitIndex + 1)
	logs := append(make([]*raftpb.LogEntry, 0), r.log[begin:end]...)
	r.mu.RUnlock()
	for _, log := range logs {
		_ = r.stateMachine.ApplyLog(log.Data)
	}
}
