package traft

import "context"

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
	defer r.mu.RUnlock()

	if r.lastApplied >= r.commitIndex {
		return
	}

	for i := r.lastApplied + 1; i <= r.commitIndex; i++ {
		oi, _ := r.logOffset(i)
		entry := r.log[oi]
		_ = r.stateMachine.ApplyLog(entry.Data)
	}
}
