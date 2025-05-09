package traft

import raftpb "github.com/asmile1559/traft/internal/apis/raft"

// In this file, all functions designed lock-free because they are called by RPC Handler.
// RPC Handler is locked by raftNode.lock.

func (r *raftNode) lastLogIndex() uint64 {
	if len(r.log) == 0 && r.snapshot == nil {
		// this case is when the node is just started and no log entry is added
		return 0
	}
	if len(r.log) == 0 {
		// this case is when the node is just compacted and no log entry is added
		return r.snapshot.LastIncludedIndex
	}
	return r.log[len(r.log)-1].Index
}

func (r *raftNode) lastLogTerm() uint64 {
	if len(r.log) == 0 && r.snapshot == nil {
		// this case is when the node is just started and no log entry is added
		return 0
	}
	if len(r.log) == 0 {
		// this case is when the node is just compacted and no log entry is added
		return r.snapshot.LastIncludedTerm
	}
	return r.log[len(r.log)-1].Term
}

func (r *raftNode) logOffset(index uint64) (uint64, error) {
	if index <= r.snapshot.LastIncludedIndex {
		return 0, ErrInvalidIndex
	}

	if r.snapshot != nil {
		// 5 -> LastIncludedIndex
		// 10 -> index
		//      snapshot     <|>           r.log
		// [0, 1, 2, 3, 4, 5] | [6, 7, 8, 9, 10, 11, 12, ...]
		//                    | [0, 1, 2, 3,  4,  5,  6, ...]
		//                 A
		//         LastIncludedIndex
		// index = index - r.snapshot.LastIncludedIndex - 1
		index = index - r.snapshot.LastIncludedIndex - 1
	}
	return index, nil
}

// 获取指定索引的日志条目的任期
func (r *raftNode) getLogTerm(index uint64) (uint64, error) {
	if r.snapshot != nil && index <= r.snapshot.LastIncludedIndex {
		if index == r.snapshot.LastIncludedIndex {
			return r.snapshot.LastIncludedTerm, nil
		}
		return 0, ErrLogAlreadySnapshot
	}
	if index > r.lastLogIndex() {
		return 0, ErrLogOutOfRange
	}

	i, err := r.logOffset(index)
	if err != nil {
		return 0, err
	}
	return r.log[i].Term, nil
}

func (r *raftNode) lastIndexOf(term uint64) (uint64, error) {
	if len(r.log) == 0 {
		if r.snapshot != nil && term == r.snapshot.LastIncludedTerm {
			return r.snapshot.LastIncludedIndex, nil
		}
		return 0, ErrLogOutOfRange
	}

	for i := len(r.log) - 1; i >= 0; i-- {
		if r.log[i].Term == term {
			return r.log[i].Index, nil
		}
	}
	return 0, ErrLogOutOfRange
}

// 压缩日志，保留索引大于给定索引的日志条目
func (r *raftNode) compactLog() (*raftpb.Snapshot, error) {
	// 当前已经交由状态机执行的日志的索引和对应的
	index := r.lastApplied
	term, _ := r.getLogTerm(index)

	snapshot := &raftpb.Snapshot{
		LastIncludedIndex: index,
		LastIncludedTerm:  term,
		Data:              nil,
	}
	// 获取当前状态机的状态
	snapshotData := r.stateMachine.TakeSnapshot()
	snapshot.Data = snapshotData
	// 更新节点的快照
	r.snapshot = snapshot
	// TODO: persist snapshot to disk

	i, err := r.logOffset(index)
	if err != nil {
		return nil, err
	}
	// update the log
	r.log = append(make([]*raftpb.LogEntry, 0), r.log[i+1:]...)
	return snapshot, nil
}

// 截断日志，保留索引小于给定索引的日志条目
func (r *raftNode) truncateLog(index uint64) error {
	if r.snapshot != nil && index < r.snapshot.LastIncludedIndex {
		return ErrLogAlreadySnapshot
	}
	if index > r.lastLogIndex() {
		return ErrLogOutOfRange
	}
	if index == r.lastLogIndex() {
		// nothing to do
		return nil
	}

	i, err := r.logOffset(index)
	if err != nil {
		return err
	}
	r.log = append(make([]*raftpb.LogEntry, 0), r.log[:i+1]...)
	return nil
}

// 检查日志条目是否匹配
func (r *raftNode) checkLogMatch(prevLogTerm, prevLogIndex uint64) (bool, *raftpb.LogEntry, error) {
	term, err := r.getLogTerm(prevLogIndex)
	if err != nil {
		// use snapshot to recover
		return false, nil, err
	}

	if term != prevLogTerm {
		// log mismatch, rematch by leader next time
		return false, &raftpb.LogEntry{
			Index: prevLogIndex,
			Term:  term,
		}, ErrLogConflict
	}

	if r.lastLogIndex() > prevLogIndex {
		// log out of range, need to resend
		return true, &raftpb.LogEntry{
			Index: prevLogIndex,
			Term:  term,
		}, ErrNeedTruncate
	}
	return true, nil, nil
}
