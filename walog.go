package traft

import (
	"errors"

	raftpb "github.com/asmile1559/traft/internal/apis/raft"
)

// In this file, all functions designed lock-free because they are called by RPC Handler.
// RPC Handler is locked by raftNode.lock.

// r.walogs is writing ahead walogs. The position "0" is invalid.
// r.walogs[0] = *raftpb.LogEntry{Index: lastIncludeIndex, Term: lastIncludeTerm}

// walog@v1: use a slice to store walogs, the first element is a dummy walogs entry.
// TODO: walog@v2: use a cyclic queue to store walogs, auto compact when the walogs is full.

func (r *raftNode) lastLogIndex() uint64 {
	r.logger.Debug("lastLogIndex entry")
	defer r.logger.Debug("lastLogIndex exit")
	n := len(r.walogs)
	return r.walogs[n-1].Index
}

func (r *raftNode) lastLogTerm() uint64 {
	r.logger.Debug("lastLogTerm entry")
	defer r.logger.Debug("lastLogTerm exit")
	n := len(r.walogs)
	return r.walogs[n-1].Term
}

func (r *raftNode) logAtIndex(index uint64) (*raftpb.LogEntry, error) {
	r.logger.Debug("logAtIndex entry", "index", index)
	defer r.logger.Debug("logAtIndex exit", "index", index)
	if index == 0 {
		r.logger.Error("logAt index is at a invalid index '0'")
		return nil, ErrInvalidIndex
	}

	dummy := r.walogs[0]
	n := len(r.walogs)
	if dummy.Term == 0 {
		// not compacted yet
		if index >= uint64(n) {
			r.logger.Error("logAt index is out of range", "index", index, "max index", n-1)
			return nil, ErrLogOutOfRange
		}
		return r.walogs[index], nil
	}

	// compacted
	if index <= dummy.Index {
		r.logger.Error("logAt index is snapshot", "index", index, "lastIncludedIndex", dummy.Index)
		return nil, ErrLogAlreadySnapshot
	}
	// 5 -> dummy.Index
	// 10 -> index
	//      snapshot     <|> r.walogs
	// [0, 1, 2, 3, 4, 5] | [6, 7, 8, 9, 10, 11, 12, ...]
	//                [0,    1, 2, 3, 4, 5,  6,  7, ...]
	//                 A
	//         LastIncludedIndex
	// index = index - dummy.Index

	index = index - dummy.Index
	if index >= uint64(n) {
		r.logger.Error("logAt index is out of range", "index", index+dummy.Index, "lastIncludedIndex", uint64(n)+dummy.Index-1)
		return nil, ErrLogOutOfRange
	}
	return r.walogs[index], nil
}

// get the term of the walogs at the given index
func (r *raftNode) getLogTerm(index uint64) (uint64, error) {
	r.logger.Debug("getLogTerm entry", "index", index)
	defer r.logger.Debug("getLogTerm exit", "index", index)
	entry, err := r.logAtIndex(index)
	if err != nil {
		return 0, err
	}
	return entry.Term, nil
}

func (r *raftNode) lastIndexOf(term uint64) (uint64, error) {
	r.logger.Debug("lastIndexOf entry", "term", term)
	defer r.logger.Debug("lastIndexOf exit", "term", term)
	if term == 0 {
		r.logger.Error("lastIndexOf term is at a invalid term '0'")
		return 0, ErrInvalidTerm
	}

	n := len(r.walogs)

	if r.walogs[n-1].Term < term {
		// given term = 6
		// index:  5, 6, 7, 8, 9, 10, 11, 12, 13, 14
		// term:   1, 1, 2, 2, 2, 3,  3,  3,  4,  4
		//                                        Δ
		//                                      target
		r.logger.Warn("The last walogs term is less than the given term", "term", term, "lastLogTerm", r.walogs[n-1].Term)
		return r.walogs[n-1].Index, ErrTruncatedTerm
	}

	// given term = 2
	// index:  5, 6, 7, 8, 9, 10, 11, 12, 13, 14
	// term:   1, 1, 2, 2, 2, 3,  3,  3,  4,  4
	//                     Δ          <- - - idx
	//                   target
	// Start from the last walogs entry and iterate backward, excluding index 0.
	// Index 0 is excluded because it is a dummy walogs entry (see lines 11-12).
	for idx := n - 1; idx > 0; idx-- {
		if r.walogs[idx].Term == term {
			return r.walogs[idx].Index, nil
		}
	}

	// given term = 2
	// index:  ...,         5, | 6, 7, 8, 9, 10, 11, 12, 13, 14
	// term:    2, ...,     3, | 4, 5, 5, 5, 5,  5,  6,  6,  6
	//          Δ           Δ                        <- - - idx
	//        target      dummy
	r.logger.Debug("The given term is invalid", "term", term)
	return 0, ErrInvalidTerm
}

// compact walogs and generate snapshot
func (r *raftNode) compactLog() (*raftpb.Snapshot, error) {
	r.logger.Debug("compactLog entry")
	defer r.logger.Debug("compactLog exit")
	index := r.lastApplied
	if index == 0 {
		// no snapshot
		return &raftpb.Snapshot{
			LastIncludedIndex: 0,
			LastIncludedTerm:  0,
			Data:              nil,
		}, nil
	}

	// no err expected
	term, err := r.getLogTerm(index)
	if err != nil {
		r.logger.Error("failed to get walogs term", "err", err)
		// panic("no error expected, please check the code!!!")
		return nil, err
	}

	snapshot := &raftpb.Snapshot{
		LastIncludedIndex: index,
		LastIncludedTerm:  term,
	}

	// take a snapshot of the state machine
	// no err expected, please check the code!!!
	snapshotData, err := r.stateMachine.TakeSnapshot()
	if err != nil {
		r.logger.Error("failed to take snapshot", "err", err)
		//panic("no error expected, please check the code!!!")
		return nil, err
	}
	snapshot.Data = snapshotData
	// update the snapshot
	r.snapshot = snapshot
	// persist the snapshot
	err = r.persister.SaveSnapshot(snapshot)
	if err != nil {
		r.logger.Error("failed to save snapshot", "err", err)
	}

	// no err expected, please check the code!!!
	entry, err := r.logAtIndex(index)
	if err != nil {
		r.logger.Error("failed to get walogs at index", "err", err)
		//panic("no error expected, please check the code!!!")
		return nil, err
	}

	newLog := make([]*raftpb.LogEntry, 1)
	for _, ent := range r.walogs {
		if ent.Index <= entry.Index {
			continue
		}
		newLog = append(newLog, ent)
	}

	// set the dummy walogs entry
	newLog[0] = &raftpb.LogEntry{
		Index: entry.Index,
		Term:  entry.Term,
	}

	// update the walogs
	r.walogs = newLog
	return snapshot, nil
}

// truncate walogs to the given index, include the given index
func (r *raftNode) truncateLog(index uint64) error {
	r.logger.Debug("truncateLog entry", "index", index)
	defer r.logger.Debug("truncateLog exit", "index", index)
	entry, err := r.logAtIndex(index)
	if err != nil {
		return err
	}

	newLog := make([]*raftpb.LogEntry, 0)

	for _, ent := range r.walogs {
		if ent.Index > entry.Index {
			break
		}
		newLog = append(newLog, ent)
	}

	newLog[0] = r.walogs[0]
	r.walogs = newLog
	return nil
}

// check if the walogs at the given index is match
func (r *raftNode) checkLogMatch(prevLogIndex, prevLogTerm uint64) (bool, *raftpb.LogEntry, error) {
	r.logger.Debug("checkLogMatch entry", "prevLogIndex", prevLogIndex, "prevLogTerm", prevLogTerm)
	defer r.logger.Debug("checkLogMatch exit", "prevLogIndex", prevLogIndex, "prevLogTerm", prevLogTerm)
	term, err := r.getLogTerm(prevLogIndex)
	if err != nil {
		// ErrInvalidIndex return true
		if errors.Is(err, ErrInvalidIndex) {
			if len(r.walogs) == 1 {
				// when walogs is empty, return true
				return true, nil, nil
			}
			return false, nil, err
		} else {
			// use snapshot to recover
			return false, nil, err
		}

	}

	if term != prevLogTerm {
		// walogs mismatch, rematch by leader next time
		return false, &raftpb.LogEntry{
			Index: prevLogIndex,
			Term:  term,
		}, ErrLogConflict
	}

	if r.lastLogIndex() > prevLogIndex {
		// walogs out of range, need to resend
		return true, &raftpb.LogEntry{
			Index: prevLogIndex,
			Term:  term,
		}, ErrNeedTruncate
	}
	return true, nil, nil
}

func (r *raftNode) firstDiffTermEntry(index, term uint64) (*raftpb.LogEntry, error) {
	r.logger.Debug("firstDiffTermEntry entry", "index", index, "term", term)
	defer r.logger.Debug("firstDiffTermEntry exit", "index", index, "term", term)

	entry, err := r.logAtIndex(index)
	if err != nil {
		// no err expected
		return nil, err
	}

	if entry.Term != term {
		return nil, ErrInvalidIndex
	}

	var idx uint64 = 0
	for i := len(r.walogs) - 1; i > 0; i-- {
		if r.walogs[i].Term < term {
			idx = r.walogs[i].Index
			break
		}
	}

	if idx == 0 {
		// all walogs are the same term
		return nil, ErrLogNotFound
	}

	return &raftpb.LogEntry{
		Index: idx,
		Term:  term - 1,
	}, nil
}

func (r *raftNode) cutoffLogsByIndex(index uint64) {
	r.logger.Debug("cutoffLogByIndex entry", "index", index)
	defer r.logger.Debug("cutoffLogByIndex exit", "index", index)
	if index == 0 {
		return
	}

	dummyIndex := r.walogs[0].Index
	if index <= dummyIndex {
		r.logger.Debug("cutoffLogByIndex index is less than dummyIndex", "index", index, "dummyIndex", dummyIndex)
		return
	}
	n := len(r.walogs)
	if dummyIndex+uint64(n)-1 <= index {
		r.walogs = []*raftpb.LogEntry{
			r.walogs[n-1],
		}
		return
	}

	lastIndex := r.lastLogIndex()
	between := lastIndex - index
	cutIndex := n - 1 - int(between)
	r.walogs = append(make([]*raftpb.LogEntry, 0, between+1), r.walogs[cutIndex:]...)
}
