package traft

import (
	"errors"
	raftpb "github.com/asmile1559/traft/internal/apis/raft"
)

// In this file, all functions designed lock-free because they are called by RPC Handler.
// RPC Handler is locked by raftNode.lock.

// r.log is writing ahead log. The position "0" is invalid.
// r.log[0] = *raftpb.LogEntry{Index: lastIncludeIndex, Term: lastIncludeTerm}

func (r *raftNode) lastLogIndex() uint64 {
	n := len(r.log)
	return r.log[n-1].Index
}

func (r *raftNode) lastLogTerm() uint64 {
	n := len(r.log)
	return r.log[n-1].Term
}

func (r *raftNode) logAtIndex(index uint64) (*raftpb.LogEntry, error) {
	if index == 0 {
		r.logger.Error("logAt index is at a invalid index '0'")
		return nil, ErrInvalidIndex
	}

	dummy := r.log[0]
	n := len(r.log)
	if dummy.Term == 0 {
		// not compacted yet
		if index >= uint64(n) {
			r.logger.Error("logAt index is out of range", "index", index, "len", n)
			return nil, ErrLogOutOfRange
		}
		return r.log[index], nil
	}

	// compacted
	if index <= dummy.Index {
		r.logger.Error("logAt index is snapshot", "index", index, "lastIncludedIndex", dummy.Index)
		return nil, ErrLogAlreadySnapshot
	}
	// 5 -> dummy.Index
	// 10 -> index
	//      snapshot     <|> r.log
	// [0, 1, 2, 3, 4, 5] | [6, 7, 8, 9, 10, 11, 12, ...]
	//                [0,    1, 2, 3, 4, 5,  6,  7, ...]
	//                 A
	//         LastIncludedIndex
	// index = index - dummy.Index

	index = index - dummy.Index
	if index >= uint64(n) {
		r.logger.Error("logAt index is out of range", "index", index, "len", n)
		return nil, ErrLogOutOfRange
	}
	return r.log[index], nil
}

// get the term of the log at the given index
func (r *raftNode) getLogTerm(index uint64) (uint64, error) {
	entry, err := r.logAtIndex(index)
	if err != nil {
		return 0, err
	}
	return entry.Term, nil
}

func (r *raftNode) lastIndexOf(term uint64) (uint64, error) {
	if term == 0 {
		r.logger.Error("lastIndexOf term is at a invalid term '0'")
		return 0, ErrInvalidTerm
	}

	n := len(r.log)

	if r.log[n-1].Term < term {
		// given term = 6
		// index:  5, 6, 7, 8, 9, 10, 11, 12, 13, 14
		// term:   1, 1, 2, 2, 2, 3,  3,  3,  4,  4
		//                                        Δ
		//                                      target
		r.logger.Debug("The last log term is less than the given term", "term", term, "lastLogTerm", r.log[n-1].Term)
		return r.log[n-1].Index, nil
	}

	// given term = 2
	// index:  5, 6, 7, 8, 9, 10, 11, 12, 13, 14
	// term:   1, 1, 2, 2, 2, 3,  3,  3,  4,  4
	//                     Δ          <- - - idx
	//                   target
	for idx := n - 1; idx > 0; idx-- {
		if r.log[idx].Term == term {
			return r.log[idx].Index, nil
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

// compact log and generate snapshot
func (r *raftNode) compactLog() (*raftpb.Snapshot, error) {
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
		r.logger.Error("failed to get log term", "err", err)
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
		r.logger.Error("failed to get log at index", "err", err)
		//panic("no error expected, please check the code!!!")
		return nil, err
	}

	newLog := make([]*raftpb.LogEntry, 1)
	for _, ent := range r.log {
		if ent.Index <= entry.Index {
			continue
		}
		newLog = append(newLog, ent)
	}

	// set the dummy log entry
	newLog[0] = &raftpb.LogEntry{
		Index: entry.Index,
		Term:  entry.Term,
	}

	// update the log
	r.log = newLog
	return snapshot, nil
}

// truncate log to the given index, include the given index
func (r *raftNode) truncateLog(index uint64) error {

	entry, err := r.logAtIndex(index)
	if err != nil {
		return err
	}

	newLog := make([]*raftpb.LogEntry, 1)
	newLog[0] = r.log[0]

	for _, ent := range r.log {
		if ent.Index > entry.Index {
			break
		}
		newLog = append(newLog, ent)
	}

	r.log = newLog
	return nil
}

// check if the log at the given index is match
func (r *raftNode) checkLogMatch(prevLogTerm, prevLogIndex uint64) (bool, *raftpb.LogEntry, error) {
	term, err := r.getLogTerm(prevLogIndex)
	if err != nil {
		// TODO: think about the ErrInvalidIndex? should it return false?
		if !errors.Is(err, ErrInvalidIndex) {
			// use snapshot to recover
			return false, nil, err
		}
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
