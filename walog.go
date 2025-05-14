package traft

import (
	"fmt"

	raftpb "github.com/asmile1559/traft/internal/apis/raft"
)

//TODO: dummy entry error! need to be fixed

// In this file, all functions designed lock-free because they are called by RPC Handler.
// RPC Handler is locked by raftNode.lock.

// r.walogs is writing ahead walogs. The position "0" is invalid.
// r.walogs[0] = *raftpb.LogEntry{Index: lastIncludeIndex, Term: lastIncludeTerm}

// walog@v1: use a slice to store walogs, the first element is a dummy walogs entry.
// TODO: walog@v2: use a cyclic queue to store walogs, auto compact when the walogs is full.

var (
	WALogEnd uint64 = 0xFFFFFFFFFFFFFFFF
)

func (r *raftNode) lastLogIndex() uint64 {
	n := len(r.walogs)
	return r.walogs[n-1].Index
}

func (r *raftNode) lastLogTerm() uint64 {
	n := len(r.walogs)
	return r.walogs[n-1].Term
}

// get the walogs at the given index
// two errors may be returned:
//  1. ErrLogOutOfRange: the given index is out of range
//  2. ErrLogEntryCompacted: the given index is less than the dummy index
func (r *raftNode) logAtIndex(index uint64) (*raftpb.LogEntry, error) {
	dummy := r.walogs[0]
	n := len(r.walogs)
	if dummy.Term == 0 {
		// not compacted yet
		if index >= uint64(n) {
			err := fmt.Errorf("%w: last log index is %d, but required index is %d", ErrLogOutOfRange, n-1, index)
			r.logger.Error(err.Error())
			return nil, err
		}
		return r.walogs[index], nil
	}

	// compacted
	if index < dummy.Index {
		err := fmt.Errorf("%w: last compacted index is %d, but required index is %d", ErrLogEntryCompacted, dummy.Index, index)
		r.logger.Error(err.Error())
		return nil, err
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
		err := fmt.Errorf("%w: last log index is %d, but required index is %d", ErrLogOutOfRange, uint64(n)-1+dummy.Index, index+dummy.Index)
		r.logger.Error(err.Error())
		return nil, err
	}
	return r.walogs[index], nil
}

// get the term of the walogs at the given index, an interface of logAtIndex
func (r *raftNode) getLogTerm(index uint64) (uint64, error) {
	entry, err := r.logAtIndex(index)
	if err != nil {
		return 0, err
	}
	return entry.Term, nil
}

func (r *raftNode) lastIndexOf(term uint64) (uint64, error) {
	if term == 0 {
		return 0, nil
	}

	n := len(r.walogs)

	if r.walogs[n-1].Term < term {
		// given term = 6
		// index:  5, 6, 7, 8, 9, 10, 11, 12, 13, 14
		// term:   1, 1, 2, 2, 2, 3,  3,  3,  4,  4
		//                                        Δ
		//                                      target
		r.logger.Warn(
			fmt.Sprintf("Warning! The given term is out of range, last term is %d, but required term is %d",
				r.walogs[n-1].Term, term,
			),
		)
		return r.walogs[n-1].Index, nil
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
	if r.walogs[0].Term == term {
		// dummy term is equal to the given term
		return r.walogs[0].Index, nil
	}
	err := fmt.Errorf("%w: the given term %d is not found", ErrLogEntryNotFound, term)
	r.logger.Debug(err.Error())
	return 0, err
}

// cutoff the prefix of walogs by the given index, exclude the given index
// [0, 1, 2, 3, 4, ...].cutoffLogsByIndex(3) -> [3, 4, ...]
func (r *raftNode) cutoffLogsByIndex(index uint64) {
	if index == 0 {
		return
	}

	dummyIndex := r.walogs[0].Index
	if index <= dummyIndex {
		r.logger.Warn("cutoffLogByIndex index is less than dummyIndex", "index", index, "dummyIndex", dummyIndex)
		return
	}
	n := len(r.walogs)
	lastIndex := r.lastLogIndex()
	if lastIndex <= index {
		r.walogs = []*raftpb.LogEntry{
			r.walogs[n-1],
		}
		return
	}

	between := lastIndex - index
	cutIndex := n - 1 - int(between)
	r.walogs = append(make([]*raftpb.LogEntry, 0, between+1), r.walogs[cutIndex:]...)
}

// extract walogs entries between the given start and end index, include the start and end index
// [0, 1, 2, 3, 4, ...].extractLogEntries(2, 4) -> [2, 3, 4]
func (r *raftNode) extractLogEntries(start, end uint64) ([]*raftpb.LogEntry, error) {
	if start > end {
		err := fmt.Errorf(
			"%w: start index must less than end index, but got <start %d, end %d>",
			ErrLogInvalidIndex, start, end,
		)
		return nil, err
	}

	if start > r.lastLogIndex() {
		err := fmt.Errorf(
			"%w: start index must less than last log index, but got <start: %d> while <last index: %d>",
			ErrLogOutOfRange, start, r.lastLogIndex(),
		)
		return nil, err
	}

	if end < r.walogs[0].Index {
		err := fmt.Errorf(
			"%w: end index must greater than the dummy index, but got <end: %d> while <dummy index: %d>",
			ErrLogEntryCompacted, end, r.walogs[0].Index,
		)
		return nil, err
	}

	if start < r.walogs[0].Index {
		start = r.walogs[0].Index
	}
	if end > r.lastLogIndex() {
		end = r.lastLogIndex()
	}

	logs := make([]*raftpb.LogEntry, 0, end-start+1)
	for _, ent := range r.walogs {
		if ent.Index >= start && ent.Index <= end {
			logs = append(logs, ent)
		}
	}
	return logs, nil
}

// compact walogs and generate snapshot
func (r *raftNode) compactLog() error {
	lastApplied := r.lastApplied
	if lastApplied == 0 {
		// no snapshot
		return nil
	}

	if lastApplied == r.walogs[0].Index {
		return nil
	}

	// get the term of the walogs at the given lastApplied
	term, err := r.getLogTerm(lastApplied)
	if err != nil {
		// no err expected
		return err
	}

	snapshot := &raftpb.Snapshot{
		LastIncludedIndex: lastApplied,
		LastIncludedTerm:  term,
	}

	// take a snapshot of the state machine
	snapshotData, err := r.stateMachine.TakeSnapshot()
	if err != nil {
		// no error expected
		return err
	}
	snapshot.Data = snapshotData
	// update the snapshot
	r.snapshot = snapshot
	// persist the snapshot
	err = r.persister.SaveSnapshot(snapshot)
	if err != nil {
		return err
	}

	r.cutoffLogsByIndex(lastApplied)
	return nil
}

// truncate walogs to the given index, include the given index
func (r *raftNode) truncateLog(index uint64) error {
	if index < r.walogs[0].Index {
		err := fmt.Errorf(
			"%w: index must greater than or equal the dummy index, but got <index: %d> while <dummy index: %d>",
			ErrLogEntryCompacted, index, r.walogs[0].Index,
		)
		return err
	}

	newLog := make([]*raftpb.LogEntry, 0)

	for _, ent := range r.walogs {
		if ent.Index > index {
			break
		}
		newLog = append(newLog, ent)
	}

	//newLog[0] = r.walogs[0]
	r.walogs = newLog
	return nil
}

// check if the walogs at the given index is matched
// four errors may be returned:
//  1. ErrLogEntryCompacted: the given index is less than the dummy index(failed)
//  2. ErrLogOutOfRange: the given index is out of range(failed)
//  3. ErrLogEntryConflict: the given <term, index> pair does not match the existing log entry(failed)
//  4. ErrLogNeedTruncate: the walogs at the given index is not the last log entry(success)
func (r *raftNode) checkLogMatch(prevLogIndex, prevLogTerm uint64) (bool, *raftpb.LogEntry, error) {
	term, err := r.getLogTerm(prevLogIndex)
	if err != nil {
		// use snapshot to recover
		return false, nil, err
	}

	if term != prevLogTerm {
		// walogs mismatch, rematch by leader next time
		return false, &raftpb.LogEntry{
			Index: prevLogIndex,
			Term:  term,
		}, ErrLogEntryConflict
	}

	if r.lastLogIndex() > prevLogIndex {
		// walogs out of range, need to resend
		return true, &raftpb.LogEntry{
			Index: prevLogIndex,
			Term:  term,
		}, ErrLogNeedTruncate
	}
	return true, nil, nil
}

func (r *raftNode) firstDiffTermEntry(term uint64) (*raftpb.LogEntry, error) {
	if term == 0 {
		err := fmt.Errorf("%w: term is 0", ErrLogEntryNotFound)
		return nil, err
	}
	idx, err := r.lastIndexOf(term - 1)
	if err != nil {
		return nil, err
	}
	return r.logAtIndex(idx)
}
