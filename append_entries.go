package traft

import (
	"context"
	"errors"
	raftpb "github.com/asmile1559/traft/internal/apis/raft"
)

// AppendEntries workflow (Follower side):
//
// 1. [Term Check]
//    if req.Term < currentTerm:
//        [Reject] return false
//    else if req.Term > currentTerm:
//        transition to Follower
//
// 2. [Role Check]
//    if role != Follower:
//        transition to Follower
//
// 3. [Reset Election Timer]
//    electionTimer.reset()
//
// 4. [Log Consistency Check]
//    if req.PrevLogIndex < snapshot.LastIncludedIndex:
//        [Reject] => ErrLogAlreadySnapshot (need snapshot)
//
//    if req.PrevLogIndex >= len(log):
//        [Reject] => ErrLogOutOfRange (log too short)
//        => Suggest nextIndex = len(log)
//
//    if log[req.PrevLogIndex].Term != req.PrevLogTerm:
//        [Reject] => ErrLogConflict (term mismatch)
//        => Return conflictTerm & conflictIndex for fast backoff
//
// 5. [Append or Heartbeat Handling]
//    if len(req.Entries) == 0:
//        [Accept] => This is a heartbeat
//        => matchIndex = req.PrevLogIndex
//
//    else:
//        [Accept]
//        => Truncate conflict entries starting at PrevLogIndex + 1
//        => Append new entries
//        => matchIndex = last index of new log
//
// 6. [Update Commit Index]
//    if req.LeaderCommit > commitIndex:
//        commitIndex = min(req.LeaderCommit, lastLogIndex)
//        apply entries up to commitIndex to state machine

// AppendEntries is the raft heartbeat and log replication RPC.
func (r *raftNode) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesReq) (*raftpb.AppendEntriesResp, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	resp := &raftpb.AppendEntriesResp{
		Term:    r.currentTerm,
		Success: false,
	}

	// reject when an old term request received
	if req.Term < r.currentTerm {
		return resp, nil
	}

	// transition to follower when a new term request received
	if req.Term > r.currentTerm {
		r.transitionToFollower(req.Term, req.LeaderId)
	}

	// only follower appends entries
	if r.role != Follower {
		r.transitionToFollower(req.Term, req.LeaderId)
	}

	// reset electionTime to prevent election
	r.electionTimer.Reset(RandomElectionTimeout())

	resp.Term = r.currentTerm
	ok, entry, err := r.checkLogMatch(req.PrevLogTerm, req.PrevLogIndex)
	if !ok {
		// check log match failed, return false
		// case 1: need to recover by snapshot(ErrLogAlreadySnapshot)
		// case 2: resend append entries(ErrLogConflict or ErrLogOutOfRange)
		if errors.Is(err, ErrLogOutOfRange) {
			resp.ConflictTerm = r.lastLogTerm()
			resp.ConflictIndex = r.lastLogIndex() + 1
		} else if errors.Is(err, ErrLogConflict) {
			conflictTerm := entry.Term
			conflictIndex := entry.Index

			resp.ConflictTerm = conflictTerm
			resp.ConflictIndex = conflictIndex
			for i := conflictIndex; i > r.snapshot.LastIncludedIndex; i-- {
				idx, err := r.logOffset(i)
				if err != nil {
					continue
				}
				if r.log[idx].Term != conflictTerm {
					resp.ConflictTerm = conflictTerm
					resp.ConflictIndex = i + 1
					break
				}
			}
		}
		return resp, err
	}

	if errors.Is(err, ErrNeedTruncate) && len(req.Entries) > 0 {
		// log match, but need to truncate
		_ = r.truncateLog(req.PrevLogIndex)
	}

	if len(req.Entries) > 0 {
		// no entries to append, return success
		r.log = append(r.log, req.Entries...)
	}

	resp.Success = true
	resp.MatchIndex = req.PrevLogIndex + uint64(len(req.Entries))
	// update commit index
	if req.LeaderCommit > r.commitIndex {
		last := r.lastLogIndex()
		r.commitIndex = min(req.LeaderCommit, last)
		//r.applyLogToStateMachine()
	}
	return resp, nil
}
