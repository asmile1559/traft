package traft

import (
	"context"
	"errors"
	"fmt"
	raftpb "github.com/asmile1559/traft/internal/apis/raft"
)

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
	if req.Term > r.currentTerm || r.role != Follower {
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

// a call chain
// appendEntries -- not ok --> appendEntries -- not ok --> appendEntries ...
// |                                |
// +----- ok ---> exit              +----- ok ---> exit
func (r *raftNode) appendEntries(peer string, client raftpb.TRaftServiceClient) error {
	prevLogIndex := r.nextIndex[peer] - 1
	prevLogTerm, _ := r.getLogTerm(prevLogIndex)
	logOffset, _ := r.logOffset(prevLogIndex)
	req := &raftpb.AppendEntriesReq{
		Term:         r.currentTerm,
		LeaderId:     r.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      r.log[logOffset+1:],
		LeaderCommit: r.commitIndex,
	}

	resp, err := client.AppendEntries(context.Background(), req)
	if err != nil {
		if errors.Is(err, ErrLogAlreadySnapshot) {
			// snapshot is needed, update nextIndex
			r.nextIndex[peer] = r.snapshot.LastIncludedIndex + 1
			isResp, err := client.InstallSnapshot(context.Background(), &raftpb.InstallSnapshotReq{
				Term:     r.currentTerm,
				LeaderId: r.id,
				Snapshot: r.snapshot,
			})
			if err != nil {
				return err
			}
			fmt.Println(isResp)
		}
		return err
	}
	if !resp.Success {
		go func() {
			_ = r.appendEntries(peer, client)
		}()
	}
	return nil
}
