package traft

import (
	"context"
	"fmt"
	raftpb "github.com/asmile1559/traft/internal/apis/raft"
)

func (r *raftNode) InstallSnapshot(ctx context.Context, req *raftpb.InstallSnapshotReq) (*raftpb.InstallSnapshotResp, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	resp := &raftpb.InstallSnapshotResp{
		Term:    r.currentTerm,
		Success: false,
	}

	// reject when an old term request received
	if req.Term < r.currentTerm {
		err := fmt.Errorf(
			"%w: Caller{id: %s, term: %d}, Callee{id %s, term: %d}",
			ErrTermDenied, req.LeaderId, req.Term, r.id, r.currentTerm,
		)
		return resp, err
	}

	// transition to follower when a new term request received
	if req.Term > r.currentTerm || r.role != Follower {
		r.transitionToFollower(req.Term, req.LeaderId)
	}

	// reset electionTime to prevent waitElection
	r.electionTimer.Reset(RandomElectionTimeout())

	// check if the snapshot is valid
	if req.Snapshot != nil && r.snapshot != nil &&
		req.Snapshot.LastIncludedIndex <= r.snapshot.LastIncludedIndex {
		err := fmt.Errorf(
			"%w: Caller{id: %s, lastIncludedIndex: %d}, Callee{id %s, lastIncludedIndex: %d}",
			ErrSnapshotOutOfDate, req.LeaderId, req.Snapshot.LastIncludedIndex, r.id, r.snapshot.LastIncludedIndex,
		)
		return resp, err
	}

	if req.Snapshot == nil && r.snapshot != nil {
		err := fmt.Errorf(
			"%w: Caller{id: %s, lastIncludedIndex is nil}, Callee{id %s, lastIncludedIndex: %d}",
			ErrSnapshotOutOfDate, req.LeaderId, r.id, r.snapshot.LastIncludedIndex,
		)
		return resp, err
	}

	err := r.stateMachine.ApplySnapshot(req.Snapshot.Data)
	if err != nil {
		return resp, err
	}

	r.snapshot = req.Snapshot
	r.walogs = req.Entries
	r.commitIndex = req.Snapshot.LastIncludedIndex
	r.lastApplied = req.Snapshot.LastIncludedIndex

	_ = r.persister.SaveSnapshot(r.snapshot)
	_ = r.persister.SaveLogEntries(r.walogs)
	resp.Success = true
	return resp, nil
}

func (r *raftNode) listenInstallSnapshotRequest(ctx context.Context) {
	select {
	case <-ctx.Done():
		return
	case peerId := <-r.installSnapshotC:
		r.logger.Debug(fmt.Sprintf("receive install snapshot, peer id: %s", peerId))
		peer := r.peers[peerId]
		if peer == nil {
			r.logger.Error(fmt.Sprintf("peer %s not found", peerId))
			return
		}
		r.sendHeartbeat(ctx, peer)
	}
}

func (r *raftNode) sendInstallSnapshot(ctx context.Context, peer *Peer) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	req := &raftpb.InstallSnapshotReq{
		Term:     r.currentTerm,
		LeaderId: r.id,
		Snapshot: r.snapshot,
		Entries:  r.walogs,
	}
	resp, err := peer.SendInstallSnapshotRequest(ctx, req)
	if err != nil {
		r.logger.Error(fmt.Sprintf("send install snapshot to peer %s failed, error: %s", peer.Id(), err.Error()))
		return
	}

	if resp.Success {
		r.logger.Debug(fmt.Sprintf("send install snapshot to peer %s success", peer.Id()))
		return
	}

	r.logger.Debug(fmt.Sprintf("send install snapshot to peer %s failed", peer.Id()))

	if resp.Term > r.currentTerm {
		r.transitionToFollower(resp.Term, VotedForNone)
		return
	}
}
