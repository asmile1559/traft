package traft

import (
	"context"
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
		return resp, nil
	}

	// transition to follower when a new term request received
	if req.Term > r.currentTerm || r.role != Follower {
		r.transitionToFollower(req.Term, req.LeaderId)
	}

	// reset electionTime to prevent election
	r.electionTimer.Reset(RandomElectionTimeout())

	// check if the snapshot is valid
	if r.snapshot != nil && req.Snapshot.LastIncludedIndex <= r.snapshot.LastIncludedIndex {
		return resp, nil
	}

	err := r.stateMachine.ApplySnapshot(req.Snapshot.Data)
	if err != nil {
		return resp, err
	}

	r.snapshot = req.Snapshot

	if req.Snapshot.LastIncludedIndex < r.lastLogIndex() {
		// truncate the log
		_ = r.truncateLog(req.Snapshot.LastIncludedIndex)
	} else {
		r.log = make([]*raftpb.LogEntry, 0)
	}

	r.commitIndex = req.Snapshot.LastIncludedIndex
	r.lastApplied = req.Snapshot.LastIncludedIndex

	_ = r.persister.SaveSnapshot(r.snapshot)
	_ = r.persister.SaveLogEntries(r.log)
	resp.Success = true
	return resp, nil
}

func (r *raftNode) installSnapshot(ctx context.Context) {
	select {
	case <-ctx.Done():
		return
	case peerId := <-r.installSnapshotC:
		peer := r.peers[peerId]
		if peer == nil {
			// TODO: handle error
			panic(ErrPeerIsNil)
		}
		r.installSnapshotPeer(ctx, peer)
	}
}

func (r *raftNode) installSnapshotPeer(ctx context.Context, peer *Peer) {
	r.mu.RLock()
	if r.snapshot == nil {
		_, _ = r.compactLog()
	}
	req := &raftpb.InstallSnapshotReq{
		Term:     r.currentTerm,
		LeaderId: r.id,
		Snapshot: r.snapshot,
	}
	r.mu.RUnlock()
	resp, err := peer.SendInstallSnapshotRequest(ctx, req)
	if err != nil {
		return
	}
	if resp.Term > r.currentTerm {
		r.transitionToFollower(resp.Term, VotedForNone)
		return
	}
	if resp.Success {
		r.appendEntriesC <- peer.Id()
	}
}
