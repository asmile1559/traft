package traft

import (
	"context"
	"errors"

	raftpb "github.com/asmile1559/traft/internal/apis/raft"
)

// AppendEntries is the raft waitHeartbeat and walogs replication RPC.
func (r *raftNode) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesReq) (*raftpb.AppendEntriesResp, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.logger.Debug("received AppendEntries request", "term", req.Term, "leaderId", req.LeaderId)
	defer r.logger.Debug("exit AppendEntries request", "term", req.Term, "leaderId", req.LeaderId)

	resp := &raftpb.AppendEntriesResp{
		Term:          r.currentTerm,
		Success:       false,
		MatchIndex:    0,
		ConflictTerm:  0,
		ConflictIndex: 0,
	}

	// reject when an old term request received
	if req.Term < r.currentTerm {
		r.logger.Debug("reject AppendEntries request cause old term", "term", req.Term, "currentTerm", r.currentTerm)
		return resp, nil
	}

	// transition to follower when a new term request received
	if req.Term > r.currentTerm || r.role != Follower {
		r.transitionToFollower(req.Term, req.LeaderId)
	}

	// reset electionTime to prevent waitElection
	r.electionTimer.Reset(RandomElectionTimeout())

	resp.Term = r.currentTerm
	ok, entry, err := r.checkLogMatch(req.PrevLogIndex, req.PrevLogTerm)

	if !ok {
		if errors.Is(err, ErrLogEntryCompacted) {
			r.logger.Debug("reject AppendEntries request cause walogs already snapshot")
		} else if errors.Is(err, ErrLogIndexOutOfRange) {
			r.logger.Debug("reject AppendEntries request cause walogs out of range")
			resp.ConflictIndex = r.lastLogIndex() + 1
		} else if errors.Is(err, ErrLogInvalidIndex) {
			r.logger.Debug("reject AppendEntries request cause walogs invalid index")
		} else {
			r.logger.Debug("reject AppendEntries request cause walogs not match")
			ct := entry.Term
			ci := entry.Index
			// find the first walogs entry whose term is not conflictTerm
			ent, err := r.firstDiffTermEntry(ci, ct)
			if err != nil {
				r.logger.Debug("reject AppendEntries request cause walogs not match", "err", err)
			} else {
				r.logger.Debug("reject AppendEntries request cause walogs not match", "conflictTerm", ct, "conflictIndex", entry.Index+1)
				resp.ConflictIndex = ent.Index + 1
				resp.ConflictTerm = ct
			}
		}
		return resp, nil
	}

	r.logger.Debug("success AppendEntries request", "term", req.Term, "leaderId", req.LeaderId)
	if errors.Is(err, ErrLogNeedTruncate) {
		// walogs match, but need to truncate
		_ = r.truncateLog(req.PrevLogIndex)
	}

	if len(req.Entries) > 0 {
		r.walogs = append(r.walogs, req.Entries...)
	}

	resp.Success = true
	resp.MatchIndex = req.PrevLogIndex + uint64(len(req.Entries))
	if resp.MatchIndex > r.lastLogIndex() {
		// should not happen
		r.logger.Error("the match index is greater than the last log index", "matchIndex", resp.MatchIndex, "lastLogIndex", r.lastLogIndex())
	}

	// update commit index
	if req.LeaderCommit > r.commitIndex {
		last := r.lastLogIndex()
		r.commitIndex = min(req.LeaderCommit, last)
		r.applyC <- struct{}{}
	}

	if len(req.Entries) > 0 {
		_ = r.persister.SaveLogEntries(r.walogs)
	}
	return resp, nil
}

// When a follower transitions to a leader, it resets the nextIndex[] and matchIndex[]. The nextIndex[] is set to the
// `lastLogIndex + 1`, and the matchIndex[] is set to 0. Then, the leader sends waitHeartbeat to all peers. If the peer who
// receives the waitHeartbeat, it will check the walogs, and then feed back the `AppendEntriesResp` to the leader.

// when a follower transitions to a leader, it calls this function
func (r *raftNode) listenAppendEntriesRequest(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case peerId := <-r.appendEntriesC:
			r.logger.Debug("send append entries to peer", "peer id", peerId)
			peer, ok := r.peers[peerId]
			if !ok {
				r.logger.Error("no such peer in the cluster", "peer id", peerId)
				continue
			}
			go r.sendAppendEntries(ctx, peer)
		}
	}
}

func (r *raftNode) sendAppendEntries(ctx context.Context, peer *Peer) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	select {
	case <-ctx.Done():
		return
	default:
	}
	r.logger.Debug("enter append entries to peer", "peerId", peer.Id())
	defer r.logger.Debug("exit append entries to peer", "peerId", peer.Id())
	prevLogIndex := peer.NextIndex() - 1
	prevLogTerm, err := r.getLogTerm(prevLogIndex)
	if err != nil {
		r.installSnapshotC <- peer.Id()
		return
	}
	req := &raftpb.AppendEntriesReq{
		Term:         r.currentTerm,
		LeaderId:     r.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: r.commitIndex,
		Entries:      r.walogs[prevLogIndex+1:],
	}
	resp, err := peer.SendAppendEntriesRequest(ctx, req)
	r.handleResultC <- &Result{
		PeerID: peer.Id(),
		Resp:   resp,
	}
}
