package traft

import (
	"context"
	"errors"
	raftpb "github.com/asmile1559/traft/internal/apis/raft"
)

type Response struct {
	PeerID string
	Resp   *raftpb.AppendEntriesResp
	Err    error
}

// AppendEntries is the raft heartbeat and log replication RPC.
// TODO: check it is thread safe!
func (r *raftNode) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesReq) (*raftpb.AppendEntriesResp, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.logger.Debug("received AppendEntries request", "term", req.Term, "leaderId", req.LeaderId)
	resp := &raftpb.AppendEntriesResp{
		Term:    r.currentTerm,
		Success: false,
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

	// reset electionTime to prevent election
	r.electionTimer.Reset(RandomElectionTimeout())

	resp.Term = r.currentTerm
	ok, entry, err := r.checkLogMatch(req.PrevLogTerm, req.PrevLogIndex)

	if !ok {
		if errors.Is(err, ErrLogAlreadySnapshot) {
			r.logger.Debug("reject AppendEntries request cause log already snapshot")
			resp.ConflictTerm = 0
			resp.ConflictIndex = 0
		} else if errors.Is(err, ErrLogOutOfRange) {
			r.logger.Debug("reject AppendEntries request cause log out of range")
			resp.ConflictTerm = 0
			resp.ConflictIndex = r.lastLogIndex() + 1
		} else {
			r.logger.Debug("reject AppendEntries request cause log not match")
			ct := entry.Term
			ci := entry.Index
			// find the first log entry whose term is not conflictTerm
			if r.snapshot != nil {
				for ; ci > r.snapshot.LastIncludedIndex; ci-- {
					idx, _ := r.logOffset(ci)
					if r.log[idx].Term != ct {
						break
					}
				}
				resp.ConflictTerm = ct
				resp.ConflictIndex = ci + 1
			} else {
				for ; ci < entry.Index; ci-- {
					if r.log[ci].Term != ct {
						break
					}
				}
				resp.ConflictTerm = ct
				resp.ConflictIndex = ci + 1
			}
		}
		return resp, nil
	}

	r.logger.Debug("success AppendEntries request", "term", req.Term, "leaderId", req.LeaderId)
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
		r.applyC <- struct{}{}
	}
	r.persister.SaveLogEntries(r.log)
	return resp, nil
}

// When a follower transitions to a leader, it resets the nextIndex[] and matchIndex[]. The nextIndex[] is set to the
// `lastLogIndex + 1`, and the matchIndex[] is set to 0. Then, the leader sends heartbeat to all peers. If the peer who
// receives the heartbeat, it will check the log, and then feed back the `AppendEntriesResp` to the leader.

func (r *raftNode) appendEntries(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			// TODO: clean resources
			return
		case p := <-r.appendEntriesC:
			// send append entries to peer
			r.logger.Debug("send append entries to peer", "peerId", p)
			peer := r.peers[p]
			if peer == nil {
				// TODO: use other method to handle this error
				panic(ErrPeerIsNil.Error())
			}
			// send append entries to peer
			r.appendEntriesPeer(ctx, peer)
		}
	}
}

func (r *raftNode) appendEntriesPeer(ctx context.Context, peer *Peer) {
	r.logger.Debug("enter append entries to peer", "peerId", peer.Id())
	defer r.logger.Debug("exit append entries to peer", "peerId", peer.Id())
	prevLogIndex := peer.NextIndex() - 1
	r.mu.RLock()
	prevLogTerm, err := r.getLogTerm(prevLogIndex)
	if err != nil {
		r.installSnapshotC <- peer.Id()
		r.mu.RUnlock()
		return
	}
	req := &raftpb.AppendEntriesReq{
		Term:         r.currentTerm,
		LeaderId:     r.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: r.commitIndex,
		Entries:      r.log[prevLogIndex+1:],
	}
	r.mu.RUnlock()
	resp, err := peer.SendAppendEntriesRequest(ctx, req)
	r.appendEntriesRespC <- &Response{
		PeerID: peer.Id(),
		Resp:   resp,
		Err:    err,
	}
}

// no matter heartbeat or appendEntries request, the response will be processed by that function.
// cause there maybe
func (r *raftNode) processResponse(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			// TODO: clean resources
			return
		case rPack := <-r.appendEntriesRespC:
			r.logger.Debug("receive append entries response from peer", "peerId", rPack.PeerID)
			id := rPack.PeerID
			err := rPack.Err
			resp := rPack.Resp
			peer := r.peers[id]
			if resp.Term > r.currentTerm {
				// update current term and transition to follower
				r.transitionToFollower(resp.Term, VotedForNone)
				return
			}

			if resp.Success {
				r.logger.Debug("success append entries response from peer", "peerId", id)
				// update nextIndex and matchIndex
				peer.UpdateNextIndex(resp.MatchIndex + 1)
				peer.UpdateMatchIndex(resp.MatchIndex)
				return
			}

			if resp.ConflictTerm == 0 && resp.ConflictIndex == 0 {
				r.logger.Debug("reject, use install snapshot", "peerId", id)
				// use snapshot to recover
				r.installSnapshotC <- id
				return
			}

			if resp.ConflictTerm == 0 {
				r.logger.Debug("reject, reset nextIndex", "peerId", id)
				peer.UpdateNextIndex(resp.ConflictIndex)
				// call appendEntries again
				r.appendEntriesC <- id
				return
			}

			_, err = r.getLogTerm(resp.ConflictIndex)
			if err != nil {
				peer.UpdateNextIndex(resp.ConflictIndex)
			} else {
				ct := resp.ConflictTerm

				li, err := r.lastIndexOf(ct)
				if err != nil {
					r.installSnapshotC <- peer.Id()
					return
				}
				peer.UpdateNextIndex(li + 1)
			}
			r.appendEntriesC <- id
		}
	}
}
