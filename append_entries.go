package traft

import (
	"context"
	"errors"
	"fmt"

	raftpb "github.com/asmile1559/traft/internal/apis/raft"
)

// AppendEntries is the raft waitHeartbeat and walogs replication RPC.
func (r *raftNode) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesReq) (*raftpb.AppendEntriesResp, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(req.Entries) == 0 {
		r.logger.Debug(fmt.Sprintf("[Heartbeat], From {%s}", req))
	} else {
		r.logger.Debug(fmt.Sprintf("[AppendEntries], From {%s}", req))
	}

	resp := &raftpb.AppendEntriesResp{
		Term:          r.currentTerm,
		Success:       false,
		MatchIndex:    0,
		ConflictTerm:  0,
		ConflictIndex: 0,
	}

	// reject when an old term request received
	if req.Term < r.currentTerm {
		err := fmt.Errorf(
			"%w: leader term %d, current term %d",
			ErrWithLowPriorityTerm, req.Term, r.currentTerm,
		)
		r.logger.Debug(err.Error())
		return resp, err
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
		if errors.Is(err, ErrLogOutOfRange) {
			resp.ConflictIndex = r.lastLogIndex() + 1
		} else if errors.Is(err, ErrLogEntryConflict) {
			ct := entry.Term
			// find the first walogs entry whose term is not conflictTerm
			ent, Err := r.firstDiffTermEntry(ct)
			if Err == nil {
				resp.ConflictIndex = ent.Index + 1
				resp.ConflictTerm = ct
			}
		}
		err := fmt.Errorf(
			"[Reject] %w: check log match failed, prevLog<index: %d, term: %d>",
			err, req.PrevLogIndex, req.PrevLogTerm,
		)
		r.logger.Debug(err.Error())
		return resp, err
	}

	if errors.Is(err, ErrLogNeedTruncate) {
		_ = r.truncateLog(req.PrevLogIndex)
	}

	if len(req.Entries) > 0 {
		r.walogs = append(r.walogs, req.Entries...)
	}

	resp.Success = true
	resp.MatchIndex = req.PrevLogIndex + uint64(len(req.Entries))
	if resp.MatchIndex > r.lastLogIndex() {
		// should not happen
		err := fmt.Errorf(
			"[SHOULD NOT HAPPEN] %w: matchIndex %d, lastLogIndex %d",
			ErrLogOutOfRange, resp.MatchIndex, r.lastLogIndex(),
		)
		r.logger.Error(err.Error())
		return resp, err
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
			peer, ok := r.peers[peerId]
			if !ok {
				err := fmt.Errorf("%w: %s", ErrPeerIsNotFound, peerId)
				r.logger.Error(err.Error())
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
	prevLogIndex := peer.NextIndex() - 1
	prevLogTerm, err := r.getLogTerm(prevLogIndex)
	if err != nil {
		r.logger.Debug("reach here")
		if r.snapshot != nil && len(r.walogs) > 0 {
			r.installSnapshotC <- peer.Id()
			return
		}
	}
	entries, _ := r.extractLogEntries(prevLogIndex+1, WALogEnd)
	req := &raftpb.AppendEntriesReq{
		Term:         r.currentTerm,
		LeaderId:     r.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: r.commitIndex,
		Entries:      entries,
	}
	r.logger.Debug(
		fmt.Sprintf(
			"[SendAppendEntries] To {%s}, Req: %s",
			peer.id, req,
		),
	)
	resp, err := peer.SendAppendEntriesRequest(ctx, req)
	if resp == nil {
		return
	}
	r.handleResultC <- &Result{
		PeerID: peer.Id(),
		Resp:   resp,
	}
}

func (r *raftNode) AppendLogEntry(entry *raftpb.LogEntry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	lastIndex := r.lastLogIndex()
	if entry.Index != lastIndex+1 {
		err := fmt.Errorf("%w: expect: %d, got: %d", ErrLogWrongIndexEntryToAppend, lastIndex+1, entry.Index)
		r.logger.Error(err.Error())
		return err
	}
	r.walogs = append(r.walogs, entry)
	for peer := range r.peers {
		r.appendEntriesC <- peer
	}
	return nil
}
