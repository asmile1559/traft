package traft

import (
	"context"
	raftpb "github.com/asmile1559/traft/internal/apis/raft"
)

type Result struct {
	PeerID string
	Resp   *raftpb.AppendEntriesResp
}

func (r *raftNode) listenHandleResultRequest(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case result := <-r.handleResultC:
			go r.handleResult(ctx, result)
		}
	}
}

func (r *raftNode) handleResult(ctx context.Context, result *Result) {
	r.mu.Lock()
	defer r.mu.Unlock()

	select {
	case <-ctx.Done():
		return
	default:
	}
	r.logger.Debug("start handle result", "peerId", result.PeerID)
	id := result.PeerID
	resp := result.Resp
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
		// ErrLogInvalidIndex or ErrLogEntryCompacted
		r.installSnapshotC <- id
		return
	}

	// ErrLogIndexOutOfRange in follower
	if resp.ConflictTerm == 0 {
		r.logger.Debug("reject, reset nextIndex", "peerId", id)
		peer.UpdateNextIndex(resp.ConflictIndex)
		// call appendEntries again
		r.appendEntriesC <- id
		return
	}

	// find the last log index of the conflict term - 1 in leader walogs
	idx, err := r.lastIndexOf(resp.ConflictTerm - 1)
	if err != nil {
		r.installSnapshotC <- id
		return
	} else {
		peer.UpdateNextIndex(idx + 1)
	}
	r.appendEntriesC <- id
}
