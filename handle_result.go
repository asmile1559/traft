package traft

import (
	"context"
	"fmt"
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
	r.logger.Debug(
		fmt.Sprintf("Receive AppendEntriesResp from peerId: %s, Resp:%v",
			result.PeerID, result.Resp,
		),
	)
	id := result.PeerID
	resp := result.Resp
	peer := r.peers[id]
	if resp.Term > r.currentTerm {
		// update current term and transition to follower
		r.transitionToFollower(resp.Term, VotedForNone)
		return
	}

	if resp.Success {
		// update nextIndex and matchIndex
		peer.UpdateNextIndex(resp.MatchIndex + 1)
		peer.UpdateMatchIndex(resp.MatchIndex)
		for {
			cnt := 0
			for _, peer := range r.peers {
				if peer.MatchIndex() > r.commitIndex {
					cnt++
				}
			}
			if 2*cnt < len(r.peers)+1 {
				break
			}
			r.commitIndex++
		}
		r.applyC <- struct{}{}
		return
	}

	if resp.ConflictTerm == 0 && resp.ConflictIndex == 0 {
		// ErrLogInvalidIndex or ErrLogEntryCompacted
		r.installSnapshotC <- id
		return
	}

	// ErrLogOutOfRange in follower
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
