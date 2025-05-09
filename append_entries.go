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
			resp.ConflictIndex = r.lastLogIndex()
		} else if errors.Is(err, ErrLogConflict) {
			resp.ConflictTerm = entry.Term
			resp.ConflictIndex = entry.Index
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
	resp.MatchIndex = r.lastLogIndex()
	// update commit index
	if req.LeaderCommit > r.commitIndex {
		last := r.lastLogIndex()
		r.commitIndex = min(req.LeaderCommit, last)
		//r.applyLogToStateMachine()
	}
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
	prevLogIndex := peer.NextIndex() - 1
	prevLogTerm, err := r.getLogTerm(prevLogIndex)
	if err != nil {
		// TODO: use snapshot to recover
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
			id := rPack.PeerID
			err := rPack.Err
			resp := rPack.Resp
			peer := r.peers[id]
			if err != nil {
				if errors.Is(err, ErrLogAlreadySnapshot) {
					// call InstallSnapshot
				} else if errors.Is(err, ErrLogOutOfRange) {
					conflictIndex := resp.ConflictIndex
					conflictTerm := resp.ConflictTerm
					term, _ := r.getLogTerm(conflictIndex)
					if term != conflictTerm {
						// call InstallSnapshot

						break
					}
					peer.Update(conflictIndex+1, conflictIndex)
				} else if errors.Is(err, ErrLogConflict) {
					conflictIndex := resp.ConflictIndex
					conflictTerm := resp.ConflictTerm
					ci := conflictIndex
					// find the first log entry whose term is not conflictTerm
					if r.snapshot != nil {
						for ; ci > r.snapshot.LastIncludedIndex; ci-- {
							idx, _ := r.logOffset(ci)
							if r.log[idx].Term != conflictTerm {
								peer.UpdateNextIndex(ci + 1)
								break
							}
						}
						// if not found, call InstallSnapshot
						if ci == r.snapshot.LastIncludedIndex {
							peer.UpdateNextIndex(r.snapshot.LastIncludedIndex + 1)
						}
					} else {
						for ; ci < conflictIndex; ci-- {
							if r.log[ci].Term != conflictTerm {
								peer.UpdateNextIndex(ci + 1)
								break
							}
						}
						// if not found, call InstallSnapshot
						if ci > conflictIndex {
							// call InstallSnapshot
							peer.UpdateNextIndex(r.lastLogIndex() + 1)
						}
					}
				}
				r.appendEntriesC <- rPack.PeerID
				return
			}

			if resp.Success {
				// update nextIndex and matchIndex
				peer.UpdateNextIndex(resp.MatchIndex + 1)
				peer.UpdateMatchIndex(resp.MatchIndex)
			}
		}
	}
}
