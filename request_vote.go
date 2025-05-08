package traft

import (
	"context"
	raftpb "github.com/asmile1559/traft/internal/apis/raft"
)

func (r *raftNode) RequestVote(ctx context.Context, req *raftpb.RequestVoteReq) (*raftpb.RequestVoteResp, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	resp := &raftpb.RequestVoteResp{
		Term:        r.currentTerm,
		VoteGranted: false,
	}

	// reject old term vote request
	if req.Term < r.currentTerm {
		return resp, nil
	}

	if req.Term > r.currentTerm {
		// if a newer term request, reset the status of raft node and transition to follower
		// no matter what's the role before
		r.transitionToFollower(req.Term, VotedForNone)
	}

	if r.role != Follower {
		// only followers could vote, reject if r is not a follower
		return resp, nil
	}

	// r has not voted yet, or r voted for the candidate
	if r.votedFor == VotedForNone || r.votedFor == req.CandidateId {
		lastLogIndex := r.lastLogIndex()
		lastLogTerm := r.lastLogTerm()

		// only a newer candidate could earn the vote
		upToDate := req.LastLogTerm > lastLogTerm ||
			(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)

		if upToDate {
			r.votedFor = req.CandidateId
			resp.VoteGranted = true
		}
	}

	resp.Term = r.currentTerm
	return resp, nil
}
