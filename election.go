package traft

import (
	"context"
	"sync"

	raftpb "github.com/asmile1559/traft/internal/apis/raft"
)

// 选举函数，用于处理选举逻辑，不加锁，该函数会在选举超时后被调用，不会并发执行
func (r *raftNode) election(ctx context.Context) {
	r.logger.Debug("init election")
	defer r.logger.Debug("exit election")
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.electionTimer.C:
			r.transitionToCandidate()
			r.startElection(ctx)
		}
	}
}

func (r *raftNode) startElection(ctx context.Context) {
	r.logger.Debug("start election")
	// 如果当前角色不是 candidate，直接返回
	if r.role != Candidate {
		r.logger.Warn("election role is not Candidate")
		return
	}

	// 总的投票数
	totalVotes := len(r.peers)
	grantedVotes := 1
	req := &raftpb.RequestVoteReq{
		Term:         r.currentTerm,
		CandidateId:  r.id,
		LastLogIndex: r.lastLogIndex(),
		LastLogTerm:  r.lastLogTerm(),
	}

	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	wg := sync.WaitGroup{}
	respChan := make(chan *raftpb.RequestVoteResp, totalVotes)
	// 发送投票请求给所有节点
	for _, peer := range r.peers {
		wg.Add(1)
		go func(peer *Peer) {
			resp, _ := peer.SendRequestVoteRequest(ctx, req)
			respChan <- resp
		}(peer)
	}

	go func() {
		wg.Wait()
		close(respChan)
	}()

	for resp := range respChan {
		if resp.Term > r.currentTerm {
			// 如果收到的任期比当前任期大，转化为 follower
			r.transitionToFollower(resp.Term, VotedForNone)
			cancel()
			r.logger.Debug("election complete, become follower")
			return
		}
		if resp.VoteGranted {
			grantedVotes++
			if grantedVotes > totalVotes/2 {
				// 获得了大多数的投票，成为领导者
				r.transitionToLeader()
				r.logger.Debug("election complete, become leader")
				cancel()
				return
			}
		}
	}

	// 如果没有获得足够的投票，继续等待选举超时
	if r.role == Candidate {
		r.logger.Debug("election complete, as candidate")
		r.electionTimer.Reset(RandomElectionTimeout())
	}
}
