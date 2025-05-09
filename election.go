package traft

import (
	"context"
	raftpb "github.com/asmile1559/traft/internal/apis/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log/slog"
	"sync"
)

// 选举函数，用于处理选举逻辑，不加锁，该函数会在选举超时后被调用，不会并发执行
func (r *raftNode) election() {
	// 如果当前角色不是 candidate，直接返回
	if r.role != Candidate {
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg := sync.WaitGroup{}
	respChan := make(chan *raftpb.RequestVoteResp, totalVotes)
	// 发送投票请求给所有节点
	for _, peer := range r.peers {
		if peer == r.id {
			// 跳过自己
			continue
		}
		wg.Add(1)
		func(peer string, ctx context.Context, req *raftpb.RequestVoteReq) {
			defer wg.Done()
			// TODO: use connection pool
			cc, err := grpc.NewClient(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				slog.Error("failed to connect to peer", "peer", peer, "error", err)
				return
			}
			defer func(conn *grpc.ClientConn) {
				_ = conn.Close()
			}(cc)

			client := raftpb.NewTRaftServiceClient(cc)
			resp, err := client.RequestVote(context.Background(), req)
			if err != nil {
				slog.Error("failed to vote", "peer", peer, "error", err)
				return
			}
			select {
			case respChan <- resp:
			case <-ctx.Done():
			}
		}(peer, ctx, req)
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
			return
		}
		if resp.VoteGranted {
			grantedVotes++
			if grantedVotes > totalVotes/2 {
				// 获得了大多数的投票，成为领导者
				r.transitionToLeader()
				cancel()
				return
			}
		}
	}

	// 如果没有获得足够的投票，继续等待选举超时
	if r.role == Candidate {
		r.electionTimer.Reset(RandomElectionTimeout())
	}
}
