package traft

import (
	"context"
	"fmt"
	raftpb "github.com/asmile1559/traft/internal/apis/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"sync"
)

func (r *raftNode) heartbeat(ctx context.Context) {
	// 发送心跳
	wg := sync.WaitGroup{}
	for peer := range r.heartbeatC {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			if err := r.heartbeatPeer(ctx, peer); err != nil {
				fmt.Println("heartbeat error:", err)
			}
		}(peer)
	}
	wg.Wait()

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.heartbeatTicker.C:
			// if the ticker stops, it will not send heartbeats
			for _, c := range r.heartbeatC {
				c <- struct{}{}
			}
		}
	}

	// clean resources

}

func (r *raftNode) heartbeatPeer(ctx context.Context, peer string) error {
	cc, err := grpc.NewClient(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer func(conn *grpc.ClientConn) {
		_ = conn.Close()
	}(cc)

	client := raftpb.NewTRaftServiceClient(cc)
	heartbeatC := r.heartbeatC[peer]
	stopFlag := false
	for {
		select {
		case <-ctx.Done():
			stopFlag = true
		case <-heartbeatC:
			r.sendHeartbeat(ctx, client)
		}
		if stopFlag {
			break
		}
	}

	// clean resources

	return nil
}

func (r *raftNode) sendHeartbeat(ctx context.Context, client raftpb.TRaftServiceClient) {
	resp, err := client.AppendEntries(ctx, &raftpb.AppendEntriesReq{
		Term:         r.currentTerm,
		LeaderId:     r.id,
		PrevLogIndex: r.lastLogIndex(),
		PrevLogTerm:  r.lastLogTerm(),
		LeaderCommit: r.commitIndex,
		Entries:      nil,
	})

	fmt.Println(resp, err)
}
