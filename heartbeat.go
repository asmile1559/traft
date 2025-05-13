package traft

import (
	"context"
	"sync"

	raftpb "github.com/asmile1559/traft/internal/apis/raft"
)

func (r *raftNode) heartbeat(ctx context.Context) {
	// 发送心跳
	r.logger.Debug("init heartbeat")
	defer r.logger.Debug("exit heartbeat")
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.heartbeatTicker.C:
			// if the ticker stops, it will not send heartbeats
			if r.role != Leader {
				continue
			}
			r.logger.Debug("send heartbeat")
			r.startHeartbeat(ctx)
		}
	}
}

func (r *raftNode) startHeartbeat(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	select {
	case <-ctx.Done():
		return
	default:
	}
	wg := sync.WaitGroup{}
	for _, peer := range r.peers {
		wg.Add(1)
		go func(peer *Peer) {
			defer wg.Done()
			r.sendHeartbeat(ctx, peer)
		}(peer)
	}
	wg.Wait()
}

func (r *raftNode) sendHeartbeat(ctx context.Context, peer *Peer) {
	r.logger.Debug("send heartbeat")
	prevLogIndex := peer.NextIndex() - 1
	prevLogTerm, err := r.getLogTerm(prevLogIndex)
	if err != nil {
		if len(r.walogs) > 1 {
			r.installSnapshotC <- peer.Id()
			return

		}
	}
	req := &raftpb.AppendEntriesReq{
		Term:         r.currentTerm,
		LeaderId:     r.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: r.commitIndex,
		Entries:      nil,
	}

	client := raftpb.NewTRaftServiceClient(peer.cc)
	resp, err := client.AppendEntries(ctx, req)
	r.handleResultC <- &Result{
		PeerID: peer.Id(),
		Resp:   resp,
	}
}
