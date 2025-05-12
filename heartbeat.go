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
	for _, peer := range r.peers {
		go r.heartbeatPeer(ctx, peer)
	}
	for {
		select {
		case <-ctx.Done():
			// TODO: clean resources
			return
		case <-r.heartbeatTicker.C:
			// if the ticker stops, it will not send heartbeats
			if r.role != Leader {
				continue
			}
			r.logger.Debug("send heartbeat")
			r.startHeartbeat()
		}
	}
}

func (r *raftNode) startHeartbeat() {
	wg := sync.WaitGroup{}
	for _, peer := range r.peers {
		wg.Add(1)
		go func(peer *Peer) {
			defer wg.Done()
			peer.NotifyHeartbeat()
		}(peer)
	}
	wg.Wait()
}

func (r *raftNode) heartbeatPeer(ctx context.Context, peer *Peer) {
	select {
	case <-ctx.Done():
		// TODO: clean resources
		return
	case <-peer.RecvNotifyHeartbeatChan():
		r.logger.Debug("received heartbeat")
		prevLogIndex := peer.NextIndex() - 1
		prevLogTerm, err := r.getLogTerm(prevLogIndex)
		if err != nil {
			r.installSnapshotC <- peer.Id()
			return
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
}
