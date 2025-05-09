package traft

import (
	"context"
	raftpb "github.com/asmile1559/traft/internal/apis/raft"
	"sync"
)

func (r *raftNode) heartbeat(ctx context.Context) {
	// 发送心跳
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
			wg := sync.WaitGroup{}
			for _, peer := range r.peers {
				wg.Add(1)
				go func(peer *Peer) {
					defer wg.Done()
					peer.notifyHeartbeatC <- struct{}{}
				}(peer)
			}
			wg.Wait()
		}
	}

	// clean resources

}

func (r *raftNode) heartbeatPeer(ctx context.Context, peer *Peer) {
	select {
	case <-ctx.Done():
		// TODO: clean resources
		return
	case <-peer.notifyHeartbeatC:
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
		r.appendEntriesRespC <- &Response{
			PeerID: peer.Id(),
			Resp:   resp,
			Err:    err,
		}
	}
}
