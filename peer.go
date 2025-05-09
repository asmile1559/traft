package traft

import (
	"context"
	raftpb "github.com/asmile1559/traft/internal/apis/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"sync"
)

const (
	chanSize = 10
)

type Peer struct {
	ID         string
	Address    string
	NextIndex  uint64
	MatchIndex uint64

	NotifyHeartbeatC         chan struct{}
	NotifyAppendEntriesC     chan struct{}
	NotifyAppendEntriesRespC chan *Response

	cc *grpc.ClientConn
	l  sync.Mutex
}

func NewPeer(ctx context.Context, id, address string) (*Peer, error) {
	cc, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		return nil, err
	}

	p := &Peer{
		ID:                       id,
		Address:                  address,
		NextIndex:                0,
		MatchIndex:               0,
		NotifyHeartbeatC:         make(chan struct{}, chanSize),
		NotifyAppendEntriesC:     make(chan struct{}, chanSize),
		NotifyAppendEntriesRespC: make(chan *Response, chanSize),
		cc:                       cc,
	}

	return p, nil
}

func (p *Peer) SendAppendEntriesRequest(ctx context.Context, req *raftpb.AppendEntriesReq) (*raftpb.AppendEntriesResp, error) {
	client := raftpb.NewTRaftServiceClient(p.cc)
	resp, err := client.AppendEntries(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (p *Peer) SendInstallSnapshotRequest(ctx context.Context, req *raftpb.InstallSnapshotReq) (*raftpb.InstallSnapshotResp, error) {
	client := raftpb.NewTRaftServiceClient(p.cc)
	resp, err := client.InstallSnapshot(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (p *Peer) SendRequestVoteRequest(ctx context.Context, req *raftpb.RequestVoteReq) (*raftpb.RequestVoteResp, error) {
	client := raftpb.NewTRaftServiceClient(p.cc)
	resp, err := client.RequestVote(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (p *Peer) Update(nextIndex, matchIndex uint64) {
	p.l.Lock()
	defer p.l.Unlock()
	p.NextIndex = nextIndex
	p.MatchIndex = matchIndex
	return
}

func (p *Peer) Reset(nextIndex uint64) {
	p.Update(nextIndex, 0)
}

func (p *Peer) Close() {
	p.l.Lock()
	defer p.l.Unlock()
	if p.cc != nil {
		_ = p.cc.Close()
	}
	close(p.NotifyHeartbeatC)
	close(p.NotifyAppendEntriesC)
	close(p.NotifyAppendEntriesRespC)
}
