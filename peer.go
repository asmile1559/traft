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
	id         string
	address    string
	nextIndex  uint64
	matchIndex uint64

	notifyHeartbeatC chan struct{}

	cc *grpc.ClientConn
	l  sync.Mutex
}

func (p *Peer) Id() string {
	return p.id
}

func (p *Peer) WaitHeartbeat() struct{} {
	return <-p.notifyHeartbeatC
}

func (p *Peer) MatchIndex() uint64 {
	p.l.Lock()
	defer p.l.Unlock()
	return p.matchIndex
}

func (p *Peer) SetMatchIndex(matchIndex uint64) {
	p.l.Lock()
	defer p.l.Unlock()
	p.matchIndex = matchIndex
}

func (p *Peer) NextIndex() uint64 {
	p.l.Lock()
	defer p.l.Unlock()
	return p.nextIndex
}

func (p *Peer) SetNextIndex(nextIndex uint64) {
	p.l.Lock()
	defer p.l.Unlock()
	p.nextIndex = nextIndex
}

func NewPeer(id, address string) (*Peer, error) {
	cc, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		return nil, err
	}

	p := &Peer{
		id:               id,
		address:          address,
		nextIndex:        0,
		matchIndex:       0,
		notifyHeartbeatC: make(chan struct{}, chanSize),
		cc:               cc,
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

func (p *Peer) UpdateNextIndex(nextIndex uint64) {
	p.l.Lock()
	defer p.l.Unlock()
	p.nextIndex = nextIndex
}

func (p *Peer) UpdateMatchIndex(matchIndex uint64) {
	p.l.Lock()
	defer p.l.Unlock()
	p.matchIndex = matchIndex
}

func (p *Peer) Update(nextIndex, matchIndex uint64) {
	p.l.Lock()
	defer p.l.Unlock()
	p.nextIndex = nextIndex
	p.matchIndex = matchIndex
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
	p.cc = nil
	close(p.notifyHeartbeatC)
}
