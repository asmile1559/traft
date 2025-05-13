package traft

import (
	"context"
	raftpb "github.com/asmile1559/traft/internal/apis/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log/slog"
)

type Peer struct {
	id         string
	address    string
	nextIndex  uint64
	matchIndex uint64

	logger *slog.Logger
	cc     *grpc.ClientConn
}

func (p *Peer) Id() string {
	return p.id
}

func (p *Peer) MatchIndex() uint64 {
	return p.matchIndex
}

func (p *Peer) SetMatchIndex(matchIndex uint64) {
	p.matchIndex = matchIndex
}

func (p *Peer) NextIndex() uint64 {
	return p.nextIndex
}

func (p *Peer) SetNextIndex(nextIndex uint64) {
	p.nextIndex = nextIndex
}

func NewPeer(id, address string) (*Peer, error) {
	cc, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		return nil, err
	}

	logger := FormatLogger("peer", slog.String("id", id), slog.String("address", address))
	p := &Peer{
		id:         id,
		address:    address,
		nextIndex:  0,
		matchIndex: 0,
		logger:     logger,
		cc:         cc,
	}

	return p, nil
}

func (p *Peer) SendAppendEntriesRequest(ctx context.Context, req *raftpb.AppendEntriesReq) (*raftpb.AppendEntriesResp, error) {
	p.logger.Debug("Peer_SendAppendEntriesRequest")
	client := raftpb.NewTRaftServiceClient(p.cc)
	resp, err := client.AppendEntries(ctx, req)
	if err != nil {
		p.logger.Error("SendAppendEntriesRequest failed", slog.String("error", err.Error()))
		return nil, err
	}
	return resp, nil
}

func (p *Peer) SendInstallSnapshotRequest(ctx context.Context, req *raftpb.InstallSnapshotReq) (*raftpb.InstallSnapshotResp, error) {
	p.logger.Debug("Peer_SendInstallSnapshotRequest")
	client := raftpb.NewTRaftServiceClient(p.cc)
	resp, err := client.InstallSnapshot(ctx, req)
	if err != nil {
		p.logger.Error("SendInstallSnapshotRequest failed", slog.String("error", err.Error()))
		return nil, err
	}
	return resp, nil
}

func (p *Peer) SendRequestVoteRequest(ctx context.Context, req *raftpb.RequestVoteReq) (*raftpb.RequestVoteResp, error) {
	p.logger.Debug("Peer_SendRequestVoteRequest")
	client := raftpb.NewTRaftServiceClient(p.cc)
	resp, err := client.RequestVote(ctx, req)
	if err != nil {
		p.logger.Error("SendRequestVoteRequest failed", slog.String("error", err.Error()))
		return nil, err
	}
	return resp, nil
}

func (p *Peer) UpdateNextIndex(nextIndex uint64) {
	p.nextIndex = nextIndex
}

func (p *Peer) UpdateMatchIndex(matchIndex uint64) {
	p.matchIndex = matchIndex
}

func (p *Peer) Update(nextIndex, matchIndex uint64) {
	p.nextIndex = nextIndex
	p.matchIndex = matchIndex
	return
}

func (p *Peer) Reset(nextIndex uint64) {
	p.Update(nextIndex, 0)
}

func (p *Peer) Close() {
	if p.cc != nil {
		_ = p.cc.Close()
	}
	p.cc = nil
}
