package traft

import (
	"context"
	"fmt"
	raftpb "github.com/asmile1559/traft/internal/apis/raft"
	"google.golang.org/grpc"
	"net"
	"os"
	"os/signal"
	"syscall"
)

func (r *raftNode) Serve() error {
	r.logger.Debug("start raft node")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := grpc.NewServer()
	raftpb.RegisterTRaftServiceServer(server, r)
	listener, err := net.Listen("tcp", r.addr)
	if err != nil {
		err := fmt.Errorf("failed to listen: %w", err)
		r.logger.Error(err.Error())
		return err
	}

	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC,
		os.Interrupt,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	)

	r.startWaitService(ctx)
	r.startListeningService(ctx)
	go func() {
		if err := server.Serve(listener); err != nil {
			err = fmt.Errorf("failed to serve: %w", err)
			r.logger.Error(err.Error())
			cancel()
			sigC <- os.Interrupt
		}
	}()

	r.gracefulStop(server, listener, sigC)
	return nil
}

func (r *raftNode) startWaitService(ctx context.Context) {
	// wait for election timer
	go r.waitElection(ctx)
	// wait for heartbeat timer
	go r.waitHeartbeat(ctx)
}

func (r *raftNode) startListeningService(ctx context.Context) {
	go r.listenAppendEntriesRequest(ctx)
	go r.listenInstallSnapshotRequest(ctx)
	go r.listenHandleResultRequest(ctx)
	go r.listenApplyLogsRequest(ctx)
	r.transitionToFollower(0, VotedForNone)
}

func (r *raftNode) cleanup() {
	r.electionTimer.Stop()
	r.heartbeatTicker.Stop()
	close(r.applyC)
	close(r.appendEntriesC)
	close(r.handleResultC)
	close(r.installSnapshotC)

	for _, peer := range r.peers {
		peer.Close()
	}
	_ = r.persister.Close()
	_ = r.stateMachine.Close()
}
