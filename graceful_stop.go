package traft

import (
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
)

func (r *raftNode) gracefulStop(server *grpc.Server, listener net.Listener) {

	// signal to stop the election and heartbeat goroutines
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC,
		os.Interrupt,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	)

	// wait for the signal
	<-sigC

	// 关闭选举定时器
	r.electionTimer.Stop()
	// 关闭心跳定时器
	r.heartbeatTicker.Stop()
	// 关闭 appendEntriesC 通道
	close(r.appendEntriesC)
	// 关闭 appendEntriesRespC 通道
	// close(r.appendEntriesRespC)
	// 关闭 installSnapshotC 通道
	close(r.installSnapshotC)
	// 关闭 applyC 通道
	close(r.applyC)

	// 关闭所有的 peer
	for _, peer := range r.peers {
		peer.Close()
	}

	server.GracefulStop()
	_ = listener.Close()
}
