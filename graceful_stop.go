package traft

import (
	"google.golang.org/grpc"
	"net"
	"os"
)

func (r *raftNode) gracefulStop(server *grpc.Server, listener net.Listener, sigC <-chan os.Signal) {

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
