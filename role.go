package traft

type Role string

const (
	Leader    Role = "leader"
	Candidate Role = "candidate"
	Follower  Role = "follower"
)

func (r *raftNode) Role() Role {
	return r.role
}

func (r *raftNode) transitionToFollower(term uint64, votedFor string) {
	r.role = Follower
	// 更新当前任期
	r.currentTerm = term
	// 更新投票人
	r.votedFor = votedFor
	// 开启选举定时器
	r.electionTimer.Reset(RandomElectionTimeout())
	// 关闭心跳定时器
	r.heartbeatTicker.Stop()
}

func (r *raftNode) transitionToCandidate() {
	r.role = Candidate
	// 更新当前任期
	r.currentTerm++
	// 重置选票
	r.votedFor = r.id
	// 开启选举定时器
	r.electionTimer.Reset(RandomElectionTimeout())
	// 关闭心跳定时器
	r.heartbeatTicker.Stop()
}

func (r *raftNode) transitionToLeader() {
	r.role = Leader
	// 开启心跳定时器
	r.heartbeatTicker.Reset(HeartbeatInterval)
	// 关闭选举定时器
	r.electionTimer.Stop()

	r.mu.Lock()
	nextIndex := r.lastLogIndex() + 1
	// 初始化 nextIndex 和 matchIndex
	for _, peer := range r.peers {
		peer.Reset(nextIndex)
	}
	r.mu.Unlock()
}
