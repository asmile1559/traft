package traft

import (
	raftpb "github.com/asmile1559/traft/internal/apis/raft"
	"sync"
	"time"
)

const (
	VotedForNone = ""

	HeartbeatInterval = 100 * time.Millisecond // 心跳间隔

	// MinElectionTimeout 最小选举超时范围, 单位为毫秒
	MinElectionTimeout = 150
	// MaxElectionTimeout 最大选举超时范围, 单位为毫秒
	MaxElectionTimeout = 300
)

type RaftNode interface {
	// transition role
	transitionToFollower(term uint64, votedFor string)
	transitionToCandidate()
	transitionToLeader()

	// leader
	heartbeat()

	// candidate
	election()

	// run a cron job
	cronJob()

	// Start the raft node
	Start() error
}

type raftNode struct {

	// raft node must implement TRaftService
	raftpb.UnimplementedTRaftServiceServer

	// Metadata
	id          string
	role        Role // leader, follower, candidate
	currentTerm uint64
	votedFor    string

	// write ahead log 预写日志
	log []*raftpb.LogEntry

	// volatile state on all servers
	commitIndex uint64 // 已提交的日志索引
	lastApplied uint64 // 已经交由状态机处理的日志索引, lastApplied <= commitIndex

	// volatile state on leaders
	nextIndex  map[string]uint64 // 下一个日志索引, key为节点id
	matchIndex map[string]uint64 // 已经复制到大多数节点的日志索引, key为节点id

	// snapshot for state
	snapshot *raftpb.Snapshot // 上一个状态的快照

	// used for executing log entries
	stateMachine StateMachine // 状态机接口，用于执行日志条目

	// used for persisting data
	// persister Persister // 持久化接口，持久化数据

	// 选举定时器
	// when electionTimer expires, start a new election, it used by follower and candidate
	electionTimer *time.Timer

	// 心跳定时器
	// when heartbeatTicker expires, send heartbeat to all followers, it used by leader
	heartbeatTicker *time.Ticker

	peers []string // 其他节点的地址

	mu sync.RWMutex
}
