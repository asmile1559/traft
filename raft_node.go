package traft

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"time"

	raftpb "github.com/asmile1559/traft/internal/apis/raft"
)

const (
	VotedForNone = ""
)

type RaftNode interface {
	// transition role
	transitionToFollower(term uint64, votedFor string)
	transitionToCandidate()
	transitionToLeader()

	listenApplyLogsRequest(ctx context.Context)
	listenAppendEntriesRequest(ctx context.Context)
	listenHandleResultRequest(ctx context.Context)
	listenInstallSnapshotRequest(ctx context.Context)

	// leader
	waitHeartbeat(ctx context.Context)

	// candidate
	waitElection(ctx context.Context)

	// Serve the raft node
	Serve() error

	// Recover the raft node
	Recover() error
}

type raftNode struct {

	// raft node must implement TRaftService
	raftpb.UnimplementedTRaftServiceServer

	// Metadata
	id          string
	addr        string
	role        Role // leader, follower, candidate
	currentTerm uint64
	votedFor    string

	// write ahead logs 预写日志
	walogs []*raftpb.LogEntry
	mu     sync.RWMutex

	// volatile state on all servers
	commitIndex uint64 // 已提交的日志索引
	lastApplied uint64 // 已经交由状态机处理的日志索引, lastApplied <= commitIndex

	// snapshot for state
	snapshot *raftpb.Snapshot // 上一个状态的快照

	// used for executing walogs entries
	stateMachine StateMachine // 状态机接口，用于执行日志条目

	// used for persisting data
	persister Persister // 持久化接口，持久化数据

	// 选举定时器
	// when electionTimer expires, start a new waitElection, it used by follower and candidate
	electionTimer *time.Timer

	// 心跳定时器
	// when heartbeatTicker expires, send waitHeartbeat to all followers, it used by leader
	heartbeatTicker *time.Ticker

	applyC           chan struct{}
	appendEntriesC   chan string // chan of peer id, used to notify append entries
	installSnapshotC chan string // chan of peer id, used to notify install snapshot
	handleResultC    chan *Result
	// only provide read operation, each peer in raft node has its own mutex.
	peers map[string]*Peer

	logger *slog.Logger
}

type Config struct {
	// Metadata
	Id string

	// IP Address
	Addr string

	// State machine
	StateMachine StateMachine

	// Persister
	Persister Persister

	// Peers, map[id] = addr
	Peers map[string]string
}

func New(config *Config) RaftNode {

	peers := make(map[string]*Peer)
	for id, addr := range config.Peers {
		if id == config.Id {
			continue
		}
		p, err := NewPeer(id, addr)
		if err != nil {
			return nil
		}
		peers[id] = p
	}

	peerCount := len(peers)

	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelDebug,
	})

	logger := slog.New(h).With(
		"raft_id", config.Id,
		"raft_addr", config.Addr,
	)

	return &raftNode{
		id:               config.Id,
		addr:             config.Addr,
		role:             Follower,
		currentTerm:      0,
		votedFor:         VotedForNone,
		walogs:           make([]*raftpb.LogEntry, 1),
		commitIndex:      0,
		lastApplied:      0,
		snapshot:         nil,
		stateMachine:     config.StateMachine,
		persister:        config.Persister,
		electionTimer:    time.NewTimer(RandomElectionTimeout()),
		heartbeatTicker:  time.NewTicker(HeartbeatInterval),
		applyC:           make(chan struct{}, 1),
		appendEntriesC:   make(chan string, 2*peerCount),
		handleResultC:    make(chan *Result, 2*peerCount),
		installSnapshotC: make(chan string, 2*peerCount),
		peers:            peers,
		logger:           logger,
	}
}
