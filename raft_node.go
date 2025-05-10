package traft

import (
	"context"
	raftpb "github.com/asmile1559/traft/internal/apis/raft"
	"google.golang.org/grpc"
	"net"
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
	heartbeat(ctx context.Context)
	appendEntries(ctx context.Context)

	// candidate
	election(ctx context.Context)

	// Start the raft node
	Start() error

	// recover the raft node
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

	// write ahead log 预写日志
	log []*raftpb.LogEntry
	mu  sync.RWMutex

	// volatile state on all servers
	commitIndex uint64 // 已提交的日志索引
	lastApplied uint64 // 已经交由状态机处理的日志索引, lastApplied <= commitIndex

	// snapshot for state
	snapshot *raftpb.Snapshot // 上一个状态的快照

	// used for executing log entries
	stateMachine StateMachine // 状态机接口，用于执行日志条目

	// used for persisting data
	persister Persister // 持久化接口，持久化数据

	// 选举定时器
	// when electionTimer expires, start a new election, it used by follower and candidate
	electionTimer *time.Timer

	// 心跳定时器
	// when heartbeatTicker expires, send heartbeat to all followers, it used by leader
	heartbeatTicker *time.Ticker

	applyC             chan struct{}
	appendEntriesC     chan string // chan of peer id, used to notify append entries
	installSnapshotC   chan string // chan of peer id, used to notify install snapshot
	appendEntriesRespC chan *Response
	// only provide read operation, each peer in raft node has its own mutex.
	peers map[string]*Peer
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

	return &raftNode{
		id:                 config.Id,
		addr:               config.Addr,
		role:               Follower,
		currentTerm:        0,
		votedFor:           VotedForNone,
		log:                make([]*raftpb.LogEntry, 0),
		commitIndex:        0,
		lastApplied:        0,
		snapshot:           nil,
		stateMachine:       config.StateMachine,
		persister:          config.Persister,
		electionTimer:      time.NewTimer(RandomElectionTimeout()),
		heartbeatTicker:    time.NewTicker(HeartbeatInterval),
		applyC:             make(chan struct{}, 1),
		appendEntriesC:     make(chan string, 2*peerCount),
		appendEntriesRespC: make(chan *Response, 2*peerCount),
		installSnapshotC:   make(chan string, 2*peerCount),
		peers:              peers,
	}
}

func (r *raftNode) Start() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// start election
	go r.election(ctx)
	// start heartbeat
	go r.heartbeat(ctx)
	// start append entries
	go r.appendEntries(ctx)
	// start process response
	go r.processResponse(ctx)
	// start install snapshot
	go r.installSnapshot(ctx)
	// start apply log
	go r.applyStateMachine(ctx)

	server := grpc.NewServer()
	raftpb.RegisterTRaftServiceServer(server, r)
	listener, err := net.Listen("tcp", r.addr)
	if err != nil {
		panic(err)
	}
	go func() {
		if err := server.Serve(listener); err != nil {
			panic(err)
		}
	}()

	r.gracefulStop(server, listener)
	return nil
}

func (r *raftNode) Recover() error {
	term, votedFor, err := r.persister.LoadMetadata()
	if err != nil {
		return err
	}
	r.currentTerm = term
	r.votedFor = votedFor
	entries, err := r.persister.LoadLogEntries()
	if err != nil {
		return err
	}
	r.log = entries
	snapshot, err := r.persister.LoadSnapshot()
	if err != nil {
		return err
	}
	r.snapshot = snapshot
	_ = r.stateMachine.ApplySnapshot(snapshot.Data)
	return nil
}
