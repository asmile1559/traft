package traft

import (
	raftpb "github.com/asmile1559/traft/internal/apis/raft"
	"log/slog"
	"os"
	"testing"
	"time"
)

func TestRaftNode_RequestVote(t *testing.T) {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelDebug,
	})

	logger := slog.New(h)
	kv := NewKVStateMachine()
	p := NewFilePersister(TestDir)
	rn := &raftNode{
		id:          "node0",
		addr:        "localhost:12344",
		role:        Leader,
		currentTerm: 3,
		votedFor:    "node1",
		walogs: []*raftpb.LogEntry{
			{Index: 0, Term: 0},
			{Index: 1, Term: 1, Data: []byte(`{"operation": "put", "key": "a", "value": "1"}`)},
			{Index: 2, Term: 1, Data: []byte(`{"operation": "put", "key": "b", "value": "2"}`)},
			{Index: 3, Term: 1, Data: []byte(`{"operation": "put", "key": "c", "value": "3"}`)},
			{Index: 4, Term: 1, Data: []byte(`{"operation": "put", "key": "d", "value": "4"}`)},
			{Index: 5, Term: 1, Data: []byte(`{"operation": "put", "key": "e", "value": "5"}`)},
			{Index: 6, Term: 1, Data: []byte(`{"operation": "put", "key": "f", "value": "6"}`)},
			{Index: 7, Term: 2, Data: []byte(`{"operation": "put", "key": "g", "value": "7"}`)},
			{Index: 8, Term: 2, Data: []byte(`{"operation": "put", "key": "h", "value": "8"}`)},
			{Index: 9, Term: 2, Data: []byte(`{"operation": "put", "key": "i", "value": "9"}`)},
			{Index: 10, Term: 3, Data: []byte(`{"operation": "put", "key": "j", "value": "10"}`)},
			{Index: 11, Term: 3, Data: []byte(`{"operation": "put", "key": "k", "value": "11"}`)},
			{Index: 12, Term: 4, Data: []byte(`{"operation": "put", "key": "l", "value": "12"}`)},
			{Index: 13, Term: 4, Data: []byte(`{"operation": "put", "key": "m", "value": "13"}`)},
			{Index: 14, Term: 4, Data: []byte(`{"operation": "put", "key": "n", "value": "14"}`)},
			{Index: 15, Term: 5, Data: []byte(`{"operation": "put", "key": "o", "value": "15"}`)},
			{Index: 16, Term: 5, Data: []byte(`{"operation": "put", "key": "p", "value": "16"}`)},
		},
		commitIndex:      3,
		lastApplied:      0,
		snapshot:         nil,
		stateMachine:     kv,
		persister:        p,
		electionTimer:    time.NewTimer(time.Hour),
		heartbeatTicker:  time.NewTicker(time.Hour),
		applyC:           make(chan struct{}, 1),
		appendEntriesC:   make(chan string, 1),
		installSnapshotC: make(chan string, 1),
		handleResultC:    make(chan *Result, 1),
		peers:            make(map[string]*Peer),
		logger:           logger,
	}

	req := &raftpb.RequestVoteReq{
		Term:         3,
		CandidateId:  "node1",
		LastLogIndex: 16,
		LastLogTerm:  5,
	}

	resp, err := rn.RequestVote(nil, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.VoteGranted {
		t.Fatalf("expected vote not granted, got granted")
	}

	req.Term = 4
	resp, err = rn.RequestVote(nil, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.VoteGranted {
		t.Fatalf("expected vote granted, got not granted")
	}
	if rn.votedFor != "node1" {
		t.Fatalf("expected votedFor to be node1, got %s", rn.votedFor)
	}
	if rn.currentTerm != 4 {
		t.Fatalf("expected currentTerm to be 4, got %d", rn.currentTerm)
	}
	if rn.role != Follower {
		t.Fatalf("expected role to be Follower, got %s", rn.role)
	}

	req.Term = 5
	req.CandidateId = "node2"
	req.LastLogIndex = 15
	req.LastLogTerm = 5
	resp, err = rn.RequestVote(nil, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.VoteGranted {
		t.Fatalf("expected vote not granted, got granted")
	}
	if rn.votedFor != "" {
		t.Fatalf("expected votedFor to be void, got %s", rn.votedFor)
	}
	if rn.currentTerm != 5 {
		t.Fatalf("expected currentTerm to be 5, got %d", rn.currentTerm)
	}

}
