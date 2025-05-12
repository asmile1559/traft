package traft

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	raftpb "github.com/asmile1559/traft/internal/apis/raft"
)

func TestHandleResult(t *testing.T) {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelDebug,
	})

	logger := slog.New(h)
	kv := NewKVStateMachine()
	p := NewFilePersister(TestDir)
	peer, err := NewPeer("node1", "localhost:12345")
	if err != nil {
		t.Fatalf("unexpected error: %v\n", err)
	}
	rn := &raftNode{
		id:          "node0",
		addr:        "localhost:12344",
		role:        Leader,
		currentTerm: 4,
		votedFor:    "node0",
		walogs: []*raftpb.LogEntry{
			{Index: 0, Term: 0},
			{Index: 1, Term: 1, Data: []byte(`{"operation": "put", "key": "a", "value": "1"}`)},
			{Index: 2, Term: 2, Data: []byte(`{"operation": "put", "key": "b", "value": "2"}`)},
			{Index: 3, Term: 2, Data: []byte(`{"operation": "put", "key": "c", "value": "3"}`)},
			{Index: 4, Term: 3, Data: []byte(`{"operation": "put", "key": "d", "value": "4"}`)},
			{Index: 5, Term: 3, Data: []byte(`{"operation": "put", "key": "e", "value": "5"}`)},
			{Index: 6, Term: 4, Data: []byte(`{"operation": "put", "key": "f", "value": "6"}`)},
			{Index: 7, Term: 4, Data: []byte(`{"operation": "put", "key": "g", "value": "7"}`)},
			{Index: 8, Term: 4, Data: []byte(`{"operation": "put", "key": "h", "value": "8"}`)},
		},
		commitIndex:      0,
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
		peers:            map[string]*Peer{"node1": peer},
		logger:           logger,
	}

	ctx := context.Background()
	result := &Result{
		PeerID: "node1",
		Resp: &raftpb.AppendEntriesResp{
			Term:       4,
			Success:    true,
			MatchIndex: 1,
		},
	}

	// Test case 1: Successful AppendEntries response
	rn.handleResult(ctx, result)

	if peer.NextIndex() != 2 || peer.MatchIndex() != 1 {
		t.Errorf("expected nextIndex: 2, matchIndex: 1, got nextIndex: %d, matchIndex: %d", peer.NextIndex(), peer.MatchIndex())
	}

	// Test case 2: Failed AppendEntries response, ErrInvalidIndex and ErrLogAlreadySnapshot
	result.Resp = &raftpb.AppendEntriesResp{
		Term:          4,
		Success:       false,
		ConflictTerm:  0,
		ConflictIndex: 0,
	}

	rn.handleResult(ctx, result)
	id := <-rn.installSnapshotC
	if id != "node1" {
		t.Errorf("expected installSnapshotC to be called with node1, got %s", id)
	}

	// Test case 3: Failed AppendEntries response, ErrLogOutOfRange
	result.Resp = &raftpb.AppendEntriesResp{
		Term:          4,
		Success:       false,
		ConflictTerm:  0,
		ConflictIndex: 4,
	}

	rn.handleResult(ctx, result)
	id = <-rn.appendEntriesC
	if id != "node1" {
		t.Errorf("expected installSnapshotC to be called with node1, got %s", id)
	}
	if peer.NextIndex() != 4 {
		t.Errorf("expected nextIndex: 4, got %d", peer.NextIndex())
	}

	// Test case 4: Failed AppendEntries response, ErrLogConflict
	result.Resp = &raftpb.AppendEntriesResp{
		Term:          4,
		Success:       false,
		ConflictTerm:  3,
		ConflictIndex: 3,
	}

	rn.handleResult(ctx, result)
	id = <-rn.appendEntriesC
	if id != "node1" {
		t.Errorf("expected installSnapshotC to be called with node1, got %s", id)
	}
	if peer.NextIndex() != 4 {
		t.Errorf("expected nextIndex: 4, got %d", peer.NextIndex())
	}

	rn.walogs = []*raftpb.LogEntry{
		{Index: 4, Term: 3, Data: []byte(`{"operation": "put", "key": "d", "value": "4"}`)},
		{Index: 5, Term: 3, Data: []byte(`{"operation": "put", "key": "e", "value": "5"}`)},
		{Index: 6, Term: 4, Data: []byte(`{"operation": "put", "key": "f", "value": "6"}`)},
		{Index: 7, Term: 4, Data: []byte(`{"operation": "put", "key": "g", "value": "7"}`)},
		{Index: 8, Term: 4, Data: []byte(`{"operation": "put", "key": "h", "value": "8"}`)},
	}
	rn.handleResult(ctx, result)
	id = <-rn.installSnapshotC
	if id != "node1" {
		t.Errorf("expected installSnapshotC to be called with node1, got %s", id)
	}

	result.Resp = &raftpb.AppendEntriesResp{
		Term:          4,
		Success:       false,
		ConflictTerm:  4,
		ConflictIndex: 5,
	}

	rn.handleResult(ctx, result)
	id = <-rn.appendEntriesC
	if id != "node1" {
		t.Errorf("expected installSnapshotC to be called with node1, got %s", id)
	}
	if peer.NextIndex() != 6 {
		t.Errorf("expected nextIndex: 6, got %d", peer.NextIndex())
	}
}
