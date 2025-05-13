package traft

import (
	"context"
	raftpb "github.com/asmile1559/traft/internal/apis/raft"
	"log/slog"
	"os"
	"testing"
	"time"
)

func TestInstallSnapshot(t *testing.T) {

	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelDebug,
	})

	logger := slog.New(h)
	kv := NewKVStateMachine()
	p := NewFilePersister(TestDir + "/node0")
	rn := &raftNode{
		id:          "node0",
		addr:        "localhost:12344",
		role:        Leader,
		currentTerm: 5,
		votedFor:    "node0",
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
		commitIndex:      8,
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

	rn.applyLogs()
	snapshotData, err := rn.stateMachine.TakeSnapshot()
	if err != nil {
		t.Fatalf("take snapshot failed: %v", err)
	}

	snapshot := &raftpb.Snapshot{
		LastIncludedIndex: 8,
		LastIncludedTerm:  2,
		Data:              snapshotData,
	}

	entries, err := rn.extractLogEntries(8, WALogEnd)
	if err != nil {
		t.Fatalf("extract log entries failed: %v", err)
	}
	req := &raftpb.InstallSnapshotReq{
		Term:     5,
		LeaderId: "node0",
		Snapshot: snapshot,
		Entries:  entries,
	}

	kv1 := NewKVStateMachine()
	p1 := NewFilePersister(TestDir + "/node1")
	rn1 := &raftNode{
		id:          "node1",
		addr:        "localhost:12345",
		role:        Follower,
		currentTerm: 5,
		votedFor:    "node0",
		walogs: []*raftpb.LogEntry{
			{Index: 0, Term: 0},
		},
		commitIndex:      0,
		lastApplied:      0,
		snapshot:         nil,
		stateMachine:     kv1,
		persister:        p1,
		electionTimer:    time.NewTimer(time.Hour),
		heartbeatTicker:  time.NewTicker(time.Hour),
		applyC:           make(chan struct{}, 1),
		appendEntriesC:   make(chan string, 1),
		installSnapshotC: make(chan string, 1),
		handleResultC:    make(chan *Result, 1),
		peers:            make(map[string]*Peer),
		logger:           logger,
	}

	ctx := context.Background()
	resp, err := rn1.InstallSnapshot(ctx, req)
	if err != nil {
		return
	}

	if resp.Success {
		t.Logf("install snapshot success")
	} else {
		t.Errorf("install snapshot failed")
	}

	if len(kv1.store) != 8 {
		t.Errorf("expected 8 keys, got %d", len(kv1.store))
	}

	if len(rn1.walogs) != 9 {
		t.Errorf("expected 9 walogs, got %d", len(rn1.walogs))
	}
}
