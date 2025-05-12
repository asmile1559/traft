package traft

import (
	raftpb "github.com/asmile1559/traft/internal/apis/raft"
	"log/slog"
	"os"
	"testing"
	"time"
)

func TestApplyLogs(t *testing.T) {
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
		role:        Follower,
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
		peers:            make(map[string]*Peer),
		logger:           logger,
	}

	rn.commitIndex = 3
	rn.applyLogs()

	if len(rn.walogs) != 14 {
		t.Errorf("expected walogs length to be 14, got %d", len(rn.walogs))
	}

	if rn.walogs[0].Index != 3 {
		t.Errorf("expected walogs[0].Index to be 3, got %d", rn.walogs[0].Index)
	}

	rn.commitIndex = 12
	rn.applyLogs()
	if len(rn.walogs) != 5 {
		t.Errorf("expected walogs length to be 15, got %d", len(rn.walogs))
	}

	if rn.walogs[0].Index != 12 {
		t.Errorf("expected walogs[0].Index to be 12, got %d", rn.walogs[0].Index)
	}

	rn.commitIndex = 16
	rn.applyLogs()
	if len(rn.walogs) != 1 {
		t.Errorf("expected walogs length to be 1, got %d", len(rn.walogs))
	}
}
