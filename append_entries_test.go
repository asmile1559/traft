package traft

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	raftpb "github.com/asmile1559/traft/internal/apis/raft"
)

func TestRaftNode_AppendEntries(t *testing.T) {
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

	ctx := context.Background()
	// Test case 1: request with a term less than the current term
	req := &raftpb.AppendEntriesReq{
		Term:         2,
		LeaderId:     "node1",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: 0,
	}

	resp, err := rn.AppendEntries(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v\n", err)
	}
	if resp.Success != false {
		t.Fatalf("unexpected success\n")
	}

	// Test case 2: request with a term equal to the current term
	req = &raftpb.AppendEntriesReq{
		Term:         3,
		LeaderId:     "node1",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
	}

	// checkLogMatch should return true
	resp, err = rn.AppendEntries(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v\n", err)
	}
	if resp.Success != true {
		t.Fatalf("unexpected failure\n")
	}

	// Test case 3: request with a term greater than the current term
	rn.role = Leader
	req = &raftpb.AppendEntriesReq{
		Term:         4,
		LeaderId:     "node2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: 0,
	}

	resp, err = rn.AppendEntries(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v\n", err)
	}
	if resp.Success != true {
		t.Fatalf("unexpected failure\n")
	}
	if rn.currentTerm != 4 {
		t.Fatalf("unexpected current term: %d\n", rn.currentTerm)
	}
	if rn.role != Follower {
		t.Fatalf("unexpected role: %s\n", rn.role)
	}
	if rn.votedFor != "node2" {
		t.Fatalf("unexpected votedFor: %s\n", rn.votedFor)
	}

	// Test case 4: append entries for rn
	req = &raftpb.AppendEntriesReq{
		Term:         4,
		LeaderId:     "node2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []*raftpb.LogEntry{
			{Index: 1, Term: 4, Data: []byte(`{"operation": "put", "key": "a", "value": "1"}`)},
		},
		LeaderCommit: 0,
	}

	resp, err = rn.AppendEntries(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v\n", err)
	}

	if resp.Success != true {
		t.Fatalf("unexpected failure\n")
	}

	if len(rn.walogs) != 2 {
		t.Fatalf("unexpected walogs length: %d\n", len(rn.walogs))
	}

	if rn.walogs[1].Index != 1 || rn.walogs[1].Term != 4 {
		t.Fatalf("unexpected walogs: %v\n", rn.walogs[1])
	}

	if resp.MatchIndex != 1 {
		t.Fatalf("unexpected match index: %d\n", resp.MatchIndex)
	}

	// Test case 5: commit index
	req = &raftpb.AppendEntriesReq{
		Term:         4,
		LeaderId:     "node2",
		PrevLogIndex: 1,
		PrevLogTerm:  4,
		Entries:      nil,
		LeaderCommit: 1,
	}

	resp, err = rn.AppendEntries(ctx, req)
	<-rn.applyC
	_ = rn.stateMachine.ApplyLog(rn.walogs[1].Data)
	rn.lastApplied++
	if err != nil {
		t.Fatalf("unexpected error: %v\n", err)
	}
	if resp.Success != true {
		t.Fatalf("unexpected failure\n")
	}
	if rn.commitIndex != 1 {
		t.Fatalf("unexpected commit index: %d\n", rn.commitIndex)
	}

	rn.walogs = append(rn.walogs,
		&raftpb.LogEntry{Index: 2, Term: 4, Data: []byte(`{"operation": "put", "key": "b", "value": "2"}`)},
		&raftpb.LogEntry{Index: 3, Term: 4, Data: []byte(`{"operation": "put", "key": "c", "value": "3"}`)},
		&raftpb.LogEntry{Index: 4, Term: 4, Data: []byte(`{"operation": "put", "key": "d", "value": "4"}`)},
	)

	// Test case 6: append entries with truncation
	req = &raftpb.AppendEntriesReq{
		Term:         4,
		LeaderId:     "node2",
		PrevLogIndex: 1,
		PrevLogTerm:  4,
		Entries: []*raftpb.LogEntry{
			{Index: 2, Term: 4, Data: []byte(`{"operation": "put", "key": "e", "value": "5"}`)},
			{Index: 3, Term: 4, Data: []byte(`{"operation": "put", "key": "f", "value": "6"}`)},
		},
		LeaderCommit: 1,
	}

	resp, err = rn.AppendEntries(ctx, req)
	_ = rn.stateMachine.ApplyLog(rn.walogs[2].Data)
	_ = rn.stateMachine.ApplyLog(rn.walogs[3].Data)
	rn.lastApplied += 2
	if err != nil {
		t.Fatalf("unexpected error: %v\n", err)
	}

	if resp.Success != true {
		t.Fatalf("unexpected failure\n")
	}
	if len(rn.walogs) != 4 {
		t.Fatalf("unexpected walogs length: %d\n", len(rn.walogs))
	}
	if rn.walogs[2].Index != 2 || rn.walogs[2].Term != 4 {
		t.Fatalf("unexpected walogs: %v\n", rn.walogs[2])
	}
	if rn.walogs[3].Index != 3 || rn.walogs[3].Term != 4 {
		t.Fatalf("unexpected walogs: %v\n", rn.walogs[3])
	}
	if resp.MatchIndex != 3 {
		t.Fatalf("unexpected match index: %d\n", resp.MatchIndex)
	}
}

func TestRaftNode_AppendEntries2(t *testing.T) {
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

	ctx := context.Background()

	// Test case 1: ErrLogInvalidIndex
	req := &raftpb.AppendEntriesReq{
		Term:         4,
		LeaderId:     "node2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: 0,
	}
	resp, err := rn.AppendEntries(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v\n", err)
	}
	if resp.Success != false {
		t.Fatalf("unexpected success\n")
	}
	if resp.ConflictTerm != 0 || resp.ConflictIndex != 0 {
		t.Fatalf("unexpected conflict term and index: %d, %d\n", resp.ConflictTerm, resp.ConflictIndex)
	}

	rn.walogs = []*raftpb.LogEntry{
		{Index: 0, Term: 0},
		{Index: 1, Term: 1, Data: []byte(`{"operation": "put", "key": "a", "value": "1"}`)},
		{Index: 2, Term: 2, Data: []byte(`{"operation": "put", "key": "b", "value": "2"}`)},
		{Index: 3, Term: 2, Data: []byte(`{"operation": "put", "key": "c", "value": "3"}`)},
		{Index: 4, Term: 3, Data: []byte(`{"operation": "put", "key": "d", "value": "4"}`)},
		{Index: 5, Term: 3, Data: []byte(`{"operation": "put", "key": "e", "value": "5"}`)},
		{Index: 6, Term: 4, Data: []byte(`{"operation": "put", "key": "f", "value": "6"}`)},
		{Index: 7, Term: 4, Data: []byte(`{"operation": "put", "key": "g", "value": "7"}`)},
		{Index: 8, Term: 4, Data: []byte(`{"operation": "put", "key": "h", "value": "8"}`)},
	}
	// Test case 2: ErrLogEntryConflict
	req = &raftpb.AppendEntriesReq{
		Term:         4,
		LeaderId:     "node1",
		PrevLogIndex: 5,
		PrevLogTerm:  2,
		Entries:      nil,
		LeaderCommit: 0,
	}
	resp, err = rn.AppendEntries(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v\n", err)
	}
	if resp.Success != false {
		t.Fatalf("unexpected success\n")
	}
	if resp.ConflictTerm != 3 || resp.ConflictIndex != 4 {
		t.Fatalf("unexpected conflict term and index: %d, %d\n", resp.ConflictTerm, resp.ConflictIndex)
	}

	// Test case 3: ErrLogIndexOutOfRange
	req = &raftpb.AppendEntriesReq{
		Term:         4,
		LeaderId:     "node1",
		PrevLogIndex: 10,
		PrevLogTerm:  4,
		Entries:      nil,
		LeaderCommit: 0,
	}

	resp, err = rn.AppendEntries(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v\n", err)
	}
	if resp.Success != false {
		t.Fatalf("unexpected success\n")
	}
	if resp.ConflictTerm != 0 || resp.ConflictIndex != 9 {
		t.Fatalf("unexpected conflict term and index: %d, %d\n", resp.ConflictTerm, resp.ConflictIndex)
	}

	// Test case 4: ErrLogEntryCompacted
	rn.walogs = []*raftpb.LogEntry{
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
	}
	req = &raftpb.AppendEntriesReq{
		Term:         4,
		LeaderId:     "node1",
		PrevLogIndex: 5,
		PrevLogTerm:  1,
		Entries:      nil,
		LeaderCommit: 5,
	}
	resp, err = rn.AppendEntries(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v\n", err)
	}
	if resp.Success != false {
		t.Fatalf("unexpected success\n")
	}
	if resp.ConflictTerm != 0 || resp.ConflictIndex != 0 {
		t.Fatalf("unexpected conflict term and index: %d, %d\n", resp.ConflictTerm, resp.ConflictIndex)
	}
}
