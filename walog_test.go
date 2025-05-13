package traft

import (
	"errors"
	"log/slog"
	"os"
	"testing"

	raftpb "github.com/asmile1559/traft/internal/apis/raft"
)

func TestLastLogIndex(t *testing.T) {
	rn := &raftNode{
		walogs: []*raftpb.LogEntry{
			{Index: 0, Term: 0},
		},
	}
	if rn.lastLogIndex() != 0 {
		t.Errorf("expected last walogs index to be 2, got %d", rn.lastLogIndex())
	}

	rn.walogs = append(rn.walogs, &raftpb.LogEntry{Index: 1, Term: 1})
	if rn.lastLogIndex() != 1 {
		t.Errorf("expected last walogs index to be 1, got %d", rn.lastLogIndex())
	}

	rn.walogs = append(rn.walogs, &raftpb.LogEntry{Index: 2, Term: 2})
	if rn.lastLogIndex() != 2 {
		t.Errorf("expected last walogs index to be 2, got %d", rn.lastLogIndex())
	}

}

func TestLastLogTerm(t *testing.T) {
	rn := &raftNode{
		walogs: []*raftpb.LogEntry{
			{Index: 0, Term: 0},
		},
	}
	if rn.lastLogTerm() != 0 {
		t.Errorf("expected last walogs term to be 0, got %d", rn.lastLogTerm())
	}

	rn.walogs = append(rn.walogs, &raftpb.LogEntry{Index: 1, Term: 1})
	if rn.lastLogTerm() != 1 {
		t.Errorf("expected last walogs term to be 1, got %d", rn.lastLogTerm())
	}

	rn.walogs = append(rn.walogs, &raftpb.LogEntry{Index: 2, Term: 2})
	if rn.lastLogTerm() != 2 {
		t.Errorf("expected last walogs term to be 2, got %d", rn.lastLogTerm())
	}
}

func TestLogAtIndex1(t *testing.T) {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelDebug,
	})

	logger := slog.New(h)
	rn := &raftNode{
		walogs: []*raftpb.LogEntry{
			{Index: 0, Term: 0},
			{Index: 1, Term: 1},
			{Index: 2, Term: 1},
			{Index: 3, Term: 1},
			{Index: 4, Term: 1},
			{Index: 5, Term: 1},
			{Index: 6, Term: 1},
			{Index: 7, Term: 2},
			{Index: 8, Term: 2},
			{Index: 9, Term: 2},
			{Index: 10, Term: 3},
			{Index: 11, Term: 3},
			{Index: 12, Term: 4},
			{Index: 13, Term: 4},
			{Index: 14, Term: 4},
			{Index: 15, Term: 5},
			{Index: 16, Term: 5},
		},
		logger: logger,
	}
	// Test valid index
	testCase := []struct {
		index    uint64
		expected *raftpb.LogEntry
	}{
		{index: 1, expected: &raftpb.LogEntry{Index: 1, Term: 1}},
		{index: 2, expected: &raftpb.LogEntry{Index: 2, Term: 1}},
		{index: 3, expected: &raftpb.LogEntry{Index: 3, Term: 1}},
		{index: 4, expected: &raftpb.LogEntry{Index: 4, Term: 1}},
		{index: 5, expected: &raftpb.LogEntry{Index: 5, Term: 1}},
		{index: 6, expected: &raftpb.LogEntry{Index: 6, Term: 1}},
		{index: 7, expected: &raftpb.LogEntry{Index: 7, Term: 2}},
		{index: 8, expected: &raftpb.LogEntry{Index: 8, Term: 2}},
		{index: 9, expected: &raftpb.LogEntry{Index: 9, Term: 2}},
		{index: 10, expected: &raftpb.LogEntry{Index: 10, Term: 3}},
		{index: 11, expected: &raftpb.LogEntry{Index: 11, Term: 3}},
		{index: 12, expected: &raftpb.LogEntry{Index: 12, Term: 4}},
		{index: 13, expected: &raftpb.LogEntry{Index: 13, Term: 4}},
		{index: 14, expected: &raftpb.LogEntry{Index: 14, Term: 4}},
		{index: 15, expected: &raftpb.LogEntry{Index: 15, Term: 5}},
		{index: 16, expected: &raftpb.LogEntry{Index: 16, Term: 5}},
	}

	for _, tc := range testCase {
		entry, err := rn.logAtIndex(tc.index)
		if err != nil {
			t.Errorf("unexpected error for index %d: %v", tc.index, err)
		}
		if entry.Index != tc.expected.Index || entry.Term != tc.expected.Term {
			t.Errorf("expected walogs entry at index %d to be %+v, got %+v", tc.index, tc.expected, entry)
		}
	}

	// Test invalid index
	_, err := rn.logAtIndex(0)
	if err == nil {
		t.Errorf("expected error for index 0, got nil")
	}
	_, err = rn.logAtIndex(17)
	if err == nil {
		t.Errorf("expected error for index 17, got nil")
	}

	// Test Compacted walogs
	rn.walogs = []*raftpb.LogEntry{
		{Index: 5, Term: 1},
		{Index: 6, Term: 1},
		{Index: 7, Term: 2},
		{Index: 8, Term: 2},
		{Index: 9, Term: 2},
		{Index: 10, Term: 3},
		{Index: 11, Term: 3},
		{Index: 12, Term: 4},
		{Index: 13, Term: 4},
		{Index: 14, Term: 4},
		{Index: 15, Term: 5},
		{Index: 16, Term: 5},
	}

	testCase2 := []struct {
		index    uint64
		expected *raftpb.LogEntry
	}{
		{index: 4, expected: nil},
		{index: 5, expected: nil},
		{index: 6, expected: &raftpb.LogEntry{Index: 6, Term: 1}},
		{index: 7, expected: &raftpb.LogEntry{Index: 7, Term: 2}},
		{index: 8, expected: &raftpb.LogEntry{Index: 8, Term: 2}},
		{index: 9, expected: &raftpb.LogEntry{Index: 9, Term: 2}},
		{index: 10, expected: &raftpb.LogEntry{Index: 10, Term: 3}},
		{index: 11, expected: &raftpb.LogEntry{Index: 11, Term: 3}},
		{index: 12, expected: &raftpb.LogEntry{Index: 12, Term: 4}},
		{index: 13, expected: &raftpb.LogEntry{Index: 13, Term: 4}},
		{index: 14, expected: &raftpb.LogEntry{Index: 14, Term: 4}},
		{index: 15, expected: &raftpb.LogEntry{Index: 15, Term: 5}},
		{index: 16, expected: &raftpb.LogEntry{Index: 16, Term: 5}},
		{index: 17, expected: nil},
	}

	for _, tc := range testCase2 {
		entry, err := rn.logAtIndex(tc.index)
		if err != nil {
			if entry != nil || tc.expected != nil {
				t.Errorf("unexpected error for index %d: %v", tc.index, err)
			}
		} else {
			if entry.Index != tc.expected.Index || entry.Term != tc.expected.Term {
				t.Errorf("expected walogs entry at index %d to be %+v, got %+v", tc.index, tc.expected, entry)
			}
		}
	}
}

func TestGetLogTerm(t *testing.T) {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelDebug,
	})

	logger := slog.New(h)
	rn := &raftNode{
		walogs: []*raftpb.LogEntry{
			{Index: 0, Term: 0},
			{Index: 1, Term: 1},
			{Index: 2, Term: 1},
			{Index: 3, Term: 1},
			{Index: 4, Term: 1},
			{Index: 5, Term: 1},
			{Index: 6, Term: 1},
			{Index: 7, Term: 2},
			{Index: 8, Term: 2},
			{Index: 9, Term: 2},
			{Index: 10, Term: 3},
			{Index: 11, Term: 3},
			{Index: 12, Term: 4},
			{Index: 13, Term: 4},
			{Index: 14, Term: 4},
			{Index: 15, Term: 5},
			{Index: 16, Term: 5},
		},
		logger: logger,
	}

	testCase := []struct {
		index    uint64
		expected uint64
	}{
		{index: 0, expected: 0},
		{index: 1, expected: 1},
		{index: 2, expected: 1},
		{index: 3, expected: 1},
		{index: 4, expected: 1},
		{index: 5, expected: 1},
		{index: 6, expected: 1},
		{index: 7, expected: 2},
		{index: 8, expected: 2},
		{index: 9, expected: 2},
		{index: 10, expected: 3},
		{index: 11, expected: 3},
		{index: 12, expected: 4},
		{index: 13, expected: 4},
		{index: 14, expected: 4},
		{index: 15, expected: 5},
		{index: 16, expected: 5},
		{index: 17, expected: 0},
		{index: 18, expected: 0},
	}

	for _, tc := range testCase {
		term, err := rn.getLogTerm(tc.index)
		if err != nil {
			if tc.expected != 0 {
				t.Errorf("unexpected error for index %d: %v", tc.index, err)
			}
		} else {
			if term != tc.expected {
				t.Errorf("expected walogs term at index %d to be %d, got %d", tc.index, tc.expected, term)
			}
		}
	}

	rn.walogs = []*raftpb.LogEntry{
		{Index: 5, Term: 1},
		{Index: 6, Term: 1},
		{Index: 7, Term: 2},
		{Index: 8, Term: 2},
		{Index: 9, Term: 2},
		{Index: 10, Term: 3},
		{Index: 11, Term: 3},
		{Index: 12, Term: 4},
		{Index: 13, Term: 4},
		{Index: 14, Term: 4},
		{Index: 15, Term: 5},
		{Index: 16, Term: 5},
	}

	testCase2 := []struct {
		index    uint64
		expected uint64
	}{
		{index: 4, expected: 0},
		{index: 5, expected: 0},
		{index: 6, expected: 1},
		{index: 7, expected: 2},
		{index: 8, expected: 2},
		{index: 9, expected: 2},
		{index: 10, expected: 3},
		{index: 11, expected: 3},
		{index: 12, expected: 4},
		{index: 13, expected: 4},
		{index: 14, expected: 4},
		{index: 15, expected: 5},
		{index: 16, expected: 5},
		{index: 17, expected: 0},
		{index: 18, expected: 0},
	}

	for _, tc := range testCase2 {
		term, err := rn.getLogTerm(tc.index)
		if err != nil {
			if tc.expected != 0 {
				t.Errorf("unexpected error for index %d: %v", tc.index, err)
			}
		} else {
			if term != tc.expected {
				t.Errorf("expected walogs term at index %d to be %d, got %d", tc.index, tc.expected, term)
			}
		}
	}
}

func TestLastIndexOf(t *testing.T) {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelDebug,
	})

	logger := slog.New(h)
	rn := &raftNode{
		walogs: []*raftpb.LogEntry{
			{Index: 0, Term: 0},
			{Index: 1, Term: 1},
			{Index: 2, Term: 1},
			{Index: 3, Term: 1},
			{Index: 4, Term: 1},
			{Index: 5, Term: 1},
			{Index: 6, Term: 1},
			{Index: 7, Term: 2},
			{Index: 8, Term: 2},
			{Index: 9, Term: 2},
			{Index: 10, Term: 3},
			{Index: 11, Term: 3},
			{Index: 12, Term: 4},
			{Index: 13, Term: 4},
			{Index: 14, Term: 4},
			{Index: 15, Term: 5},
			{Index: 16, Term: 5},
		},
		logger: logger,
	}

	testCase := []struct {
		term     uint64
		expected uint64
	}{
		{term: 0, expected: 0},
		{term: 1, expected: 6},
		{term: 2, expected: 9},
		{term: 3, expected: 11},
		{term: 4, expected: 14},
		{term: 5, expected: 16},
		{term: 6, expected: 16},
	}

	for _, tc := range testCase {
		index, err := rn.lastIndexOf(tc.term)
		if err != nil {
			if tc.expected != index {
				t.Errorf("unexpected error for index %d", tc.term)
			}
			continue
		}
		if index != tc.expected {
			t.Errorf("expected last index of term %d to be %d", tc.term, index)
		}
	}
}

func TestCompactLog(t *testing.T) {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelDebug,
	})

	logger := slog.New(h)
	kv := NewKVStateMachine()
	p := NewFilePersister(TestDir)
	rn := &raftNode{
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
		stateMachine: kv,
		persister:    p,
		logger:       logger,
	}

	err := rn.compactLog()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	for i := 1; i <= 5; i++ {
		_ = kv.ApplyLog(rn.walogs[i].Data)
		rn.lastApplied++
	}

	err = rn.compactLog()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	if len(rn.walogs) != 12 {
		t.Errorf("expected walogs length to be 11, got %d", len(rn.walogs))
	}

	if rn.walogs[0].Index != 5 || rn.walogs[0].Term != 1 {
		t.Errorf("expected walogs[0] to be {Index: 5, Term: 1}, got %+v", rn.walogs[0])
	}
}

func TestTrunctateLog(t *testing.T) {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelDebug,
	})

	logger := slog.New(h)
	rn := &raftNode{
		walogs: []*raftpb.LogEntry{
			{Index: 0, Term: 0},
			{Index: 1, Term: 1},
			{Index: 2, Term: 1},
			{Index: 3, Term: 1},
			{Index: 4, Term: 1},
			{Index: 5, Term: 1},
			{Index: 6, Term: 1},
			{Index: 7, Term: 2},
			{Index: 8, Term: 2},
			{Index: 9, Term: 2},
			{Index: 10, Term: 3},
			{Index: 11, Term: 3},
			{Index: 12, Term: 4},
			{Index: 13, Term: 4},
			{Index: 14, Term: 4},
			{Index: 15, Term: 5},
			{Index: 16, Term: 5},
		},
		logger: logger,
	}

	err := rn.truncateLog(0)
	if err == nil {
		t.Errorf("expected error")
	}

	err = rn.truncateLog(17)
	if err == nil {
		t.Errorf("expected error")
	}

	err = rn.truncateLog(5)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(rn.walogs) != 6 {
		t.Errorf("expected walogs length to be 6, got %d", len(rn.walogs))
	}

	for i := 1; i < len(rn.walogs); i++ {
		t.Log(rn.walogs[i])
	}
}

func TestCheckLogMatch(t *testing.T) {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelDebug,
	})

	logger := slog.New(h)
	rn := &raftNode{
		walogs: []*raftpb.LogEntry{
			{Index: 0, Term: 0},
			{Index: 1, Term: 1},
			{Index: 2, Term: 1},
			{Index: 3, Term: 1},
			{Index: 4, Term: 1},
			{Index: 5, Term: 1},
			{Index: 6, Term: 1},
			{Index: 7, Term: 2},
			{Index: 8, Term: 2},
			{Index: 9, Term: 2},
			{Index: 10, Term: 3},
			{Index: 11, Term: 3},
			{Index: 12, Term: 4},
			{Index: 13, Term: 4},
			{Index: 14, Term: 4},
			{Index: 15, Term: 5},
			{Index: 16, Term: 5},
		},
		logger: logger,
	}

	testCase := []struct {
		index    uint64
		term     uint64
		expected bool
	}{
		{index: 0, term: 0, expected: true},
		{index: 6, term: 1, expected: true},
		{index: 7, term: 2, expected: true},
		{index: 8, term: 3, expected: false},
		{index: 16, term: 5, expected: true},
		{index: 17, term: 0, expected: false},
	}

	for _, tc := range testCase {
		match, entry, err := rn.checkLogMatch(tc.index, tc.term)
		if match != tc.expected {
			t.Errorf("expected log match for index %d and term %d to be %v, got %v", tc.index, tc.term, tc.expected, match)
		}
		if errors.Is(err, ErrLogInvalidIndex) {
			if entry != nil {
				t.Errorf("expected entry to not be nil, got %+v", entry)
			}
		} else if errors.Is(err, ErrLogEntryConflict) {
			if entry.Index != 8 || entry.Term == 3 {
				t.Errorf("expected entry to be {Index: 8, Term: 3}, got %+v", entry)
			}
		} else if errors.Is(err, ErrLogNeedTruncate) {
			if !(entry.Index == 6 || entry.Index == 7) {
				t.Errorf("expected entry to be {Index: 6, Term: 1} or {Index: 7, Term: 2}, got %+v", entry)
			}
		} else if errors.Is(err, ErrLogIndexOutOfRange) {
			if entry != nil {
				t.Errorf("expected entry to be {Index: 17, Term: 0}, got %+v", entry)
			}
		} else if err != nil {
			if entry.Index != 16 || entry.Term != 5 {
				t.Errorf("expected entry to be {Index: 16, Term: 5}, got %+v", entry)
			}
		}
	}

	rn.walogs = []*raftpb.LogEntry{
		{Index: 5, Term: 1},
		{Index: 6, Term: 1},
		{Index: 7, Term: 2},
		{Index: 8, Term: 2},
		{Index: 9, Term: 2},
		{Index: 10, Term: 3},
		{Index: 11, Term: 3},
		{Index: 12, Term: 4},
		{Index: 13, Term: 4},
		{Index: 14, Term: 4},
		{Index: 15, Term: 5},
		{Index: 16, Term: 5},
	}

	testCase2 := []struct {
		index    uint64
		term     uint64
		expected bool
	}{
		{index: 4, term: 1, expected: false},
		{index: 5, term: 1, expected: false},
		{index: 6, term: 1, expected: true},
	}

	for _, tc := range testCase2 {
		match, entry, err := rn.checkLogMatch(tc.index, tc.term)
		if match != tc.expected {
			t.Errorf("expected log match for index %d and term %d to be %v, got %v", tc.index, tc.term, tc.expected, match)
		}
		if errors.Is(err, ErrLogEntryCompacted) {
			if entry != nil {
				t.Errorf("expected entry to be nil, got %+v", entry)
			}
		} else if errors.Is(err, ErrLogNeedTruncate) {
			if entry.Index != 6 {
				t.Errorf("expected entry to be {Index: 6, Term: 1}, got %+v", entry)
			}
		}
	}
}

func TestFirstDiffTermEntry(t *testing.T) {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelDebug,
	})

	logger := slog.New(h)
	rn := &raftNode{
		walogs: []*raftpb.LogEntry{
			{Index: 0, Term: 0},
			{Index: 1, Term: 1},
			{Index: 2, Term: 1},
			{Index: 3, Term: 1},
			{Index: 4, Term: 1},
			{Index: 5, Term: 1},
			{Index: 6, Term: 1},
			{Index: 7, Term: 2},
			{Index: 8, Term: 2},
			{Index: 9, Term: 2},
			{Index: 10, Term: 3},
			{Index: 11, Term: 3},
			{Index: 12, Term: 4},
			{Index: 13, Term: 4},
			{Index: 14, Term: 4},
			{Index: 15, Term: 5},
			{Index: 16, Term: 5},
		},
		logger: logger,
	}

	testCase := []struct {
		index    uint64
		term     uint64
		expected *raftpb.LogEntry
	}{
		{index: 0, term: 0, expected: nil},
		{index: 6, term: 1, expected: nil},
		{index: 7, term: 2, expected: &raftpb.LogEntry{
			Index: 6, Term: 1,
		}},
		{index: 16, term: 5, expected: &raftpb.LogEntry{
			Index: 14, Term: 4,
		}},
		{index: 17, term: 0, expected: nil},
	}

	for _, tc := range testCase {
		ent, err := rn.firstDiffTermEntry(tc.index, tc.term)
		if err != nil {
			if !(tc.index == 0 || tc.index == 17 || tc.index == 6) {
				t.Error("expect tc.index is 0, 6 or 17")
			}
			continue
		}
		if ent.Index != tc.expected.Index || ent.Term != tc.expected.Term {
			t.Errorf("expect get {index: %v, term: %v}, but got {index: %v, term: %v}", tc.index, tc.term, ent.Index, ent.Term)
		}
	}

	rn.walogs = []*raftpb.LogEntry{
		{Index: 5, Term: 1},
		{Index: 6, Term: 1},
		{Index: 7, Term: 2},
		{Index: 8, Term: 2},
		{Index: 9, Term: 2},
		{Index: 10, Term: 3},
		{Index: 11, Term: 3},
		{Index: 12, Term: 4},
		{Index: 13, Term: 4},
		{Index: 14, Term: 4},
		{Index: 15, Term: 5},
		{Index: 16, Term: 5},
	}

	testCase2 := []struct {
		index    uint64
		term     uint64
		expected *raftpb.LogEntry
	}{
		{index: 4, term: 1, expected: nil},
		{index: 5, term: 1, expected: nil},
		{index: 7, term: 2, expected: &raftpb.LogEntry{
			Index: 6, Term: 1,
		}},
		{index: 16, term: 5, expected: &raftpb.LogEntry{
			Index: 14, Term: 4,
		}},
		{index: 17, term: 0, expected: nil},
	}

	for _, tc := range testCase2 {
		ent, err := rn.firstDiffTermEntry(tc.index, tc.term)
		if err != nil {
			if !(tc.index == 4 || tc.index == 5 || tc.index == 17) {
				t.Error("expect tc.index is 4, 5 or 17")
			}
			continue
		}
		if ent.Index != tc.expected.Index || ent.Term != tc.expected.Term {
			t.Errorf("expect get {index: %v, term: %v}, but got {index: %v, term: %v}", tc.index, tc.term, ent.Index, ent.Term)
		}
	}
}

func TestCutoffLogsByIndex(t *testing.T) {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelDebug,
	})

	logger := slog.New(h)
	rn := &raftNode{
		walogs: []*raftpb.LogEntry{
			{Index: 0, Term: 0},
			{Index: 1, Term: 1},
			{Index: 2, Term: 1},
			{Index: 3, Term: 1},
			{Index: 4, Term: 1},
			{Index: 5, Term: 1},
			{Index: 6, Term: 1},
			{Index: 7, Term: 2},
			{Index: 8, Term: 2},
			{Index: 9, Term: 2},
			{Index: 10, Term: 3},
			{Index: 11, Term: 3},
			{Index: 12, Term: 4},
			{Index: 13, Term: 4},
			{Index: 14, Term: 4},
			{Index: 15, Term: 5},
			{Index: 16, Term: 5},
		},
		logger: logger,
	}

	rn.cutoffLogsByIndex(0)
	if len(rn.walogs) != 17 {
		t.Errorf("expected walogs length to be 14, got %d", len(rn.walogs))
	}

	rn.cutoffLogsByIndex(3)
	if len(rn.walogs) != 14 {
		t.Errorf("expected walogs length to be 14, got %d", len(rn.walogs))
	}
	if rn.walogs[0].Index != 3 || rn.walogs[0].Term != 1 {
		t.Errorf("expected walogs[0] to be {Index: 3, Term: 1}, got %+v", rn.walogs[0])
	}

	rn.cutoffLogsByIndex(12)
	if len(rn.walogs) != 5 {
		t.Errorf("expected walogs length to be 5, got %d", len(rn.walogs))
	}
	if rn.walogs[0].Index != 12 || rn.walogs[0].Term != 4 {
		t.Errorf("expected walogs[0] to be {Index: 12, Term: 4}, got %+v", rn.walogs[0])
	}

	rn.cutoffLogsByIndex(5)
	if len(rn.walogs) != 5 {
		t.Errorf("expected walogs length to be 5, got %d", len(rn.walogs))
	}
	if rn.walogs[0].Index != 12 || rn.walogs[0].Term != 4 {
		t.Errorf("expected walogs[0] to be {Index: 12, Term: 4}, got %+v", rn.walogs[0])
	}

	rn.cutoffLogsByIndex(17)
	if len(rn.walogs) != 1 {
		t.Errorf("expected walogs length to be 1, got %d", len(rn.walogs))
	}
	if rn.walogs[0].Index != 16 || rn.walogs[0].Term != 5 {
		t.Errorf("expected walogs[0] to be {Index: 17, Term: 0}, got %+v", rn.walogs[0])
	}

	rn.cutoffLogsByIndex(19)
	if len(rn.walogs) != 1 {
		t.Errorf("expected walogs length to be 1, got %d", len(rn.walogs))
	}
	if rn.walogs[0].Index != 16 || rn.walogs[0].Term != 5 {
		t.Errorf("expected walogs[0] to be {Index: 17, Term: 0}, got %+v", rn.walogs[0])
	}
}

func TestExtractLogEntries(t *testing.T) {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelDebug,
	})

	logger := slog.New(h)
	rn := &raftNode{
		walogs: []*raftpb.LogEntry{
			{Index: 0, Term: 0},
			{Index: 1, Term: 1},
			{Index: 2, Term: 1},
			{Index: 3, Term: 1},
			{Index: 4, Term: 1},
			{Index: 5, Term: 1},
			{Index: 6, Term: 1},
			{Index: 7, Term: 2},
			{Index: 8, Term: 2},
			{Index: 9, Term: 2},
			{Index: 10, Term: 3},
			{Index: 11, Term: 3},
			{Index: 12, Term: 4},
			{Index: 13, Term: 4},
			{Index: 14, Term: 4},
			{Index: 15, Term: 5},
			{Index: 16, Term: 5},
		},
		logger: logger,
	}

	var start, end uint64 = 0, 16
	entries, err := rn.extractLogEntries(start, end)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(entries) != 17 {
		t.Errorf("expected entries length to be 17, got %d", len(entries))
	}

	start, end = 1, 5
	entries, err = rn.extractLogEntries(start, end)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if len(entries) != 5 {
		t.Errorf("expected entries length to be 5, got %d", len(entries))
	}

	for i := 0; i < len(entries); i++ {
		if entries[i].Index != start+uint64(i) {
			t.Errorf("expected entry index to be %d, got %d", start+uint64(i), entries[i].Index)
		}
	}

	start, end = 5, 17
	entries, err = rn.extractLogEntries(start, end)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(entries) != 12 {
		t.Errorf("expected entries length to be 12, got %d", len(entries))
	}
	for i := 0; i < len(entries); i++ {
		if entries[i].Index != start+uint64(i) {
			t.Errorf("expected entry index to be %d, got %d", start+uint64(i), entries[i].Index)
		}
	}

	start, end = 17, 19
	entries, err = rn.extractLogEntries(start, end)
	if err == nil {
		t.Errorf("expected error for start %d and end %d, got nil", start, end)
	}
	if len(entries) != 0 {
		t.Errorf("expected entries length to be 0, got %d", len(entries))
	}

	rn.walogs = []*raftpb.LogEntry{
		{Index: 5, Term: 1},
		{Index: 6, Term: 1},
		{Index: 7, Term: 2},
		{Index: 8, Term: 2},
		{Index: 9, Term: 2},
		{Index: 10, Term: 3},
		{Index: 11, Term: 3},
		{Index: 12, Term: 4},
	}

	start, end = 5, 12
	entries, err = rn.extractLogEntries(start, end)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(entries) != 8 {
		t.Errorf("expected entries length to be 8, got %d", len(entries))
	}

	for i := 0; i < len(entries); i++ {
		if entries[i].Index != start+uint64(i) {
			t.Errorf("expected entry index to be %d, got %d", start+uint64(i), entries[i].Index)
		}
	}

	start, end = 7, 8
	entries, err = rn.extractLogEntries(start, end)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(entries) != 2 {
		t.Errorf("expected entries length to be 2, got %d", len(entries))
	}

}
