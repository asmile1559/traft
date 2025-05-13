package traft

import (
	"errors"
	raftpb "github.com/asmile1559/traft/internal/apis/raft"
	"os"
	"testing"
)

func TestFilePersister_checkBeforeLoad(t *testing.T) {
	p := NewFilePersister(TestDir)
	file, err := p.checkBeforeLoad(PLogFile)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("latest file: %s", file)
	file, err = p.checkBeforeLoad(PMetadataFile)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("latest file: %s", file)
	file, err = p.checkBeforeLoad(PSnapshotFile)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("latest file: %s", file)

	p.dir = "./test/empty"
	if err := os.MkdirAll(p.dir, 0755); err != nil {
		t.Fatal(err)
	}

	_, err = p.checkBeforeLoad(PLogFile)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ErrNoLogPersisted) {
		t.Fatal("expected ErrNoLogPersisted, got", err)
	}
	_, err = p.checkBeforeLoad(PMetadataFile)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ErrNoMetadataPersisted) {
		t.Fatal("expected ErrNoMetadataPersisted, got", err)
	}
	_, err = p.checkBeforeLoad(PSnapshotFile)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ErrNoSnapshotPersisted) {
		t.Fatal("expected ErrNoSnapshotPersisted, got", err)
	}

	p.dir = ""
	_, err = p.checkBeforeLoad(PLogFile)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ErrDirectoryNotExist) {
		t.Fatal("expected ErrDirectoryNotExist, got", err)
	}
	_, err = p.checkBeforeLoad(PMetadataFile)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ErrDirectoryNotExist) {
		t.Fatal("expected ErrDirectoryNotExist, got", err)
	}
	_, err = p.checkBeforeLoad(PSnapshotFile)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ErrDirectoryNotExist) {
		t.Fatal("expected ErrDirectoryNotExist, got", err)
	}
}

func TestFilePersister_SaveMetadata(t *testing.T) {
	p := NewFilePersister(TestDir)
	if err := p.SaveMetadata(1, "test"); err != nil {
		t.Fatal(err)
	}
	if p.metadataFile == "" {
		t.Fatal("metadata file not set")
	}
	if _, err := os.Stat(p.metadataFile); os.IsNotExist(err) {
		t.Fatal("metadata file not exist")
	}
	file, err := os.OpenFile(p.metadataFile, os.O_RDONLY, 0666)
	if err != nil {
		return
	}
	defer file.Close()
	b := make([]byte, 1024)
	_, err = file.Read(b)
	if err != nil {
		t.Fatal(err)
		return
	}
	t.Log(string(b))
}

func TestFilePersister_LoadMetadata(t *testing.T) {
	p := NewFilePersister(TestDir)
	_ = p.SaveMetadata(1, "test")
	term, votedFor, err := p.LoadMetadata()
	if err != nil {
		t.Fatal(err)
	}
	if term != 1 {
		t.Fatalf("expected term 1, got %d", term)
	}
	if votedFor != "test" {
		t.Fatalf("expected votedFor test, got %s", votedFor)
	}
}

func TestFilePersister_SaveLogEntries(t *testing.T) {
	p := NewFilePersister(TestDir)
	entries := []*raftpb.LogEntry{
		{Term: 1, Index: 1, Data: []byte("test")},
		{Term: 2, Index: 2, Data: []byte("test2")},
	}
	if err := p.SaveLogEntries(entries); err != nil {
		t.Fatal(err)
	}
	if p.logFile == "" {
		t.Fatal("walogs file not set")
	}
	if _, err := os.Stat(p.logFile); os.IsNotExist(err) {
		t.Fatal("walogs file not exist")
	}
	file, err := os.OpenFile(p.logFile, os.O_RDONLY, 0666)
	if err != nil {
		return
	}
	defer file.Close()
	b := make([]byte, 1024)
	_, err = file.Read(b)
	if err != nil {
		t.Fatal(err)
		return
	}
	t.Log(string(b))
}

func TestFilePersister_LoadLogEntries(t *testing.T) {
	p := NewFilePersister(TestDir)
	entries := []*raftpb.LogEntry{
		{Term: 1, Index: 1, Data: []byte("test")},
		{Term: 2, Index: 2, Data: []byte("test2")},
	}
	if err := p.SaveLogEntries(entries); err != nil {
		t.Fatal(err)
	}
	logEntries, err := p.LoadLogEntries()
	if err != nil {
		t.Fatal(err)
	}
	if len(logEntries) != len(entries) {
		t.Fatalf("expected %d entries, got %d", len(entries), len(logEntries))
	}
	for i, entry := range logEntries {
		if entry.Term != entries[i].Term {
			t.Fatalf("expected term %d, got %d", entries[i].Term, entry.Term)
		}
		if entry.Index != entries[i].Index {
			t.Fatalf("expected index %d, got %d", entries[i].Index, entry.Index)
		}
		if string(entry.Data) != string(entries[i].Data) {
			t.Fatalf("expected data %s, got %s", string(entries[i].Data), string(entry.Data))
		}
	}
}

func TestFilePersister_SaveSnapshot(t *testing.T) {
	p := NewFilePersister(TestDir)
	snapshot := &raftpb.Snapshot{
		LastIncludedIndex: 1,
		LastIncludedTerm:  2,
		Data:              []byte("test"),
	}
	if err := p.SaveSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	if p.snapshotFile == "" {
		t.Fatal("snapshot file not set")
	}
	if _, err := os.Stat(p.snapshotFile); os.IsNotExist(err) {
		t.Fatal("snapshot file not exist")
	}
	file, err := os.OpenFile(p.snapshotFile, os.O_RDONLY, 0666)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	b := make([]byte, 1024)
	_, err = file.Read(b)
	if err != nil {
		t.Fatal(err)
		return
	}
}

func TestFilePersister_LoadSnapshot(t *testing.T) {
	p := NewFilePersister(TestDir)
	snapshot := &raftpb.Snapshot{
		LastIncludedIndex: 1,
		LastIncludedTerm:  2,
		Data:              []byte("test"),
	}
	if err := p.SaveSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	snap, err := p.LoadSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if snap.LastIncludedIndex != snapshot.LastIncludedIndex {
		t.Fatalf("expected LastIncludedIndex %d, got %d", snapshot.LastIncludedIndex, snap.LastIncludedIndex)
	}
	if snap.LastIncludedTerm != snapshot.LastIncludedTerm {
		t.Fatalf("expected LastIncludedTerm %d, got %d", snapshot.LastIncludedTerm, snap.LastIncludedTerm)
	}
	if string(snap.Data) != string(snapshot.Data) {
		t.Fatalf("expected data %s, got %s", string(snapshot.Data), string(snap.Data))
	}
}
