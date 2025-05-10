package traft

import (
	"encoding/gob"
	raftpb "github.com/asmile1559/traft/internal/apis/raft"
	"os"
	"path/filepath"
	"time"
)

type Persister interface {
	SaveMetadata(term uint64, votedFor string) error
	LoadMetadata() (term uint64, votedFor string, err error)

	SaveLogEntries(entries []*raftpb.LogEntry, firstIndex uint64) error
	LoadLogEntries() ([]*raftpb.LogEntry, uint64, error)

	SaveSnapshot(snapshot *raftpb.Snapshot) error
	LoadSnapshot() (*raftpb.Snapshot, error)
}

type FilePersister struct {
	dir          string
	metadataFile string
	logFile      string
	snapshotFile string
}

func NewFilePersister(dir string) *FilePersister {
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		//TODO: handle error
		panic(err)
	}
	return &FilePersister{dir: dir}
}

func (p *FilePersister) SaveMetadata(term uint64, votedFor string) error {
	fName := time.Now().Format("060102150405") + ".metadata"
	f, err := os.Create(filepath.Join(p.dir, fName))
	if err != nil {
		return err
	}
	defer func(f *os.File) {
		_ = f.Close()
	}(f)

	enc := gob.NewEncoder(f)
	err = enc.Encode(struct {
		Term     uint64
		VotedFor string
	}{Term: term, VotedFor: votedFor})

	if err != nil {
		return err
	}

	p.metadataFile = filepath.Join(p.dir, fName)
	return nil
}

func (p *FilePersister) LoadMetadata() (term uint64, votedFor string, err error) {
	if p.metadataFile == "" {
		return 0, "", ErrNoMetadataPersisted
	}
	f, err := os.Open(p.metadataFile)
	if err != nil {
		return 0, "", err
	}
	defer func(f *os.File) {
		_ = f.Close()
	}(f)

	dec := gob.NewDecoder(f)
	var metadata struct {
		Term     uint64
		VotedFor string
	}
	err = dec.Decode(&metadata)
	return metadata.Term, metadata.VotedFor, err
}

func (p *FilePersister) SaveLogEntries(entries []*raftpb.LogEntry, firstIndex uint64) error {
	fName := time.Now().Format("060102150405") + ".log"
	f, err := os.Create(filepath.Join(p.dir, fName))
	if err != nil {
		return err
	}
	defer func(f *os.File) {
		_ = f.Close()
	}(f)

	enc := gob.NewEncoder(f)
	err = enc.Encode(struct {
		FirstIndex uint64
		Entries    []*raftpb.LogEntry
	}{FirstIndex: firstIndex, Entries: entries})

	if err != nil {
		return err
	}

	p.logFile = filepath.Join(p.dir, fName)
	return nil
}

func (p *FilePersister) LoadLogEntries() ([]*raftpb.LogEntry, uint64, error) {
	if p.logFile == "" {
		return nil, 0, ErrNoLogPersisted
	}
	f, err := os.Open(p.logFile)
	if err != nil {
		return nil, 0, err
	}
	defer func(f *os.File) {
		_ = f.Close()
	}(f)

	dec := gob.NewDecoder(f)
	var logData struct {
		FirstIndex uint64
		Entries    []*raftpb.LogEntry
	}
	err = dec.Decode(&logData)
	return logData.Entries, logData.FirstIndex, err
}

func (p *FilePersister) SaveSnapshot(snapshot *raftpb.Snapshot) error {
	fName := time.Now().Format("060102150405") + ".snapshot"
	f, err := os.Create(filepath.Join(p.dir, fName))
	if err != nil {
		return err
	}
	defer func(f *os.File) {
		_ = f.Close()
	}(f)

	enc := gob.NewEncoder(f)
	err = enc.Encode(snapshot)
	if err != nil {
		return err
	}

	p.snapshotFile = filepath.Join(p.dir, fName)
	return nil
}

func (p *FilePersister) LoadSnapshot() (*raftpb.Snapshot, error) {
	if p.snapshotFile == "" {
		return nil, ErrNoSnapshotPersisted
	}
	f, err := os.Open(p.snapshotFile)
	if err != nil {
		return nil, err
	}
	defer func(f *os.File) {
		_ = f.Close()
	}(f)

	dec := gob.NewDecoder(f)
	var snapshot *raftpb.Snapshot
	err = dec.Decode(&snapshot)
	return snapshot, err
}
