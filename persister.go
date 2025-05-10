package traft

import (
	"encoding/gob"
	raftpb "github.com/asmile1559/traft/internal/apis/raft"
	"os"
	"path/filepath"
	"time"
)

type PFileType string

const (
	PMetadataFile PFileType = "metadata"
	PLogFile      PFileType = "log"
	PSnapshotFile PFileType = "snapshot"
)

type Persister interface {
	SaveMetadata(term uint64, votedFor string) error
	LoadMetadata() (term uint64, votedFor string, err error)

	SaveLogEntries(entries []*raftpb.LogEntry) error
	LoadLogEntries() ([]*raftpb.LogEntry, error)

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

func (p *FilePersister) checkBeforeLoad(t PFileType) (string, error) {
	switch t {
	case PMetadataFile:
		if p.metadataFile == "" && p.dir == "" {
			return "", ErrNoMetadataPersisted
		}
	case PLogFile:
		if p.logFile == "" && p.dir == "" {
			return "", ErrNoLogPersisted
		}
	case PSnapshotFile:
		if p.snapshotFile == "" && p.dir == "" {
			return "", ErrNoSnapshotPersisted
		}
	}
	file, err := GetLatestFile(p.dir, t)
	if err != nil {
		return "", err
	}
	switch t {
	case PMetadataFile:
		p.metadataFile = file
	case PLogFile:
		p.logFile = file
	case PSnapshotFile:
		p.snapshotFile = file
	}
	return file, nil
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

func (p *FilePersister) LoadMetadata() (uint64, string, error) {
	_, err := p.checkBeforeLoad(PMetadataFile)
	if err != nil {
		return 0, "", err
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

func (p *FilePersister) SaveLogEntries(entries []*raftpb.LogEntry) error {
	fName := time.Now().Format("060102150405") + ".log"
	f, err := os.Create(filepath.Join(p.dir, fName))
	if err != nil {
		return err
	}
	defer func(f *os.File) {
		_ = f.Close()
	}(f)

	enc := gob.NewEncoder(f)
	err = enc.Encode(entries)

	if err != nil {
		return err
	}

	p.logFile = filepath.Join(p.dir, fName)
	return nil
}

func (p *FilePersister) LoadLogEntries() ([]*raftpb.LogEntry, error) {
	_, err := p.checkBeforeLoad(PLogFile)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(p.logFile)
	if err != nil {
		return nil, err
	}
	defer func(f *os.File) {
		_ = f.Close()
	}(f)

	dec := gob.NewDecoder(f)
	var entries []*raftpb.LogEntry
	err = dec.Decode(&entries)
	return entries, err
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
	_, err := p.checkBeforeLoad(PSnapshotFile)
	if err != nil {
		return nil, err
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
