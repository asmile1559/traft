package traft

import (
	"encoding/gob"
	raftpb "github.com/asmile1559/traft/internal/apis/raft"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

type PFileType string

const (
	PMetadataFile PFileType = "metadata"
	PLogFile      PFileType = "walogs"
	PSnapshotFile PFileType = "snapshot"
)

type Persister interface {
	SaveMetadata(term uint64, votedFor string) error
	LoadMetadata() (term uint64, votedFor string, err error)

	SaveLogEntries(entries []*raftpb.LogEntry) error
	LoadLogEntries() ([]*raftpb.LogEntry, error)

	SaveSnapshot(snapshot *raftpb.Snapshot) error
	LoadSnapshot() (*raftpb.Snapshot, error)

	Close() error
}

type FilePersister struct {
	dir          string
	metadataFile string
	logFile      string
	snapshotFile string

	logger *slog.Logger
}

func NewFilePersister(dir string) *FilePersister {
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		panic(err)
	}
	return &FilePersister{
		dir:    dir,
		logger: FormatLogger("persister"),
	}
}

func (p *FilePersister) checkBeforeLoad(t PFileType) (string, error) {
	p.logger.Debug("checkBeforeLoad entry", "type", t)
	defer p.logger.Debug("checkBeforeLoad exit", "type", t)
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
	p.logger.Debug("SaveMetadata entry", "type", term, "votedFor", votedFor)
	defer p.logger.Debug("SaveMetadata exit", "type", term, "votedFor", votedFor)
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
	p.logger.Debug("LoadMetadata entry")
	defer p.logger.Debug("LoadMetadata exit")
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
	p.logger.Debug("SaveLogEntries entry")
	defer p.logger.Debug("SaveLogEntries exit")
	fName := time.Now().Format("060102150405") + ".walogs"
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
	p.logger.Debug("LoadLogEntries entry")
	defer p.logger.Debug("LoadLogEntries exit")
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
	p.logger.Debug("SaveSnapshot entry")
	defer p.logger.Debug("SaveSnapshot exit")
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
	p.logger.Debug("LoadSnapshot entry")
	defer p.logger.Debug("LoadSnapshot exit")
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

func (p *FilePersister) Close() error {
	return nil
}
