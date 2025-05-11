package traft

import (
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	LoggerLevel = slog.LevelDebug
)

func SetLogger() {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     LoggerLevel,
	})

	logger := slog.New(handler)
	slog.SetDefault(logger)
}

func RandomElectionTimeout() time.Duration {
	// 随机选举超时时间，范围在 [150ms, 300ms]
	return time.Duration(MinElectionTimeout+rand.Intn(MaxElectionTimeout-MinElectionTimeout)) * time.Millisecond
}

func GetLatestFile(dir string, t PFileType) (string, error) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return "", ErrPersisterDirNotExist
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}

	patten := ""
	switch t {
	case PMetadataFile:
		patten = ".metadata"
		err = ErrNoMetadataPersisted
	case PLogFile:
		patten = ".log"
		err = ErrNoLogPersisted
	case PSnapshotFile:
		patten = ".snapshot"
		err = ErrNoSnapshotPersisted
	}

	target := ""
	latest := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if strings.HasSuffix(entry.Name(), patten) {
			info, err := entry.Info()
			if err != nil {
				continue
			}
			if info.ModTime().After(latest) {
				latest = info.ModTime()
				target = entry.Name()
			}
		}
	}
	if target == "" {
		return "", err
	}
	return filepath.Join(dir, target), nil
}
