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
	LoggerLevel       = slog.LevelDebug
	HeartbeatInterval = 100 * time.Millisecond // 心跳间隔

	// MinElectionTimeout 最小选举超时范围, 单位为毫秒
	MinElectionTimeout = 150
	// MaxElectionTimeout 最大选举超时范围, 单位为毫秒
	MaxElectionTimeout = 300

	Debug = false
)

func FormatLogger(module string, args ...slog.Attr) *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     LoggerLevel,
	})

	attrs := make([]any, 0, len(args)+1)
	attrs = append(attrs, slog.String("module", module))
	for _, arg := range args {
		attrs = append(attrs, arg)
	}
	logger := slog.New(handler).With(
		slog.Group("property", attrs...),
	)
	return logger
}

func RandomElectionTimeout() time.Duration {
	// 随机选举超时时间，范围在 [150ms, 300ms]
	if Debug {
		return time.Hour
	}
	return time.Duration(MinElectionTimeout+rand.Intn(MaxElectionTimeout-MinElectionTimeout)) * time.Millisecond
}

func GetHeartbeatDuration() time.Duration {
	// 心跳间隔时间，范围在 [50ms, 100ms]
	if Debug {
		return time.Hour
	}
	return HeartbeatInterval
}

func GetLatestFile(dir string, t PFileType) (string, error) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return "", ErrDirectoryNotExist
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
		patten = ".walogs"
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
