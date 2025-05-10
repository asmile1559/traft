package traft

import "errors"

var (
	ErrLogAlreadySnapshot = errors.New("log already snapshot")
	ErrLogOutOfRange      = errors.New("log out of range")
	ErrLogConflict        = errors.New("log conflict")

	ErrNeedTruncate = errors.New("need truncate")
	ErrInvalidIndex = errors.New("invalid index")

	ErrPeerIsNil = errors.New("peer is nil")

	ErrNoMetadataPersisted = errors.New("no metadata persisted")
	ErrNoLogPersisted      = errors.New("no log persisted")
	ErrNoSnapshotPersisted = errors.New("no snapshot persisted")
)
