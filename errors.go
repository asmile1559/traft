package traft

import "errors"

var (
	ErrLogAlreadySnapshot = errors.New("log already snapshot")
	ErrLogOutOfRange      = errors.New("log out of range")
	ErrLogConflict        = errors.New("log conflict")

	ErrNeedTruncate = errors.New("need truncate")
	ErrInvalidIndex = errors.New("invalid index")
	ErrInvalidTerm  = errors.New("invalid term")

	ErrPeerIsNil = errors.New("peer is nil")

	ErrPersisterDirNotExist = errors.New("persister dir not exist")
	ErrNoMetadataPersisted  = errors.New("no metadata persisted")
	ErrNoLogPersisted       = errors.New("no log persisted")
	ErrNoSnapshotPersisted  = errors.New("no snapshot persisted")

	ErrUnknownKVStateMachineOperation = errors.New("unknown kv state machine operation")
)
