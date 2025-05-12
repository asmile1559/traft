package traft

import "errors"

var (
	ErrLogAlreadySnapshot = errors.New("walogs already snapshot")
	ErrLogOutOfRange      = errors.New("walogs out of range")
	ErrLogConflict        = errors.New("walogs conflict")

	ErrNeedTruncate  = errors.New("need truncate")
	ErrInvalidIndex  = errors.New("invalid index")
	ErrInvalidTerm   = errors.New("invalid term")
	ErrTruncatedTerm = errors.New("truncated term")
	ErrLogNotFound   = errors.New("walogs not found")
	
	ErrPeerIsNil = errors.New("peer is nil")

	ErrPersisterDirNotExist = errors.New("persister dir not exist")
	ErrNoMetadataPersisted  = errors.New("no metadata persisted")
	ErrNoLogPersisted       = errors.New("no walogs persisted")
	ErrNoSnapshotPersisted  = errors.New("no snapshot persisted")

	ErrUnknownKVStateMachineOperation = errors.New("unknown kv state machine operation")
)
