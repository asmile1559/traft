package traft

import "errors"

var (
	// Common errors:

	// ErrTermDenied when a requested term is less than the existing term
	ErrTermDenied = errors.New("term denied")

	ErrLogAlreadySnapshot = errors.New("walogs already snapshot")
	ErrLogOutOfRange      = errors.New("walogs out of range")
	ErrLogConflict        = errors.New("walogs conflict")

	ErrNeedTruncate  = errors.New("need truncate")
	ErrInvalidIndex  = errors.New("invalid index")
	ErrInvalidTerm   = errors.New("invalid term")
	ErrTruncatedTerm = errors.New("truncated term")
	ErrLogNotFound   = errors.New("walogs not found")

	ErrPeerIsNotFound = errors.New("peer is nil")

	ErrPersisterDirNotExist = errors.New("persister dir not exist")
	ErrNoMetadataPersisted  = errors.New("no metadata persisted")
	ErrNoLogPersisted       = errors.New("no walogs persisted")
	ErrNoSnapshotPersisted  = errors.New("no snapshot persisted")

	ErrUnknownKVStateMachineOperation = errors.New("unknown kv state machine operation")

	// Install Snapshot RPC errors:

	// ErrSnapshotOutOfDate when a requested snapshot's last included index is less than the existing snapshot's
	ErrSnapshotOutOfDate = errors.New("snapshot out of date")

	// StateMachine errors:

	// ErrStateMachineApplyLogFailed when apply log failed
	ErrStateMachineApplyLogFailed = errors.New("state machine apply log failed")
	// ErrStateMachineApplySnapshotFailed when apply snapshot failed
	ErrStateMachineApplySnapshotFailed = errors.New("state machine apply snapshot failed")
	// ErrStateMachineTakeSnapshotFailed when take snapshot failed
	ErrStateMachineTakeSnapshotFailed = errors.New("state machine take snapshot failed")
)
