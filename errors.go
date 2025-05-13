package traft

import "errors"

var (
	// Common errors:

	// ErrTermDenied when a requested term is less than the existing term
	ErrTermDenied = errors.New("term denied")

	// WALogs errors:

	// ErrLogEntryCompacted when the given index is less than the dummy index
	ErrLogEntryCompacted = errors.New("log entry already compacted")
	// ErrLogIndexOutOfRange when the given index greater than the last log index
	ErrLogIndexOutOfRange = errors.New("log index out of range")
	// ErrLogEntryConflict when the given <term, index> pair does not match the existing log entry
	ErrLogEntryConflict = errors.New("log entry conflict")
	// ErrLogNeedTruncate when the matching log entry is not the last log entry
	ErrLogNeedTruncate = errors.New("log need truncate")
	// ErrLogInvalidIndex when the given index is invalid, usually is the dummy index
	ErrLogInvalidIndex = errors.New("log invalid index")
	// ErrLogInvalidTerm when the given term is invalid, usually is the dummy term
	ErrLogInvalidTerm = errors.New("log invalid term")
	// ErrLogEntryNotFound when the given index is not found in the walogs
	ErrLogEntryNotFound = errors.New("log entry not found")

	ErrPeerIsNotFound = errors.New("peer is nil")

	// Persister errors:

	// ErrDirectoryNotExist when the given directory does not exist and cannot be created
	ErrDirectoryNotExist = errors.New("directory not exist")
	// ErrNoMetadataPersisted when the metadata file does not exist
	ErrNoMetadataPersisted = errors.New("no metadata persisted")
	// ErrNoLogPersisted when the log file does not exist
	ErrNoLogPersisted = errors.New("no log persisted")
	// ErrNoSnapshotPersisted when the snapshot file does not exist
	ErrNoSnapshotPersisted = errors.New("no snapshot persisted")

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
