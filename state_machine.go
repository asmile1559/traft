package traft

type StateMachine interface {
	// arg: logData, json-type
	ApplyLog(logData []byte) error
	// arg: snapshotData, json-type
	ApplySnapshot(snapshotData []byte) error

	// return: snapshotData, json-type
	TakeSnapshot() ([]byte, error)
}
