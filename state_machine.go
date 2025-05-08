package traft

type StateMachine interface {
	// ApplyLog
	//	arg: logData, json-type
	ApplyLog(logData []byte) error
	// ApplySnapshot
	//	arg: snapshotData, json-type
	ApplySnapshot(snapshotData []byte) error

	// TakeSnapshot
	//	return: snapshotData, json-type
	TakeSnapshot() []byte
}
