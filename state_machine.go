package traft

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
)

var (
	ErrKVStateMachineInvalidOperation = errors.New("invalid operation")
)

type StateMachine interface {
	// ApplyLog
	//	arg: logData, json-type
	ApplyLog(logData []byte) error
	// ApplySnapshot
	//	arg: snapshotData, json-type
	ApplySnapshot(snapshotData []byte) error

	// TakeSnapshot
	//	return: snapshotData, json-type
	TakeSnapshot() ([]byte, error)

	Close() error
}

type KVStateMachine struct {
	// kv store
	store  map[string]string
	logger *slog.Logger
}

func NewKVStateMachine() *KVStateMachine {
	return &KVStateMachine{
		store:  make(map[string]string),
		logger: FormatLogger("state_machine"),
	}
}

func (k *KVStateMachine) ApplyLog(logData []byte) error {
	k.logger.Debug("ApplyLog entry", "logData", string(logData))

	kv := struct {
		Operation string `json:"operation"`
		Key       string `json:"key"`
		Value     string `json:"value"`
	}{}

	if err := json.Unmarshal(logData, &kv); err != nil {
		err = fmt.Errorf("%w, error: %s", ErrStateMachineApplyLogFailed, err.Error())
		k.logger.Error(err.Error())
		return err
	}

	switch kv.Operation {
	case "put":
		k.store[kv.Key] = kv.Value
	case "delete":
		delete(k.store, kv.Key)
	default:
		// unknown operation
		err := fmt.Errorf("%w, operation: %s", ErrKVStateMachineInvalidOperation, kv.Operation)
		k.logger.Error(err.Error())
		return err
	}
	return nil
}

func (k *KVStateMachine) ApplySnapshot(snapshotData []byte) error {
	k.logger.Debug("ApplySnapshot entry", "snapshotData", string(snapshotData))

	if err := json.Unmarshal(snapshotData, &k.store); err != nil {
		err = fmt.Errorf("%w, error: %s", ErrStateMachineApplySnapshotFailed, err.Error())
		k.logger.Error(err.Error())
		return err
	}
	return nil
}

func (k *KVStateMachine) TakeSnapshot() ([]byte, error) {
	k.logger.Debug("TakeSnapshot entry")

	snapshotData, err := json.Marshal(k.store)
	if err != nil {
		err = fmt.Errorf("%w, error: %s", ErrStateMachineTakeSnapshotFailed, err.Error())
		k.logger.Error(err.Error())
		return nil, err
	}
	return snapshotData, nil
}

func (k *KVStateMachine) Close() error {
	return nil
}
