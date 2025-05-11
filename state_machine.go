package traft

import (
	"encoding/json"
	"log/slog"
	"os"
	"sync"
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
}

type KVStateMachine struct {
	// kv store
	nodeId string
	store  map[string]string
	mu     sync.RWMutex
	logger *slog.Logger
}

func NewKVStateMachine(nodeId string) *KVStateMachine {

	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     LoggerLevel,
	})

	logger := slog.New(h).With(
		"node_id", nodeId,
		"module", "state_machine",
	)

	return &KVStateMachine{
		nodeId: nodeId,
		store:  make(map[string]string),
		logger: logger,
	}
}

func (k *KVStateMachine) ApplyLog(logData []byte) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	kv := struct {
		Operation string `json:"operation"`
		Key       string `json:"key"`
		Value     string `json:"value"`
	}{}

	if err := json.Unmarshal(logData, &kv); err != nil {
		k.logger.Error("ApplyLog failed", "error", err.Error())
		return err
	}

	switch kv.Operation {
	case "put":
		k.store[kv.Key] = kv.Value
	case "delete":
		delete(k.store, kv.Key)
	default:
		k.logger.Error("Unknown operation", "operation", kv.Operation)
		return ErrUnknownKVStateMachineOperation
	}
	return nil
}

func (k *KVStateMachine) ApplySnapshot(snapshotData []byte) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if err := json.Unmarshal(snapshotData, &k.store); err != nil {
		k.logger.Error("ApplySnapshot failed", "error", err.Error())
		return err
	}
	return nil
}

func (k *KVStateMachine) TakeSnapshot() ([]byte, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()

	snapshotData, err := json.Marshal(k.store)
	if err != nil {
		k.logger.Error("TakeSnapshot failed", "error", err.Error())
		return nil, err
	}
	return snapshotData, nil
}
