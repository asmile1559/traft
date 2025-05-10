package traft

import (
	"encoding/json"
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
	TakeSnapshot() []byte
}

type KVStateMachine struct {
	// kv store
	store map[string]string
	mu    sync.RWMutex
}

func NewKVStateMachine() *KVStateMachine {
	return &KVStateMachine{
		store: make(map[string]string),
	}
}

func (k *KVStateMachine) ApplyLog(logData []byte) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	kv := struct {
		Operarion string `json:"operation"`
		Key       string `json:"key"`
		Value     string `json:"value"`
	}{}

	if err := json.Unmarshal(logData, &kv); err != nil {
		return err
	}

	switch kv.Operarion {
	case "put":
		k.store[kv.Key] = kv.Value
	case "delete":
		delete(k.store, kv.Key)
	default:
		return nil
	}
	return nil
}

func (k *KVStateMachine) ApplySnapshot(snapshotData []byte) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if err := json.Unmarshal(snapshotData, &k.store); err != nil {
		return err
	}
	return nil
}

func (k *KVStateMachine) TakeSnapshot() []byte {
	k.mu.RLock()
	defer k.mu.RUnlock()

	snapshotData, err := json.Marshal(k.store)
	if err != nil {
		return nil
	}
	return snapshotData
}
