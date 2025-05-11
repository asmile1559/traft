package traft

import "testing"

func TestKVStateMachine_ApplyLog(t *testing.T) {
	kv := NewKVStateMachine()
	logData := []byte(`{"operation":"put","key":"foo","value":"bar"}`)
	if err := kv.ApplyLog(logData); err != nil {
		t.Fatalf("ApplyLog failed: %v", err)
	}

	if value, ok := kv.store["foo"]; !ok || value != "bar" {
		t.Fatalf("Expected value 'bar', got '%s'", value)
	}

	logData = []byte(`{"operation":"delete","key":"foo"}`)
	if err := kv.ApplyLog(logData); err != nil {
		t.Fatalf("ApplyLog failed: %v", err)
	}

	if _, ok := kv.store["foo"]; ok {
		t.Fatalf("Expected key 'foo' to be deleted")
	}
}

func TestKVStateMachine_ApplySnapshot(t *testing.T) {
	kv := NewKVStateMachine()
	snapshotData := []byte(`{"foo":"bar","baz":"qux"}`)
	if err := kv.ApplySnapshot(snapshotData); err != nil {
		t.Fatalf("ApplySnapshot failed: %v", err)
	}

	if value, ok := kv.store["foo"]; !ok || value != "bar" {
		t.Fatalf("Expected value 'bar', got '%s'", value)
	}
	if value, ok := kv.store["baz"]; !ok || value != "qux" {
		t.Fatalf("Expected value 'qux', got '%s'", value)
	}
}

func TestKVStateMachine_TakeSnapshot(t *testing.T) {
	kv := NewKVStateMachine()
	kv.store["foo"] = "bar"
	kv.store["baz"] = "qux"

	snapshotData, err := kv.TakeSnapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	kv1 := NewKVStateMachine()
	err = kv1.ApplySnapshot(snapshotData)
	if err != nil {
		t.Fatalf("ApplySnapshot failed: %v", err)
	}
	for key, value := range kv.store {
		if kv1.store[key] != value {
			t.Fatalf("Expected key '%s' to have value '%s', got '%s'", key, value, kv1.store[key])
		}
	}
}
