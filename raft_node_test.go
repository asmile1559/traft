package traft

import (
	"fmt"
	raftpb "github.com/asmile1559/traft/internal/apis/raft"
	"testing"
	"time"
)

func TestRaft(t *testing.T) {
	sm1 := NewKVStateMachine()
	pe1 := NewFilePersister("./test/raft1")
	cf1 := &Config{
		Id:           "abc",
		Addr:         "localhost:12333",
		StateMachine: sm1,
		Persister:    pe1,
		Peers:        map[string]string{"abc": "localhost:12333", "def": "localhost:12334"},
	}

	raft1 := New(cf1)
	if raft1 == nil {
		t.Fatal("raft1 is nil")
	}

	sm2 := NewKVStateMachine()
	pe2 := NewFilePersister("./test/raft2")
	cf2 := &Config{
		Id:           "def",
		Addr:         "localhost:12334",
		StateMachine: sm2,
		Persister:    pe2,
		Peers:        map[string]string{"abc": "localhost:12333", "def": "localhost:12334"},
	}
	raft2 := New(cf2)
	if raft2 == nil {
		t.Fatal("raft2 is nil")
	}

	go func() {
		err := raft1.Serve()
		if err != nil {
			return
		}
	}()
	go func() {
		err := raft2.Serve()
		if err != nil {
			return
		}
	}()

	time.Sleep(1 * time.Hour)
}
func TestRaft1(t *testing.T) {
	sm1 := NewKVStateMachine()
	pe1 := NewFilePersister("./test/raft1")
	cf1 := &Config{
		Id:           "abc",
		Addr:         "localhost:12333",
		StateMachine: sm1,
		Persister:    pe1,
		Peers:        map[string]string{"abc": "localhost:12333", "def": "localhost:12334"},
	}

	raft1 := New(cf1)
	if raft1 == nil {
		t.Fatal("raft1 is nil")
	}

	sm2 := NewKVStateMachine()
	pe2 := NewFilePersister("./test/raft2")
	cf2 := &Config{
		Id:           "def",
		Addr:         "localhost:12334",
		StateMachine: sm2,
		Persister:    pe2,
		Peers:        map[string]string{"abc": "localhost:12333", "def": "localhost:12334"},
	}
	raft2 := New(cf2)
	if raft2 == nil {
		t.Fatal("raft2 is nil")
	}

	go func() {
		err := raft1.Serve()
		if err != nil {
			return
		}
	}()
	go func() {
		err := raft2.Serve()
		if err != nil {
			return
		}
	}()

	time.Sleep(1 * time.Second)
	fmt.Println()
	fmt.Println()
	fmt.Println()
	fmt.Println()
	fmt.Println()
	if raft1.(*raftNode).role == Leader {
		for i := range 20 {
			_ = raft1.AppendLogEntry(&raftpb.LogEntry{
				Index: uint64(i + 1),
				Term:  1,
				Data:  []byte(fmt.Sprintf(`{"operation": "put", "key": "%d", "value": "%d"}`, i, i)),
			})
			time.Sleep(1 * time.Second)
		}
	} else {
		for i := range 20 {
			_ = raft2.AppendLogEntry(&raftpb.LogEntry{
				Index: uint64(i + 1),
				Term:  1,
				Data:  []byte(fmt.Sprintf(`{"operation": "put", "key": "%d", "value": "%d"}`, i, i)),
			})
			time.Sleep(1 * time.Second)
		}
	}
	time.Sleep(1 * time.Hour)
}
