package traft

//
//func TestRaft(t *testing.T) {
//	sm1 := NewKVStateMachine()
//	pe1 := NewFilePersister("./test/raft1")
//	cf1 := &Config{
//		Id:           "abc",
//		Addr:         "localhost:12333",
//		StateMachine: sm1,
//		Persister:    pe1,
//		Peers:        map[string]string{"abc": "localhost:12333", "def": "localhost:12334"},
//	}
//
//	raft1 := New(cf1)
//	if raft1 == nil {
//		t.Fatal("raft1 is nil")
//	}
//
//	sm2 := NewKVStateMachine()
//	pe2 := NewFilePersister("./test/raft2")
//	cf2 := &Config{
//		Id:           "def",
//		Addr:         "localhost:12334",
//		StateMachine: sm2,
//		Persister:    pe2,
//		Peers:        map[string]string{"abc": "localhost:12333", "def": "localhost:12334"},
//	}
//	raft2 := New(cf2)
//	if raft2 == nil {
//		t.Fatal("raft2 is nil")
//	}
//
//	go func() {
//		err := raft1.Start()
//		if err != nil {
//			t.Fatal("raft1 start failed", err)
//			return
//		}
//	}()
//	go func() {
//		err := raft2.Start()
//		if err != nil {
//			t.Fatal("raft2 start failed", err)
//			return
//		}
//	}()
//
//	time.Sleep(1 * time.Hour)
//}
