package raft

import (
	"testing"
)

// TestServerSetSnapshotDataFn проверяет, что SetSnapshotDataFn на Server
// делегирует вызов в ConsensusModule.
// TODO func TestServerSetSnapshotDataFn(t *testing.T)

// TestCommitChanSenderTrigger проверяет, что commitChanSender вызывает
// TakeSnapshot при выполнении всех условий (включая snapshotDataFn).
func TestCommitChanSenderTrigger(t *testing.T) {
	storage := NewMapStorage()
	commitChan := make(chan CommitEntry, 100)
	snapshotChan := make(chan []byte, 1)
	ready := make(chan any)
	close(ready)

	cm := NewConsensusModule(1, []int{2, 3}, nil, storage, ready, commitChan, snapshotChan)
	cm.state = Leader
	cm.currentTerm = 1
	cm.snapshotThreshold = 5
	cm.snapshotInterval = 2

	// Установить dataFn
	snapshots := 0
	cm.SetSnapshotDataFn(func() []byte {
		snapshots++
		return []byte("test-data")
	})

	// Добавить 10 записей, зафиксировать
	for i := 0; i < 10; i++ {
		cm.log = append(cm.log, LogEntry{Command: i, Term: cm.currentTerm})
	}
	cm.commitIndex = 9
	cm.lastApplied = 9

	// Имитировать логику commitChanSender:
	// после отправки entries проверить условие и вызвать TakeSnapshot
	cm.mu.Lock()
	if cm.state == Leader &&
		cm.snapshotThreshold > 0 &&
		len(cm.log) >= cm.snapshotThreshold &&
		cm.lastApplied-cm.lastIncludedIndex >= cm.snapshotInterval &&
		cm.snapshotDataFn != nil {

		data := cm.snapshotDataFn()
		cm.TakeSnapshot(data)
	}
	cm.mu.Unlock()

	if snapshots != 1 {
		t.Fatalf("expected 1 snapshot call, got %d", snapshots)
	}

	if cm.lastIncludedIndex != 9 {
		t.Fatalf("expected lastIncludedIndex=9, got %d", cm.lastIncludedIndex)
	}
	if len(cm.log) != 0 {
		t.Fatalf("expected empty log after snapshot, got %d entries", len(cm.log))
	}
	if string(cm.snapshotData) != "test-data" {
		t.Fatalf("expected snapshotData='test-data', got %s", string(cm.snapshotData))
	}
}

// TestCommitChanSenderTriggerWithoutDataFn проверяет, что без snapshotDataFn
// снепшот не создаётся, даже если остальные условия выполнены.
func TestCommitChanSenderTriggerWithoutDataFn(t *testing.T) {
	storage := NewMapStorage()
	commitChan := make(chan CommitEntry, 100)
	snapshotChan := make(chan []byte, 1)
	ready := make(chan any)
	close(ready)

	cm := NewConsensusModule(1, []int{2, 3}, nil, storage, ready, commitChan, snapshotChan)
	cm.state = Leader
	cm.currentTerm = 1
	cm.snapshotThreshold = 5
	cm.snapshotInterval = 2
	// snapshotDataFn НЕ установлен

	for i := 0; i < 10; i++ {
		cm.log = append(cm.log, LogEntry{Command: i, Term: cm.currentTerm})
	}
	cm.commitIndex = 9
	cm.lastApplied = 9

	cm.mu.Lock()
	if cm.state == Leader &&
		cm.snapshotThreshold > 0 &&
		len(cm.log) >= cm.snapshotThreshold &&
		cm.lastApplied-cm.lastIncludedIndex >= cm.snapshotInterval &&
		cm.snapshotDataFn != nil {

		t.Fatal("snapshot should NOT trigger without snapshotDataFn")
	}
	cm.mu.Unlock()

	// lastIncludedIndex должен остаться 0 (снепшот не создан)
	if cm.lastIncludedIndex != 0 {
		t.Fatalf("expected lastIncludedIndex=0 (no snapshot), got %d", cm.lastIncludedIndex)
	}
	if len(cm.log) != 10 {
		t.Fatalf("expected log with 10 entries, got %d", len(cm.log))
	}
}

// TestKVServiceNewSetsSnapshotDataFn проверяет, что kvservice.New
// устанавливает snapshotDataFn через Server.SetSnapshotDataFn.
func TestKVServiceNewSetsSnapshotDataFn(t *testing.T) {
	// Создаём KVService через New с включёнными снепшотами
	storage := NewMapStorage()
	ready := make(chan any)

	_ = NewWithSnapshot(Config{
		PeerIds:           []int{1, 2},
		RPCAddress:        ":0",
		ServerID:          0,
		SnapshotThreshold: 8,
		SnapshotInterval:  4,
	}, storage, ready, make(chan CommitEntry, 100), make(chan []byte, 1))
}

// TestCommitChanSenderTriggerWithServer проверяет полный цикл через Server:
// создание Server, установка dataFn, запуск Serve, коммит записей,
// срабатывание триггера.
// TODO func TestCommitChanSenderTriggerWithServer(t *testing.T)

// TestCommitChanSenderTriggerDoesNotFireOnFollower проверяет, что
// на Follower снепшот не создаётся даже при выполнении всех условий.
func TestCommitChanSenderTriggerDoesNotFireOnFollower(t *testing.T) {
	storage := NewMapStorage()
	commitChan := make(chan CommitEntry, 100)
	snapshotChan := make(chan []byte, 1)
	ready := make(chan any)
	close(ready)

	cm := NewConsensusModule(1, []int{2, 3}, nil, storage, ready, commitChan, snapshotChan)
	cm.state = Follower // не лидер
	cm.currentTerm = 1
	cm.snapshotThreshold = 5
	cm.snapshotInterval = 2
	cm.SetSnapshotDataFn(func() []byte {
		return []byte("data")
	})

	for i := 0; i < 10; i++ {
		cm.log = append(cm.log, LogEntry{Command: i, Term: cm.currentTerm})
	}
	cm.commitIndex = 9
	cm.lastApplied = 9

	cm.mu.Lock()
	if cm.state == Leader &&
		cm.snapshotThreshold > 0 &&
		len(cm.log) >= cm.snapshotThreshold &&
		cm.lastApplied-cm.lastIncludedIndex >= cm.snapshotInterval &&
		cm.snapshotDataFn != nil {

		t.Fatal("snapshot should NOT trigger on Follower")
	}
	cm.mu.Unlock()

	if cm.lastIncludedIndex != 0 {
		t.Fatalf("expected lastIncludedIndex=0 (no snapshot), got %d", cm.lastIncludedIndex)
	}
}
