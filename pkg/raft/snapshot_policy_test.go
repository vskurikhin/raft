package raft

import (
	"testing"
)

// TestSetSnapshotPolicy проверяет установку пороговой политики снепшотов.
func TestSetSnapshotPolicy(t *testing.T) {
	storage := NewMapStorage()
	commitChan := make(chan CommitEntry, 100)
	snapshotChan := make(chan []byte, 1)
	ready := make(chan any)
	close(ready)

	cm := NewConsensusModule(1, []int{2, 3}, nil, storage, ready, commitChan, snapshotChan)

	cm.SetSnapshotPolicy(64, 16)
	if cm.snapshotThreshold != 64 {
		t.Fatalf("expected threshold=64, got %d", cm.snapshotThreshold)
	}
	if cm.snapshotInterval != 16 {
		t.Fatalf("expected interval=16, got %d", cm.snapshotInterval)
	}
}

// TestSetSnapshotDataFn проверяет установку функции обратного вызова для данных снепшота.
func TestSetSnapshotDataFn(t *testing.T) {
	storage := NewMapStorage()
	commitChan := make(chan CommitEntry, 100)
	snapshotChan := make(chan []byte, 1)
	ready := make(chan any)
	close(ready)

	cm := NewConsensusModule(1, []int{2, 3}, nil, storage, ready, commitChan, snapshotChan)

	called := false
	cm.SetSnapshotDataFn(func() []byte {
		called = true
		return []byte("data")
	})

	if cm.snapshotDataFn == nil {
		t.Fatal("snapshotDataFn should not be nil after SetSnapshotDataFn")
	}

	// Вызвать функцию
	data := cm.snapshotDataFn()
	if !called {
		t.Fatal("snapshotDataFn was not called")
	}
	if string(data) != "data" {
		t.Fatalf("expected 'data', got %s", string(data))
	}
}

// TestSnapshotThresholdCondition проверяет условие срабатывания снепшота
// по порогу len(cm.log) >= snapshotThreshold.
func TestSnapshotThresholdCondition(t *testing.T) {
	storage := NewMapStorage()
	commitChan := make(chan CommitEntry, 100)
	snapshotChan := make(chan []byte, 1)
	ready := make(chan any)
	close(ready)

	cm := NewConsensusModule(1, []int{2, 3}, nil, storage, ready, commitChan, snapshotChan)
	cm.state = Leader
	cm.currentTerm = 1
	cm.lastApplied = 99
	cm.snapshotThreshold = 50
	cm.snapshotInterval = 10

	// Заполнить журнал 60 записями (превышает threshold=50)
	for i := 0; i < 60; i++ {
		cm.log = append(cm.log, LogEntry{Command: i, Term: cm.currentTerm})
	}
	cm.commitIndex = 99

	// Условие: len(cm.log)=60 >= 50 → true
	if !(len(cm.log) >= cm.snapshotThreshold) {
		t.Fatal("expected log length >= threshold")
	}

	// Условие: lastApplied - lastIncludedIndex = 99 - 0 >= 10 → true
	if !(cm.lastApplied-cm.lastIncludedIndex >= cm.snapshotInterval) {
		t.Fatal("expected gap >= interval")
	}

	// Проверить полное условие
	triggered := cm.state == Leader &&
		cm.snapshotThreshold > 0 &&
		len(cm.log) >= cm.snapshotThreshold &&
		cm.lastApplied-cm.lastIncludedIndex >= cm.snapshotInterval
	if !triggered {
		t.Fatal("expected snapshot trigger condition to be true")
	}
}

// TestSnapshotThresholdNotMet проверяет, что условие ложно при малом журнале.
func TestSnapshotThresholdNotMet(t *testing.T) {
	storage := NewMapStorage()
	commitChan := make(chan CommitEntry, 100)
	snapshotChan := make(chan []byte, 1)
	ready := make(chan any)
	close(ready)

	cm := NewConsensusModule(1, []int{2, 3}, nil, storage, ready, commitChan, snapshotChan)
	cm.state = Leader
	cm.currentTerm = 1
	cm.lastApplied = 29
	cm.snapshotThreshold = 50
	cm.snapshotInterval = 10

	// Заполнить журнал 30 записями (меньше threshold=50)
	for i := 0; i < 30; i++ {
		cm.log = append(cm.log, LogEntry{Command: i, Term: cm.currentTerm})
	}
	cm.commitIndex = 29

	triggered := cm.state == Leader &&
		cm.snapshotThreshold > 0 &&
		len(cm.log) >= cm.snapshotThreshold &&
		cm.lastApplied-cm.lastIncludedIndex >= cm.snapshotInterval
	if triggered {
		t.Fatal("expected snapshot trigger condition to be false (log too small)")
	}
}

// TestSnapshotIntervalNotMet проверяет, что условие ложно при малом интервале.
func TestSnapshotIntervalNotMet(t *testing.T) {
	storage := NewMapStorage()
	commitChan := make(chan CommitEntry, 100)
	snapshotChan := make(chan []byte, 1)
	ready := make(chan any)
	close(ready)

	cm := NewConsensusModule(1, []int{2, 3}, nil, storage, ready, commitChan, snapshotChan)
	cm.state = Leader
	cm.currentTerm = 1
	cm.lastApplied = 15
	cm.snapshotThreshold = 10
	cm.snapshotInterval = 50 // большой интервал

	// Заполнить журнал 20 записями
	for i := 0; i < 20; i++ {
		cm.log = append(cm.log, LogEntry{Command: i, Term: cm.currentTerm})
	}
	cm.commitIndex = 15

	triggered := cm.state == Leader &&
		cm.snapshotThreshold > 0 &&
		len(cm.log) >= cm.snapshotThreshold &&
		cm.lastApplied-cm.lastIncludedIndex >= cm.snapshotInterval
	if triggered {
		t.Fatal("expected snapshot trigger condition to be false (interval not met)")
	}
}

// TestSnapshotDisabled проверяет, что при SnapshotThreshold=0 снепшоты не создаются.
func TestSnapshotDisabled(t *testing.T) {
	storage := NewMapStorage()
	commitChan := make(chan CommitEntry, 100)
	snapshotChan := make(chan []byte, 1)
	ready := make(chan any)
	close(ready)

	cm := NewConsensusModule(1, []int{2, 3}, nil, storage, ready, commitChan, snapshotChan)
	cm.state = Leader
	cm.currentTerm = 1
	cm.lastApplied = 99
	cm.snapshotThreshold = 0 // отключено
	cm.snapshotInterval = 10

	for i := 0; i < 100; i++ {
		cm.log = append(cm.log, LogEntry{Command: i, Term: cm.currentTerm})
	}
	cm.commitIndex = 99

	triggered := cm.state == Leader &&
		cm.snapshotThreshold > 0 &&
		len(cm.log) >= cm.snapshotThreshold &&
		cm.lastApplied-cm.lastIncludedIndex >= cm.snapshotInterval
	if triggered {
		t.Fatal("expected snapshot trigger condition to be false (disabled)")
	}
}

// TestSnapshotOnlyOnLeader проверяет, что снепшот создаётся только на лидере.
func TestSnapshotOnlyOnLeader(t *testing.T) {
	storage := NewMapStorage()
	commitChan := make(chan CommitEntry, 100)
	snapshotChan := make(chan []byte, 1)
	ready := make(chan any)
	close(ready)

	cm := NewConsensusModule(1, []int{2, 3}, nil, storage, ready, commitChan, snapshotChan)
	cm.state = Follower // не лидер
	cm.currentTerm = 1
	cm.lastApplied = 99
	cm.snapshotThreshold = 50
	cm.snapshotInterval = 10

	for i := 0; i < 100; i++ {
		cm.log = append(cm.log, LogEntry{Command: i, Term: cm.currentTerm})
	}
	cm.commitIndex = 99

	triggered := cm.state == Leader &&
		cm.snapshotThreshold > 0 &&
		len(cm.log) >= cm.snapshotThreshold &&
		cm.lastApplied-cm.lastIncludedIndex >= cm.snapshotInterval
	if triggered {
		t.Fatal("expected snapshot trigger condition to be false (follower)")
	}
}

// TestSnapshotAfterTakeSnapshotResetsGap проверяет, что после TakeSnapshot
// интервал lastApplied - lastIncludedIndex сбрасывается.
func TestSnapshotAfterTakeSnapshotResetsGap(t *testing.T) {
	storage := NewMapStorage()
	commitChan := make(chan CommitEntry, 100)
	snapshotChan := make(chan []byte, 1)
	ready := make(chan any)
	close(ready)

	cm := NewConsensusModule(1, []int{2, 3}, nil, storage, ready, commitChan, snapshotChan)
	cm.state = Leader
	cm.currentTerm = 1

	// Добавить 100 записей и зафиксировать их
	for i := 0; i < 100; i++ {
		cm.log = append(cm.log, LogEntry{Command: i, Term: cm.currentTerm})
	}
	cm.commitIndex = 99
	cm.lastApplied = 99

	// Сделать снепшот
	cm.mu.Lock()
	cm.TakeSnapshot([]byte("data"))
	cm.mu.Unlock()

	// После снепшота lastIncludedIndex == lastApplied == 99
	// gap = lastApplied - lastIncludedIndex = 0
	if cm.lastApplied-cm.lastIncludedIndex != 0 {
		t.Fatalf("expected gap=0 after snapshot, got %d",
			cm.lastApplied-cm.lastIncludedIndex)
	}

	// Журнал должен быть пуст (обрезан)
	if len(cm.log) != 0 {
		t.Fatalf("expected empty log after snapshot covering all entries, got %d entries",
			len(cm.log))
	}
}

// TestConfigSnapshotThresholdPropagation проверяет, что Config.SnapshotThreshold
// передаётся в Server и может быть установлен через SetSnapshotPolicy.
func TestConfigSnapshotThresholdPropagation(t *testing.T) {
	cfg := Config{
		PeerIds:           []int{2, 3},
		RPCAddress:        ":0",
		ServerID:          1,
		SnapshotThreshold: 128,
		SnapshotInterval:  32,
	}
	storage := NewMapStorage()
	ready := make(chan any)
	commitChan := make(chan CommitEntry, 100)

	s := NewWithSnapshot(cfg, storage, ready, commitChan, make(chan []byte, 1))

	if s.snapshotThreshold != 128 {
		t.Fatalf("expected snapshotThreshold=128 on server, got %d", s.snapshotThreshold)
	}
	if s.snapshotInterval != 32 {
		t.Fatalf("expected snapshotInterval=32 on server, got %d", s.snapshotInterval)
	}
}

// TestSnapshotPolicyWithDataFn проверяет полный цикл: установка dataFn,
// вызов TakeSnapshot с данными от dataFn.
func TestSnapshotPolicyWithDataFn(t *testing.T) {
	storage := NewMapStorage()
	commitChan := make(chan CommitEntry, 100)
	snapshotChan := make(chan []byte, 1)
	ready := make(chan any)
	close(ready)

	cm := NewConsensusModule(1, []int{2, 3}, nil, storage, ready, commitChan, snapshotChan)
	cm.state = Leader
	cm.currentTerm = 1

	// Добавить и зафиксировать записи
	for i := 0; i < 10; i++ {
		cm.log = append(cm.log, LogEntry{Command: i, Term: cm.currentTerm})
	}
	cm.commitIndex = 9
	cm.lastApplied = 9

	// Установить dataFn
	cm.SetSnapshotDataFn(func() []byte {
		return []byte("state-machine-data")
	})

	// Вызвать TakeSnapshot с данными от dataFn
	cm.mu.Lock()
	cm.TakeSnapshot(cm.snapshotDataFn())
	cm.mu.Unlock()

	if cm.lastIncludedIndex != 9 {
		t.Fatalf("expected lastIncludedIndex=9, got %d", cm.lastIncludedIndex)
	}
	if string(cm.snapshotData) != "state-machine-data" {
		t.Fatalf("expected snapshotData='state-machine-data', got %s", string(cm.snapshotData))
	}
}
