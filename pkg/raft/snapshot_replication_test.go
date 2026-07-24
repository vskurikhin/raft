package raft

import (
	"testing"
)

// TestGetLogLengthAfterSnapshot проверяет, что getLogLength() возвращает
// правильное значение после создания снапшота. Баг: после снапшота
// getLogLength() возвращал lastIncludedIndex + len(log) вместо
// lastIncludedIndex + 1 + len(log), что приводило к нерепликации новых записей.
func TestGetLogLengthAfterSnapshot(t *testing.T) {
	storage := NewMapStorage()
	commitChan := make(chan CommitEntry, 100)
	snapshotChan := make(chan []byte, 1)
	ready := make(chan any)
	close(ready)

	cm := NewConsensusModule(1, []int{2, 3}, nil, storage, ready, commitChan, snapshotChan, nil)

	// Добавить 8 записей (логические индексы 0..7)
	cm.mu.Lock()
	cm.state = Leader
	cm.currentTerm = 1
	for i := 0; i < 8; i++ {
		cm.log = append(cm.log, LogEntry{Command: i, Term: cm.currentTerm})
	}
	cm.commitIndex = 7
	cm.lastApplied = 7
	cm.mu.Unlock()

	// Сделать снапшот (snapIndex = 7)
	cm.mu.Lock()
	cm.TakeSnapshot([]byte("test-snapshot"))
	cm.mu.Unlock()

	// После снапшота: lastIncludedIndex=7, len(log)=0
	// getLogLength() ДОЛЖЕН возвращать 7 + 1 + 0 = 8
	// (баг: возвращал 7 + 0 = 7)
	logLen := cm.getLogLength()
	if logLen != 8 {
		t.Fatalf("getLogLength() after snapshot: expected 8, got %d", logLen)
	}

	// Добавить ещё 1 запись (key8, логический индекс 8)
	cm.mu.Lock()
	cm.log = append(cm.log, LogEntry{Command: 8, Term: 1})
	cm.commitIndex = 8
	cm.lastApplied = 8
	cm.mu.Unlock()

	// После добавления: lastIncludedIndex=7, len(log)=1
	// getLogLength() ДОЛЖЕН возвращать 7 + 1 + 1 = 9
	// (баг: возвращал 7 + 1 = 8)
	logLen = cm.getLogLength()
	if logLen != 9 {
		t.Fatalf("getLogLength() after snapshot + 1 entry: expected 9, got %d", logLen)
	}

	// Проверить lastLogIndexAndTerm
	lastIdx, lastTerm := cm.lastLogIndexAndTerm()
	if lastIdx != 8 {
		t.Fatalf("lastLogIndexAndTerm: expected index 8, got %d", lastIdx)
	}
	if lastTerm != 1 {
		t.Fatalf("lastLogIndexAndTerm: expected term 1, got %d", lastTerm)
	}
}

// TestLogOffsetAfterSnapshot проверяет, что logOffset() правильно
// возвращает смещение после снапшота.
func TestLogOffsetAfterSnapshot(t *testing.T) {
	storage := NewMapStorage()
	commitChan := make(chan CommitEntry, 100)
	snapshotChan := make(chan []byte, 1)
	ready := make(chan any)
	close(ready)

	cm := NewConsensusModule(1, []int{2, 3}, nil, storage, ready, commitChan, snapshotChan, nil)

	// Случай 1: до снапшота (lastIncludedIndex = 0) → offset = 0
	if cm.logOffset() != 0 {
		t.Fatalf("logOffset() before snapshot: expected 0, got %d", cm.logOffset())
	}

	// Случай 2: lastIncludedIndex = 7 (снапшот на индексе 7) → offset = 8
	cm.mu.Lock()
	cm.state = Leader
	cm.currentTerm = 1
	for i := 0; i < 8; i++ {
		cm.log = append(cm.log, LogEntry{Command: i, Term: cm.currentTerm})
	}
	cm.commitIndex = 7
	cm.lastApplied = 7
	cm.TakeSnapshot([]byte("snap-at-7"))
	cm.mu.Unlock()

	if cm.lastIncludedIndex != 7 {
		t.Fatalf("expected lastIncludedIndex=7, got %d", cm.lastIncludedIndex)
	}

	offset := cm.logOffset()
	if offset != 8 {
		t.Fatalf("logOffset() after snapshot: expected 8, got %d", offset)
	}

	// getLogLength() должен вернуть 7 + 1 + 0 = 8
	logLen := cm.getLogLength()
	if logLen != 8 {
		t.Fatalf("getLogLength() after snapshot: expected 8, got %d", logLen)
	}
}

// TestSnapshotThenReplication имитирует сценарий из бага:
// 8 записей → снапшот → ещё 1 запись, проверяет что новая запись
// реплицируется (nextIndexArgsEntries включает её в AppendEntries).
func TestSnapshotThenReplication(t *testing.T) {
	storage := NewMapStorage()
	commitChan := make(chan CommitEntry, 100)
	snapshotChan := make(chan []byte, 1)
	ready := make(chan any)
	close(ready)

	cm := NewConsensusModule(1, []int{2, 3}, nil, storage, ready, commitChan, snapshotChan, nil)

	// Добавить 8 записей (логические индексы 0..7)
	cm.mu.Lock()
	cm.state = Leader
	cm.currentTerm = 1
	for i := 0; i < 8; i++ {
		cm.log = append(cm.log, LogEntry{Command: i, Term: cm.currentTerm})
	}
	cm.commitIndex = 7
	cm.lastApplied = 7
	cm.mu.Unlock()

	// Сделать снапшот (snapIndex = 7)
	cm.mu.Lock()
	cm.TakeSnapshot([]byte("test-snapshot"))
	cm.mu.Unlock()

	// Установить nextIndex для peer 2 на 8 (как после репликации снапшота)
	cm.mu.Lock()
	cm.nextIndex[2] = 8
	cm.matchIndex[2] = 7
	cm.mu.Unlock()

	// Добавить ещё 1 запись (key8, логический индекс 8)
	cm.mu.Lock()
	cm.log = append(cm.log, LogEntry{Command: 8, Term: 1})
	cm.mu.Unlock()

	// Проверить nextIndexArgsEntries: при ni=8, logLen=9 (было 8 — баг)
	// Должен включить entries (не heartbeat)
	cm.mu.Lock()
	logLen := cm.getLogLength()
	ni := cm.nextIndex[2]
	// При баге: logLen=8, ni=8, ni < logLen → false → entries=nil
	// После исправления: logLen=9, ni=8, ni < logLen → true → entries не пуст
	if logLen != 9 {
		cm.mu.Unlock()
		t.Fatalf("expected getLogLength()=9, got %d", logLen)
	}
	cm.mu.Unlock()

	ni, args, _ := cm.nextIndexArgsEntries(2, 1)
	if ni != 8 {
		t.Fatalf("expected nextIndex=8, got %d", ni)
	}
	if len(args.Entries) != 1 {
		t.Fatalf("expected 1 entry in AppendEntries, got %d (BUG: snapshot blocks replication)", len(args.Entries))
	}
	if args.Entries[0].Command.(int) != 8 {
		t.Fatalf("expected entry command=8, got %v", args.Entries[0].Command)
	}
	if args.PrevLogIndex != 7 {
		t.Fatalf("expected PrevLogIndex=7, got %d", args.PrevLogIndex)
	}
	if args.PrevLogTerm != 1 {
		t.Fatalf("expected PrevLogTerm=1, got %d", args.PrevLogTerm)
	}
}

// TestSnapshotThenCommit проверяет, что commit loop корректно
// обрабатывает записи после снапшота.
func TestSnapshotThenCommit(t *testing.T) {
	storage := NewMapStorage()
	commitChan := make(chan CommitEntry, 100)
	snapshotChan := make(chan []byte, 1)
	ready := make(chan any)
	close(ready)

	cm := NewConsensusModule(1, []int{2, 3}, nil, storage, ready, commitChan, snapshotChan, nil)

	// Добавить 8 записей (логические индексы 0..7)
	cm.mu.Lock()
	cm.state = Leader
	cm.currentTerm = 1
	for i := 0; i < 8; i++ {
		cm.log = append(cm.log, LogEntry{Command: i, Term: cm.currentTerm})
	}
	cm.commitIndex = 7
	cm.lastApplied = 7
	cm.mu.Unlock()

	// Сделать снапшот (snapIndex = 7)
	cm.mu.Lock()
	cm.TakeSnapshot([]byte("test-snapshot"))
	cm.mu.Unlock()

	// Добавить ещё 1 запись (key8, логический индекс 8)
	cm.mu.Lock()
	cm.log = append(cm.log, LogEntry{Command: 8, Term: 1})
	cm.mu.Unlock()

	// Имитация коммит-лупа:
	// commitIndex=7, getLogLength()=9
	// Цикл: for i := commitIndex+1; i < logLen; i++ → i=8
	// При баге: logLen=8, цикл не выполняется
	// После исправления: logLen=9, i=8 < 9 → выполняется
	cm.mu.Lock()
	logLen := cm.getLogLength()
	// Установим matchIndex для peer 2 = 8 (имитация успешной репликации)
	cm.matchIndex[2] = 8
	cm.commitIndex = 7

	// Запустим логику коммита
	for i := cm.commitIndex + 1; i < logLen; i++ {
		entry, ok := cm.getLogEntry(i)
		if !ok {
			break
		}
		if entry.Term == cm.currentTerm {
			matchCount := 1
			for _, pid := range cm.peerIds {
				if cm.matchIndex[pid] >= i {
					matchCount++
				}
			}
			if matchCount*2 > len(cm.peerIds)+1 {
				cm.commitIndex = i
			}
		}
	}
	cm.mu.Unlock()

	if cm.commitIndex != 8 {
		t.Fatalf("expected commitIndex=8, got %d (BUG: commit loop not advancing after snapshot)", cm.commitIndex)
	}
}

// TestGetLogLengthAfterSnapshotWithEntries проверяет getLogLength()
// после снапшота с несколькими добавленными записями.
func TestGetLogLengthAfterSnapshotWithEntries(t *testing.T) {
	storage := NewMapStorage()
	commitChan := make(chan CommitEntry, 100)
	snapshotChan := make(chan []byte, 1)
	ready := make(chan any)
	close(ready)

	cm := NewConsensusModule(1, []int{2, 3}, nil, storage, ready, commitChan, snapshotChan, nil)

	// Добавить 8 записей (логические индексы 0..7)
	cm.mu.Lock()
	cm.state = Leader
	cm.currentTerm = 1
	for i := 0; i < 8; i++ {
		cm.log = append(cm.log, LogEntry{Command: i, Term: cm.currentTerm})
	}
	cm.commitIndex = 7
	cm.lastApplied = 7
	cm.mu.Unlock()

	// Сделать снапшот (snapIndex = 7)
	cm.mu.Lock()
	cm.TakeSnapshot([]byte("test-snapshot"))
	cm.mu.Unlock()

	// После снапшота: lastIncludedIndex=7, len(log)=0
	// getLogLength() = 7 + 1 + 0 = 8
	logLen := cm.getLogLength()
	if logLen != 8 {
		t.Fatalf("getLogLength() after snapshot: expected 8, got %d", logLen)
	}

	// Добавить 3 записи (логические индексы 8, 9, 10)
	cm.mu.Lock()
	cm.log = append(cm.log, LogEntry{Command: 8, Term: 1})
	cm.log = append(cm.log, LogEntry{Command: 9, Term: 1})
	cm.log = append(cm.log, LogEntry{Command: 10, Term: 1})
	cm.mu.Unlock()

	// getLogLength() = 7 + 1 + 3 = 11
	logLen = cm.getLogLength()
	if logLen != 11 {
		t.Fatalf("getLogLength() after snapshot + 3 entries: expected 11, got %d", logLen)
	}

	// Проверить, что getLogEntry работает для новых записей
	for i := 8; i <= 10; i++ {
		entry, ok := cm.getLogEntry(i)
		if !ok {
			t.Fatalf("getLogEntry(%d): expected true, got false", i)
		}
		if entry.Command.(int) != i {
			t.Fatalf("getLogEntry(%d): expected Command=%d, got %v", i, i, entry.Command)
		}
	}

	// Проверить lastLogIndexAndTerm
	lastIdx, lastTerm := cm.lastLogIndexAndTerm()
	if lastIdx != 10 {
		t.Fatalf("lastLogIndexAndTerm: expected index 10, got %d", lastIdx)
	}
	if lastTerm != 1 {
		t.Fatalf("lastLogIndexAndTerm: expected term 1, got %d", lastTerm)
	}
}
