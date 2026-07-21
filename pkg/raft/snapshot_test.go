package raft

import (
	"bytes"
	"encoding/gob"
	"testing"
	"time"
)

// TestSnapshotBasic проверяет создание снепшота, обрезание журнала
// и работу хелперов getLogEntry/getLogLength/getLastLogIndex/getLastLogTerm.
func TestSnapshotBasic(t *testing.T) {
	storage := NewMapStorage()
	commitChan := make(chan CommitEntry, 100)
	snapshotChan := make(chan []byte, 1)
	ready := make(chan any)
	close(ready)

	cm := NewConsensusModule(1, []int{2, 3}, nil, storage, ready, commitChan, snapshotChan)

	// Добавить 10 записей (логические индексы 0..9 при offset=0).
	cm.mu.Lock()
	cm.state = Leader
	cm.currentTerm = 1
	for i := 0; i < 10; i++ {
		cm.log = append(cm.log, LogEntry{Command: i, Term: cm.currentTerm})
	}
	cm.commitIndex = 9
	cm.lastApplied = 9
	cm.mu.Unlock()

	// Вызвать TakeSnapshot — snapIndex = 9
	snapData := []byte("test-snapshot-data")
	cm.mu.Lock()
	cm.TakeSnapshot(snapData)
	cm.mu.Unlock()

	// Проверить lastIncludedIndex
	if cm.lastIncludedIndex != 9 {
		t.Fatalf("expected lastIncludedIndex=9, got %d", cm.lastIncludedIndex)
	}
	if cm.lastIncludedTerm != 1 {
		t.Fatalf("expected lastIncludedTerm=1, got %d", cm.lastIncludedTerm)
	}
	if !bytes.Equal(cm.snapshotData, snapData) {
		t.Fatalf("snapshotData mismatch")
	}

	// Журнал должен быть пуст (все 10 записей покрыты снепшотом)
	if len(cm.log) != 0 {
		t.Fatalf("expected empty log, got %d entries", len(cm.log))
	}

	// Проверить хелперы
	logLen := cm.getLogLength()
	if logLen != 9 {
		t.Fatalf("expected getLogLength()=9, got %d", logLen)
	}
	lastIdx := cm.getLastLogIndex()
	if lastIdx != 9 {
		t.Fatalf("expected getLastLogIndex()=9, got %d", lastIdx)
	}
	lastTerm := cm.getLastLogTerm()
	if lastTerm != 1 {
		t.Fatalf("expected getLastLogTerm()=1, got %d", lastTerm)
	}

	// getLogEntry для индекса, покрытого снепшотом, должен вернуть false
	_, ok := cm.getLogEntry(5)
	if ok {
		t.Fatal("expected getLogEntry(5) to return false (covered by snapshot)")
	}

	// Добавить ещё 2 записи (логические индексы 10, 11)
	cm.mu.Lock()
	cm.state = Leader
	cm.log = append(cm.log, LogEntry{Command: 10, Term: 1})
	cm.log = append(cm.log, LogEntry{Command: 11, Term: 1})
	cm.commitIndex = 11
	cm.lastApplied = 11
	cm.mu.Unlock()

	// Проверить getLogEntry для новых записей
	entry, ok := cm.getLogEntry(10)
	if !ok {
		t.Fatal("expected getLogEntry(10) to return true")
	}
	if entry.Command != 10 {
		t.Fatalf("expected entry.Command=10, got %v", entry.Command)
	}

	entry, ok = cm.getLogEntry(11)
	if !ok {
		t.Fatal("expected getLogEntry(11) to return true")
	}
	if entry.Command != 11 {
		t.Fatalf("expected entry.Command=11, got %v", entry.Command)
	}

	// Проверить lastLogIndexAndTerm
	lastIdx, lastTerm = cm.lastLogIndexAndTerm()
	if lastIdx != 11 {
		t.Fatalf("expected lastLogIndexAndTerm() index=11, got %d", lastIdx)
	}
	if lastTerm != 1 {
		t.Fatalf("expected lastLogIndexAndTerm() term=1, got %d", lastTerm)
	}

	_ = commitChan
}

// TestSnapshotPersistence проверяет сохранение и восстановление снепшота
// через persistToStorage/restoreFromStorage.
func TestSnapshotPersistence(t *testing.T) {
	storage := NewMapStorage()
	commitChan := make(chan CommitEntry, 100)
	snapshotChan := make(chan []byte, 1)
	ready := make(chan any)
	close(ready)

	cm := NewConsensusModule(1, []int{2, 3}, nil, storage, ready, commitChan, snapshotChan)

	// Добавить записи и сделать снепшот
	cm.mu.Lock()
	cm.state = Leader
	cm.currentTerm = 1
	for i := 0; i < 10; i++ {
		cm.log = append(cm.log, LogEntry{Command: i, Term: cm.currentTerm})
	}
	cm.commitIndex = 9
	cm.lastApplied = 9
	cm.TakeSnapshot([]byte("persistence-test-data"))
	cm.mu.Unlock()

	// Сохранить состояние через persistToStorage
	cm.mu.Lock()
	cm.persistToStorage()
	cm.mu.Unlock()

	// Создать новый CM с тем же storage
	snapshotChan2 := make(chan []byte, 1)
	cm2 := NewConsensusModule(1, []int{2, 3}, nil, storage, make(chan any), commitChan, snapshotChan2)

	// Проверить восстановленное состояние
	if cm2.lastIncludedIndex != 9 {
		t.Fatalf("expected restored lastIncludedIndex=9, got %d", cm2.lastIncludedIndex)
	}
	if cm2.lastIncludedTerm != 1 {
		t.Fatalf("expected restored lastIncludedTerm=1, got %d", cm2.lastIncludedTerm)
	}
	if !bytes.Equal(cm2.snapshotData, []byte("persistence-test-data")) {
		t.Fatalf("restored snapshotData mismatch")
	}
	if len(cm2.log) != 0 {
		t.Fatalf("expected restored empty log, got %d entries", len(cm2.log))
	}
	if cm2.currentTerm != 1 {
		t.Fatalf("expected restored currentTerm=1, got %d", cm2.currentTerm)
	}
}

// TestSnapshotPersistenceEmptyStorage проверяет, что restoreFromStorage
// корректно обрабатывает отсутствие снепшот-ключей (обратная совместимость).
func TestSnapshotPersistenceEmptyStorage(t *testing.T) {
	storage := NewMapStorage()
	commitChan := make(chan CommitEntry, 100)
	snapshotChan := make(chan []byte, 1)
	ready := make(chan any)
	close(ready)

	// Сохранить только основные ключи (без снепшот-ключей)
	func() {
		var termBuf bytes.Buffer
		gob.NewEncoder(&termBuf).Encode(1)
		storage.Set("currentTerm", termBuf.Bytes())

		var votedBuf bytes.Buffer
		gob.NewEncoder(&votedBuf).Encode(1)
		storage.Set("votedFor", votedBuf.Bytes())

		var logBuf bytes.Buffer
		gob.NewEncoder(&logBuf).Encode([]LogEntry{
			{Command: 0, Term: 1},
			{Command: 1, Term: 1},
		})
		storage.Set("log", logBuf.Bytes())
	}()

	cm := NewConsensusModule(1, []int{2, 3}, nil, storage, ready, commitChan, snapshotChan)

	if cm.lastIncludedIndex != 0 {
		t.Fatalf("expected lastIncludedIndex=0, got %d", cm.lastIncludedIndex)
	}
	if cm.lastIncludedTerm != 0 {
		t.Fatalf("expected lastIncludedTerm=0, got %d", cm.lastIncludedTerm)
	}
	if cm.snapshotData != nil {
		t.Fatalf("expected snapshotData=nil, got %v", cm.snapshotData)
	}
	if cm.currentTerm != 1 {
		t.Fatalf("expected currentTerm=1, got %d", cm.currentTerm)
	}
	if len(cm.log) != 2 {
		t.Fatalf("expected log with 2 entries, got %d", len(cm.log))
	}
}

// TestCommitChanSenderAfterSnapshot проверяет, что commitChanSender
// корректно отправляет записи после снепшота.
func TestCommitChanSenderAfterSnapshot(t *testing.T) {
	storage := NewMapStorage()
	commitChan := make(chan CommitEntry, 100)
	snapshotChan := make(chan []byte, 1)
	ready := make(chan any)
	close(ready)

	cm := NewConsensusModule(1, []int{2, 3}, nil, storage, ready, commitChan, snapshotChan)

	// Добавить 10 записей (логические индексы 0..9 при offset=0)
	cm.mu.Lock()
	cm.state = Leader
	cm.currentTerm = 1
	for i := 0; i < 10; i++ {
		cm.log = append(cm.log, LogEntry{Command: i, Term: cm.currentTerm})
	}
	cm.commitIndex = 9
	cm.lastApplied = 9
	cm.mu.Unlock()

	// Сделать снепшот (snapIndex = 9)
	cm.mu.Lock()
	cm.TakeSnapshot([]byte("test"))
	cm.mu.Unlock()

	// После снепшота: lastIncludedIndex=9, offset=10
	// Добавить ещё 3 записи (логические индексы 10, 11, 12)
	cm.mu.Lock()
	cm.log = append(cm.log, LogEntry{Command: 10, Term: 1})
	cm.log = append(cm.log, LogEntry{Command: 11, Term: 1})
	cm.log = append(cm.log, LogEntry{Command: 12, Term: 1})
	cm.commitIndex = 12
	cm.mu.Unlock()

	// Запустить commitChanSender
	cm.mu.Lock()
	var entries []LogEntry
	if cm.commitIndex > cm.lastApplied {
		offset := cm.logOffset()
		from := cm.lastApplied + 1 - offset
		to := cm.commitIndex + 1 - offset
		if from < 0 {
			from = 0
		}
		if to > len(cm.log) {
			to = len(cm.log)
		}
		if to > from {
			entries = append([]LogEntry{}, cm.log[from:to]...)
		}
		cm.lastApplied = cm.commitIndex
	}
	cm.mu.Unlock()

	if len(entries) != 3 {
		t.Fatalf("expected 3 entries after snapshot, got %d", len(entries))
	}
	for i, e := range entries {
		expectedCmd := 10 + i
		if e.Command.(int) != expectedCmd {
			t.Fatalf("entry %d: expected Command=%d, got %v", i, expectedCmd, e.Command)
		}
	}
	if cm.lastApplied != 12 {
		t.Fatalf("expected lastApplied=12, got %d", cm.lastApplied)
	}
}

// TestCommitChanSenderAfterInstallSnapshot проверяет, что после InstallSnapshot
// commitChanSender не отправляет записи, покрытые снепшотом.
func TestCommitChanSenderAfterInstallSnapshot(t *testing.T) {
	storage := NewMapStorage()
	commitChan := make(chan CommitEntry, 100)
	snapshotChan := make(chan []byte, 1)
	ready := make(chan any)
	close(ready)

	cm := NewConsensusModule(1, []int{2, 3}, nil, storage, ready, commitChan, snapshotChan)

	// Имитация получения InstallSnapshot
	args := InstallSnapshotArgs{
		Term:              1,
		LeaderID:          2,
		LastIncludedIndex: 10,
		LastIncludedTerm:  1,
		Data:              []byte("snapshot-data"),
	}
	var reply InstallSnapshotReply
	err := cm.InstallSnapshot(args, &reply)
	if err != nil {
		t.Fatalf("InstallSnapshot failed: %v", err)
	}

	// Проверить snapshotChan
	select {
	case data := <-snapshotChan:
		if !bytes.Equal(data, []byte("snapshot-data")) {
			t.Fatal("snapshot data mismatch")
		}
	default:
		t.Fatal("expected snapshot data on snapshotChan")
	}

	// Проверить, что commitChanSender не отправляет записей (entries пуст)
	cm.mu.Lock()
	savedLastApplied := cm.lastApplied
	var entries []LogEntry
	if cm.commitIndex > cm.lastApplied {
		offset := cm.lastIncludedIndex + 1
		from := cm.lastApplied + 1 - offset
		to := cm.commitIndex + 1 - offset
		if from < 0 {
			from = 0
		}
		if to > len(cm.log) {
			to = len(cm.log)
		}
		if to > from {
			entries = append([]LogEntry{}, cm.log[from:to]...)
		}
		cm.lastApplied = cm.commitIndex
	}
	cm.mu.Unlock()

	if len(entries) != 0 {
		t.Fatalf("expected 0 entries after InstallSnapshot, got %d", len(entries))
	}
	if cm.lastIncludedIndex != 10 {
		t.Fatalf("expected lastIncludedIndex=10, got %d", cm.lastIncludedIndex)
	}
	if cm.lastApplied != 10 {
		t.Fatalf("expected lastApplied=10, got %d", cm.lastApplied)
	}

	// Добавить 2 записи через AppendEntries, чтобы симулировать новые коммиты
	// (логические индексы 11, 12 при offset=11)
	cm.mu.Lock()
	cm.log = append(cm.log, LogEntry{Command: 11, Term: 1})
	cm.log = append(cm.log, LogEntry{Command: 12, Term: 1})
	cm.commitIndex = 12
	cm.lastApplied = 10
	cm.mu.Unlock()

	// Проверить, что теперь commitChanSender видит 2 записи
	cm.mu.Lock()
	savedLastApplied = cm.lastApplied
	if cm.commitIndex > cm.lastApplied {
		offset := cm.lastIncludedIndex + 1
		from := cm.lastApplied + 1 - offset
		to := cm.commitIndex + 1 - offset
		if from < 0 {
			from = 0
		}
		if to > len(cm.log) {
			to = len(cm.log)
		}
		if to > from {
			entries = append([]LogEntry{}, cm.log[from:to]...)
		}
		cm.lastApplied = cm.commitIndex
	}
	cm.mu.Unlock()

	if len(entries) != 2 {
		t.Fatalf("expected 2 entries after AppendEntries, got %d", len(entries))
	}
	for i, e := range entries {
		expectedIdx := savedLastApplied + i + 1
		if e.Command.(int) != expectedIdx {
			t.Fatalf("entry %d: expected Command=%d, got %v", i, expectedIdx, e.Command)
		}
	}
}

// TestSnapshotInstall проверяет полный цикл InstallSnapshot:
// лидер отправляет снепшот, follower получает и применяет.
func TestSnapshotInstall(t *testing.T) {
	storage := NewMapStorage()
	commitChan := make(chan CommitEntry, 100)
	snapshotChan := make(chan []byte, 1)
	ready := make(chan any)
	close(ready)

	cm := NewConsensusModule(1, []int{2, 3}, nil, storage, ready, commitChan, snapshotChan)

	// Подготовить состояние лидера со снепшотом
	cm.mu.Lock()
	cm.state = Leader
	cm.currentTerm = 1
	for i := 0; i < 10; i++ {
		cm.log = append(cm.log, LogEntry{Command: i, Term: cm.currentTerm})
	}
	cm.commitIndex = 9
	cm.lastApplied = 9
	cm.TakeSnapshot([]byte("leader-snapshot"))
	cm.mu.Unlock()

	// Добавить ещё 2 записи после снепшота (логические индексы 10, 11)
	cm.mu.Lock()
	cm.log = append(cm.log, LogEntry{Command: 10, Term: 1})
	cm.log = append(cm.log, LogEntry{Command: 11, Term: 1})
	cm.commitIndex = 11
	cm.lastApplied = 11
	cm.mu.Unlock()

	// Имитация follower, которому нужен снепшот (nextIndex=0, lastIncludedIndex=0)
	followerStorage := NewMapStorage()
	followerCommitChan := make(chan CommitEntry, 100)
	followerSnapshotChan := make(chan []byte, 1)
	followerReady := make(chan any)
	close(followerReady)

	follower := NewConsensusModule(2, []int{1, 3}, nil, followerStorage, followerReady, followerCommitChan, followerSnapshotChan)

	// Лидер отправляет InstallSnapshot follower'у
	cm.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              1,
		LeaderID:          1,
		LastIncludedIndex: cm.lastIncludedIndex,
		LastIncludedTerm:  cm.lastIncludedTerm,
		Data:              cm.snapshotData,
	}
	cm.mu.Unlock()

	var reply InstallSnapshotReply
	err := follower.InstallSnapshot(args, &reply)
	if err != nil {
		t.Fatalf("InstallSnapshot failed: %v", err)
	}

	// Проверить состояние follower после InstallSnapshot
	if follower.lastIncludedIndex != 9 {
		t.Fatalf("expected follower lastIncludedIndex=9, got %d", follower.lastIncludedIndex)
	}
	if follower.lastIncludedTerm != 1 {
		t.Fatalf("expected follower lastIncludedTerm=1, got %d", follower.lastIncludedTerm)
	}
	if !bytes.Equal(follower.snapshotData, []byte("leader-snapshot")) {
		t.Fatal("follower snapshotData mismatch")
	}
	if follower.commitIndex != 9 {
		t.Fatalf("expected follower commitIndex=9, got %d", follower.commitIndex)
	}
	if follower.lastApplied != 9 {
		t.Fatalf("expected follower lastApplied=9, got %d", follower.lastApplied)
	}
	if len(follower.log) != 0 {
		t.Fatalf("expected follower empty log, got %d entries", len(follower.log))
	}

	// Проверить, что follower получил данные снепшота через snapshotChan
	select {
	case data := <-followerSnapshotChan:
		if !bytes.Equal(data, []byte("leader-snapshot")) {
			t.Fatal("follower snapshot data mismatch")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for snapshot data on follower snapshotChan")
	}

	// Проверить getLogEntry на follower
	_, ok := follower.getLogEntry(5)
	if ok {
		t.Fatal("expected getLogEntry(5) on follower to return false (covered by snapshot)")
	}

	// Добавить запись через AppendEntries (имитация последующих коммитов,
	// логические индексы 10, 11)
	follower.mu.Lock()
	follower.log = append(follower.log, LogEntry{Command: 10, Term: 1})
	follower.log = append(follower.log, LogEntry{Command: 11, Term: 1})
	follower.commitIndex = 11
	follower.mu.Unlock()

	// Проверить, что follower может читать новые записи
	entry, ok := follower.getLogEntry(10)
	if !ok {
		t.Fatal("expected follower getLogEntry(10) to return true")
	}
	if entry.Command != 10 {
		t.Fatalf("expected follower entry.Command=10, got %v", entry.Command)
	}

	entry, ok = follower.getLogEntry(11)
	if !ok {
		t.Fatal("expected follower getLogEntry(11) to return true")
	}
	if entry.Command != 11 {
		t.Fatalf("expected follower entry.Command=11, got %v", entry.Command)
	}
}

// TestInstallSnapshotTerm проверяет обработку InstallSnapshot
// при несовпадении термов.
func TestInstallSnapshotTerm(t *testing.T) {
	storage := NewMapStorage()
	commitChan := make(chan CommitEntry, 100)
	snapshotChan := make(chan []byte, 1)
	ready := make(chan any)
	close(ready)

	cm := NewConsensusModule(1, []int{2, 3}, nil, storage, ready, commitChan, snapshotChan)

	// Случай 1: args.Term < cm.currentTerm — отклоняем
	cm.mu.Lock()
	cm.currentTerm = 5
	cm.mu.Unlock()

	args := InstallSnapshotArgs{
		Term:              3,
		LastIncludedIndex: 10,
		LastIncludedTerm:  3,
		Data:              []byte("data"),
	}
	var reply InstallSnapshotReply
	err := cm.InstallSnapshot(args, &reply)
	if err != nil {
		t.Fatalf("InstallSnapshot failed: %v", err)
	}
	if reply.Term != 5 {
		t.Fatalf("expected reply.Term=5, got %d", reply.Term)
	}
	if cm.lastIncludedIndex != 0 {
		t.Fatalf("expected lastIncludedIndex unchanged (0), got %d", cm.lastIncludedIndex)
	}

	// Случай 2: args.LastIncludedIndex <= cm.lastIncludedIndex — игнорируем
	cm.mu.Lock()
	cm.lastIncludedIndex = 10
	cm.mu.Unlock()

	args2 := InstallSnapshotArgs{
		Term:              5,
		LastIncludedIndex: 8,
		LastIncludedTerm:  3,
		Data:              []byte("data2"),
	}
	err = cm.InstallSnapshot(args2, &reply)
	if err != nil {
		t.Fatalf("InstallSnapshot failed: %v", err)
	}
	if cm.lastIncludedIndex != 10 {
		t.Fatalf("expected lastIncludedIndex unchanged (10), got %d", cm.lastIncludedIndex)
	}
}

// TestSnapshotIgnoreWhenCovered проверяет, что TakeSnapshot
// игнорируется, если lastApplied не продвинулся дальше lastIncludedIndex.
func TestSnapshotIgnoreWhenCovered(t *testing.T) {
	storage := NewMapStorage()
	commitChan := make(chan CommitEntry, 100)
	snapshotChan := make(chan []byte, 1)
	ready := make(chan any)
	close(ready)

	cm := NewConsensusModule(1, []int{2, 3}, nil, storage, ready, commitChan, snapshotChan)

	// Установить lastApplied=5, lastIncludedIndex=5
	cm.mu.Lock()
	cm.lastApplied = 5
	cm.lastIncludedIndex = 5
	cm.mu.Unlock()

	// Вызов TakeSnapshot с lastApplied=5 не должен ничего менять
	cm.mu.Lock()
	cm.TakeSnapshot([]byte("data"))
	cm.mu.Unlock()

	if cm.lastIncludedIndex != 5 {
		t.Fatalf("expected lastIncludedIndex unchanged (5), got %d", cm.lastIncludedIndex)
	}
}

// TestTakeSnapshotNilData проверяет, что TakeSnapshot падает при nil данных.
func TestTakeSnapshotNilData(t *testing.T) {
	storage := NewMapStorage()
	commitChan := make(chan CommitEntry, 100)
	snapshotChan := make(chan []byte, 1)
	ready := make(chan any)
	close(ready)

	cm := NewConsensusModule(1, []int{2, 3}, nil, storage, ready, commitChan, snapshotChan)

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for nil stateMachineData")
		}
	}()

	cm.mu.Lock()
	cm.TakeSnapshot(nil)
	cm.mu.Unlock()
}
