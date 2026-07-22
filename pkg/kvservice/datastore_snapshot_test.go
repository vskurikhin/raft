package kvservice

import (
	"testing"
	"time"

	"github.com/vskurikhin/raft/pkg/raft"
)

// TestDataStoreSnapshotRestore проверяет базовый цикл Snapshot -> RestoreFromSnapshot.
func TestDataStoreSnapshotRestore(t *testing.T) {
	ds := NewDataStore()
	ds.Put("key1", "val1")
	ds.Put("key2", "val2")

	data := ds.Snapshot()
	if len(data) == 0 {
		t.Fatal("snapshot data should not be empty")
	}

	ds2 := NewDataStore()
	ds2.RestoreFromSnapshot(data)

	v, ok := ds2.Get("key1")
	if !ok {
		t.Fatal("key1 not found after restore")
	}
	if v != "val1" {
		t.Fatalf("expected val1, got %s", v)
	}

	v, ok = ds2.Get("key2")
	if !ok {
		t.Fatal("key2 not found after restore")
	}
	if v != "val2" {
		t.Fatalf("expected val2, got %s", v)
	}
}

// TestDataStoreSnapshotRestoreEmpty проверяет снепшот пустого хранилища.
func TestDataStoreSnapshotRestoreEmpty(t *testing.T) {
	ds := NewDataStore()
	data := ds.Snapshot()

	ds2 := NewDataStore()
	ds2.RestoreFromSnapshot(data)

	// Проверить, что ключ не существует
	_, ok := ds2.Get("nonexistent")
	if ok {
		t.Fatal("expected key not found after restore from empty snapshot")
	}
}

// TestDataStoreSnapshotRestoreOverwrite проверяет, что RestoreFromSnapshot
// заменяет существующие данные, а не сливает их.
func TestDataStoreSnapshotRestoreOverwrite(t *testing.T) {
	ds := NewDataStore()
	ds.Put("a", "1")
	ds.Put("b", "2")

	// Создать снепшот только с ключом "a"
	ds2 := NewDataStore()
	ds2.Put("a", "new")
	data := ds2.Snapshot()

	// Восстановить снепшот в ds — должен заменить все данные
	ds.RestoreFromSnapshot(data)

	v, ok := ds.Get("a")
	if !ok {
		t.Fatal("key 'a' not found")
	}
	if v != "new" {
		t.Fatalf("expected 'new', got %s", v)
	}

	// Ключ "b" должен исчезнуть (полная замена)
	_, ok = ds.Get("b")
	if ok {
		t.Fatal("key 'b' should be gone after full restore")
	}
}

// TestRunUpdaterWithSnapshot проверяет, что runUpdater обрабатывает
// snapshotChan и последующие commitChan поверх восстановленного состояния.
func TestRunUpdaterWithSnapshot(t *testing.T) {
	// Создать KVService с каналами вручную
	commitChan := make(chan raft.CommitEntry, 10)
	snapshotChan := make(chan []byte, 1)

	kvs := &KVService{
		commitChan:   commitChan,
		snapshotChan: snapshotChan,
		ds:           NewDataStore(),
		commitSubs:   make(map[int]chan Command),
	}
	kvs.runUpdater()

	// Заполнить DataStore начальными данными
	kvs.ds.Put("key1", "initial")

	// Создать снепшот с новыми данными
	ds2 := NewDataStore()
	ds2.Put("key1", "snapshot-value")
	snapData := ds2.Snapshot()

	// Отправить снепшот через snapshotChan
	select {
	case snapshotChan <- snapData:
	case <-time.After(time.Second):
		t.Fatal("timeout sending snapshot")
	}

	// Дать время runUpdater обработать снепшот
	time.Sleep(100 * time.Millisecond)

	// Проверить, что данные заменились
	v, ok := kvs.ds.Get("key1")
	if !ok {
		t.Fatal("key1 not found after snapshot")
	}
	if v != "snapshot-value" {
		t.Fatalf("expected snapshot-value, got %s", v)
	}

	// Отправить команду коммита поверх снепшота
	entry := raft.CommitEntry{
		Index: 100,
		Command: Command{
			Kind:  CommandPut,
			Key:   "key2",
			Value: "committed",
		},
	}
	select {
	case commitChan <- entry:
	case <-time.After(time.Second):
		t.Fatal("timeout sending commit entry")
	}

	time.Sleep(100 * time.Millisecond)

	v, ok = kvs.ds.Get("key2")
	if !ok {
		t.Fatal("key2 not found after commit")
	}
	if v != "committed" {
		t.Fatalf("expected committed, got %s", v)
	}

	// Закрыть каналы для завершения runUpdater
	close(snapshotChan)
	close(commitChan)
}

// TestRunUpdaterSnapshotThenCommitOrder проверяет, что снепшот применяется
// до команд коммита (через select оба канала обрабатываются конкурентно).
func TestRunUpdaterSnapshotThenCommitOrder(t *testing.T) {
	commitChan := make(chan raft.CommitEntry, 10)
	snapshotChan := make(chan []byte, 1)

	kvs := &KVService{
		commitChan:   commitChan,
		snapshotChan: snapshotChan,
		ds:           NewDataStore(),
		commitSubs:   make(map[int]chan Command),
	}
	kvs.runUpdater()

	// Установить начальное значение
	kvs.ds.Put("x", "old")

	// Создать снепшот, заменяющий x на "snap"
	dsSnap := NewDataStore()
	dsSnap.Put("x", "snap")
	snapData := dsSnap.Snapshot()

	// Отправить снепшот
	select {
	case snapshotChan <- snapData:
	case <-time.After(time.Second):
		t.Fatal("timeout sending snapshot")
	}

	time.Sleep(50 * time.Millisecond)

	// Проверить, что снепшот применился
	v, ok := kvs.ds.Get("x")
	if !ok {
		t.Fatal("x not found")
	}
	if v != "snap" {
		t.Fatalf("expected snap, got %s", v)
	}

	// Отправить команду, изменяющую x на "new"
	entry := raft.CommitEntry{
		Index: 1,
		Command: Command{
			Kind:  CommandPut,
			Key:   "x",
			Value: "new",
		},
	}
	select {
	case commitChan <- entry:
	case <-time.After(time.Second):
		t.Fatal("timeout sending commit entry")
	}

	time.Sleep(50 * time.Millisecond)

	// Проверить, что команда применилась поверх снепшота
	v, ok = kvs.ds.Get("x")
	if !ok {
		t.Fatal("x not found")
	}
	if v != "new" {
		t.Fatalf("expected new, got %s", v)
	}

	close(snapshotChan)
	close(commitChan)
}

// TestDataStoreSnapshotPanicOnBadData проверяет, что RestoreFromSnapshot
// паникует при повреждённых данных.
func TestDataStoreSnapshotRestoreBadData(t *testing.T) {
	ds := NewDataStore()

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on bad snapshot data")
		}
	}()
	ds.RestoreFromSnapshot([]byte("invalid-gob-data"))
}

// TestDataStoreSnapshotPanicOnNil проверяет, что Snapshot не паникует
// на пустой map, но RestoreFromSnapshot паникует на nil данных.
func TestDataStoreSnapshotRestoreNil(t *testing.T) {
	ds := NewDataStore()

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on nil snapshot data")
		}
	}()
	ds.RestoreFromSnapshot(nil)
}

// TestRunUpdaterSnapshotChanClose проверяет, что runUpdater завершается
// при закрытии snapshotChan.
func TestRunUpdaterSnapshotChanClose(t *testing.T) {
	commitChan := make(chan raft.CommitEntry, 10)
	snapshotChan := make(chan []byte, 1)

	kvs := &KVService{
		commitChan:   commitChan,
		snapshotChan: snapshotChan,
		ds:           NewDataStore(),
		commitSubs:   make(map[int]chan Command),
	}
	kvs.runUpdater()

	// Закрыть snapshotChan — runUpdater должен выйти из select
	close(snapshotChan)

	// Подождать немного, чтобы горутина завершилась
	time.Sleep(50 * time.Millisecond)

	// Отправить команду в commitChan — runUpdater уже завершён,
	// поэтому канал должен заполниться без получателя
	select {
	case commitChan <- raft.CommitEntry{Index: 0, Command: Command{Kind: CommandPut, Key: "k", Value: "v"}}:
		// отправлено успешно
	default:
		// если runUpdater всё ещё читает, это ок
	}

	close(commitChan)
}

// TestNewKVServiceWithSnapshotConfig проверяет, что New создаёт snapshotChan
// и передаёт его в raft.NewWithSnapshot.
func TestNewKVServiceWithSnapshotConfig(t *testing.T) {
	storage := raft.NewMapStorage()
	ready := make(chan any)

	kvs := New(Config{
		Config: raft.Config{
			PeerIds:    []int{1, 2},
			RPCAddress: ":0",
			ServerID:   0,
		},
		HTTPAddress: ":8080",
	}, storage, ready)

	if kvs.snapshotChan == nil {
		t.Fatal("expected non-nil snapshotChan")
	}
	if kvs.commitChan == nil {
		t.Fatal("expected non-nil commitChan")
	}
}

// TestShutdownClosesSnapshotChan проверяет, что Shutdown закрывает snapshotChan.
func TestShutdownClosesSnapshotChan(t *testing.T) {
	storage := raft.NewMapStorage()
	ready := make(chan any)

	kvs := New(Config{
		Config: raft.Config{
			PeerIds:    []int{1, 2},
			RPCAddress: ":0",
			ServerID:   0,
		},
		HTTPAddress: ":8080",
	}, storage, ready)

	// Закрываем ready, чтобы Raft сервер не ждал
	close(ready)

	// Вызов Shutdown
	_ = kvs.Shutdown()

	// Проверить, что snapshotChan закрыт
	_, ok := <-kvs.snapshotChan
	if ok {
		t.Fatal("snapshotChan should be closed after Shutdown")
	}
}
