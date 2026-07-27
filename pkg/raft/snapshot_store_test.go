package raft

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
)

// TestInmemSnapshotStore_CreateAndList проверяет создание снэпшота и получение
// списка. Create должен перезаписывать предыдущий снэпшот.
func TestInmemSnapshotStore_CreateAndList(t *testing.T) {
	defer leaktest.CheckTimeout(t, time.Second)()

	store := NewInmemSnapshotStore()

	// Создаём первый снэпшот с Index=10, Term=3.
	cfg := Configuration{
		ConfigServers: []ConfigServer{
			{ID: 1, Address: "node1", Suffrage: Voter},
			{ID: 2, Address: "node2", Suffrage: Voter},
		},
	}
	sink, err := store.Create(10, 3, cfg, 5)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if _, err := sink.Write([]byte("snap data")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	_ = sink.Close()

	// List должен вернуть один снэпшот с правильными метаданными.
	snapshots, err := store.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(snapshots) != 1 {
		t.Fatalf("List() = %d snapshots, want 1", len(snapshots))
	}
	if snapshots[0].Index != 10 {
		t.Fatalf("Index = %d, want 10", snapshots[0].Index)
	}
	if snapshots[0].Term != 3 {
		t.Fatalf("Term = %d, want 3", snapshots[0].Term)
	}
	if snapshots[0].ConfigIndex != 5 {
		t.Fatalf("ConfigIndex = %d, want 5", snapshots[0].ConfigIndex)
	}

	// Создаём второй снэпшот — должен перезаписать первый.
	sink2, err := store.Create(20, 4, Configuration{}, 10)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	_ = sink2.Close()

	snapshots, err = store.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(snapshots) != 1 {
		t.Fatalf("List() = %d snapshots, want 1", len(snapshots))
	}
	if snapshots[0].Index != 20 {
		t.Fatalf("Index = %d, want 20", snapshots[0].Index)
	}
	if snapshots[0].Term != 4 {
		t.Fatalf("Term = %d, want 4", snapshots[0].Term)
	}
}

// TestInmemSnapshotStore_Open проверяет открытие снэпшота по ID.
func TestInmemSnapshotStore_Open(t *testing.T) {
	defer leaktest.CheckTimeout(t, time.Second)()

	store := NewInmemSnapshotStore()
	sink, err := store.Create(10, 3, Configuration{}, 0)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if _, err := sink.Write([]byte("test data")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	_ = sink.Close()

	// Открываем по ID — проверяем метаданные и данные.
	meta, reader, err := store.Open(sink.ID())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if meta.Index != 10 {
		t.Fatalf("Index = %d, want 10", meta.Index)
	}
	if meta.Term != 3 {
		t.Fatalf("Term = %d, want 3", meta.Term)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if string(data) != "test data" {
		t.Fatalf("data = %q, want %q", string(data), "test data")
	}
	_ = reader.Close()

	// Открытие несуществующего снэпшота — ошибка.
	if _, _, err := store.Open("nonexistent"); err == nil {
		t.Fatal("Open nonexistent: want error, got nil")
	}
}

// TestInmemSnapshotStore_ListEmpty проверяет, что пустой store возвращает nil.
func TestInmemSnapshotStore_ListEmpty(t *testing.T) {
	defer leaktest.CheckTimeout(t, time.Second)()

	store := NewInmemSnapshotStore()
	snapshots, err := store.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(snapshots) != 0 {
		t.Fatalf("List() = %d snapshots, want 0", len(snapshots))
	}
}

// TestInmemSnapshotSink_WriteCloseCancel проверяет базовые операции SnapshotSink.
func TestInmemSnapshotSink_WriteCloseCancel(t *testing.T) {
	defer leaktest.CheckTimeout(t, time.Second)()

	store := NewInmemSnapshotStore()
	sink, err := store.Create(5, 2, Configuration{}, 0)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Write — проверяем, что Size обновляется.
	n, err := sink.Write([]byte("hello"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != 5 {
		t.Fatalf("Write returned %d, want 5", n)
	}

	// Close — без ошибки.
	if err := sink.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// ID — непустой.
	if sink.ID() == "" {
		t.Fatal("ID is empty")
	}

	// Cancel — без ошибки.
	if err := sink.Cancel(); err != nil {
		t.Fatalf("Cancel failed: %v", err)
	}
}

// TestInmemSnapshotStore_ListDoesNotMutate проверяет, что List возвращает
// копию метаданных, а не указатель на внутренние данные.
func TestInmemSnapshotStore_ListDoesNotMutate(t *testing.T) {
	defer leaktest.CheckTimeout(t, time.Second)()

	store := NewInmemSnapshotStore()
	sink, err := store.Create(10, 3, Configuration{}, 5)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	_ = sink.Close()

	snapshots, _ := store.List()
	snapshots[0].Index = 999

	// Повторный List должен вернуть оригинальные данные.
	snapshots, _ = store.List()
	if snapshots[0].Index != 10 {
		t.Fatalf("mutated: Index = %d, want 10", snapshots[0].Index)
	}
}

// TestInmemSnapshotStore_OpenReturnsCopy проверяет, что Open возвращает
// копию данных, а не ссылку на внутренний буфер.
func TestInmemSnapshotStore_OpenReturnsCopy(t *testing.T) {
	defer leaktest.CheckTimeout(t, time.Second)()

	store := NewInmemSnapshotStore()
	sink, err := store.Create(10, 3, Configuration{}, 0)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	_, _ = sink.Write([]byte("original"))
	_ = sink.Close()

	// Читаем и модифицируем.
	_, reader, _ := store.Open(sink.ID())
	data, _ := io.ReadAll(reader)
	_ = reader.Close()
	copy(data, bytes.Repeat([]byte("X"), len(data)))

	// Повторное открытие должно вернуть оригинал.
	_, reader, _ = store.Open(sink.ID())
	data, _ = io.ReadAll(reader)
	_ = reader.Close()
	if string(data) != "original" {
		t.Fatalf("data = %q, want %q", string(data), "original")
	}
}
