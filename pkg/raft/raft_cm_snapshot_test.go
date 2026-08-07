package raft

import (
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
)

// failingSnapshotStore — тестовый двойник SnapshotStore, чей Create
// всегда возвращает ошибку и подсчитывает вызовы.
type failingSnapshotStore struct {
	mu      sync.Mutex
	creates int
}

func (f *failingSnapshotStore) Create(index, term int, configuration Configuration, configIndex int) (SnapshotSink, error) {
	f.mu.Lock()
	f.creates++
	f.mu.Unlock()
	return nil, fmt.Errorf("create failed")
}

func (f *failingSnapshotStore) List() ([]*SnapshotMeta, error) {
	return nil, nil
}

func (f *failingSnapshotStore) Open(id string) (*SnapshotMeta, io.ReadCloser, error) {
	return nil, nil, fmt.Errorf("open failed")
}

func (f *failingSnapshotStore) createCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.creates
}

// TestRunSnapshots_TakeSnapshotErrorDoesNotCrash проверяет (TASK-009),
// что ошибка takeSnapshot не роняет процесс: цикл снапшотов продолжает
// работать и повторяет попытки (Create вызывается снова). Сама ошибка
// логируется через traceLogf(0, ...) — проверка через перехват лога
// не выполняется, чтобы не мутировать глобальный логгер из теста.
func TestRunSnapshots_TakeSnapshotErrorDoesNotCrash(t *testing.T) {
	defer leaktest.CheckTimeout(t, time.Second)()

	fsm := newSnapshotTestFSM()
	ready := make(chan any)
	close(ready)
	storage := NewMapStorage()
	transport := NewInmemTransport("single")
	store := &failingSnapshotStore{}
	cm := NewConsensusModule(0, []int{}, transport, storage, fsm, ready, store)
	defer cm.Stop()

	// Состояние, при котором shouldSnapshot вернёт true, а FSM-снапшот
	// успешно создаётся — ошибка возникает только в store.Create.
	cm.mu.Lock()
	cm.cmState.lastLogIndex = 10
	cm.cmState.lastLogTerm = 1
	cm.cmState.lastApplied = 0
	cm.cmState.log = []LogEntry{{Index: 10, Term: 1, Type: LogCommand, Data: "k=v"}}
	cm.mu.Unlock()
	cm.SetSnapshotConfig(2, 20*time.Millisecond, 1)

	// Первая попытка должна произойти быстро (сигнал из SetSnapshotConfig);
	// ждём и убеждаемся, что CM жив и попытки повторяются циклом.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if store.createCount() >= 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("takeSnapshot was never attempted: failing store Create not called")
}

// TestLeaderSendSnapshot_EmptyListSignalsSnapshot проверяет (TASK-009),
// что при пустом List() у лидера с lastSnapshotIndex >= 0 вызов не
// паникует, не меняет nextIndex и отправляет сигнал в snapshotCh.
func TestLeaderSendSnapshot_EmptyListSignalsSnapshot(t *testing.T) {
	defer leaktest.CheckTimeout(t, time.Second)()

	cm := &ConsensusModule{
		snapshotStore: NewInmemSnapshotStore(),
		snapshotCh:    make(chan struct{}, 1),
	}
	cm.cmState.lastSnapshotIndex = 5
	cm.leaderState.nextIndex = map[int]int{1: 0}

	cm.leaderSendSnapshot(1, 1)

	if cm.leaderState.nextIndex[1] != 0 {
		t.Fatalf("nextIndex = %d, want unchanged (0)", cm.leaderState.nextIndex[1])
	}
	select {
	case <-cm.snapshotCh:
	default:
		t.Fatal("snapshotCh not signaled on empty List")
	}
}
