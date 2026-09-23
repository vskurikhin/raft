package raft

import (
	"testing"

	"github.com/fortytw2/leaktest"
	"github.com/vskurikhin/raft/pkg/raft/store"
	"github.com/vskurikhin/raft/pkg/raft/transp"
)

// dropWriteStorage — файловое хранилище с управляемым пропуском записи:
// выбранный скалярный ключ не сохраняется, а операции журнала не выполняют
// ввода-вывода. Приспособление моделирует сбой процесса между двумя вызовами
// Set при сохранении метаданных снимка, не завершая процесс: так проверяется
// наблюдаемый результат окна, а не только его отсутствие.
type dropWriteStorage struct {
	inner         LogStorage
	dropScalarKey string
	dropJournal   bool
}

func (s *dropWriteStorage) Set(key string, value []byte) {
	if key == s.dropScalarKey {
		return
	}
	s.inner.Set(key, value)
}

func (s *dropWriteStorage) Get(key string) ([]byte, bool) { return s.inner.Get(key) }

func (s *dropWriteStorage) HasData() bool { return s.inner.HasData() }

func (s *dropWriteStorage) StoreLogEntries(fromIndex int, entries []LogEntry) LogWriteResult {
	if s.dropJournal {
		return LogWriteResult{}
	}
	return s.inner.StoreLogEntries(fromIndex, entries)
}

func (s *dropWriteStorage) RewriteLog(entries []LogEntry) LogWriteResult {
	if s.dropJournal {
		return LogWriteResult{}
	}
	return s.inner.RewriteLog(entries)
}

func (s *dropWriteStorage) LoadLog() ([]LogEntry, error) { return s.inner.LoadLog() }

// TestAppendEntriesDurableAfterRestart проверяет, что запись, на которую
// ведомый ответил Success, переживает полный рестарт узла: новый экземпляр
// ConsensusModule читает принятый журнал и терм с диска. Локально сохранённая
// запись при этом не выдаётся за зафиксированную протоколом.
func TestAppendEntriesDurableAfterRestart(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	dir := t.TempDir()
	cm, _ := newAEDurabilityCM(dir)

	entries := []LogEntry{{Index: 0, Term: 1, Type: LogCommand, Data: "k0=v0"}}
	var reply AppendEntriesReply
	if err := cm.AppendEntries(aeArgs(-1, -1, -1, entries), &reply); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if !reply.Success {
		t.Fatal("reply.Success = false, want true")
	}
	cm.mu.Lock()
	commitIndex := cm.cmState.commitIndex
	cm.mu.Unlock()
	if commitIndex != -1 {
		t.Fatalf("commitIndex = %d, want -1: сценарий проверяет локальную долговечность без фиксации", commitIndex)
	}

	ready := make(chan any)
	transport := transp.NewInmemTransport("append-durable-restart")
	restarted := NewConsensusModule(0, []int{}, transport, store.NewFileStorage(dir), newSnapshotTestFSM(), ready)

	restarted.mu.Lock()
	gotTerm := restarted.cmState.currentTerm
	gotLog := append([]LogEntry(nil), restarted.cmState.log...)
	restarted.mu.Unlock()

	close(ready)
	restarted.Stop()
	transport.Close()

	if gotTerm != 1 {
		t.Fatalf("currentTerm после рестарта = %d, want 1", gotTerm)
	}
	if len(gotLog) != 1 || gotLog[0].Index != 0 || gotLog[0].Term != 1 {
		t.Fatalf("журнал после рестарта = %+v, want одну запись (индекс 0, терм 1)", gotLog)
	}
}

// TestRestartSnapshotKeyGapFullLog фиксирует ожидаемый результат разрыва
// между двумя вызовами Set снимок-ключей при полном журнале: после записи
// lastSnapshotIndex и до записи lastSnapshotTerm узел стартует с парой
// (lastSnapshotIndex=N, lastSnapshotTerm=-1). Журнал ещё не переписан, поэтому
// проверка согласованности снимок-ключей проходит, а восстановление терма не
// выполняется. Это сохранение поведения HEAD: этап не меняет порядок записи
// скаляров и не вводит нового инварианта корректности.
func TestRestartSnapshotKeyGapFullLog(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	const (
		snapIndex = 3
		snapTerm  = 2
	)
	dir := t.TempDir()
	inner := store.NewFileStorage(dir)
	full := []LogEntry{
		{Index: 0, Term: 0, Type: LogNoop},
		{Index: 1, Term: 1, Type: LogCommand, Data: "k1=v1"},
		{Index: 2, Term: 1, Type: LogCommand, Data: "k2=v2"},
		{Index: 3, Term: 1, Type: LogCommand, Data: "k3=v3"},
	}
	inner.RewriteLog(full)
	inner.Set("currentTerm", gobEncode(t, 1))
	inner.Set("votedFor", gobEncode(t, -1))

	// Сбой после Set(lastSnapshotIndex) и до Set(lastSnapshotTerm): журнал
	// остаётся полным, потому что полная замена идёт последней и тоже не
	// выполняется.
	storage := &dropWriteStorage{
		inner:         inner,
		dropScalarKey: "lastSnapshotTerm",
		dropJournal:   true,
	}
	cm := &ConsensusModule{storage: storage}
	cm.cmState.currentTerm = 1
	cm.cmState.votedFor = -1
	cm.cmState.lastSnapshotIndex = snapIndex
	cm.cmState.lastSnapshotTerm = snapTerm
	cm.cmState.log = []LogEntry{full[len(full)-1]}
	cm.markLogRewriteDirtyLocked()
	cm.mu.Lock()
	cm.persistToStorageLocked(persistSourceTest)
	cm.mu.Unlock()

	if _, found := inner.Get("lastSnapshotTerm"); found {
		t.Fatal("lastSnapshotTerm сохранён: разрыв между двумя Set не воспроизведён")
	}
	raw, found := inner.Get("lastSnapshotIndex")
	if !found {
		t.Fatal("lastSnapshotIndex отсутствует: первая половина пары не записана")
	}
	var storedIndex int
	gobDecode(t, raw, &storedIndex)
	if storedIndex != snapIndex {
		t.Fatalf("lastSnapshotIndex в хранилище = %d, want %d", storedIndex, snapIndex)
	}

	ready := make(chan any)
	transport := transp.NewInmemTransport("snapshot-key-gap")
	restarted := NewConsensusModule(0, []int{}, transport, store.NewFileStorage(dir), newSnapshotTestFSM(), ready)

	restarted.mu.Lock()
	gotIndex := restarted.cmState.lastSnapshotIndex
	gotTerm := restarted.cmState.lastSnapshotTerm
	gotLen := len(restarted.cmState.log)
	restarted.mu.Unlock()

	close(ready)
	restarted.Stop()
	transport.Close()

	if gotIndex != snapIndex {
		t.Fatalf("lastSnapshotIndex после старта = %d, want %d", gotIndex, snapIndex)
	}
	if gotTerm != -1 {
		t.Fatalf("lastSnapshotTerm после старта = %d, want -1 (поведение HEAD при полном журнале)", gotTerm)
	}
	if gotLen != len(full) {
		t.Fatalf("журнал после старта = %d записей, want полный журнал из %d записей", gotLen, len(full))
	}
}

// TestRestartNewSnapshotKeysFullLogKept проверяет безопасное окно порядка
// публикации: скаляры снимка записаны, а журнал ещё не переписан и остаётся
// полным. Узел стартует с новыми метаданными снимка и полным журналом;
// полный журнал консервативнее и не теряет ни одной записи.
func TestRestartNewSnapshotKeysFullLogKept(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	const (
		snapIndex = 3
		snapTerm  = 2
	)
	dir := t.TempDir()
	inner := store.NewFileStorage(dir)
	full := []LogEntry{
		{Index: 0, Term: 0, Type: LogNoop},
		{Index: 1, Term: 1, Type: LogCommand, Data: "k1=v1"},
		{Index: 2, Term: 1, Type: LogCommand, Data: "k2=v2"},
		{Index: 3, Term: 1, Type: LogCommand, Data: "k3=v3"},
	}
	inner.RewriteLog(full)
	inner.Set("currentTerm", gobEncode(t, 1))
	inner.Set("votedFor", gobEncode(t, -1))

	// Сбой после записи обоих снимок-ключей и до полной замены журнала:
	// журнал остаётся прежним полным файлом.
	storage := &dropWriteStorage{inner: inner, dropJournal: true}
	cm := &ConsensusModule{storage: storage}
	cm.cmState.currentTerm = 1
	cm.cmState.votedFor = -1
	cm.cmState.lastSnapshotIndex = snapIndex
	cm.cmState.lastSnapshotTerm = snapTerm
	cm.cmState.log = []LogEntry{full[len(full)-1]}
	cm.markLogRewriteDirtyLocked()
	cm.mu.Lock()
	cm.persistToStorageLocked(persistSourceTest)
	cm.mu.Unlock()

	ready := make(chan any)
	transport := transp.NewInmemTransport("snapshot-new-keys-full-log")
	restarted := NewConsensusModule(0, []int{}, transport, store.NewFileStorage(dir), newSnapshotTestFSM(), ready)

	restarted.mu.Lock()
	gotIndex := restarted.cmState.lastSnapshotIndex
	gotTerm := restarted.cmState.lastSnapshotTerm
	gotLen := len(restarted.cmState.log)
	restarted.mu.Unlock()

	close(ready)
	restarted.Stop()
	transport.Close()

	if gotIndex != snapIndex || gotTerm != snapTerm {
		t.Fatalf("метаданные снимка после старта = (%d, %d), want (%d, %d)",
			gotIndex, gotTerm, snapIndex, snapTerm)
	}
	if gotLen != len(full) {
		t.Fatalf("журнал после старта = %d записей, want полный журнал из %d записей", gotLen, len(full))
	}
}

// TestRepeatedCMRestartNoLeaks проверяет многократное создание, остановку и
// повторное открытие узла на одном каталоге: операции журнала не оставляют
// горутин, а хранилище не удерживает дескриптор между вызовами. Проверка
// утечки выполняется приспособлением leaktest после последней остановки.
func TestRepeatedCMRestartNoLeaks(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	dir := t.TempDir()
	base, _ := newAEDurabilityCM(dir)
	entries := []LogEntry{{Index: 0, Term: 1, Type: LogCommand, Data: "k0=v0"}}
	var reply AppendEntriesReply
	if err := base.AppendEntries(aeArgs(-1, -1, -1, entries), &reply); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}

	for i := 0; i < 5; i++ {
		ready := make(chan any)
		transport := transp.NewInmemTransport("repeated-restart")
		cm := NewConsensusModule(0, []int{}, transport, store.NewFileStorage(dir), newSnapshotTestFSM(), ready)
		close(ready)
		cm.Stop()
		transport.Close()
	}
}
