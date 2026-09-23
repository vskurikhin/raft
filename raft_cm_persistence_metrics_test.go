package raft

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/vskurikhin/raft/pkg/raft/store"
)

// -- Тесты матрицы сохранений CM, диагностического снимка хранилища и
// -- документа PersistV2. Источники в этих тестах задаются напрямую: здесь
// -- проверяются арифметика, единицы и форма публикации, а не переходы
// -- (переходы покрыты отдельным файлом). --

// persistenceSnapshotOf снимает копию матрицы сохранений под cm.mu — тем же
// контрактом, что и секундный отчёт.
func persistenceSnapshotOf(cm *ConsensusModule) persistenceSnapshot {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.persistence.snapshot()
}

// persistenceCellOf возвращает ячейку источника из снятого снимка.
func persistenceCellOf(s persistenceSnapshot, source persistSource) persistCell {
	return s.cells[source]
}

// TestPersistenceMetrics_MatrixAndDocument проверяет арифметику матрицы и
// документа PersistV2 на детерминированных значениях: равенство
// N = N_log + N_scalar = сумме источников, суммы длительностей, размеры
// журнального сохранения, фиксированный порядок одиннадцати источников и
// исключение тестового источника из публикации.
func TestPersistenceMetrics_MatrixAndDocument(t *testing.T) {
	cm := new(ConsensusModule)
	cm.persistence.observe(persistSourceApply, true, 3*time.Millisecond, 100, 2048, 1)
	cm.persistence.observe(persistSourceApply, false, time.Millisecond, 0, 0, 0)
	cm.persistence.observe(persistSourceVote, false, 2*time.Millisecond, 0, 0, 0)
	// Прямой вызов приспособления учитывается отдельной ячейкой и не должен
	// попадать ни в матрицу, ни в общие суммы.
	cm.persistence.observe(persistSourceTest, true, time.Second, 999, 999999, 1)

	snap := persistenceSnapshotOf(cm)

	apply := persistenceCellOf(snap, persistSourceApply)
	if apply.logCalls != 1 || apply.logElapsedNS != int64(3*time.Millisecond) {
		t.Fatalf("apply log = (%d, %d), want (1, %d)", apply.logCalls, apply.logElapsedNS, int64(3*time.Millisecond))
	}
	if apply.scalarCalls != 1 || apply.scalarElapsedNS != int64(time.Millisecond) {
		t.Fatalf("apply scalar = (%d, %d), want (1, %d)", apply.scalarCalls, apply.scalarElapsedNS, int64(time.Millisecond))
	}
	vote := persistenceCellOf(snap, persistSourceVote)
	if vote.scalarCalls != 1 || vote.scalarElapsedNS != int64(2*time.Millisecond) {
		t.Fatalf("vote scalar = (%d, %d), want (1, %d)", vote.scalarCalls, vote.scalarElapsedNS, int64(2*time.Millisecond))
	}

	doc := snap.document()
	if doc.NPersist != doc.NLogPersist+doc.NScalarOnly {
		t.Fatalf("N = %d, N_log + N_scalar = %d + %d", doc.NPersist, doc.NLogPersist, doc.NScalarOnly)
	}
	if doc.NPersist != 3 || doc.NLogPersist != 1 || doc.NScalarOnly != 2 {
		t.Fatalf("итоги = (N=%d, N_log=%d, N_scalar=%d), want (3, 1, 2)",
			doc.NPersist, doc.NLogPersist, doc.NScalarOnly)
	}
	if doc.ElapsedNs != int64(6*time.Millisecond) {
		t.Fatalf("ElapsedNs = %d, want %d", doc.ElapsedNs, int64(6*time.Millisecond))
	}
	if doc.LogElapsedNs != int64(3*time.Millisecond) || doc.ScalarElapsedNs != int64(3*time.Millisecond) {
		t.Fatalf("длительности режимов = (%d, %d), want (3ms, 3ms)", doc.LogElapsedNs, doc.ScalarElapsedNs)
	}
	if doc.ElapsedNs != doc.LogElapsedNs+doc.ScalarElapsedNs {
		t.Fatalf("ElapsedNs = %d, N_log + N_scalar по времени = %d", doc.ElapsedNs, doc.LogElapsedNs+doc.ScalarElapsedNs)
	}

	// Полное наблюдение одно: len(log) = 100, готовые байты кодирования 2048.
	if doc.LogLenSum != 100 || doc.LogBytesWrittenSum != 2048 {
		t.Fatalf("полное сохранение = (len sum %d, bytes sum %d), want (100, 2048)", doc.LogLenSum, doc.LogBytesWrittenSum)
	}
	if doc.LogLenMin == nil || doc.LogLenMax == nil {
		t.Fatal("LogLenMin/Max = null при наличии полного сохранения")
	}
	if *doc.LogLenMin != 100 || *doc.LogLenMax != 100 {
		t.Fatalf("LogLenMin/Max = (%d, %d), want (100, 100)", *doc.LogLenMin, *doc.LogLenMax)
	}

	// Порядок источников зафиксирован порядком кодов: позиция совпадает
	// с объявленным порядком.
	if len(doc.Sources) != persistSourceCount {
		t.Fatalf("источников %d, want %d", len(doc.Sources), persistSourceCount)
	}
	var sourceSum int64
	for i, name := range persistSourceNames {
		if doc.Sources[i].Source != name {
			t.Fatalf("источник %d = %q, want %q", i, doc.Sources[i].Source, name)
		}
		sourceSum += doc.Sources[i].LogCalls + doc.Sources[i].ScalarCalls
	}
	if sourceSum != doc.NPersist {
		t.Fatalf("сумма источников = %d, N = %d", sourceSum, doc.NPersist)
	}

	// Документ сериализуется и разбирается без потерь: все поля матрицы
	// машиночитаемы.
	data, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("json.Marshal матрицы: %v", err)
	}
	var decoded statsPersistenceV2
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal матрицы: %v", err)
	}
	if decoded.NPersist != doc.NPersist || decoded.LogLenSum != doc.LogLenSum || decoded.LogBytesWrittenSum != doc.LogBytesWrittenSum {
		t.Fatalf("round-trip потерял поля: %+v -> %+v", *doc, decoded)
	}
	if len(decoded.Sources) != persistSourceCount || decoded.Sources[0].Source != "apply" {
		t.Fatalf("round-trip источников: %+v", decoded.Sources)
	}
}

// TestPersistenceMetrics_NoFullObservationHasNullMinMax проверяет, что
// отсутствие полного сохранения не выдаётся за минимум ноль: min/max
// публикуются как null в JSON, а суммы остаются нулевыми.
func TestPersistenceMetrics_NoFullObservationHasNullMinMax(t *testing.T) {
	cm := new(ConsensusModule)
	cm.persistence.observe(persistSourceVote, false, time.Millisecond, 0, 0, 0)

	snap := persistenceSnapshotOf(cm)
	doc := snap.document()
	if doc.NLogPersist != 0 || doc.LogLenSum != 0 || doc.LogBytesWrittenSum != 0 {
		t.Fatalf("скалярный вызов дал полные величины: N_log=%d, len sum=%d, bytes sum=%d",
			doc.NLogPersist, doc.LogLenSum, doc.LogBytesWrittenSum)
	}
	if doc.LogLenMin != nil || doc.LogLenMax != nil {
		t.Fatal("LogLenMin/Max не равны null при отсутствии полных сохранений")
	}
	data, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	if !strings.Contains(string(data), `"LogLenMin":null`) || !strings.Contains(string(data), `"LogLenMax":null`) {
		t.Fatalf("JSON не различает отсутствие наблюдения: %s", data)
	}
}

// TestPersistenceMetrics_ObserveZeroAllocations проверяет, что обновление
// счётчиков на горячем пути не выделяет память: наблюдение — обычные
// инкременты полей под уже удерживаемым мьютексом.
func TestPersistenceMetrics_ObserveZeroAllocations(t *testing.T) {
	var metrics persistenceMetrics
	allocs := testing.AllocsPerRun(1000, func() {
		metrics.observe(persistSourceApply, false, time.Microsecond, 0, 0, 0)
	})
	if allocs != 0 {
		t.Fatalf("аллокаций на инкремент = %v, want 0", allocs)
	}

	allocs = testing.AllocsPerRun(1000, func() {
		metrics.observe(persistSourceApply, true, time.Microsecond, 7, 128, 1)
	})
	if allocs != 0 {
		t.Fatalf("аллокаций на полный инкремент = %v, want 0", allocs)
	}
}

// TestPersistenceSnapshot_NoSharedState проверяет, что снимок матрицы не
// разделяет изменяемую память с CM: последующие наблюдения не меняют ни
// снятую копию, ни уже собранный из неё документ.
func TestPersistenceSnapshot_NoSharedState(t *testing.T) {
	cm := new(ConsensusModule)
	cm.persistence.observe(persistSourceApply, true, time.Millisecond, 5, 64, 1)

	before := persistenceSnapshotOf(cm)
	cm.persistence.observe(persistSourceApply, true, time.Millisecond, 50, 640, 1)

	doc := before.document()
	if doc.NPersist != 1 || doc.LogLenSum != 5 || doc.NLogPersist != 1 {
		t.Fatalf("снимок изменился вместе с CM: N=%d, len sum=%d, N_log=%d", doc.NPersist, doc.LogLenSum, doc.NLogPersist)
	}
	if doc.LogLenMin == nil || *doc.LogLenMin != 5 || *doc.LogLenMax != 5 {
		t.Fatalf("снимок потерял исходные размеры: %v..%v", doc.LogLenMin, doc.LogLenMax)
	}
}

// writeCountOnlyStorage — двойник без сумм Sync: реализует только подсчёт
// фактических записей, как сторонняя реализация хранилища.
type writeCountOnlyStorage struct {
	LogStorage
	writes int
}

func (s *writeCountOnlyStorage) WriteCount() int { return s.writes }

// TestStorageDiagnostics_AvailabilityGroups проверяет различение полной
// диагностики, частичной (только записи) и недоступной: недоступность
// публикуется признаком, а не нулевыми значениями.
func TestStorageDiagnostics_AvailabilityGroups(t *testing.T) {
	if d := takeStorageDiagnostics(nil); d.available || d.group() != _statsGroupUnavailable {
		t.Fatalf("nil-хранилище: available=%t, group=%q", d.available, d.group())
	}
	if d := takeStorageDiagnostics(store.NewMapStorage()); d.available || d.group() != _statsGroupUnavailable {
		t.Fatalf("MapStorage без возможностей: available=%t, group=%q", d.available, d.group())
	}

	partial := &writeCountOnlyStorage{writes: 7}
	d := takeStorageDiagnostics(partial)
	if !d.available || d.group() != _statsStorageWriteCount {
		t.Fatalf("частичная диагностика: available=%t, group=%q", d.available, d.group())
	}
	if !d.writesOK || d.writes != 7 || d.fileSyncOK || d.dirSyncOK {
		t.Fatalf("частичный снимок = %+v, want только записи", d)
	}
	if d.at <= 0 {
		t.Fatal("момент снимка частичной диагностики не снят")
	}

	fs := store.NewFileStorage(t.TempDir())
	fs.Set("currentTerm", []byte{1, 2, 3})
	d = takeStorageDiagnostics(fs)
	if !d.available || d.group() != _statsStorageFull {
		t.Fatalf("полная диагностика: available=%t, group=%q", d.available, d.group())
	}
	if d.writes != 1 || !d.fileSyncOK || !d.dirSyncOK || d.fileSyncNS <= 0 || d.dirSyncNS <= 0 {
		t.Fatalf("полный снимок = %+v, want записи и обе суммы", d)
	}
}

// TestPublishStats_PersistenceAndStorageDocuments проверяет третью строку
// отчёта: группа сохранений доступна и несёт матрицу, диагностика FileStorage
// публикует точные записи и суммы Sync, а недоступное хранилище отличается
// от нулевого. Момент снимка Storage отделён от момента снимка CM.
func TestPublishStats_PersistenceAndStorageDocuments(t *testing.T) {
	storage := store.NewFileStorage(t.TempDir())
	cm := newStatsTestCM()
	cm.storage = storage
	storage.Set("currentTerm", []byte{1})
	cm.persistence.observe(persistSourceApply, true, 2*time.Millisecond, 7, 128, 1)

	var out, diag bytes.Buffer
	cm.publishStats(&out, &diag)
	doc := statsDecodePersistV2(t, statsThirdLineBody(t, out.String()))

	if doc.Persistence != _statsGroupAvailable || doc.Persist == nil {
		t.Fatalf("Persistence = %q, Persist = %v, want ok и матрицу", doc.Persistence, doc.Persist)
	}
	if doc.Persist.NPersist != 1 || doc.Persist.NLogPersist != 1 {
		t.Fatalf("матрица в документе: N=%d, N_log=%d, want (1, 1)", doc.Persist.NPersist, doc.Persist.NLogPersist)
	}
	if doc.Storage != _statsStorageFull {
		t.Fatalf("Storage = %q, want %q", doc.Storage, _statsStorageFull)
	}
	// StorageWrites — циклы сохранения: скалярные записи ключей хранилища
	// плюс операция журнала, наблюдённая CM.
	wantWrites := int64(storage.WriteCount()) + doc.Persist.LogWrites
	if doc.StorageWrites == nil || *doc.StorageWrites != wantWrites {
		t.Fatalf("StorageWrites = %v, want %d (WriteCount %d + LogWrites %d)",
			doc.StorageWrites, wantWrites, storage.WriteCount(), doc.Persist.LogWrites)
	}
	if doc.StorageFileSyncNs == nil || doc.StorageDirSyncNs == nil {
		t.Fatalf("суммы Sync = (%v, %v), want обе опубликованы", doc.StorageFileSyncNs, doc.StorageDirSyncNs)
	}
	if doc.StorageSnapshotNs == nil {
		t.Fatal("момент снимка Storage не опубликован при доступной возможности")
	}
	if *doc.StorageSnapshotNs < doc.CmSnapshotNs {
		t.Fatalf("момент Storage = %d раньше момента CM = %d", *doc.StorageSnapshotNs, doc.CmSnapshotNs)
	}
	if doc.Dirty != _statsGroupAvailable || doc.DirtyPeriods == nil {
		t.Fatalf("Dirty = %q, DirtyPeriods = %v, want ok и документ", doc.Dirty, doc.DirtyPeriods)
	}

	// Хранилище без диагностических возможностей: признак unavailable,
	// числовые поля отсутствуют, а не равны нулю.
	cmNo := newStatsTestCM()
	var outNo bytes.Buffer
	cmNo.publishStats(&outNo, io.Discard)
	docNo := statsDecodePersistV2(t, statsThirdLineBody(t, outNo.String()))
	if docNo.Storage != _statsGroupUnavailable {
		t.Fatalf("Storage без возможностей = %q, want %q", docNo.Storage, _statsGroupUnavailable)
	}
	if docNo.StorageWrites != nil || docNo.StorageFileSyncNs != nil || docNo.StorageDirSyncNs != nil || docNo.StorageSnapshotNs != nil {
		t.Fatalf("недоступная диагностика опубликована значениями: %+v", docNo)
	}

	// Частичная возможность: только число записей, суммы отсутствуют.
	cmPart := newStatsTestCM()
	cmPart.storage = &writeCountOnlyStorage{writes: 3}
	var outPart bytes.Buffer
	cmPart.publishStats(&outPart, io.Discard)
	docPart := statsDecodePersistV2(t, statsThirdLineBody(t, outPart.String()))
	if docPart.Storage != _statsStorageWriteCount {
		t.Fatalf("Storage частичной возможности = %q, want %q", docPart.Storage, _statsStorageWriteCount)
	}
	if docPart.StorageWrites == nil || *docPart.StorageWrites != 3 {
		t.Fatalf("StorageWrites = %v, want 3", docPart.StorageWrites)
	}
	if docPart.StorageFileSyncNs != nil || docPart.StorageDirSyncNs != nil {
		t.Fatalf("частичная возможность опубликовала суммы Sync: %+v", docPart)
	}
}

// probePersistenceStats — диагностическая возможность-зонд: при вызове
// проверяет, что cm.mu свободен. Проверка детерминирована: публикация
// вызывает возможность синхронно из горутины теста.
type probePersistenceStats struct {
	LogStorage
	cm       *ConsensusModule
	violated bool
}

func (p *probePersistenceStats) PersistenceStats() (writes int, fileSyncNS int64, dirSyncNS int64) {
	if !p.cm.mu.TryLock() {
		p.violated = true
		return 0, 0, 0
	}
	p.cm.mu.Unlock()
	return 1, 2, 3
}

// TestPublishStats_StorageSnapshotOutsideCmMu проверяет порядок блокировок:
// диагностический снимок хранилища снимается после освобождения cm.mu.
// Зонд обязан получить свободный cm.mu; цикл fs.mu → cm.mu не появляется.
func TestPublishStats_StorageSnapshotOutsideCmMu(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	cm := newStatsTestCM()
	probe := &probePersistenceStats{cm: cm}
	cm.storage = probe

	var out bytes.Buffer
	cm.publishStats(&out, io.Discard)

	if probe.violated {
		t.Fatal("диагностический снимок хранилища снят при удержанном cm.mu")
	}
	doc := statsDecodePersistV2(t, statsThirdLineBody(t, out.String()))
	if doc.StorageWrites == nil || *doc.StorageWrites != 1 {
		t.Fatalf("публикация не использовала согласованный снимок возможности: %v", doc.StorageWrites)
	}
	if doc.StorageFileSyncNs == nil || *doc.StorageFileSyncNs != 2 || doc.StorageDirSyncNs == nil || *doc.StorageDirSyncNs != 3 {
		t.Fatalf("суммы зонда = (%v, %v), want (2, 3)", doc.StorageFileSyncNs, doc.StorageDirSyncNs)
	}
}

// TestPersistenceCounters_DisabledStatsOutputStillRecords проверяет, что
// выключенный вывод не влияет на наблюдения: завершённое сохранение
// увеличивает матрицу, фактические записи и суммы Sync, а публикация
// не пишет ни байта.
func TestPersistenceCounters_DisabledStatsOutputStillRecords(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	storage := store.NewFileStorage(t.TempDir())
	cm := &ConsensusModule{storage: storage, disableStatsOutput: true}
	cm.cmState.currentTerm = 1
	cm.cmState.votedFor = -1
	cm.cmState.lastSnapshotIndex = -1
	cm.cmState.lastSnapshotTerm = -1
	cm.cmState.log = []LogEntry{{Index: 0, Term: 1}}
	cm.markLogRewriteDirtyLocked()

	cm.mu.Lock()
	cm.persistToStorageLocked(persistSourceApply)
	cm.mu.Unlock()

	after := persistenceSnapshotOf(cm)
	cell := persistenceCellOf(after, persistSourceApply)
	if cell.logCalls != 1 {
		t.Fatalf("apply log = %d, want 1 при выключенном выводе", cell.logCalls)
	}
	if got := storage.WriteCount(); got != 4 {
		t.Fatalf("WriteCount = %d, want 4 (четыре скаляра)", got)
	}
	if got := journalWritesOf(cm); got != 1 {
		t.Fatalf("journal writes = %d, want 1", got)
	}
	// Сбор сумм Sync принадлежит хранилищу и переключателем не управляется:
	// диагностический снимок доступен и видит те же четыре скалярные записи;
	// запись журнала хранит собственная матрица сохранений CM.
	d := takeStorageDiagnostics(storage)
	if !d.available || d.group() != _statsStorageFull || d.writes != 4 {
		t.Fatalf("диагностика хранилища = %+v, want полную с четырьмя записями", d)
	}

	sink := &countingWriter{}
	cm.publishStats(sink, sink)
	if sink.calls != 0 {
		t.Fatalf("выключенный вывод выполнил %d записей, want 0", sink.calls)
	}
}

// TestPersistenceCounters_AbortedPersistNotObserved проверяет, что при
// прерывании сохранения до завершения прежнего цикла Set наблюдение
// не создаётся: паника хранилища на первой записи не увеличивает ни матрицу,
// ни размеры полного сохранения.
func TestPersistenceCounters_AbortedPersistNotObserved(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	storage := &abortOnWriteStorage{MapStorage: store.NewMapStorage()}
	cm := &ConsensusModule{storage: storage}
	cm.cmState.currentTerm = 1
	cm.cmState.votedFor = -1
	cm.cmState.lastSnapshotIndex = -1
	cm.cmState.lastSnapshotTerm = -1
	cm.cmState.log = []LogEntry{{Index: 0, Term: 1}}
	cm.markLogRewriteDirtyLocked()

	before := persistenceSnapshotOf(cm)
	storage.armed.Store(true)
	func() {
		defer func() {
			if recover() == nil {
				t.Error("прерывание записи не воспроизведено")
			}
		}()
		cm.mu.Lock()
		defer cm.mu.Unlock()
		cm.persistToStorageLocked(persistSourceApply)
	}()

	after := persistenceSnapshotOf(cm)
	if after != before {
		t.Fatalf("прерванное сохранение изменило матрицу: %+v -> %+v", before, after)
	}
}
