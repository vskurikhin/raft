package raft

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/fortytw2/leaktest"
	"github.com/vskurikhin/raft/pkg/raft/contract"
	"github.com/vskurikhin/raft/pkg/raft/store"
)

// -- Интеграционные проверки CM: выбор операции хранилища по явной точке
// -- грязи, взятие байтов и записей из результата store, счётчик конфликтных
// -- замен суффикса и публикация PersistV2/Schema=2. --

// TestLoadLogForRestore_MissingAndEmpty проверяет различение отсутствующего
// и существующего пустого журнала на пути восстановления: отсутствие — это
// маркерная ошибка с прежним смыслом «log not found», пустой созданный
// журнал даёт пустой срез и nil.
func TestLoadLogForRestore_MissingAndEmpty(t *testing.T) {
	missing := &ConsensusModule{storage: store.NewMapStorage()}
	if _, err := missing.loadLogForRestore(); !errors.Is(err, contract.ErrLogNotFound) {
		t.Fatalf("отсутствующий журнал: err = %v, want ErrLogNotFound", err)
	} else if !strings.Contains(err.Error(), "log not found") {
		t.Fatalf("ошибка отсутствия потеряла прежний текст: %v", err)
	}

	empty := store.NewMapStorage()
	empty.RewriteLog([]LogEntry{})
	cm := &ConsensusModule{storage: empty}
	entries, err := cm.loadLogForRestore()
	if err != nil {
		t.Fatalf("существующий пустой журнал: неожиданная ошибка %v", err)
	}
	if entries == nil || len(entries) != 0 {
		t.Fatalf("существующий пустой журнал = %v, want пустой неотрицательный срез", entries)
	}
}

// TestPersistV2_JournalStatsFromStoreResult проверяет, что байты и число
// записей берутся из результата операции хранилища, размер журнала — из
// памяти CM, а повторное кодирование ради статистики не выполняется.
func TestPersistV2_JournalStatsFromStoreResult(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	storage := store.NewFileStorage(t.TempDir())
	cm := &ConsensusModule{storage: storage}
	cm.cmState.currentTerm = 1
	cm.cmState.votedFor = -1
	cm.cmState.lastSnapshotIndex = -1
	cm.cmState.lastSnapshotTerm = -1
	cm.cmState.log = []LogEntry{{Index: 0, Term: 1, Type: LogCommand, Data: "k0=v0"}}
	cm.markLogRewriteDirtyLocked()

	cm.mu.Lock()
	cm.persistToStorageLocked(persistSourceApply)
	cm.mu.Unlock()

	snap := persistenceSnapshotOf(cm)
	if snap.logLenSum != 1 || snap.logLenMin != 1 || snap.logLenMax != 1 {
		t.Fatalf("LogLen = (sum %d, min %d, max %d), want все 1", snap.logLenSum, snap.logLenMin, snap.logLenMax)
	}
	if snap.logWrites != 1 {
		t.Fatalf("LogWrites = %d, want 1 (результат RewriteLog)", snap.logWrites)
	}
	if snap.logBytesWrittenSum <= 0 {
		t.Fatalf("LogBytesWrittenSum = %d, want > 0 из результата хранилища", snap.logBytesWrittenSum)
	}
	if snap.conflictSuffixReplacements != 0 {
		t.Fatalf("ConflictSuffixReplacements = %d, want 0 на полной замене", snap.conflictSuffixReplacements)
	}
}

// TestPersistV2_MapStorageLogicalCallWithoutDisk проверяет, что на
// MapStorage логическая операция журнала учитывается, а дисковые счётчики
// (байты и записи) остаются нулевыми: диска у хранилища нет.
func TestPersistV2_MapStorageLogicalCallWithoutDisk(t *testing.T) {
	cm := &ConsensusModule{storage: store.NewMapStorage()}
	cm.cmState.log = []LogEntry{{Index: 0, Term: 1}}
	cm.markLogRewriteDirtyLocked()

	cm.mu.Lock()
	cm.persistToStorageLocked(persistSourceApply)
	cm.mu.Unlock()

	snap := persistenceSnapshotOf(cm)
	if got := persistenceCellOf(snap, persistSourceApply).logCalls; got != 1 {
		t.Fatalf("LogCalls = %d, want 1 (логическая операция учтена)", got)
	}
	if snap.logWrites != 0 || snap.logBytesWrittenSum != 0 {
		t.Fatalf("дисковые счётчики = (writes %d, bytes %d), want 0 у MapStorage",
			snap.logWrites, snap.logBytesWrittenSum)
	}
}

// TestPersistV2_Schema2Document проверяет публикацию PersistV2: схема 2,
// новые поля журнала присутствуют в JSON, а старые имена схемы 1 отсутствуют.
func TestPersistV2_Schema2Document(t *testing.T) {
	cm := newStatsTestCM()
	cm.persistence.observe(persistSourceApply, true, 0, 4, 512, 1)
	cm.persistence.markConflictSuffixReplacementLocked()

	snap := cm.takeStatsSnapshot()
	doc := snap.persistDocument(1, "", storageDiagnostics{})
	if doc.Schema != 2 {
		t.Fatalf("Schema = %d, want 2", doc.Schema)
	}
	data, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("json.Marshal PersistV2: %v", err)
	}
	text := string(data)
	for _, key := range []string{`"LogBytesWrittenSum"`, `"LogWrites"`, `"ConflictSuffixReplacements"`} {
		if !strings.Contains(text, key) {
			t.Fatalf("PersistV2 не содержит поле %s: %s", key, text)
		}
	}
	if strings.Contains(text, `"LogBytesSum"`) {
		t.Fatalf("PersistV2 содержит устаревшее поле LogBytesSum: %s", text)
	}
	if doc.Persist == nil || doc.Persist.LogWrites != 1 || doc.Persist.ConflictSuffixReplacements != 1 {
		t.Fatalf("PersistV2 потерял поля журнала: %+v", doc.Persist)
	}
}

// TestConflictSuffixReplacements_OnlyOnActualReplacement проверяет счётчик
// конфликтных замен: он растёт ровно на фактическую замену существующего
// суффикса, а совпадающий повтор AppendEntries и чистое добавление его не
// увеличивают.
func TestConflictSuffixReplacements_OnlyOnActualReplacement(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	cm, _ := newDirtyFollowerCM(t, 5)
	before := persistenceSnapshotOf(cm).conflictSuffixReplacements

	conflict := []LogEntry{
		{Index: 3, Term: 2, Type: LogCommand},
		{Index: 4, Term: 2, Type: LogCommand},
	}
	var reply AppendEntriesReply
	if err := cm.AppendEntries(aeArgs(2, 1, -1, conflict), &reply); err != nil {
		t.Fatalf("конфликтный AppendEntries: %v", err)
	}
	if !reply.Success {
		t.Fatal("конфликтный reply.Success = false, want true")
	}
	afterConflict := persistenceSnapshotOf(cm).conflictSuffixReplacements
	if afterConflict != before+1 {
		t.Fatalf("ConflictSuffixReplacements после замены = %d, want %d", afterConflict, before+1)
	}

	// Совпадающий повтор: мутации нет, счётчик не растёт.
	if err := cm.AppendEntries(aeArgs(2, 1, -1, conflict), &reply); err != nil {
		t.Fatalf("повторный AppendEntries: %v", err)
	}
	if got := persistenceSnapshotOf(cm).conflictSuffixReplacements; got != afterConflict {
		t.Fatalf("повтор AppendEntries изменил счётчик: %d -> %d", afterConflict, got)
	}

	// Чистое добавление за конец: замены нет, счётчик не растёт.
	appendOnly := []LogEntry{{Index: 5, Term: 2, Type: LogCommand}}
	if err := cm.AppendEntries(aeArgs(4, 2, -1, appendOnly), &reply); err != nil {
		t.Fatalf("добавляющий AppendEntries: %v", err)
	}
	if got := persistenceSnapshotOf(cm).conflictSuffixReplacements; got != afterConflict {
		t.Fatalf("чистое добавление изменило счётчик: %d -> %d", afterConflict, got)
	}
}

// TestRepeatedAppendEntriesNoExtraJournalWrite проверяет отсутствие лишней
// записи журнала на совпадающем повторе AppendEntries: после первой
// действительной замены повтор не выполняет операций журнала.
func TestRepeatedAppendEntriesNoExtraJournalWrite(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	cm, _ := newDirtyFollowerCM(t, 5)
	entries := []LogEntry{
		{Index: 3, Term: 2, Type: LogCommand},
		{Index: 4, Term: 2, Type: LogCommand},
	}
	var reply AppendEntriesReply
	if err := cm.AppendEntries(aeArgs(2, 1, -1, entries), &reply); err != nil {
		t.Fatalf("первый AppendEntries: %v", err)
	}
	afterFirst := persistenceSnapshotOf(cm)

	if err := cm.AppendEntries(aeArgs(2, 1, -1, entries), &reply); err != nil {
		t.Fatalf("повторный AppendEntries: %v", err)
	}
	afterRepeat := persistenceSnapshotOf(cm)
	if afterRepeat.logWrites != afterFirst.logWrites {
		t.Fatalf("повтор выполнил %d записей журнала, want 0",
			afterRepeat.logWrites-afterFirst.logWrites)
	}
	if got := totalLogCalls(afterRepeat) - totalLogCalls(afterFirst); got != 0 {
		t.Fatalf("повтор создал %d логических операций журнала, want 0", got)
	}
}

// totalLogCalls суммирует логические операции журнала по всем источникам
// снимка матрицы сохранений.
func totalLogCalls(s persistenceSnapshot) int64 {
	var total int64
	for i := range s.cells {
		total += s.cells[i].logCalls
	}
	return total
}

// TestSuffixDirtyModeTransition проверяет правила явной точки грязи:
// suffix объединяется минимумом индекса, полная замена доминирует и не
// понижается последующей suffix-мутацией, успех очищает состояние.
func TestSuffixDirtyModeTransition(t *testing.T) {
	cm := new(ConsensusModule)
	cm.markLogSuffixDirtyLocked(5)
	if cm.cmState.logPersistMode != logPersistSuffix || cm.cmState.logDirtyFrom != 5 {
		t.Fatalf("после suffix(5) = (%v, %d), want (suffix, 5)", cm.cmState.logPersistMode, cm.cmState.logDirtyFrom)
	}
	cm.markLogSuffixDirtyLocked(3)
	if cm.cmState.logPersistMode != logPersistSuffix || cm.cmState.logDirtyFrom != 3 {
		t.Fatalf("после suffix(3) = (%v, %d), want (suffix, 3)", cm.cmState.logPersistMode, cm.cmState.logDirtyFrom)
	}
	cm.markLogSuffixDirtyLocked(7)
	if cm.cmState.logDirtyFrom != 3 {
		t.Fatalf("suffix(7) увеличил индекс до %d, want минимум 3", cm.cmState.logDirtyFrom)
	}
	cm.markLogRewriteDirtyLocked()
	if cm.cmState.logPersistMode != logPersistRewrite || cm.cmState.logDirtyFrom != -1 {
		t.Fatalf("после rewrite = (%v, %d), want (rewrite, -1)", cm.cmState.logPersistMode, cm.cmState.logDirtyFrom)
	}
	cm.markLogSuffixDirtyLocked(0)
	if cm.cmState.logPersistMode != logPersistRewrite || cm.cmState.logDirtyFrom != -1 {
		t.Fatalf("suffix понизил rewrite до (%v, %d)", cm.cmState.logPersistMode, cm.cmState.logDirtyFrom)
	}
	cm.clearLogDirtyLocked()
	if cm.cmState.logPersistMode != logPersistClean || cm.cmState.logDirtyFrom != -1 {
		t.Fatalf("после очистки = (%v, %d), want (clean, -1)", cm.cmState.logPersistMode, cm.cmState.logDirtyFrom)
	}
}

// TestSuffixPersistDurableAfterReopen проверяет, что заменённый суффикс
// долговечен и читается повторно открытым FileStorage: полный журнал в памяти
// равен журналу на диске после переоткрытия каталога.
func TestSuffixPersistDurableAfterReopen(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	dir := t.TempDir()
	cm, _ := newAEDurabilityCM(dir)
	cm.cmState.log = []LogEntry{
		{Index: 0, Term: 1, Type: LogCommand, Data: "k0=v0"},
		{Index: 1, Term: 1, Type: LogCommand, Data: "k1=v1"},
	}
	cm.storage.RewriteLog(cm.cmState.log)
	cm.clearLogDirtyLocked()
	cm.cmState.lastLogIndex = 1
	cm.cmState.lastLogTerm = 1

	cm.cmState.log = append(cm.cmState.log, LogEntry{Index: 2, Term: 1, Type: LogCommand, Data: "k2=v2"})
	cm.markLogSuffixDirtyLocked(2)
	cm.mu.Lock()
	cm.persistToStorageLocked(persistSourceApply)
	cm.mu.Unlock()

	onDisk, err := store.NewFileStorage(dir).LoadLog()
	if err != nil {
		t.Fatalf("LoadLog после переоткрытия: %v", err)
	}
	if len(onDisk) != 3 || onDisk[2].Index != 2 || onDisk[2].Data != "k2=v2" {
		t.Fatalf("журнал на диске = %+v, want три записи с последней k2=v2", onDisk)
	}
}

// TestFreshNodeFirstPersistCreatesJournal проверяет, что первый персист
// свежего узла выполняет полную замену и создаёт журнал, включая пустой.
func TestFreshNodeFirstPersistCreatesJournal(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	storage := store.NewFileStorage(t.TempDir())
	cm := &ConsensusModule{storage: storage}
	cm.markLogRewriteDirtyLocked()

	cm.mu.Lock()
	cm.persistToStorageLocked(persistSourceApply)
	cm.mu.Unlock()

	// Журнал в каталоге существует и пуст.
	if !storage.HasData() {
		t.Fatal("HasData = false после первого персиста свежего узла")
	}
	got, err := storage.LoadLog()
	if err != nil {
		t.Fatalf("созданный пустой журнал не читается: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("созданный журнал = %+v, want пустой", got)
	}
}
