package raft

import (
	"bytes"
	"math"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/vskurikhin/raft/pkg/raft/store"
)

// -- Тесты кэша четырёх скалярных кодировок (L1). Кэш хранит последнее
// -- закодированное значение и готовые байты gob-представления для
// -- currentTerm, votedFor, lastSnapshotIndex, lastSnapshotTerm.
// -- Доказательство «повторного кодирования нет» — белый ящик: прямое
// -- чтение valid/value и сравнение идентичности подлежащего массива
// -- encoded до и после вызова. Production-хуков наблюдения (счётчиков,
// -- колбэков, экспорта) в коде нет. --

// newScalarCacheCM собирает CM с хранилищем в памяти и заданным набором
// четырёх скаляров; журнал не задан, поэтому первое сохранение скалярное.
// Прямая сборка приспособления, как в newBenchCM: достигается только
// сохраняемое состояние, горутины не запускаются.
func newScalarCacheCM(currentTerm, votedFor, lastSnapshotIndex, lastSnapshotTerm int) *ConsensusModule {
	cm := &ConsensusModule{storage: store.NewMapStorage()}
	cm.cmState.currentTerm = currentTerm
	cm.cmState.votedFor = votedFor
	cm.cmState.lastSnapshotIndex = lastSnapshotIndex
	cm.cmState.lastSnapshotTerm = lastSnapshotTerm
	return cm
}

// assertScalarEntry сверяет элемент кэша с ожидаемым значением: valid,
// value и байтовое равенство gob-представлению. Поля кэша читаются напрямую
// из внутритестового кода пакета raft: это способ доказательства
// переиспользования массива без production-хуков наблюдения.
func assertScalarEntry(t *testing.T, entry scalarCacheEntry, want int) {
	t.Helper()
	if !entry.valid {
		t.Fatalf("элемент кэша невалиден, want valid со значением %d", want)
	}
	if entry.value != want {
		t.Fatalf("значение кэша = %d, want %d", entry.value, want)
	}
	if !bytes.Equal(entry.encoded, gobEncode(t, want)) {
		t.Fatalf("байты кэша не равны gob(%d)", want)
	}
}

// scalarEntryPtr возвращает адрес первого байта подлежащего массива
// кодирования. Совпадение адреса до и после вызова сохранения доказывает,
// что массив не был заменён, то есть повторного кодирования не было.
// Вызывается только для валидного элемента: gob(int) всегда непуст.
func scalarEntryPtr(entry scalarCacheEntry) *byte {
	return &entry.encoded[0]
}

// cacheSnapshotOf снимает копию четырёх элементов кэша под cm.mu.
func cacheSnapshotOf(cm *ConsensusModule) scalarCache {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.scalarCache
}

// encodeScalarEntryForTest кодирует произвольное значение в элемент кэша
// currentTerm под cm.mu и возвращает снимок элемента. Проверяется только
// представление int, а не достижимое состояние CM: значения терма, голоса и
// границ снимка CM не подменяются. Владение cm.mu не передаётся — захват и
// снятие выполняются в этой же функции.
func encodeScalarEntryForTest(cm *ConsensusModule, value int) scalarCacheEntry {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	encodeScalarLocked(&cm.scalarCache.currentTerm, value)
	return cm.scalarCache.currentTerm
}

// TestScalarCache_FirstCallEncodesAllFour проверяет пустой кэш: первое
// сохранение кодирует ровно четыре скаляра независимо от ролей и нулей,
// каждый элемент становится валидным, байты равны прежнему gob(int).
func TestScalarCache_FirstCallEncodesAllFour(t *testing.T) {
	cm := newScalarCacheCM(1, -1, -1, -1)

	if cache := cacheSnapshotOf(cm); cache.currentTerm.valid ||
		cache.votedFor.valid || cache.lastSnapshotIndex.valid || cache.lastSnapshotTerm.valid {
		t.Fatal("свежий CM: кэш должен быть пустым (нулевое состояние invalid)")
	}

	persistToStorageForTest(cm)

	cache := cacheSnapshotOf(cm)
	assertScalarEntry(t, cache.currentTerm, 1)
	assertScalarEntry(t, cache.votedFor, -1)
	assertScalarEntry(t, cache.lastSnapshotIndex, -1)
	assertScalarEntry(t, cache.lastSnapshotTerm, -1)
}

// TestScalarCache_WarmedRepeatZeroReencodes проверяет прогретый повтор:
// при неизменных значениях ни один из четырёх подлежащих массивов не
// заменяется — повторных gob(int) нет. Число вызовов при этом неважно:
// удерживаемая память остаётся четырьмя скалярными представлениями.
func TestScalarCache_WarmedRepeatZeroReencodes(t *testing.T) {
	cm := newScalarCacheCM(1, -1, -1, -1)
	persistToStorageForTest(cm)
	before := cacheSnapshotOf(cm)

	for range 10 {
		persistToStorageForTest(cm)
	}

	after := cacheSnapshotOf(cm)
	entries := [][2]scalarCacheEntry{
		{before.currentTerm, after.currentTerm},
		{before.votedFor, after.votedFor},
		{before.lastSnapshotIndex, after.lastSnapshotIndex},
		{before.lastSnapshotTerm, after.lastSnapshotTerm},
	}
	for _, pair := range entries {
		if scalarEntryPtr(pair[0]) != scalarEntryPtr(pair[1]) {
			t.Fatalf("прогретый повтор заменил массив кодирования: %p → %p",
				scalarEntryPtr(pair[0]), scalarEntryPtr(pair[1]))
		}
	}
	assertScalarEntry(t, after.currentTerm, 1)
	assertScalarEntry(t, after.votedFor, -1)
	assertScalarEntry(t, after.lastSnapshotIndex, -1)
	assertScalarEntry(t, after.lastSnapshotTerm, -1)
}

// TestScalarCache_OneChangeReencodesExactlyOne проверяет ровно одно
// изменение: заменяется только элемент изменившегося скаляра, остальные
// три массива сохраняют идентичность, новое значение кодируется заново.
func TestScalarCache_OneChangeReencodesExactlyOne(t *testing.T) {
	cm := newScalarCacheCM(1, -1, -1, -1)
	persistToStorageForTest(cm)
	before := cacheSnapshotOf(cm)

	cm.cmState.currentTerm = 2
	persistToStorageForTest(cm)

	after := cacheSnapshotOf(cm)
	if scalarEntryPtr(before.currentTerm) == scalarEntryPtr(after.currentTerm) {
		t.Fatal("изменённый терм не перекодирован: массив прежний")
	}
	if scalarEntryPtr(before.votedFor) != scalarEntryPtr(after.votedFor) ||
		scalarEntryPtr(before.lastSnapshotIndex) != scalarEntryPtr(after.lastSnapshotIndex) ||
		scalarEntryPtr(before.lastSnapshotTerm) != scalarEntryPtr(after.lastSnapshotTerm) {
		t.Fatal("неизменённый скаляр был перекодирован: заменился его массив")
	}
	assertScalarEntry(t, after.currentTerm, 2)
	assertScalarEntry(t, after.votedFor, -1)
	assertScalarEntry(t, after.lastSnapshotIndex, -1)
	assertScalarEntry(t, after.lastSnapshotTerm, -1)
}

// TestScalarCache_ABAReencodesA проверяет путь A→B→A на отдельном элементе
// кодировщика: история из двух значений не хранится, поэтому возврат к A
// кодирует A заново — массив отличается и от A-первого, и от B-промежуточного,
// байты снова равны gob(A). Значения подаются напрямую в кодировщик под cm.mu;
// терм и голоса CM при этом не изменяются.
func TestScalarCache_ABAReencodesA(t *testing.T) {
	cm := newScalarCacheCM(1, -1, -1, -1)

	ptrA := scalarEntryPtr(encodeScalarEntryForTest(cm, 5))
	ptrB := scalarEntryPtr(encodeScalarEntryForTest(cm, 9))
	if ptrA == ptrB {
		t.Fatal("A→B не перекодировал значение: массив прежний")
	}

	after := encodeScalarEntryForTest(cm, 5)
	if scalarEntryPtr(after) == ptrB {
		t.Fatal("B→A переиспользовал массив B вместо нового кодирования A")
	}
	if scalarEntryPtr(after) == ptrA {
		t.Fatal("B→A переиспользовал массив первого A: история из двух значений сохранена")
	}
	assertScalarEntry(t, after, 5)
}

// TestScalarCache_BoundaryAndNegativeValues проверяет граничные и
// отрицательные значения int на отдельном элементе кодировщика: каждый переход
// между значениями кодируется заново с байтами, равными прежнему gob; повтор
// того же значения массив не заменяет. Значения подаются напрямую в кодировщик
// под cm.mu, поэтому невозможные для терма корректного узла величины (нули,
// отрицательные, границы int) не записываются как состояние CM.
func TestScalarCache_BoundaryAndNegativeValues(t *testing.T) {
	values := []int{
		0, -1, 1, math.MinInt64, math.MaxInt64, math.MinInt32, math.MaxInt32,
		-(1 << 40), 1 << 40,
	}
	cm := newScalarCacheCM(1, -1, -1, -1)

	entry := encodeScalarEntryForTest(cm, values[0])
	assertScalarEntry(t, entry, values[0])
	prevPtr := scalarEntryPtr(entry)

	for _, v := range values[1:] {
		entry = encodeScalarEntryForTest(cm, v)
		if scalarEntryPtr(entry) == prevPtr {
			t.Fatalf("переход к %d не перекодировал значение: массив прежний", v)
		}
		assertScalarEntry(t, entry, v)

		// Повтор того же граничного значения — попадание в кэш.
		if ptr := scalarEntryPtr(encodeScalarEntryForTest(cm, v)); ptr != scalarEntryPtr(entry) {
			t.Fatalf("повтор значения %d заменил массив кодирования", v)
		}
		prevPtr = scalarEntryPtr(entry)
	}
}

// TestScalarCache_TwoCMsIndependent проверяет два CM: кэш — состояние
// экземпляра, у каждого свои массивы, изменение одного CM не затрагивает
// кэш другого даже при одинаковых значениях.
func TestScalarCache_TwoCMsIndependent(t *testing.T) {
	cm1 := newScalarCacheCM(1, -1, -1, -1)
	cm2 := newScalarCacheCM(1, -1, -1, -1)
	persistToStorageForTest(cm1)
	persistToStorageForTest(cm2)

	if scalarEntryPtr(cacheSnapshotOf(cm1).currentTerm) ==
		scalarEntryPtr(cacheSnapshotOf(cm2).currentTerm) {
		t.Fatal("два CM разделяют один массив кодирования: кэш не является состоянием экземпляра")
	}

	cm1.cmState.currentTerm = 7
	persistToStorageForTest(cm1)

	if entry := cacheSnapshotOf(cm2).currentTerm; entry.value != 1 {
		t.Fatalf("изменение cm1 отразилось в кэше cm2: value = %d, want 1", entry.value)
	}
	assertScalarEntry(t, cacheSnapshotOf(cm1).currentTerm, 7)
}

// TestScalarCache_RestoreLeavesCacheEmpty проверяет восстановленный CM:
// restore не прогревает кэш от диска — все элементы невалидны, а первое
// сохранение снова кодирует четыре текущих значения. Инициализация
// выполняется как в производственном конструкторе: границы отсутствующего
// снимка равны -1, поэтому после restore при отсутствии snapshot-ключей
// сохраняются именно -1, а не нулевое значение Go.
func TestScalarCache_RestoreLeavesCacheEmpty(t *testing.T) {
	storage := store.NewMapStorage()
	storage.Set("currentTerm", gobEncode(t, 2))
	storage.Set("votedFor", gobEncode(t, 0))
	storage.RewriteLog([]LogEntry{{Index: 0, Term: 1}})
	cm := &ConsensusModule{storage: storage}
	cm.initSnapshotConfig(nil)
	cm.restoreFromStorage()

	if cache := cacheSnapshotOf(cm); cache.currentTerm.valid ||
		cache.votedFor.valid || cache.lastSnapshotIndex.valid || cache.lastSnapshotTerm.valid {
		t.Fatal("после restore кэш должен быть пустым (от диска не прогревается)")
	}

	persistToStorageForTest(cm)

	cache := cacheSnapshotOf(cm)
	assertScalarEntry(t, cache.currentTerm, 2)
	assertScalarEntry(t, cache.votedFor, 0)
	// Снимок-ключи в хранилище отсутствуют, restore их не заполняет:
	// сохраняются границы отсутствующего снимка -1/-1, заданные
	// инициализацией, а не нулевое значение свежего узла.
	assertScalarEntry(t, cache.lastSnapshotIndex, -1)
	assertScalarEntry(t, cache.lastSnapshotTerm, -1)
}

// recordedSet — одна наблюдённая запись (ключ, клонированное значение).
type recordedSet struct {
	key   string
	value []byte
}

// valueRecordingStorage — обёртка над LogStorage, фиксирующая каждую пару
// (ключ, значение) в порядке вызовов Set и псевдоключ "log" на операцию
// записи журнала. Значение клонируется на записи, чтобы наблюдения не
// зависели от повторного использования буферов вызывающим.
type valueRecordingStorage struct {
	LogStorage
	mu    sync.Mutex
	calls []recordedSet
}

func (r *valueRecordingStorage) Set(key string, value []byte) {
	r.record(key, value)
	r.LogStorage.Set(key, value)
}

func (r *valueRecordingStorage) StoreLogEntries(fromIndex int, entries []LogEntry) LogWriteResult {
	r.record("log", nil)
	return r.LogStorage.StoreLogEntries(fromIndex, entries)
}

func (r *valueRecordingStorage) RewriteLog(entries []LogEntry) LogWriteResult {
	r.record("log", nil)
	return r.LogStorage.RewriteLog(entries)
}

func (r *valueRecordingStorage) record(key string, value []byte) {
	r.mu.Lock()
	r.calls = append(r.calls, recordedSet{key: key, value: bytes.Clone(value)})
	r.mu.Unlock()
}

func (r *valueRecordingStorage) callsSnapshot() []recordedSet {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.calls)
}

// assertRecordedSets сверяет наблюдаемую последовательность Set с
// ожидаемыми ключами и значениями (каждое — байтовое равенство gob).
func assertRecordedSets(t *testing.T, got []recordedSet, want []recordedSet) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("наблюдено %d вызовов Set, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i].key != want[i].key || !bytes.Equal(got[i].value, want[i].value) {
			t.Fatalf("Set[%d] = (%q, %x), want (%q, %x)",
				i, got[i].key, got[i].value, want[i].key, want[i].value)
		}
	}
}

// TestScalarCache_SetCalledOnEveryPersist проверяет контракт «кэш не
// означает долговечности»: записывающий LogStorage видит прежние 4 Set на
// каждом скалярном вызове и пятую операцию журнала в режиме полной замены,
// порядок ключей и значения равны базе — в том числе на повторных
// попаданиях в кэш.
func TestScalarCache_SetCalledOnEveryPersist(t *testing.T) {
	rec := &valueRecordingStorage{LogStorage: store.NewMapStorage()}
	cm := &ConsensusModule{storage: rec}
	cm.cmState.currentTerm = 1
	cm.cmState.votedFor = -1
	cm.cmState.lastSnapshotIndex = -1
	cm.cmState.lastSnapshotTerm = -1

	scalarWant := []recordedSet{
		{key: "currentTerm", value: gobEncode(t, 1)},
		{key: "votedFor", value: gobEncode(t, -1)},
		{key: "lastSnapshotIndex", value: gobEncode(t, -1)},
		{key: "lastSnapshotTerm", value: gobEncode(t, -1)},
	}

	// Первый вызов: четыре Set с кодированием всех четырёх значений.
	persistToStorageForTest(cm)
	assertRecordedSets(t, rec.callsSnapshot(), scalarWant)

	// Повтор неизменённого состояния: снова те же четыре Set — попадание
	// в кэш не отменяет вызовы.
	rec.calls = nil
	persistToStorageForTest(cm)
	assertRecordedSets(t, rec.callsSnapshot(), scalarWant)

	// Режим полной замены: журнал идёт последним, пятая операция.
	cm.cmState.log = []LogEntry{{Index: 0, Term: 1, Type: LogCommand, Data: "k0=v0"}}
	cm.markLogRewriteDirtyLocked()
	rec.calls = nil
	persistToStorageForTest(cm)
	fullWant := slices.Clone(scalarWant)
	fullWant = append(fullWant, recordedSet{key: "log"})
	assertRecordedSets(t, rec.callsSnapshot(), fullWant)

	// Изменённый скаляр на повторном вызове: значение Set — новое,
	// число и порядок ключей прежние.
	cm.cmState.currentTerm = 4
	rec.calls = nil
	persistToStorageForTest(cm)
	changedWant := slices.Clone(scalarWant)
	changedWant[0].value = gobEncode(t, 4)
	assertRecordedSets(t, rec.callsSnapshot(), changedWant)
}

// TestScalarCache_FileStorageWriteSequenceUnchanged проверяет, что
// детерминированное число записей FileStorage при одинаковой
// последовательности состояний не изменилось: первый персист — четыре
// скалярных записи и одна запись журнала, неизменённое состояние — 0,
// смена одного скаляра — 1, возврат A→B→A — снова 1 (значение на диске
// меняется в обе стороны). Скалярные записи считает FileStorage, запись
// журнала — матрица сохранений CM.
func TestScalarCache_FileStorageWriteSequenceUnchanged(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	storage := store.NewFileStorage(t.TempDir())
	counter := &journalCountingStorage{LogStorage: storage}
	cm := &ConsensusModule{storage: counter}
	cm.cmState.currentTerm = 1
	cm.cmState.votedFor = -1
	cm.cmState.lastSnapshotIndex = -1
	cm.cmState.lastSnapshotTerm = -1
	cm.cmState.log = []LogEntry{{Index: 0, Term: 1}}
	cm.markLogRewriteDirtyLocked()

	persistToStorageForTest(cm)
	if got := storage.WriteCount(); got != 4 {
		t.Fatalf("первый персист: %d скалярных записей, want 4", got)
	}
	if got := counter.journalWrites(); got != 1 {
		t.Fatalf("первый персист: %d записей журнала, want 1", got)
	}

	before := storage.WriteCount()
	persistToStorageForTest(cm)
	if got := storage.WriteCount() - before; got != 0 {
		t.Fatalf("неизменённое состояние: %d записей, want 0", got)
	}

	cm.cmState.votedFor = 7
	before = storage.WriteCount()
	persistToStorageForTest(cm)
	if got := storage.WriteCount() - before; got != 1 {
		t.Fatalf("смена одного скаляра: %d записей, want 1", got)
	}

	cm.cmState.votedFor = -1
	before = storage.WriteCount()
	persistToStorageForTest(cm)
	if got := storage.WriteCount() - before; got != 1 {
		t.Fatalf("возврат A→B→A: %d записей, want 1", got)
	}
}

// TestScalarCache_StorageCopyMutationLeavesEncodedIntact проверяет
// защитные копии Storage: мутация копии, выдаваемой хранилищем, не влияет
// ни на байты, ни на подлежащий массив кэша кодирования.
func TestScalarCache_StorageCopyMutationLeavesEncodedIntact(t *testing.T) {
	storage := store.NewMapStorage()
	cm := &ConsensusModule{storage: storage}
	cm.cmState.currentTerm = 5
	cm.cmState.votedFor = -1
	cm.cmState.lastSnapshotIndex = -1
	cm.cmState.lastSnapshotTerm = -1
	persistToStorageForTest(cm)
	before := cacheSnapshotOf(cm).currentTerm

	got, ok := storage.Get("currentTerm")
	if !ok {
		t.Fatal("ключ currentTerm отсутствует в хранилище")
	}
	for i := range got {
		got[i] = 0xFF
	}

	after := cacheSnapshotOf(cm).currentTerm
	if scalarEntryPtr(before) != scalarEntryPtr(after) {
		t.Fatal("мутация копии Storage заменила массив кэша")
	}
	assertScalarEntry(t, after, 5)
}

// TestScalarCache_StateChangeBetweenAECommitAndAEFinish проверяет
// достижимый сценарий, где постоянное состояние меняется между двумя
// сохранениями одного AppendEntries: после ae_commit cm.mu отпускается
// для processLogs (raft_cm_rpc.go:134–136), и конкурентный RequestVote
// поднимает терм до повторного захвата блокировки в ae_finish. Кэш обязан
// заметить изменение: на диск и в кэш попадает новый терм, а не устаревшее
// кодирование.
func TestScalarCache_StateChangeBetweenAECommitAndAEFinish(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	dir := t.TempDir()
	cm, _ := newAEDurabilityCM(dir)
	cm.shutdownCh = make(chan struct{})
	cm.cmState.electionTimerDone = make(chan struct{})
	cm.cmState.leaderID = -1
	cm.cmState.configurations.latest = Configuration{ConfigServers: []ConfigServer{
		{ID: 0, Suffrage: Voter},
		{ID: 99, Suffrage: Voter},
	}}
	// Потребитель fsmMutateCh не запускается: sendBatch блокируется на
	// отправке батча после apply-сохранения и до ae_finish, пока не будет
	// закрыт shutdownCh. Это достижимое производственное окно, в котором
	// cm.mu свободен (аналог медленного применения к машине состояний).
	cm.fsmMutateCh = make(chan []*commitTuple)
	cm.leaderState.inflight = make(map[int]*logFuture)
	closeShutdown := sync.OnceFunc(func() { close(cm.shutdownCh) })
	defer closeShutdown()

	aeDone := make(chan error, 1)
	var reply AppendEntriesReply
	go func() {
		aeDone <- cm.AppendEntries(aeArgs(-1, -1, 0,
			[]LogEntry{{Index: 0, Term: 1, Type: LogCommand, Data: "k0=v0"}}), &reply)
	}()

	// Ждём, пока AppendEntries пройдёт ae_commit (журнал на диске) и
	// processLogs продвинет lastApplied: обработчик гарантированно прошёл
	// точку отпускания cm.mu и заблокирован на отправке батча.
	if err := waitCond("AppendEntries достиг окна между ae_commit и ae_finish", 5*time.Second,
		func() bool {
			cm.mu.Lock()
			defer cm.mu.Unlock()
			return cm.cmState.lastApplied == 0
		}, func() string {
			cm.mu.Lock()
			defer cm.mu.Unlock()
			return "lastApplied = " + itoa(cm.cmState.lastApplied)
		}); err != nil {
		t.Fatal(err)
	}

	// Конкурентный RequestVote меняет постоянное состояние именно в этом окне.
	var voteReply RequestVoteReply
	if err := cm.RequestVote(RequestVoteArgs{
		RPCHeader:    RPCHeader{ProtocolVersion: ProtocolVersion, ServerID: 99},
		Term:         2,
		CandidateID:  99,
		LastLogIndex: 0,
		LastLogTerm:  1,
	}, &voteReply); err != nil {
		t.Fatalf("RequestVote: %v", err)
	}
	if !voteReply.VoteGranted {
		t.Fatalf("VoteGranted = false, want true: %+v", voteReply)
	}

	// Разблокируем sendBatch: AppendEntries завершает ae_finish и отвечает.
	closeShutdown()
	if err := <-aeDone; err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if !reply.Success {
		t.Fatalf("reply.Success = false, want true: %+v", reply)
	}
	if reply.Term != 2 {
		t.Fatalf("reply.Term = %d, want 2 (терм поднят в окне)", reply.Term)
	}

	// На диске — новый терм и голос; старые байты ae_finish не перезаписал.
	disk := store.NewFileStorage(dir)
	termData, ok := disk.Get("currentTerm")
	if !ok {
		t.Fatal("currentTerm missing on disk")
	}
	var persistedTerm int
	gobDecode(t, termData, &persistedTerm)
	if persistedTerm != 2 {
		t.Fatalf("persisted currentTerm = %d, want 2", persistedTerm)
	}
	voteData, ok := disk.Get("votedFor")
	if !ok {
		t.Fatal("votedFor missing on disk")
	}
	var persistedVote int
	gobDecode(t, voteData, &persistedVote)
	if persistedVote != 99 {
		t.Fatalf("persisted votedFor = %d, want 99", persistedVote)
	}

	// Кэш тоже содержит новое значение: изменение между сохранениями
	// замечено и перекодировано.
	assertScalarEntry(t, cacheSnapshotOf(cm).currentTerm, 2)
	assertScalarEntry(t, cacheSnapshotOf(cm).votedFor, 99)
}
