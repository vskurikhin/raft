package raft

import (
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
)

// TestSendBatch_SkipsMissingPrefix проверяет, что sendBatch не добавляет
// в батч запись, чей Index не совпадает с запрошенным idx (FINDING-005):
// для индексов ниже первого индекса компактированного журнала в батч
// раньше подставлялась запись log[0].
func TestSendBatch_SkipsMissingPrefix(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	cm := &ConsensusModule{}
	cm.cmState.log = []LogEntry{{Index: 5, Term: 1, Type: LogCommand, Data: "k=v"}}
	cm.cmState.lastLogIndex = 5
	cm.cmState.lastLogTerm = 1
	cm.leaderState.inflight = make(map[int]*logFuture)
	cm.fsmMutateCh = make(chan []*commitTuple, 10)

	// Индексы 0..4 отсутствуют в журнале: батч должен быть пустым.
	cm.sendBatch(0, 4)
	select {
	case batch := <-cm.fsmMutateCh:
		t.Fatalf("sendBatch(0, 4) sent batch of %d entries, want none", len(batch))
	default:
	}

	// Индекс 5 существует: батч содержит ровно одну запись с Index == 5.
	cm.sendBatch(5, 5)
	select {
	case batch := <-cm.fsmMutateCh:
		if len(batch) != 1 {
			t.Fatalf("sendBatch(5, 5) batch = %d entries, want 1", len(batch))
		}
		if batch[0].log.Index != 5 {
			t.Fatalf("sendBatch(5, 5) entry Index = %d, want 5", batch[0].log.Index)
		}
	default:
		t.Fatal("sendBatch(5, 5) sent no batch, want 1 entry")
	}
}

// --- regression-тесты: граница копирования CM→FSM (ADR-001) ---
//
// TestSendBatch_CopyNotAliasedWithLog — regression INV-1 (Raft Log Isolation):
// конфликтная перезапись слотов уже сформированного батча не влияет на записи,
// которые применяет FSM. Пересечение конструируется искусственно (прямая
// подготовка состояния + прямые вызовы AppendEntries): при корректном кворуме
// «естественное» пересечение недостижимо, поэтому без искусственной подготовки
// тест был бы зелёным на обеих версиях кода (FINDING-08).
func TestSendBatch_CopyNotAliasedWithLog(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	const k = 3
	fsm := newGateBatchFSM()
	cm := newAliasTestCM(k)
	cm.fsm = fsm
	go cm.runFSM()
	defer cm.Stop()
	defer fsm.releaseApply()

	// Первый AppendEntries (пустые Entries) продвигает commitIndex до k и через
	// processLogs формирует батч [0..k], отправляемый в fsmMutateCh.
	var reply1 AppendEntriesReply
	if err := cm.AppendEntries(AppendEntriesArgs{
		RPCHeader:    RPCHeader{ProtocolVersion: ProtocolVersion, ServerID: 99},
		Term:         1,
		LeaderID:     99,
		PrevLogIndex: -1,
		PrevLogTerm:  0,
		LeaderCommit: k,
	}, &reply1); err != nil {
		t.Fatalf("первый AppendEntries: %v", err)
	}

	// Обязательные assertions ДО конфликтной перезаписи (TASK-016 Acceptance
	// Criteria п. 2):
	//  1) адрес backing array и cap на момент формирования батча;
	//  2) logInsertPos конфликтной перезаписи <= максимальной позиции батча;
	//  3) (после второго AE) адрес backing array неизменен.
	cm.mu.Lock()
	baseAddr := &cm.cmState.log[0]
	_ = cap(cm.cmState.log)
	logInsertPos := cm.logPosition(0) // PrevLogIndex+1 == 0
	cm.mu.Unlock()
	if logInsertPos > k {
		t.Fatalf("logInsertPos конфликтной перезаписи = %d > maxBatchPos = %d", logInsertPos, k)
	}

	// Ожидаемые значения на момент формирования батча (снимок до второго AE).
	expected := make([]any, k+1)
	for i := 0; i <= k; i++ {
		expected[i] = fmt.Sprintf("original-%d", i)
	}

	// Второй AppendEntries с конфликтными записями (терм 2) по тем же индексам
	// 0..k — писатель raft_cm_rpc.go:149 перезаписывает слоты батча in-place.
	entries := make([]LogEntry, k+1)
	for i := 0; i <= k; i++ {
		entries[i] = LogEntry{Index: i, Term: 2, Type: LogCommand, Data: fmt.Sprintf("rewritten-%d", i)}
	}
	var reply2 AppendEntriesReply
	if err := cm.AppendEntries(AppendEntriesArgs{
		RPCHeader:    RPCHeader{ProtocolVersion: ProtocolVersion, ServerID: 99},
		Term:         1,
		LeaderID:     99,
		PrevLogIndex: -1,
		PrevLogTerm:  0,
		Entries:      entries,
		LeaderCommit: k,
	}, &reply2); err != nil {
		t.Fatalf("второй AppendEntries: %v", err)
	}

	// Assertion 3: перезапись прошла без реаллокации backing array — иначе
	// append создал бы новый массив и пересечения с батчем не было бы.
	cm.mu.Lock()
	newAddr := &cm.cmState.log[0]
	cm.mu.Unlock()
	if newAddr != baseAddr {
		t.Fatalf("backing array реаллоцирован: %p -> %p", baseAddr, newAddr)
	}

	// Разрешаем применение и дожидаемся завершения.
	fsm.releaseApply()
	select {
	case <-fsm.done:
	case <-time.After(2 * time.Second):
		t.Fatal("FSM не применила батч вовремя")
	}

	// Главный assertion: FSM применила значения, зафиксированные на момент
	// формирования батча, а не перезаписанные конфликтным AppendEntries.
	applied := fsm.getApplied()
	if len(applied) != len(expected) {
		t.Fatalf("применено %d записей, ожидалось %d", len(applied), len(expected))
	}
	for i := range expected {
		if applied[i] != expected[i] {
			t.Fatalf("applied[%d] = %v, want %v (алиасинг не устранён)", i, applied[i], expected[i])
		}
	}
}

// TestFSMRetainedEntryImmutable — regression INV-1/INV-2 (Entry Lifetime):
// удержанный после возврата из Apply *LogEntry (и его payload) остаётся
// неизменным после конфликтной перезаписи и компактирования журнала.
func TestFSMRetainedEntryImmutable(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	const k = 3
	cm := &ConsensusModule{}
	cm.fsmMutateCh = make(chan []*commitTuple, batchApplyBuffer)
	cm.leaderState.inflight = make(map[int]*logFuture)
	cm.cmState.log = []LogEntry{
		{Index: 0, Term: 1, Type: LogCommand, Data: "v0"},
		{Index: 1, Term: 1, Type: LogCommand, Data: "v1"},
		{Index: 2, Term: 1, Type: LogCommand, Data: "v2"},
		{Index: 3, Term: 1, Type: LogCommand, Data: "v3"},
	}
	cm.cmState.lastLogIndex = k
	cm.cmState.lastLogTerm = 1

	// Формируем батч; batch[0].log — это объект, который FSM получит в Apply
	// и удержит после возврата.
	cm.sendBatch(0, k)
	batch := <-cm.fsmMutateCh
	retained := batch[0].log

	// Конфликтная перезапись слота 0 (писатель raft_cm_rpc.go:149).
	cm.mu.Lock()
	cm.cmState.log[0] = LogEntry{Index: 0, Term: 2, Type: LogCommand, Data: "rewritten"}
	cm.mu.Unlock()

	// Компактирование заменяет backing array (make+copy).
	cm.mu.Lock()
	cm.compactLogs(2)
	cm.mu.Unlock()

	// Удержанный объект и его payload неизменны.
	if retained.Index != 0 || retained.Term != 1 || retained.Data != "v0" {
		t.Fatalf("удержанная запись изменилась после перезаписи+компактирования: %+v", retained)
	}
}

// TestCompactLogs_PendingBatchUnchanged — regression INV-4 (Compaction Safety):
// компактирование журнала во время применения (батч в полёте) не влияет на
// значения, которые применит FSM. Компактирование обязано заменять backing
// array аллокацией нового среза, никогда — in-place обрезкой.
func TestCompactLogs_PendingBatchUnchanged(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	const k = 3
	fsm := newGateBatchFSM()
	cm := newAliasTestCM(k)
	cm.fsm = fsm
	go cm.runFSM()
	defer cm.Stop()
	defer fsm.releaseApply()

	// Формируем батч [0..k] напрямую через processLogs: runFSM получает его
	// и блокируется в ApplyBatch (батч в полёте).
	cm.processLogs(k)

	expected := make([]any, k+1)
	for i := 0; i <= k; i++ {
		expected[i] = fmt.Sprintf("original-%d", i)
	}

	// Компактирование во время удержания батча (замена массива make+copy).
	cm.mu.Lock()
	cm.compactLogs(2)
	cm.mu.Unlock()

	fsm.releaseApply()
	select {
	case <-fsm.done:
	case <-time.After(2 * time.Second):
		t.Fatal("FSM не применила батч вовремя")
	}

	applied := fsm.getApplied()
	if len(applied) != len(expected) {
		t.Fatalf("применено %d записей, ожидалось %d", len(applied), len(expected))
	}
	for i := range expected {
		if applied[i] != expected[i] {
			t.Fatalf("applied[%d] = %v, want %v после компактирования", i, applied[i], expected[i])
		}
	}
}

// TestClusterConvergence_ForcedReelections — интеграционный convergence-тест
// (INV-1..INV-4 end-to-end): кластер 3 узлов, непрерывная запись при
// принудительных перевыборах (RAFT_FORCE_MORE_REELECTION), после стабилизации
// все узлы сходятся к одному состоянию — последняя команда закоммичена на всех.
func TestClusterConvergence_ForcedReelections(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	// Test-only хук: electionTimeout возвращает ReelectionTimeoutMs в трети
	// случаев, вызывая частые смены лидера.
	t.Setenv("RAFT_FORCE_MORE_REELECTION", "1")

	h := NewHarness(t, 3)
	defer h.Shutdown()

	const numCmds = 20
	for i := 0; i < numCmds; i++ {
		lid, _ := h.CheckSingleLeader()
		future := h.cluster[lid].Apply(i, 0)
		if err := waitFuture(t, future, 10*time.Second); err != nil {
			t.Fatalf("Apply(%d) завершился с ошибкой: %v", i, err)
		}
	}

	// Сходимость: последняя команда присутствует в логе коммитов всех узлов.
	// Интеграционный тест с реальными таймерами — опрос с дедлайном.
	err := h.waitFor(
		fmt.Sprintf("cmd=%d committed on all connected peers", numCmds-1),
		10*time.Second,
		func() bool {
			for i := 0; i < h.n; i++ {
				if !h.connected[i] {
					continue
				}
				found := false
				for _, c := range h.commits[i] {
					if c.Data.(int) == numCmds-1 {
						found = true
						break
					}
				}
				if !found {
					return false
				}
			}
			return true
		},
		h.commitsDiag,
	)
	if err != nil {
		t.Fatalf("кластер не сошёлся к команде %d: %v", numCmds-1, err)
	}
}

// newAliasTestCM создаёт минимальный ConsensusModule в состоянии follower с
// журналом из k+1 записей (индексы 0..k, терм 1, Data "original-i").
// Журнал предвыделяется с запасом ёмкости, чтобы конфликтная перезапись в
// AppendEntries проходила без реаллокации backing array (требование к тесту 1).
// fsm и запуск runFSM выполняет вызывающий.
func newAliasTestCM(k int) *ConsensusModule {
	cm := &ConsensusModule{}
	cm.storage = NewMapStorage()
	cm.fsmMutateCh = make(chan []*commitTuple, batchApplyBuffer)
	cm.shutdownCh = make(chan struct{})
	cm.leaderState.inflight = make(map[int]*logFuture)
	cm.cmState.state = Follower
	cm.cmState.currentTerm = 1
	cm.cmState.votedFor = -1
	cm.cmState.lastSnapshotIndex = -1
	cm.cmState.lastSnapshotTerm = -1
	cm.cmState.termIndexMap = make(map[int]int)
	cm.cmState.log = make([]LogEntry, 0, k+1+8)
	for i := 0; i <= k; i++ {
		cm.cmState.log = append(cm.cmState.log, LogEntry{
			Index: i,
			Term:  1,
			Type:  LogCommand,
			Data:  fmt.Sprintf("original-%d", i),
		})
	}
	cm.cmState.lastLogIndex = k
	cm.cmState.lastLogTerm = 1
	cm.cmState.commitIndex = -1
	cm.cmState.lastApplied = -1
	return cm
}

// gateBatchFSM — тестовая BatchingFSM, задерживающая применение батча до
// явного разрешения. Пока runFSM заблокирована в ApplyBatch, тест может
// перезаписать слоты журнала или компактировать его — это детерминированно
// создаёт окно «батч сформирован, но не применён», требуемое для проверки
// границы копирования (INV-1) и безопасности компактирования (INV-4).
type gateBatchFSM struct {
	mu          sync.Mutex
	release     chan struct{}
	releaseOnce sync.Once
	done        chan struct{}
	doneOnce    sync.Once
	applied     []any
}

func newGateBatchFSM() *gateBatchFSM {
	return &gateBatchFSM{
		release: make(chan struct{}),
		done:    make(chan struct{}),
	}
}

// releaseApply разрешает применение батча. Идемпотентен: безопасен для вызова
// из тела теста и из defer-очистки (не паникует при повторном close).
func (f *gateBatchFSM) releaseApply() {
	f.releaseOnce.Do(func() { close(f.release) })
}

// ApplyBatch блокируется до releaseApply, затем фиксирует применённые Data и
// сигнализирует о завершении через done. Чтение Data происходит ПОСЛЕ
// разблокировки — так проверяется, что FSM применила значение, зафиксированное
// на момент формирования батча, а не перезаписанное позже.
func (f *gateBatchFSM) ApplyBatch(logs []*LogEntry) []any {
	<-f.release
	f.mu.Lock()
	for _, l := range logs {
		f.applied = append(f.applied, l.Data)
	}
	f.mu.Unlock()
	f.doneOnce.Do(func() { close(f.done) })
	return make([]any, len(logs))
}

// Apply не используется: gateBatchFSM реализует BatchingFSM, поэтому runFSM
// вызывает только ApplyBatch.
func (f *gateBatchFSM) Apply(*LogEntry) any { return nil }

// Snapshot — заглушка; в этих тестах снэпшоты не используются.
func (f *gateBatchFSM) Snapshot() (FSMSnapshot, error) { return nil, ErrNotImplemented }

// Restore — заглушка; в этих тестах восстановление не используется.
func (f *gateBatchFSM) Restore(io.ReadCloser) error { return ErrNotImplemented }

// getApplied возвращает копию применённых Data (потокобезопасно).
func (f *gateBatchFSM) getApplied() []any {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]any, len(f.applied))
	copy(out, f.applied)
	return out
}
