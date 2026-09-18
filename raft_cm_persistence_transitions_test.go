package raft

import (
	"encoding/gob"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/vskurikhin/raft/pkg/raft/store"
	"github.com/vskurikhin/raft/pkg/raft/transp"
)

// -- Тесты источников сохранения на штатных переходах: пульс, продвижение
// -- фиксации, смена терма, передача лидерства, RequestVote, выборы,
// -- добавление лидера, применение, установка и создание снимка,
// -- самолечение при старте. Каждый тест проверяет собственную ячейку
// -- матрицы и, где применимо, фактические записи FileStorage. --

// assertPersistenceInvariants проверяет точные равенства согласованного
// снимка: N = N_log + N_scalar = сумме одиннадцати источников.
func assertPersistenceInvariants(t *testing.T, doc *statsPersistenceV1) {
	t.Helper()
	var sourceSum int64
	for i := range doc.Sources {
		sourceSum += doc.Sources[i].LogCalls + doc.Sources[i].ScalarCalls
	}
	if doc.NPersist != doc.NLogPersist+doc.NScalarOnly || doc.NPersist != sourceSum {
		t.Fatalf("нарушено равенство снимка: N=%d, N_log=%d, N_scalar=%d, сумма источников=%d",
			doc.NPersist, doc.NLogPersist, doc.NScalarOnly, sourceSum)
	}
}

// TestPersistCounters_AppendEntriesHeartbeatScalar проверяет пульс с
// неизменным состоянием: вызов сохранения есть, он скалярный, фактических
// записей нет. Прежнее поведение «пульс не пишет» сохраняется.
func TestPersistCounters_AppendEntriesHeartbeatScalar(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	dir := t.TempDir()
	cm, storage := newAEDurabilityCM(dir)

	before := persistenceSnapshotOf(cm)
	writesBefore := storage.WriteCount()

	var reply AppendEntriesReply
	if err := cm.AppendEntries(aeArgs(-1, -1, -1, nil), &reply); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if !reply.Success {
		t.Fatal("reply.Success = false, want true")
	}

	after := persistenceSnapshotOf(cm)
	cellBefore := persistenceCellOf(before, persistSourceAEFinish)
	cellAfter := persistenceCellOf(after, persistSourceAEFinish)
	if cellAfter.scalarCalls != cellBefore.scalarCalls+1 || cellAfter.logCalls != cellBefore.logCalls {
		t.Fatalf("ae_finish = (log %d, scalar %d), want (log %d, scalar %d)",
			cellAfter.logCalls, cellAfter.scalarCalls, cellBefore.logCalls, cellBefore.scalarCalls+1)
	}
	if got := storage.WriteCount() - writesBefore; got != 0 {
		t.Fatalf("%d записей на пульсе, want 0", got)
	}
	assertPersistenceInvariants(t, after.document())
}

// TestPersistCounters_AppendEntriesCommitAdvance проверяет приём записей с
// продвижением индекса фиксации: прежнее ожидаемое число фактических
// записей — одна; вызовов сохранения больше, потому что processLogs
// добавляет собственный полный вызов с источником apply между удержаниями
// блокировки. Ровно два общих вызова не требуются.
func TestPersistCounters_AppendEntriesCommitAdvance(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	dir := t.TempDir()
	cm, storage := newAEDurabilityCM(dir)
	startAEDurabilityFSM(t, cm)
	defer close(cm.shutdownCh)

	before := persistenceSnapshotOf(cm)
	writesBefore := storage.WriteCount()

	entries := []LogEntry{{Index: 0, Term: 1, Type: LogCommand, Data: "k0=v0"}}
	var reply AppendEntriesReply
	if err := cm.AppendEntries(aeArgs(-1, -1, 0, entries), &reply); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if !reply.Success {
		t.Fatal("reply.Success = false, want true")
	}

	after := persistenceSnapshotOf(cm)
	if got := storage.WriteCount() - writesBefore; got != 1 {
		t.Fatalf("%d записей на AppendEntries с продвижением фиксации, want 1", got)
	}
	commit := persistenceCellOf(after, persistSourceAECommit)
	commitBefore := persistenceCellOf(before, persistSourceAECommit)
	if commit.logCalls != commitBefore.logCalls+1 {
		t.Fatalf("ae_commit log = %d, want %d (ветка продвижения фиксации сохраняет журнал)",
			commit.logCalls, commitBefore.logCalls+1)
	}
	// processLogs сохраняет lastApplied собственным вызовом: третий общий
	// вызов допустим и обязателен для этого перехода. Журнал к этому моменту
	// уже сохранён, поэтому вызов скалярный.
	apply := persistenceCellOf(after, persistSourceApply)
	applyBefore := persistenceCellOf(before, persistSourceApply)
	if apply.scalarCalls != applyBefore.scalarCalls+1 {
		t.Fatalf("apply scalar = %d, want %d (processLogs после продвижения)",
			apply.scalarCalls, applyBefore.scalarCalls+1)
	}
	assertPersistenceInvariants(t, after.document())
}

// TestPersistCounters_FollowerTermScalarChangeWrites проверяет шаг вниз по
// большему терму: источник follower_term фиксируется как скалярный вызов,
// при этом смена скаляра даёт фактическую запись. Пульсовая ветка того же
// обработчика сохраняется скалярно и новых записей не создаёт.
func TestPersistCounters_FollowerTermScalarChangeWrites(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	dir := t.TempDir()
	cm, storage := newAEDurabilityCM(dir)
	cm.shutdownCh = make(chan struct{})
	cm.cmState.electionTimerDone = make(chan struct{})
	defer close(cm.shutdownCh)

	writesBefore := storage.WriteCount()

	// Терм 2 при текущем 1: обработчик обязан шагнуть в ведомые и сохранить
	// новый терм до зависимой обработки.
	var reply AppendEntriesReply
	args := aeArgs(-1, -1, -1, nil)
	args.Term = 2
	args.LeaderID = 99
	if err := cm.AppendEntries(args, &reply); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if !reply.Success {
		t.Fatal("reply.Success = false, want true")
	}

	after := persistenceSnapshotOf(cm)
	stepDown := persistenceCellOf(after, persistSourceFollowerTerm)
	if stepDown.scalarCalls != 1 || stepDown.logCalls != 0 {
		t.Fatalf("follower_term = (log %d, scalar %d), want (0, 1)", stepDown.logCalls, stepDown.scalarCalls)
	}
	if got := storage.WriteCount() - writesBefore; got != 1 {
		t.Fatalf("%d записей при смене скаляра, want 1 (новый терм)", got)
	}
	if finish := persistenceCellOf(after, persistSourceAEFinish); finish.scalarCalls != 1 {
		t.Fatalf("ae_finish scalar = %d, want 1 (сохранение до возврата обработчика)", finish.scalarCalls)
	}
	assertPersistenceInvariants(t, after.document())
}

// TestPersistCounters_CandidateFromLeadershipTransfer проверяет специальную
// ветку передачи лидерства: кандидат с флагом передачи сохраняет состояние
// с источником ae_transfer и отвечает отказом; других источников ветка
// не добавляет.
func TestPersistCounters_CandidateFromLeadershipTransfer(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	cm := &ConsensusModule{}
	cm.id = 7
	cm.storage = store.NewMapStorage()
	cm.shutdownCh = make(chan struct{})
	cm.cmState.state = Candidate
	cm.cmState.currentTerm = 5
	cm.cmState.votedFor = 7
	cm.cmState.electionTimerDone = make(chan struct{})
	cm.cmState.candidateFromLeadershipTransfer.Store(true)
	defer close(cm.shutdownCh)

	before := persistenceSnapshotOf(cm)
	var reply AppendEntriesReply
	if err := cm.AppendEntries(AppendEntriesArgs{
		RPCHeader:    RPCHeader{ProtocolVersion: ProtocolVersion, ServerID: 1},
		Term:         5,
		LeaderID:     1,
		PrevLogIndex: -1,
		PrevLogTerm:  -1,
	}, &reply); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if reply.Success {
		t.Fatal("reply.Success = true, want false (ветка передачи лидерства)")
	}

	after := persistenceSnapshotOf(cm)
	transfer := persistenceCellOf(after, persistSourceAETransfer)
	transferBefore := persistenceCellOf(before, persistSourceAETransfer)
	if transfer.scalarCalls != transferBefore.scalarCalls+1 {
		t.Fatalf("ae_transfer scalar = %d, want %d", transfer.scalarCalls, transferBefore.scalarCalls+1)
	}
	if transfer.logCalls != transferBefore.logCalls {
		t.Fatalf("ae_transfer log = %d, want без изменений", transfer.logCalls)
	}
	assertPersistenceInvariants(t, after.document())
}

// TestPersistCounters_RequestVote проверяет предоставление голоса с
// подъёмом терма: сначала сохраняется новый терм (follower_term), затем
// собственный голос (vote); обе записи — скалярные, фактических записей две
// (смена терма и смена votedFor).
func TestPersistCounters_RequestVote(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	dir := t.TempDir()
	cm, storage := newAEDurabilityCM(dir)
	cm.shutdownCh = make(chan struct{})
	cm.cmState.electionTimerDone = make(chan struct{})
	cm.cmState.leaderID = -1
	cm.cmState.configurations.latest = Configuration{ConfigServers: []ConfigServer{
		{ID: 0, Suffrage: Voter},
		{ID: 99, Suffrage: Voter},
	}}
	defer close(cm.shutdownCh)

	writesBefore := storage.WriteCount()

	args := RequestVoteArgs{
		RPCHeader:    RPCHeader{ProtocolVersion: ProtocolVersion, ServerID: 99},
		Term:         2,
		CandidateID:  99,
		LastLogIndex: -1,
		LastLogTerm:  -1,
	}
	var reply RequestVoteReply
	if err := cm.RequestVote(args, &reply); err != nil {
		t.Fatalf("RequestVote: %v", err)
	}
	if !reply.VoteGranted {
		t.Fatalf("VoteGranted = false, want true: %+v", reply)
	}

	after := persistenceSnapshotOf(cm)
	stepDown := persistenceCellOf(after, persistSourceFollowerTerm)
	if stepDown.scalarCalls != 1 {
		t.Fatalf("follower_term scalar = %d, want 1 (подъём терма)", stepDown.scalarCalls)
	}
	vote := persistenceCellOf(after, persistSourceVote)
	if vote.scalarCalls != 1 || vote.logCalls != 0 {
		t.Fatalf("vote = (log %d, scalar %d), want (0, 1)", vote.logCalls, vote.scalarCalls)
	}
	if got := storage.WriteCount() - writesBefore; got != 2 {
		t.Fatalf("%d записей на RequestVote, want 2 (терм и голос)", got)
	}
	assertPersistenceInvariants(t, after.document())
}

// TestPersistCounters_ElectionLeaderAppendApply проверяет один узел целиком:
// выборы сохраняют терм и голос (candidate), добавление записи лидером
// (leader_append) и её применение (apply) фиксируются своими источниками.
func TestPersistCounters_ElectionLeaderAppendApply(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	storage := store.NewFileStorage(t.TempDir())
	ready := make(chan any)
	close(ready)
	transport := transp.NewInmemTransport("single")
	cm := NewConsensusModule(0, []int{}, transport, storage, newSnapshotTestFSM(), ready)
	defer func() {
		cm.Stop()
		transport.Close()
	}()

	waitForLeader(t, cm, 5*time.Second)

	candidate := persistenceCellOf(persistenceSnapshotOf(cm), persistSourceCandidate)
	if candidate.logCalls+candidate.scalarCalls < 1 {
		t.Fatalf("candidate = (log %d, scalar %d), want хотя бы один сохранённый вызов",
			candidate.logCalls, candidate.scalarCalls)
	}

	future := cm.Apply("k0=v0", 0)
	if err := future.Error(); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if got := persistenceCellOf(persistenceSnapshotOf(cm), persistSourceLeaderAppend).logCalls; got < 1 {
		t.Fatalf("leader_append log = %d, want >= 1", got)
	}
	// Применение зафиксированной записи сохраняет lastApplied собственным
	// вызовом с источником apply; журнал к этому моменту уже сохранён
	// добавлением лидера, поэтому вызов скалярный. При одном узле фиксация
	// происходит синхронно с добавлением, поэтому ожидание короткое.
	err := waitCond("apply persist observation", 2*time.Second, func() bool {
		cell := persistenceCellOf(persistenceSnapshotOf(cm), persistSourceApply)
		return cell.logCalls+cell.scalarCalls >= 1
	}, func() string {
		cell := persistenceCellOf(persistenceSnapshotOf(cm), persistSourceApply)
		return "apply = (log " + itoa(int(cell.logCalls)) + ", scalar " + itoa(int(cell.scalarCalls)) + ")"
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := persistenceSnapshotOf(cm)
	assertPersistenceInvariants(t, snapshot.document())
}

// TestPersistCounters_InstallSnapshot проверяет установку снимка: состояние
// согласуется и сохраняется с источником install_snapshot в полном режиме.
func TestPersistCounters_InstallSnapshot(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	cm, _ := newInstallSnapshotCM()
	data := installSnapshotRequestData(t, map[string]string{"k0": "v0"})
	req := installSnapshotRequest(99, 12, 2, data)

	reply := driveInstallSnapshotRPC(t, cm, req, data)
	if !reply.Success {
		t.Fatalf("InstallSnapshot: Success=false: %+v", reply)
	}

	cell := persistenceCellOf(persistenceSnapshotOf(cm), persistSourceInstallSnapshot)
	if cell.logCalls != 1 || cell.scalarCalls != 0 {
		t.Fatalf("install_snapshot = (log %d, scalar %d), want (1, 0)", cell.logCalls, cell.scalarCalls)
	}
	snapshot := persistenceSnapshotOf(cm)
	assertPersistenceInvariants(t, snapshot.document())
}

// TestPersistCounters_TakeSnapshot проверяет создание локального снимка:
// после запечатывания тела и уплотнения журнала состояние сохраняется
// с источником take_snapshot в полном режиме.
func TestPersistCounters_TakeSnapshot(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	transport := transp.NewInmemTransport("single")
	storage := store.NewFileStorage(t.TempDir())
	cm := NewConsensusModule(
		0, []int{}, transport,
		storage, newSnapshotTestFSM(), closedReadyChan(),
		store.NewInmemSnapshot(),
	)
	defer func() {
		cm.Stop()
		transport.Close()
	}()

	waitForLeader(t, cm, 5*time.Second)
	cm.SetSnapshotConfig(4, 2, 50*time.Millisecond)

	const numCommands = 10
	for i := 0; i < numCommands; i++ {
		cmd := "k" + itoa(i) + "=v" + itoa(i)
		if err := cm.Apply(cmd, 0).Error(); err != nil {
			t.Fatalf("Apply %q: %v", cmd, err)
		}
	}
	waitForSnapshotAndCompaction(t, cm, numCommands)

	if got := persistenceCellOf(persistenceSnapshotOf(cm), persistSourceTakeSnapshot).logCalls; got < 1 {
		t.Fatalf("take_snapshot log = %d, want >= 1", got)
	}
}

// closedReadyChan возвращает закрытый канал готовности узла.
func closedReadyChan() chan any {
	ready := make(chan any)
	close(ready)
	return ready
}

// TestPersistCounters_StartupRestore проверяет самолечение при старте:
// снимок в хранилище новее lastSnapshotIndex на диске (окно сбоя между
// Close снимка и сохранением) — метаданные исправляются и сохраняются
// с источником startup_restore; блокировка берётся внутри восстановления.
func TestPersistCounters_StartupRestore(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	snapStore := store.NewInmemSnapshot()
	sink, err := snapStore.Create(10, 2, 0, Configuration{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := gob.NewEncoder(sink).Encode(map[string]string{"k0": "v0"}); err != nil {
		t.Fatalf("gob encode: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	cm := &ConsensusModule{
		storage:       store.NewMapStorage(),
		snapshotStore: snapStore,
		fsm:           newSnapshotTestFSM(),
	}
	// Отставшие метаданные на диске: снимок уже запечатан (индекс 10),
	// а ключ lastSnapshotIndex содержит 9.
	cm.cmState.lastSnapshotIndex = 9
	cm.cmState.lastSnapshotTerm = 1
	cm.cmState.lastLogIndex = 10
	cm.cmState.lastLogTerm = 2

	before := persistenceSnapshotOf(cm)
	if err := cm.restoreFromSnapshotStore(); err != nil {
		t.Fatalf("restoreFromSnapshotStore: %v", err)
	}

	if cm.cmState.lastSnapshotIndex != 10 {
		t.Fatalf("lastSnapshotIndex = %d, want 10 (из снимка)", cm.cmState.lastSnapshotIndex)
	}
	after := persistenceSnapshotOf(cm)
	restore := persistenceCellOf(after, persistSourceStartupRestore)
	restoreBefore := persistenceCellOf(before, persistSourceStartupRestore)
	if restore.scalarCalls != restoreBefore.scalarCalls+1 || restore.logCalls != restoreBefore.logCalls {
		t.Fatalf("startup_restore = (log %d, scalar %d), want (log %d, scalar %d)",
			restore.logCalls, restore.scalarCalls, restoreBefore.logCalls, restoreBefore.scalarCalls+1)
	}
	assertPersistenceInvariants(t, after.document())
}
