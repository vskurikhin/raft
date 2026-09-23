package raft

import (
	"encoding/gob"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/vskurikhin/raft/pkg/raft/store"
	"github.com/vskurikhin/raft/pkg/raft/transp"
)

// -- Тесты грязных периодов на штатных переходах: пакет лидера, приём и
// -- замена суффикса ведомым, повтор AppendEntries, уплотнение, установка
// -- снимка, начальный период пустого журнала и отмена периода при
// -- восстановлении. Каждый тест сравнивает дельту накопительных агрегатов
// -- до и после перехода и проверяет, что незавершённых периодов не осталось. --

// newDirtyFollowerCM собирает ведомого с постоянным хранилищем и журналом
// из entries записей терма 1 (индексы 1..entries). Исходное сохранение
// фиксирует установившуюся точку наблюдений: дельты считаются от неё.
func newDirtyFollowerCM(t *testing.T, entries int) (*ConsensusModule, *store.FileStorage) {
	t.Helper()
	cm, storage := newAEDurabilityCM(t.TempDir())
	cm.cmState.log = make([]LogEntry, entries)
	for i := range cm.cmState.log {
		cm.cmState.log[i] = LogEntry{Index: i + 1, Term: 1, Type: LogCommand}
	}
	cm.cmState.termIndexMap = make(map[int]int)
	if entries > 0 {
		cm.cmState.lastLogIndex = entries
		cm.cmState.lastLogTerm = 1
		cm.cmState.termIndexMap[1] = entries
	}
	// Хранилище приводится в соответствие памяти: посев выполняется полной
	// заменой журнала, как после первого персиста. Состояние достижимо.
	storage.RewriteLog(cm.cmState.log)
	cm.clearLogDirtyLocked()
	return cm, storage
}

// assertNoActiveDirty проверяет, что к моменту вызова незавершённого
// грязного периода нет: обычная отметка обязана закрыться до конца операции.
func assertNoActiveDirty(t *testing.T, snap dirtySnapshot) {
	t.Helper()
	if snap.active.present {
		t.Fatalf("незавершённый период: причина %q, отметок %d",
			dirtyCauseNames[snap.active.cause], snap.active.marks)
	}
}

// TestDirtyTransitions_LeaderAppendPacket проверяет пакет лидера: число
// добавлений равно длине фактически добавленного среза, период отмечается
// один раз и закрывается немедленным полным сохранением.
func TestDirtyTransitions_LeaderAppendPacket(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	cm, _ := newDirtyFollowerCM(t, 0)
	cm.leaderState.inflight = make(map[int]*logFuture)
	cm.leaderState.matchIndex = make(map[int]int)
	before := dirtySnapshotOf(cm)

	futures := make([]*logFuture, 3)
	for i := range futures {
		futures[i] = &logFuture{
			deferError: deferError{errCh: make(chan error, 1)},
			log:        LogEntry{Type: LogCommand, Data: []byte("x")},
		}
	}
	cm.mu.Lock()
	cm.dispatchLogsLocked(futures)
	closed := !cm.dirty.open
	cm.mu.Unlock()
	if !closed {
		t.Fatal("пакет лидера оставил период открытым")
	}

	after := dirtySnapshotOf(cm)
	leader, leaderBefore := dirtyCauseOf(after, dirtyCauseLeaderAppend), dirtyCauseOf(before, dirtyCauseLeaderAppend)
	if leader.periods != leaderBefore.periods+1 {
		t.Fatalf("leader_append периодов = %d, want %d", leader.periods, leaderBefore.periods+1)
	}
	if leader.sumAdditions != leaderBefore.sumAdditions+3 {
		t.Fatalf("leader_append добавления = %d, want %d", leader.sumAdditions, leaderBefore.sumAdditions+3)
	}
	if leader.sumMarks != leaderBefore.sumMarks+1 {
		t.Fatalf("leader_append отметки = %d, want %d", leader.sumMarks, leaderBefore.sumMarks+1)
	}
	assertNoActiveDirty(t, after)
}

// TestDirtyTransitions_FollowerAppendCountsAddedSlice проверяет приём
// записей ведомым: добавления считаются по фактически приписанному остатку,
// совпавший префикс не считается, а заменённые записи считаются даже при
// уменьшении последнего индекса журнала.
func TestDirtyTransitions_FollowerAppendCountsAddedSlice(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	cm, _ := newDirtyFollowerCM(t, 5)
	before := dirtySnapshotOf(cm)

	entries := []LogEntry{
		{Index: 3, Term: 2, Type: LogCommand},
		{Index: 4, Term: 2, Type: LogCommand},
		{Index: 5, Term: 2, Type: LogCommand},
	}
	var reply AppendEntriesReply
	if err := cm.AppendEntries(aeArgs(2, 1, -1, entries), &reply); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if !reply.Success {
		t.Fatal("reply.Success = false, want true")
	}

	after := dirtySnapshotOf(cm)
	follower, followerBefore := dirtyCauseOf(after, dirtyCauseFollowerAppend), dirtyCauseOf(before, dirtyCauseFollowerAppend)
	if follower.periods != followerBefore.periods+1 || follower.sumAdditions != followerBefore.sumAdditions+3 {
		t.Fatalf("follower_append = (периодов %d, добавлений %d), want (%d, %d)",
			follower.periods, follower.sumAdditions, followerBefore.periods+1, followerBefore.sumAdditions+3)
	}
	if cm.cmState.lastLogIndex != 5 {
		t.Fatalf("lastLogIndex = %d, want 5 (замена без роста)", cm.cmState.lastLogIndex)
	}
	assertNoActiveDirty(t, after)

	// Замена суффикса с уменьшением последнего индекса: приписано две
	// записи, последний индекс журнала уменьшился с 5 до 4.
	shortened := []LogEntry{
		{Index: 3, Term: 3, Type: LogCommand},
		{Index: 4, Term: 3, Type: LogCommand},
	}
	if err := cm.AppendEntries(aeArgs(2, 1, -1, shortened), &reply); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if !reply.Success {
		t.Fatal("reply.Success = false, want true")
	}
	final := dirtySnapshotOf(cm)
	finalFollower := dirtyCauseOf(final, dirtyCauseFollowerAppend)
	if finalFollower.periods != follower.periods+1 || finalFollower.sumAdditions != follower.sumAdditions+2 {
		t.Fatalf("замена с уменьшением = (периодов %d, добавлений %d), want (%d, %d)",
			finalFollower.periods, finalFollower.sumAdditions, follower.periods+1, follower.sumAdditions+2)
	}
	if cm.cmState.lastLogIndex != 4 || len(cm.cmState.log) != 4 {
		t.Fatalf("журнал = (last %d, len %d), want (4, 4)", cm.cmState.lastLogIndex, len(cm.cmState.log))
	}
}

// TestDirtyTransitions_RepeatAppendEntriesZeroAdditions проверяет совпадающий
// повтор AppendEntries: префикс уже durable, фактического добавления нет,
// новой отметки не появляется и накопительные агрегаты не меняются.
func TestDirtyTransitions_RepeatAppendEntriesZeroAdditions(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	cm, _ := newDirtyFollowerCM(t, 5)
	entries := []LogEntry{
		{Index: 3, Term: 2, Type: LogCommand},
		{Index: 4, Term: 2, Type: LogCommand},
		{Index: 5, Term: 2, Type: LogCommand},
	}
	var reply AppendEntriesReply
	if err := cm.AppendEntries(aeArgs(2, 1, -1, entries), &reply); err != nil {
		t.Fatalf("первый AppendEntries: %v", err)
	}
	afterFirst := dirtySnapshotOf(cm)

	if err := cm.AppendEntries(aeArgs(2, 1, -1, entries), &reply); err != nil {
		t.Fatalf("повторный AppendEntries: %v", err)
	}
	if !reply.Success {
		t.Fatal("повторный reply.Success = false, want true")
	}

	afterRepeat := dirtySnapshotOf(cm)
	if afterRepeat != afterFirst {
		t.Fatalf("совпадающий повтор изменил наблюдения:\nbefore = %+v\nafter  = %+v", afterFirst, afterRepeat)
	}
	assertNoActiveDirty(t, afterRepeat)
}

// TestDirtyTransitions_AECommitClosesBeforeUnlock проверяет ветку продвижения
// индекса фиксации: период закрывается внутри appendMatchingEntriesLocked,
// то есть до разблокирования cm.mu для processLogs, а применение записей
// к машине состояний в закрытие не входит.
func TestDirtyTransitions_AECommitClosesBeforeUnlock(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	cm, _ := newDirtyFollowerCM(t, 5)
	before := dirtySnapshotOf(cm)
	entries := []LogEntry{{Index: 6, Term: 2, Type: LogCommand}}
	args := aeArgs(5, 1, 5, entries)
	var reply AppendEntriesReply

	cm.mu.Lock()
	commitIdx, advanced := cm.appendMatchingEntriesLocked(&args, &reply)
	closed := !cm.dirty.open
	cm.mu.Unlock()
	if !advanced || commitIdx != 5 {
		t.Fatalf("appendMatchingEntriesLocked = (%d, %v), want (5, true)", commitIdx, advanced)
	}
	if !closed {
		t.Fatal("период оставался открытым при разблокировании для processLogs")
	}

	after := dirtySnapshotOf(cm)
	follower, followerBefore := dirtyCauseOf(after, dirtyCauseFollowerAppend), dirtyCauseOf(before, dirtyCauseFollowerAppend)
	if follower.periods != followerBefore.periods+1 || follower.sumAdditions != followerBefore.sumAdditions+1 {
		t.Fatalf("период продвижения фиксации = (периодов %d, добавлений %d), want (%d, %d)",
			follower.periods, follower.sumAdditions, followerBefore.periods+1, followerBefore.sumAdditions+1)
	}
}

// TestDirtyTransitions_AppendEntriesCommitAdvanceSinglePeriod проверяет
// обработчик AppendEntries целиком с продвижением фиксации: несмотря на
// несколько сохранений внутри обработчика (продвижение фиксации и финальное),
// период учитывается ровно один раз.
func TestDirtyTransitions_AppendEntriesCommitAdvanceSinglePeriod(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	dir := t.TempDir()
	cm, storage := newAEDurabilityCM(dir)
	startAEDurabilityFSM(t, cm)
	defer close(cm.shutdownCh)

	before := dirtySnapshotOf(cm)
	persistBefore := persistenceSnapshotOf(cm)
	scalarsBefore := storage.WriteCount()

	entries := []LogEntry{{Index: 0, Term: 1, Type: LogCommand, Data: "k0=v0"}}
	var reply AppendEntriesReply
	if err := cm.AppendEntries(aeArgs(-1, -1, 0, entries), &reply); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if !reply.Success {
		t.Fatal("reply.Success = false, want true")
	}
	persistAfter := persistenceSnapshotOf(cm)
	if got := persistAfter.logWrites - persistBefore.logWrites; got != 1 {
		t.Fatalf("%d записей журнала на продвижение фиксации, want 1", got)
	}
	if got := storage.WriteCount() - scalarsBefore; got != 0 {
		t.Fatalf("%d скалярных записей на продвижение фиксации, want 0", got)
	}

	after := dirtySnapshotOf(cm)
	follower, followerBefore := dirtyCauseOf(after, dirtyCauseFollowerAppend), dirtyCauseOf(before, dirtyCauseFollowerAppend)
	if follower.periods != followerBefore.periods+1 || follower.sumAdditions != followerBefore.sumAdditions+1 {
		t.Fatalf("период обработчика = (периодов %d, добавлений %d), want (%d, %d)",
			follower.periods, follower.sumAdditions, followerBefore.periods+1, followerBefore.sumAdditions+1)
	}
	if follower.sumMarks != followerBefore.sumMarks+1 {
		t.Fatalf("отметок периода = %d, want %d", follower.sumMarks, followerBefore.sumMarks+1)
	}
	assertNoActiveDirty(t, after)
}

// TestDirtyTransitions_CompactZeroAdditions проверяет уплотнение: журнал
// переписывается, но добавлений нет; период отмечается с нулём и закрывается
// полным сохранением.
func TestDirtyTransitions_CompactZeroAdditions(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	cm, _ := newDirtyFollowerCM(t, 10)
	before := dirtySnapshotOf(cm)

	cm.mu.Lock()
	cm.compactLogsLocked(6)
	if !cm.dirty.open {
		t.Fatal("уплотнение не открыло период")
	}
	if cm.dirty.additions != 0 {
		t.Fatalf("уплотнение дало %d добавлений, want 0", cm.dirty.additions)
	}
	cm.persistToStorageLocked(persistSourceTest)
	closed := !cm.dirty.open
	cm.mu.Unlock()
	if !closed {
		t.Fatal("полное сохранение не закрыло период уплотнения")
	}

	after := dirtySnapshotOf(cm)
	compact, compactBefore := dirtyCauseOf(after, dirtyCauseCompact), dirtyCauseOf(before, dirtyCauseCompact)
	if compact.periods != compactBefore.periods+1 {
		t.Fatalf("compact периодов = %d, want %d", compact.periods, compactBefore.periods+1)
	}
	if compact.sumAdditions != compactBefore.sumAdditions || compact.sumAdditions != 0 {
		t.Fatalf("compact добавлений = %d, want 0", compact.sumAdditions)
	}
	if compact.sumAgeNS <= 0 {
		t.Fatalf("compact возраст = %d, want > 0", compact.sumAgeNS)
	}
}

// TestDirtyTransitions_InstallSnapshotZeroAdditions проверяет установку
// снимка на реальном пути обработчика: журнал переписывается, добавлений нет,
// период учитывается ровно один раз.
func TestDirtyTransitions_InstallSnapshotZeroAdditions(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	cm, _ := newInstallSnapshotCM()
	before := dirtySnapshotOf(cm)

	data := installSnapshotRequestData(t, map[string]string{"k0": "v0"})
	req := installSnapshotRequest(99, 12, 2, data)
	reply := driveInstallSnapshotRPC(t, cm, req, data)
	if !reply.Success {
		t.Fatalf("InstallSnapshot: Success=false: %+v", reply)
	}

	after := dirtySnapshotOf(cm)
	installed, installedBefore := dirtyCauseOf(after, dirtyCauseInstallSnapshot), dirtyCauseOf(before, dirtyCauseInstallSnapshot)
	if installed.periods != installedBefore.periods+1 {
		t.Fatalf("install_snapshot периодов = %d, want %d", installed.periods, installedBefore.periods+1)
	}
	if installed.sumAdditions != installedBefore.sumAdditions || installed.sumAdditions != 0 {
		t.Fatalf("install_snapshot добавлений = %d, want 0", installed.sumAdditions)
	}
	assertNoActiveDirty(t, after)
}

// TestDirtyTransitions_InitialEmptyLog проверяет первый запуск без данных:
// служебный начальный период пустого журнала закрывается первым полным
// сохранением выборов с нулём добавлений и не остаётся открытым.
func TestDirtyTransitions_InitialEmptyLog(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	storage := store.NewFileStorage(t.TempDir())
	transport := transp.NewInmemTransport("single")
	cm := NewConsensusModule(
		0, []int{}, transport,
		storage, newSnapshotTestFSM(), closedReadyChan(),
	)
	defer func() {
		cm.Stop()
		transport.Close()
	}()

	waitForLeader(t, cm, 5*time.Second)

	snap := dirtySnapshotOf(cm)
	initial := dirtyCauseOf(snap, dirtyCauseInitial)
	if initial.periods != 1 || initial.sumMarks != 1 || initial.sumAdditions != 0 {
		t.Fatalf("initial = (периодов %d, отметок %d, добавлений %d), want (1, 1, 0)",
			initial.periods, initial.sumMarks, initial.sumAdditions)
	}
	if initial.sumAgeNS <= 0 || initial.sumWaitNS <= 0 {
		t.Fatalf("initial время = (age %d, wait %d), want > 0", initial.sumAgeNS, initial.sumWaitNS)
	}
	if snap.unattributed != 0 {
		t.Fatalf("unattributed = %d, want 0 (все сохранения атрибутированы)", snap.unattributed)
	}
	if snap.active.present && snap.active.cause == dirtyCauseInitial {
		t.Fatal("начальный период остался открытым")
	}
}

// TestDirtyTransitions_RestoreCancelsInitial проверяет восстановление готового
// хранилища: служебный начальный период отменяется, а не завершается мнимым
// персистом; самолечение метаданных снимка (скалярное сохранение) не попадает
// ни в группы, ни в счётчик необъяснённых наблюдений.
func TestDirtyTransitions_RestoreCancelsInitial(t *testing.T) {
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

	storage := store.NewMapStorage()
	storage.Set(_storageKeyCurrentTerm, gobEncode(t, 3))
	storage.Set(_storageKeyVotedFor, gobEncode(t, 1))
	storage.RewriteLog([]LogEntry{{Index: 0, Term: 1}})

	cm := &ConsensusModule{
		storage:       storage,
		snapshotStore: snapStore,
		fsm:           newSnapshotTestFSM(),
	}
	// Отставшие метаданные на диске: снимок уже запечатан (индекс 10),
	// а ключ lastSnapshotIndex отсутствует. Начальный период отмечен, как
	// в конструкторе до восстановления.
	cm.cmState.lastSnapshotIndex = 9
	cm.cmState.lastSnapshotTerm = 1
	cm.markLogRewriteDirtyLocked()
	cm.dirty.markAt(dirtyCauseInitial, 0, time.Now())

	restoreData(cm)

	if cm.cmState.lastSnapshotIndex != 10 {
		t.Fatalf("lastSnapshotIndex = %d, want 10 (самолечение из снимка)", cm.cmState.lastSnapshotIndex)
	}
	snap := dirtySnapshotOf(cm)
	if snap.causes[dirtyCauseInitial].periods != 0 {
		t.Fatalf("отмена начального периода создала наблюдение: %+v", snap.causes[dirtyCauseInitial])
	}
	if snap.unattributed != 0 {
		t.Fatalf("unattributed = %d, want 0 (скалярное самолечение не наблюдение персиста)", snap.unattributed)
	}
	if snap.active.present {
		t.Fatal("после восстановления остался открытый период")
	}
}
