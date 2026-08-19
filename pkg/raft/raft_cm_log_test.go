package raft

import (
	"testing"

	"github.com/fortytw2/leaktest"
)

// --- Тесты rebuildLastLog и INV-S2 (TASK-001, ADR-P07-001). ---
//
// Инвариант INV-S2: при пустом суффиксе журнала lastLogIndex/lastLogTerm
// берутся из lastSnapshotIndex/lastSnapshotTerm. Для свежего узла без
// снапшота (lastSnapshotIndex == -1) поведение прежнее: (-1, -1).
// Для непустого среза поведение не меняется: последняя запись среза.

// TestRebuildLastLog_EmptyLogWithoutSnapshot проверяет, что поведение
// свежего узла не изменилось: пустой журнал и lastSnapshotIndex == -1
// дают (-1, -1).
func TestRebuildLastLog_EmptyLogWithoutSnapshot(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	cm := &ConsensusModule{}
	cm.cmState.log = nil
	cm.cmState.lastSnapshotIndex = -1
	cm.cmState.lastSnapshotTerm = -1

	cm.rebuildLastLog()

	if cm.cmState.lastLogIndex != -1 || cm.cmState.lastLogTerm != -1 {
		t.Fatalf("lastLog = (%d, %d), want (-1, -1) — fresh node behavior must not change",
			cm.cmState.lastLogIndex, cm.cmState.lastLogTerm)
	}
}

// TestRebuildLastLog_EmptyLogWithSnapshot — ядро INV-S2 (ADR-P07-001):
// пустой журнал при установленном снапшоте даёт (N, T) от снапшота.
// Именно эта ветка ломала догон в инциденте: rebuildLastLog затирал
// корректные lastLogIndex/lastLogTerm в -1/-1.
func TestRebuildLastLog_EmptyLogWithSnapshot(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	cm := &ConsensusModule{}
	cm.cmState.log = nil
	cm.cmState.lastSnapshotIndex = 10
	cm.cmState.lastSnapshotTerm = 2

	cm.rebuildLastLog()

	if cm.cmState.lastLogIndex != 10 || cm.cmState.lastLogTerm != 2 {
		t.Fatalf("lastLog = (%d, %d), want (10, 2) — INV-S2 violated",
			cm.cmState.lastLogIndex, cm.cmState.lastLogTerm)
	}
}

// TestRebuildLastLog_NonEmptyLog проверяет, что при непустом срезе
// поведение не меняется: берётся последняя запись среза, граница
// снапшота не участвует.
func TestRebuildLastLog_NonEmptyLog(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	cm := &ConsensusModule{}
	cm.cmState.log = []LogEntry{
		{Index: 5, Term: 1},
		{Index: 7, Term: 3},
	}
	cm.cmState.lastSnapshotIndex = 5
	cm.cmState.lastSnapshotTerm = 1

	cm.rebuildLastLog()

	if cm.cmState.lastLogIndex != 7 || cm.cmState.lastLogTerm != 3 {
		t.Fatalf("lastLog = (%d, %d), want (7, 3)", cm.cmState.lastLogIndex, cm.cmState.lastLogTerm)
	}
}

// TestRebuildLastLog_AppendEntriesCallSite фиксирует неизменность
// поведения call site raft_cm_rpc.go:152 (AppendEntries с вставкой
// записей): срез после вставки гарантированно непуст, ветка INV-S2
// недостижима, результат — последняя вставленная запись (ADR-P07-001,
// анализ побочных эффектов п.1).
func TestRebuildLastLog_AppendEntriesCallSite(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	cm := &ConsensusModule{}
	cm.storage = NewMapStorage()
	cm.cmState.state = Follower
	cm.cmState.currentTerm = 1
	cm.cmState.votedFor = -1
	cm.cmState.lastSnapshotIndex = -1
	cm.cmState.lastSnapshotTerm = -1
	cm.cmState.termIndexMap = make(map[int]int)
	cm.cmState.commitIndex = -1

	var reply AppendEntriesReply
	err := cm.AppendEntries(AppendEntriesArgs{
		RPCHeader:    RPCHeader{ProtocolVersion: ProtocolVersion, ServerID: 99},
		Term:         1,
		LeaderID:     99,
		PrevLogIndex: -1,
		PrevLogTerm:  -1,
		Entries: []LogEntry{
			{Index: 0, Term: 1},
			{Index: 1, Term: 1},
		},
		LeaderCommit: -1,
	}, &reply)
	if err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if !reply.Success {
		t.Fatalf("AppendEntries reply: %+v, want Success", reply)
	}
	if cm.cmState.lastLogIndex != 1 || cm.cmState.lastLogTerm != 1 {
		t.Fatalf("lastLog = (%d, %d), want (1, 1) — call site behavior must not change",
			cm.cmState.lastLogIndex, cm.cmState.lastLogTerm)
	}
}

// TestRebuildLastLog_RestoreFromStorageCallSite фиксирует неизменность
// поведения call site raft_cm_storage.go:96 (restoreFromStorage):
// rebuildLastLog вызывается до загрузки lastSnapshotIndex из storage
// (поле ещё -1, как в конструкторе), поэтому пустой журнал даёт (-1, -1)
// (ADR-P07-001, анализ побочных эффектов п.2).
func TestRebuildLastLog_RestoreFromStorageCallSite(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	storage := NewMapStorage()
	storage.Set("currentTerm", gobEncode(t, 1))
	storage.Set("votedFor", gobEncode(t, -1))
	storage.Set("log", gobEncode(t, []LogEntry{}))

	cm := &ConsensusModule{storage: storage}
	// Конструктор устанавливает lastSnapshotIndex = -1 до restoreFromStorage.
	cm.cmState.lastSnapshotIndex = -1
	cm.cmState.lastSnapshotTerm = -1

	cm.restoreFromStorage()

	if cm.cmState.lastLogIndex != -1 || cm.cmState.lastLogTerm != -1 {
		t.Fatalf("lastLog = (%d, %d), want (-1, -1) — restore call site behavior must not change",
			cm.cmState.lastLogIndex, cm.cmState.lastLogTerm)
	}
}

// TestRebuildLastLog_RestoreFromStorageNonEmptyLog — тот же call site
// с непустым журналом в storage: последняя запись (поведение прежнее).
func TestRebuildLastLog_RestoreFromStorageNonEmptyLog(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	storage := NewMapStorage()
	storage.Set("currentTerm", gobEncode(t, 1))
	storage.Set("votedFor", gobEncode(t, -1))
	storage.Set("log", gobEncode(t, []LogEntry{
		{Index: 0, Term: 1},
		{Index: 1, Term: 1},
	}))

	cm := &ConsensusModule{storage: storage}
	cm.cmState.lastSnapshotIndex = -1
	cm.cmState.lastSnapshotTerm = -1

	cm.restoreFromStorage()

	if cm.cmState.lastLogIndex != 1 || cm.cmState.lastLogTerm != 1 {
		t.Fatalf("lastLog = (%d, %d), want (1, 1)", cm.cmState.lastLogIndex, cm.cmState.lastLogTerm)
	}
}
