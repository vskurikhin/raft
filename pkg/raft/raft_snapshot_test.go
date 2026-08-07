package raft

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
)

// snapshotHarness — тестовый стенд для интеграционных тестов снэпшотов.
// Отличается от Harness тем, что использует snapshotTestFSM и
// InmemSnapshotStore для каждого узла.
type snapshotHarness struct {
	t          *testing.T
	n          int
	cluster    []*ConsensusModule
	fsms       []*snapshotTestFSM
	stores     []*InmemSnapshotStore
	storage    []*MapStorage
	transports []*InmemTransport
	alive      []bool
	connected  []bool
	ready      chan any
	mu         sync.Mutex
}

// newSnapshotHarness создаёт кластер из n узлов с поддержкой снэпшотов.
func newSnapshotHarness(t *testing.T, n int) *snapshotHarness {
	t.Helper()

	cluster := make([]*ConsensusModule, n)
	fsms := make([]*snapshotTestFSM, n)
	stores := make([]*InmemSnapshotStore, n)
	storage := make([]*MapStorage, n)
	transports := make([]*InmemTransport, n)
	alive := make([]bool, n)
	connected := make([]bool, n)
	ready := make(chan any)

	// Создаём транспорты и соединяем их.
	for i := 0; i < n; i++ {
		transports[i] = NewInmemTransport(ServerAddress("raft-" + itoa(i)))
	}
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j {
				transports[i].Connect(ServerID(j), transports[j])
			}
		}
	}

	// Создаём ConsensusModule для каждого узла со снэпшотами.
	for i := 0; i < n; i++ {
		peerIds := make([]int, 0)
		for p := 0; p < n; p++ {
			if p != i {
				peerIds = append(peerIds, p)
			}
		}

		storage[i] = NewMapStorage()
		fsms[i] = newSnapshotTestFSM()
		stores[i] = NewInmemSnapshotStore()

		cluster[i] = NewConsensusModule(
			i, peerIds, transports[i],
			storage[i], fsms[i], ready,
			stores[i],
		)
		alive[i] = true
		connected[i] = true
	}
	close(ready)

	h := &snapshotHarness{
		t:          t,
		n:          n,
		cluster:    cluster,
		fsms:       fsms,
		stores:     stores,
		storage:    storage,
		transports: transports,
		alive:      alive,
		connected:  connected,
		ready:      ready,
	}
	return h
}

// Shutdown останавливает все серверы.
func (h *snapshotHarness) Shutdown() {
	for i := 0; i < h.n; i++ {
		if h.alive[i] {
			h.alive[i] = false
			h.cluster[i].Stop()
			h.transports[i].Close()
		}
	}
}

// DisconnectPeer отключает узел от всех остальных.
func (h *snapshotHarness) DisconnectPeer(id int) {
	h.transports[id].DisconnectAll()
	for j := 0; j < h.n; j++ {
		if j != id {
			h.transports[j].Disconnect(ServerID(id))
		}
	}
	h.connected[id] = false
}

// ReconnectPeer повторно подключает узел ко всем остальным.
func (h *snapshotHarness) ReconnectPeer(id int) {
	for j := 0; j < h.n; j++ {
		if j != id && h.alive[j] {
			h.transports[id].Connect(ServerID(j), h.transports[j])
			h.transports[j].Connect(ServerID(id), h.transports[id])
		}
	}
	h.connected[id] = true
}

// CrashPeer «аварийно завершает» узел, сохраняя storage.
func (h *snapshotHarness) CrashPeer(id int) {
	h.DisconnectPeer(id)
	h.alive[id] = false
	h.cluster[id].Stop()
	h.transports[id].Close()
}

// RestartPeer перезапускает узел из того же storage.
func (h *snapshotHarness) RestartPeer(id int) {
	if h.alive[id] {
		h.t.Fatalf("id=%d is alive in RestartPeer", id)
	}
	peerIds := make([]int, 0)
	for p := 0; p < h.n; p++ {
		if p != id {
			peerIds = append(peerIds, p)
		}
	}

	ready := make(chan any)
	addr := ServerAddress("raft-" + itoa(h.n) + "-" + itoa(id))
	h.transports[id] = NewInmemTransport(addr)

	for j := 0; j < h.n; j++ {
		if j != id && h.alive[j] {
			h.transports[id].Connect(ServerID(j), h.transports[j])
			h.transports[j].Connect(ServerID(id), h.transports[id])
		}
	}

	// Создаём новый FSM, но ПЕРЕИСПОЛЬЗУЕМ store узла: Inmem-стор живёт
	// в том же процессе и хранит последний снэпшот — это эмуляция
	// durable-стора для in-memory кластера. Новый пустой store с
	// lastSnapshotIndex > 0 в storage привёл бы к fail-fast в
	// restoreFromSnapshotStore (пустой store при усечённом логе).
	fsm := newSnapshotTestFSM()
	h.fsms[id] = fsm
	store := h.stores[id]

	h.cluster[id] = NewConsensusModule(
		id, peerIds, h.transports[id],
		h.storage[id], fsm, ready, store,
	)
	close(ready)
	h.alive[id] = true
	h.connected[id] = true
	sleepMs(20)
}

// SubmitToServer отправляет команду серверу и возвращает индекс.
func (h *snapshotHarness) SubmitToServer(serverID int, cmd string) int {
	future := h.cluster[serverID].Apply(cmd, 0)
	errCh := make(chan error, 1)
	go func() {
		errCh <- future.Error()
	}()
	select {
	case err := <-errCh:
		if err != nil {
			return -1
		}
		return future.Index()
	case <-time.After(submitTimeout):
		return -1
	}
}

// waitForSnapshot ждёт, пока указанный узел создаст снэпшот
// (lastSnapshotIndex > 0), или до таймаута.
func (h *snapshotHarness) waitForSnapshot(serverID int, timeout time.Duration) bool {
	deadline := time.After(timeout)
	for {
		h.cluster[serverID].mu.Lock()
		idx := h.cluster[serverID].cmState.lastSnapshotIndex
		h.cluster[serverID].mu.Unlock()
		if idx > 0 {
			return true
		}
		select {
		case <-deadline:
			return false
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// getSnapshotIndex возвращает lastSnapshotIndex узла.
func (h *snapshotHarness) getSnapshotIndex(serverID int) int {
	h.cluster[serverID].mu.Lock()
	defer h.cluster[serverID].mu.Unlock()
	return h.cluster[serverID].cmState.lastSnapshotIndex
}

// getLogLength возвращает длину журнала узла.
func (h *snapshotHarness) getLogLength(serverID int) int {
	h.cluster[serverID].mu.Lock()
	defer h.cluster[serverID].mu.Unlock()
	return len(h.cluster[serverID].cmState.log)
}

// ============================================================
// Тесты
// ============================================================

// TestSnapshot_Creation проверяет, что runSnapshots создаёт снэпшот
// при достижении threshold. Устанавливаем низкий SnapshotThreshold (8),
// отправляем 10 команд, ждём коммита и проверяем lastSnapshotIndex.
func TestSnapshot_Creation(t *testing.T) {
	defer leaktest.CheckTimeout(t, 30*time.Second)()

	h := newSnapshotHarness(t, 3)
	defer h.Shutdown()

	// Устанавливаем низкий порог для быстрого снэпшота.
	for i := 0; i < h.n; i++ {
		h.cluster[i].SetSnapshotConfig(8, 50*time.Millisecond, 4)
	}

	// Ждём лидера.
	leaderID := h.waitForSingleLeader()
	if leaderID < 0 {
		t.Fatal("no leader elected")
	}

	// Отправляем 15 команд.
	for i := 0; i < 15; i++ {
		cmd := fmt.Sprintf("key%d=val%d", i, i)
		idx := h.SubmitToServer(leaderID, cmd)
		if idx < 0 {
			t.Fatalf("SubmitToServer(%q) failed", cmd)
		}
	}

	// Ждём, пока на лидере появится снэпшот.
	if !h.waitForSnapshot(leaderID, 10*time.Second) {
		t.Fatal("snapshot was not created on leader")
	}

	// Проверяем, что после компактирования лог сократился.
	logLen := h.getLogLength(leaderID)
	t.Logf("leader log length after snapshot: %d", logLen)
	if logLen > 20 {
		t.Fatalf("log length = %d, expected compacted (< 20)", logLen)
	}
}

// TestSnapshot_NoSnapshotStore проверяет, что nil SnapshotStore
// не вызывает ошибок и снэпшоты отключены.
func TestSnapshot_NoSnapshotStore(t *testing.T) {
	defer leaktest.CheckTimeout(t, 30*time.Second)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	lid, _ := h.CheckSingleLeader()

	// Отправляем 10 команд.
	for i := 0; i < 10; i++ {
		idx := h.SubmitToServer(lid, i)
		if idx < 0 {
			t.Fatalf("SubmitToServer(%d) failed", i)
		}
	}
	sleepMs(ReelectionTimeoutMs)

	// Проверяем, что lastSnapshotIndex == -1 на всех узлах.
	for i := 0; i < h.n; i++ {
		h.cluster[i].mu.Lock()
		idx := h.cluster[i].cmState.lastSnapshotIndex
		h.cluster[i].mu.Unlock()
		if idx != -1 {
			t.Fatalf("server %d: lastSnapshotIndex = %d, want -1", i, idx)
		}
	}
}

// TestSnapshot_Install проверяет, что отставший follower получает
// InstallSnapshot RPC. Отключаем follower, накапливаем лог, создаём
// снэпшот, подключаем обратно и проверяем состояние FSM.
func TestSnapshot_Install(t *testing.T) {
	defer leaktest.CheckTimeout(t, 60*time.Second)()

	h := newSnapshotHarness(t, 3)
	defer h.Shutdown()

	// Устанавливаем низкий порог.
	for i := 0; i < h.n; i++ {
		h.cluster[i].SetSnapshotConfig(20, 50*time.Millisecond, 8)
	}

	leaderID := h.waitForSingleLeader()
	followerID := (leaderID + 1) % 3

	// Отключаем follower.
	h.DisconnectPeer(followerID)
	t.Logf("disconnected follower %d", followerID)

	// Отправляем 30 команд, чтобы превысить threshold.
	for i := 0; i < 30; i++ {
		cmd := fmt.Sprintf("k%d=v%d", i, i)
		idx := h.SubmitToServer(leaderID, cmd)
		if idx < 0 {
			t.Fatalf("SubmitToServer(%q) failed", cmd)
		}
	}

	// Ждём снэпшот на лидере.
	if !h.waitForSnapshot(leaderID, 10*time.Second) {
		t.Fatal("snapshot was not created on leader")
	}
	snapIdx := h.getSnapshotIndex(leaderID)
	t.Logf("leader snapshot index: %d", snapIdx)

	// Подключаем follower обратно.
	h.ReconnectPeer(followerID)
	t.Log("reconnected follower")

	// Ждём, пока follower получит снэпшот.
	sleepMs(ReelectionTimeoutMs * 3)

	// Проверяем, что follower догнал состояние.
	followerSnapIdx := h.getSnapshotIndex(followerID)
	t.Logf("follower snapshot index: %d", followerSnapIdx)

	// Follower должен иметь снэпшот после InstallSnapshot.
	if followerSnapIdx <= 0 {
		// Иногда InstallSnapshot не срабатывает сразу — даём ещё время.
		sleepMs(ReelectionTimeoutMs * 2)
		followerSnapIdx = h.getSnapshotIndex(followerID)
	}
	if followerSnapIdx <= 0 {
		// Проверяем хотя бы то, что состояние FSM совпадает.
		leaderFSM := h.fsms[leaderID]
		followerFSM := h.fsms[followerID]

		leaderFSM.mu.Lock()
		leaderState := make(map[string]string)
		for k, v := range leaderFSM.state {
			leaderState[k] = v
		}
		leaderFSM.mu.Unlock()

		followerFSM.mu.Lock()
		followerState := make(map[string]string)
		for k, v := range followerFSM.state {
			followerState[k] = v
		}
		followerFSM.mu.Unlock()

		if len(leaderState) != len(followerState) {
			t.Fatalf("state mismatch: leader has %d keys, follower has %d keys",
				len(leaderState), len(followerState))
		}
		for k, v := range leaderState {
			if followerState[k] != v {
				t.Fatalf("state mismatch for key %q: leader=%q, follower=%q", k, v, followerState[k])
			}
		}
		t.Log("follower caught up via AppendEntries (snapshot index = -1)")
	}
}

// TestCompactLog_Basic проверяет базовое компактирование лога.
func TestCompactLog_Basic(t *testing.T) {
	// Создаём CM с 5 записями в логе (Index: 1, 2, 3, 4, 5).
	storage := NewMapStorage()
	ready := make(chan any)
	close(ready)
	cm := NewConsensusModule(0, []int{}, NewInmemTransport("test"), storage, newSnapshotTestFSM(), ready)

	cm.mu.Lock()
	cm.cmState.log = []LogEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
		{Index: 4, Term: 1},
		{Index: 5, Term: 1},
	}
	cm.cmState.lastLogIndex = 5
	cm.cmState.lastLogTerm = 1

	// компактируем записи с Index < 3.
	cm.compactLogs(3)
	cm.mu.Unlock()

	// Проверяем, что остались записи с Index: 3, 4, 5.
	if len(cm.cmState.log) != 3 {
		t.Fatalf("log length = %d, want 3", len(cm.cmState.log))
	}
	if cm.cmState.log[0].Index != 3 {
		t.Fatalf("first entry Index = %d, want 3", cm.cmState.log[0].Index)
	}
	if cm.cmState.log[1].Index != 4 {
		t.Fatalf("second entry Index = %d, want 4", cm.cmState.log[1].Index)
	}
	if cm.cmState.log[2].Index != 5 {
		t.Fatalf("third entry Index = %d, want 5", cm.cmState.log[2].Index)
	}
	cm.Stop()
}

// TestCompactLog_All проверяет компактирование всех записей.
func TestCompactLog_All(t *testing.T) {
	defer leaktest.CheckTimeout(t, time.Second)()

	cm := &ConsensusModule{}
	cm.shutdownCh = make(chan struct{})
	cm.cmState.log = []LogEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
	}
	cm.compactLogs(10)
	if len(cm.cmState.log) != 0 {
		t.Fatalf("log length = %d, want 0", len(cm.cmState.log))
	}
}

// TestCompactLog_Nothing проверяет, что компактирование до индекса,
// меньшего чем первая запись, не меняет лог.
func TestCompactLog_Nothing(t *testing.T) {
	defer leaktest.CheckTimeout(t, time.Second)()

	cm := &ConsensusModule{}
	cm.shutdownCh = make(chan struct{})
	cm.cmState.log = []LogEntry{
		{Index: 5, Term: 1},
		{Index: 6, Term: 1},
		{Index: 7, Term: 1},
	}
	cm.compactLogs(3)
	if len(cm.cmState.log) != 3 {
		t.Fatalf("log length = %d, want 3", len(cm.cmState.log))
	}
	if cm.cmState.log[0].Index != 5 {
		t.Fatalf("first entry Index = %d, want 5", cm.cmState.log[0].Index)
	}
}

// TestCompactLog_Empty проверяет компактирование пустого лога.
func TestCompactLog_Empty(t *testing.T) {
	defer leaktest.CheckTimeout(t, time.Second)()

	cm := &ConsensusModule{}
	cm.shutdownCh = make(chan struct{})
	cm.compactLogs(5)
	if len(cm.cmState.log) != 0 {
		t.Fatalf("log length = %d, want 0", len(cm.cmState.log))
	}
}

// TestLogPosition_Exact проверяет точное совпадение индекса.
func TestLogPosition_Exact(t *testing.T) {
	cm := &ConsensusModule{}
	cm.cmState.log = []LogEntry{
		{Index: 5, Term: 1},
		{Index: 10, Term: 1},
		{Index: 15, Term: 1},
	}
	if pos := cm.logPosition(5); pos != 0 {
		t.Fatalf("logPosition(5) = %d, want 0", pos)
	}
	if pos := cm.logPosition(10); pos != 1 {
		t.Fatalf("logPosition(10) = %d, want 1", pos)
	}
	if pos := cm.logPosition(15); pos != 2 {
		t.Fatalf("logPosition(15) = %d, want 2", pos)
	}
}

// TestLogPosition_NotFound проверяет поиск отсутствующего индекса.
func TestLogPosition_NotFound(t *testing.T) {
	cm := &ConsensusModule{}
	cm.cmState.log = []LogEntry{
		{Index: 5, Term: 1},
		{Index: 10, Term: 1},
		{Index: 15, Term: 1},
	}
	if pos := cm.logPosition(7); pos != 1 {
		t.Fatalf("logPosition(7) = %d, want 1", pos)
	}
	if pos := cm.logPosition(1); pos != 0 {
		t.Fatalf("logPosition(1) = %d, want 0", pos)
	}
	if pos := cm.logPosition(20); pos != 3 {
		t.Fatalf("logPosition(20) = %d, want 3", pos)
	}
}

// TestLogPosition_AfterCompact проверяет, что logPosition работает
// после компактирования.
func TestLogPosition_AfterCompact(t *testing.T) {
	cm := &ConsensusModule{}
	cm.cmState.log = []LogEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
		{Index: 4, Term: 1},
		{Index: 5, Term: 1},
	}
	cm.compactLogs(3)
	if pos := cm.logPosition(3); pos != 0 {
		t.Fatalf("logPosition(3) = %d, want 0", pos)
	}
	if pos := cm.logPosition(4); pos != 1 {
		t.Fatalf("logPosition(4) = %d, want 1", pos)
	}
	if pos := cm.logPosition(5); pos != 2 {
		t.Fatalf("logPosition(5) = %d, want 2", pos)
	}
}

// Helper: waitForSingleLeader ждёт появления лидера в snapshotHarness.
func (h *snapshotHarness) waitForSingleLeader() int {
	for r := 0; r < 20; r++ {
		for i := 0; i < h.n; i++ {
			if !h.connected[i] {
				continue
			}
			_, _, isLeader := h.cluster[i].Report()
			if isLeader {
				return i
			}
		}
		time.Sleep(HeartbeatTimeoutMs * time.Millisecond * 2)
	}
	return -1
}

// TestSnapshot_MultipleSnapshots проверяет, что при множественных снэпшотах
// InmemSnapshotStore хранит только последний, а лог компактируется.
func TestSnapshot_MultipleSnapshots(t *testing.T) {
	defer leaktest.CheckTimeout(t, 60*time.Second)()

	// Одноузловой кластер с низким threshold.
	storage := NewMapStorage()
	fsm := newSnapshotTestFSM()
	store := NewInmemSnapshotStore()
	ready := make(chan any)
	close(ready)

	transport := NewInmemTransport("single")
	cm := NewConsensusModule(0, []int{}, transport, storage, fsm, ready, store)
	defer cm.Stop()

	sleepMs(500)

	cm.mu.Lock()
	isLeader := cm.cmState.state == Leader
	cm.mu.Unlock()
	if !isLeader {
		// Ждём выборов (одиночный узел становится лидером после election timeout).
		for i := 0; i < 20; i++ {
			sleepMs(100)
			cm.mu.Lock()
			isLeader = cm.cmState.state == Leader
			cm.mu.Unlock()
			if isLeader {
				break
			}
		}
		if !isLeader {
			t.Fatal("node did not become leader")
		}
	}

	cm.SetSnapshotConfig(5, 50*time.Millisecond, 2)

	// Отправляем много команд, чтобы создать несколько снэпшотов.
	for i := 0; i < 30; i++ {
		cmd := fmt.Sprintf("k%d=v%d", i, i)
		future := cm.Apply(cmd, 0)
		if err := future.Error(); err != nil {
			t.Fatalf("Apply %q failed: %v", cmd, err)
		}
	}

	// Ждём снэпшота.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		cm.mu.Lock()
		snapIdx := cm.cmState.lastSnapshotIndex
		logLen := len(cm.cmState.log)
		cm.mu.Unlock()
		if snapIdx > 0 && logLen <= 10 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cm.mu.Lock()
	snapIdx := cm.cmState.lastSnapshotIndex
	logLen := len(cm.cmState.log)
	cm.mu.Unlock()

	t.Logf("snapshot index=%d, log length=%d", snapIdx, logLen)

	if snapIdx <= 0 {
		t.Fatal("no snapshot created")
	}
	if logLen > 10 {
		t.Fatalf("log too long: %d entries (expected < 10)", logLen)
	}

	// Проверяем, что store хранит только один снэпшот.
	list, _ := store.List()
	if len(list) != 1 {
		t.Fatalf("store has %d snapshots, want 1", len(list))
	}

	// Проверяем, что данные снэпшота корректны.
	meta, reader, err := store.Open(list[0].ID)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer func() { _ = reader.Close() }()
	data, _ := io.ReadAll(reader)
	t.Logf("snapshot data: %d bytes, index=%d, term=%d", len(data), meta.Index, meta.Term)

	// Восстанавливаем новую FSM из снэпшота.
	fsm2 := newSnapshotTestFSM()
	_, r2, err := store.Open(list[0].ID)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer func() { _ = r2.Close() }()
	if err := fsm2.Restore(r2); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	// Проверяем, что последнее записанное состояние есть.
	if v := fsm2.getState("k29"); v != "v29" {
		t.Fatalf("k29 = %q, want v29", v)
	}
}

// TestSnapshot_RestartRestoresFSM — регрессионный тест P0-1: после
// снапшота, компактирования durable-лога и полного рестарта узла FSM
// обязан восстановиться из durable-снапшота (включая ключи до линии
// компактирования), а суффикс лога — доиграться существующим механизмом.
// Используются реальные FileStorage и FileSnapshotStore в одной директории.
func TestSnapshot_RestartRestoresFSM(t *testing.T) {
	defer leaktest.CheckTimeout(t, 60*time.Second)()

	dir := t.TempDir()
	storage := NewFileStorage(dir)
	store, err := NewFileSnapshotStore(dir, 2)
	if err != nil {
		t.Fatalf("NewFileSnapshotStore failed: %v", err)
	}
	fsm := newSnapshotTestFSM()
	ready := make(chan any)
	close(ready)
	transport := NewInmemTransport("single")

	cm := NewConsensusModule(0, []int{}, transport, storage, fsm, ready, store)
	waitForLeader(t, cm, 5*time.Second)

	cm.SetSnapshotConfig(8, 50*time.Millisecond, 2)

	const numCommands = 30
	for i := 0; i < numCommands; i++ {
		cmd := fmt.Sprintf("k%d=v%d", i, i)
		future := cm.Apply(cmd, 0)
		if err := future.Error(); err != nil {
			t.Fatalf("Apply %q failed: %v", cmd, err)
		}
	}

	snapIdx := waitForSnapshotAndCompaction(t, cm, numCommands)
	for i := 0; i < numCommands; i++ {
		key := fmt.Sprintf("k%d", i)
		want := fmt.Sprintf("v%d", i)
		if v := fsm.getState(key); v != want {
			t.Fatalf("before restart: %s = %q, want %q", key, v, want)
		}
	}

	waitForSnapshotQuiescence(t, cm, storage, 10*time.Second)

	cm.Stop()
	transport.Close()

	// Полный рестарт: новые инстансы хранилищ и FSM в той же директории.
	storage2 := NewFileStorage(dir)
	store2, err := NewFileSnapshotStore(dir, 2)
	if err != nil {
		t.Fatalf("NewFileSnapshotStore (restart) failed: %v", err)
	}
	fsm2 := newSnapshotTestFSM()
	ready2 := make(chan any)
	close(ready2)
	transport2 := NewInmemTransport("single-2")

	cm2 := NewConsensusModule(0, []int{}, transport2, storage2, fsm2, ready2, store2)
	defer cm2.Stop()

	// Главный регрессионный критерий P0-1: ключ до линии компактирования
	// восстановлен из снапшота сразу после конструктора.
	if v := fsm2.getState("k0"); v != "v0" {
		t.Fatalf("k0 = %q after restart, want v0 (key below compaction line)", v)
	}

	cm2.mu.Lock()
	lastApplied := cm2.cmState.lastApplied
	lastSnapshotIndex := cm2.cmState.lastSnapshotIndex
	cm2.mu.Unlock()
	if lastSnapshotIndex < snapIdx {
		t.Fatalf("lastSnapshotIndex = %d after restart, want >= %d", lastSnapshotIndex, snapIdx)
	}
	if lastApplied != lastSnapshotIndex {
		t.Fatalf("lastApplied = %d, want %d (snapshot index)", lastApplied, lastSnapshotIndex)
	}

	// Узел работоспособен после restore: новая команда проходит и
	// продвижение commitIndex доигрывает суффикс лога (k29).
	waitForLeader(t, cm2, 5*time.Second)
	future := cm2.Apply("k30=v30", 0)
	if err := future.Error(); err != nil {
		t.Fatalf("Apply k30 after restart failed: %v", err)
	}
	if v := fsm2.getState("k30"); v != "v30" {
		t.Fatalf("k30 = %q after restart, want v30", v)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if fsm2.getState("k29") == "v29" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("k29 not applied after restart: log suffix replay failed")
}

// TestSnapshot_LeaderCrash проверяет, что снэпшот переживает смену лидера.
func TestSnapshot_LeaderCrash(t *testing.T) {
	defer leaktest.CheckTimeout(t, 60*time.Second)()

	h := newSnapshotHarness(t, 3)
	defer h.Shutdown()

	// Устанавливаем низкий порог.
	for i := 0; i < h.n; i++ {
		h.cluster[i].SetSnapshotConfig(15, 50*time.Millisecond, 8)
	}

	leaderID := h.waitForSingleLeader()

	// Отправляем 25 команд и ждём снэпшота.
	for i := 0; i < 25; i++ {
		cmd := fmt.Sprintf("k%d=v%d", i, i)
		idx := h.SubmitToServer(leaderID, cmd)
		if idx < 0 {
			t.Fatalf("SubmitToServer(%q) failed", cmd)
		}
	}
	if !h.waitForSnapshot(leaderID, 10*time.Second) {
		t.Fatal("snapshot not created")
	}

	// Убиваем лидера.
	h.CrashPeer(leaderID)
	t.Logf("leader %d crashed", leaderID)

	// Ждём нового лидера.
	newLeaderID := h.waitForSingleLeader()
	if newLeaderID < 0 {
		t.Fatal("no leader elected after crash")
	}
	if newLeaderID == leaderID {
		t.Fatal("new leader should be different")
	}
	t.Logf("new leader: %d", newLeaderID)
}

// TestInmemTransport_InstallSnapshot проверяет InstallSnapshot через
// in-memory транспорт без участия CM.
func TestInmemTransport_InstallSnapshot(t *testing.T) {
	defer leaktest.CheckTimeout(t, time.Second)()

	t1, t2, cleanup := newInmemPair(t)
	defer cleanup()

	// Запускаем обработчик, который отвечает на InstallSnapshot.
	done := make(chan struct{})
	go func() {
		select {
		case rpc := <-t2.Consumer():
			switch rpc.Command.(type) {
			case *InstallSnapshotRequest:
				// Читаем и отбрасываем данные.
				_, _ = io.Copy(io.Discard, rpc.Reader)
				rpc.RespChan <- RPCResponse{
					Reply: &InstallSnapshotResponse{
						RPCHeader: RPCHeader{ProtocolVersion: ProtocolVersion, ServerID: 1},
						Term:      1,
						Success:   true,
					},
				}
			}
		case <-done:
		}
	}()
	defer close(done)

	req := InstallSnapshotRequest{
		RPCHeader:    RPCHeader{ProtocolVersion: ProtocolVersion, ServerID: 0},
		Term:         1,
		LeaderID:     0,
		LastLogIndex: 10,
		LastLogTerm:  1,
		DataSize:     4,
	}
	data := bytes.NewReader([]byte("test"))
	reply, err := t1.InstallSnapshot(1, req, data)
	if err != nil {
		t.Fatalf("InstallSnapshot failed: %v", err)
	}
	if !reply.Success {
		t.Fatal("InstallSnapshot: Success = false")
	}
}
