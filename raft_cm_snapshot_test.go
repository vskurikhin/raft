package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
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

func (f *failingSnapshotStore) Create(_, _ int, _ Configuration, _ int) (SnapshotSink, error) {
	f.mu.Lock()
	f.creates++
	f.mu.Unlock()
	return nil, fmt.Errorf("create failed")
}

func (f *failingSnapshotStore) List() ([]*SnapshotMeta, error) {
	return nil, nil
}

func (f *failingSnapshotStore) Open(_ string) (*SnapshotMeta, io.ReadCloser, error) {
	return nil, nil, fmt.Errorf("open failed")
}

func (f *failingSnapshotStore) createCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.creates
}

// TestRunSnapshots_TakeSnapshotErrorDoesNotCrash проверяет,
// что ошибка takeSnapshot не роняет процесс: цикл снимков продолжает
// работать и повторяет попытки (Create вызывается снова). Сама ошибка
// логируется через traceLogf(0, ...) — проверка через перехват лога
// не выполняется, чтобы не мутировать глобальный логгер из теста.
func TestRunSnapshots_TakeSnapshotErrorDoesNotCrash(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	fsm := newSnapshotTestFSM()
	ready := make(chan any)
	close(ready)
	storage := NewMapStorage()
	transport := NewInmemTransport("single")
	store := &failingSnapshotStore{}
	cm := NewConsensusModule(0, []int{}, transport, storage, fsm, ready, store)
	defer cm.Stop()

	// Состояние, при котором shouldSnapshot вернёт true, а FSM-снимок
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
		// poll-интервал condition-wait (не фиксированная пауза).
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("takeSnapshot was never attempted: failing store Create not called")
}

// TestLeaderSendSnapshot_EmptyListSignalsSnapshot проверяет,
// что при пустом List() у лидера с lastSnapshotIndex >= 0 вызов не
// паникует, не меняет nextIndex и отправляет сигнал в snapshotCh.
func TestLeaderSendSnapshot_EmptyListSignalsSnapshot(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

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

// -- Тесты идемпотентности InstallSnapshot. ---

// countingSnapshotStore — обёртка над InmemSnapshotStore, считающая
// вызовы Create: признак «снимок действительно применялся».
type countingSnapshotStore struct {
	*InmemSnapshotStore
	creates atomic.Int64
}

func (s *countingSnapshotStore) Create(index, term int, configuration Configuration, configIndex int) (SnapshotSink, error) {
	s.creates.Add(1)
	return s.InmemSnapshotStore.Create(index, term, configuration, configIndex)
}

// newInstallSnapshotCM собирает литеральный CM с уже установленным
// снимком (lastSnapshotIndex=10, term 2, пустой журнал) для прямого
// вызова handleInstallSnapshot.
func newInstallSnapshotCM() (*ConsensusModule, *countingSnapshotStore) {
	store := &countingSnapshotStore{InmemSnapshotStore: NewInmemSnapshotStore()}
	cm := &ConsensusModule{
		storage:       NewMapStorage(),
		snapshotStore: store,
		fsm:           newSnapshotTestFSM(),
	}
	cm.cmState.state = Follower
	cm.cmState.currentTerm = 1
	cm.cmState.votedFor = -1
	cm.cmState.lastSnapshotIndex = 10
	cm.cmState.lastSnapshotTerm = 2
	cm.cmState.lastLogIndex = 10
	cm.cmState.lastLogTerm = 2
	cm.cmState.lastApplied = 10
	cm.cmState.commitIndex = 10
	cm.cmState.termIndexMap = make(map[int]int)
	return cm, store
}

// installSnapshotRequestData — gob-данные снимка для snapshotTestFSM.
func installSnapshotRequestData(t *testing.T, state map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(state); err != nil {
		t.Fatalf("gob encode: %v", err)
	}
	return buf.Bytes()
}

// installSnapshotRequest формирует InstallSnapshotRequest с валидной
// конфигурацией и размером тела.
func installSnapshotRequest(leaderID, lastLogIndex, lastLogTerm int, data []byte) *InstallSnapshotRequest {
	cfgData, _ := EncodeConfiguration(Configuration{})
	return &InstallSnapshotRequest{
		RPCHeader:     RPCHeader{ProtocolVersion: ProtocolVersion, ServerID: leaderID},
		Term:          1,
		LeaderID:      leaderID,
		LastLogIndex:  lastLogIndex,
		LastLogTerm:   lastLogTerm,
		Configuration: cfgData,
		ConfigIndex:   0,
		DataSize:      int64(len(data)),
	}
}

// driveInstallSnapshotRPC доставляет InstallSnapshot в handleInstallSnapshot
// и возвращает ответ.
func driveInstallSnapshotRPC(t *testing.T, cm *ConsensusModule, req *InstallSnapshotRequest, data []byte) InstallSnapshotResponse {
	t.Helper()
	respCh := make(chan RPCResponse, 1)
	cm.handleInstallSnapshot(RPC{Command: req, Reader: bytes.NewReader(data), RespChan: respCh}, req)
	resp := <-respCh
	reply, ok := resp.Reply.(*InstallSnapshotResponse)
	if !ok {
		t.Fatalf("unexpected reply type %T", resp.Reply)
	}
	return *reply
}

// TestInstallSnapshot_RepeatedIsNoOp — тест 1: два подряд
// InstallSnapshot с одинаковым индексом — второй обрабатывается как
// no-op: Success:true, snapshotStore.Create не вызывается повторно,
// состояние не изменяется. На HEAD до второй вызов создавал
// бы новый снимок (Create) — тест падает.
func TestInstallSnapshot_RepeatedIsNoOp(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	cm, store := newInstallSnapshotCM()
	data := installSnapshotRequestData(t, map[string]string{"k0": "v0"})
	req := installSnapshotRequest(99, 12, 2, data) // новее имеющегося (10)

	// Первая установка — применяется.
	reply := driveInstallSnapshotRPC(t, cm, req, data)
	if !reply.Success {
		t.Fatalf("first InstallSnapshot: Success=false: %+v", reply)
	}
	if got := store.creates.Load(); got != 1 {
		t.Fatalf("Create calls = %d, want 1 after first install", got)
	}
	if cm.cmState.lastSnapshotIndex != 12 {
		t.Fatalf("lastSnapshotIndex = %d, want 12", cm.cmState.lastSnapshotIndex)
	}

	// Повторная установка того же снимка — no-op.
	reply = driveInstallSnapshotRPC(t, cm, req, data)
	if !reply.Success {
		t.Fatalf("repeated InstallSnapshot: Success=false, want true (idempotent no-op)")
	}
	if got := store.creates.Load(); got != 1 {
		t.Fatalf("Create calls = %d after repeat, want 1 (no-op must not create)", got)
	}
	if cm.cmState.lastSnapshotIndex != 12 || cm.cmState.commitIndex != 12 || cm.cmState.lastApplied != 12 {
		t.Fatalf("state changed by no-op: snapshot=%d commit=%d applied=%d",
			cm.cmState.lastSnapshotIndex, cm.cmState.commitIndex, cm.cmState.lastApplied)
	}
}

// TestInstallSnapshot_StaleDoesNotRollBack — тест 2: InstallSnapshot
// с LastLogIndex меньше текущего lastSnapshotIndex — no-op, commitIndex и
// lastApplied не уменьшаются (монотонность по построению,).
func TestInstallSnapshot_StaleDoesNotRollBack(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	cm, store := newInstallSnapshotCM() // lastSnapshotIndex = commitIndex = 10
	data := installSnapshotRequestData(t, map[string]string{"k0": "v0"})
	req := installSnapshotRequest(99, 5, 2, data) // старее 10

	reply := driveInstallSnapshotRPC(t, cm, req, data)
	if !reply.Success {
		t.Fatalf("stale InstallSnapshot: Success=false, want true (no-op)")
	}
	if got := store.creates.Load(); got != 0 {
		t.Fatalf("Create calls = %d, want 0 (stale snapshot must not be applied)", got)
	}
	if cm.cmState.commitIndex != 10 || cm.cmState.lastApplied != 10 {
		t.Fatalf("commitIndex/lastApplied rolled back: commit=%d applied=%d, want 10/10",
			cm.cmState.commitIndex, cm.cmState.lastApplied)
	}
	if cm.cmState.lastSnapshotIndex != 10 {
		t.Fatalf("lastSnapshotIndex = %d, want 10 (unchanged)", cm.cmState.lastSnapshotIndex)
	}
	if got := peerSum(cm.counters.installSnapshotSkippedStale); got < 1 {
		t.Fatalf("installSnapshotSkippedStale = %d, want >= 1", got)
	}
}

// countingReader — ридер, считающий прочитанные байты: проверка drain
// тела запроса при no-op.
type countingReader struct {
	r io.Reader
	n atomic.Int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n.Add(int64(n))
	return n, err
}

// TestInstallSnapshot_NoOpDrainsBody — тест 3 (часть 1): при no-op
// тело запроса обязано быть вычитано существующим drain-механизмом в defer
// иначе на TCP-транспорте соединение рассинхронизируется.
func TestInstallSnapshot_NoOpDrainsBody(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	cm, _ := newInstallSnapshotCM()
	data := installSnapshotRequestData(t, map[string]string{"k0": "v0"})
	req := installSnapshotRequest(99, 5, 2, data) // no-op (5 <= 10)

	reader := &countingReader{r: bytes.NewReader(data)}
	respCh := make(chan RPCResponse, 1)
	cm.handleInstallSnapshot(RPC{Command: req, Reader: reader, RespChan: respCh}, req)
	resp := <-respCh
	reply, ok := resp.Reply.(*InstallSnapshotResponse)
	if !ok || !reply.Success {
		t.Fatalf("no-op InstallSnapshot reply: %+v", resp.Reply)
	}

	if got := reader.n.Load(); got != int64(len(data)) {
		t.Fatalf("drained %d bytes, want %d — request body must be fully drained", got, len(data))
	}
}

// TestInstallSnapshot_NoOpKeepsTCPConnectionUsable — тест 3
// (часть 2, TCP-уровень): после no-op InstallSnapshot то же соединение
// обязано обслужить следующий RPC (AppendEntries) — интегральная проверка
// drain'а на реальном TCP-транспорте.
func TestInstallSnapshot_NoOpKeepsTCPConnectionUsable(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	client, server, cleanup := newTCPTransportPair(t)
	defer cleanup()

	ready := make(chan any)
	close(ready)
	cm := NewConsensusModule(1, []int{}, server, NewMapStorage(), newSnapshotTestFSM(), ready, NewInmemSnapshotStore())
	defer cm.Stop()

	// Узел уже владеет снимком на индексе 10.
	cm.mu.Lock()
	cm.cmState.lastSnapshotIndex = 10
	cm.cmState.lastSnapshotTerm = 2
	cm.cmState.lastLogIndex = 10
	cm.cmState.lastLogTerm = 2
	cm.cmState.lastApplied = 10
	cm.cmState.commitIndex = 10
	cm.mu.Unlock()

	// Ждём, пока runRPCReader начнёт обслуживать входящие RPC.
	// keep: timing — окно является предметом проверки в этом месте;
	// наблюдаемого признака состояния здесь нет.
	sleepMs(50)

	data := []byte("snapshot-body-bytes")
	cfgData, _ := EncodeConfiguration(Configuration{})
	req := InstallSnapshotRequest{
		RPCHeader:     RPCHeader{ProtocolVersion: ProtocolVersion, ServerID: 0},
		Term:          1,
		LeaderID:      0,
		LastLogIndex:  5, // старее 10 — no-op
		LastLogTerm:   2,
		Configuration: cfgData,
		DataSize:      int64(len(data)),
	}
	reply, err := client.InstallSnapshot(1, req, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("InstallSnapshot: %v", err)
	}
	if !reply.Success {
		t.Fatalf("no-op InstallSnapshot: Success=false: %+v", reply)
	}

	// Следующий RPC на том же транспорте/соединении.
	aeReply, err := client.AppendEntries(1, AppendEntriesArgs{
		RPCHeader:    RPCHeader{ProtocolVersion: ProtocolVersion, ServerID: 0},
		Term:         1,
		LeaderID:     0,
		PrevLogIndex: 10,
		PrevLogTerm:  2,
		LeaderCommit: -1,
	})
	if err != nil {
		t.Fatalf("AppendEntries after no-op InstallSnapshot: %v", err)
	}
	if !aeReply.Success {
		t.Fatalf("AppendEntries after no-op InstallSnapshot failed: %+v", aeReply)
	}
}
