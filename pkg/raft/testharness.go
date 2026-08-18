package raft

import (
	"log"
	"sync"
	"testing"
	"time"
)

const submitTimeout = 5 * time.Second

// Test-only константы бюджетов (единственный владелец — TASK-009; ADR-002,
// ADR-003 п.8). Разделение Protocol timeout / Test deadline / Poll interval:
// бюджеты — Test deadline, выведенный из worst-case фаз протокола.

const (
	// maxElectionTimeout — максимальный election timeout:
	// 2*ReelectionTimeoutMs = 762ms.
	maxElectionTimeout = 2 * ReelectionTimeoutMs * time.Millisecond

	// preVoteRound — worst-case раунда Pre-Vote (≈ maxElectionTimeout).
	preVoteRound = maxElectionTimeout

	// inmemRPCTimeout — именованная копия значения InmemTransport.timeout
	// (transport_inmem.go:35). При рассинхроне с транспортом источник
	// истины — транспорт.
	inmemRPCTimeout = 500 * time.Millisecond

	// commitBudgetSteady — бюджет ожидания коммита при устоявшемся лидере:
	// 4*(applyBatchInterval + inmemRPCTimeout) = 4*(50ms+500ms) = 2.2s.
	commitBudgetSteady = 4 * (applyBatchInterval + inmemRPCTimeout)

	// failoverBudgetMargin — запас бюджета after-failover для поглощения
	// межфазных задержек (worst-case 2*762 + 762 + 50 + 200 ≈ 2.5s).
	failoverBudgetMargin = 200 * time.Millisecond

	// commitBudgetAfterFailover — бюджет ожидания коммита для сценариев
	// с возможными перевыборами (disconnect/restart/leadership transfer):
	// 2*maxElectionTimeout + preVoteRound + applyBatchInterval + запас ≈ 2.5–3s.
	commitBudgetAfterFailover = 2*maxElectionTimeout + preVoteRound + applyBatchInterval + failoverBudgetMargin

	// LeaktestBudget — единый бюджет leaktest:
	// max(inmemRPCTimeout, TCPRPCTimeout) + 100ms = 600ms.
	LeaktestBudget = 600 * time.Millisecond
)

func init() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
}

// Harness — тестовый стенд для unit-тестирования Raft.
// Использует InmemTransport вместо TCP, не открывает порты.
type Harness struct {
	mu sync.Mutex

	// cluster — список ConsensusModule, участвующих в кластере.
	cluster []*ConsensusModule

	// transports — in-memory транспорты для каждого узла.
	transports []*InmemTransport

	storage []*MapStorage

	// commitChans содержит по одному каналу фиксации для каждого сервера.
	commitChans []chan CommitEntry

	// commits с индексом i содержит последовательность записей,
	// зафиксированных сервером i на текущий момент.
	commits [][]CommitEntry

	// connected содержит по одному логическому значению для каждого сервера
	// кластера и определяет, подключён ли данный сервер в настоящий момент
	// к соседям.
	connected []bool

	// alive содержит по одному логическому значению для каждого сервера
	// кластера и определяет, работает ли данный сервер в настоящий момент.
	alive []bool

	// shutdownOnce защищает Shutdown() от повторного вызова.
	shutdownOnce sync.Once

	// wg — регистрация коллекторов collectCommits; Shutdown() закрывает
	// каналы и присоединяет коллекторы через wg.Wait() (INV-T3: закрытие
	// каналов — только после гарантированного завершения producers).
	wg sync.WaitGroup

	// collectorDone содержит по одному каналу завершения коллектора
	// текущей incarnation каждого узла. Используется quiesce-барьером
	// CrashPeer (ADR-003 п.7): коллектор старой incarnation
	// присоединяется до очистки h.commits[id].
	collectorDone []chan struct{}

	n int
	t *testing.T
}

// HarnessOption — опция конфигурации узлов Harness. Применяется к
// каждому созданному ConsensusModule ДО close(ready), т.е. до старта
// фоновых горутин (INV-T5: конфигурация — до старта, без post-start
// мутаций). Поддерживает кластерное применение (ADR-005).
type HarnessOption func(cm *ConsensusModule)

// DisablePreVote — опция Harness: отключение Pre-Vote на узле
// (preVoteDisabled=true до старта горутин). При передаче в
// NewHarnessWithOptions применяется ко всем узлам кластера.
func DisablePreVote() HarnessOption {
	return func(cm *ConsensusModule) { cm.preVoteDisabled = true }
}

// NewHarness создаёт новую тестовую запряжку,
// инициализированную n серверами, соединёнными друг с другом
// через InmemTransport.
func NewHarness(t *testing.T, n int) *Harness {
	return NewHarnessWithOptions(t, n)
}

// NewHarnessWithOptions создаёт Harness с опциями opts, применяемыми
// к каждому узлу до close(ready) (до старта фоновых горутин).
func NewHarnessWithOptions(t *testing.T, n int, opts ...HarnessOption) *Harness {
	cluster := make([]*ConsensusModule, n)
	transports := make([]*InmemTransport, n)
	connected := make([]bool, n)
	alive := make([]bool, n)
	commitChans := make([]chan CommitEntry, n)
	commits := make([][]CommitEntry, n)
	ready := make(chan any)
	storage := make([]*MapStorage, n)

	// Создаём транспорты и соединяем их все друг с другом.
	for i := 0; i < n; i++ {
		addr := ServerAddress("raft-" + itoa(i))
		transports[i] = NewInmemTransport(addr)
	}

	// Соединяем все транспорты друг с другом.
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j {
				transports[i].Connect(ServerID(j), transports[j])
			}
		}
	}

	// Создаём ConsensusModule для каждого узла.
	for i := 0; i < n; i++ {
		peerIds := make([]int, 0)
		for p := 0; p < n; p++ {
			if p != i {
				peerIds = append(peerIds, p)
			}
		}

		storage[i] = NewMapStorage()
		commitChans[i] = make(chan CommitEntry)
		cluster[i] = NewConsensusModule(
			i, peerIds, transports[i],
			storage[i], NewCommitChannelFSM(commitChans[i]), ready,
		)
		// Опции применяются до close(ready) — до старта фоновых
		// горутин узла (INV-T5); никакой post-start мутации.
		for _, opt := range opts {
			opt(cluster[i])
		}
		alive[i] = true
		connected[i] = true
	}
	close(ready)

	h := &Harness{
		cluster:       cluster,
		transports:    transports,
		storage:       storage,
		commitChans:   commitChans,
		commits:       commits,
		connected:     connected,
		alive:         alive,
		collectorDone: make([]chan struct{}, n),
		n:             n,
		t:             t,
	}
	for i := 0; i < n; i++ {
		h.collectorDone[i] = h.startCollector(i, commitChans[i])
	}
	return h
}

// Shutdown останавливает все серверы в тестовом окружении.
// Идемпотентен — повторные вызовы безопасны.
//
// Последовательность (ADR-003 п.9 / ADR-011 п.2): cm.Stop() (join) →
// transports[i].Close() → close(commitChans[i]) → join коллекторов.
// Контракт порядка: transport.Close() выполняется до или одновременно
// с join горутин CM — фактически сразу после Stop(), поэтому
// отправители RPC не могут блокироваться навсегда.
// Инвариант границ критических секций h.mu (NEW-02): h.mu удерживается
// только короткими секциями над полями Harness и НИКОГДА не удерживается
// через cm.Stop(), transport.Close(), close(commitChans[i]) и join
// коллекторов (цикл h.mu → join runFSM → Apply в канал → collectCommits
// ждёт h.mu = вечный deadlock).
func (h *Harness) Shutdown() {
	h.shutdownOnce.Do(func() {
		for i := 0; i < h.n; i++ {
			if h.alive[i] {
				h.alive[i] = false
				h.cluster[i].Stop()
				h.transports[i].Close()
			}
			close(h.commitChans[i])
		}
	})
}

// DisconnectPeer отключает сервер от всех остальных серверов кластера
// через разрыв логического соединения в InmemTransport.
func (h *Harness) DisconnectPeer(id int) {
	tlog("Disconnect %d", id)
	h.transports[id].DisconnectAll()
	for j := 0; j < h.n; j++ {
		if j != id {
			h.transports[j].Disconnect(ServerID(id))
		}
	}
	h.connected[id] = false
}

// ReconnectPeer повторно подключает сервер ко всем остальным серверам кластера.
func (h *Harness) ReconnectPeer(id int) {
	tlog("Reconnect %d", id)
	for j := 0; j < h.n; j++ {
		if j != id && h.alive[j] {
			h.transports[id].Connect(ServerID(j), h.transports[j])
			h.transports[j].Connect(ServerID(id), h.transports[id])
		}
	}
	h.connected[id] = true
}

// CrashPeer «аварийно завершает работу» сервера, отключая его
// и останавливая CM. Хранилище сохраняется.
//
// Quiesce-барьер (ADR-003 п.7, SA-005): канал commitChans[id] пересоздаётся
// per-incarnation под владением Harness — закрытие старого канала → join
// коллектора старой incarnation → новый канал → новый коллектор → очистка
// h.commits[id] под h.mu после join. Инвариант: в момент возврата ни одна
// горутина не держит ссылку на старый канал, h.commits[id] пуст и остаётся
// пустым. Инвариант границ h.mu (NEW-02): блокирующие lifecycle-операции
// выполняются ВНЕ h.mu.
func (h *Harness) CrashPeer(id int) {
	tlog("Crash %d", id)
	h.DisconnectPeer(id)

	// [h.mu: alive[id]=false] release — короткая секция.
	h.mu.Lock()
	h.alive[id] = false
	h.mu.Unlock()

	h.cluster[id].Stop()
	h.transports[id].Close()

	// Quiesce-барьер: join коллектора старой incarnation до очистки.
	oldCh := h.commitChans[id]
	close(oldCh)
	<-h.collectorDone[id]

	h.commitChans[id] = make(chan CommitEntry)
	h.collectorDone[id] = h.startCollector(id, h.commitChans[id])

	// [h.mu: очистка commits[id]] release — после join старого коллектора.
	h.mu.Lock()
	h.commits[id] = h.commits[id][:0]
	h.mu.Unlock()
}

// RestartPeer «перезапускает» сервер, создавая новый CM и InmemTransport.
func (h *Harness) RestartPeer(id int) {
	if h.alive[id] {
		log.Fatalf("id=%d is alive in RestartPeer", id)
	}
	tlog("Restart %d", id)

	peerIds := make([]int, 0)
	for p := 0; p < h.n; p++ {
		if p != id {
			peerIds = append(peerIds, p)
		}
	}

	ready := make(chan any)
	addr := ServerAddress("raft-" + itoa(h.n) + "-" + itoa(id))
	h.transports[id] = NewInmemTransport(addr)

	// Подключаем новый транспорт к остальным.
	for j := 0; j < h.n; j++ {
		if j != id && h.alive[j] {
			h.transports[id].Connect(ServerID(j), h.transports[j])
			h.transports[j].Connect(ServerID(id), h.transports[id])
		}
	}

	h.cluster[id] = NewConsensusModule(
		id, peerIds, h.transports[id],
		h.storage[id], NewCommitChannelFSM(h.commitChans[id]), ready,
	)
	close(ready)
	h.alive[id] = true
	h.connected[id] = true
	h.waitForPeerReady(id)
}

// waitForPeerReady ожидает готовность перезапущенного узла: CM отвечает
// на Report() (CM создан и restore завершён синхронно в конструкторе)
// и транспорт узла подключён ко всем живым соседям. Бюджет —
// commitBudgetSteady (условия обычно истинны сразу — poll немедленный).
func (h *Harness) waitForPeerReady(id int) {
	h.t.Helper()
	deadline := time.Now().Add(commitBudgetSteady)
	for time.Now().Before(deadline) {
		reportID, _, _ := h.cluster[id].Report()
		ready := reportID == id
		if ready {
			for j := 0; j < h.n; j++ {
				if j != id && h.alive[j] && h.transports[id].IsDisconnected(ServerID(j)) {
					ready = false
					break
				}
			}
		}
		if ready {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	h.t.Fatalf("restarted peer %d did not become ready within %v", id, commitBudgetSteady)
}

// PeerDropCallsAfterN — больше не поддерживается (RPCProxy удалён).
// Метод сохранён для совместимости, но не выполняет никаких действий.
func (h *Harness) PeerDropCallsAfterN(id int, n int) {
	tlog("peer %d drop calls after %d (no-op)", id, n)
}

// PeerDontDropCalls — больше не поддерживается (RPCProxy удалён).
func (h *Harness) PeerDontDropCalls(id int) {
	tlog("peer %d don't drop calls (no-op)", id)
}

// CheckSingleLeader проверяет, что только один сервер считает себя лидером.
func (h *Harness) CheckSingleLeader() (int, int) {
	for r := 0; r < 8; r++ {
		leaderId := -1
		leaderTerm := -1
		for i := 0; i < h.n; i++ {
			if h.connected[i] {
				_, term, isLeader := h.cluster[i].Report()
				if isLeader {
					if leaderId < 0 {
						leaderId = i
						leaderTerm = term
					} else {
						h.t.Fatalf("both %d and %d think they're leaders", leaderId, i)
					}
				}
			}
		}
		if leaderId >= 0 {
			return leaderId, leaderTerm
		}
		time.Sleep(50 * Quantum * time.Millisecond)
	}

	h.t.Fatalf("leader not found")
	return -1, -1
}

// GetTerm возвращает текущий term указанного сервера.
// Используется в тестах для проверки стабильности term при Pre-Vote.
func (h *Harness) GetTerm(serverID int) int {
	id, term, _ := h.cluster[serverID].Report()
	if id != serverID {
		return -1
	}
	return term
}

// CheckNoLeader проверяет, что ни один из подключённых серверов
// не считает себя лидером.
func (h *Harness) CheckNoLeader() {
	for i := 0; i < h.n; i++ {
		if h.connected[i] {
			_, _, isLeader := h.cluster[i].Report()
			if isLeader {
				h.t.Fatalf("server %d leader; want none", i)
			}
		}
	}
}

// CheckCommitted проверяет, что команда cmd зафиксирована на всех
// подключённых серверах с одинаковым индексом.
func (h *Harness) CheckCommitted(cmd int) (nc int, index int) {
	h.t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()

	commitsLen := -1
	for i := 0; i < h.n; i++ {
		if h.connected[i] {
			if commitsLen >= 0 {
				if len(h.commits[i]) != commitsLen {
					h.t.Fatalf("commits[%d] = %d, commitsLen = %d", i, h.commits[i], commitsLen)
				}
			} else {
				commitsLen = len(h.commits[i])
			}
		}
	}

	for c := 0; c < commitsLen; c++ {
		cmdAtC := -1
		for i := 0; i < h.n; i++ {
			if h.connected[i] {
				cmdOfN := h.commits[i][c].Data.(int)
				if cmdAtC >= 0 {
					if cmdOfN != cmdAtC {
						h.t.Errorf("got %d, want %d at h.commits[%d][%d]", cmdOfN, cmdAtC, i, c)
					}
				} else {
					cmdAtC = cmdOfN
				}
			}
		}
		if cmdAtC == cmd {
			index := -1
			nc := 0
			for i := 0; i < h.n; i++ {
				if h.connected[i] {
					if index >= 0 && h.commits[i][c].Index != index {
						h.t.Errorf("got Index=%d, want %d at h.commits[%d][%d]", h.commits[i][c].Index, index, i, c)
					} else {
						index = h.commits[i][c].Index
					}
					nc++
				}
			}
			return nc, index
		}
	}

	h.t.Errorf("cmd=%d not found in commits", cmd)
	return -1, -1
}

// CheckCommittedN проверяет, что команда cmd зафиксирована на n серверах.
func (h *Harness) CheckCommittedN(cmd int, n int) {
	h.t.Helper()
	nc, _ := h.CheckCommitted(cmd)
	if nc != n {
		h.t.Errorf("CheckCommittedN got nc=%d, want %d", nc, n)
	}
}

// WaitForCommit ждёт, пока команда cmd не будет зафиксирована
// на n серверах (с таймаутом 500ms), затем проверяет фиксацию.
func (h *Harness) WaitForCommit(cmd int, n int) {
	h.t.Helper()
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		h.mu.Lock()
		nc := 0
		for i := 0; i < h.n; i++ {
			if h.connected[i] {
				for c := 0; c < len(h.commits[i]); c++ {
					if h.commits[i][c].Data.(int) == cmd {
						nc++
						break
					}
				}
			}
		}
		h.mu.Unlock()
		if nc >= n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	h.CheckCommittedN(cmd, n)
}

// CheckNotCommitted проверяет, что команда cmd ещё не зафиксирована.
func (h *Harness) CheckNotCommitted(cmd int) {
	h.t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()

	for i := 0; i < h.n; i++ {
		if h.connected[i] {
			for c := 0; c < len(h.commits[i]); c++ {
				gotCmd := h.commits[i][c].Data.(int)
				if gotCmd == cmd {
					h.t.Errorf("found %d at commits[%d][%d], expected none", cmd, i, c)
				}
			}
		}
	}
}

// SubmitToServer отправляет команду серверу и дожидается коммита.
func (h *Harness) SubmitToServer(serverId int, cmd any) int {
	future := h.cluster[serverId].Apply(cmd, 0)

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

// DisconnectAll отключает все соединения указанного транспорта.
func (t *InmemTransport) DisconnectAll() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peers = make(map[ServerID]*InmemTransport)
}

func tlog(format string, a ...any) {
	format = "[TEST] " + format
	log.Printf(format, a...)
}

func sleepMs(n int) {
	time.Sleep(time.Duration(n) * time.Millisecond)
}

// startCollector запускает коллектор узла i для канала ch и возвращает
// канал завершения коллектора. Коллектор регистрируется в h.wg (join в
// Shutdown) и закрывает done по завершении (quiesce-барьер CrashPeer).
func (h *Harness) startCollector(i int, ch chan CommitEntry) chan struct{} {
	done := make(chan struct{})
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		defer close(done)
		h.collectCommits(i, ch)
	}()
	return done
}

// collectCommits читает сообщения из канала ch и добавляет их в
// h.commits[i] под h.mu. Завершается по закрытию канала (range).
func (h *Harness) collectCommits(i int, ch <-chan CommitEntry) {
	for c := range ch {
		h.mu.Lock()
		tlog("collectCommits(%d) got %+v", i, c)
		h.commits[i] = append(h.commits[i], c)
		h.mu.Unlock()
	}
}

// LeadershipTransfer инициирует передачу лидерства через публичный API.
func (h *Harness) LeadershipTransfer(serverID int, targetID ServerID) LeadershipTransferFuture {
	return h.cluster[serverID].LeadershipTransfer(targetID)
}

// SendTimeoutNow отправляет TimeoutNow напрямую через транспорт.
// Используется в тестах для проверки реакции на TimeoutNow без участия
// лидера (например, тест TimeoutNow на Follower).
func (h *Harness) SendTimeoutNow(from, to int) (TimeoutNowResponse, error) {
	return h.transports[from].TimeoutNow(
		ServerID(to),
		TimeoutNowRequest{
			RPCHeader: RPCHeader{
				ProtocolVersion: ProtocolVersion,
				ServerID:        from,
			},
		},
	)
}

// CheckLeaderIs проверяет, что лидером является указанный сервер.
func (h *Harness) CheckLeaderIs(expectedID int) {
	h.t.Helper()
	leaderID, _ := h.CheckSingleLeader()
	if leaderID != expectedID {
		h.t.Fatalf("leader = %d, want %d", leaderID, expectedID)
	}
}

// itoa — простейшее преобразование int в строку (без импорта strconv).
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}
