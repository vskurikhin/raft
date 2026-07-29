package raft

import (
	"log"
	"sync"
	"testing"
	"time"
)

const submitTimeout = 5 * time.Second

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

	n int
	t *testing.T
}

// NewHarness создаёт новую тестовую запряжку,
// инициализированную n серверами, соединёнными друг с другом
// через InmemTransport.
func NewHarness(t *testing.T, n int) *Harness {
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
		alive[i] = true
		connected[i] = true
	}
	close(ready)

	h := &Harness{
		cluster:     cluster,
		transports:  transports,
		storage:     storage,
		commitChans: commitChans,
		commits:     commits,
		connected:   connected,
		alive:       alive,
		n:           n,
		t:           t,
	}
	for i := 0; i < n; i++ {
		go h.collectCommits(i)
	}
	return h
}

// Shutdown останавливает все серверы в тестовом окружении.
func (h *Harness) Shutdown() {
	for i := 0; i < h.n; i++ {
		if h.alive[i] {
			h.alive[i] = false
			h.cluster[i].Stop()
			h.transports[i].Close()
		}
		close(h.commitChans[i])
	}
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
func (h *Harness) CrashPeer(id int) {
	tlog("Crash %d", id)
	h.DisconnectPeer(id)
	h.alive[id] = false
	h.cluster[id].Stop()
	h.transports[id].Close()

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
	sleepMs(20)
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
		time.Sleep(150 * Quantum * time.Millisecond)
	}

	h.t.Fatalf("leader not found")
	return -1, -1
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
				cmdOfN := h.commits[i][c].Command.(int)
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

// CheckNotCommitted проверяет, что команда cmd ещё не зафиксирована.
func (h *Harness) CheckNotCommitted(cmd int) {
	h.t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()

	for i := 0; i < h.n; i++ {
		if h.connected[i] {
			for c := 0; c < len(h.commits[i]); c++ {
				gotCmd := h.commits[i][c].Command.(int)
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

// collectCommits читает сообщения из канала commitChans[i].
func (h *Harness) collectCommits(i int) {
	for c := range h.commitChans[i] {
		h.mu.Lock()
		tlog("collectCommits(%d) got %+v", i, c)
		h.commits[i] = append(h.commits[i], c)
		h.mu.Unlock()
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
