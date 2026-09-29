package raft

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/vskurikhin/raft/pkg/raft/contract"
	"github.com/vskurikhin/raft/pkg/raft/store"
	"github.com/vskurikhin/raft/pkg/raft/transp"
)

// tcpDiskHarness — TCP-кластер с дисковым хранилищем каждого узла: позволяет
// останавливать и перезапускать узел с тем же каталогом данных, а также
// включать снимки для догона через InstallSnapshot.
type tcpDiskHarness struct {
	t          *testing.T
	n          int
	dirs       []string
	cms        []*ConsensusModule
	transports []*transp.TCPTransport
	alive      []bool
	ready      chan any
}

// newTCPDiskHarness собирает кластер из n узлов на TCP с FileStorage; при
// withSnapshots создаётся и FileSnapshot, иначе снимки не запускаются.
func newTCPDiskHarness(t *testing.T, n int, withSnapshots bool) *tcpDiskHarness {
	t.Helper()
	transports := make([]*transp.TCPTransport, n)
	for i := range n {
		tr, err := transp.NewTCPTransport("127.0.0.1:0", uniformTCPTimeouts(500*time.Millisecond), 2)
		if err != nil {
			t.Fatalf("NewTCPTransport(%d): %v", i, err)
		}
		transports[i] = tr
	}
	for i := range n {
		for j := range n {
			if i != j {
				transports[i].Connect(ServerID(j), string(transports[j].LocalAddr()))
			}
		}
	}
	h := &tcpDiskHarness{
		t:          t,
		n:          n,
		dirs:       make([]string, n),
		cms:        make([]*ConsensusModule, n),
		transports: transports,
		alive:      make([]bool, n),
		ready:      make(chan any),
	}
	for i := range n {
		h.dirs[i] = t.TempDir()
		var peers []int
		for j := range n {
			if j != i {
				peers = append(peers, j)
			}
		}
		h.cms[i] = h.build(i, peers, transports[i], withSnapshots)
		h.alive[i] = true
	}
	close(h.ready)
	return h
}

// build создаёт узел в каталоге h.dirs[id] с указанным транспортом.
func (h *tcpDiskHarness) build(id int, peers []int, tr Transport, withSnapshots bool) *ConsensusModule {
	storage := store.NewFileStorage(h.dirs[id])
	opts := make([]SnapshotStore, 0, 1)
	if withSnapshots {
		snaps, err := store.NewFileSnapshot(h.dirs[id], 2)
		if err != nil {
			h.t.Fatalf("NewFileSnapshot(%d): %v", id, err)
		}
		opts = append(opts, snaps)
	}
	return newConsensusModule(cmConfig{timers: tcpHarnessTimers(600 * time.Millisecond), disableStatsOutput: true},
		id, peers, tr, storage, newSnapshotTestFSM(), h.ready, opts...)
}

// Close останавливает живые узлы.
func (h *tcpDiskHarness) Close() {
	for i := range h.n {
		if h.alive[i] {
			h.cms[i].Stop()
			h.transports[i].Close()
			h.alive[i] = false
		}
	}
}

// stopPeer останавливает узел, сохраняя каталог данных для перезапуска.
func (h *tcpDiskHarness) stopPeer(id int) {
	if h.alive[id] {
		h.cms[id].Stop()
		h.transports[id].Close()
		h.alive[id] = false
	}
}

// restartPeer перезапускает узел с тем же каталогом данных на новом порту и
// восстанавливает двусторонние соединения с живыми соседями.
func (h *tcpDiskHarness) restartPeer(id int) {
	tr, err := transp.NewTCPTransport("127.0.0.1:0", uniformTCPTimeouts(500*time.Millisecond), 2)
	if err != nil {
		h.t.Fatalf("restart transport %d: %v", id, err)
	}
	h.transports[id] = tr
	for j := range h.n {
		if j == id || !h.alive[j] {
			continue
		}
		tr.Connect(ServerID(j), string(h.transports[j].LocalAddr()))
		h.transports[j].Connect(ServerID(id), string(tr.LocalAddr()))
	}
	var peers []int
	for j := range h.n {
		if j != id {
			peers = append(peers, j)
		}
	}
	h.cms[id] = h.build(id, peers, tr, false)
	h.alive[id] = true
}

// isolate разрывает соединения узла со всеми живыми соседями в обе стороны.
func (h *tcpDiskHarness) isolate(id int) {
	for j := range h.n {
		if j == id || !h.alive[j] {
			continue
		}
		h.transports[id].Disconnect(ServerID(j))
		h.transports[j].Disconnect(ServerID(id))
	}
}

// reconnect восстанавливает соединения узла со всеми живыми соседями.
func (h *tcpDiskHarness) reconnect(id int) {
	for j := range h.n {
		if j == id || !h.alive[j] {
			continue
		}
		h.transports[id].Connect(ServerID(j), string(h.transports[j].LocalAddr()))
		h.transports[j].Connect(ServerID(id), string(h.transports[id].LocalAddr()))
	}
}

// isLeader сообщает, считает ли узел себя лидером.
func (h *tcpDiskHarness) isLeader(id int) bool {
	_, _, leader := h.cms[id].Report()
	return leader
}

// waitLeader ждёт единственного лидера кластера.
func (h *tcpDiskHarness) waitLeader() int {
	h.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		leader, count := -1, 0
		for i := range h.n {
			if h.alive[i] && h.isLeader(i) {
				leader, count = i, count+1
			}
		}
		if count == 1 {
			return leader
		}
		time.Sleep(20 * time.Millisecond)
	}
	h.t.Fatal("no single leader")
	return -1
}

// waitLeaderExcept ждёт лидера, отличного от exclude.
func (h *tcpDiskHarness) waitLeaderExcept(exclude int) int {
	h.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		leader, count := -1, 0
		for i := range h.n {
			if i == exclude || !h.alive[i] {
				continue
			}
			if h.isLeader(i) {
				leader, count = i, count+1
			}
		}
		if count == 1 {
			return leader
		}
		time.Sleep(20 * time.Millisecond)
	}
	h.t.Fatal("no single leader except excluded")
	return -1
}

// commitTail возвращает индекс фиксации и последний индекс журнала узла.
func (h *tcpDiskHarness) commitTail(id int) (commit, tail int) {
	h.cms[id].mu.Lock()
	defer h.cms[id].mu.Unlock()
	return h.cms[id].cmState.commitIndex, h.cms[id].cmState.lastLogIndex
}

// waitAllCommit ждёт, пока все живые узлы зафиксируют хвост лидера.
func (h *tcpDiskHarness) waitAllCommit() {
	h.t.Helper()
	leader := h.waitLeader()
	_, tail := h.commitTail(leader)
	for i := range h.n {
		if i == leader || !h.alive[i] {
			continue
		}
		h.waitCommitTail(i, tail)
	}
}

// waitCommitTail ждёт фиксации узлом указанного хвоста.
func (h *tcpDiskHarness) waitCommitTail(id, want int) {
	h.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if commit, _ := h.commitTail(id); commit == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	commit, tail := h.commitTail(id)
	h.t.Fatalf("узел %d не догнал: commit=%d tail=%d, хочу %d", id, commit, tail, want)
}

// waitCatchUp ждёт, пока узел повторит журнал текущего лидера и зафиксирует
// его хвост.
func (h *tcpDiskHarness) waitCatchUp(id int) {
	h.t.Helper()
	leader := h.waitLeader()
	wantLog := copyCMLog(h.cms[leader])
	_, tail := h.commitTail(leader)
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		log := copyCMLog(h.cms[id])
		if logIsSuffix(log, wantLog) {
			if commit, _ := h.commitTail(id); commit == tail {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	lSnap, lCommit, lTail := h.snapshotCommitTail(leader)
	fSnap, fCommit, fTail := h.snapshotCommitTail(id)
	h.t.Fatalf("узел %d не догнал лидера %d: leader log=%d snap=%d commit=%d tail=%d; "+
		"follower log=%d snap=%d commit=%d tail=%d", id, leader, len(wantLog), lSnap, lCommit, lTail, len(copyCMLog(h.cms[id])), fSnap, fCommit, fTail)
}

// snapshotCommitTail возвращает индекс снимка, фиксации и последний индекс.
func (h *tcpDiskHarness) snapshotCommitTail(id int) (snap, commit, tail int) {
	h.cms[id].mu.Lock()
	defer h.cms[id].mu.Unlock()
	return h.cms[id].cmState.lastSnapshotIndex, h.cms[id].cmState.commitIndex, h.cms[id].cmState.lastLogIndex
}

// logsEqual сравнивает журналы по индексам, термам и типам.
func logsEqual(a, b []LogEntry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Index != b[i].Index || a[i].Term != b[i].Term || a[i].Type != b[i].Type {
			return false
		}
	}
	return true
}

// logIsSuffix сообщает, что журнал a совпадает с хвостом журнала b: после
// установки снимка лидер может хранить более длинный усечённый префикс, чем
// ведомый, а существенной является совпадающая часть от первого индекса
// ведомого.
func logIsSuffix(a, b []LogEntry) bool {
	if len(a) > len(b) {
		return false
	}
	offset := len(b) - len(a)
	for i := range a {
		if a[i].Index != b[offset+i].Index || a[i].Term != b[offset+i].Term || a[i].Type != b[offset+i].Type {
			return false
		}
	}
	return true
}

// TestTCPClusterDivergingLogConflict — V22: расходящийся журнал на реальном
// TCP. Изолированный старый лидер дописывает незафиксированную запись;
// большинство выбирает нового лидера и пишет на тот же индекс свою. После
// возврата старый лидер обязан вытеснить конфликтующий суффикс и повторить
// журнал нового лидера.
func TestTCPClusterDivergingLogConflict(t *testing.T) {
	defer leaktest.CheckTimeout(t, 3*LeaktestBudget)()
	h := newTCPDiskHarness(t, 3, false)
	defer h.Close()

	leader := h.waitLeader()
	for i := range 2 {
		if err := h.cms[leader].Apply(fmt.Sprintf("k%d=v%d", i, i), 3*time.Second).Error(); err != nil {
			t.Fatalf("начальная команда %d: %v", i, err)
		}
	}
	h.waitAllCommit()

	_, oldTerm, _ := h.cms[leader].Report()
	h.isolate(leader)
	_, tailBefore := h.commitTail(leader)
	// Лидер теряет кворум, но успевает дописать запись в свой журнал.
	_ = h.cms[leader].Apply("divergent", 500*time.Millisecond).Error()
	divergentIndex := waitLogGrowth(t, h, leader, tailBefore)

	newLeader := h.waitLeaderExcept(leader)
	if _, term, _ := h.cms[newLeader].Report(); term <= oldTerm {
		t.Fatalf("новый лидер %d с термом %d, старый терм %d", newLeader, term, oldTerm)
	}
	if err := h.cms[newLeader].Apply("winner", 3*time.Second).Error(); err != nil {
		t.Fatalf("команда нового лидера: %v", err)
	}
	winnerLog := waitWinnerLog(t, h, newLeader, divergentIndex+1)

	oldDivergentTerm := logTermAt(t, h, leader, divergentIndex)
	h.reconnect(leader)
	h.waitCatchUp(leader)

	finalLog := copyCMLog(h.cms[leader])
	if !logsEqual(finalLog, winnerLog) {
		t.Fatalf("журнал старого лидера не совпал с новым: %d/%d записей", len(finalLog), len(winnerLog))
	}
	if newTerm := finalLog[divergentIndex].Term; newTerm == oldDivergentTerm {
		t.Fatalf("конфликтующий терм на индексе %d не вытеснен: %d", divergentIndex, newTerm)
	}
}

// waitLogGrowth ждёт роста журнала узла выше before и возвращает индекс
// последней записи.
func waitLogGrowth(t *testing.T, h *tcpDiskHarness, id, before int) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_, tail := h.commitTail(id)
		if tail > before {
			return tail
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("журнал узла %d не вырос выше %d", id, before)
	return -1
}

// waitWinnerLog ждёт, пока журнал нового лидера достигнет minIndex.
func waitWinnerLog(t *testing.T, h *tcpDiskHarness, id, minIndex int) []LogEntry {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		log := copyCMLog(h.cms[id])
		if len(log) > minIndex {
			return log
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("журнал лидера %d не достиг индекса %d", id, minIndex)
	return nil
}

// logTermAt возвращает терм записи с указанным индексом.
func logTermAt(t *testing.T, h *tcpDiskHarness, id, index int) int {
	t.Helper()
	log := copyCMLog(h.cms[id])
	for i := range log {
		if log[i].Index == index {
			return log[i].Term
		}
	}
	t.Fatalf("у узла %d нет записи с индексом %d", id, index)
	return -1
}

// TestTCPClusterRestartRecovery — V22: перезапуск узла с дисковым состоянием
// на реальном TCP. Ведомый возвращается в кластер и догоняет хвост; затем
// перезапускается лидер, кластер выбирает нового, старый возвращается и
// догоняет.
func TestTCPClusterRestartRecovery(t *testing.T) {
	defer leaktest.CheckTimeout(t, 3*LeaktestBudget)()
	h := newTCPDiskHarness(t, 3, false)
	defer h.Close()

	leader := h.waitLeader()
	for i := range 3 {
		if err := h.cms[leader].Apply(fmt.Sprintf("r%d=v%d", i, i), 3*time.Second).Error(); err != nil {
			t.Fatalf("команда %d: %v", i, err)
		}
	}
	h.waitAllCommit()

	follower := (leader + 1) % 3
	h.stopPeer(follower)
	for i := range 3 {
		if err := h.cms[leader].Apply(fmt.Sprintf("after-stop-%d", i), 3*time.Second).Error(); err != nil {
			t.Fatalf("команда после остановки %d: %v", i, err)
		}
	}
	h.restartPeer(follower)
	h.waitCatchUp(follower)

	h.stopPeer(leader)
	newLeader := h.waitLeaderExcept(leader)
	if err := h.cms[newLeader].Apply("after-leader-restart", 3*time.Second).Error(); err != nil {
		t.Fatalf("команда нового лидера: %v", err)
	}
	h.restartPeer(leader)
	h.waitCatchUp(leader)
}

// TestTCPClusterSnapshotCatchUp — V22: догон отставшего узла снимком на
// реальном TCP. Лидер уплотняет журнал дальше позиции отставшего; после
// возврата отставший получает InstallSnapshot и восстанавливает FSM.
func TestTCPClusterSnapshotCatchUp(t *testing.T) {
	defer leaktest.CheckTimeout(t, 3*LeaktestBudget)()
	h := newTCPDiskHarness(t, 3, true)
	defer h.Close()
	for i := range h.n {
		h.cms[i].SetSnapshotConfig(4, 2, 20*time.Millisecond)
	}

	leader := h.waitLeader()
	follower := (leader + 1) % 3
	for i := range 5 {
		if err := h.cms[leader].Apply(fmt.Sprintf("s%03d=v%03d", i, i), 3*time.Second).Error(); err != nil {
			t.Fatalf("начальная команда %d: %v", i, err)
		}
	}
	h.waitAllCommit()

	_, followerTail := h.commitTail(follower)
	h.isolate(follower)
	const extra = 40
	for i := 5; i < extra; i++ {
		if err := h.cms[leader].Apply(fmt.Sprintf("s%03d=v%03d", i, i), 3*time.Second).Error(); err != nil {
			t.Fatalf("команда %d: %v", i, err)
		}
	}
	h.waitCommitTail((leader+2)%3, mustTail(t, h, leader))
	leaderSnap := waitLeaderSnapshotAbove(t, h, leader, followerTail)

	h.reconnect(follower)
	waitSnapshotReached(t, h, follower, leaderSnap)
	h.waitCatchUp(follower)

	fsm := h.cms[follower].fsm.(*snapshotTestFSM)
	waitFSMKey(t, fsm, "s039", "v039")
}

// waitFSMKey ждёт, пока машина состояний применит ключ: применение идёт
// асинхронно и может отставать от индекса фиксации.
func waitFSMKey(t *testing.T, fsm *snapshotTestFSM, key, want string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if got := fsm.getState(key); got == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("FSM ведомого после снимка: %s=%q, хочу %q", key, fsm.getState(key), want)
}

// mustTail возвращает последний индекс журнала узла.
func mustTail(t *testing.T, h *tcpDiskHarness, id int) int {
	t.Helper()
	_, tail := h.commitTail(id)
	return tail
}

// waitLeaderSnapshotAbove ждёт, пока индекс снимка лидера превысит min, и
// возвращает его. Условие гарантирует, что отставший узел действительно
// находится за пределами усечённого журнала.
func waitLeaderSnapshotAbove(t *testing.T, h *tcpDiskHarness, leader, min int) int {
	t.Helper()
	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		h.cms[leader].mu.Lock()
		snapIdx := h.cms[leader].cmState.lastSnapshotIndex
		h.cms[leader].mu.Unlock()
		if snapIdx > min {
			return snapIdx
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("лидер не уплотнил журнал выше %d", min)
	return -1
}

// waitSnapshotReached ждёт, пока отставший узел примет снимок не старее
// указанного индекса.
func waitSnapshotReached(t *testing.T, h *tcpDiskHarness, id, want int) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		h.cms[id].mu.Lock()
		snapIdx := h.cms[id].cmState.lastSnapshotIndex
		h.cms[id].mu.Unlock()
		if snapIdx >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	h.cms[id].mu.Lock()
	snapIdx := h.cms[id].cmState.lastSnapshotIndex
	h.cms[id].mu.Unlock()
	t.Fatalf("узел %d не принял снимок: lastSnapshotIndex=%d, want >= %d", id, snapIdx, want)
}

// TestTCPClusterLegacyOversizeRefusal — V22 на реальном TCP: legacy-запись
// длиннее D не отправляется; отказ именован (ErrLimit), счётчик parameter=D
// и классификация по индексу записи видимы, nextIndex сохраняется.
func TestTCPClusterLegacyOversizeRefusal(t *testing.T) {
	defer leaktest.CheckTimeout(t, 2*LeaktestBudget)()
	limits := contract.Limits{MaxFrameBytes: 4128, MaxEntries: 3, MaxDataBytes: 64, MaxConfigurationBytes: 16}
	sender, err := transp.NewTCPTransportWithLimits("127.0.0.1:0", uniformTCPTimeouts(500*time.Millisecond), 2, limits)
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := transp.NewTCPTransportWithLimits("127.0.0.1:0", uniformTCPTimeouts(500*time.Millisecond), 2, limits)
	if err != nil {
		t.Fatal(err)
	}
	sender.Connect(1, string(receiver.LocalAddr()))
	defer sender.Close()
	defer receiver.Close()

	cm := commitRuleCM(1, []LogEntry{
		{Index: 0, Term: 1, Data: "ok"},
		{Index: 1, Term: 1, Data: strings.Repeat("x", 200)},
	}, -1)
	cm.limits = limits
	cm.transport = sender
	cm.cmState.state = Leader
	cm.leaderState.nextIndex = map[int]int{1: 1}
	cm.leaderState.matchIndex = map[int]int{1: 0}
	cm.leaderState.inflightAE = map[int]*atomic.Bool{1: new(atomic.Bool)}
	cm.leaderState.nextVerifyRedispatchAt = map[int]time.Time{}

	cm.mu.Lock()
	nextBefore := cm.leaderState.nextIndex[1]
	cm.mu.Unlock()
	cm.leaderSendAEsToPeer(1, 1, 0, true)
	cm.mu.Lock()
	counts := cm.limitRejectionsLocked()
	nextAfter := cm.leaderState.nextIndex[1]
	cm.mu.Unlock()
	if counts[limitRejectionKey{direction: limitDirectionSend, peer: "1", parameter: limitParameterD}] != 1 {
		t.Fatalf("счётчик parameter=D: %v", counts)
	}
	if nextAfter != nextBefore {
		t.Fatalf("nextIndex изменён: %d -> %d", nextBefore, nextAfter)
	}
	parameter, index, actual := classifyAELimit(cm.cmState.log[1:], limits)
	if parameter != limitParameterD || index != 1 || actual <= 64 {
		t.Fatalf("классификация: %s %d %d", parameter, index, actual)
	}
}

// TestTCPClusterPreVoteAndTimeoutNow — V22: PreVote не раздувает терм
// изолированного узла, а передача лидерства действительно уходит по TCP.
func TestTCPClusterPreVoteAndTimeoutNow(t *testing.T) {
	defer leaktest.CheckTimeout(t, 3*LeaktestBudget)()
	h := newTCPDiskHarness(t, 3, false)
	defer h.Close()

	leader := h.waitLeader()
	_, term0, _ := h.cms[leader].Report()
	follower := (leader + 1) % 3
	h.isolate(follower)
	// Несколько баз перевыборов: без PreVote узел раздувал бы терм.
	time.Sleep(3 * time.Second)
	if _, isoTerm, _ := h.cms[follower].Report(); isoTerm > term0 {
		t.Fatalf("изолированный узел раздул терм %d -> %d: PreVote не подавил перевыборы", term0, isoTerm)
	}
	h.reconnect(follower)
	h.waitCatchUp(follower)

	cur := h.waitLeader()
	target := (cur + 1) % 3
	if err := h.cms[cur].LeadershipTransfer(ServerID(target)).Error(); err != nil {
		t.Fatalf("LeadershipTransfer: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if h.isLeader(target) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("лидерство не перешло на узел %d", target)
}
