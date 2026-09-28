package raft

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/vskurikhin/raft/pkg/raft/contract"
	"github.com/vskurikhin/raft/pkg/raft/protocol"
	"github.com/vskurikhin/raft/pkg/raft/store"
	"github.com/vskurikhin/raft/pkg/raft/transp"
)

// hdr3 — заголовок RPC совместимой версии протокола.
func hdr3(serverID int) RPCHeader {
	return RPCHeader{ProtocolVersion: ProtocolVersion, ServerID: serverID}
}

// TestErrUnsupportedProtocolIsContractMarker — корневой маркер несовместимой
// версии — то же значение, что contract.ErrUnsupportedProtocol.
func TestErrUnsupportedProtocolIsContractMarker(t *testing.T) {
	if ErrUnsupportedProtocol != contract.ErrUnsupportedProtocol { //nolint:errorlint // проверка идентичности значения
		t.Fatal("root marker is an independent value")
	}
	if !errors.Is(fmt.Errorf("x: %w", contract.ErrUnsupportedProtocol), ErrUnsupportedProtocol) ||
		!errors.Is(fmt.Errorf("x: %w", ErrUnsupportedProtocol), contract.ErrUnsupportedProtocol) {
		t.Fatal("errors.Is does not hold both ways")
	}
}

// deadCM — остановленный узел: обработчики трёх RPC с Dead-веткой.
func deadCM() *ConsensusModule {
	cm := &ConsensusModule{limits: testLimits}
	cm.cmState.state = Dead
	cm.cmState.currentTerm = 5
	cm.cmState.lastSnapshotIndex = -1
	return cm
}

// serveCM обслуживает входящие RPC транспорта обработчиками CM до stop.
func serveCM(cm *ConsensusModule, trans Transport) func() {
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			select {
			case rpc := <-trans.Consumer():
				cm.handleRPC(rpc)
			case <-done:
				return
			}
		}
	}()
	return func() { close(done); <-stopped }
}

// TestDeadHandlersReturnShutdownMarker — три существующие Dead-ветки
// (AppendEntries, RequestVote, RequestPreVote) возвращают явный
// contract.ErrRaftShutdown: прямой вызов, Inmem и реальный TCP дают
// одинаковый errors.Is. TimeoutNow и InstallSnapshot Dead-проверки не
// получают: их штатный ответ — код 0 с Success=false и Error=nil.
func TestDeadHandlersReturnShutdownMarker(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()
	cm := deadCM()

	if err := cm.AppendEntries(AppendEntriesArgs{RPCHeader: hdr3(1)}, &AppendEntriesReply{}); !errors.Is(err, contract.ErrRaftShutdown) {
		t.Fatalf("direct AppendEntries: %v", err)
	}
	if err := cm.RequestVote(RequestVoteArgs{RPCHeader: hdr3(1)}, &RequestVoteReply{}); !errors.Is(err, contract.ErrRaftShutdown) {
		t.Fatalf("direct RequestVote: %v", err)
	}
	if err := cm.RequestPreVote(RequestPreVoteArgs{RPCHeader: hdr3(1)}, &RequestPreVoteReply{}); !errors.Is(err, contract.ErrRaftShutdown) {
		t.Fatalf("direct RequestPreVote: %v", err)
	}

	inmemClient, inmemServer := transp.NewInmemTransport("c"), transp.NewInmemTransport("s")
	inmemClient.Connect(1, inmemServer)
	tcpClient, tcpServer, cleanup := newTCPTransportPair(t)
	defer cleanup()
	defer inmemClient.Close()
	defer inmemServer.Close()
	defer serveCM(cm, inmemServer)()
	defer serveCM(cm, tcpServer)()

	for name, client := range map[string]Transport{"inmem": inmemClient, "tcp": tcpClient} {
		if _, err := client.AppendEntries(1, AppendEntriesArgs{RPCHeader: hdr3(0), Term: 1}); !errors.Is(err, contract.ErrRaftShutdown) {
			t.Fatalf("%s AppendEntries: %v", name, err)
		}
		if _, err := client.RequestVote(1, RequestVoteArgs{RPCHeader: hdr3(0), Term: 1}); !errors.Is(err, contract.ErrRaftShutdown) {
			t.Fatalf("%s RequestVote: %v", name, err)
		}
		if _, err := client.RequestPreVote(1, RequestPreVoteArgs{RPCHeader: hdr3(0), Term: 1}); !errors.Is(err, contract.ErrRaftShutdown) {
			t.Fatalf("%s RequestPreVote: %v", name, err)
		}
		reply, err := client.TimeoutNow(1, TimeoutNowRequest{RPCHeader: hdr3(0)})
		if err != nil || reply.Success || reply.Term != 5 {
			t.Fatalf("%s TimeoutNow on dead node: %+v %v (want code 0, Success=false)", name, reply, err)
		}
		isReply, err := client.InstallSnapshot(1, InstallSnapshotRequest{RPCHeader: hdr3(0), Term: 1, LastLogIndex: 1,
			LastLogTerm: 1, DataSize: 3}, bytes.NewReader([]byte{1, 2, 3}))
		if err != nil || isReply.Success {
			t.Fatalf("%s InstallSnapshot on dead node: %+v %v (want code 0, Success=false)", name, isReply, err)
		}
	}
}

// followerCM — ведомый без запущенных выборов: ready не закрыт.
func followerCM(t *testing.T) (*ConsensusModule, chan any) {
	t.Helper()
	ready := make(chan any)
	cm := NewConsensusModule(1, []int{0, 2}, transp.NewInmemTransport("f"), store.NewMapStorage(), NoOpFSM{}, ready,
		store.NewInmemSnapshot())
	return cm, ready
}

// TestVersionCheckedBeforeSemantics — V21: TimeoutNow и InstallSnapshot
// с несовместимой версией протокола отвечают маркером до изменения терма,
// роли, журнала и снимка; корректная версия рядом проходит.
func TestVersionCheckedBeforeSemantics(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()
	cm, ready := followerCM(t)
	defer func() { close(ready); cm.Stop() }()

	state := func() (CMState, int, int, int) {
		cm.mu.Lock()
		defer cm.mu.Unlock()
		return cm.cmState.state, cm.cmState.currentTerm, cm.cmState.leaderID, cm.cmState.lastSnapshotIndex
	}
	st0, term0, leader0, snap0 := state()

	for _, pv := range []int{0, 2, 4, -1} {
		respCh := make(chan RPCResponse, 1)
		req := &TimeoutNowRequest{RPCHeader: RPCHeader{ProtocolVersion: pv, ServerID: 0}}
		cm.timeoutNow(RPC{Command: req, RespChan: respCh}, req)
		if resp := <-respCh; !errors.Is(resp.Error, ErrUnsupportedProtocol) {
			t.Fatalf("TimeoutNow PV %d: %+v", pv, resp)
		}
		respCh = make(chan RPCResponse, 1)
		snap := &InstallSnapshotRequest{RPCHeader: RPCHeader{ProtocolVersion: pv}, Term: term0 + 7, LeaderID: 0,
			LastLogIndex: 50, LastLogTerm: 3, DataSize: 4}
		reader := bytes.NewReader([]byte{1, 2, 3, 4})
		cm.handleInstallSnapshot(RPC{Command: snap, Reader: reader, RespChan: respCh}, snap)
		resp := <-respCh
		if !errors.Is(resp.Error, ErrUnsupportedProtocol) {
			t.Fatalf("InstallSnapshot PV %d: %+v", pv, resp)
		}
		if reader.Len() != 0 {
			t.Fatalf("InstallSnapshot PV %d: body not drained", pv)
		}
		if st, term, leader, sn := state(); st != st0 || term != term0 || leader != leader0 || sn != snap0 {
			t.Fatalf("PV %d changed state: %v/%d/%d/%d -> %v/%d/%d/%d", pv, st0, term0, leader0, snap0, st, term, leader, sn)
		}
		if cm.cmState.candidateFromLeadershipTransfer.Load() {
			t.Fatalf("PV %d: leadership transfer flag set", pv)
		}
	}

	// Корректная версия: TimeoutNow начинает выборы.
	respCh := make(chan RPCResponse, 1)
	req := &TimeoutNowRequest{RPCHeader: hdr3(0)}
	cm.timeoutNow(RPC{Command: req, RespChan: respCh}, req)
	resp := <-respCh
	reply, ok := resp.Reply.(*TimeoutNowResponse)
	if resp.Error != nil || !ok || !reply.Success || reply.Term != term0+1 {
		t.Fatalf("valid TimeoutNow: %+v %+v", resp, reply)
	}
}

// TestTimeoutNowTermReadNoRace — V21: ветви timeoutNow лидера и не-ведомого
// снимают currentTerm под cm.mu до Unlock; параллельные изменения терма под
// cm.mu не образуют гонки (проверяется детектором гонок).
func TestTimeoutNowTermReadNoRace(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()
	cm, ready := followerCM(t)
	defer func() { close(ready); cm.Stop() }()

	for _, role := range []CMState{Candidate, PreCandidate, Leader} {
		cm.mu.Lock()
		cm.cmState.state = role
		cm.mu.Unlock()
		var wg sync.WaitGroup
		stop := make(chan struct{})
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				cm.mu.Lock()
				cm.cmState.currentTerm++
				cm.mu.Unlock()
			}
		}()
		for range 200 {
			respCh := make(chan RPCResponse, 1)
			req := &TimeoutNowRequest{RPCHeader: hdr3(0)}
			cm.timeoutNow(RPC{Command: req, RespChan: respCh}, req)
			if r := (<-respCh).Reply.(*TimeoutNowResponse); r.Success {
				t.Fatalf("%v: TimeoutNow succeeded", role)
			}
		}
		close(stop)
		wg.Wait()
	}
	cm.mu.Lock()
	cm.cmState.state = Follower
	cm.mu.Unlock()
}

// commitRuleCM — ведомый с журналом entries (индексы с 0) в терме term.
func commitRuleCM(term int, entries []LogEntry, commitIndex int) *ConsensusModule {
	cm := &ConsensusModule{limits: testLimits}
	cm.storage = store.NewMapStorage()
	cm.fsmMutateCh = make(chan []*commitTuple, _batchApplyBuffer)
	cm.shutdownCh = make(chan struct{})
	cm.leaderState.inflight = make(map[int]*logFuture)
	cm.cmState.state = Follower
	cm.cmState.currentTerm = term
	cm.cmState.votedFor = -1
	cm.cmState.lastSnapshotIndex = -1
	cm.cmState.lastSnapshotTerm = -1
	cm.cmState.termIndexMap = make(map[int]int)
	cm.cmState.log = append([]LogEntry(nil), entries...)
	cm.cmState.commitIndex = commitIndex
	cm.cmState.lastApplied = commitIndex
	cm.rebuildLastLogLocked()
	cm.rebuildTermIndexMapLocked()
	cm.storage.RewriteLog(cm.cmState.log)
	cm.clearLogDirtyLocked()
	return cm
}

// drainBatches возвращает индексы записей, переданных на применение.
func drainBatches(cm *ConsensusModule) []int {
	var applied []int
	for {
		select {
		case batch := <-cm.fsmMutateCh:
			for _, ct := range batch {
				applied = append(applied, ct.log.Index)
			}
		default:
			return applied
		}
	}
}

func entriesOfTerms(terms ...int) []LogEntry {
	out := make([]LogEntry, len(terms))
	for i, term := range terms {
		out[i] = LogEntry{Index: i, Term: term, Type: LogCommand, Data: i}
	}
	return out
}

// TestFollowerCommitBoundedByBatch — V25: commitIndex ведомого =
// max(commitIndex, min(LeaderCommit, PrevLogIndex+len(Entries))).
func TestFollowerCommitBoundedByBatch(t *testing.T) {
	ae := func(prev, prevTerm, leaderCommit int, entries ...LogEntry) AppendEntriesArgs {
		return AppendEntriesArgs{RPCHeader: hdr3(9), Term: 3, LeaderID: 9, PrevLogIndex: prev, PrevLogTerm: prevTerm,
			LeaderCommit: leaderCommit, Entries: entries}
	}
	run := func(cm *ConsensusModule, args AppendEntriesArgs) AppendEntriesReply {
		var reply AppendEntriesReply
		if err := cm.AppendEntries(args, &reply); err != nil {
			t.Fatal(err)
		}
		return reply
	}

	t.Run("partial batch does not commit stale tail", func(t *testing.T) {
		// Журнал ведомого [0:t1, 1:t1, 2:t1, 3:t2]; пакет Prev=1, Entries=[2:t1],
		// LeaderCommit=4: фиксируется только 2, запись 3:t2 не применяется.
		cm := commitRuleCM(3, entriesOfTerms(1, 1, 1, 2), -1)
		reply := run(cm, ae(1, 1, 4, LogEntry{Index: 2, Term: 1, Data: 2}))
		if !reply.Success || cm.cmState.commitIndex != 2 {
			t.Fatalf("commitIndex = %d, want 2 (%+v)", cm.cmState.commitIndex, reply)
		}
		if got := drainBatches(cm); fmt.Sprint(got) != "[0 1 2]" {
			t.Fatalf("applied %v, want [0 1 2]", got)
		}
	})
	t.Run("full batch commits tail", func(t *testing.T) {
		cm := commitRuleCM(3, entriesOfTerms(1, 1, 1, 2), -1)
		run(cm, ae(1, 1, 4, LogEntry{Index: 2, Term: 1, Data: 2}, LogEntry{Index: 3, Term: 2, Data: 3}))
		if cm.cmState.commitIndex != 3 {
			t.Fatalf("commitIndex = %d, want 3", cm.cmState.commitIndex)
		}
	})
	t.Run("empty entries", func(t *testing.T) {
		cm := commitRuleCM(3, entriesOfTerms(1, 1, 1, 2), -1)
		run(cm, ae(2, 1, 4))
		if cm.cmState.commitIndex != 2 {
			t.Fatalf("commitIndex = %d, want min(LeaderCommit, PrevLogIndex) = 2", cm.cmState.commitIndex)
		}
	})
	t.Run("monotonic: no decrease and no re-apply", func(t *testing.T) {
		terms := make([]int, 161)
		for i := range terms {
			terms[i] = 1
		}
		cm := commitRuleCM(3, entriesOfTerms(terms...), 100)
		batch := make([]LogEntry, 10)
		for i := range batch {
			batch[i] = LogEntry{Index: 51 + i, Term: 1, Data: 51 + i}
		}
		reply := run(cm, ae(50, 1, 150, batch...))
		if !reply.Success || cm.cmState.commitIndex != 100 || cm.cmState.lastApplied != 100 {
			t.Fatalf("commitIndex = %d lastApplied = %d, want 100", cm.cmState.commitIndex, cm.cmState.lastApplied)
		}
		if got := drainBatches(cm); len(got) != 0 {
			t.Fatalf("re-applied %v", got)
		}
		run(cm, ae(40, 1, 150))
		if cm.cmState.commitIndex != 100 {
			t.Fatalf("empty AE decreased commitIndex to %d", cm.cmState.commitIndex)
		}
	})
	if got := followerCommitIndex(10, math.MaxInt, 1); got != 10 {
		t.Fatalf("overflow guard: %d", got)
	}
	if got := followerCommitIndex(150, 50, 10); got != 60 {
		t.Fatalf("followerCommitIndex(150, 50, 10) = %d, want 60", got)
	}
}

// TestNextIndexArgsEntriesBoundedByN — V06/V25: пакет — префикс суффикса
// не длиннее MaxEntries; успешный ответ продвигает nextIndex ровно на длину
// отправленного префикса; последовательные пакеты доходят до хвоста.
func TestNextIndexArgsEntriesBoundedByN(t *testing.T) {
	limits := contract.Limits{MaxFrameBytes: 262144, MaxEntries: 3, MaxDataBytes: 8192, MaxConfigurationBytes: 2560}
	terms := make([]int, 10)
	for i := range terms {
		terms[i] = 1
	}
	cm := commitRuleCM(1, entriesOfTerms(terms...), -1)
	cm.limits = limits
	cm.cmState.state = Leader
	cm.leaderState.nextIndex = map[int]int{1: 2}
	cm.leaderState.matchIndex = map[int]int{1: 1}
	cm.leaderState.commitmentTracker = newCommitmentTracker(cm.id, 1, 0, make(chan int, 1))

	for _, want := range [][2]int{{2, 3}, {5, 3}, {8, 2}} {
		ni, args, entries, _ := cm.nextIndexArgsEntries(1, 1)
		if ni != want[0] || len(entries) != want[1] || len(args.Entries) != want[1] || args.Entries[0].Index != ni {
			t.Fatalf("ni=%d entries=%d, want %v", ni, len(entries), want)
		}
		cm.mu.Lock()
		cm.applyAESuccessLocked(1, ni, len(entries))
		next := cm.leaderState.nextIndex[1]
		cm.mu.Unlock()
		if next != ni+len(entries) {
			t.Fatalf("nextIndex = %d, want %d", next, ni+len(entries))
		}
	}
	if cm.leaderState.matchIndex[1] != 9 {
		t.Fatalf("matchIndex = %d, want tail 9", cm.leaderState.matchIndex[1])
	}
}

// TestAELimitRejectionVisible — V34: запись журнала с сетевой длиной Data
// больше D (легаси) не отправляется: именованный отказ ErrLimit, счётчик по
// соседу и параметру D, nextIndex не меняется; классификация без Data.
func TestAELimitRejectionVisible(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()
	limits := contract.Limits{MaxFrameBytes: 4128, MaxEntries: 3, MaxDataBytes: 64, MaxConfigurationBytes: 16}
	sender, err := transp.NewInmemTransportWithLimits("l", limits)
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := transp.NewInmemTransportWithLimits("r", limits)
	if err != nil {
		t.Fatal(err)
	}
	sender.Connect(1, receiver)
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

	cm.leaderSendAEsToPeer(1, 1, 0, true)

	cm.mu.Lock()
	counts := cm.limitRejectionsLocked()
	next := cm.leaderState.nextIndex[1]
	cm.mu.Unlock()
	if counts[limitRejectionKey{direction: limitDirectionSend, peer: "1", parameter: limitParameterD}] != 1 || next != 1 {
		t.Fatalf("counts %v nextIndex %d, want D=1 and nextIndex 1", counts, next)
	}
	parameter, index, actual := classifyAELimit(cm.cmState.log[1:], limits)
	if parameter != limitParameterD || index != 1 || actual <= 64 {
		t.Fatalf("classify: %s %d %d", parameter, index, actual)
	}
	if parameter, _, _ = classifyAELimit(make([]LogEntry, 4), limits); parameter != limitParameterN {
		t.Fatalf("classify N: %s", parameter)
	}
}

// TestApplyPreflight — V07/V31/V35: Apply измеряет сетевую длину Data вне
// cm.mu: превышение D — ErrCommandTooLarge, некодируемая Data — ошибка
// кодирования (не ErrCommandTooLarge); в обоих случаях журнал не растёт
// (в том числе при Inmem+MapStorage). Значение на границе принимается.
func TestApplyPreflight(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()
	h := NewHarness(t, 3)
	defer h.Shutdown()
	leader, _ := h.CheckSingleLeader()
	cm := h.cluster[leader]
	lastIndex := func() int {
		cm.mu.Lock()
		defer cm.mu.Unlock()
		return cm.cmState.lastLogIndex
	}
	before := lastIndex()

	if err := cm.Apply(strings.Repeat("x", 9000), time.Second).Error(); !errors.Is(err, ErrCommandTooLarge) {
		t.Fatalf("oversized: %v", err)
	}
	type unregistered struct{ A int }
	for name, data := range map[string]any{"func": func() {}, "channel": make(chan int), "unregistered": unregistered{1}} {
		err := cm.Apply(data, time.Second).Error()
		if err == nil || errors.Is(err, ErrCommandTooLarge) {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if got := lastIndex(); got != before {
		t.Fatalf("log grew %d -> %d on rejected Apply", before, got)
	}
	// Наибольшая допустимая строка: сетевая длина ровно D.
	n := 8192 - 64
	for {
		size, err := protocol.MeasureData(strings.Repeat("y", n+1), 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		if size > 8192 {
			break
		}
		n++
	}
	if err := cm.Apply(strings.Repeat("y", n), time.Second).Error(); err != nil {
		t.Fatalf("at D: %v", err)
	}
	if err := cm.Apply(strings.Repeat("y", n+1), time.Second).Error(); !errors.Is(err, ErrCommandTooLarge) {
		t.Fatalf("D+1: %v", err)
	}
}

// TestConfigurationChangeLimits — V30: запись конфигурации проверяется до
// журнала: EncodeConfiguration больше C — именованный отказ, журнал и
// конфигурация не меняются; допустимое изменение проходит.
func TestConfigurationChangeLimits(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()
	h := NewHarness(t, 3)
	defer h.Shutdown()
	leader, _ := h.CheckSingleLeader()
	cm := h.cluster[leader]

	cm.mu.Lock()
	before := cm.cmState.lastLogIndex
	cm.mu.Unlock()
	err := cm.AddNonvoter(7, ServerAddress(strings.Repeat("a", 3000))).Error()
	if !errors.Is(err, protocol.ErrLimit) || !strings.Contains(err.Error(), "MaxConfigurationBytes 2560") {
		t.Fatalf("oversized configuration: %v", err)
	}
	cm.mu.Lock()
	after := cm.cmState.lastLogIndex
	servers := len(cm.cmState.configurations.latest.ConfigServers)
	cm.mu.Unlock()
	if after != before || servers != 3 {
		t.Fatalf("log %d -> %d, servers %d after rejected change", before, after, servers)
	}
	if _, err = checkConfigurationData(make([]byte, 2560), testLimits); err != nil {
		t.Fatalf("configuration at C: %v", err)
	}
	if parameter, err := checkConfigurationData(make([]byte, 2561), testLimits); !errors.Is(err, protocol.ErrLimit) ||
		parameter != limitParameterC {
		t.Fatalf("configuration at C+1: %s %v", parameter, err)
	}
}

// noProviderTransport — транспорт без интерфейсов профиля.
type noProviderTransport struct{ Transport }

// recoverPanic выполняет fn и возвращает текст паники.
func recoverPanic(fn func()) (msg string) {
	defer func() {
		if r := recover(); r != nil {
			msg = fmt.Sprint(r)
		}
	}()
	fn()
	return ""
}

// TestConstructorProfileChecks — V27/V28/V34: общий конструктор CM
// проверяет профиль до запуска горутин: отсутствие LimitsProvider или
// TransportTimingProvider и несовпадение пределов — паника с именем
// параметра; TCP 500/500 на умолчаниях таймеров (553 >= 430) — паника
// временного профиля; с базой выборов 600 мс (553 < 600) — узел создаётся;
// Inmem 500/500 (InProcess=true) проходит на умолчаниях.
func TestConstructorProfileChecks(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()
	ready := make(chan any)
	close(ready)
	build := func(cfg cmConfig, trans Transport) func() {
		return func() {
			cm := newConsensusModule(cfg, 0, nil, trans, store.NewMapStorage(), NoOpFSM{}, ready)
			cm.Stop()
		}
	}
	if msg := recoverPanic(build(cmConfig{}, noProviderTransport{})); !strings.Contains(msg, "does not implement contract.LimitsProvider") {
		t.Fatalf("missing provider: %q", msg)
	}
	inmem := transp.NewInmemTransport("i")
	defer inmem.Close()
	upper := contract.Limits{MaxFrameBytes: 262144, MaxEntries: 6, MaxDataBytes: 41984, MaxConfigurationBytes: 2560}
	if msg := recoverPanic(build(cmConfig{limits: upper}, inmem)); !strings.Contains(msg,
		"limits mismatch: parameter MaxEntries node 6 transport 31") {
		t.Fatalf("mismatch: %q", msg)
	}
	if msg := recoverPanic(build(cmConfig{limits: contract.Limits{MaxFrameBytes: 262144}}, inmem)); !strings.Contains(msg, "invalid limits profile") {
		t.Fatalf("partial zero: %q", msg)
	}
	if msg := recoverPanic(build(cmConfig{}, inmem)); msg != "" {
		t.Fatalf("Inmem on defaults: %q", msg)
	}

	tcp, err := transp.NewTCPTransport("127.0.0.1:0", uniformTCPTimeouts(500*time.Millisecond), 2)
	if err != nil {
		t.Fatal(err)
	}
	defer tcp.Close()
	if tcp.TransportTiming().InProcess {
		t.Fatal("TCP provider reports InProcess")
	}
	msg := recoverPanic(build(cmConfig{}, tcp))
	if !strings.Contains(msg, "invalid timing profile: heartbeat 33ms + ticker 20ms + max RPC window 500ms = 553ms >= reelection base 430ms") {
		t.Fatalf("TCP 500/500 on defaults: %q", msg)
	}
	if msg = recoverPanic(build(cmConfig{timers: tcpHarnessTimers(600 * time.Millisecond)}, tcp)); msg != "" {
		t.Fatalf("TCP 500/500 at RE 600: %q", msg)
	}
}

// TestValidateConfigProfiles — V27/V29 библиотечный путь: контрпример
// connect=10/rpc=390/snapshot=390 при RE=1000 принимается (нормализованные
// таймеры 33/20/1000, не умолчания), на умолчании RE — отвергается; профиль
// 200/5/1 при TCP 200/200 — 206 >= 200 с текстом; диапазонно-невалидный
// RE=150 — прежняя ошибка ValidateTiming, не временной профиль.
func TestValidateConfigProfiles(t *testing.T) {
	_, fast := transp.NormalizeTCPTimeouts(transp.TCPTimeouts{ConnectionTimeout: 10 * time.Millisecond,
		GenericRPCTimeout: 390 * time.Millisecond, InstallSnapshotTimeout: 390 * time.Millisecond,
		ResponseTimeout: 390 * time.Millisecond})
	if err := ValidateConfig(&Config{ReelectionTimeout: time.Second}, fast); err != nil {
		t.Fatalf("10/390 at RE 1000: %v", err)
	}
	if err := ValidateConfig(&Config{}, fast); err == nil ||
		!strings.Contains(err.Error(), "33ms + ticker 20ms + max RPC window 390ms = 443ms >= reelection base 430ms") {
		t.Fatalf("10/390 at RE 430: %v", err)
	}
	_, defaults := transp.NormalizeTCPTimeouts(transp.TCPTimeouts{})
	lib := &Config{ReelectionTimeout: 200 * time.Millisecond, HeartbeatTimeout: 5 * time.Millisecond,
		TickerTimeout: time.Millisecond, ApplyBatchInterval: time.Millisecond}
	if err := ValidateConfig(lib, defaults); err == nil || !strings.Contains(err.Error(),
		"invalid timing profile: heartbeat 5ms + ticker 1ms + max RPC window 200ms = 206ms >= reelection base 200ms") {
		t.Fatalf("200/5/1: %v", err)
	}
	if err := ValidateTiming(normalizeTimerConfig(timerConfigOf(lib))); err != nil {
		t.Fatalf("200/5/1 must stay range-valid: %v", err)
	}
	err := ValidateConfig(&Config{ReelectionTimeout: 150 * time.Millisecond}, defaults)
	if err == nil || !strings.Contains(err.Error(), "reelection-timeout must be between") ||
		strings.Contains(err.Error(), "invalid timing profile") {
		t.Fatalf("RE 150: %v", err)
	}
	inproc := contract.TransportTiming{BaseSend: transp.InmemTransportTimeout, BaseRecv: transp.InmemTransportTimeout,
		InProcess: true}
	if err = ValidateConfig(&Config{}, inproc); err != nil {
		t.Fatalf("in-process on defaults: %v", err)
	}
	for name, bad := range map[string]contract.TransportTiming{
		"dial < 0":        {BaseSend: time.Second, BaseRecv: time.Second, Dial: -1},
		"dial 0 over net": {BaseSend: 100 * time.Millisecond, BaseRecv: 100 * time.Millisecond},
		"base send 0":     {BaseRecv: time.Second, Dial: time.Millisecond},
	} {
		if err = ValidateConfig(&Config{}, bad); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
	if err = ValidateConfig(&Config{Limits: contract.Limits{MaxEntries: 3}}, defaults); err == nil {
		t.Fatal("partial-zero limits accepted")
	}
}

// TestServeUsesNormalizedTimers — V27: Server.Serve проверяет временной
// профиль на фактических нормализованных таймерах (RE=1000 при окне RPC
// 390 мс — 443 < 1000 проходит) и отвергает библиотечный профиль 200/5/1
// при TCP 200/200 до запуска горутин.
func TestServeUsesNormalizedTimers(t *testing.T) {
	defer leaktest.CheckTimeout(t, 2*LeaktestBudget)()
	tcp, err := transp.NewTCPTransport("127.0.0.1:0", transp.TCPTimeouts{ConnectionTimeout: 10 * time.Millisecond,
		GenericRPCTimeout: 390 * time.Millisecond, InstallSnapshotTimeout: 390 * time.Millisecond,
		ResponseTimeout: 390 * time.Millisecond}, 2)
	if err != nil {
		t.Fatal(err)
	}
	ready := make(chan any)
	close(ready)
	s := New(&Config{ServerID: 0, Storage: store.NewMapStorage(), Fsm: NoOpFSM{}, Transport: tcp,
		ReelectionTimeout: time.Second, DisableStatsOutput: true}, ready)
	if msg := recoverPanic(s.Serve); msg != "" {
		t.Fatalf("Serve 10/390 at RE 1000: %s", msg)
	}
	s.Shutdown()

	tcp2, err := transp.NewTCPTransport("127.0.0.1:0", transp.TCPTimeouts{}, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer tcp2.Close()
	s2 := New(&Config{ServerID: 0, Storage: store.NewMapStorage(), Fsm: NoOpFSM{}, Transport: tcp2,
		ReelectionTimeout: 200 * time.Millisecond, HeartbeatTimeout: 5 * time.Millisecond,
		TickerTimeout: time.Millisecond, ApplyBatchInterval: time.Millisecond}, ready)
	if msg := recoverPanic(s2.Serve); !strings.Contains(msg, "206ms >= reelection base 200ms") {
		t.Fatalf("Serve 200/5/1: %q", msg)
	}
	if msg := recoverPanic(func() {
		New(&Config{Storage: store.NewMapStorage(), Transport: tcp2, Limits: contract.Limits{MaxDataBytes: 1}}, ready)
	}); !strings.Contains(msg, "invalid limits profile") {
		t.Fatalf("New partial-zero limits: %q", msg)
	}
}

// TestTCPClusterBatchedCatchUp — V22/V28 на реальном TCP: при MaxEntries=2
// журнал лидера длиннее N доставляется последовательными пакетами. Ведомый,
// отставший на время изоляции, догоняет хвост, когда кворум зависит только
// от него (третий узел изолирован): лидер сохраняет лидерство в том же
// терме, commitIndex ведомого достигает хвоста и не убывает.
func TestTCPClusterBatchedCatchUp(t *testing.T) {
	defer leaktest.CheckTimeout(t, 2*LeaktestBudget)()
	limits := contract.Limits{MaxFrameBytes: 262144, MaxEntries: 2, MaxDataBytes: 8192, MaxConfigurationBytes: 2560}
	const n = 3
	transports := make([]*transp.TCPTransport, n)
	for i := range transports {
		trans, err := transp.NewTCPTransportWithLimits("127.0.0.1:0", uniformTCPTimeouts(200*time.Millisecond), 2, limits)
		if err != nil {
			t.Fatal(err)
		}
		transports[i] = trans
	}
	for i := range transports {
		for j := range transports {
			if i != j {
				transports[i].Connect(ServerID(j), string(transports[j].LocalAddr()))
			}
		}
	}
	ready := make(chan any)
	cms := make([]*ConsensusModule, n)
	for i := range cms {
		var peers []int
		for j := range n {
			if j != i {
				peers = append(peers, j)
			}
		}
		cms[i] = newConsensusModule(cmConfig{limits: limits, disableStatsOutput: true}, i, peers, transports[i],
			store.NewMapStorage(), NoOpFSM{}, ready)
	}
	close(ready)
	defer func() {
		for i := range cms {
			cms[i].Stop()
			transports[i].Close()
		}
	}()

	leader := waitTCPLeader(t, cms)
	follower := (leader + 1) % n
	// Изоляция ведомого на стороне лидера и третьего узла.
	transports[leader].Disconnect(ServerID(follower))
	transports[(leader+2)%n].Disconnect(ServerID(follower))
	const commands = 9
	for i := range commands {
		if err := cms[leader].Apply(fmt.Sprintf("cmd-%d", i), time.Second).Error(); err != nil {
			t.Fatalf("Apply %d: %v", i, err)
		}
	}
	_, termBefore, _ := cms[leader].Report()
	// Возвращение ведомого и изоляция третьего узла: кворум лидера зависит
	// от догоняющего ведомого.
	third := (leader + 2) % n
	transports[leader].Disconnect(ServerID(third))
	transports[third].Disconnect(ServerID(leader))
	transports[third].Disconnect(ServerID(follower))
	transports[follower].Disconnect(ServerID(third))
	transports[leader].Connect(ServerID(follower), string(transports[follower].LocalAddr()))

	deadline := time.Now().Add(10 * time.Second)
	lastCommit := -1
	for {
		cms[leader].mu.Lock()
		tail := cms[leader].cmState.lastLogIndex
		cms[leader].mu.Unlock()
		cms[follower].mu.Lock()
		commit := cms[follower].cmState.commitIndex
		last := cms[follower].cmState.lastLogIndex
		cms[follower].mu.Unlock()
		if commit < lastCommit {
			t.Fatalf("follower commitIndex decreased %d -> %d", lastCommit, commit)
		}
		lastCommit = commit
		if commit == tail && last == tail {
			// Лидер фиксирует хвост с опорой на догнавшего ведомого.
			if _, term, isLeader := cms[leader].Report(); !isLeader || term != termBefore {
				t.Fatalf("leadership changed during catch-up: leader=%v term %d -> %d", isLeader, termBefore, term)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("follower did not catch up: commit %d last %d, leader tail %d", commit, last, tail)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitTCPLeader ждёт единственного лидера кластера.
func waitTCPLeader(t *testing.T, cms []*ConsensusModule) int {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		leader := -1
		count := 0
		for i, cm := range cms {
			if _, _, isLeader := cm.Report(); isLeader {
				leader = i
				count++
			}
		}
		if count == 1 {
			return leader
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no single leader")
	return -1
}
