package raft

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/vskurikhin/raft/pkg/raft/store"
)

// Регрессии предварительного голосования на трёх настоящих ConsensusModule:
// действующий лидер не отдаёт PreVote, а решение таймера выборов, устаревшее
// из-за AppendEntries, смены терма или остановки, не запускает кампанию.
//
// Транспорт доставляет запросы настоящим обработчикам внутри процесса и
// возвращает их ответы без изменений. Ускорение долгой паузы — только сдвиг
// отметок времени под cm.mu; терм, голоса и журнал получены обычными
// выборами и AppendEntries.

type preVoteRegressionRecord struct {
	from, to  int
	delivered bool
	args      RequestPreVoteArgs
	reply     RequestPreVoteReply
}

type voteRegressionRecord struct {
	from, to int
	args     RequestVoteArgs
	reply    RequestVoteReply
}

type preVoteRegressionNetwork struct {
	nodes [3]*ConsensusModule
	// aeBlocked[from][to] — AppendEntries от from к to не доставляются.
	aeBlocked [3][3]atomic.Bool
	// aeInFlight — AppendEntries, прошедшие проверку доставки и ещё
	// не вернувшиеся из обработчика.
	aeInFlight atomic.Int32
	// offline[i] — узел i недоступен для любых RPC.
	offline [3]atomic.Bool
	// gate задерживает возврат уже вычисленных ответов PreVote узла 2 до
	// общего разрешения release либо разрешения по адресату releasePeer.
	gate            atomic.Bool
	arrived         chan int
	release         chan struct{}
	releaseOnce     sync.Once
	releasePeer     [3]chan struct{}
	releasePeerOnce [3]sync.Once
	// voteFailures[i] — RequestVote узла i, не доставленные из-за offline.
	voteFailures [3]atomic.Int32

	mu    sync.Mutex
	pre   []preVoteRegressionRecord
	votes []voteRegressionRecord
}

type preVoteRegressionTransport struct {
	Transport
	from int
	net  *preVoteRegressionNetwork
}

func (tr *preVoteRegressionTransport) Consumer() <-chan RPC { return nil }

func (tr *preVoteRegressionTransport) AppendEntries(
	peer ServerID, args AppendEntriesArgs,
) (AppendEntriesReply, error) {
	to := int(peer)
	tr.net.aeInFlight.Add(1)
	defer tr.net.aeInFlight.Add(-1)
	if tr.net.offline[to].Load() || tr.net.aeBlocked[tr.from][to].Load() {
		return AppendEntriesReply{}, errors.New("delivery paused")
	}
	var reply AppendEntriesReply
	err := tr.net.nodes[to].AppendEntries(args, &reply)
	return reply, err
}

func (tr *preVoteRegressionTransport) RequestPreVote(
	peer ServerID, args RequestPreVoteArgs,
) (RequestPreVoteReply, error) {
	to := int(peer)
	if tr.net.offline[to].Load() {
		tr.net.recordPreVote(preVoteRegressionRecord{from: tr.from, to: to, args: args})
		return RequestPreVoteReply{}, errors.New("peer unavailable")
	}
	var reply RequestPreVoteReply
	err := tr.net.nodes[to].RequestPreVote(args, &reply)
	tr.net.recordPreVote(preVoteRegressionRecord{from: tr.from, to: to, delivered: true, args: args, reply: reply})
	if tr.net.gate.Load() && tr.from == 2 {
		tr.net.arrived <- to
		select {
		case <-tr.net.release:
		case <-tr.net.releasePeer[to]:
		}
	}
	return reply, err
}

func (tr *preVoteRegressionTransport) RequestVote(
	peer ServerID, args RequestVoteArgs,
) (RequestVoteReply, error) {
	to := int(peer)
	if tr.net.offline[to].Load() {
		tr.net.voteFailures[tr.from].Add(1)
		return RequestVoteReply{}, errors.New("peer unavailable")
	}
	var reply RequestVoteReply
	err := tr.net.nodes[to].RequestVote(args, &reply)
	tr.net.mu.Lock()
	tr.net.votes = append(tr.net.votes, voteRegressionRecord{from: tr.from, to: to, args: args, reply: reply})
	tr.net.mu.Unlock()
	return reply, err
}

func (n *preVoteRegressionNetwork) recordPreVote(r preVoteRegressionRecord) {
	n.mu.Lock()
	n.pre = append(n.pre, r)
	n.mu.Unlock()
}

func (n *preVoteRegressionNetwork) preVotesFrom(from int) []preVoteRegressionRecord {
	n.mu.Lock()
	defer n.mu.Unlock()
	var out []preVoteRegressionRecord
	for _, r := range n.pre {
		if r.from == from {
			out = append(out, r)
		}
	}
	return out
}

func (n *preVoteRegressionNetwork) votesFrom(from int) []voteRegressionRecord {
	n.mu.Lock()
	defer n.mu.Unlock()
	var out []voteRegressionRecord
	for _, r := range n.votes {
		if r.from == from {
			out = append(out, r)
		}
	}
	return out
}

func preVoteRegressionWait(t *testing.T, label string, f func() bool) {
	t.Helper()
	limit := time.Now().Add(3 * time.Second)
	for !f() {
		if time.Now().After(limit) {
			t.Fatal(label)
		}
		time.Sleep(time.Millisecond)
	}
}

func preVoteRegressionRole(cm *ConsensusModule) (state CMState, term, votedFor int) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.cmState.state, cm.cmState.currentTerm, cm.cmState.votedFor
}

// newPreVoteRegressionNetwork собирает три голосующих узла с большим сроком
// выборов и проверки кворума: собственные таймеры не срабатывают за время
// теста, пока отметки не сдвинуты явно. Остановка всех созданных узлов
// регистрируется до первого ожидания, в том числе при частичной сборке.
func newPreVoteRegressionNetwork(t *testing.T) *preVoteRegressionNetwork {
	t.Helper()
	n := &preVoteRegressionNetwork{arrived: make(chan int, 2), release: make(chan struct{})}
	for i := range n.releasePeer {
		n.releasePeer[i] = make(chan struct{})
	}
	t.Cleanup(func() {
		n.gate.Store(false)
		n.releaseGate()
		for i := range n.releasePeer {
			n.releaseGateFor(i)
		}
		for _, c := range n.nodes {
			if c != nil {
				c.Stop()
			}
		}
	})
	// До первой выборки лидера узел 2 не получает AppendEntries от узла 0.
	n.aeBlocked[0][2].Store(true)
	ready := make(chan any)
	for i := range n.nodes {
		var peers []int
		for j := range n.nodes {
			if i != j {
				peers = append(peers, j)
			}
		}
		n.nodes[i] = newConsensusModule(
			cmConfig{disableStatsOutput: true}, i, peers,
			&preVoteRegressionTransport{from: i, net: n},
			store.NewMapStorage(), newSnapshotTestFSM(), ready,
		)
		n.nodes[i].reelectionTimeout = 5 * time.Second
		n.nodes[i].checkQuorumTimeout = 5 * time.Second
	}
	close(ready)
	// Начальный лидер выбирается настоящими RequestVote.
	n.nodes[0].mu.Lock()
	n.nodes[0].startElectionLocked()
	n.nodes[0].mu.Unlock()
	preVoteRegressionWait(t, "bootstrap leader", func() bool {
		s, _, _ := preVoteRegressionRole(n.nodes[0])
		return s == Leader
	})
	preVoteRegressionWait(t, "healthy follower replication", func() bool {
		c := n.nodes[0]
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.leaderState.matchIndex[1] >= 0
	})
	return n
}

// releaseGate снимает барьер ответов PreVote ровно один раз.
func (n *preVoteRegressionNetwork) releaseGate() {
	n.releaseOnce.Do(func() { close(n.release) })
}

// releaseGateFor снимает барьер ответов PreVote, адресованных узлу to.
func (n *preVoteRegressionNetwork) releaseGateFor(to int) {
	n.releasePeerOnce[to].Do(func() { close(n.releasePeer[to]) })
}

// catchUpFromLeader доставляет узлу to настоящий AppendEntries лидера 0 с
// его текущим журналом.
func catchUpFromLeader(t *testing.T, n *preVoteRegressionNetwork, to int) {
	t.Helper()
	c := n.nodes[0]
	c.mu.Lock()
	args := AppendEntriesArgs{
		RPCHeader:    RPCHeader{ProtocolVersion: ProtocolVersion, ServerID: 0},
		Term:         c.cmState.currentTerm,
		LeaderID:     0,
		PrevLogIndex: -1,
		PrevLogTerm:  -1,
		Entries:      append([]LogEntry(nil), c.cmState.log...),
		LeaderCommit: c.cmState.commitIndex,
	}
	c.mu.Unlock()
	var r AppendEntriesReply
	if err := n.nodes[to].AppendEntries(args, &r); err != nil || !r.Success {
		t.Fatalf("catch-up 0→%d: %+v %v", to, r, err)
	}
}

// ageNode сдвигает отметки последнего контакта узла в прошлое, имитируя долгую
// паузу. Если holdTimer, действующий таймер выборов узла останавливается без
// замены: узел не начнёт выборы сам, пока его не переведёт производственный
// переход (becomeFollowerLocked и т. п.).
func ageNode(cm *ConsensusModule, holdTimer bool) {
	old := time.Now().Add(-20 * time.Second)
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.cmState.electionResetEvent = old
	cm.cmState.leaderLastContact = old
	if holdTimer {
		close(cm.cmState.electionTimerDone)
		cm.cmState.electionTimerDone = make(chan struct{})
	}
}

// restartTimerExpired заменяет таймер узла настоящим runElectionTimer с уже
// истёкшим сроком.
func restartTimerExpired(cm *ConsensusModule) {
	ageNode(cm, true)
	cm.goSpawn(cm.runElectionTimer)
}

// isolateLeaderAppends прекращает доставку AppendEntries лидера 0 обоим
// ведомым; узел 1 при этом считается давно не получавшим пульс и не
// запускает выборы сам.
func isolateLeaderAppends(t *testing.T, n *preVoteRegressionNetwork) {
	t.Helper()
	n.aeBlocked[0][1].Store(true)
	n.aeBlocked[0][2].Store(true)
	// Доставка, начатая до блокировки, завершается до сдвига отметок.
	preVoteRegressionWait(t, "in-flight AppendEntries did not drain", func() bool {
		return n.aeInFlight.Load() == 0
	})
	ageNode(n.nodes[1], true)
}

func requireLiveLeaderQuorum(t *testing.T, leader *ConsensusModule) {
	t.Helper()
	leader.mu.Lock()
	contact := leader.leaderState.lastContact[1]
	quorum := leader.quorumContactedLocked(time.Now())
	leader.mu.Unlock()
	if !quorum || time.Since(contact) > time.Second {
		t.Fatalf("leader quorum not live: quorum=%v contactAge=%v", quorum, time.Since(contact))
	}
}

func requireRole(t *testing.T, cm *ConsensusModule, wantState CMState, wantTerm int) {
	t.Helper()
	s, term, _ := preVoteRegressionRole(cm)
	if s != wantState || term != wantTerm {
		t.Fatalf("node %d: state=%s term=%d, want %s term=%d", cm.id, s, term, wantState, wantTerm)
	}
}

// requirePreVoteReplies сверяет доставленные ответы PreVote узла 2: терм
// предложения, терм ответа и решение каждого адресата.
func requirePreVoteReplies(
	t *testing.T, recs []preVoteRegressionRecord, proposedTerm, replyTerm int, want map[int]bool,
) {
	t.Helper()
	got := map[int]bool{}
	for _, r := range recs {
		if r.args.Term != proposedTerm {
			t.Fatalf("PreVote 2→%d proposed term=%d, want %d", r.to, r.args.Term, proposedTerm)
		}
		if !r.delivered {
			continue
		}
		if r.reply.Term != replyTerm || r.reply.ServerID != r.to {
			t.Fatalf("PreVote 2→%d reply header/term: %+v, want term=%d", r.to, r.reply, replyTerm)
		}
		got[r.to] = r.reply.VoteGranted
	}
	for peer, grant := range want {
		g, ok := got[peer]
		if !ok || g != grant {
			t.Fatalf("PreVote 2→%d: delivered=%v grant=%v, want grant=%v; all=%+v", peer, ok, g, grant, recs)
		}
	}
}

func awaitGatedPreVotes(t *testing.T, n *preVoteRegressionNetwork, candidate *ConsensusModule) {
	t.Helper()
	for range 2 {
		select {
		case <-n.arrived:
		case <-time.After(3 * time.Second):
			t.Fatal("missing PreVote response")
		}
	}
	// Решение таймера и переход в PreCandidate выполнены до сетевой работы.
	if s, _, _ := preVoteRegressionRole(candidate); s != PreCandidate {
		t.Fatalf("candidate state during PreVote=%s, want PreCandidate", s)
	}
}

// TestPreVote_LiveLeaderDeniesPreVote: действующий лидер с живым кворумом
// отказывает в PreVote узлу с актуальным журналом и истёкшим таймером; свежий
// контакт ведомого защищает лидера. Выборов, смены роли и терма нет.
func TestPreVote_LiveLeaderDeniesPreVote(t *testing.T) {
	for _, agedLeaderStamp := range []bool{true, false} {
		name := "fresh_leader_stamp"
		if agedLeaderStamp {
			name = "aged_leader_stamp"
		}
		t.Run(name, func(t *testing.T) {
			t.Cleanup(leaktest.CheckTimeout(t, LeaktestBudget))
			n := newPreVoteRegressionNetwork(t)
			leader, healthy, candidate := n.nodes[0], n.nodes[1], n.nodes[2]
			if agedLeaderStamp {
				// Отметка лидера ставится при вступлении в роль и стареет при
				// долгом лидерстве; живые ответы ведомого её не обновляют.
				leader.mu.Lock()
				leader.cmState.leaderLastContact = time.Now().Add(-20 * time.Second)
				leader.mu.Unlock()
			}
			catchUpFromLeader(t, n, 2)
			requireLiveLeaderQuorum(t, leader)
			_, _, leaderVotedFor := preVoteRegressionRole(leader)

			n.gate.Store(true)
			restartTimerExpired(candidate)
			awaitGatedPreVotes(t, n, candidate)
			n.releaseGate()

			preVoteRegressionWait(t, "candidate did not return to follower", func() bool {
				s, _, _ := preVoteRegressionRole(candidate)
				return s == Follower
			})
			requirePreVoteReplies(t, n.preVotesFrom(2), 2, 1, map[int]bool{0: false, 1: false})
			if v := n.votesFrom(2); len(v) != 0 {
				t.Fatalf("unexpected RequestVote: %+v", v)
			}
			requireRole(t, leader, Leader, 1)
			requireRole(t, healthy, Follower, 1)
			requireRole(t, candidate, Follower, 1)
			if _, _, v := preVoteRegressionRole(leader); v != leaderVotedFor {
				t.Fatalf("leader votedFor changed by PreVote: %d → %d", leaderVotedFor, v)
			}
			requireLiveLeaderQuorum(t, leader)
		})
	}
}

// TestPreVote_StaleLogDenied: узел с отстающим журналом получает отказ и от
// ведомого, давно не видевшего лидера (проверка журнала), и от лидера.
func TestPreVote_StaleLogDenied(t *testing.T) {
	t.Cleanup(leaktest.CheckTimeout(t, LeaktestBudget))
	n := newPreVoteRegressionNetwork(t)
	leader, follower, candidate := n.nodes[0], n.nodes[1], n.nodes[2]
	isolateLeaderAppends(t, n)

	n.gate.Store(true)
	restartTimerExpired(candidate)
	awaitGatedPreVotes(t, n, candidate)
	n.releaseGate()

	preVoteRegressionWait(t, "candidate did not return to follower", func() bool {
		s, _, _ := preVoteRegressionRole(candidate)
		return s == Follower
	})
	recs := n.preVotesFrom(2)
	requirePreVoteReplies(t, recs, 2, 1, map[int]bool{0: false, 1: false})
	for _, r := range recs {
		if r.args.LastLogIndex != -1 {
			t.Fatalf("candidate log unexpectedly caught up: %+v", r.args)
		}
	}
	if v := n.votesFrom(2); len(v) != 0 {
		t.Fatalf("unexpected RequestVote: %+v", v)
	}
	requireRole(t, leader, Leader, 1)
	requireRole(t, follower, Follower, 1)
	requireRole(t, candidate, Follower, 1)
}

// TestPreVote_AppendEntriesCancelsComputedCampaign: ответы PreVote уже
// вычислены (грант ведомого набрал бы кворум вместе с собственным голосом),
// но ещё не разобраны; AppendEntries лидера возвращает узел в ведомые, и
// кампания отменяется без RequestVote.
func TestPreVote_AppendEntriesCancelsComputedCampaign(t *testing.T) {
	t.Cleanup(leaktest.CheckTimeout(t, LeaktestBudget))
	n := newPreVoteRegressionNetwork(t)
	leader, follower, candidate := n.nodes[0], n.nodes[1], n.nodes[2]
	catchUpFromLeader(t, n, 2)
	isolateLeaderAppends(t, n)

	n.gate.Store(true)
	restartTimerExpired(candidate)
	awaitGatedPreVotes(t, n, candidate)
	catchUpFromLeader(t, n, 2)
	requireRole(t, candidate, Follower, 1)
	n.releaseGate()

	requirePreVoteReplies(t, n.preVotesFrom(2), 2, 1, map[int]bool{0: false, 1: true})
	requireRole(t, leader, Leader, 1)
	requireRole(t, follower, Follower, 1)
	// Stop дожидается всех горутин кандидата, включая сборщик ответов:
	// после него отсутствие RequestVote окончательно.
	requireRole(t, candidate, Follower, 1)
	candidate.Stop()
	if v := n.votesFrom(2); len(v) != 0 {
		t.Fatalf("unexpected RequestVote: %+v", v)
	}
	requireRole(t, candidate, Dead, 1)
}

// TestPreVote_IsolatedLeaderDoesNotBlockMajority: лидер жив, но его
// AppendEntries не доходят до ведомых. Его отказ в PreVote не мешает
// оставшемуся большинству выбрать нового лидера; старый лидер шагает вниз
// по RequestVote большего терма.
func TestPreVote_IsolatedLeaderDoesNotBlockMajority(t *testing.T) {
	t.Cleanup(leaktest.CheckTimeout(t, LeaktestBudget))
	n := newPreVoteRegressionNetwork(t)
	leader, follower, candidate := n.nodes[0], n.nodes[1], n.nodes[2]
	catchUpFromLeader(t, n, 2)
	isolateLeaderAppends(t, n)

	restartTimerExpired(candidate)
	preVoteRegressionWait(t, "candidate did not win", func() bool {
		s, term, _ := preVoteRegressionRole(candidate)
		return s == Leader && term == 2
	})
	requirePreVoteReplies(t, n.preVotesFrom(2), 2, 1, map[int]bool{0: false, 1: true})
	requireVoteGranted(t, n, 1, 2)
	preVoteRegressionWait(t, "old leader did not step down", func() bool {
		s, term, _ := preVoteRegressionRole(leader)
		return s == Follower && term == 2
	})
	requireRole(t, follower, Follower, 2)
}

// TestPreVote_DeadLeaderMajorityElects: лидер остановлен и недоступен;
// оставшееся большинство выбирает нового лидера.
func TestPreVote_DeadLeaderMajorityElects(t *testing.T) {
	t.Cleanup(leaktest.CheckTimeout(t, LeaktestBudget))
	n := newPreVoteRegressionNetwork(t)
	leader, follower, candidate := n.nodes[0], n.nodes[1], n.nodes[2]
	catchUpFromLeader(t, n, 2)
	n.offline[0].Store(true)
	leader.Stop()
	ageNode(follower, true)

	restartTimerExpired(candidate)
	preVoteRegressionWait(t, "candidate did not win", func() bool {
		s, term, _ := preVoteRegressionRole(candidate)
		return s == Leader && term == 2
	})
	recs := n.preVotesFrom(2)
	requirePreVoteReplies(t, recs, 2, 1, map[int]bool{1: true})
	for _, r := range recs {
		if r.to == 0 && r.delivered {
			t.Fatalf("PreVote delivered to stopped leader: %+v", r)
		}
	}
	requireVoteGranted(t, n, 1, 2)
	requireRole(t, follower, Follower, 2)
}

func requireVoteGranted(t *testing.T, n *preVoteRegressionNetwork, voter, term int) {
	t.Helper()
	preVoteRegressionWait(t, "missing RequestVote reply", func() bool {
		for _, r := range n.votesFrom(2) {
			if r.to == voter {
				return true
			}
		}
		return false
	})
	for _, r := range n.votesFrom(2) {
		if r.to == voter && (r.args.Term != term || r.reply.Term != term || !r.reply.VoteGranted) {
			t.Fatalf("RequestVote 2→%d: %+v, want granted term=%d", voter, r, term)
		}
	}
}

// preVoteWorker — горутина кампании предварительного голосования, запущенная
// решением таймера выборов и задержанная барьером до решения теста.
type preVoteWorker struct {
	campaign preVoteCampaign
	// execute: true — исполнить кампанию, false — завершиться без неё.
	execute chan bool
	// done закрывается, когда горутина кампании завершилась.
	done chan struct{}
}

// installPreVoteWorkerBarrier задерживает каждую горутину кампании узла cm,
// запущенную по цепочке runElectionTimer → startCampaignLocked →
// goSpawnLocked, до явного решения теста. Барьер меняет только момент
// исполнения кампании, выданной производственным кодом. При остановке узла
// задержанная горутина исполняет кампанию сама, поэтому Stop не зависает.
func installPreVoteWorkerBarrier(cm *ConsensusModule) <-chan *preVoteWorker {
	arrived := make(chan *preVoteWorker, 4)
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.preVoteWorkerHook = func(c preVoteCampaign, run func()) {
		w := &preVoteWorker{campaign: c, execute: make(chan bool, 1), done: make(chan struct{})}
		defer close(w.done)
		select {
		case arrived <- w:
		case <-cm.shutdownCh:
			run()
			return
		}
		execute := true
		select {
		case execute = <-w.execute:
		case <-cm.shutdownCh:
		}
		if execute {
			run()
		}
	}
	return arrived
}

func awaitPreVoteWorker(t *testing.T, workers <-chan *preVoteWorker) *preVoteWorker {
	t.Helper()
	select {
	case w := <-workers:
		return w
	case <-time.After(3 * time.Second):
		t.Fatal("election timer did not launch a PreVote campaign")
		return nil
	}
}

func awaitPreVoteWorkerDone(t *testing.T, w *preVoteWorker, limit time.Duration) {
	t.Helper()
	select {
	case <-w.done:
	case <-time.After(limit):
		t.Fatalf("campaign %d did not finish", w.campaign.generation)
	}
}

func preVoteWorkerDone(w *preVoteWorker) bool {
	select {
	case <-w.done:
		return true
	default:
		return false
	}
}

// electionTimerOf возвращает канал остановки действующего таймера выборов.
func electionTimerOf(cm *ConsensusModule) chan struct{} {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.cmState.electionTimerDone
}

// requireCurrentCampaign проверяет, что узел находится в PreCandidate
// кампании w в терме term.
func requireCurrentCampaign(t *testing.T, cm *ConsensusModule, w *preVoteWorker, term int) {
	t.Helper()
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.cmState.state != PreCandidate || cm.cmState.currentTerm != term ||
		cm.cmState.preVoteGeneration != w.campaign.generation || !cm.preVoteCampaignCurrentLocked(w.campaign) {
		t.Fatalf("node %d: state=%s term=%d generation=%d, want PreCandidate term=%d of campaign %d",
			cm.id, cm.cmState.state, cm.cmState.currentTerm, cm.cmState.preVoteGeneration,
			term, w.campaign.generation)
	}
}

func requireNoVotesFrom(t *testing.T, n *preVoteRegressionNetwork, from int) {
	t.Helper()
	if v := n.votesFrom(from); len(v) != 0 {
		t.Fatalf("unexpected RequestVote: %+v", v)
	}
}

// campaignRace — узел 2 с барьером горутин кампаний в кластере с лидером 0.
type campaignRace struct {
	n         *preVoteRegressionNetwork
	candidate *ConsensusModule
	workers   <-chan *preVoteWorker
}

// newCampaignRace собирает кластер, догоняет журнал узла 2 до журнала лидера
// и ставит барьер на его кампании. Если isolate, AppendEntries лидера до
// ведомых не доходят и узел 1 давно не видел лидера: его PreVote — грант.
func newCampaignRace(t *testing.T, isolate bool) *campaignRace {
	t.Helper()
	n := newPreVoteRegressionNetwork(t)
	catchUpFromLeader(t, n, 2)
	if isolate {
		isolateLeaderAppends(t, n)
	}
	return &campaignRace{n: n, candidate: n.nodes[2], workers: installPreVoteWorkerBarrier(n.nodes[2])}
}

// startOld запускает настоящий таймер выборов узла 2 с истёкшим сроком и
// ждёт горутину его кампании: узел уже PreCandidate, запросов ещё нет.
func (r *campaignRace) startOld(t *testing.T) *preVoteWorker {
	t.Helper()
	restartTimerExpired(r.candidate)
	w := awaitPreVoteWorker(t, r.workers)
	requireCurrentCampaign(t, r.candidate, w, 1)
	return w
}

// cancelAndStartNew отменяет кампанию old настоящим AppendEntries лидера и
// дожидается новой кампании того же терма от таймера, перезапущенного этим
// AppendEntries. Сам тест таймеров не запускает: сдвигаются только отметки
// последнего контакта. Горутина новой кампании остаётся задержанной.
func (r *campaignRace) cancelAndStartNew(t *testing.T, old *preVoteWorker) *preVoteWorker {
	t.Helper()
	timerBefore := electionTimerOf(r.candidate)
	catchUpFromLeader(t, r.n, 2)
	requireRole(t, r.candidate, Follower, 1)
	if electionTimerOf(r.candidate) == timerBefore {
		t.Fatal("AppendEntries did not restart election timer")
	}
	ageNode(r.candidate, false)
	w := awaitPreVoteWorker(t, r.workers)
	if w.campaign.generation != old.campaign.generation+1 || w.campaign.proposedTerm != old.campaign.proposedTerm {
		t.Fatalf("new campaign %+v, old %+v: want next generation of the same term",
			w.campaign, old.campaign)
	}
	requireCurrentCampaign(t, r.candidate, w, 1)
	return w
}

// finishNewCampaignWins исполняет новую кампанию и проверяет её штатное
// завершение: лидер 0 отказывает, ведомый 1 даёт грант, узел 2 выигрывает
// выборы терма 2. Возвращает запросы PreVote новой кампании.
func (r *campaignRace) finishNewCampaignWins(t *testing.T, w *preVoteWorker) []preVoteRegressionRecord {
	t.Helper()
	before := len(r.n.preVotesFrom(2))
	r.n.gate.Store(false)
	w.execute <- true
	preVoteRegressionWait(t, "new campaign did not win", func() bool {
		s, term, _ := preVoteRegressionRole(r.candidate)
		return s == Leader && term == 2
	})
	awaitPreVoteWorkerDone(t, w, 3*time.Second)
	recs := r.n.preVotesFrom(2)[before:]
	if len(recs) != 2 {
		t.Fatalf("new campaign PreVote requests=%d, want 2: %+v", len(recs), recs)
	}
	requirePreVoteReplies(t, recs, 2, 1, map[int]bool{0: false, 1: true})
	requireVoteGranted(t, r.n, 1, 2)
	return recs
}

// TestPreVote_StaleTimerDecision: решение таймера уже принято и узел
// переведён в PreCandidate, но горутина кампании ещё не исполнялась.
// AppendEntries, смена терма или остановка отменяют кампанию: PreVote не
// отправляется. После AppendEntries таймер выборов остаётся рабочим, и
// последующее настоящее истечение приводит к выборам.
func TestPreVote_StaleTimerDecision(t *testing.T) {
	t.Run("append_entries", func(t *testing.T) {
		t.Cleanup(leaktest.CheckTimeout(t, LeaktestBudget))
		// Ведомый 1 дал бы грант: без проверки актуальности кампания выиграла бы.
		r := newCampaignRace(t, true)
		leader, follower, candidate := r.n.nodes[0], r.n.nodes[1], r.candidate
		old := r.startOld(t)
		timerBefore := electionTimerOf(candidate)

		catchUpFromLeader(t, r.n, 2)
		requireRole(t, candidate, Follower, 1)
		if electionTimerOf(candidate) == timerBefore {
			t.Fatal("AppendEntries did not restart election timer")
		}

		old.execute <- true
		awaitPreVoteWorkerDone(t, old, 3*time.Second)
		if recs := r.n.preVotesFrom(2); len(recs) != 0 {
			t.Fatalf("stale campaign sent PreVote: %+v", recs)
		}
		requireNoVotesFrom(t, r.n, 2)
		requireRole(t, candidate, Follower, 1)
		requireRole(t, leader, Leader, 1)

		// Настоящая потеря лидера: отметки узла 2 стареют, таймер,
		// перезапущенный AppendEntries, не трогается.
		ageNode(candidate, false)
		next := awaitPreVoteWorker(t, r.workers)
		if next.campaign.generation != old.campaign.generation+1 {
			t.Fatalf("restored timer campaign generation=%d, want %d",
				next.campaign.generation, old.campaign.generation+1)
		}
		r.finishNewCampaignWins(t, next)
		requireRole(t, follower, Follower, 2)
	})

	t.Run("term_change", func(t *testing.T) {
		t.Cleanup(leaktest.CheckTimeout(t, LeaktestBudget))
		r := newCampaignRace(t, false)
		follower, candidate := r.n.nodes[1], r.candidate
		old := r.startOld(t)

		// Настоящие выборы узла 1 поднимают терм кластера до 2.
		follower.mu.Lock()
		follower.startElectionLocked()
		follower.mu.Unlock()
		preVoteRegressionWait(t, "candidate did not observe term 2", func() bool {
			s, term, _ := preVoteRegressionRole(candidate)
			return s == Follower && term == 2
		})

		old.execute <- true
		awaitPreVoteWorkerDone(t, old, 3*time.Second)
		if recs := r.n.preVotesFrom(2); len(recs) != 0 {
			t.Fatalf("stale campaign sent PreVote: %+v", recs)
		}
		requireNoVotesFrom(t, r.n, 2)
		if _, term, _ := preVoteRegressionRole(candidate); term != 2 {
			t.Fatalf("candidate term=%d, want 2", term)
		}
	})

	t.Run("stop", func(t *testing.T) {
		t.Cleanup(leaktest.CheckTimeout(t, LeaktestBudget))
		r := newCampaignRace(t, true)
		old := r.startOld(t)

		// Stop дожидается задержанной горутины кампании: барьер отпускает
		// её исполнять кампанию уже после перехода узла в Dead.
		r.candidate.Stop()
		if !preVoteWorkerDone(old) {
			t.Fatal("Stop returned before the campaign goroutine finished")
		}
		if recs := r.n.preVotesFrom(2); len(recs) != 0 {
			t.Fatalf("stale campaign sent PreVote: %+v", recs)
		}
		requireRole(t, r.candidate, Dead, 1)
	})
}

// TestPreVote_CancelledCampaignDoesNotAffectNewCampaign: кампания узла 2
// отменена настоящим AppendEntries, после чего таймер, перезапущенный этим
// AppendEntries, начинает новую кампанию в том же терме — роль и терм
// совпадают. Любое продолжение отменённой кампании (запуск рассылки, грант,
// отказы до исчерпания, ответ с большим термом, срок сбора, завершение
// паузы после победы) не засчитывается новой кампании и не меняет роль,
// терм и таймер узла. Новая кампания затем завершается штатно.
func TestPreVote_CancelledCampaignDoesNotAffectNewCampaign(t *testing.T) {
	t.Run("delayed_launch", func(t *testing.T) {
		t.Cleanup(leaktest.CheckTimeout(t, LeaktestBudget))
		r := newCampaignRace(t, true)
		old := r.startOld(t)
		next := r.cancelAndStartNew(t, old)

		old.execute <- true
		awaitPreVoteWorkerDone(t, old, 3*time.Second)
		if recs := r.n.preVotesFrom(2); len(recs) != 0 {
			t.Fatalf("cancelled campaign sent PreVote: %+v", recs)
		}
		requireNoVotesFrom(t, r.n, 2)
		requireCurrentCampaign(t, r.candidate, next, 1)

		r.finishNewCampaignWins(t, next)
	})

	t.Run("stale_grant", func(t *testing.T) {
		t.Cleanup(leaktest.CheckTimeout(t, LeaktestBudget))
		r := newCampaignRace(t, true)
		r.n.gate.Store(true)
		old := r.startOld(t)
		old.execute <- true
		awaitGatedPreVotes(t, r.n, r.candidate)
		next := r.cancelAndStartNew(t, old)

		// Грант ведомого 1 разбирается первым: вместе с собственным голосом
		// он составил бы кворум отменённой кампании.
		r.n.releaseGateFor(1)
		awaitPreVoteWorkerDone(t, old, 3*time.Second)
		requireNoVotesFrom(t, r.n, 2)
		requireCurrentCampaign(t, r.candidate, next, 1)
		r.n.releaseGateFor(0)

		oldRecs := r.n.preVotesFrom(2)
		requirePreVoteReplies(t, oldRecs, 2, 1, map[int]bool{0: false, 1: true})
		r.finishNewCampaignWins(t, next)
	})

	t.Run("stale_denies_exhausted", func(t *testing.T) {
		t.Cleanup(leaktest.CheckTimeout(t, LeaktestBudget))
		// Ведомый 1 видит пульс лидера: обе кампании получили бы отказы.
		r := newCampaignRace(t, false)
		r.n.gate.Store(true)
		old := r.startOld(t)
		old.execute <- true
		awaitGatedPreVotes(t, r.n, r.candidate)
		next := r.cancelAndStartNew(t, old)

		// Оба отказа отменённой кампании исчерпали бы её ответы.
		r.n.releaseGateFor(0)
		r.n.releaseGateFor(1)
		awaitPreVoteWorkerDone(t, old, 3*time.Second)
		requireNoVotesFrom(t, r.n, 2)
		requireCurrentCampaign(t, r.candidate, next, 1)
		requirePreVoteReplies(t, r.n.preVotesFrom(2), 2, 1, map[int]bool{0: false, 1: false})

		// Теперь ведомый 1 теряет лидера и даёт грант новой кампании.
		isolateLeaderAppends(t, r.n)
		r.finishNewCampaignWins(t, next)
	})

	t.Run("stale_timeout", func(t *testing.T) {
		t.Cleanup(leaktest.CheckTimeout(t, LeaktestBudget))
		r := newCampaignRace(t, true)
		// Срок сбора отменённой кампании — от 1 до 2 секунд: он истекает,
		// когда новая кампания уже начата.
		r.candidate.mu.Lock()
		r.candidate.reelectionTimeout = time.Second
		r.candidate.mu.Unlock()
		r.n.gate.Store(true)
		old := r.startOld(t)
		old.execute <- true
		awaitGatedPreVotes(t, r.n, r.candidate)
		next := r.cancelAndStartNew(t, old)
		if preVoteWorkerDone(old) {
			t.Fatal("fixture: cancelled campaign finished before the new one started")
		}

		// Ответы отменённой кампании задержаны: она завершается по сроку.
		awaitPreVoteWorkerDone(t, old, 3*time.Second)
		requireNoVotesFrom(t, r.n, 2)
		requireCurrentCampaign(t, r.candidate, next, 1)
		r.n.releaseGate()

		requirePreVoteReplies(t, r.n.preVotesFrom(2), 2, 1, map[int]bool{0: false, 1: true})
		r.finishNewCampaignWins(t, next)
	})

	t.Run("stale_jitter", func(t *testing.T) {
		t.Cleanup(leaktest.CheckTimeout(t, LeaktestBudget))
		r := newCampaignRace(t, true)
		old := r.startOld(t)
		next := r.cancelAndStartNew(t, old)
		old.execute <- false
		awaitPreVoteWorkerDone(t, old, 3*time.Second)
		_, _, votedFor := preVoteRegressionRole(r.candidate)

		// Отменённая кампания выиграла предварительное голосование и
		// возвращается из случайной паузы: переход после паузы исполняется с
		// её снимком, выданным таймером. Длительность паузы случайна, поэтому
		// этот участок вызывается напрямую.
		r.candidate.startElectionAfterPreVote(old.campaign, 2)
		requireNoVotesFrom(t, r.n, 2)
		requireCurrentCampaign(t, r.candidate, next, 1)
		if _, _, v := preVoteRegressionRole(r.candidate); v != votedFor {
			t.Fatalf("votedFor changed by cancelled campaign: %d → %d", votedFor, v)
		}

		r.finishNewCampaignWins(t, next)
	})

	t.Run("stale_higher_term", func(t *testing.T) {
		t.Cleanup(leaktest.CheckTimeout(t, LeaktestBudget))
		r := newCampaignRace(t, true)
		follower := r.n.nodes[1]
		// Узел 1 переходит в терм 2 настоящими выборами, RequestVote которых
		// не доставлены: остальные узлы остаются в терме 1.
		r.n.offline[0].Store(true)
		r.n.offline[2].Store(true)
		follower.mu.Lock()
		follower.startElectionLocked()
		follower.mu.Unlock()
		preVoteRegressionWait(t, "RequestVote of node 1 not attempted", func() bool {
			return r.n.voteFailures[1].Load() == 2
		})
		r.n.offline[0].Store(false)
		r.n.offline[2].Store(false)
		requireRole(t, follower, Candidate, 2)

		r.n.gate.Store(true)
		old := r.startOld(t)
		old.execute <- true
		awaitGatedPreVotes(t, r.n, r.candidate)
		next := r.cancelAndStartNew(t, old)

		// Грант узла 1 несёт терм 2: отменённая кампания его не применяет.
		r.n.releaseGateFor(1)
		awaitPreVoteWorkerDone(t, old, 3*time.Second)
		requireCurrentCampaign(t, r.candidate, next, 1)
		r.n.releaseGateFor(0)
		oldRecs := r.n.preVotesFrom(2)
		if len(oldRecs) != 2 {
			t.Fatalf("cancelled campaign PreVote requests=%d, want 2: %+v", len(oldRecs), oldRecs)
		}
		for _, rec := range oldRecs {
			wantTerm := map[int]int{0: 1, 1: 2}[rec.to]
			if !rec.delivered || rec.args.Term != 2 || rec.reply.Term != wantTerm {
				t.Fatalf("cancelled campaign PreVote 2→%d: %+v, want reply term %d", rec.to, rec, wantTerm)
			}
		}

		// Актуальная кампания применяет больший терм по прежним правилам:
		// узел 2 возвращается в ведомые терма 2 без выборов.
		r.n.gate.Store(false)
		next.execute <- true
		awaitPreVoteWorkerDone(t, next, 3*time.Second)
		requireRole(t, r.candidate, Follower, 2)
		if _, _, v := preVoteRegressionRole(r.candidate); v != -1 {
			t.Fatalf("votedFor=%d after higher term, want -1", v)
		}
		requireNoVotesFrom(t, r.n, 2)
		if recs := r.n.preVotesFrom(2); len(recs) < 3 {
			t.Fatalf("new campaign did not send PreVote: %+v", recs)
		}
	})
}

// TestPreVote_CandidateRetryViaElectionTimer: кандидат, не набравший кворум
// (split vote), повторяет выборы через настоящий таймер выборов: решение
// таймера переводит его из Candidate в PreCandidate и далее в новые выборы.
func TestPreVote_CandidateRetryViaElectionTimer(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	cm := &ConsensusModule{
		limits:            testLimits,
		id:                0,
		transport:         &mockPreVoteGrant{peerTerm: 2},
		storage:           store.NewMapStorage(),
		shutdownCh:        make(chan struct{}),
		reelectionTimeout: 50 * time.Millisecond,
		cmState: cmState{
			state:              Candidate,
			currentTerm:        2,
			votedFor:           0,
			electionResetEvent: time.Now().Add(-time.Second),
			electionTimerDone:  make(chan struct{}),
			configurations: configurations{
				latest: Configuration{
					ConfigServers: []ConfigServer{
						{ID: 0, Suffrage: Voter},
						{ID: 1, Suffrage: Voter},
					},
				},
			},
		},
	}
	cm.cmState.log = make([]LogEntry, 0)
	cm.goSpawn(cm.runElectionTimer)
	defer func() {
		cm.mu.Lock()
		cm.cmState.state = Dead
		cm.shutdownClosed = true
		cm.mu.Unlock()
		close(cm.shutdownCh)
		cm.wg.Wait()
	}()

	preVoteRegressionWait(t, "candidate did not retry election", func() bool {
		_, term, _ := preVoteRegressionRole(cm)
		return term >= 3
	})
	s, _, votedFor := preVoteRegressionRole(cm)
	if s != Candidate && s != PreCandidate {
		t.Fatalf("state=%s after retry, want Candidate or PreCandidate", s)
	}
	if votedFor != 0 {
		t.Fatalf("votedFor=%d after retry, want self", votedFor)
	}
}
