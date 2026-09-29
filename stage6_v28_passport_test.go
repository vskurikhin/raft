package raft

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/vskurikhin/raft/pkg/raft/contract"
	"github.com/vskurikhin/raft/pkg/raft/protocol"
	"github.com/vskurikhin/raft/pkg/raft/store"
	"github.com/vskurikhin/raft/pkg/raft/transp"
)

// slowPeerTransport добавляет управляемую задержку к отправке AppendEntries
// конкретному соседу: это детерминированный медленный канал ведомого.
// Обёртка без длительных горутин — задержка перед делегированием вызова.
type slowPeerTransport struct {
	*transp.InmemTransport
	slowID ServerID
	delay  time.Duration

	// delayed считает фактически применённые задержки: доказывает
	// управляемость канала в сценарии, а не только его настройку.
	delayed atomic.Int64
}

// AppendEntries откладывает обращение к медленному соседу на delay.
func (s *slowPeerTransport) AppendEntries(id ServerID, args AppendEntriesArgs) (AppendEntriesReply, error) {
	if id == s.slowID {
		s.delayed.Add(1)
		time.Sleep(s.delay)
	}
	return s.InmemTransport.AppendEntries(id, args)
}

// v28Cluster — кластер на Inmem с одним медленным ведомым.
type v28Cluster struct {
	t          *testing.T
	n          int
	cms        []*ConsensusModule
	transports []*transp.InmemTransport
	slows      []*slowPeerTransport
	alive      []bool
}

// newV28Cluster собирает 3 узла; обращения к узлу slow задерживаются на
// delay на стороне отправителя.
func newV28Cluster(t *testing.T, slow int, delay time.Duration) *v28Cluster {
	t.Helper()
	const n = 3
	transports := make([]*transp.InmemTransport, n)
	for i := range n {
		transports[i] = transp.NewInmemTransport(ServerAddress(fmt.Sprintf("v28-%d", i)))
	}
	for i := range n {
		for j := range n {
			if i != j {
				transports[i].Connect(ServerID(j), transports[j])
			}
		}
	}
	ready := make(chan any)
	cluster := &v28Cluster{t: t, n: n, transports: transports, slows: make([]*slowPeerTransport, n), alive: make([]bool, n)}
	for i := range n {
		var peers []int
		for j := range n {
			if j != i {
				peers = append(peers, j)
			}
		}
		slowT := &slowPeerTransport{InmemTransport: transports[i], slowID: ServerID(slow), delay: delay}
		cluster.slows[i] = slowT
		transport := Transport(slowT)
		cluster.cms = append(cluster.cms, newConsensusModule(
			cmConfig{disableStatsOutput: true}, i, peers, transport, store.NewMapStorage(), NoOpFSM{}, ready))
		cluster.alive[i] = true
	}
	close(ready)
	return cluster
}

// Close останавливает узлы и закрывает транспорты.
func (c *v28Cluster) Close() {
	for i := range c.n {
		if c.alive[i] {
			c.cms[i].Stop()
			c.transports[i].Close()
			c.alive[i] = false
		}
	}
}

// waitLeader ждёт единственного лидера.
func (c *v28Cluster) waitLeader() int {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		leader, count := -1, 0
		for i := range c.n {
			if _, _, isLeader := c.cms[i].Report(); isLeader {
				leader, count = i, count+1
			}
		}
		if count == 1 {
			return leader
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatal("нет единственного лидера")
	return -1
}

// isolate разрывает связь узла с остальными.
func (c *v28Cluster) isolate(id int) {
	for j := range c.n {
		if j != id {
			c.transports[id].Disconnect(ServerID(j))
			c.transports[j].Disconnect(ServerID(id))
		}
	}
}

// TestV28SlowChannelQuorumByCatchUpPeer — V28: детерминированный медленный
// канал ведомого, от которого зависит кворум. Лидер сохраняет лидерство и
// терм (нет PreVote и перевыборов), а фиксация продолжается, пока медленный
// ведомый отвечает в пределах окна.
func TestV28SlowChannelQuorumByCatchUpPeer(t *testing.T) {
	defer leaktest.CheckTimeout(t, 3*LeaktestBudget)()
	const slow = 1
	c := newV28Cluster(t, slow, 100*time.Millisecond)
	defer c.Close()
	leader := c.waitLeader()
	if leader == slow {
		// Медленным обязан быть ведомый, а не лидер: передаём лидерство
		// соседу и ждём нового лидера.
		target := (leader + 1) % c.n
		if err := c.cms[leader].LeadershipTransfer(ServerID(target)).Error(); err != nil {
			t.Fatalf("передача лидерства с медленного узла: %v", err)
		}
		leader = c.waitLeader()
		if leader == slow {
			t.Fatalf("медленный узел снова стал лидером")
		}
	}
	third := 3 - leader - slow

	if err := c.cms[leader].Apply("before-slow", 2*time.Second).Error(); err != nil {
		t.Fatalf("начальная фиксация: %v", err)
	}
	// Кворум зависит только от медленного ведомого.
	c.isolate(third)
	_, termBefore, _ := c.cms[leader].Report()

	for i := range 5 {
		if err := c.cms[leader].Apply(fmt.Sprintf("slow-%d", i), 3*time.Second).Error(); err != nil {
			t.Fatalf("фиксация через медленного ведомого %d: %v", i, err)
		}
	}
	// Управляемость канала доказана: задержка фактически применялась.
	var delayed int64
	for _, s := range c.slows {
		delayed += s.delayed.Load()
	}
	if delayed == 0 {
		t.Fatalf("медленный канал не применялся: задержек 0")
	}
	t.Logf("применено задержек медленного канала: %d", delayed)
	// Лидер не должен ни шагнуть вниз, ни сменить терм: PreVote не запускался.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, term, isLeader := c.cms[leader].Report(); !isLeader || term != termBefore {
			t.Fatalf("лидер потерял роль: isLeader=%t term=%d (был %d)", isLeader, term, termBefore)
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.cms[slow].mu.Lock()
	commit := c.cms[slow].cmState.commitIndex
	c.cms[slow].mu.Unlock()
	if commit <= 0 {
		t.Fatalf("медленный ведомый не зафиксировал ни одной записи: commit=%d", commit)
	}
}

// TestV28OfflinePassportAB — V28: офлайн-проверка паспорта на контрпримерах
// A и B через экспортированный protocol.CheckTimingPassport. Профиль B
// отвергается по I1 (малая полоса при малом окне), профиль A — по I3
// (долгий Dial при узком CheckQuorum). Это проверка формы A/B, а не
// G-PASSPORT владельца.
func TestV28OfflinePassportAB(t *testing.T) {
	timer := protocol.TimerProfile{Heartbeat: 33 * time.Millisecond, Tick: 20 * time.Millisecond,
		ReelectionMin: 2 * time.Second}

	// Профиль B: узкое окно и низкая полоса — t(F) больше T_min(F).
	profileB := protocol.TimingPassport{DialMax: time.Millisecond, MinBitsPerSecond: 20_000_000}
	err := protocol.CheckTimingPassport(protocol.DefaultLimits(),
		contract.TransportTiming{BaseSend: time.Millisecond, BaseRecv: time.Millisecond, Dial: time.Millisecond},
		timer, 400*time.Millisecond, profileB)
	if !errors.Is(err, protocol.ErrLimit) || !strings.Contains(err.Error(), "I1") {
		t.Fatalf("профиль B: err=%v", err)
	}

	// Профиль A: долгий Dial при узком CheckQuorum — нарушено I3.
	profileA := protocol.TimingPassport{DialMax: 200 * time.Millisecond, MinBitsPerSecond: 1_000_000_000}
	err = protocol.CheckTimingPassport(protocol.DefaultLimits(),
		contract.TransportTiming{BaseSend: 300 * time.Millisecond, BaseRecv: 300 * time.Millisecond, Dial: 200 * time.Millisecond},
		timer, 250*time.Millisecond, profileA)
	if !errors.Is(err, protocol.ErrLimit) || !strings.Contains(err.Error(), "I3") {
		t.Fatalf("профиль A: err=%v", err)
	}
}
