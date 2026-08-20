package raft

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
)

// TestVerifyLeader_TwoNodes_FollowerDownNoQuorum — пороговый тест n=2:
// при отключённом follower'е подтверждение лидерства НЕ наступает
// (порог quorumSize(2)=2, есть только self-голос); после восстановления
// связи — наступает после первого ack'а follower'а.
//
// Старый код (порог len(peerIds)/2+1 = 1) подтверждал бы лидерство
// в одиночку — тест дискриминирует.
func TestVerifyLeader_TwoNodes_FollowerDownNoQuorum(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()
	h := NewHarness(t, 2)
	defer h.Shutdown()

	lid, _ := h.CheckSingleLeader()
	follower := (lid + 1) % 2
	h.DisconnectPeer(follower)

	future := h.cluster[lid].VerifyLeader()

	// Контрольное окно с запасом на несколько heartbeat-интервалов:
	// подтверждение не должно наступить без ack'а follower'а.
	select {
	case err := <-future.ErrorCh():
		t.Fatalf("VerifyLeader confirmed without follower ack (err=%v)", err)
	case <-time.After(HeartbeatTimeoutMs * 5 * time.Millisecond):
	}

	h.ReconnectPeer(follower)
	if err := waitFuture(t, future, 3*time.Second); err != nil {
		t.Fatalf("VerifyLeader failed after reconnect: %v", err)
	}
}

// TestVerifyLeader_FourNodes_OneFollowerNotEnough — порог 3/4:
// self + один подключённый follower (2/4) НЕ подтверждают лидерство,
// сколько бы повторных подтверждений пульса ни прислал тот же follower
// (дедупликация голосов по peer). После подключения второго follower'а
// (3/4 = кворум) подтверждение наступает.
//
// Старый код (порог len(peerIds)/2+1 = 2) подтверждал бы при self + 1
// follower — тест дискриминирует.
func TestVerifyLeader_FourNodes_OneFollowerNotEnough(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()
	h := NewHarness(t, 4)
	defer h.Shutdown()

	lid, _ := h.CheckSingleLeader()

	// Отключить двух follower'ов; один остаётся подключённым.
	down1 := (lid + 1) % 4
	down2 := (lid + 2) % 4
	h.DisconnectPeer(down1)
	h.DisconnectPeer(down2)

	future := h.cluster[lid].VerifyLeader()

	// Негативное окно с запасом на несколько heartbeat-тиков:
	// повторные ack'и одного follower'а не накапливаются.
	select {
	case err := <-future.ErrorCh():
		t.Fatalf("VerifyLeader confirmed with 2/4 votes (err=%v)", err)
	case <-time.After(HeartbeatTimeoutMs * 5 * time.Millisecond):
	}

	// Подключить второго follower'а — 3/4 = кворум.
	h.ReconnectPeer(down1)
	if err := waitFuture(t, future, 3*time.Second); err != nil {
		t.Fatalf("VerifyLeader failed with 3/4 alive: %v", err)
	}
}

// TestVerifyLeader_FourNodes_OneDownStillVerifies — позитивный контроль:
// при одном отключённом follower (живы 3/4) подтверждение наступает.
// Окно ожидания включает несколько heartbeat-интервалов
// (подтверждение может задержаться до следующего тика heartbeat из-за
// занятого inflightAE).
func TestVerifyLeader_FourNodes_OneDownStillVerifies(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()
	h := NewHarness(t, 4)
	defer h.Shutdown()

	lid, _ := h.CheckSingleLeader()

	down := (lid + 1) % 4
	h.DisconnectPeer(down)

	future := h.cluster[lid].VerifyLeader()
	if err := waitFuture(t, future, 3*time.Second); err != nil {
		t.Fatalf("VerifyLeader failed with 3/4 alive: %v", err)
	}
}

// TestVerifyLeader_NonvoterAckDoesNotVote — негатив по неголосующему:
// после DemoteVoter живы только лидер и неголосующий с непрерывными
// ack'ами — подтверждение НЕ наступает: ack неголосующего голосом не
// является, порог 2 голосов (3 voters) собирается только из голосующих.
//
// Старый код голосовал на каждый success-ответ без фильтра Suffrage —
// self + неголосующий давали бы 2 голоса и ложное подтверждение.
func TestVerifyLeader_NonvoterAckDoesNotVote(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()
	h := NewHarness(t, 4)
	defer h.Shutdown()

	lid, _ := h.CheckSingleLeader()

	// Демотировать одного follower'а в неголосующего и дождаться фиксации
	// конфигурации (дедлайн-поллинг GetConfiguration).
	demoteID := (lid + 1) % 4
	f := h.cluster[lid].DemoteVoter(ServerID(demoteID))
	if err := waitFuture(t, f, 3*time.Second); err != nil {
		t.Fatalf("DemoteVoter failed: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	demoted := false
	for time.Now().Before(deadline) && !demoted {
		cfg := h.cluster[lid].GetConfiguration().Configuration()
		for _, s := range cfg.ConfigServers {
			if s.ID == ServerID(demoteID) {
				demoted = s.Suffrage == Nonvoter
			}
		}
		if !demoted {
			// poll-интервал condition-wait (не фиксированная пауза).
			time.Sleep(10 * time.Millisecond)
		}
	}
	if !demoted {
		t.Fatalf("server %d not demoted to nonvoter in time", demoteID)
	}

	// Отключить остальных голосующих follower'ов: живы лидер + неголосующий.
	for i := 0; i < h.n; i++ {
		if i == lid || i == demoteID {
			continue
		}
		h.DisconnectPeer(i)
	}

	future := h.cluster[lid].VerifyLeader()

	// Негативное окно с запасом: ack'и неголосующего не голосуют.
	select {
	case err := <-future.ErrorCh():
		t.Fatalf("VerifyLeader confirmed by nonvoter acks (err=%v)", err)
	case <-time.After(HeartbeatTimeoutMs * 5 * time.Millisecond):
	}
}

// TestVerifyFuture_VoteDeduplication — белый короб на verifyFuture.vote:
// повторные голоса одного peerID не накапливаются.
// Прямая проверка счётчика: два голоса одного узла дают votes=1,
// голос другого узла — votes=2.
func TestVerifyFuture_VoteDeduplication(t *testing.T) {
	vf := &verifyFuture{
		voted:      make(map[int]struct{}),
		quorumSize: 2,
	}

	vf.vote(1, true)
	vf.vote(1, true) // повторное подтверждение пульса того же узла — no-op
	if vf.votes != 1 {
		t.Fatalf("votes = %d, want 1 (duplicate vote must be ignored)", vf.votes)
	}

	vf.vote(2, true)
	if vf.votes != 2 {
		t.Fatalf("votes = %d, want 2", vf.votes)
	}
}

// TestVerifyLeader_EpochFilter: голос учитывается только если AE отправлен
// не раньше постановки запроса (vf.epoch <= dispatchEpoch).
func TestVerifyLeader_EpochFilter(t *testing.T) {
	newCM := func(epoch uint64) (*ConsensusModule, *verifyFuture) {
		cm := &ConsensusModule{
			id:        0,
			transport: &mockTransportAE{},
			commitCh:  make(chan int, 1),
			leaderState: leaderState{
				nextIndex:  map[int]int{1: 0},
				matchIndex: map[int]int{1: -1},
				inflightAE: map[int]*atomic.Bool{1: new(atomic.Bool)},
			},
			cmState: cmState{
				state:        Leader,
				log:          []LogEntry{{Index: 0, Term: 1}},
				lastLogIndex: 0,
				currentTerm:  1,
				commitIndex:  -1,
				configurations: configurations{
					committed: Configuration{ConfigServers: []ConfigServer{
						{ID: 0, Suffrage: Voter},
						{ID: 1, Suffrage: Voter},
					}},
					latest: Configuration{ConfigServers: []ConfigServer{
						{ID: 0, Suffrage: Voter},
						{ID: 1, Suffrage: Voter},
					}},
				},
			},
		}
		cm.leaderState.commitmentTracker = newCommitmentTracker(cm.id, cm.commitCh, 1, 0)
		cm.leaderState.commitmentTracker.setConfiguration([]int{0, 1}, cm.lookupTerm)
		vf := &verifyFuture{
			voted:      make(map[int]struct{}),
			quorumSize: 2,
			epoch:      epoch,
		}
		cm.leaderState.pendingVerify = []*verifyFuture{vf}
		return cm, vf
	}

	// Ответ AE, отправленного ДО запроса (dispatchEpoch=1 < epoch=2):
	// голос не засчитывается.
	cm, vf := newCM(2)
	cm.leaderSendAEsToPeer(1, 0, 1)
	if vf.votes != 0 {
		t.Fatalf("votes = %d, want 0 (AE dispatched before request must not vote)", vf.votes)
	}

	// Ответ AE, отправленного ПОСЛЕ запроса (dispatchEpoch=2 == epoch=2):
	// голос засчитывается.
	cm, vf = newCM(2)
	cm.leaderSendAEsToPeer(1, 0, 2)
	if vf.votes != 1 {
		t.Fatalf("votes = %d, want 1 (AE dispatched after request must vote)", vf.votes)
	}
}
