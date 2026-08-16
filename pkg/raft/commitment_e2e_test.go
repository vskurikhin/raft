package raft

import (
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
)

// waitFuture ожидает завершения future через канал ErrorCh с дедлайном.
// Единственная допустимая форма ожидания future в новых тестах (SA-014):
// у future нет собственного таймаута (future.go:98-109), поэтому голый
// вызов f.Error() при потере кворума завис бы навсегда вместо красного
// assert'а в CI.
func waitFuture(t *testing.T, f Future, timeout time.Duration) error {
	t.Helper()
	select {
	case err := <-f.ErrorCh():
		return err
	case <-time.After(timeout):
		t.Fatalf("future not resolved in %v", timeout)
		return nil
	}
}

// TestCommitmentE2E_FourNodes_TwoDownNoCommit — n=4 negative:
// при двух отключённых follower'ах (живы 2/4, кворум 3/4 недостижим)
// команда лидера НЕ коммитится и FSM её не применяет; после
// восстановления связи коммит проходит на реальном кворуме.
func TestCommitmentE2E_FourNodes_TwoDownNoCommit(t *testing.T) {
	defer leaktest.CheckTimeout(t, 200*Quantum*time.Millisecond)()
	h := NewHarness(t, 4)
	defer h.Shutdown()

	lid, _ := h.CheckSingleLeader()

	// Отключить двух follower'ов — остаются лидер + 1 follower = 2/4.
	f1 := (lid + 1) % 4
	f2 := (lid + 2) % 4
	h.DisconnectPeer(f1)
	h.DisconnectPeer(f2)

	cmd := 1001
	future := h.cluster[lid].Apply(cmd, 0)

	// Контрольное окно ~ election timeout: команда не должна
	// закоммититься при 2/4 (старый код коммитил бы на первом ack).
	select {
	case err := <-future.ErrorCh():
		t.Fatalf("command committed without quorum (err=%v)", err)
	case <-time.After(ReelectionTimeoutMs * time.Millisecond):
	}
	h.CheckNotCommitted(cmd)

	// Восстановить связь — кворум 4/4 достижим, команда коммитится.
	h.ReconnectPeer(f1)
	h.ReconnectPeer(f2)

	if err := waitFuture(t, future, 3*time.Second); err != nil {
		t.Fatalf("Apply failed after reconnect: %v", err)
	}
	h.WaitForCommit(cmd, 3)
}

// TestCommitmentE2E_FourNodes_OneDownStillCommits — n=4 positive control:
// при одном отключённом follower (живы 3/4 = кворум) коммит проходит —
// фикс не «перетянул» порог в обратную сторону.
func TestCommitmentE2E_FourNodes_OneDownStillCommits(t *testing.T) {
	defer leaktest.CheckTimeout(t, 200*Quantum*time.Millisecond)()
	h := NewHarness(t, 4)
	defer h.Shutdown()

	lid, _ := h.CheckSingleLeader()

	down := (lid + 1) % 4
	h.DisconnectPeer(down)

	cmd := 1002
	future := h.cluster[lid].Apply(cmd, 0)
	if err := waitFuture(t, future, 3*time.Second); err != nil {
		t.Fatalf("Apply failed with 3/4 alive: %v", err)
	}
	h.WaitForCommit(cmd, 3)
}

// TestCommitmentE2E_TwoNodes_FollowerDownNoCommit — n=2 regression:
// при отключённом follower (жив 1/2) команда НЕ коммитится — старый код
// коммитил её в одиночку (дефект P0-2). После восстановления — 2/2.
func TestCommitmentE2E_TwoNodes_FollowerDownNoCommit(t *testing.T) {
	defer leaktest.CheckTimeout(t, 200*Quantum*time.Millisecond)()
	h := NewHarness(t, 2)
	defer h.Shutdown()

	lid, _ := h.CheckSingleLeader()
	follower := (lid + 1) % 2
	h.DisconnectPeer(follower)

	cmd := 1003
	future := h.cluster[lid].Apply(cmd, 0)

	// Контрольное окно: при 1/2 коммит невозможен (кворум 2/2).
	select {
	case err := <-future.ErrorCh():
		t.Fatalf("command committed by isolated leader (err=%v)", err)
	case <-time.After(ReelectionTimeoutMs * time.Millisecond):
	}
	h.CheckNotCommitted(cmd)

	h.ReconnectPeer(follower)
	if err := waitFuture(t, future, 3*time.Second); err != nil {
		t.Fatalf("Apply failed after reconnect: %v", err)
	}
	h.WaitForCommit(cmd, 2)
}

// TestCommitmentE2E_NoopCommitsAfterFailover — SA-003/SA-015,
// failover-форма: отключается ЛИДЕР (недоступный узел обязан быть
// недоступен ДО избрания нового), после избрания нового лидера его
// noop-запись текущего терма должна закоммититься без единой клиентской
// команды: cmState.commitIndex >= leaderStartIndex+1 (self-match
// синхронизирован с noop — §9 п.3 ADR-005).
//
// ВНИМАНИЕ: форма «отключить follower ПОСЛЕ избрания» не дискриминирует
// (noop успевает закоммититься тремя живыми follower'ами) и запрещена.
func TestCommitmentE2E_NoopCommitsAfterFailover(t *testing.T) {
	defer leaktest.CheckTimeout(t, 200*Quantum*time.Millisecond)()
	h := NewHarness(t, 4)
	defer h.Shutdown()

	lid, _ := h.CheckSingleLeader()

	// Отключить действующего лидера до избрания нового.
	h.DisconnectPeer(lid)

	// Дождаться нового лидера среди трёх оставшихся узлов.
	deadline := time.Now().Add(5 * time.Second)
	newLeader := -1
	for time.Now().Before(deadline) && newLeader < 0 {
		for i := 0; i < h.n; i++ {
			if i == lid || !h.connected[i] {
				continue
			}
			if _, _, isLeader := h.cluster[i].Report(); isLeader {
				newLeader = i
				break
			}
		}
		if newLeader < 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if newLeader < 0 {
		t.Fatalf("no new leader elected after disconnecting leader %d", lid)
	}

	// Поллинг: noop нового терма должен закоммититься без клиентских
	// записей. Без §9 п.3 commitIndex остаётся <= leaderStartIndex
	// (дискриминирует код без self-match с noop).
	cm := h.cluster[newLeader]
	deadline = time.Now().Add(5 * time.Second)
	committed := false
	for time.Now().Before(deadline) && !committed {
		cm.mu.Lock()
		committed = cm.cmState.commitIndex >= cm.leaderState.leaderStartIndex+1
		cm.mu.Unlock()
		if !committed {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if !committed {
		cm.mu.Lock()
		ci := cm.cmState.commitIndex
		lsi := cm.leaderState.leaderStartIndex
		cm.mu.Unlock()
		t.Fatalf("noop of new leader not committed: commitIndex=%d leaderStartIndex=%d", ci, lsi)
	}
}

// TestConfiguration_AddVoterFromSingleVoter — SA-002/SA-013: bootstrap
// AddVoter из кластера с одним voter'ом. Harness неприменим (все его
// узлы — voter'ы), поэтому одноузловой кластер строится вручную.
//
// Узел B конструируется с peerIds=[0] (ОБЯЗАТЕЛЬНО непустым: с
// peerIds=[] B самоизбирается лидером терма 1 со своим noop на индексе
// 0 — две «единицы истины» одного терма; тест проходил бы лишь по
// совпадению содержимого noop). Перед AddVoter фиксируется состояние B:
// не лидер и currentTerm == 0.
//
// На коде без add-only репликации с момента append (§9 п.4) тест
// зацикливается: config entry не реплицируется на B, а его ack входит
// в кворум 2/2 — future не завершается (дедлайн обязателен).
func TestConfiguration_AddVoterFromSingleVoter(t *testing.T) {
	defer leaktest.CheckTimeout(t, 200*Quantum*time.Millisecond)()

	transportA := NewInmemTransport(ServerAddress("raft-A"))
	transportB := NewInmemTransport(ServerAddress("raft-B"))
	transportA.Connect(ServerID(1), transportB)
	transportB.Connect(ServerID(0), transportA)

	commitChA := make(chan CommitEntry, 16)
	commitChB := make(chan CommitEntry, 16)
	readyA := make(chan any)
	readyB := make(chan any)

	// Узел A: одноузловой кластер (voter только он сам).
	cmA := NewConsensusModule(0, []int{}, transportA, NewMapStorage(),
		NewCommitChannelFSM(commitChA), readyA)
	defer cmA.Stop()
	// Узел B: follower с известным соседом 0 (непустой peerIds).
	// Готовность B закрывается ПОЗЖЕ — после избрания лидера A:
	// иначе B (не получая AE, т.к. ещё не в конфигурации A) успевает
	// самоизбраться в терм 1 и assert на currentTerm == 0 флакает.
	cmB := NewConsensusModule(1, []int{0}, transportB, NewMapStorage(),
		NewCommitChannelFSM(commitChB), readyB)
	defer cmB.Stop()

	// Сначала запускается только A и дожидается лидерства.
	close(readyA)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, _, isLeader := cmA.Report(); isLeader {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, _, isLeader := cmA.Report(); !isLeader {
		t.Fatal("node A did not become leader")
	}

	// Фиксация состояния B ДО запуска его election timer: B не лидер
	// и currentTerm == 0 (узел не успевает самоизбраться).
	if _, term, isLeader := cmB.Report(); isLeader || term != 0 {
		t.Fatalf("B before ready: term=%d isLeader=%v, want term=0 isLeader=false", term, isLeader)
	}

	// Запуск B и немедленный AddVoter: AE с config entry доходят до B
	// раньше, чем истекает его election timeout, поэтому B остаётся
	// follower'ом (терм переходит в 1 от AE лидера — штатно).
	close(readyB)
	if _, term, isLeader := cmB.Report(); isLeader || term != 0 {
		t.Fatalf("B before AddVoter: term=%d isLeader=%v, want term=0 isLeader=false", term, isLeader)
	}

	// Bootstrap AddVoter: config entry реплицируется на B (add-only
	// репликация с момента append) и коммитится 2/2.
	future := cmA.AddVoter(ServerID(1), ServerAddress("raft-B"))
	if err := waitFuture(t, future, 5*time.Second); err != nil {
		t.Fatalf("AddVoter from single voter failed: %v", err)
	}

	// Конфигурация A содержит B как voter'а.
	if !hasVote(cmA.GetConfiguration().Configuration(), 1) {
		t.Fatal("node B is not a voter after AddVoter")
	}

	// Клиентская запись на A применяется на ОБОИХ узлах (B получает
	// записи после добавления).
	cmd := 2001
	future = cmA.Apply(cmd, 0)
	if err := waitFuture(t, future, 5*time.Second); err != nil {
		t.Fatalf("Apply after AddVoter failed: %v", err)
	}
	for _, ch := range []chan CommitEntry{commitChA, commitChB} {
		select {
		case entry := <-ch:
			if entry.Data.(int) != cmd {
				t.Fatalf("committed %v, want %d", entry.Data, cmd)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for committed command")
		}
	}
}

// TestConfiguration_RemovedServerSeesRemoval — SA-012 (E2E-6): после
// RemoveServer(non-leader) удалённый узел видит себя вне конфигурации
// (GetConfiguration не содержит его как voter'а). Дискриминирует
// симметричную форму §9 п.4: при останове репликации на момент append
// удаляемый сервер не получает запись о собственном удалении и
// остаётся «сиротой» (видит себя voter'ом).
func TestConfiguration_RemovedServerSeesRemoval(t *testing.T) {
	defer leaktest.CheckTimeout(t, 200*Quantum*time.Millisecond)()
	h := NewHarness(t, 3)
	defer h.Shutdown()

	lid, _ := h.CheckSingleLeader()
	victim := (lid + 1) % 3

	future := h.cluster[lid].RemoveServer(ServerID(victim))
	if err := waitFuture(t, future, 5*time.Second); err != nil {
		t.Fatalf("RemoveServer failed: %v", err)
	}

	// Поллинг на удалённом узле: запись об удалении доставляется ему
	// best-effort репликацией (несколько heartbeat-интервалов).
	deadline := time.Now().Add(3 * time.Second)
	removed := false
	for time.Now().Before(deadline) && !removed {
		cfg := h.cluster[victim].GetConfiguration().Configuration()
		removed = !hasVote(cfg, victim)
		if !removed {
			time.Sleep(10 * time.Millisecond)
		}
	}
	cfg := h.cluster[victim].GetConfiguration().Configuration()
	if hasVote(cfg, victim) {
		t.Fatalf("victim %d still sees itself as voter; cfg=%+v", victim, cfg.ConfigServers)
	}
}
