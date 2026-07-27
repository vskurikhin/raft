package raft

import (
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
)

// --- Pre-Vote: защита от лишних выборов при сетевых сбоях (§4) ---
//
// Pre-Vote предотвращает нестабильность кластера, вызванную отключёнными
// узлами, которые инициируют выборы с более высоким term после
// переподключения. Перед увеличением currentTerm кандидат отправляет
// RequestPreVote всем voter'ам и ждёт кворум. Только после этого
// запускаются настоящие выборы.

// TestPreVote_DisconnectedFollower_NoElection проверяет, что отключённый
// follower НЕ инициирует выборы и term кластера остаётся стабильным.
//
// Сценарий:
//  1. Кластер из 3 узлов, лидер выбран
//  2. Отключаем один follower от всех
//  3. Ждём несколько election timeout — отключённый узел пытается PreVote,
//     но получает отказ (лидер известен, log-safety не проходит)
//  4. Проверяем: лидер остался тем же, term не изменился
//
// Это ключевое преимущество Pre-Vote перед классическим Raft: в классическом
// Raft отключённый follower увеличил бы term при каждой попытке выборов.
func TestPreVote_DisconnectedFollower_NoElection(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*Quantum*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	lid, origTerm := h.CheckSingleLeader()

	// Отключаем один follower — он перестаёт получать heartbeat от лидера.
	otherID := (lid + 1) % 3
	h.DisconnectPeer(otherID)

	// Ждём несколько election timeout, чтобы отключённый узел успел
	// несколько раз запустить PreVote.
	time.Sleep(time.Duration(ReelectionTimeoutMs) * time.Millisecond * 3)

	// Лидер должен остаться тем же, term не должен вырасти — PreVote
	// предотвращает выборы отключённым узлом.
	newLid, newTerm := h.CheckSingleLeader()
	if newLid != lid {
		t.Errorf("leader changed from %d to %d, want same", lid, newLid)
	}
	if newTerm != origTerm {
		t.Errorf("term changed from %d to %d, want same", origTerm, newTerm)
	}
}

// TestPreVote_CrashedLeader_NewElection проверяет, что при краше лидера
// PreVote НЕ блокирует выборы нового лидера.
//
// Сценарий:
//  1. Кластер из 3 узлов, лидер выбран
//  2. Крашим лидера (CrashPeer) — он полностью недоступен
//  3. Два оставшихся узла не получают heartbeat
//  4. Один из них проходит PreVote (leaderLastContact устарел, log-safety OK)
//     и запускает настоящие выборы
//  5. Проверяем: выбран новый лидер (не crashed)
func TestPreVote_CrashedLeader_NewElection(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*Quantum*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	lid, _ := h.CheckSingleLeader()

	// Крашим лидера — теперь два follower'а должны выбрать нового.
	h.CrashPeer(lid)
	h.CheckNoLeader()

	// Ждём, чтобы PreVote + настоящие выборы завершились.
	time.Sleep(time.Duration(ReelectionTimeoutMs) * time.Millisecond * 2)

	// Должен быть выбран новый лидер (не crashed).
	newLid, _ := h.CheckSingleLeader()
	if newLid == lid {
		t.Errorf("same leader after crash")
	}
}

// TestPreVote_MajorityPartition проверяет, что при разделении сети
// (majority partition) лидер остается в большинстве, а minority не
// может инициировать выборы.
//
// Сценарий:
//  1. Кластер из 3 узлов, лидер выбран
//  2. Отключаем один follower (minority partition — {лидер, follower} | {follower})
//  3. Majority ({лидер, follower}) продолжает работу — лидер остаётся
//  4. Minority (отключённый узел) пытается PreVote, но получает отказ
//     от majority (leader known)
//  5. Проверяем: лидер из majority, term не изменился
func TestPreVote_MajorityPartition(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*Quantum*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	lid, origTerm := h.CheckSingleLeader()

	// Отключаем один follower — он оказывается в minority partition.
	otherID := (lid + 1) % 3
	h.DisconnectPeer(otherID)

	// Ждём несколько election timeout, чтобы minority успел попытаться
	// несколько раз запустить PreVote.
	time.Sleep(time.Duration(ReelectionTimeoutMs) * time.Millisecond * 4)

	// Лидер должен остаться в majority (лидер или второй follower).
	newLid, newTerm := h.CheckSingleLeader()
	if newLid == otherID {
		t.Errorf("minority node became leader, want majority")
	}
	majorityHasVote := (newLid == lid || newLid == (lid+2)%3)
	if !majorityHasVote {
		t.Errorf("leader %d not in majority", newLid)
	}
	// Term не должен вырасти — PreVote предотвращает выборы minority.
	if newTerm != origTerm {
		t.Errorf("term changed from %d to %d, want same", origTerm, newTerm)
	}
}

// TestPreVote_ReconnectStable проверяет, что при циклическом
// отключении/подключении follower'а лидер и term остаются стабильными.
//
// Сценарий:
//  1. Кластер из 3 узлов, лидер выбран
//  2. Циклически (5 раз): отключаем follower, ждём, подключаем обратно
//  3. После каждого цикла PreVote предотвращает выборы отключённым узлом
//  4. После всех циклов проверяем: term не изменился
//
// Это стресс-тест стабильности Pre-Vote при повторяющихся сетевых сбоях.
func TestPreVote_ReconnectStable(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*Quantum*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	lid, origTerm := h.CheckSingleLeader()

	otherID := (lid + 1) % 3
	for range 5 {
		h.DisconnectPeer(otherID)

		h.CheckSingleLeader()

		h.ReconnectPeer(otherID)

		h.CheckSingleLeader()
	}

	_, newTerm := h.CheckSingleLeader()
	if newTerm != origTerm {
		t.Errorf("term changed from %d to %d after reconnect cycles", origTerm, newTerm)
	}
}

// TestPreVote_Disabled проверяет, что при preVoteDisabled = true поведение
// соответствует классическому Raft: отключённый узел увеличивает term.
//
// Сценарий:
//  1. Кластер из 3 узлов с отключённым PreVote, лидер выбран
//  2. Отключаем один follower
//  3. Отключённый узел переходит в Candidate (без PreVote) и увеличивает term
//  4. После переподключения follower'а обнаруживается его более высокий term,
//     что приводит к stepDown текущего лидера и новым выборам
//  5. Проверяем: term вырос (выборы состоялись)
func TestPreVote_Disabled(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*Quantum*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	// Отключаем Pre-Vote на всех узлах сразу после создания Harness,
	// но до того, как election timer успеет сработать (первый tick через
	// TickerTimeoutMs = 20ms).
	for _, cm := range h.cluster {
		cm.preVoteDisabled = true
	}

	lid, origTerm := h.CheckSingleLeader()

	// Отключаем один follower — в классическом Raft он начнёт выборы
	// и увеличит term при каждом election timeout.
	otherID := (lid + 1) % 3
	h.DisconnectPeer(otherID)

	// Ждём несколько election timeout, чтобы отключённый узел успел
	// несколько раз увеличить term.
	time.Sleep(time.Duration(ReelectionTimeoutMs) * time.Millisecond * 3)

	// В классическом Raft отключённый узел увеличивает term.
	// Проверяем, что term отключённого узла вырос относительно origTerm.
	disconnectedTerm := h.GetTerm(otherID)
	if disconnectedTerm <= origTerm {
		t.Errorf("disconnected node term = %d, want > %d", disconnectedTerm, origTerm)
	}
}
