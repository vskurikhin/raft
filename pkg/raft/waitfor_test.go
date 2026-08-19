package raft

import (
	"strings"
	"testing"
	"time"
)

// newSyntheticHarness создаёт Harness без кластера: заполнены только
// поля, читаемые предикатами ожидания (commits, connected, n, t).
// Используется для юнит-тестов waitFor/committedOn, где нужен полный
// контроль над содержимым h.commits.
func newSyntheticHarness(t *testing.T, n int) *Harness {
	t.Helper()
	h := &Harness{
		cluster:       make([]*ConsensusModule, n),
		commits:       make([][]CommitEntry, n),
		connected:     make([]bool, n),
		alive:         make([]bool, n),
		collectorDone: make([]chan struct{}, n),
		n:             n,
		t:             t,
	}
	for i := 0; i < n; i++ {
		h.connected[i] = true
		h.alive[i] = true
	}
	return h
}

// TestWaitFor_ConditionAlreadyTrue: условие истинно сразу — waitFor
// возвращается немедленно и без ошибки.
func TestWaitFor_ConditionAlreadyTrue(t *testing.T) {
	h := newSyntheticHarness(t, 3)

	start := time.Now()
	err := h.waitFor("always true", time.Second, func() bool { return true }, nil)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("waitFor returned %v, want nil", err)
	}
	if elapsed > pollInterval {
		t.Fatalf("waitFor took %v, want immediate return (<= %v)", elapsed, pollInterval)
	}
}

// TestWaitFor_ConditionBecomesTrue: условие становится истинным
// позже — waitFor дожидается его в пределах бюджета.
func TestWaitFor_ConditionBecomesTrue(t *testing.T) {
	h := newSyntheticHarness(t, 3)

	calls := 0
	err := h.waitFor("true on 3rd poll", time.Second, func() bool {
		calls++
		return calls >= 3
	}, nil)

	if err != nil {
		t.Fatalf("waitFor returned %v, want nil", err)
	}
	if calls < 3 {
		t.Fatalf("cond evaluated %d times, want >= 3", calls)
	}
}

// TestWaitFor_TimeoutDiagnostics: условие никогда не истинно — waitFor
// исчерпывает бюджет и возвращает структурированную диагностику
// (§21 architecture.md: ожидание, бюджет, достигнутое состояние).
func TestWaitFor_TimeoutDiagnostics(t *testing.T) {
	h := newSyntheticHarness(t, 3)
	h.commits[0] = []CommitEntry{{Data: 41, Index: 1, Term: 1}}

	budget := 60 * time.Millisecond
	start := time.Now()
	err := h.waitFor("cmd=42 committed", budget, func() bool { return false }, h.commitsDiag)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("waitFor returned nil, want timeout error")
	}
	if elapsed < budget {
		t.Fatalf("waitFor returned after %v, want >= budget %v", elapsed, budget)
	}
	msg := err.Error()
	for _, want := range []string{"budget", "expected: cmd=42 committed", "commits:", "len[0]=1", "state:"} {
		if !strings.Contains(msg, want) {
			t.Errorf("diagnostics %q does not contain %q", msg, want)
		}
	}
}

// TestCommittedOn_NotWeakerThanAssert — регрессионный тест SA-001/RC-1:
// предикат ожидания не слабее assert'а CheckCommitted. Узел, «ушедший
// вперёд» по commits, обязан удерживать converged == false, а wait —
// не возвращаться преждевременно.
func TestCommittedOn_NotWeakerThanAssert(t *testing.T) {
	h := newSyntheticHarness(t, 3)

	// Узел 0 ушёл вперёд: команда 42 есть у всех, но длины не равны —
	// предусловие CheckCommitted (равные длины) не выполнено.
	h.commits[0] = []CommitEntry{{Data: 42, Index: 1, Term: 1}, {Data: 43, Index: 2, Term: 1}}
	h.commits[1] = []CommitEntry{{Data: 42, Index: 1, Term: 1}}
	h.commits[2] = []CommitEntry{{Data: 42, Index: 1, Term: 1}}

	h.mu.Lock()
	nc, converged := h.committedOn(42)
	h.mu.Unlock()
	if nc != 3 {
		t.Fatalf("committedOn nc = %d, want 3", nc)
	}
	if converged {
		t.Fatal("committedOn converged = true while commits lengths differ; predicate is weaker than CheckCommitted")
	}

	// Прежний WaitForCommit вернулся бы здесь немедленно (nc >= n);
	// новый предикат обязан исчерпать бюджет.
	budget := 60 * time.Millisecond
	start := time.Now()
	err := h.waitFor("cmd=42 converged", budget, func() bool {
		n, ok := h.committedOn(42)
		return n >= 3 && ok
	}, h.commitsDiag)
	if err == nil {
		t.Fatal("waitFor returned early: predicate is weaker than assert")
	}
	if elapsed := time.Since(start); elapsed < budget {
		t.Fatalf("waitFor returned after %v, want >= budget %v", elapsed, budget)
	}

	// После сходимости узлов предикат обязан стать истинным.
	h.mu.Lock()
	h.commits[1] = append(h.commits[1], CommitEntry{Data: 43, Index: 2, Term: 1})
	h.commits[2] = append(h.commits[2], CommitEntry{Data: 43, Index: 2, Term: 1})
	nc, converged = h.committedOn(42)
	h.mu.Unlock()
	if nc != 3 || !converged {
		t.Fatalf("committedOn = (%d, %t), want (3, true) after convergence", nc, converged)
	}
}

// TestCommittedOn_IndexMismatch: равные длины, но разный Index записи —
// предусловие CheckCommitted (равный Index) не выполнено.
func TestCommittedOn_IndexMismatch(t *testing.T) {
	h := newSyntheticHarness(t, 3)
	h.commits[0] = []CommitEntry{{Data: 42, Index: 1, Term: 1}}
	h.commits[1] = []CommitEntry{{Data: 42, Index: 2, Term: 1}}
	h.commits[2] = []CommitEntry{{Data: 42, Index: 1, Term: 1}}

	h.mu.Lock()
	nc, converged := h.committedOn(42)
	h.mu.Unlock()

	if nc != 3 {
		t.Fatalf("committedOn nc = %d, want 3", nc)
	}
	if converged {
		t.Fatal("committedOn converged = true while Index differs")
	}
}

// TestCommittedOn_IgnoresDisconnected: отключённые узлы не участвуют
// ни в подсчёте nc, ни в проверке сходимости.
func TestCommittedOn_IgnoresDisconnected(t *testing.T) {
	h := newSyntheticHarness(t, 3)
	h.commits[0] = []CommitEntry{{Data: 42, Index: 1, Term: 1}}
	h.commits[1] = []CommitEntry{{Data: 42, Index: 1, Term: 1}}
	// Узел 2 отстал и отключён — на сходимость влиять не должен.
	h.commits[2] = nil
	h.connected[2] = false

	h.mu.Lock()
	nc, converged := h.committedOn(42)
	h.mu.Unlock()

	if nc != 2 || !converged {
		t.Fatalf("committedOn = (%d, %t), want (2, true)", nc, converged)
	}
}

// TestWaitForApplyInflight: примитив не-commit ожидания дожидается
// появления inflight-future на лидере после Apply.
func TestWaitForApplyInflight(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	lid, _ := h.CheckSingleLeader()

	// Изолируем лидера — Apply останется в inflight (закоммитить
	// его без кворума невозможно).
	h.DisconnectPeer((lid + 1) % 3)
	h.DisconnectPeer((lid + 2) % 3)

	inflightBefore := h.applyInflightCount(lid)
	future := h.cluster[lid].Apply(42, 0)
	h.waitForApplyInflight(lid, inflightBefore+1, commitBudgetSteady)

	// Возвращаем связность, чтобы future разрешился и тест не оставил
	// висящих ожиданий.
	h.ReconnectPeer((lid + 1) % 3)
	h.ReconnectPeer((lid + 2) % 3)
	_ = future.Error()
}
