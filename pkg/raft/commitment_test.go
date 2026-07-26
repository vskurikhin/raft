package raft

import (
	"testing"
	"time"
)

// lookupTermStub возвращает фиксированный term для заданного индекса.
// Если index < 0, возвращает -1 (начальное состояние).
type lookupTermStub struct {
	terms map[int]int
}

func (l *lookupTermStub) lookup(index int) int {
	if index < 0 {
		return -1
	}
	if t, ok := l.terms[index]; ok {
		return t
	}
	return -1
}

func newTracker(t testing.TB, selfID int, peerIDs []int, term int) (*commitmentTracker, chan int) {
	t.Helper()
	commitCh := make(chan int, 1)
	return newCommitmentTracker(selfID, peerIDs, commitCh, term), commitCh
}

// --- 2.1. Базовая majority (3 узла) ---

func TestCommitment_NoMatch(t *testing.T) {
	ct, _ := newTracker(t, 0, []int{1, 2}, 1)
	if got := ct.getCommitIndex(); got != -1 {
		t.Fatalf("getCommitIndex() = %d, want -1", got)
	}
}

func TestCommitment_SelfOnly(t *testing.T) {
	ct, _ := newTracker(t, 0, []int{1, 2}, 1)
	lts := &lookupTermStub{terms: map[int]int{5: 1}}
	ct.setMatch(0, 5, lts.lookup)
	if got := ct.getCommitIndex(); got != -1 {
		t.Fatalf("getCommitIndex() = %d, want -1", got)
	}
}

func TestCommitment_SelfAndOnePeer(t *testing.T) {
	ct, _ := newTracker(t, 0, []int{1, 2}, 1)
	lts := &lookupTermStub{terms: map[int]int{5: 1}}
	ct.setMatch(0, 5, lts.lookup)
	ct.setMatch(1, 5, lts.lookup)
	if got := ct.getCommitIndex(); got != 5 {
		t.Fatalf("getCommitIndex() = %d, want 5", got)
	}
}

func TestCommitment_AllNodes(t *testing.T) {
	ct, _ := newTracker(t, 0, []int{1, 2}, 1)
	lts := &lookupTermStub{terms: map[int]int{10: 1}}
	ct.setMatch(0, 10, lts.lookup)
	ct.setMatch(1, 10, lts.lookup)
	ct.setMatch(2, 10, lts.lookup)
	if got := ct.getCommitIndex(); got != 10 {
		t.Fatalf("getCommitIndex() = %d, want 10", got)
	}
}

func TestCommitment_MedianOrdering_Odd(t *testing.T) {
	ct, _ := newTracker(t, 0, []int{1, 2, 3, 4}, 1)
	lts := &lookupTermStub{terms: map[int]int{-1: 1, 1: 1, 2: 1, 3: 1, 4: 1, 5: 1}}
	ct.setMatch(0, 5, lts.lookup)
	ct.setMatch(1, 1, lts.lookup)
	ct.setMatch(2, 2, lts.lookup)
	ct.setMatch(3, 3, lts.lookup)
	ct.setMatch(4, 4, lts.lookup)
	// sorted: [-1, 1, 2, 3, 4, 5] -> len=6, n/2=3 -> values[3] = 3
	// Wait, we have 5 nodes, so 5 matchIndex values: [1, 2, 3, 4, 5]
	// sorted: [1, 2, 3, 4, 5], len=5, n/2=2 -> values[2] = 3
	if got := ct.getCommitIndex(); got != 3 {
		t.Fatalf("getCommitIndex() = %d, want 3", got)
	}
}

func TestCommitment_MedianOrdering_Even(t *testing.T) {
	ct, _ := newTracker(t, 0, []int{1, 2}, 1)
	lts := &lookupTermStub{terms: map[int]int{-1: 0, 1: 0, 2: 1, 3: 1}}
	// 3 nodes: matchIndex[self=0]=3, peer1=1, peer2=2
	ct.setMatch(0, 3, lts.lookup)
	ct.setMatch(1, 1, lts.lookup)
	ct.setMatch(2, 2, lts.lookup)
	// sorted: [1, 2, 3], len=3, n/2=1 -> values[1] = 2
	if got := ct.getCommitIndex(); got != 2 {
		t.Fatalf("getCommitIndex() = %d, want 2", got)
	}
}

// --- 2.3. Raft safety (проверка term) ---

func TestCommitment_Safety_DifferentTerm(t *testing.T) {
	ct, _ := newTracker(t, 0, []int{1, 2}, 2)
	lts := &lookupTermStub{terms: map[int]int{5: 1}}
	ct.setMatch(0, 5, lts.lookup)
	ct.setMatch(1, 5, lts.lookup)
	ct.setMatch(2, 5, lts.lookup)
	// median=5, но lookupTerm(5)=1 != ct.term=2, поэтому commitIndex не меняется
	if got := ct.getCommitIndex(); got != -1 {
		t.Fatalf("getCommitIndex() = %d, want -1", got)
	}
}

func TestCommitment_Safety_SameTerm(t *testing.T) {
	ct, _ := newTracker(t, 0, []int{1, 2}, 2)
	lts := &lookupTermStub{terms: map[int]int{5: 2}}
	ct.setMatch(0, 5, lts.lookup)
	ct.setMatch(1, 5, lts.lookup)
	ct.setMatch(2, 5, lts.lookup)
	if got := ct.getCommitIndex(); got != 5 {
		t.Fatalf("getCommitIndex() = %d, want 5", got)
	}
}

func TestCommitment_Safety_MedianShifts(t *testing.T) {
	ct, _ := newTracker(t, 0, []int{1, 2, 3}, 2)
	lts := &lookupTermStub{terms: map[int]int{3: 1, 5: 2}}
	// 4 peers + self = 5 nodes? Actually self=0, peers=[1,2,3] = 4 nodes total
	// Let me be more explicit: 4 nodes, so matchIndex has 4 entries
	// matchIndexes = {0:-1, 1:-1, 2:-1, 3:-1}

	// mode: put most nodes at index 3 (term=1, not current term),
	// one at index 5 (term=2, current term)
	ct.setMatch(0, 5, lts.lookup)
	ct.setMatch(1, 3, lts.lookup)
	ct.setMatch(2, 3, lts.lookup)
	// matchIndexes = {0:5, 1:3, 2:3, 3:-1}
	// sorted = [-1, 3, 3, 5], len=4, n/2=2, median = values[2] = 3
	// lookupTerm(3)=1 != term=2, so no commit
	if got := ct.getCommitIndex(); got != -1 {
		t.Fatalf("first: getCommitIndex() = %d, want -1 (median 3 has term 1)", got)
	}

	// Now peer 3 also gets index 5
	ct.setMatch(3, 5, lts.lookup)
	// matchIndexes = {0:5, 1:3, 2:3, 3:5}
	// sorted = [3, 3, 5, 5], len=4, n/2=2, median = values[2] = 5
	// lookupTerm(5)=2 == term=2 ✅
	if got := ct.getCommitIndex(); got != 5 {
		t.Fatalf("second: getCommitIndex() = %d, want 5", got)
	}

	// median now jumped from 3 (rejected) to 5 (accepted)
}

// --- 2.4. Продвижение commitIndex (incremental) ---

func TestCommitment_Progressive(t *testing.T) {
	ct, _ := newTracker(t, 0, []int{1, 2}, 1)
	lts := &lookupTermStub{terms: map[int]int{3: 1, 5: 1}}

	ct.setMatch(0, 3, lts.lookup)
	ct.setMatch(1, 3, lts.lookup)
	// matchIndexes = {0:3, 1:3, 2:-1} -> sorted [-1, 3, 3], median=3
	if got := ct.getCommitIndex(); got != 3 {
		t.Fatalf("after first two: getCommitIndex() = %d, want 3", got)
	}

	ct.setMatch(2, 5, lts.lookup)
	// matchIndexes = {0:3, 1:3, 2:5} -> sorted [3, 3, 5], median=3
	// 3 == ct.commitIndex, поэтому не продвигается
	if got := ct.getCommitIndex(); got != 3 {
		t.Fatalf("after third: getCommitIndex() = %d, want 3", got)
	}
}

func TestCommitment_Progressive_LargeSteps(t *testing.T) {
	ct, _ := newTracker(t, 0, []int{1, 2}, 1)
	lts := &lookupTermStub{terms: map[int]int{100: 1, 150: 1, 200: 1}}

	ct.setMatch(1, 100, lts.lookup)
	// matchIndexes = {0:-1, 1:100, 2:-1} -> sorted [-1, -1, 100], median=-1
	if got := ct.getCommitIndex(); got != -1 {
		t.Fatalf("after peer1: getCommitIndex() = %d, want -1", got)
	}

	ct.setMatch(2, 200, lts.lookup)
	// matchIndexes = {0:-1, 1:100, 2:200} -> sorted [-1, 100, 200], median=100
	// lookupTerm(100)=1 == term=1 ✅
	if got := ct.getCommitIndex(); got != 100 {
		t.Fatalf("after peer2: getCommitIndex() = %d, want 100", got)
	}

	ct.setMatch(0, 150, lts.lookup)
	// matchIndexes = {0:150, 1:100, 2:200} -> sorted [100, 150, 200], median=150
	// lookupTerm(150)=1 == term=1 ✅
	if got := ct.getCommitIndex(); got != 150 {
		t.Fatalf("after self: getCommitIndex() = %d, want 150", got)
	}
}

func TestCommitment_SelfCommit(t *testing.T) {
	ct, _ := newTracker(t, 0, []int{1, 2}, 1)
	lts := &lookupTermStub{terms: map[int]int{10: 1}}

	ct.commit(10, lts.lookup)
	// matchIndex[self=0] = 10, но peer1, peer2 = -1
	// sorted [-1, -1, 10], median=-1
	if got := ct.getCommitIndex(); got != -1 {
		t.Fatalf("getCommitIndex() = %d, want -1 (no majority)", got)
	}
}

func TestCommitment_SelfCommit_SingleNode(t *testing.T) {
	ct, _ := newTracker(t, 0, []int{}, 1)
	lts := &lookupTermStub{terms: map[int]int{10: 1}}

	ct.commit(10, lts.lookup)
	// single node: matchIndex = {0:10}, sorted [10], median=10
	if got := ct.getCommitIndex(); got != 10 {
		t.Fatalf("getCommitIndex() = %d, want 10", got)
	}
}

// --- 2.5. Граничные случаи setMatch ---

func TestCommitment_SetMatchDecreasingIgnored(t *testing.T) {
	ct, _ := newTracker(t, 0, []int{1, 2}, 1)
	lts := &lookupTermStub{terms: map[int]int{10: 1, 5: 1}}

	ct.setMatch(1, 10, lts.lookup)
	ct.setMatch(1, 5, lts.lookup)
	// matchIndex[1] должен остаться 10 (инвариант: не уменьшается)
	// Проверяем внутреннее состояние
	if got := ct.matchIndex[1]; got != 10 {
		t.Fatalf("matchIndex[1] = %d, want 10", got)
	}
}

func TestCommitment_SetMatchUnknownPeer(t *testing.T) {
	ct, _ := newTracker(t, 0, []int{1, 2}, 1)
	lts := &lookupTermStub{terms: map[int]int{5: 1}}

	// setMatch для неизвестного peer-а — не должен паниковать
	ct.setMatch(99, 5, lts.lookup)
	// matchIndex[99] не существует, проверяем что tracker не сломан
	if got := ct.getCommitIndex(); got != -1 {
		t.Fatalf("getCommitIndex() = %d, want -1", got)
	}
}

func TestCommitment_NegativeMatchIndex(t *testing.T) {
	ct, _ := newTracker(t, 0, []int{1, 2}, 1)
	// После конструктора все matchIndex = -1
	if got := ct.getCommitIndex(); got != -1 {
		t.Fatalf("initial getCommitIndex() = %d, want -1", got)
	}
}

func TestCommitment_EmptyPeerList(t *testing.T) {
	ct, _ := newTracker(t, 0, []int{}, 1)
	lts := &lookupTermStub{terms: map[int]int{5: 1, -1: 0}}

	// Пустой список peer-ов, self тоже -1
	if got := ct.getCommitIndex(); got != -1 {
		t.Fatalf("initial getCommitIndex() = %d, want -1", got)
	}

	// После commit(5) -> self matchIndex = 5, median = 5
	ct.commit(5, lts.lookup)
	if got := ct.getCommitIndex(); got != 5 {
		t.Fatalf("after commit: getCommitIndex() = %d, want 5", got)
	}
}

// --- 2.6. commitCh non-blocking send ---

func TestCommitment_ChannelNonBlocking(t *testing.T) {
	ct, _ := newTracker(t, 0, []int{1, 2}, 1)
	lts := &lookupTermStub{terms: map[int]int{5: 1}}

	// Не читаем из commitCh. setMatch не должен блокироваться.
	ct.setMatch(0, 5, lts.lookup)
	ct.setMatch(1, 5, lts.lookup)
	// Если бы send блокировался, тест бы завис.
	if got := ct.getCommitIndex(); got != 5 {
		t.Fatalf("getCommitIndex() = %d, want 5", got)
	}
}

func TestCommitment_ChannelDelivery(t *testing.T) {
	ct, commitCh := newTracker(t, 0, []int{1, 2}, 1)
	lts := &lookupTermStub{terms: map[int]int{5: 1}}

	ct.setMatch(0, 5, lts.lookup)
	ct.setMatch(1, 5, lts.lookup)

	select {
	case got := <-commitCh:
		if got != 5 {
			t.Fatalf("commitCh = %d, want 5", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for commitCh")
	}
}

// --- 2.7. setTerm ---

func TestCommitment_SetTerm(t *testing.T) {
	ct, _ := newTracker(t, 0, []int{1, 2}, 1)
	lts := &lookupTermStub{terms: map[int]int{5: 1, 6: 2}}

	// term=1, median=5, lookupTerm(5)=1 == term ✅
	ct.setMatch(0, 5, lts.lookup)
	ct.setMatch(1, 5, lts.lookup)
	ct.setMatch(2, 5, lts.lookup)
	if got := ct.getCommitIndex(); got != 5 {
		t.Fatalf("after first majority: getCommitIndex() = %d, want 5", got)
	}

	// Меняем term на 2
	ct.setTerm(2)

	// median=5, но lookupTerm(5)=1 != 2, поэтому не коммитится
	// Проверяем что commitIndex не уменьшился
	if got := ct.getCommitIndex(); got != 5 {
		t.Fatalf("after setTerm: getCommitIndex() = %d, want 5 (unchanged)", got)
	}

	// Новая setMatch с median=6, lookupTerm(6)=2 == term ✅
	ct.setMatch(0, 6, lts.lookup)
	ct.setMatch(1, 6, lts.lookup)
	ct.setMatch(2, 6, lts.lookup)
	// sorted: [6, 6, 6], median=6
	if got := ct.getCommitIndex(); got != 6 {
		t.Fatalf("after new majority: getCommitIndex() = %d, want 6", got)
	}
}

// --- 2.8. Конструктор ---

func TestCommitment_NewTracker(t *testing.T) {
	commitCh := make(chan int, 1)
	ct := newCommitmentTracker(0, []int{1, 2}, commitCh, 1)

	if ct.selfID != 0 {
		t.Fatalf("selfID = %d, want 0", ct.selfID)
	}
	if ct.term != 1 {
		t.Fatalf("term = %d, want 1", ct.term)
	}
	if ct.getCommitIndex() != -1 {
		t.Fatalf("commitIndex = %d, want -1", ct.getCommitIndex())
	}
	if ct.matchIndex[0] != -1 {
		t.Fatalf("matchIndex[0] = %d, want -1", ct.matchIndex[0])
	}
	if ct.matchIndex[1] != -1 {
		t.Fatalf("matchIndex[1] = %d, want -1", ct.matchIndex[1])
	}
	if ct.matchIndex[2] != -1 {
		t.Fatalf("matchIndex[2] = %d, want -1", ct.matchIndex[2])
	}
}

// --- 2.9. Multiple setMatch with recalculate ---

func TestCommitment_MultipleSetMatchNoCommit(t *testing.T) {
	ct, _ := newTracker(t, 0, []int{1, 2}, 1)
	lts := &lookupTermStub{terms: map[int]int{5: 1, 6: 1, 7: 1}}

	ct.setMatch(1, 5, lts.lookup)
	ct.setMatch(2, 6, lts.lookup)
	// matchIndexes = {0:-1, 1:5, 2:6} -> sorted [-1, 5, 6], median=5
	if got := ct.getCommitIndex(); got != 5 {
		t.Fatalf("getCommitIndex() = %d, want 5", got)
	}

	ct.setMatch(0, 7, lts.lookup)
	// matchIndexes = {0:7, 1:5, 2:6} -> sorted [5, 6, 7], median=6
	if got := ct.getCommitIndex(); got != 6 {
		t.Fatalf("getCommitIndex() = %d, want 6", got)
	}
}

// --- Дополнительные тесты ---

func TestCommitment_CommitThenSetMatch(t *testing.T) {
	ct, _ := newTracker(t, 0, []int{1, 2}, 1)
	lts := &lookupTermStub{terms: map[int]int{5: 1, 8: 1}}

	// Сначала self.commit(5), потом setMatch для peer-ов
	ct.commit(5, lts.lookup)
	// matchIndexes = {0:5, 1:-1, 2:-1} -> sorted [-1, -1, 5], median=-1
	if got := ct.getCommitIndex(); got != -1 {
		t.Fatalf("after commit only: getCommitIndex() = %d, want -1", got)
	}

	ct.setMatch(1, 8, lts.lookup)
	// matchIndexes = {0:5, 1:8, 2:-1} -> sorted [-1, 5, 8], median=5
	if got := ct.getCommitIndex(); got != 5 {
		t.Fatalf("after peer1: getCommitIndex() = %d, want 5", got)
	}

	ct.setMatch(2, 8, lts.lookup)
	// matchIndexes = {0:5, 1:8, 2:8} -> sorted [5, 8, 8], median=8
	if got := ct.getCommitIndex(); got != 8 {
		t.Fatalf("after peer2: getCommitIndex() = %d, want 8", got)
	}
}

func TestCommitment_FiveNodes(t *testing.T) {
	ct, _ := newTracker(t, 0, []int{1, 2, 3, 4}, 1)
	lts := &lookupTermStub{terms: map[int]int{10: 1, -1: 0}}

	// self + 1 peer = 2/5 — кворума нет
	ct.setMatch(0, 10, lts.lookup)
	ct.setMatch(1, 10, lts.lookup)
	// sorted [-1, -1, -1, 10, 10], median=-1
	if got := ct.getCommitIndex(); got != -1 {
		t.Fatalf("2/5: getCommitIndex() = %d, want -1", got)
	}

	// self + 2 peers = 3/5 — кворум есть
	ct.setMatch(2, 10, lts.lookup)
	// sorted [-1, -1, 10, 10, 10], median=10
	if got := ct.getCommitIndex(); got != 10 {
		t.Fatalf("3/5: getCommitIndex() = %d, want 10", got)
	}
}

func TestCommitment_commitChBufferSize1PreventsLoss(t *testing.T) {
	ct, commitCh := newTracker(t, 0, []int{1, 2}, 1)
	lts := &lookupTermStub{terms: map[int]int{5: 1, 10: 1}}

	// Первый commit — отправляет в commitCh
	ct.setMatch(0, 5, lts.lookup)
	ct.setMatch(1, 5, lts.lookup)
	ct.setMatch(2, 5, lts.lookup)

	// Читаем первое сообщение
	select {
	case <-commitCh:
	case <-time.After(time.Second):
		t.Fatal("timeout reading first commitCh message")
	}

	// Второй commit — должен отправить новое сообщение (буфер 1)
	ct.setMatch(0, 10, lts.lookup)
	ct.setMatch(1, 10, lts.lookup)
	ct.setMatch(2, 10, lts.lookup)

	select {
	case got := <-commitCh:
		if got != 10 {
			t.Fatalf("commitCh = %d, want 10", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout reading second commitCh message")
	}
}

func TestCommitment_CommitIndexNeverDecreases(t *testing.T) {
	ct, _ := newTracker(t, 0, []int{1, 2}, 2)
	lts := &lookupTermStub{terms: map[int]int{5: 2, 8: 1}}

	// term=2, median=5, lookupTerm(5)=2 ✅ → commitIndex=5
	ct.setMatch(0, 5, lts.lookup)
	ct.setMatch(1, 5, lts.lookup)
	ct.setMatch(2, 5, lts.lookup)
	if got := ct.getCommitIndex(); got != 5 {
		t.Fatalf("first: getCommitIndex() = %d, want 5", got)
	}

	// median=8, но lookupTerm(8)=1 != 2 ❌ → commitIndex не меняется
	ct.setMatch(0, 8, lts.lookup)
	ct.setMatch(1, 8, lts.lookup)
	ct.setMatch(2, 8, lts.lookup)
	if got := ct.getCommitIndex(); got != 5 {
		t.Fatalf("second: getCommitIndex() = %d, want 5 (no regression)", got)
	}
}

func TestCommitment_RecalculateWithNegativeMatch(t *testing.T) {
	ct, _ := newTracker(t, 0, []int{1, 2}, 1)
	lts := &lookupTermStub{terms: map[int]int{-1: 0, 3: 1}}

	// Все matchIndex = -1 (начальное состояние)
	ct.setMatch(0, 3, lts.lookup)
	// matchIndexes = {0:3, 1:-1, 2:-1} -> sorted [-1, -1, 3], median=-1
	if got := ct.getCommitIndex(); got != -1 {
		t.Fatalf("getCommitIndex() = %d, want -1", got)
	}

	// Два peer с 3 -> кворум
	ct.setMatch(1, 3, lts.lookup)
	// matchIndexes = {0:3, 1:3, 2:-1} -> sorted [-1, 3, 3], median=3
	if got := ct.getCommitIndex(); got != 3 {
		t.Fatalf("getCommitIndex() = %d, want 3", got)
	}
}

func TestCommitment_EmptyMatchMap(t *testing.T) {
	// Трекер с пустым matchIndex — теоретически невозможно через конструктор,
	// но проверим защиту в recalculateCommitIndex
	ct := &commitmentTracker{
		matchIndex:  map[int]int{},
		commitIndex: -1,
		selfID:      0,
		peerIDs:     []int{},
		commitCh:    make(chan int, 1),
		term:        1,
	}
	// Не должен паниковать
	lts := &lookupTermStub{terms: map[int]int{}}
	ct.recalculateCommitIndex(lts.lookup)
	if got := ct.getCommitIndex(); got != -1 {
		t.Fatalf("getCommitIndex() = %d, want -1", got)
	}
}
