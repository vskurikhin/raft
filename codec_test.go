package raft

import (
	"bytes"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
)

// TestStage4AppendEntriesRetryNoReapply — матрица «Повтор AE»: настоящий
// AppendEntries с текущим термом после штатных выборов, а затем повтор с теми
// же bytes после перезапуска ведомого не применяют запись к машине состояний
// повторно: зафиксированная запись испускается в канал фиксации ровно один
// раз на воплощение узла, совпадающий повтор не порождает записей журнала.
func TestStage4AppendEntriesRetryNoReapply(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	leader, term := h.CheckSingleLeader()
	index := h.SubmitToServer(leader, 42)
	h.WaitForCommitAll(42, _commitBudgetSteady)

	follower := (leader + 1) % 3
	if got := h.GetTerm(follower); got != term {
		t.Fatalf("follower term = %d, want %d", got, term)
	}
	entries, err := h.storage[follower].LoadLog()
	if err != nil {
		t.Fatalf("LoadLog: %v", err)
	}
	var prevTerm int
	var entryData []byte
	for _, e := range entries {
		switch e.Index {
		case index:
			entryData = e.Data
		case index - 1:
			prevTerm = e.Term
		}
	}
	if entryData == nil {
		t.Fatalf("follower log has no committed entry at index %d", index)
	}

	countCommits := func() int {
		h.mu.Lock()
		defer h.mu.Unlock()
		n := 0
		for _, c := range h.commits[follower] {
			if c.Index == index {
				n++
			}
		}
		return n
	}
	if err := waitCond("committed entry present once on the follower", _commitBudgetSteady,
		func() bool { return countCommits() == 1 }, h.commitsDiag); err != nil {
		t.Fatal(err)
	}

	// Повтор того же AppendEntries живому ведомому: те же bytes, текущий терм.
	duplicate := func() {
		args := AppendEntriesArgs{
			RPCHeader:    RPCHeader{ProtocolVersion: ProtocolVersion, ServerID: leader},
			Term:         term,
			LeaderID:     leader,
			PrevLogIndex: index - 1,
			PrevLogTerm:  prevTerm,
			Entries: []LogEntry{{
				Index: index,
				Term:  term,
				Type:  LogCommand,
				Data:  bytes.Clone(entryData),
			}},
			LeaderCommit: index,
		}
		var reply AppendEntriesReply
		if err := h.cluster[follower].AppendEntries(args, &reply); err != nil {
			t.Fatalf("duplicate AppendEntries: %v", err)
		}
		if !reply.Success {
			t.Fatalf("duplicate AppendEntries: Success=false, reply=%+v", reply)
		}
	}
	logCallsBefore := totalLogCalls(persistenceSnapshotOf(h.cluster[follower]))
	duplicate()
	if after := totalLogCalls(persistenceSnapshotOf(h.cluster[follower])); after != logCallsBefore {
		t.Fatalf("duplicate AppendEntries wrote to the journal: %d -> %d", logCallsBefore, after)
	}
	assertCommitCountStable(t, countCommits, 1)

	// Перезапуск ведомого: лидер повторяет тот же суффикс восстановленными
	// bytes; ведомый догоняет, и запись применяется ровно один раз за новое
	// воплощение.
	h.CrashPeer(follower)
	h.RestartPeer(follower)
	h.WaitForCommitConvergedValues(42, 3)
	if got := countCommits(); got != 1 {
		t.Fatalf("commits of index %d after restart = %d, want 1", index, got)
	}
	duplicate()
	assertCommitCountStable(t, countCommits, 1)
}

// assertCommitCountStable требует, чтобы число фиксаций оставалось равным
// want на протяжении окна стабильности: повторное применение записи
// появилось бы в канале фиксации немедленно.
func assertCommitCountStable(t *testing.T, count func() int, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * DefaultApplyBatchInterval)
	for time.Now().Before(deadline) {
		if got := count(); got != want {
			t.Fatalf("commit count = %d, want stable %d", got, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
