package raft

import (
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
)

// TestSendBatch_SkipsMissingPrefix проверяет, что sendBatch не добавляет
// в батч запись, чей Index не совпадает с запрошенным idx (FINDING-005):
// для индексов ниже первого индекса компактированного журнала в батч
// раньше подставлялась запись log[0].
func TestSendBatch_SkipsMissingPrefix(t *testing.T) {
	defer leaktest.CheckTimeout(t, time.Second)()

	cm := &ConsensusModule{}
	cm.cmState.log = []LogEntry{{Index: 5, Term: 1, Type: LogCommand, Data: "k=v"}}
	cm.cmState.lastLogIndex = 5
	cm.cmState.lastLogTerm = 1
	cm.leaderState.inflight = make(map[int]*logFuture)
	cm.fsmMutateCh = make(chan []*commitTuple, 10)

	// Индексы 0..4 отсутствуют в журнале: батч должен быть пустым.
	cm.sendBatch(0, 4)
	select {
	case batch := <-cm.fsmMutateCh:
		t.Fatalf("sendBatch(0, 4) sent batch of %d entries, want none", len(batch))
	default:
	}

	// Индекс 5 существует: батч содержит ровно одну запись с Index == 5.
	cm.sendBatch(5, 5)
	select {
	case batch := <-cm.fsmMutateCh:
		if len(batch) != 1 {
			t.Fatalf("sendBatch(5, 5) batch = %d entries, want 1", len(batch))
		}
		if batch[0].log.Index != 5 {
			t.Fatalf("sendBatch(5, 5) entry Index = %d, want 5", batch[0].log.Index)
		}
	default:
		t.Fatal("sendBatch(5, 5) sent no batch, want 1 entry")
	}
}
