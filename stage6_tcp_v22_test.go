package raft

import (
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/vskurikhin/raft/pkg/raft/contract"
)

// copyCMLog возвращает копию журнала узла под cm.mu.
func copyCMLog(cm *ConsensusModule) []LogEntry {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return append([]LogEntry(nil), cm.cmState.log...)
}

// cmCommitTail возвращает индекс фиксации и последний индекс журнала.
func cmCommitTail(cm *ConsensusModule) (commit, tail int) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.cmState.commitIndex, cm.cmState.lastLogIndex
}

// waitFollowerTail ждёт, пока узел зафиксирует хвост журнала лидера.
func waitFollowerTail(t *testing.T, cm *ConsensusModule, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if commit, _ := cmCommitTail(cm); commit == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	commit, tail := cmCommitTail(cm)
	t.Fatalf("ведомый не догнал: commit=%d tail=%d, хочу %d", commit, tail, want)
}

// TestTCPClusterCommandNoopConfigReplication — V22 на реальном TCP: лидер
// реплицирует команду, автоматическую noop-запись выборов и изменение
// конфигурации; все живые ведомые фиксируют хвост и повторяют журнал лидера
// по индексам, термам и типам.
func TestTCPClusterCommandNoopConfigReplication(t *testing.T) {
	defer leaktest.CheckTimeout(t, 2*LeaktestBudget)()
	h := newTCPHarness(t, 3)
	defer h.Close()

	leader := waitTCPLeader(t, h.cluster)

	// Команда: фиксация кворумом подтверждается future.
	if err := h.cluster[leader].Apply("v22-command", 3*time.Second).Error(); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	// Изменение конфигурации: добавление неполноправного узла фиксируется
	// кворумом полноправных участников и попадает в журнал типа command
	// конфигурации.
	if err := h.cluster[leader].AddNonvoter(7, ServerAddress("127.0.0.1:1")).Error(); err != nil {
		t.Fatalf("AddNonvoter: %v", err)
	}

	_, tail := cmCommitTail(h.cluster[leader])
	for i := range h.cluster {
		if i == leader {
			continue
		}
		waitFollowerTail(t, h.cluster[i], tail)
	}

	// Журналы совпадают по индексам, термам и типам, а команда, noop и
	// конфигурация присутствуют у каждого узла.
	var hasCommand, hasNoop, hasConfig bool
	leaderLog := copyCMLog(h.cluster[leader])
	for i := range h.cluster {
		log := copyCMLog(h.cluster[i])
		if len(log) != len(leaderLog) {
			t.Fatalf("узел %d: журнал длиной %d, у лидера %d", i, len(log), len(leaderLog))
		}
		for j := range log {
			if log[j].Index != leaderLog[j].Index || log[j].Term != leaderLog[j].Term || log[j].Type != leaderLog[j].Type {
				t.Fatalf("узел %d запись %d: %+v, у лидера %+v", i, j, log[j], leaderLog[j])
			}
			switch log[j].Type {
			case contract.LogCommand:
				if data, ok := log[j].Data.(string); ok && data == "v22-command" {
					hasCommand = true
				}
			case contract.LogNoop:
				hasNoop = true
			case contract.LogConfiguration:
				if _, ok := log[j].Data.([]byte); ok {
					hasConfig = true
				}
			}
		}
	}
	if !hasCommand || !hasNoop || !hasConfig {
		t.Fatalf("в журналах нет ожидаемых типов: command=%t noop=%t config=%t", hasCommand, hasNoop, hasConfig)
	}
}
