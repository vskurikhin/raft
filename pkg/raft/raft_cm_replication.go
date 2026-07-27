package raft

import (
	"time"
)

// peerReplication — контекст долгоживущей горутины репликации
// для одного удалённого peer.
//
// Создаётся при старте лидерства (startLeader) и при добавлении
// нового сервера в конфигурацию (startStopReplication).
// Живёт, пока peer в конфигурации и сервер является лидером.
//
// Триггер-канал (triggerCh) ёмкостью 1 обеспечивает coalescing:
// множественные уведомления о новых записях схлопываются в один
// сигнал. Это предотвращает burst репликаций при высокой частоте
// dispatchLogs.
//
// Канал stopCh закрывается при потере лидерства или удалении peer.
// Горутина завершает цикл и закрывает doneCh.
//
// Инвариант:
//
//	peerReplications[peerID] != nil  — peer активен
//	peerReplications[peerID] == nil  — peer не в конфигурации
type peerReplication struct {
	peerID int

	// triggerCh — сигнал о новых данных для репликации.
	// Ёмкость 1: множественные уведомления coalescятся
	// в один сигнал. Неблокирующая отправка (select + default).
	triggerCh chan struct{}

	// stopCh закрывается при выходе из лидерства или при удалении peer.
	stopCh chan struct{}

	// doneCh закрывается после завершения replicateLoop.
	doneCh chan struct{}
}

func (cm *ConsensusModule) nextIndexArgsEntries(peerID, savedCurrentTerm int) (int, AppendEntriesArgs, []LogEntry) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	ni := cm.nextIndex[peerID]
	prevLogIndex := ni - 1
	prevLogTerm := -1
	if 0 <= prevLogIndex && prevLogIndex <= cm.lastLogIndex {
		prevLogTerm = cm.lookupTerm(prevLogIndex)
	}
	var entries []LogEntry
	if ni <= cm.lastLogIndex {
		pos := cm.logPosition(ni)
		entries = append([]LogEntry{}, cm.log[pos:]...)
	} else {
		entries = nil
	}
	return ni, AppendEntriesArgs{
		RPCHeader: RPCHeader{
			ProtocolVersion: ProtocolVersion,
			ServerID:        cm.id,
		},
		Term:         savedCurrentTerm,
		LeaderID:     cm.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: cm.commitIndex,
	}, entries
}

// leaderSendAEsToPeer отправляет AppendEntries указанному peer и
// обрабатывает ответ. Вызывается из replicateLoop, выполняется
// в контексте per-peer долгоживущей горутины.
func (cm *ConsensusModule) leaderSendAEsToPeer(peerID, savedCurrentTerm int) {
	cm.mu.Lock()
	ni := cm.nextIndex[peerID]
	if ni <= cm.lastSnapshotIndex && cm.snapshotStore != nil {
		cm.mu.Unlock()
		cm.leaderSendSnapshot(peerID, savedCurrentTerm)
		return
	}
	cm.mu.Unlock()

	ni, args, entries := cm.nextIndexArgsEntries(peerID, savedCurrentTerm)
	cm.dLogf("sending AppendEntries to %v: ni=%d, args=%+v", peerID, ni, args)
	reply, err := cm.transport.AppendEntries(ServerID(peerID), args)
	if err == nil {
		cm.mu.Lock()
		if reply.Term > cm.currentTerm {
			cm.dLogf("term out of date in heartbeat reply")
			cm.becomeFollower(reply.Term)
			cm.mu.Unlock()
			return
		}
		if cm.nextIndex[peerID] != ni {
			cm.dLogf("#44 ni out of date in heartbeat reply")
			cm.mu.Unlock()
			return
		}

		if cm.state == Leader && savedCurrentTerm == reply.Term {
			if reply.Success {
				cm.nextIndex[peerID] = ni + len(entries)
				cm.matchIndex[peerID] = cm.nextIndex[peerID] - 1

				cm.commitmentTracker.setMatch(peerID, cm.matchIndex[peerID], cm.lookupTerm)
				cm.dLogf(
					"AppendEntries reply from %d success: nextIndex := %v, matchIndex := %v; commitIndex := %d",
					peerID, cm.nextIndex, cm.matchIndex, cm.commitmentTracker.getCommitIndex(),
				)
				cm.persistToStorage()

				if len(cm.pendingVerify) > 0 && cm.state == Leader {
					for _, vf := range cm.pendingVerify {
						vf.vote(true)
					}
					var remaining []*verifyFuture
					for _, vf := range cm.pendingVerify {
						if vf.votes >= vf.quorumSize {
							vf.respond(nil)
						} else {
							remaining = append(remaining, vf)
						}
					}
					cm.pendingVerify = remaining
				}

				cm.mu.Unlock()
			} else {
				if reply.ConflictTerm >= 0 {
					if lastIndex, ok := cm.termIndexMap[reply.ConflictTerm]; ok {
						cm.nextIndex[peerID] = lastIndex + 1
					} else {
						cm.nextIndex[peerID] = reply.ConflictIndex
					}
				} else {
					cm.nextIndex[peerID] = reply.ConflictIndex
				}
				cm.dLogf("AppendEntries reply from %d !success: nextIndex := %d", peerID, ni-1)
				cm.mu.Unlock()
			}
		} else {
			cm.mu.Unlock()
		}
	}
}

// triggerReplication уведомляет per-peer горутину репликации
// о наличии новых записей для отправки.
//
// Неблокирующая: если канал triggerCh уже заполнен (горутина
// ещё не обработала предыдущий сигнал), текущий вызов
// coalescится — горутина получит все новые записи при
// следующем чтении nextIndex.
func (cm *ConsensusModule) triggerReplication(peerID int) {
	cm.mu.Lock()
	pr := cm.peerReplications[peerID]
	cm.mu.Unlock()

	if pr == nil {
		return
	}

	select {
	case pr.triggerCh <- struct{}{}:
	default:
	}
}

// triggerAllPeers уведомляет все per-peer горутины репликации
// о необходимости отправки AppendEntries. Вызывается из
// runLeaderLoop после dispatchLogs, commitCh, heartbeatTicker
// и при pendingVerify.
//
// Каждая горутина получает не более одного уведомления
// благодаря coalescing-каналу ёмкостью 1.
func (cm *ConsensusModule) triggerAllPeers() {
	cm.mu.Lock()
	if cm.state != Leader {
		cm.mu.Unlock()
		return
	}
	peerIDs := make([]int, 0, len(cm.peerReplications))
	for peerID := range cm.peerReplications {
		peerIDs = append(peerIDs, peerID)
	}
	cm.mu.Unlock()

	for _, peerID := range peerIDs {
		cm.triggerReplication(peerID)
	}
}

// replicateLoop — долгоживущий цикл репликации для одного peer.
//
// Запускается в отдельной горутине. Ожидает сигнал на triggerCh
// (новые записи в журнале) или heartbeatTicker (плановый heartbeat),
// затем вызывает leaderSendAEsToPeer для синхронной отправки
// AppendEntries.
//
// Выходит при закрытии stopCh или при обнаружении, что сервер
// больше не лидер (cm.state != Leader). В обоих случаях закрывает
// doneCh для синхронизации завершения.
func (cm *ConsensusModule) replicateLoop(pr *peerReplication) {
	defer close(pr.doneCh)

	heartbeatTicker := time.NewTicker(HeartbeatTimeoutMs * time.Millisecond)
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-pr.stopCh:
			return

		case <-pr.triggerCh:

		case <-heartbeatTicker.C:
		}

		cm.mu.Lock()
		if cm.state != Leader {
			cm.mu.Unlock()
			return
		}
		savedCurrentTerm := cm.currentTerm
		cm.mu.Unlock()

		cm.leaderSendAEsToPeer(pr.peerID, savedCurrentTerm)
	}
}

// leaderSendSnapshot отправляет последний снэпшот отстающему follower.
// Вызывается, когда nextIndex[follower] <= lastSnapshotIndex.
func (cm *ConsensusModule) leaderSendSnapshot(peerID int, term int) {
	snapshots, err := cm.snapshotStore.List()
	if err != nil || len(snapshots) == 0 {
		return
	}
	meta, reader, err := cm.snapshotStore.Open(snapshots[0].ID)
	if err != nil {
		return
	}
	defer reader.Close()

	cfgData, _ := EncodeConfiguration(meta.Configuration)

	req := InstallSnapshotRequest{
		RPCHeader: RPCHeader{
			ProtocolVersion: ProtocolVersion,
			ServerID:        cm.id,
		},
		Term:          term,
		LeaderID:      cm.id,
		LastLogIndex:  meta.Index,
		LastLogTerm:   meta.Term,
		Configuration: cfgData,
		ConfigIndex:   meta.ConfigIndex,
		DataSize:      meta.Size,
	}

	reply, err := cm.transport.InstallSnapshot(ServerID(peerID), req, reader)
	if err != nil {
		return
	}
	if reply.Term > term {
		cm.becomeFollower(reply.Term)
		return
	}
	if reply.Success {
		cm.mu.Lock()
		cm.nextIndex[peerID] = meta.Index + 1
		cm.matchIndex[peerID] = meta.Index
		cm.persistToStorage()
		cm.mu.Unlock()
	}
}
