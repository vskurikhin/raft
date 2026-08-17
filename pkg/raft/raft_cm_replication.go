package raft

import "time"

func (cm *ConsensusModule) nextIndexArgsEntries(peerID, savedCurrentTerm int) (int, AppendEntriesArgs, []LogEntry) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	ni := cm.leaderState.nextIndex[peerID]
	prevLogIndex := ni - 1
	prevLogTerm := -1
	if 0 <= prevLogIndex && prevLogIndex <= cm.cmState.lastLogIndex {
		prevLogTerm = cm.lookupTerm(prevLogIndex)
	}
	var entries []LogEntry
	if ni <= cm.cmState.lastLogIndex {
		pos := cm.logPosition(ni)
		// Defensive check: если pos >= len(cm.cmState.log), инвариант
		// cm.cmState.lastLogIndex == cm.cmState.log[last].Index нарушен (аномалия при
		// компактировании между вычислениями). Слайсинг cm.cmState.log[pos:]
		// паникует без recover() в горутине leaderSendAEsToPeer, поэтому
		// вместо него логируем аномалию и отправляем пустой срез записей.
		if pos < len(cm.cmState.log) {
			entries = append([]LogEntry{}, cm.cmState.log[pos:]...)
		} else {
			cm.traceLockedLogf(1, "nextIndexArgsEntries: logPosition(%d) out of range (len=%d)", ni, len(cm.cmState.log))
			entries = nil
		}
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
		LeaderCommit: cm.cmState.commitIndex,
	}, entries
}

// leaderSendAEsToPeer отправляет AppendEntries указанному peer и
// обрабатывает ответ. Выполняется в отдельной горутине.
//
// dispatchEpoch — эпоха верификации, снятая в leaderSendAEs под cm.mu
// в момент запуска горутины: ответы этого AE являются голосами только
// для verify-запросов с epoch <= dispatchEpoch (AE отправлен не раньше
// постановки запроса, Raft §8 / SA-016).
//
// При любом завершении (успех, ошибка, step down) сбрасывает
// inflightAE[peerID], разрешая последующий вызов leaderSendAEs
// для этого peer. Сброс гарантирован через defer.
//
//nolint:funlen,gocognit
func (cm *ConsensusModule) leaderSendAEsToPeer(peerID, savedCurrentTerm int, dispatchEpoch uint64) {
	defer func() {
		cm.mu.Lock()
		if cm.leaderState.inflightAE != nil && cm.leaderState.inflightAE[peerID] != nil {
			cm.leaderState.inflightAE[peerID].Store(false)
		}
		cm.mu.Unlock()
	}()

	cm.mu.Lock()
	ni := cm.leaderState.nextIndex[peerID]
	if ni <= cm.cmState.lastSnapshotIndex && !isNilInterface(cm.snapshotStore) {
		cm.mu.Unlock()
		cm.leaderSendSnapshot(peerID, savedCurrentTerm)
		return
	}
	cm.mu.Unlock()

	ni, args, entries := cm.nextIndexArgsEntries(peerID, savedCurrentTerm)
	cm.traceLogf(8, "sending AppendEntries to %v: ni=%d, args=%+v", peerID, ni, args)
	reply, err := cm.transport.AppendEntries(ServerID(peerID), args)
	if err == nil {
		cm.mu.Lock()
		if reply.Term > cm.cmState.currentTerm {
			cm.traceLockedLogf(8, "term out of date in heartbeat reply")
			cm.becomeFollower(reply.Term)
			cm.mu.Unlock()
			return
		}
		if cm.cmState.state == Leader && savedCurrentTerm == reply.Term {
			if reply.Success {
				cm.leaderState.nextIndex[peerID] = ni + len(entries)
				cm.leaderState.matchIndex[peerID] = cm.leaderState.nextIndex[peerID] - 1

				cm.leaderState.commitmentTracker.setMatch(peerID, cm.leaderState.matchIndex[peerID], cm.lookupTerm)
				cm.traceLockedLogf(
					8, "AppendEntries reply from %d success: nextIndex := %v, matchIndex := %v; commitIndex := %d",
					peerID, cm.leaderState.nextIndex, cm.leaderState.matchIndex, cm.leaderState.commitmentTracker.getCommitIndex(),
				)

				if len(cm.leaderState.pendingVerify) > 0 && cm.cmState.state == Leader {
					for _, vf := range cm.leaderState.pendingVerify {
						// Голос засчитывается только от voter'а текущей
						// (latest) конфигурации и только для ответов на AE,
						// отправленные не раньше запроса (SA-004/SA-016).
						if hasVote(cm.cmState.configurations.latest, peerID) && vf.epoch <= dispatchEpoch {
							vf.vote(peerID, true)
						}
					}
					var remaining []*verifyFuture
					for _, vf := range cm.leaderState.pendingVerify {
						if vf.votes >= vf.quorumSize {
							vf.respond(nil)
						} else {
							remaining = append(remaining, vf)
						}
					}
					cm.leaderState.pendingVerify = remaining
				}

				cm.mu.Unlock()
			} else {
				if reply.ConflictTerm >= 0 {
					if lastIndex, ok := cm.cmState.termIndexMap[reply.ConflictTerm]; ok {
						cm.leaderState.nextIndex[peerID] = lastIndex + 1
					} else {
						cm.leaderState.nextIndex[peerID] = reply.ConflictIndex
					}
				} else {
					cm.leaderState.nextIndex[peerID] = reply.ConflictIndex
				}
				cm.traceLockedLogf(8, "AppendEntries reply from %d !success: nextIndex := %d", peerID, ni-1)
				cm.mu.Unlock()
			}
		} else {
			cm.mu.Unlock()
		}
	}
}

// leaderSendAEs отправляет AppendEntries всем peer, для которых
// в данный момент не выполняется горутина репликации.
//
// Для каждого peer проверяет inflightAE[peerID] через atomic.CompareAndSwap:
//   - Если флаг уже установлен (true) — горутина уже выполняется,
//     вызов для этого peer пропускается (дедупликация).
//   - Если флаг свободен (false) — устанавливает его и запускает
//     leaderSendAEsToPeer в отдельной горутине.
//
// Вызывается из runLeaderLoop после dispatchLogs, commitCh,
// heartbeatTicker и при pendingVerify.
func (cm *ConsensusModule) leaderSendAEs() {
	cm.mu.Lock()
	if cm.cmState.state != Leader {
		cm.mu.Unlock()
		return
	}
	savedCurrentTerm := cm.cmState.currentTerm
	// Снимок эпохи верификации под cm.mu: ответы этих AE будут
	// голосами только для verify-запросов с epoch <= dispatchEpoch
	// (SA-016 — голоса привязываются к раунду рассылки).
	dispatchEpoch := cm.leaderState.verifyEpoch

	peers := make(map[int]struct{})
	for _, s := range cm.cmState.configurations.committed.ConfigServers {
		if int(s.ID) != cm.id {
			peers[int(s.ID)] = struct{}{}
		}
	}
	for _, s := range cm.cmState.configurations.latest.ConfigServers {
		if int(s.ID) != cm.id {
			peers[int(s.ID)] = struct{}{}
		}
	}

	for peerID := range peers {
		if cm.leaderState.inflightAE[peerID] == nil {
			continue
		}
		if cm.leaderState.inflightAE[peerID].CompareAndSwap(false, true) {
			go cm.leaderSendAEsToPeer(peerID, savedCurrentTerm, dispatchEpoch)
		}
	}
	cm.mu.Unlock()
}

// leaderSendSnapshot отправляет последний снэпшот отстающему follower.
// Вызывается, когда nextIndex[follower] <= lastSnapshotIndex.
//
//nolint:funlen
func (cm *ConsensusModule) leaderSendSnapshot(peerID, term int) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		cm.traceLogf(4, "leaderSendSnapshot elapsed %s", elapsed)
	}()
	cm.traceLogf(4, "leaderSendSnapshot peer %d: term=%d", peerID, term)

	// Защита от nil-хранилища. Инвариант проверяется и в вызывающем коде
	// (leaderSendAEsToPeer), но при рефакторинге он может нарушиться,
	// а паника здесь упала бы в отдельной горутине без recover().
	if isNilInterface(cm.snapshotStore) {
		cm.traceLogf(4, "leaderSendSnapshot: snapshotStore is nil, cannot send to %d", peerID)
		return
	}

	snapshots, err := cm.snapshotStore.List()
	if err != nil || len(snapshots) == 0 {
		// Пустой List у лидера с lastSnapshotIndex >= 0 — признак
		// повреждения/удаления durable-стора (TASK-009): диагностика
		// и best-effort восстановление форсированием нового снапшота.
		cm.traceLogf(0, "leaderSendSnapshot: cannot send snapshot to %d: err=%v, snapshots=%d", peerID, err, len(snapshots))
		cm.mu.Lock()
		lastSnapshotIndex := cm.cmState.lastSnapshotIndex
		cm.mu.Unlock()
		if lastSnapshotIndex >= 0 && cm.snapshotCh != nil {
			select {
			case cm.snapshotCh <- struct{}{}:
			default:
			}
		}
		return
	}
	// List() может вернуть слайс с nil-элементом или пустым ID — оба
	// случая приводят к панике или некорректной работе Open().
	if snapshots[0] == nil {
		cm.traceLogf(4, "leaderSendSnapshot: snapshots[0] is nil")
		return
	}
	if snapshots[0].ID == "" {
		cm.traceLogf(4, "leaderSendSnapshot: snapshots[0].ID is empty")
		return
	}
	meta, reader, err := cm.snapshotStore.Open(snapshots[0].ID)
	if err != nil {
		cm.traceLogf(0, "leaderSendSnapshot: cannot open snapshot %s for peer %d: %v", snapshots[0].ID, peerID, err)
		return
	}
	defer func() { _ = reader.Close() }()

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
		// becomeFollower требует удержания cm.mu; здесь блокировка
		// не удерживается (RPC выполнялся без блокировки).
		cm.mu.Lock()
		cm.becomeFollower(reply.Term)
		cm.mu.Unlock()
		return
	}
	if reply.Success {
		cm.mu.Lock()
		cm.leaderState.nextIndex[peerID] = meta.Index + 1
		cm.leaderState.matchIndex[peerID] = meta.Index
		cm.mu.Unlock()
	}
}
