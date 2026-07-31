package raft

import "time"

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
// обрабатывает ответ. Выполняется в отдельной горутине.
//
// При любом завершении (успех, ошибка, step down) сбрасывает
// inflightAE[peerID], разрешая последующий вызов leaderSendAEs
// для этого peer. Сброс гарантирован через defer.
func (cm *ConsensusModule) leaderSendAEsToPeer(peerID, savedCurrentTerm int) {
	defer func() {
		cm.mu.Lock()
		if cm.inflightAE != nil && cm.inflightAE[peerID] != nil {
			cm.inflightAE[peerID].Store(false)
		}
		cm.mu.Unlock()
	}()

	cm.mu.Lock()
	ni := cm.nextIndex[peerID]
	if ni <= cm.lastSnapshotIndex && !isNilInterface(cm.snapshotStore) {
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
		if reply.Term > cm.currentTerm {
			cm.traceLogfLocked(8, "term out of date in heartbeat reply")
			cm.becomeFollower(reply.Term)
			cm.mu.Unlock()
			return
		}
		if cm.state == Leader && savedCurrentTerm == reply.Term {
			if reply.Success {
				cm.nextIndex[peerID] = ni + len(entries)
				cm.matchIndex[peerID] = cm.nextIndex[peerID] - 1

				cm.commitmentTracker.setMatch(peerID, cm.matchIndex[peerID], cm.lookupTerm)
				cm.traceLogfLocked(
					8, "AppendEntries reply from %d success: nextIndex := %v, matchIndex := %v; commitIndex := %d",
					peerID, cm.nextIndex, cm.matchIndex, cm.commitmentTracker.getCommitIndex(),
				)

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
				cm.traceLogfLocked(8, "AppendEntries reply from %d !success: nextIndex := %d", peerID, ni-1)
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
	if cm.state != Leader {
		cm.mu.Unlock()
		return
	}
	savedCurrentTerm := cm.currentTerm

	peers := make(map[int]struct{})
	for _, s := range cm.configurations.committed.ConfigServers {
		if int(s.ID) != cm.id {
			peers[int(s.ID)] = struct{}{}
		}
	}
	for _, s := range cm.configurations.latest.ConfigServers {
		if int(s.ID) != cm.id {
			peers[int(s.ID)] = struct{}{}
		}
	}

	for peerID := range peers {
		if cm.inflightAE[peerID] == nil {
			continue
		}
		if cm.inflightAE[peerID].CompareAndSwap(false, true) {
			go cm.leaderSendAEsToPeer(peerID, savedCurrentTerm)
		}
	}
	cm.mu.Unlock()
}

// leaderSendSnapshot отправляет последний снэпшот отстающему follower.
// Вызывается, когда nextIndex[follower] <= lastSnapshotIndex.
func (cm *ConsensusModule) leaderSendSnapshot(peerID int, term int) {
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
		cm.nextIndex[peerID] = meta.Index + 1
		cm.matchIndex[peerID] = meta.Index
		cm.mu.Unlock()
	}
}
