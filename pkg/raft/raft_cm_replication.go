package raft

import "slices"

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

// leaderSendAEsToPeer отправляет AppendEntries указанному peer-у,
// обрабатывает ответ и обновляет состояние CM. При успешной репликации
// использует commitmentTracker для вычисления нового commitIndex.
//
// Вызов происходит в отдельной goroutine для каждого peer-а.
// peerID — идентификатор получателя, savedCurrentTerm — терм на момент
// формирования запроса (для защиты от устаревших ответов).
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
					lastIndexOfTerm := -1
					for i, v := range slices.Backward(cm.log) {
						if v.Term == reply.ConflictTerm {
							lastIndexOfTerm = i
							break
						}
					}
					if lastIndexOfTerm >= 0 {
						cm.nextIndex[peerID] = lastIndexOfTerm + 1
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

// leaderSendAEs отправляет очередной раунд сообщений AppendEntries всем
// соседям, обрабатывает их ответы и обновляет состояние CM.
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
	cm.mu.Unlock()

	for peerID := range peers {
		go cm.leaderSendAEsToPeer(peerID, savedCurrentTerm)
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
