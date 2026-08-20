package raft

import (
	"fmt"
	"time"
)

func (cm *ConsensusModule) nextIndexArgsEntries(peerID, savedCurrentTerm int) (int, AppendEntriesArgs, []LogEntry, bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	ni := cm.leaderState.nextIndex[peerID]
	prevLogIndex := ni - 1
	prevLogTerm := -1
	if 0 <= prevLogIndex && prevLogIndex <= cm.cmState.lastLogIndex {
		prevLogTerm = cm.lookupTerm(prevLogIndex)
	}

	// Если терм для prevLogIndex неизвестен — нужен снимок: иначе ведомый отвергнет AE.
	snapshotNeeded := prevLogIndex >= 0 && prevLogTerm == -1
	var entries []LogEntry
	if ni <= cm.cmState.lastLogIndex {
		pos := cm.logPosition(ni)

		// Defensive check: если позиция вне диапазона — логируем аномалию и возвращаем пустой срез.
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
	}, entries, snapshotNeeded
}

// leaderSendAEsToPeer отправляет AppendEntries указанному peer и
// обрабатывает ответ. Выполняется в отдельной горутине.
//
// dispatchEpoch — эпоха верификации, снятая в leaderSendAEs под cm.mu
// в момент запуска горутины: ответы этого AE являются голосами только
// для verify-запросов с epoch <= dispatchEpoch (AE отправлен не раньше
// постановки запроса, Raft §8).
//
// При любом завершении (успех, ошибка, step down) сбрасывает
// inflightAE[peerID], разрешая последующий вызов leaderSendAEs
// для этого peer. Сброс гарантирован через defer.
//
// Задержка повторов: перед отправкой вычисляется задержка на каждого соседа
// от числа подряд идущих транспортных ошибок; при активной задержке
// попытка пропускается. Пропуск — сравнение времени с lastAttempt, а не
// time.Sleep: sleep удержал бы inflightAE[peer] и задерживал бы
// восстановление вернувшегося узла.
//
//nolint:gocognit
func (cm *ConsensusModule) leaderSendAEsToPeer(peerID, savedCurrentTerm int, dispatchEpoch uint64) {
	defer func() {
		cm.mu.Lock()
		if cm.leaderState.inflightAE != nil && cm.leaderState.inflightAE[peerID] != nil {
			cm.leaderState.inflightAE[peerID].Store(false)
		}
		cm.mu.Unlock()
	}()

	cm.mu.Lock()
	// Пропуск попытки при активном задержка повторов. Действует только для текущего
	// лидерства: запоздавшие горутины прошлых термов не влияют на
	// состояние нового лидера.
	if cm.replicationBackoffActive(peerID, savedCurrentTerm) {
		cm.mu.Unlock()
		return
	}
	ni := cm.leaderState.nextIndex[peerID]
	// Предикат «снимок или AppendEntries» по фактической обслуживаемости
	// запроса: first — первый индекс, для которого лидер
	// может сообщить терм (граница снимка при пустом срезе, первая
	// запись среза иначе); prev = ni - 1. Снимок отправляется только
	// когда prev попадает в дыру между снимком и первым индексом
	// журнала — единственный случай, когда lookupTerm(prev) == -1.
	first := cm.cmState.lastSnapshotIndex + 1
	if len(cm.cmState.log) > 0 {
		first = cm.cmState.log[0].Index
	}
	prev := ni - 1
	if !isNilInterface(cm.snapshotStore) &&
		prev >= 0 && prev != cm.cmState.lastSnapshotIndex && prev < first {
		cm.mu.Unlock()
		cm.leaderSendSnapshot(peerID, savedCurrentTerm)
		return
	}
	cm.mu.Unlock()

	ni, args, entries, snapshotNeeded := cm.nextIndexArgsEntries(peerID, savedCurrentTerm)
	// Если терм неизвестен (PrevLogIndex >= 0), отправить снимок:
	// AppendEntries будет гарантированно отвергнут ведомым.
	if snapshotNeeded && !isNilInterface(cm.snapshotStore) {
		cm.leaderSendSnapshot(peerID, savedCurrentTerm)
		return
	}
	cm.traceLogf(8, "sending AppendEntries to %v: ni=%d, args=%+v", peerID, ni, args)
	// Фиксация момента реальной попытки — только для текущего лидерства;
	// запоздавшие горутины прошлых термов состояние не трогают.
	cm.mu.Lock()
	if cm.cmState.state == Leader && cm.cmState.currentTerm == savedCurrentTerm {
		cm.recordAttempt(peerID)
	}
	cm.mu.Unlock()
	reply, err := cm.transport.AppendEntries(ServerID(peerID), args)
	if err != nil {
		// Задержка повторов — только для транспортных ошибок; логические отказы (Success:false) его не вызывают.
		cm.mu.Lock()
		if cm.cmState.state == Leader && cm.cmState.currentTerm == savedCurrentTerm {
			cm.incReplFailures(peerID)
		}
		cm.mu.Unlock()
		return
	}
	// Сброс задержка повторов при любом RPC без транспортной ошибки, независимо от reply.Success.
	cm.mu.Lock()
	if cm.cmState.state == Leader && cm.cmState.currentTerm == savedCurrentTerm {
		cm.resetReplFailures(peerID)
	}
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
					// Голос учитывается только от узла с правом голоса текущей конфигурации
					// и только для ответов на AppendEntries, отправленных не раньше запроса.
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
			cm.handleFailedAEReply(peerID, reply)
		}
	} else {
		cm.mu.Unlock()
	}
}

// replicationBackoffActive сообщает, активен ли задержка повторов для попытки
// репликации текущего лидерства: с последней реальной
// попытки прошло меньше задержки, вычисленной по числу транспортных
// ошибок. Требует удержания cm.mu.
func (cm *ConsensusModule) replicationBackoffActive(peerID, savedCurrentTerm int) bool {
	if cm.cmState.state != Leader || cm.cmState.currentTerm != savedCurrentTerm {
		return false
	}
	delay := replicationBackoffDelay(cm.leaderState.replFailures[peerID])
	return delay > 0 && !cm.leaderState.lastAttempt[peerID].IsZero() &&
		time.Since(cm.leaderState.lastAttempt[peerID]) < delay
}

// handleFailedAEReply обрабатывает логический отказ AppendEntries
// (Success:false): счётчик, предохранитель невозможного отказа
// и обновление nextIndex. Требует удержания cm.mu.
func (cm *ConsensusModule) handleFailedAEReply(peerID int, reply AppendEntriesReply) {
	// Счётчик отказов AE: «follower отвергает
	// AppendEntries?» — ключевой признак цикла IS↔AE-fail.
	cm.counters.appendEntriesRejected = incPeerCount(cm.counters.appendEntriesRejected, peerID)
	newNi := reply.ConflictIndex
	if reply.ConflictTerm >= 0 {
		if lastIndex, ok := cm.cmState.termIndexMap[reply.ConflictTerm]; ok {
			newNi = lastIndex + 1
		}
	}
	// Предохранитель: отказ, результат которого не
	// может быть истинным (отрицательный ConflictIndex либо
	// новый nextIndex не больше доказанного сохранённого на диске префикса
	// matchIndex[peer]), не изменяет состояние репликации —
	// прецедент etcd/raft Progress.MaybeDecrTo. Аномалия
	// наблюдаема через счётчик и трассировку уровня 0.
	if reply.ConflictIndex < 0 || newNi <= cm.leaderState.matchIndex[peerID] {
		cm.counters.nextIndexRejectionIgnored = incPeerCount(
			cm.counters.nextIndexRejectionIgnored, peerID,
		)
		cm.traceLockedLogf(
			0, "AppendEntries reply from %d rejected (impossible): peer=%d term=%d ni=%d matchIndex=%d ConflictIndex=%d ConflictTerm=%d",
			peerID, peerID, reply.Term, newNi, cm.leaderState.matchIndex[peerID],
			reply.ConflictIndex, reply.ConflictTerm,
		)
		cm.mu.Unlock()
		return
	}
	cm.leaderState.nextIndex[peerID] = newNi
	cm.traceLockedLogf(8, "%s", failedAETrace(
		peerID, cm.leaderState.nextIndex[peerID], cm.leaderState.matchIndex[peerID],
		reply.ConflictIndex, reply.ConflictTerm,
	))
	cm.mu.Unlock()
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
	// (голоса привязываются к раунду рассылки).
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
			cm.goSpawnLocked(func() {
				cm.leaderSendAEsToPeer(peerID, savedCurrentTerm, dispatchEpoch)
			})
		}
	}
	cm.mu.Unlock()
}

// leaderSendSnapshot отправляет последний снимок отстающему follower.
// Вызывается, когда лидер не может обслужить AppendEntries для текущего
// nextIndex[follower] (предикат).
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
		// повреждения/удаления постоянного хранилища: диагностика
		// и best-effort восстановление форсированием нового снимка.
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

	// Фиксация момента реальной попытки и счётчик
	// отправленных снимков: «лидер повторно шлёт
	// снимок?» — инкремент на каждую реальную отправку через транспорт.
	cm.mu.Lock()
	if cm.cmState.state == Leader && cm.cmState.currentTerm == term {
		cm.recordAttempt(peerID)
	}
	cm.counters.installSnapshotSent = incPeerCount(cm.counters.installSnapshotSent, peerID)
	cm.mu.Unlock()

	reply, err := cm.transport.InstallSnapshot(ServerID(peerID), req, reader)
	if err != nil {
		// Задержка повторов активируется только транспортными ошибками
		// для текущего лидерства.
		cm.mu.Lock()
		if cm.cmState.state == Leader && cm.cmState.currentTerm == term {
			cm.incReplFailures(peerID)
		}
		cm.mu.Unlock()
		return
	}
	// Сброс задержка повторов при RPC без транспортной ошибки,
	// независимо от reply.Success.
	cm.mu.Lock()
	if cm.cmState.state == Leader && cm.cmState.currentTerm == term {
		cm.resetReplFailures(peerID)
	}
	cm.mu.Unlock()
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
		// Симметрия с путём AppendEntries (raft_cm_replication.go, ветка
		// успеха AE): запоздавший ответ из вытесненного term'а не должен
		// менять состояние репликации нового лидера.
		if cm.cmState.state == Leader && term == reply.Term {
			cm.leaderState.nextIndex[peerID] = meta.Index + 1
			cm.leaderState.matchIndex[peerID] = meta.Index
			// matchIndex = meta.Index означает сохранённое на диске владение префиксом
			// до meta.Index — ровно то, что гарантирует Success:true.
			// Проверка term'а внутри setMatch работает через fallback
			// lookupTerm на lastSnapshotTerm.
			cm.leaderState.commitmentTracker.setMatch(peerID, meta.Index, cm.lookupTerm)
		}
		cm.mu.Unlock()
	}
}

// failedAETrace формирует текст трассировки неудачного AppendEntries
// печатается фактически присвоенное значение
// nextIndex (ранее ошибочно печаталось ni-1), а также matchIndex и поля
// конфликта ответа — ровно тот набор, которого не хватило при разборе
// обоих инцидентов. Выделена в чистую функцию: мутация traceCM и
// _traceLogger в тестах запрещена, формат проверяется
// unit-тестом функции.
func failedAETrace(peerID, nextIndex, matchIndex, conflictIndex, conflictTerm int) string {
	return fmt.Sprintf(
		"AppendEntries reply from %d !success: nextIndex := %d (ConflictIndex=%d, ConflictTerm=%d, matchIndex=%d)",
		peerID, nextIndex, conflictIndex, conflictTerm, matchIndex,
	)
}

// replicationBackoffDelay вычисляет задержку на каждого соседа между попытками
// репликации по числу подряд идущих транспортных ошибок:
// delay = min(HeartbeatTimeoutMs * 2^min(failures, 5), 1000) мс.
// Принудительное ограничение в заданных пределах: при HeartbeatTimeoutMs = 33
// и показателе 5 получается 33·2⁵ = 1056 мс > потолка 1000 мс.
// При нуле ошибок задержка нулевая — поведение прежнее.
func replicationBackoffDelay(failures int) time.Duration {
	if failures <= 0 {
		return 0
	}
	const maxBackoffDelay = 1000 * time.Millisecond
	delay := HeartbeatTimeoutMs * time.Millisecond * time.Duration(1<<min(failures, 5))
	if delay > maxBackoffDelay {
		delay = maxBackoffDelay
	}
	return delay
}

// recordAttempt фиксирует момент реальной попытки отправки RPC пиру.
// Требует удержания cm.mu.
func (cm *ConsensusModule) recordAttempt(peerID int) {
	if cm.leaderState.lastAttempt == nil {
		cm.leaderState.lastAttempt = make(map[int]time.Time)
	}
	cm.leaderState.lastAttempt[peerID] = time.Now()
}

// incReplFailures инкрементирует число подряд идущих транспортных ошибок
// по пиру: вызывается только при err != nil от
// транспорта. Требует удержания cm.mu.
func (cm *ConsensusModule) incReplFailures(peerID int) {
	if cm.leaderState.replFailures == nil {
		cm.leaderState.replFailures = make(map[int]int)
	}
	cm.leaderState.replFailures[peerID]++
}

// resetReplFailures сбрасывает счётчик транспортных ошибок по пиру при
// любом RPC без ошибки транспорта — независимо от reply.Success.
// Требует удержания cm.mu.
func (cm *ConsensusModule) resetReplFailures(peerID int) {
	if cm.leaderState.replFailures != nil {
		cm.leaderState.replFailures[peerID] = 0
	}
}
