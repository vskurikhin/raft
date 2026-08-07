package raft

import (
	"fmt"
	"time"
)

// applyBatch применяет все LogCommand записи из батча за один вызов
// ApplyBatch переданного BatchingFSM. Non-command записи (noop)
// разрешаются с nil-ответом без вызова FSM. При несовпадении длины
// возвращаемого среза с числом записей все future батча отвечают ошибкой
// ErrBatchFSMResponseMismatch. Паника в этом месте недопустима (AGENTS.md):
// она упала бы в горутине runFSM и убила бы весь процесс.
func (cm *ConsensusModule) applyBatch(fsm BatchingFSM, batch []*commitTuple) {
	var logs []*LogEntry
	var futures []*logFuture
	var nonCmdFutures []*logFuture
	for _, ct := range batch {
		if ct.log.Type != LogCommand {
			if ct.future != nil {
				nonCmdFutures = append(nonCmdFutures, ct.future)
			}
			continue
		}
		logs = append(logs, ct.log)
		futures = append(futures, ct.future)
	}
	for _, f := range nonCmdFutures {
		f.response = nil
		f.respond(nil)
	}
	if len(logs) == 0 {
		return
	}
	responses := fsm.ApplyBatch(logs)
	if len(responses) != len(logs) {
		// Внешний FSM нарушил контракт BatchingFSM: число ответов не
		// совпадает с числом записей. Нельзя доверять частичному
		// результату, поэтому FSM-ответы не интерпретируются. Отвечаем
		// ошибкой во все future батча (они ещё не отвечены — noop/non-command
		// записи обработаны раньше и в срез futures не входят) и выходим:
		// runFSM продолжает жить, следующие батчи обрабатываются как обычно.
		err := fmt.Errorf("%w: got %d, want %d",
			ErrBatchFSMResponseMismatch, len(responses), len(logs))
		for _, f := range futures {
			if f != nil {
				f.respond(err)
			}
		}
		return
	}
	for i, f := range futures {
		if f != nil {
			f.response = responses[i]
			f.respond(nil)
		}
	}
}

// applySingle применяет каждую запись из батча по отдельности через
// cm.fsm.Apply. Используется для FSM, не реализующих BatchingFSM.
func (cm *ConsensusModule) applySingle(batch []*commitTuple) {
	for _, ct := range batch {
		if ct.log.Type != LogCommand {
			if ct.future != nil {
				ct.future.response = nil
				ct.future.respond(nil)
			}
			continue
		}
		resp := cm.fsm.Apply(ct.log)
		if ct.future != nil {
			ct.future.response = resp
			ct.future.respond(nil)
		}
	}
}

// processLogs собирает закоммиченные записи от lastApplied+1 до commitIndex
// и отправляет их в FSM goroutine. Диапазон делится на под-батчи размером
// не более maxApplyBatchSize для снижения latency FSM.
// Метод не требует удержания cm.mu при вызове, но сам захватывает её
// внутри для чтения журнала и поиска future в карте inflight.
func (cm *ConsensusModule) processLogs(commitIndex int) {
	startTimeNow := time.Now()
	defer func() { latencyProcessLogsCh <- time.Since(startTimeNow) }()
	cm.mu.Lock()
	if commitIndex <= cm.cmState.lastApplied {
		cm.mu.Unlock()
		return
	}
	lastApplied := cm.cmState.lastApplied
	cm.cmState.lastApplied = commitIndex
	cm.persistToStorage()
	cm.mu.Unlock()

	start := lastApplied + 1
	for start <= commitIndex {
		end := start + maxApplyBatchSize - 1
		if end > commitIndex {
			end = commitIndex
		}
		cm.sendBatch(start, end)
		start = end + 1
	}
}

// runFSM — отдельная горутина, применяющая закоммиченные записи к FSM.
// Если FSM реализует BatchingFSM, использует ApplyBatch для группового
// применения; иначе вызывает Apply для каждой записи по отдельности.
// Type assertion выполняется один раз при старте горутины.
// Завершает работу при закрытии shutdownCh.
func (cm *ConsensusModule) runFSM() {
	startTimeNow := time.Now()
	defer func() { latencyBatchingFSMCh <- time.Since(startTimeNow) }()
	batchingFSM, supportsBatch := cm.fsm.(BatchingFSM)
	for {
		select {
		case batch := <-cm.fsmMutateCh:
			if supportsBatch {
				cm.applyBatch(batchingFSM, batch)
			} else {
				cm.applySingle(batch)
			}
		case req := <-cm.fsmSnapshotCh:
			cm.handleFsmSnapshot(req)
		case <-cm.shutdownCh:
			return
		}
	}
}

// sendBatch собирает под-батч записей журнала от start до end включительно
// и отправляет в fsmMutateCh. Поиск future в inflight выполняется за O(1)
// через map[int]*logFuture. Блокировка cm.mu удерживается только на время
// поиска и чтения журнала, отправка — без блокировки.
func (cm *ConsensusModule) sendBatch(start, end int) {
	startTimeNow := time.Now()
	defer func() { latencySendBatchCh <- time.Since(startTimeNow) }()
	cm.mu.Lock()
	batch := make([]*commitTuple, 0, end-start+1)
	for idx := start; idx <= end; idx++ {
		future := cm.leaderState.inflight[idx]
		delete(cm.leaderState.inflight, idx)
		if idx > cm.cmState.lastLogIndex {
			continue
		}
		pos := cm.logPosition(idx)
		if pos >= len(cm.cmState.log) {
			cm.traceLockedLogf(
				0, "sendBatch: logPosition(%d) returned %d, len(log)=%d",
				idx, pos, len(cm.cmState.log),
			)
			continue
		}
		// Защита от применения чужой записи: для idx ниже первого индекса
		// журнала бинарный поиск возвращает позицию 0, но запись log[0]
		// не соответствует idx и не должна применяться к FSM.
		if cm.cmState.log[pos].Index != idx {
			cm.traceLockedLogf(
				0, "sendBatch: no log entry at index %d (entry=%d), skipping",
				idx, cm.cmState.log[pos].Index,
			)
			continue
		}
		log := &cm.cmState.log[pos]
		batch = append(batch, &commitTuple{log: log, future: future})
	}
	cm.mu.Unlock()
	if len(batch) > 0 {
		cm.fsmMutateCh <- batch
	}
}
