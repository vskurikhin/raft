package raft

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"time"
)

// Ключи постоянного состояния в Storage. Имя файла данных на диске —
// <ключ>.dat (FileStorage), поэтому написание ключей — формат
// персистентности: изменение ключа делает уже записанные данные
// нечитаемыми без переименования файлов.
const (
	_storageKeyCurrentTerm       = "currentTerm"
	_storageKeyVotedFor          = "votedFor"
	_storageKeyLog               = "log"
	_storageKeyLastSnapshotIndex = "lastSnapshotIndex"
	_storageKeyLastSnapshotTerm  = "lastSnapshotTerm"
)

// persistToStorageLocked сохраняет постоянное состояние CM в cm.storage.
// Кодирование журнала выполняется только при cm.cmState.logNeedsPersist == true
// (т.е. когда лог действительно изменился), что позволяет избежать
// дорогого gob.Encode(cm.cmState.log) на каждом heartbeat или RPC.
//
// Порядок записи ключей важен для отказоустойчивости:
// currentTerm, votedFor, lastSnapshotIndex, lastSnapshotTerm, затем log.
// Снимок-ключи пишутся ДО усечённого лога: окно «лог усечён, а
// lastSnapshotIndex старый/отсутствует» устраняется; обратное окно
// («lastSnapshotIndex новее, лог полный») безопасно — полный лог
// консервативнее, а startup-restore самовосстанавливает рассинхрон
// (restoreFromSnapshotStore).
//
// На входе фиксируются источник, режим и начальный момент. Успешное
// завершение всех прежних Set регистрирует одно наблюдение: источник,
// режим и длительность от входа до последнего необходимого Set (до
// обновления агрегатов и постановки прежней trace-строки). В полном режиме
// вместе с наблюдением используются размер журнала и уже закодированные
// байты: повторного кодирования ради статистики нет. Метрики не влияют на
// решение о сохранении и не создают записей Storage.
//
// Четыре скалярных значения кодируются через кэш последнего представления
// (cm.scalarCache): неизменённое значение переиспользует готовые байты,
// изменённое — кодируется заново прежним способом (gob(int) в собственный
// bytes.Buffer). Кэш не означает долговечности: Set каждого скалярного
// ключа выполняется при каждом вызове независимо от попадания в кэш.
//
// Требует удержания cm.mu — мьютекса владельца сохраняемого состояния.
func (cm *ConsensusModule) persistToStorageLocked(source persistSource) {
	start := time.Now()
	full := cm.cmState.logNeedsPersist
	cm.storage.Set(_storageKeyCurrentTerm,
		encodeScalarLocked(&cm.scalarCache.currentTerm, cm.cmState.currentTerm))
	cm.storage.Set(_storageKeyVotedFor,
		encodeScalarLocked(&cm.scalarCache.votedFor, cm.cmState.votedFor))
	cm.storage.Set(_storageKeyLastSnapshotIndex,
		encodeScalarLocked(&cm.scalarCache.lastSnapshotIndex, cm.cmState.lastSnapshotIndex))
	cm.storage.Set(_storageKeyLastSnapshotTerm,
		encodeScalarLocked(&cm.scalarCache.lastSnapshotTerm, cm.cmState.lastSnapshotTerm))

	var logLen, logBytes int
	if full {
		var logData bytes.Buffer
		if err := gob.NewEncoder(&logData).Encode(cm.cmState.log); err != nil {
			log.Fatal(err)
		}
		// Размер журнала и готовые байты кодирования снимаются до Set;
		// буфер не копируется и повторно не кодируется.
		logLen = len(cm.cmState.log)
		logBytes = logData.Len()
		cm.storage.Set(_storageKeyLog, logData.Bytes())
		// Период грязного журнала закрывается после успешного Set(log)
		// и до прежнего сброса флага: возраст считается до этого момента,
		// ожидание — от первой отметки до входа в сохранение. Обе операции
		// выполняются под одним удержанием cm.mu, порядок на семантику
		// не влияет.
		cm.dirty.completeAt(start, time.Now())
		cm.cmState.logNeedsPersist = false
	}

	elapsed := time.Since(start)
	cm.persistence.observe(source, full, elapsed, logLen, logBytes)
	if traceEnabled(_traceLevelProgress) {
		cm.traceLogfLocked("persistToStorage elapsed %s", elapsed)
	}
}

// restoreFromStorage восстанавливает постоянное состояние данного CM
// из хранилища. Должен вызываться в конструкторе до запуска какой-либо
// конкурентной работы. Вызывает rebuildLastLogLocked и
// rebuildTermIndexMapLocked без удержания cm.mu: это допустимо только
// в однопоточном конструкторе, до запуска первой горутины.
func (cm *ConsensusModule) restoreFromStorage() {
	termData, found := cm.storage.Get(_storageKeyCurrentTerm)
	if !found {
		log.Fatal("currentTerm not found in storage")
	}
	d := gob.NewDecoder(bytes.NewBuffer(termData))
	if err := d.Decode(&cm.cmState.currentTerm); err != nil {
		log.Fatal(err)
	}
	votedData, found := cm.storage.Get(_storageKeyVotedFor)
	if !found {
		log.Fatal("votedFor not found in storage")
	}
	d = gob.NewDecoder(bytes.NewBuffer(votedData))
	if err := d.Decode(&cm.cmState.votedFor); err != nil {
		log.Fatal(err)
	}
	logData, found := cm.storage.Get(_storageKeyLog)
	if !found {
		log.Fatal("log not found in storage")
	}
	d = gob.NewDecoder(bytes.NewBuffer(logData))
	if err := d.Decode(&cm.cmState.log); err != nil {
		log.Fatal(err)
	}
	if err := cm.checkSnapshotKeysConsistency(); err != nil {
		log.Fatal(err)
	}
	cm.rebuildLastLogLocked()
	cm.rebuildTermIndexMapLocked()

	cm.rebuildConfigurations()

	if snapIdxData, found := cm.storage.Get(_storageKeyLastSnapshotIndex); found {
		d := gob.NewDecoder(bytes.NewBuffer(snapIdxData))
		if err := d.Decode(&cm.cmState.lastSnapshotIndex); err != nil {
			log.Fatal(err)
		}
	}
	if snapTermData, found := cm.storage.Get(_storageKeyLastSnapshotTerm); found {
		d := gob.NewDecoder(bytes.NewBuffer(snapTermData))
		if err := d.Decode(&cm.cmState.lastSnapshotTerm); err != nil {
			log.Fatal(err)
		}
	}
}

// checkSnapshotKeysConsistency проверяет, что для сжатого журнала
// (первая запись с Index > 0) в storage присутствуют ключи
// lastSnapshotIndex/lastSnapshotTerm. Их отсутствие означало бы тихую
// потерю метаданных снимка при усечённом журнале на диске.
// Для свежего узла (полный лог с первой записью Index == 0) отсутствие
// ключей допустимо (обратная совместимость данных). Выделена в отдельную
// функцию для тестирования fail-fast пути без завершения процесса.
func (cm *ConsensusModule) checkSnapshotKeysConsistency() error {
	if len(cm.cmState.log) == 0 || cm.cmState.log[0].Index == 0 {
		return nil
	}
	firstIndex := cm.cmState.log[0].Index
	if _, found := cm.storage.Get(_storageKeyLastSnapshotIndex); !found {
		return fmt.Errorf(
			"log compacted (first index %d) but lastSnapshotIndex missing in storage",
			firstIndex,
		)
	}
	if _, found := cm.storage.Get(_storageKeyLastSnapshotTerm); !found {
		return fmt.Errorf(
			"log compacted (first index %d) but lastSnapshotTerm missing in storage",
			firstIndex,
		)
	}
	return nil
}

// restoreFromSnapshotStore восстанавливает FSM из последнего снимка
// SnapshotStore и синхронизирует lastApplied/fsmAppliedIndex/commitIndex/
// lastLog* с метаданными снимка. Должен вызываться в конструкторе до запуска
// какой-либо конкурентной работы (однопоточный старт, cm.mu не требуется,
// как у restoreFromStorage). Все ошибки и нарушения инвариантов
// возвращаются вызывающему; решение о фатальности принимает вызывающий код.
func (cm *ConsensusModule) restoreFromSnapshotStore() error {
	if IsNilInterface(cm.snapshotStore) {
		return nil
	}
	snapshots, err := cm.snapshotStore.List()
	if err != nil {
		return fmt.Errorf("snapshot store List: %w", err)
	}

	snapshotChanged := false
	if len(snapshots) > 0 {
		latest := snapshots[0] // List упорядочен от новых к старым
		if latest == nil || latest.ID == "" {
			return errors.New("snapshot store returned invalid snapshot meta")
		}
		if latest.Index < cm.cmState.lastSnapshotIndex {
			return fmt.Errorf(
				"snapshot store is behind storage: snapshot index %d < lastSnapshotIndex %d",
				latest.Index, cm.cmState.lastSnapshotIndex,
			)
		}
		meta, reader, err := cm.snapshotStore.Open(latest.ID)
		if err != nil {
			return fmt.Errorf("open snapshot %s: %w", latest.ID, err)
		}
		defer func() { _ = reader.Close() }()
		if err := cm.fsm.Restore(reader); err != nil {
			return fmt.Errorf("fsm restore from snapshot %s: %w", latest.ID, err)
		}
		// Восстановление конфигурации кластера из метаданных снимка
		// снимок берёт configurations.committed в takeSnapshot,
		// поэтому конфигурация снимка зафиксирована по определению. Применяется,
		// только если новее конфигурации, восстановленной из суффикса лога;
		// пустая конфигурация (легаси/тестовые снимки) игнорируется.
		if meta.ConfigIndex > cm.cmState.configurations.latestIndex &&
			len(meta.Configuration.ConfigServers) > 0 {
			cm.cmState.configurations.committed = meta.Configuration
			cm.cmState.configurations.committedIndex = meta.ConfigIndex
			cm.cmState.configurations.latest = meta.Configuration
			cm.cmState.configurations.latestIndex = meta.ConfigIndex
		}
		if meta.Index != cm.cmState.lastSnapshotIndex {
			// Окно сбоя между Close снимка и persistToStorage
			// снимок в постоянном хранилище новее диска —
			// самовосстановление метаданных из стора с повторным персистом.
			cm.cmState.lastSnapshotIndex = meta.Index
			cm.cmState.lastSnapshotTerm = meta.Term
			snapshotChanged = true
		}
		cm.cmState.lastApplied = meta.Index
		// fsm.Restore выше заместил состояние машины состояний целиком:
		// отметка фактически применённого индекса равна индексу снимка.
		// Без этого присваивания первый запрос снимка после перезапуска
		// вернул бы ErrNothingNewToSnapshot навсегда (отметка осталась бы
		// равной -1), и журнал рос бы неограниченно.
		cm.cmState.fsmAppliedIndex = meta.Index
		cm.cmState.commitIndex = meta.Index
		if cm.cmState.lastLogIndex < meta.Index {
			// Пустой или короткий журнал (trailing=0): синхронизация
			// lastLogIndex/lastLogTerm со снимком — симметрично
			// handleInstallSnapshot. Иначе rebuildLastLogLocked() оставил бы
			// lastLogIndex = -1 при lastApplied > 0 (нарушение инварианта
			// lastApplied <= lastLogIndex, новая запись получила бы
			// индекс, уже покрытый снимком).
			cm.cmState.lastLogIndex = meta.Index
			cm.cmState.lastLogTerm = meta.Term
		}
	} else if cm.cmState.lastSnapshotIndex >= 0 {
		// Журнал на диске усечён, а снимок, обосновывающий усечение,
		// отсутствует: продолжение — молчаливая потеря данных.
		return fmt.Errorf(
			"log compacted up to snapshot index %d but snapshot store is empty",
			cm.cmState.lastSnapshotIndex,
		)
	}

	// Инварианты непрерывности лога (после восстановления).
	if err := cm.checkSnapshotLogContinuity(); err != nil {
		return err
	}

	if snapshotChanged {
		// logNeedsPersist уже false (сброшен после restoreFromStorage),
		// поэтому log-ключ не переписывается — только дешёвые ключи.
		// Локальная блокировка охватывает только сохранение и
		// заканчивается в этой же функции: snapshotStore.List/Open и
		// fsm.Restore выше выполнялись без cm.mu (однопоточный старт).
		cm.mu.Lock()
		defer cm.mu.Unlock()
		cm.persistToStorageLocked(persistSourceStartupRestore)
	}
	return nil
}

// checkSnapshotLogContinuity проверяет инварианты непрерывности лога
// относительно снимка. Выделена в отдельную функцию для тестирования
// fail-fast путей без завершения процесса.
func (cm *ConsensusModule) checkSnapshotLogContinuity() error {
	if len(cm.cmState.log) > 0 && cm.cmState.log[0].Index > cm.cmState.lastSnapshotIndex+1 {
		return fmt.Errorf(
			"gap between snapshot (index %d) and first log entry (index %d)",
			cm.cmState.lastSnapshotIndex, cm.cmState.log[0].Index,
		)
	}
	if cm.cmState.lastLogIndex < cm.cmState.lastSnapshotIndex {
		// Безусловная проверка, включая пустой журнал: guard выше при
		// len(log) == 0 молча пропускается. После синхронизации
		// lastLogIndex со снимком — defense-in-depth против регрессий.
		return fmt.Errorf(
			"lastLogIndex %d behind lastSnapshotIndex %d",
			cm.cmState.lastLogIndex, cm.cmState.lastSnapshotIndex,
		)
	}
	return nil
}
