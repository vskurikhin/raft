package raft

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/vskurikhin/raft/pkg/raft/contract"
)

// Ключи постоянного состояния в Storage. Имя файла данных на диске —
// <ключ>.dat (FileStorage), поэтому написание ключей — формат
// персистентности: изменение ключа делает уже записанные данные
// нечитаемыми без переименования файлов.
const (
	_storageKeyCurrentTerm       = "currentTerm"
	_storageKeyVotedFor          = "votedFor"
	_storageKeyLastSnapshotIndex = "lastSnapshotIndex"
	_storageKeyLastSnapshotTerm  = "lastSnapshotTerm"
)

// logPersistMode — явное состояние точки грязи журнала. Различает три
// взаимоисключающих исхода следующего сохранения: журнал не изменялся,
// изменён суффикс от абсолютного индекса либо журнал заменяется целиком.
// Режим фиксируется при мутации журнала и сбрасывается только после
// успешной операции хранилища.
type logPersistMode int

const (
	// logPersistClean — журнал не изменялся: операция хранилища не
	// выполняется, сохраняются только скаляры.
	logPersistClean logPersistMode = iota
	// logPersistSuffix — изменён суффикс: хранилище заменяет сохранённый
	// суффикс от logDirtyFrom.
	logPersistSuffix
	// logPersistRewrite — журнал заменяется целиком: первый персист
	// свежего узла, уплотнение и установка снимка.
	logPersistRewrite
)

// markLogSuffixDirtyLocked отмечает изменение суффикса журнала начиная с
// абсолютного индекса fromIndex. Полная замена имеет приоритет и не
// понижается до суффикса; две suffix-мутации объединяются минимумом
// индекса, чтобы сохранённый суффикс накрыл обе.
// Требует удержания cm.mu.
func (cm *ConsensusModule) markLogSuffixDirtyLocked(fromIndex int) {
	switch cm.cmState.logPersistMode {
	case logPersistRewrite:
		return
	case logPersistSuffix:
		if fromIndex < cm.cmState.logDirtyFrom {
			cm.cmState.logDirtyFrom = fromIndex
		}
	default:
		cm.cmState.logPersistMode = logPersistSuffix
		cm.cmState.logDirtyFrom = fromIndex
	}
}

// markLogRewriteDirtyLocked отмечает необходимость полной замены журнала.
// Переход в этот режим из любого другого безусловен: уплотнение и установка
// снимка удаляют префикс, не выразимый заменой суффикса.
// Требует удержания cm.mu.
func (cm *ConsensusModule) markLogRewriteDirtyLocked() {
	cm.cmState.logPersistMode = logPersistRewrite
	cm.cmState.logDirtyFrom = -1
}

// clearLogDirtyLocked закрывает грязный период журнала после успешного
// возврата операции хранилища. Требует удержания cm.mu.
func (cm *ConsensusModule) clearLogDirtyLocked() {
	cm.cmState.logPersistMode = logPersistClean
	cm.cmState.logDirtyFrom = -1
}

// persistToStorageLocked сохраняет постоянное состояние CM в cm.storage.
// Скаляры сохраняются всегда; операция журнала выполняется только при
// грязном журнале, что позволяет избежать записи журнала на каждом пульсе
// или RPC.
//
// Порядок операций важен для отказоустойчивости: currentTerm, votedFor,
// lastSnapshotIndex, lastSnapshotTerm, затем журнал. Снимок-ключи пишутся
// ДО замены журнала: окно «журнал уплотнён, а lastSnapshotIndex
// старый/отсутствует» устраняется; обратное окно («lastSnapshotIndex новее,
// журнал полный») безопасно — полный журнал консервативнее, а
// startup-restore самовосстанавливает рассинхрон (restoreFromSnapshotStore).
//
// Режим журнала выбирает операцию хранилища: suffix передаёт текущий суффикс
// от logDirtyFrom, rewrite — весь retained-журнал. Байты и число записей
// берутся из результата операции, повторного кодирования ради статистики
// нет. Грязный период журнала закрывается только после успешного возврата
// операции; при стратегии немедленного отказа успешного возврата при
// незакреплённых данных не бывает.
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
	mode := cm.cmState.logPersistMode
	cm.storage.Set(_storageKeyCurrentTerm,
		encodeScalarLocked(&cm.scalarCache.currentTerm, cm.cmState.currentTerm))
	cm.storage.Set(_storageKeyVotedFor,
		encodeScalarLocked(&cm.scalarCache.votedFor, cm.cmState.votedFor))
	cm.storage.Set(_storageKeyLastSnapshotIndex,
		encodeScalarLocked(&cm.scalarCache.lastSnapshotIndex, cm.cmState.lastSnapshotIndex))
	cm.storage.Set(_storageKeyLastSnapshotTerm,
		encodeScalarLocked(&cm.scalarCache.lastSnapshotTerm, cm.cmState.lastSnapshotTerm))

	var logLen int
	var bytesWritten, writes uint64
	if mode != logPersistClean {
		logLen = len(cm.cmState.log)
		switch mode {
		case logPersistSuffix:
			from := cm.cmState.logDirtyFrom
			pos := cm.logPositionLocked(from)
			result := cm.storage.StoreLogEntries(from, cm.cmState.log[pos:])
			bytesWritten, writes = result.BytesWritten, result.Writes
		case logPersistRewrite:
			result := cm.storage.RewriteLog(cm.cmState.log)
			bytesWritten, writes = result.BytesWritten, result.Writes
		}
		// Период грязного журнала закрывается после успешного возврата
		// операции и до прежнего сброса режима: возраст считается до этого
		// момента, ожидание — от первой отметки до входа в сохранение.
		cm.dirty.completeAt(start, time.Now())
		cm.clearLogDirtyLocked()
	}

	elapsed := time.Since(start)
	cm.persistence.observe(source, mode != logPersistClean, elapsed, logLen, bytesWritten, writes)
	if traceEnabled(_traceLevelProgress) {
		cm.traceLogfLocked("persistToStorage elapsed %s", elapsed)
	}
}

// loadLogForRestore читает сохранённый журнал операцией хранилища и
// переводит маркерную ошибку отсутствия журнала в ошибку с прежним текстом
// «log not found» и сохранённой причиной. Существующий пустой журнал даёт
// пустой срез и nil: он отличается от отсутствующего. Выделено в отдельную
// функцию, чтобы путь отказа был проверяем без завершения процесса.
func (cm *ConsensusModule) loadLogForRestore() ([]LogEntry, error) {
	entries, err := cm.storage.LoadLog()
	if errors.Is(err, contract.ErrLogNotFound) {
		return nil, fmt.Errorf("log not found in storage: %w", err)
	}
	return entries, err
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
	// Журнал читается операцией хранилища. Отсутствующий журнал при
	// HasData=true — прежний отказ старта с прежним смыслом «log not
	// found»; существующий пустой журнал допустим. Ошибка чтения или
	// декодирования не маскируется отсутствием.
	logEntries, err := cm.loadLogForRestore()
	if err != nil {
		log.Fatal(err)
	}
	cm.cmState.log = logEntries
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
		// Точка грязи чиста (сброшена после restoreFromStorage), поэтому
		// журнал не переписывается — только дешёвые скалярные ключи.
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
