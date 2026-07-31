package raft

import (
	"io"
)

// FSM — интерфейс машины состояний клиента Raft.
// Все закоммиченные записи журнала применяются к FSM через Apply.
// После применения всех записей FSM может создавать снэпшоты
// через Snapshot и восстанавливаться из них через Restore.
type FSM interface {
	Apply(*LogEntry) any

	// Snapshot возвращает снимок текущего состояния FSM.
	// Должен выполняться быстро — только захват состояния.
	// Дорогая сериализация выполняется в FSMSnapshot.Persist.
	Snapshot() (FSMSnapshot, error)

	// Restore восстанавливает состояние FSM из снэпшота.
	// Должен сбросить все предыдущее состояние.
	Restore(io.ReadCloser) error
}

// FSMSnapshot — снимок состояния FSM, готовый к персистентности.
// Создаётся в FSM.Snapshot(), сериализуется в SnapshotSink через Persist.
//
// Контракт: Release вызывается всегда, даже если Persist не был вызван.
type FSMSnapshot interface {
	Persist(SnapshotSink) error
	Release()
}

// SnapshotSink — получатель данных снэпшота.
// FSM пишет сериализованное состояние через Write, завершает Close.
// При ошибке вызывает Cancel.
type SnapshotSink interface {
	io.WriteCloser
	ID() string
	Cancel() error
}

// SnapshotMeta — метаданные снэпшота.
type SnapshotMeta struct {
	Version       int
	ID            string
	Index         int
	Term          int
	Configuration Configuration
	ConfigIndex   int
	Size          int64
}

// CommitChannelFSM оборачивает chan<- CommitEntry в FSM для
// обратной совместимости с тестовой инфраструктурой.
type CommitChannelFSM struct {
	commitChan chan<- CommitEntry
}

func NewCommitChannelFSM(commitChan chan<- CommitEntry) *CommitChannelFSM {
	return &CommitChannelFSM{commitChan: commitChan}
}

func (f *CommitChannelFSM) Apply(log *LogEntry) any {
	if log.Type == LogNoop {
		return nil
	}
	f.commitChan <- CommitEntry{
		Data:  log.Data,
		Index: log.Index,
		Term:  log.Term,
	}
	return nil
}

func (f *CommitChannelFSM) Snapshot() (FSMSnapshot, error) {
	return nil, ErrNotImplemented
}

func (f *CommitChannelFSM) Restore(_ io.ReadCloser) error {
	return ErrNotImplemented
}

// BatchingFSM — опциональное расширение FSM для группового применения
// записей журнала. Если FSM клиента реализует этот интерфейс, runFSM
// вызывает ApplyBatch вместо последовательных вызовов Apply.
type BatchingFSM interface {
	FSM

	// ApplyBatch получает срез закоммиченных записей журнала и применяет
	// их к FSM за один вызов. Длина возвращаемого среза должна совпадать
	// с длиной входного — каждый ответ соответствует записи с тем же
	// индексом. Нарушение контракта приводит к ошибке
	// ErrBatchFSMResponseMismatch, которой отвечаются все future батча.
	ApplyBatch([]*LogEntry) []any
}
