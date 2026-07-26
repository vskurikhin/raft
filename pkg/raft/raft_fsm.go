package raft

// FSM — интерфейс машины состояний клиента Raft.
// Все закоммиченные записи журнала применяются к FSM через Apply.
type FSM interface {
	Apply(*LogEntry) any
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
		Command: log.Command,
		Index:   log.Index,
		Term:    log.Term,
	}
	return nil
}

// BatchingFSM — опциональное расширение FSM для группового применения
// записей журнала. Если FSM клиента реализует этот интерфейс, runFSM
// вызывает ApplyBatch вместо последовательных вызовов Apply.
type BatchingFSM interface {
	FSM

	// ApplyBatch получает срез закоммиченных записей журнала и применяет
	// их к FSM за один вызов. Длина возвращаемого среза должна совпадать
	// с длиной входного — каждый ответ соответствует записи с тем же
	// индексом. Нарушение контракта вызывает panic в runFSM.
	ApplyBatch([]*LogEntry) []any
}
