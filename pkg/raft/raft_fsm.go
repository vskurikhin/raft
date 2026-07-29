package raft

import (
	"io"
)

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

// FSMSnapshot — заглушка (будет реализована в Snapshot feature).
type FSMSnapshot interface {
	Persist(SnapshotSink) error
	Release()
}

// SnapshotSink — заглушка (будет реализована в Snapshot feature).
type SnapshotSink interface {
	io.WriteCloser
	ID() string
	Cancel() error
}

// SnapshotMeta — заглушка (будет реализована в Snapshot feature).
type SnapshotMeta struct {
	Version       int
	ID            string
	Index         int
	Term          int
	Configuration []byte
	ConfigIndex   int
	Size          int64
}
