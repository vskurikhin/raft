package raft

import (
	"errors"
	"io"
	"sync"
	"time"
)

var (
	ErrNotLeader                    = errors.New("raft: not leader")
	ErrLeadershipLost               = errors.New("raft: leadership lost while committing")
	ErrRaftShutdown                 = errors.New("raft: raft is shutdown")
	ErrEnqueueTimeout               = errors.New("raft: timeout enqueuing operation")
	ErrUnsupportedProtocol          = errors.New("raft: unsupported protocol version")
	ErrLeadershipTransferInProgress = errors.New("raft: leadership transfer in progress")

	// ErrNothingNewToSnapshot возвращается, когда нет новых закоммиченных
	// записей для создания снэпшота.
	ErrNothingNewToSnapshot = errors.New("raft: nothing new to snapshot")

	// ErrAbortedByRestore возвращается лидером, когда операция прервана
	// восстановлением из снэпшота.
	ErrAbortedByRestore = errors.New("raft: snapshot restored while committing log")
)

// Future представляет асинхронную операцию Raft.
type Future interface {
	Error() error
}

// IndexFuture — Future, возвращающая индекс записи в журнале.
type IndexFuture interface {
	Future
	Index() int
}

// ApplyFuture — Future для Apply-операции.
type ApplyFuture interface {
	IndexFuture
	Response() any
}

// ConfigurationFuture — Future, возвращающая конфигурацию кластера.
type ConfigurationFuture interface {
	IndexFuture
	Configuration() Configuration
}

// deferError — базовая реализация Future с каналом завершения.
type deferError struct {
	err        error
	errCh      chan error
	once       sync.Once
	responded  bool
	shutdownCh chan struct{}
}

func (d *deferError) init(shutdownCh chan struct{}) {
	d.errCh = make(chan error, 1)
	d.shutdownCh = shutdownCh
}

func (d *deferError) Error() error {
	d.once.Do(func() {
		select {
		case d.err = <-d.errCh:
		case <-d.shutdownCh:
			d.err = ErrRaftShutdown
		}
	})
	return d.err
}

func (d *deferError) respond(err error) {
	if d.errCh == nil || d.responded {
		return
	}
	d.errCh <- err
	close(d.errCh)
	d.responded = true
}

// logFuture — future для одной записи журнала.
type logFuture struct {
	deferError
	log      LogEntry
	response any
	dispatch time.Time
}

func (l *logFuture) Index() int {
	return l.log.Index
}

func (l *logFuture) Response() any {
	return l.response
}

// configurationsFuture — future для GetConfiguration.
type configurationsFuture struct {
	deferError
	config Configuration
}

func (f *configurationsFuture) Index() int {
	return 0
}

func (f *configurationsFuture) Configuration() Configuration {
	return f.config
}

// LeadershipTransferFuture используется для ожидания результата
// передачи лидерства от одного узла другому.
type LeadershipTransferFuture interface {
	Future
}

// leadershipTransferFuture — future для отслеживания передачи лидерства.
type leadershipTransferFuture struct {
	deferError
	targetID ServerID
}

// errorFuture — future с заранее известной ошибкой.
type errorFuture struct {
	err error
}

func (e errorFuture) Error() error                 { return e.err }
func (e errorFuture) Index() int                   { return 0 }
func (e errorFuture) Response() any                { return nil }
func (e errorFuture) Configuration() Configuration { return Configuration{} }

// reqSnapshotFuture — внутренний future для запроса снэпшота у runFSM.
type reqSnapshotFuture struct {
	deferError
	index    int
	term     int
	snapshot FSMSnapshot
}

// restoreFuture — future для восстановления из снэпшота на runFSM.
type restoreFuture struct {
	deferError
	meta   *SnapshotMeta
	reader io.ReadCloser
}

// SnapshotFuture — публичный future для пользовательского Snapshot().
type SnapshotFuture struct {
	deferError
	index int
}
