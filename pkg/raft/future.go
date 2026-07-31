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

	// ErrBatchFSMResponseMismatch — нарушение контракта BatchingFSM:
	// ApplyBatch вернул число ответов, не совпадающее с числом записей
	// журнала в батче. Возвращается клиентам через ApplyFuture.Error();
	// не роняет горутину runFSM.
	ErrBatchFSMResponseMismatch = errors.New("raft: ApplyBatch response count mismatch")
)

// Future представляет асинхронную операцию Raft.
type Future interface {
	Error() error
	// ErrorCh возвращает канал, по которому приходит ошибка выполнения.
	// Позволяет использовать select для ожидания без создания го-рутины.
	ErrorCh() <-chan error
}

// verifyFuture — Future для ReadIndex-подтверждения лидерства (Raft §8).
// Отправляется в verifyCh, обрабатывается в runLeaderLoop.
// После получения кворумного подтверждения от большинства узлов
// respond(nil). При потере лидерства respond(ErrNotLeader).
type verifyFuture struct {
	deferError
	quorumSize int
	votes      int
	notifyCh   chan *verifyFuture
}

// vote учитывает голос follower при подтверждении лидерства.
// Если leader == false, future немедленно завершается с ErrNotLeader.
// Если votes >= quorumSize, future завершается с nil.
// Потокобезопасен.
func (v *verifyFuture) vote(leader bool) {
	if !leader {
		v.respond(ErrNotLeader)
		return
	}
	v.votes++
	if v.votes >= v.quorumSize {
		v.respond(nil)
	}
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
	mu         sync.Mutex
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
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.errCh == nil || d.responded {
		return
	}
	d.responded = true
	d.mu.Unlock()
	d.errCh <- err
	close(d.errCh)
	d.mu.Lock()
}

// ErrorCh возвращает канал, по которому приходит ошибка выполнения future.
// Позволяет использовать select для ожидания без создания го-рутины.
func (d *deferError) ErrorCh() <-chan error {
	return d.errCh
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

func (e errorFuture) Error() error { return e.err }
func (e errorFuture) ErrorCh() <-chan error {
	ch := make(chan error, 1)
	ch <- e.err
	close(ch)
	return ch
}
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
