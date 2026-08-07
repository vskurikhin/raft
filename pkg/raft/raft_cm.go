package raft

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// ConsensusModule (CM) реализует единый узел консенсуса Raft.
type ConsensusModule struct {
	mu sync.Mutex

	// id — идентификатор сервера этого экземпляра CM (ConsensusModule).
	id int
	// peerIds содержит список идентификаторов узлов-соседей в кластере.
	peerIds []int
	// storage is used to persist state.
	storage Storage

	transport Transport

	fsm         FSM
	fsmMutateCh chan []*commitTuple
	shutdownCh  chan struct{}

	applyCh  chan *logFuture
	verifyCh chan *verifyFuture

	commitCh     chan int
	stepDown     chan struct{}
	confChangeCh chan *configurationChangeFuture

	// preVoteDisabled отключает механизм Pre-Vote.
	// Если true, выборы начинаются сразу (как в классическом Raft).
	// По умолчанию false — Pre-Vote включён.
	preVoteDisabled bool

	// snapshotStore — хранилище снэпшотов. Если nil, снэпшоты отключены.
	snapshotStore SnapshotStore

	// snapshotInterval — интервал проверки необходимости снэпшота.
	snapshotInterval time.Duration

	// snapshotThreshold — минимальное количество записей после
	// последнего снэпшота для создания нового.
	snapshotThreshold int

	// trailingLogs — количество записей журнала, сохраняемых после
	// последнего снэпшота.
	trailingLogs int

	// snapshotCh — канал для сигнала runSnapshots о необходимости
	// проверки создания снэпшота. Буферизирован (cap=1).
	snapshotCh chan struct{}

	// fsmSnapshotCh — небуферизированный канал для запроса снэпшота у runFSM.
	fsmSnapshotCh chan *reqSnapshotFuture

	// cmState — состояние узла Raft (persistent + volatile + election + log + config).
	// Все поля защищены cm.mu (ADR-002); cmState не содержит собственного mutex.
	cmState cmState

	// leaderState — состояние, используемое только пока узел является лидером.
	// Все поля защищены cm.mu (ADR-002); leaderState не содержит собственного mutex.
	// В отличие от HashiCorp, доступен из per-peer replication goroutines и
	// apply-пути под общим cm.mu (ADR-003). Всегда инициализирован (не nil),
	// даже на follower — иначе sendBatch упадёт (RISK-5).
	leaderState leaderState
}

// leaderState — состояние, используемое только пока узел является лидером.
// Не содержит собственного mutex: все поля защищены cm.mu в ConsensusModule (ADR-002).
// В отличие от HashiCorp, доступен из per-peer replication goroutines и apply-пути
// под общим cm.mu (ADR-003). Всегда инициализирован (не nil), даже на follower —
// иначе sendBatch упадёт (RISK-5).
type leaderState struct {
	// nextIndex — индекс следующей записи журнала для отправки каждому peer.
	nextIndex map[int]int

	// matchIndex — индекс самой новой записи, о которой известно, что она
	// реплицирована на каждом peer.
	matchIndex map[int]int

	// inflightAE — per-peer флаги выполнения горутин AppendEntries.
	//
	// Используется для дедупликации в leaderSendAEs: если для данного peer
	// уже выполняется горутина leaderSendAEsToPeer, новый вызов leaderSendAEs
	// не порождает дополнительную горутину. Это предотвращает:
	//   - Гонку nextIndex (устаревший AppendEntries reply выбрасывается)
	//   - Экспоненциальный рост числа горутин при частых heartbeat
	//
	// Доступ к флагам — через atomic.CompareAndSwap без захвата cm.mu,
	// что минимизирует блокировки в leaderSendAEs.
	//
	// Инвариант:
	//   inflightAE[peerID] == nil   — peer не в конфигурации
	//   inflightAE[peerID] == false — нет активной горутины для peer
	//   inflightAE[peerID] == true  — горутина leaderSendAEsToPeer выполняется
	//
	// Сброс флага, производится:
	//   - В конце leaderSendAEsToPeer (defer)
	//   - При выходе из runLeaderLoop (блок defer)
	//   - При вызове becomeFollower
	inflightAE map[int]*atomic.Bool

	// commitmentTracker — продвижение commitIndex на лидере.
	commitmentTracker *commitmentTracker

	// inflight — карта future по индексу записи в журнале.
	// Обеспечивает O(1) поиск при отправке батча в FSM.
	inflight map[int]*logFuture

	// pendingVerify — очередь verifyFuture, ожидающих подтверждения
	// от follower-ов. Каждый успешный AppendEntries ответ голосует за
	// все ожидающие verifyFuture.
	pendingVerify []*verifyFuture

	// leaderStartIndex — первый индекс текущего терма лидера.
	leaderStartIndex int

	// leadershipTransferCh — канал для запросов на передачу лидерства.
	// Запросы обрабатываются в leaderLoop.
	leadershipTransferCh chan *leadershipTransferFuture

	// leadershipTransferInProgress — atomic-флаг, блокирующий Apply
	// во время передачи лидерства.
	leadershipTransferInProgress int32

	// leadershipTransferFuture — текущий future передачи лидерства.
	// Устанавливается в handleLeadershipTransfer, отвечается в stepDown.
	leadershipTransferFuture *leadershipTransferFuture
}

// cmState — состояние узла Raft (аналог raftState в HashiCorp state.go).
// Не содержит собственного mutex: все поля защищены cm.mu в ConsensusModule (ADR-002).
type cmState struct {
	// Постоянное состояние Raft на всех серверах
	currentTerm     int
	votedFor        int
	log             []LogEntry
	logNeedsPersist bool

	// lastLogIndex — кэш индекса последней записи в журнале.
	// Обновляется через setLastLog при любом изменении журнала.
	lastLogIndex int
	// lastLogTerm — кэш терма последней записи в журнале.
	lastLogTerm int

	// Непостоянное состояние Raft на всех серверах
	commitIndex        int
	lastApplied        int
	state              CMState
	electionResetEvent time.Time
	electionTimerDone  chan struct{}

	// leaderLastContact — время последнего успешного AppendEntries от лидера.
	// Используется в RequestPreVote для проверки: если недавно был контакт с
	// лидером, Pre-Vote отклоняется.
	leaderLastContact time.Time

	// leaderID — ID текущего лидера (если известен).
	// Устанавливается при получении AppendEntries от лидера.
	leaderID int

	// candidateFromLeadershipTransfer — true, если данный узел стал
	// кандидатом из-за получения TimeoutNow. Влияет на:
	//   — пропуск PreVote в runElectionTimer
	//   — обработку RequestVote (голосуем даже при известном лидере)
	//   — обработку AppendEntries (не step-down от старого лидера)
	candidateFromLeadershipTransfer atomic.Bool

	// lastSnapshotIndex — индекс последнего созданного снэпшота.
	// Инициализируется как -1 (нет снэпшота).
	lastSnapshotIndex int

	// lastSnapshotTerm — терм последнего созданного снэпшота.
	lastSnapshotTerm int

	// termIndexMap — карта term → последний LogEntry.Index с этим term.
	// O(1) lookup для ConflictTerm вместо O(n) slices.Backward(cm.cmState.log).
	// Инкрементально обновляется в dispatchLogsUnsafe; перестраивается
	// целиком при обрезке/компактировании/замене журнала.
	// Требует удержания cm.mu (Lock) при чтении и записи.
	termIndexMap map[int]int

	// configurations — текущие конфигурации кластера (committed/latest).
	configurations configurations
}

// GetConfiguration возвращает текущую конфигурацию кластера.
// Для лидера возвращает latest конфигурацию; для follower — committed.
func (cm *ConsensusModule) GetConfiguration() ConfigurationFuture {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return &configurationsFuture{
		config: cm.cmState.configurations.latest,
	}
}

// Report отчет о состоянии данного CM.
func (cm *ConsensusModule) Report() (id, term int, isLeader bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.id, cm.cmState.currentTerm, cm.cmState.state == Leader
}

// Stop останавливает этот CM. Безопасен для многократного вызова.
func (cm *ConsensusModule) Stop() {
	cm.mu.Lock()
	if cm.cmState.state == Dead {
		cm.mu.Unlock()
		return
	}
	cm.cmState.state = Dead
	cm.mu.Unlock()

	cm.traceLogf(0, "CM.Stop called / becomes Dead")
	close(cm.shutdownCh)
}

// debugLogf выводит отладочное сообщение.
func (cm *ConsensusModule) debugLogf(format string, args ...any) {
	if slog.Default().Enabled(context.Background(), slog.LevelDebug) {
		format = fmt.Sprintf("[%c,N:%d,T:%03d] ", stateLetter(cm.cmState.state), cm.id, cm.cmState.currentTerm) + format
		slog.Debug(fmt.Sprintf(format, args...))
	}
}

// traceLockedLogf выводит отладочное сообщение, если TraceCM > level.
// Ожидается, что cm.mu уже заблокирован вызывающим кодом, поэтому
// состояние (cm.cmState.state, cm.id, cm.cmState.currentTerm) читается напрямую.
func (cm *ConsensusModule) traceLockedLogf(level int, format string, args ...any) {
	if level < TraceCM {
		format = fmt.Sprintf("[%c,N:%d,T:%03d] ", stateLetter(cm.cmState.state), cm.id, cm.cmState.currentTerm) + format
		_traceLogger.Printf(format, args...)
	}
}

// traceLogf — потокобезопасная обёртка над traceLockedLogf для вызовов
// БЕЗ удержания cm.mu: самостоятельно захватывает блокировку, чтобы
// прочитать состояние без data race. Для вызовов из кода, который уже
// держит cm.mu, используйте traceLockedLogf — иначе будет deadlock.
func (cm *ConsensusModule) traceLogf(level int, format string, args ...any) {
	if level < TraceCM {
		cm.mu.Lock()
		cm.traceLockedLogf(level, format, args...)
		cm.mu.Unlock()
	}
}

func stateLetter(s CMState) rune {
	switch s {
	case Follower:
		return 'F'
	case Leader:
		return 'L'
	case Candidate:
		return 'C'
	default:
		return '?'
	}
}
