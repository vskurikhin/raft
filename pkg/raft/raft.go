package raft

import (
	"log"
	"sync/atomic"
	"time"
)

const (
	// ProtocolVersion — версия протокола Raft, используемая данной реализацией.
	// Значение 3. Совместимость с версиями 0, 1 и 2 не поддерживается.
	ProtocolVersion = 3

	// Quantum — множитель для всех Raft-таймеров.
	// TCP RPC timeout (TcpRpcTimeoutMs) задаётся независимо.
	// Для тестового ускорения Quantum не меняется — используются
	// test-only хуки (RAFT_FORCE_MORE_REELECTION и аналоги).
	Quantum             = 2
	HeartbeatTimeoutMs  = 23 * Quantum
	ReelectionTimeoutMs = 127 * Quantum
	TickerTimeoutMs     = 11 * Quantum

	// TcpRpcTimeoutMs — таймаут TCP RPC (не зависит от Quantum), используется
	// в production-транспорте. Исходное значение 85ms получено из 254/3 при
	// Quantum=2. Вынесено в независимую константу, чтобы изменение Quantum
	// для ускорения тестов не влияло на production-развёртывание.
	TcpRpcTimeoutMs = 85

	// leaderBatchSize — количество записей, которые лидер собирает из applyCh
	// перед одним вызовом dispatchLogs. Компромисс между latency (одиночные
	// записи) и throughput (групповой commit, Raft §5.1).
	leaderBatchSize = 64

	// maxApplyBatchSize — максимальное количество записей в одном батче,
	// отправляемом в fsmMutateCh. Если commitIndex - lastApplied превышает
	// этот порог, processLogs делит диапазон на под-батчи.
	maxApplyBatchSize = 128

	// applyBatchInterval — интервал, с которым runApplyLoop проверяет
	// необходимость применения записей к FSM. Накопление commitCh
	// уведомлений за этот интервал позволяет объединять несколько
	// мелких коммитов в один батч.
	applyBatchInterval = 50 * time.Millisecond

	// maxSnapshotDataSize — максимальный допустимый размер данных снэпшота
	// в байтах. Значение 1 ГБ выбрано как разумный предел для production
	// (больше типичного размера состояния, но достаточно для обнаружения
	// некорректного DataSize в запросе InstallSnapshot).
	// Установлено в 1 << 30 для согласованности.
	// Защита от паники io.Copy при повреждённом DataSize.
	maxSnapshotDataSize = 1 << 30
)

// TraceCM — порог детализации отладочных сообщений консенсус-модуля
// (traceLogf, traceLogLocked). Сообщение с уровнем level выводится,
// если TraceCM > level. Значение задаётся флагом --trace-log-level
// командной строки, по умолчанию 1.
var TraceCM = 1

// CommitEntry — это данные, которые Raft отправляет в канал фиксации.
// Каждая запись фиксации уведомляет клиента о том, что консенсус по команде
// был достигнут и эта команда может быть применена к машине состояний клиента.
type CommitEntry struct {
	// Data — это команда клиента, которая была зафиксирована.
	Data any

	// Index — это индекс журнала, по которому была зафиксирована команда клиента.
	Index int

	// Term — это терм Raft, в котором была зафиксирована команда клиента.
	Term int
}

type CMState int

const (
	Follower CMState = iota
	PreCandidate
	Candidate
	Leader
	Dead
)

func (s CMState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case PreCandidate:
		return "PreCandidate"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Dead:
		return "Dead"
	default:
		return "unreachable"
	}
}

// LogType определяет тип записи журнала.
type LogType int

const (
	LogCommand LogType = iota
	LogNoop
	LogConfiguration
)

// batchApplyBuffer — ёмкость fsmMutateCh для burst-устойчивости.
// Выбрана как 1024: при массовом коммите processLogs не блокируется.
const batchApplyBuffer = 1024

// defaultSnapshotThreshold — минимальное количество записей после
// последнего снэпшота, при котором создаётся новый снэпшот.
// Значение 1024 выбрано как компромисс: частые снэпшоты создают
// нагрузку на FSM.Snapshot(), редкие — увеличивают время восстановления
// и объём журнала, который нужно передавать отстающим узлам.
const defaultSnapshotThreshold = 1024

// defaultSnapshotInterval — интервал проверки необходимости снэпшота.
// Каждые 3 секунды runSnapshots проверяет, не превышен ли порог
// defaultSnapshotThreshold.
const defaultSnapshotInterval = 3 * time.Second

// defaultTakeSnapshotTimeout — таймаут отправки запроса на снэпшот
// в fsmSnapshotCh. Если runFSM не принимает запрос за это время,
// takeSnapshot возвращает ошибку вместо вечной блокировки.
// Значение 30 секунд выбрано как разумный максимум для создания
// снэпшота в production (запись состояния + persist на диск).
const defaultTakeSnapshotTimeout = 30 * time.Second

// defaultTrailingLogs — количество записей журнала, сохраняемых
// после самого свежего снэпшота. Нужно для поддержки репликации
// без отправки полного снэпшота каждому новому follower.
// Значение 128 — разумный минимум для большинства сценариев.
const defaultTrailingLogs = 128

type LogEntry struct {
	Index int
	Term  int
	Type  LogType
	Data  any
}

// commitTuple связывает запись журнала с future для отправки в FSM.
type commitTuple struct {
	log    *LogEntry
	future *logFuture
}

// configurationChangeFuture — future для изменения конфигурации кластера.
type configurationChangeFuture struct {
	deferError
	req   configurationChangeRequest
	index int
}

func (f *configurationChangeFuture) Index() int {
	return f.index
}

// NewConsensusModule создаёт новый экземпляр CM.
// fsm — машина состояний клиента, к которой применяются закоммиченные записи.
// ready уведомляет CM, что все соседи подключены.
// snapshots — хранилище снэпшотов; если nil, снэпшоты отключены.
//
// Предусловие: transport не должен быть nil. Нарушение контракта приводит к
// panic при инициализации (fail-fast) — это единственное место, где проверка
// выполняется до старта горутин. Паника допустима только здесь (исключение
// для инициализации по AGENTS.md), а не в рабочих горутинах. Проверка
// обнаруживает как «чистый» nil-интерфейс, так и типизированный nil-указатель
// (например, (*InmemTransport)(nil)) — см. isNilInterface.
func NewConsensusModule(
	id int,
	peerIds []int,
	transport Transport,
	storage Storage,
	fsm FSM,
	ready <-chan any,
	snapshots ...SnapshotStore,
) *ConsensusModule {
	// Fail-fast проверка предусловия конструктора. Паника допустима в рамках
	// исключения "инициализация программы": вызов происходит при старте узла,
	// а не в горутине, поэтому распознать ошибку проще до создания объекта.
	// isNilInterface дополнительно ловит типизированный nil: в Go интерфейс
	// не равен nil, если в него положен nil-указатель (например,
	// (*InmemTransport)(nil)) — прямое сравнение с nil такую ситуацию
	// не обнаружило бы.
	if isNilInterface(transport) {
		log.Fatalln("raft: NewConsensusModule: transport is nil")
	}
	cm := new(ConsensusModule)
	cm.id = id
	cm.peerIds = peerIds
	cm.transport = transport
	cm.storage = storage
	cm.fsm = fsm
	// fsmMutateCh — канал для отправки закоммиченных записей в runFSM.
	// Ёмкость 1024 обеспечивает burst-устойчивость: при массовом коммите
	// processLogs отправляет все записи одним батчем без блокировки.
	cm.fsmMutateCh = make(chan []*commitTuple, batchApplyBuffer)
	cm.shutdownCh = make(chan struct{})
	cm.applyCh = make(chan *logFuture)
	// verifyCh — канал для ReadIndex-подтверждения лидерства.
	// Ёмкость 64 выбрана для burst: при 10k RPS VerifyLeader не блокируется.
	cm.verifyCh = make(chan *verifyFuture, 64)
	cm.leaderState.inflight = make(map[int]*logFuture)
	cm.commitCh = make(chan int, 1)
	cm.stepDown = make(chan struct{}, 1)
	cm.confChangeCh = make(chan *configurationChangeFuture, 1)
	cm.cmState.state = Follower
	cm.cmState.votedFor = -1
	cm.cmState.commitIndex = -1
	cm.cmState.lastApplied = -1
	cm.leaderState.leaderStartIndex = -1
	cm.cmState.lastLogIndex = -1
	cm.cmState.lastLogTerm = -1
	cm.leaderState.nextIndex = make(map[int]int)
	cm.leaderState.matchIndex = make(map[int]int)
	cm.leaderState.inflightAE = make(map[int]*atomic.Bool)
	cm.cmState.termIndexMap = make(map[int]int)
	cm.cmState.electionTimerDone = make(chan struct{})
	cm.preVoteDisabled = false
	cm.cmState.leaderLastContact = time.Time{}
	cm.cmState.leaderID = -1
	cm.leaderState.leadershipTransferCh = make(chan *leadershipTransferFuture, 1)
	cm.cmState.logNeedsPersist = true

	// Инициализация snapshot-полей.
	cm.cmState.lastSnapshotIndex = -1
	cm.cmState.lastSnapshotTerm = -1
	if len(snapshots) > 0 && snapshots[0] != nil {
		cm.snapshotStore = snapshots[0]
		cm.snapshotThreshold = defaultSnapshotThreshold
		cm.snapshotInterval = defaultSnapshotInterval
		cm.trailingLogs = defaultTrailingLogs
		cm.snapshotCh = make(chan struct{}, 1)
		cm.fsmSnapshotCh = make(chan *reqSnapshotFuture)
	}

	if cm.storage.HasData() {
		cm.restoreFromStorage()
		cm.cmState.logNeedsPersist = false
		// Восстановление FSM из снэпшота до запуска горутин (однопоточно).
		// Fail-fast: старт с невосстановимым durable-состоянием недопустим —
		// громкий отказ узла предпочтительнее молчаливой потери
		// подтверждённых данных (см. ADR-002).
		if err := cm.restoreFromSnapshotStore(); err != nil {
			log.Fatalf("raft: startup snapshot restore failed: %v", err)
		}
	}

	// Установить начальную конфигурацию, если она ещё не задана
	// (первый запуск или хранилище не содержит данных).
	if len(cm.cmState.configurations.latest.ConfigServers) == 0 {
		cm.setInitialConfiguration()
	}

	go cm.runFSM()
	go cm.runRPCReader()

	if cm.snapshotStore != nil {
		go cm.runSnapshots()
	}

	go func() {
		<-ready
		cm.mu.Lock()
		cm.cmState.electionResetEvent = time.Now()
		cm.mu.Unlock()
		cm.runElectionTimer()
	}()
	go cm.stats(cm.shutdownCh)
	return cm
}

// RPCHeader — общий заголовок для всех RPC-сообщений Raft.
// Встраивается во все структуры аргументов и ответов RPC.
// Содержит версию протокола и идентификатор узла-отправителя.
type RPCHeader struct {
	ProtocolVersion int
	ServerID        int
}

// WithRPCHeader — интерфейс, позволяющий получить RPCHeader из RPC-сообщения.
type WithRPCHeader interface {
	GetRPCHeader() RPCHeader
}

// checkRPCHeader проверяет, что RPC-сообщение использует поддерживаемую
// версию протокола. Для v3-only возвращает nil только при ProtocolVersion == 3.
func checkRPCHeader(rpc WithRPCHeader) error {
	header := rpc.GetRPCHeader()
	if header.ProtocolVersion != ProtocolVersion {
		return ErrUnsupportedProtocol
	}

	return nil
}

// RequestVoteArgs См. рисунок 2 в статье.
type RequestVoteArgs struct {
	RPCHeader
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
	// LeadershipTransfer — true, если запрос голоса вызван TimeoutNow.
	// Позволяет bypass проверку "есть лидер" в RequestVote handler.
	LeadershipTransfer bool
}

func (r *RequestVoteArgs) GetRPCHeader() RPCHeader {
	return r.RPCHeader
}

type RequestVoteReply struct {
	RPCHeader
	Term        int
	VoteGranted bool
}

func (r *RequestVoteReply) GetRPCHeader() RPCHeader {
	return r.RPCHeader
}

// RequestPreVoteArgs — аргументы RPC предварительного голосования (§4 Pre-Vote).
// PreVote НЕ увеличивает currentTerm, НЕ меняет votedFor, НЕ персистится.
// Получатель проверяет только актуальность лога (log-safety) и наличие активного лидера.
type RequestPreVoteArgs struct {
	RPCHeader
	Term         int
	LastLogIndex int
	LastLogTerm  int
}

func (r *RequestPreVoteArgs) GetRPCHeader() RPCHeader {
	return r.RPCHeader
}

// RequestPreVoteReply — ответ на RequestPreVote RPC.
type RequestPreVoteReply struct {
	RPCHeader
	Term        int
	VoteGranted bool
}

func (r *RequestPreVoteReply) GetRPCHeader() RPCHeader {
	return r.RPCHeader
}

// AppendEntriesArgs См. рисунок 2 в статье.
type AppendEntriesArgs struct {
	RPCHeader
	Term     int
	LeaderID int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

func (r *AppendEntriesArgs) GetRPCHeader() RPCHeader {
	return r.RPCHeader
}

type AppendEntriesReply struct {
	RPCHeader
	Term    int
	Success bool

	// Faster conflict resolution optimization (described near the end of section
	// 5.3 in the paper.)
	ConflictIndex int
	ConflictTerm  int
}

func (r *AppendEntriesReply) GetRPCHeader() RPCHeader {
	return r.RPCHeader
}

// quorumSize возвращает минимальное количество голосов для достижения кворума.
func quorumSize(voterCount int) int {
	return voterCount/2 + 1
}
