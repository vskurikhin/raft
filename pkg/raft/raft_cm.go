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

	// wg — регистрация всех горутин CM (дисциплина goSpawn/goSpawnLocked,
	// ADR-003 п.1). Регистрация выполняется атомарно с проверкой
	// «узел не остановлен» под cm.mu; Stop() присоединяет все горутины
	// через wg.Wait().
	wg sync.WaitGroup

	// stopOnce — постусловие идемпотентного Stop(): любой возврат из Stop()
	// (включая повторный/параллельный) гарантирует завершение всех горутин
	// CM (ADR-003 п.5). sync.Once.Do блокирует параллельных вызывающих до
	// завершения первого вызова.
	stopOnce sync.Once

	// shutdownClosed — true после установки state=Dead в Stop(). Пишется
	// и читается только под cm.mu; дублирует проверку state==Dead для
	// goSpawnLocked (закрытие shutdownCh происходит вне cm.mu, поэтому
	// одним каналом проверить «остановлен» атомарно нельзя).
	shutdownClosed bool

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

	// latency — агрегаты латентности этого CM. Нулевое значение готово к
	// использованию; инициализация в конструкторе не требуется (INV-M6).
	// Собственного мьютекса нет: поля — atomic.Int64, наблюдение корректно
	// из любого контекста, включая удержание cm.mu (INV-M3, ADR-P06-002).
	latency cmLatency

	// counters — счётчики ключевых событий репликации и снапшотов
	// (ADR-P07-008). Нулевое значение готово к использованию (INV-M6);
	// per-peer карты защищены cm.mu, глобальные поля — atomic.Int64.
	counters raftCounters
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
	// все ожидающие verifyFuture (при условии, что ответ принадлежит
	// AE, отправленному после постановки запроса — см. verifyEpoch).
	pendingVerify []*verifyFuture

	// verifyEpoch — монотонный счётчик раундов верификации ReadIndex.
	// Инкрементируется в leaderLoop при обработке каждого нового
	// verify-запроса; snapshot значения снимается в leaderSendAEs и
	// передаётся в горутину leaderSendAEsToPeer (dispatchEpoch).
	// Голос засчитывается только при vf.epoch <= dispatchEpoch, то
	// есть кворум собирается из ответов на AE, отправленные не раньше
	// постановки запроса (SA-016). Защищено cm.mu.
	verifyEpoch uint64

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

	// replFailures — число подряд идущих транспортных ошибок по пиру
	// (ADR-P07-006). Владелец — leaderState; жизненный цикл строго следует
	// роли Leader: карты создаются в startLeader и уничтожаются при
	// выходе из роли (becomeFollower).
	replFailures map[int]int

	// lastAttempt — момент последней реальной попытки отправки RPC
	// каждому пиру (ADR-P07-006 п.3): backoff реализуется сравнением
	// времени, а не time.Sleep — sleep удержал бы inflightAE[peer]
	// и задерживал восстановление узла.
	lastAttempt map[int]time.Time
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
//
// Постусловие (ADR-003 п.5): любой возврат из Stop() — включая повторный
// и параллельный вызов — гарантирует, что ни одна горутина CM не
// выполняется. Реализация: sync.Once-обёртка вокруг {state=Dead под cm.mu;
// close(shutdownCh); wg.Wait() вне cm.mu}; раннего возврата без wg.Wait()
// нет. sync.Once.Do блокирует параллельных вызывающих до завершения
// первого вызова, поэтому постусловие выполняется и для них.
func (cm *ConsensusModule) Stop() {
	cm.stopOnce.Do(func() {
		cm.mu.Lock()
		cm.cmState.state = Dead
		cm.shutdownClosed = true
		cm.mu.Unlock()

		cm.traceLogf(0, "CM.Stop called / becomes Dead")
		close(cm.shutdownCh)
		cm.wg.Wait()
	})
}

// goSpawnLocked запускает fn в горутине, зарегистрированной в cm.wg.
// Вызывается при УЖЕ удерживаемой cm.mu (площадки becomeFollower,
// startElection, startLeader, leaderSendAEs — ADR-003 п.1, разметка
// площадок). Контракт: пара «проверка Dead/shutdownClosed + wg.Add»
// атомарна относительно state=Dead под cm.mu — поэтому wg.Add не может
// произойти после начала wg.Wait() в Stop().
//
// Запрет R-N1: НЕ допускается временное освобождение cm.mu внутри
// вызывающих ради unlocked-формы — это разорвало бы атомарность
// перехода состояния и открыло окно для конкурирующего Stop().
func (cm *ConsensusModule) goSpawnLocked(fn func()) {
	if cm.cmState.state == Dead || cm.shutdownClosed {
		return // узел останавливается: горутина не стартует
	}
	cm.wg.Add(1)
	go func() {
		defer cm.wg.Done()
		fn()
	}()
}

// goSpawn — то же, что goSpawnLocked, для вызывающих, НЕ удерживающих
// cm.mu (конструктор, runPreCandidate, catch-up передачи лидерства,
// post-commit горутина конфигурации). Самостоятельно захватывает cm.mu.
// Не вызывает пользовательский код под блокировкой (только проверка,
// Add и go).
func (cm *ConsensusModule) goSpawn(fn func()) {
	cm.mu.Lock()
	cm.goSpawnLocked(fn)
	cm.mu.Unlock()
}

// debugLogf выводит отладочное сообщение.
//
//nolint:unused
func (cm *ConsensusModule) debugLogf(format string, args ...any) {
	if slog.Default().Enabled(context.Background(), slog.LevelDebug) {
		format = fmt.Sprintf("[%c,N:%d,T:%03d] ", stateLetter(cm.cmState.state), cm.id, cm.cmState.currentTerm) +
			format
		slog.Debug(fmt.Sprintf(format, args...))
	}
}

func (cm *ConsensusModule) stdoutTracePrintln(msg string) {
	cm.mu.Lock()
	_, _ = fmt.Printf("[%c,N:%d,T:%03d] %s\n", stateLetter(cm.cmState.state), cm.id, cm.cmState.currentTerm, msg)
	cm.mu.Unlock()
}

// traceLockedLogf выводит отладочное сообщение, если TraceCM > level.
// Ожидается, что cm.mu уже заблокирован вызывающим кодом, поэтому
// состояние (cm.cmState.state, cm.id, cm.cmState.currentTerm) читается напрямую.
func (cm *ConsensusModule) traceLockedLogf(level int, format string, args ...any) {
	if level < TraceCM {
		format = fmt.Sprintf("[%c,N:%d,T:%03d] ", stateLetter(cm.cmState.state), cm.id, cm.cmState.currentTerm) +
			format
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
