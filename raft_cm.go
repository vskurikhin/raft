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

	// wg — регистрация всех горутин CM. Регистрация выполняется атомарно
	// с проверкой «узел не остановлен» под cm.mu; Stop() присоединяет
	// все горутины через wg.Wait().
	wg sync.WaitGroup

	// stopOnce — постусловие идемпотентного Stop(): любой возврат из Stop()
	// (включая повторный/параллельный) гарантирует завершение всех горутин
	// CM. sync.Once.Do блокирует параллельных вызывающих до завершения
	// первого вызова.
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
	// storage — постоянное хранилище состояния узла.
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

	// snapshotStore — хранилище снимков. Если nil, снимки отключены.
	snapshotStore SnapshotStore

	// snapshotInterval — интервал проверки необходимости снимка.
	snapshotInterval time.Duration

	// snapshotThreshold — минимальное количество записей после
	// последнего снимка для создания нового.
	snapshotThreshold int

	// trailingLogs — количество записей журнала, сохраняемых после
	// последнего снимка.
	trailingLogs int

	// snapshotCh — канал для сигнала runSnapshots о необходимости
	// проверки создания снимка. Буферизирован (cap=1).
	snapshotCh chan struct{}

	// fsmSnapshotCh — небуферизированный канал для запроса снимка у runFSM.
	fsmSnapshotCh chan *reqSnapshotFuture

	// cmState — состояние узла Raft (persistent + volatile + election + log + config).
	// Все поля защищены cm.mu; cmState не содержит собственного mutex.
	cmState cmState

	// leaderState — состояние, используемое только пока узел — лидер.
	// Все поля защищены cm.mu; собственного мьютекса нет.
	// Доступен из горутины репликации по каждому соседу и apply‑пути под cm.mu.
	// Всегда инициализирован (в т.ч. на ведомом), иначе sendBatch упадёт.
	leaderState leaderState

	// latency — агрегаты латентности CM.
	// Нулевое значение готово к использованию; инициализация не требуется.
	// Поля — atomic.Int64, безопасны для чтения из любого контекста
	// (включая под cm.mu).
	latency cmLatency

	// counters — счётчики ключевых событий репликации и снимков.
	// Нулевое значение готово к использованию;
	// карты «по каждому соседу» защищены cm.mu,
	// глобальные поля — atomic.Int64.
	counters raftCounters
}

type leaderState struct {
	// nextIndex — индекс следующей записи журнала для отправки каждому соседу.
	nextIndex map[int]int

	// matchIndex — индекс последней зафиксированной записи, реплицированной на каждом соседе.
	matchIndex map[int]int

	// inflightAE — флаги по каждому соседу активных горутин AppendEntries.
	//
	// Дедуплицирует вызовы leaderSendAEs: если для соседа уже есть активная
	// горутина leaderSendAEsToPeer, новая не создаётся.
	// Это предотвращает гонку nextIndex и экспоненциальный рост горутин
	// при частых пульсах.
	//
	// Доступ через atomic.CompareAndSwap без cm.mu — минимизация блокировок.
	// Инвариант:
	//		nil — сосед вне конфигурации;
	//		false — нет активной горутины;
	//		true — leaderSendAEsToPeer выполняется.
	// Сброс: в defer leaderSendAEsToPeer, runLeaderLoop и becomeFollower.
	inflightAE map[int]*atomic.Bool

	// commitmentTracker — продвижение commitIndex на лидере.
	commitmentTracker *commitmentTracker

	// inflight — карта future по индексу записи в журнале.
	// Обеспечивает O(1) поиск при отправке батча в FSM.
	inflight map[int]*logFuture

	// pendingVerify — очередь verifyFuture, ожидающих подтверждения от ведомых.
	// Успешный ответ AppendEntries голосует за все ожидающие verifyFuture,
	// если AE отправлен не раньше постановки запроса (vf.epoch <= dispatchEpoch).
	pendingVerify []*verifyFuture

	// verifyEpoch — монотонный счётчик раундов верификации ReadIndex.
	// Инкрементируется в leaderLoop при каждом verify‑запросе.
	// Значение снимается в leaderSendAEs и передаётся как dispatchEpoch.
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

	// replFailures — число подряд идущих транспортных ошибок по соседу.
	// Жизненный цикл привязан к роли лидера: создаётся в startLeader,
	// очищается в becomeFollower.
	replFailures map[int]int

	// lastAttempt — время последней попытки отправки RPC каждому соседу.
	// Задержка повторов реализуется сравнением времени (без time.Sleep), чтобы
	// не удерживать inflightAE[peer] и не задерживать восстановление узла.
	lastAttempt map[int]time.Time
}

// cmState — состояние узла Raft.
// Не содержит собственного mutex: все поля защищены cm.mu в ConsensusModule.
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

	// lastSnapshotIndex — индекс последнего созданного снимка.
	// Инициализируется как -1 (нет снимка).
	lastSnapshotIndex int

	// lastSnapshotTerm — терм последнего созданного снимка.
	lastSnapshotTerm int

	// termIndexMap — карта term → последний LogEntry.Index с этим term.
	// O(1) lookup для ConflictTerm вместо O(n) slices.Backward(cm.cmState.log).
	// Инкрементально обновляется в dispatchLogsUnsafe; перестраивается
	// целиком при обрезке/сжатии/замене журнала.
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
// Постусловие: любой возврат из Stop() — включая повторный
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

// goSpawnLocked запускает функцию fn в отдельной горутине и регистрирует её в группе ожидания cm.wg.
//
// Важное условие: метод должен вызываться только когда блокировка cm.mu уже удерживается
// (например, внутри becomeFollower, startElection, startLeader, leaderSendAEs).
//
// Ключевой контракт безопасности: проверка состояния узла (Dead/shutdownClosed) и добавление
// счётчика в wg (wg.Add) выполняются под одной блокировкой cm.mu. Это гарантирует атомарность:
// горутина не будет добавлена в wg после того, как Stop() начал ожидание завершения всех горутин (wg.Wait).
//
// Критическое ограничение: нельзя временно освобождать cm.mu внутри вызывающих функций,
// чтобы использовать «unlocked»-вариант. Если это сделать, атомарность нарушится:
// между проверкой состояния и wg.Add может успеть выполниться Stop(), и возникнет гонка.
func (cm *ConsensusModule) goSpawnLocked(fn func()) {
	if cm.cmState.state == Dead || cm.shutdownClosed {
		return // узел останавливается: запускать новую горутину нельзя
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
// Не вызывает пользовательский код под блокировкой
// (только проверка, Add и go).
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

// traceLockedLogf выводит отладочное сообщение, если traceCM > level.
// Ожидается, что cm.mu уже заблокирован вызывающим кодом, поэтому
// состояние (cm.cmState.state, cm.id, cm.cmState.currentTerm) читается напрямую.
func (cm *ConsensusModule) traceLockedLogf(level int, format string, args ...any) {
	if level < traceCM {
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
	if level < traceCM {
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
