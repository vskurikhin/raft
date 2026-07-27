package raft

import (
	"bytes"
	"container/list"
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"log/slog"
	"math/rand"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// ProtocolVersion — версия протокола Raft, используемая данной реализацией.
	// Значение 3. Совместимость с версиями 0, 1 и 2 не поддерживается.
	ProtocolVersion = 3

	Quantum             = 1
	HeartbeatTimeoutMs  = 17 * Quantum
	ReelectionTimeoutMs = 127 * Quantum
	TickerTimeoutMs     = 11 * Quantum

	DebugCM = 1
)

// CommitEntry — это данные, которые Raft отправляет в канал фиксации.
// Каждая запись фиксации уведомляет клиента о том, что консенсус по команде
// был достигнут и эта команда может быть применена к машине состояний клиента.
type CommitEntry struct {
	// Command — это команда клиента, которая была зафиксирована.
	Command any

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

// maxApplyBatchSize — максимальное количество команд, собираемых в один
// пакет при групповой фиксации (group commit). Значение 64 выбрано
// как разумный компромисс между задержкой (latency) и пропускной
// способностью (throughput): при интенсивной нагрузке лидер успевает
// собрать до 64 команд перед отправкой одного AppendEntries RPC,
// что снижает количество RPC-вызовов без значительного увеличения
// времени ожидания.
var maxApplyBatchSize = 64

type LogEntry struct {
	Index   int
	Term    int
	Type    LogType
	Command any
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

// ConsensusModule (CM) реализует единый узел консенсуса Raft.
type ConsensusModule struct {
	mu sync.Mutex

	id      int
	peerIds []int
	storage Storage

	transport Transport

	fsm         FSM
	fsmMutateCh chan []*commitTuple
	shutdownCh  chan struct{}

	applyCh  chan *logFuture
	inflight *list.List

	commitCh          chan int
	stepDown          chan struct{}
	confChangeCh      chan *configurationChangeFuture
	configurationsCh  chan *configurationsFuture
	commitmentTracker *commitmentTracker

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex        int
	lastApplied        int
	leaderStartIndex   int
	state              CMState
	electionResetEvent time.Time
	electionTimerDone  chan struct{}

	nextIndex  map[int]int
	matchIndex map[int]int

	configurations configurations

	// preVoteDisabled отключает механизм Pre-Vote.
	// Если true, выборы начинаются сразу (как в классическом Raft).
	// По умолчанию false — Pre-Vote включён.
	preVoteDisabled bool

	// leaderLastContact — время последнего успешного AppendEntries от лидера.
	// Используется в RequestPreVote для проверки: если недавно был контакт с
	// лидером, Pre-Vote отклоняется.
	leaderLastContact time.Time

	// leaderID — ID текущего лидера (если известен).
	// Устанавливается при получении AppendEntries от лидера.
	leaderID int

	// leadershipTransferCh — канал для запросов на передачу лидерства.
	// Запросы обрабатываются в leaderLoop.
	leadershipTransferCh chan *leadershipTransferFuture

	// leadershipTransferInProgress — atomic-флаг, блокирующий Apply
	// во время передачи лидерства.
	leadershipTransferInProgress int32

	// leadershipTransferFuture — текущий future передачи лидерства.
	// Устанавливается в handleLeadershipTransfer, отвечается в stepDown.
	leadershipTransferFuture *leadershipTransferFuture

	// candidateFromLeadershipTransfer — true, если данный узел стал
	// кандидатом из-за получения TimeoutNow. Влияет на:
	//   — пропуск PreVote в runElectionTimer
	//   — обработку RequestVote (голосуем даже при известном лидере)
	//   — обработку AppendEntries (не step-down от старого лидера)
	candidateFromLeadershipTransfer atomic.Bool
}

// NewConsensusModule создаёт новый экземпляр CM.
// fsm — машина состояний клиента, к которой применяются закоммиченные записи.
// ready уведомляет CM, что все соседи подключены.
func NewConsensusModule(
	id int,
	peerIds []int,
	transport Transport,
	storage Storage,
	fsm FSM,
	ready <-chan any,
) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.id = id
	cm.peerIds = peerIds
	cm.transport = transport
	cm.storage = storage
	cm.fsm = fsm
	cm.fsmMutateCh = make(chan []*commitTuple, 16)
	cm.shutdownCh = make(chan struct{})
	cm.applyCh = make(chan *logFuture)
	cm.inflight = list.New()
	cm.commitCh = make(chan int, 1)
	cm.stepDown = make(chan struct{}, 1)
	cm.confChangeCh = make(chan *configurationChangeFuture, 1)
	cm.configurationsCh = make(chan *configurationsFuture, 1)
	cm.state = Follower
	cm.votedFor = -1
	cm.commitIndex = -1
	cm.lastApplied = -1
	cm.leaderStartIndex = -1
	cm.nextIndex = make(map[int]int)
	cm.matchIndex = make(map[int]int)
	cm.electionTimerDone = make(chan struct{})
	cm.preVoteDisabled = false
	cm.leaderLastContact = time.Time{}
	cm.leaderID = -1
	cm.leadershipTransferCh = make(chan *leadershipTransferFuture, 1)

	if cm.storage.HasData() {
		cm.restoreFromStorage()
	}

	// Установить начальную конфигурацию, если она ещё не задана
	// (первый запуск или хранилище не содержит данных).
	if len(cm.configurations.latest.ConfigServers) == 0 {
		cm.setInitialConfiguration()
	}

	go cm.runFSM()
	go cm.runRPCReader()

	go func() {
		<-ready
		cm.mu.Lock()
		cm.electionResetEvent = time.Now()
		cm.mu.Unlock()
		cm.runElectionTimer()
	}()
	return cm
}

// Report отчет о состоянии данного CM.
func (cm *ConsensusModule) Report() (id, term int, isLeader bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.id, cm.currentTerm, cm.state == Leader
}

// runRPCReader — горутина, читающая входящие RPC-запросы из транспорта
// и диспатчащая их по типу команды. Ответ отправляется через RespChan.
// Завершает работу при закрытии shutdownCh.
func (cm *ConsensusModule) runRPCReader() {
	consumer := cm.transport.Consumer()
	for {
		select {
		case rpc, ok := <-consumer:
			if !ok {
				return
			}
			switch cmd := rpc.Command.(type) {
			case *RequestVoteArgs:
				var reply RequestVoteReply
				err := cm.RequestVote(*cmd, &reply)
				rpc.RespChan <- RPCResponse{Reply: &reply, Error: err}
			case *AppendEntriesArgs:
				var reply AppendEntriesReply
				err := cm.AppendEntries(*cmd, &reply)
				rpc.RespChan <- RPCResponse{Reply: &reply, Error: err}
			case *RequestPreVoteArgs:
				var reply RequestPreVoteReply
				err := cm.RequestPreVote(*cmd, &reply)
				rpc.RespChan <- RPCResponse{Reply: &reply, Error: err}
			case *TimeoutNowRequest:
				cm.timeoutNow(rpc, cmd)
			default:
				rpc.RespChan <- RPCResponse{Error: ErrNotImplemented}
			}
		case <-cm.shutdownCh:
			return
		}
	}
}

// Stop останавливает этот CM.
func (cm *ConsensusModule) Stop() {
	cm.dLogf("CM.Stop called")
	cm.mu.Lock()
	cm.state = Dead
	cm.mu.Unlock()
	cm.dLogf("becomes Dead")

	close(cm.shutdownCh)
}

// restoreFromStorage восстанавливает постоянное состояние данного CM
// из хранилища. Должен вызываться в конструкторе до запуска какой-либо
// конкурентной работы.
func (cm *ConsensusModule) restoreFromStorage() {
	if termData, found := cm.storage.Get("currentTerm"); found {
		d := gob.NewDecoder(bytes.NewBuffer(termData))
		if err := d.Decode(&cm.currentTerm); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("currentTerm not found in storage")
	}
	if votedData, found := cm.storage.Get("votedFor"); found {
		d := gob.NewDecoder(bytes.NewBuffer(votedData))
		if err := d.Decode(&cm.votedFor); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("votedFor not found in storage")
	}
	if logData, found := cm.storage.Get("log"); found {
		d := gob.NewDecoder(bytes.NewBuffer(logData))
		if err := d.Decode(&cm.log); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("log not found in storage")
	}

	cm.rebuildConfigurations()
}

// dLogf выводит отладочное сообщение, если DebugCM > 0.
func (cm *ConsensusModule) dLogf(format string, args ...any) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d] ", cm.id) + format
		log.Printf(format, args...)
	}
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

// RequestVote RPC.
func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
		return nil
	}

	if err := checkRPCHeader(&args); err != nil {
		return err
	}

	lastLogIndex, lastLogTerm := cm.lastLogIndexAndTerm()
	cm.dLogf(
		"RequestVote: %+v [currentTerm=%d, votedFor=%d, log index/term=(%d, %d)]",
		args, cm.currentTerm, cm.votedFor, lastLogIndex, lastLogTerm,
	)

	if args.Term > cm.currentTerm {
		cm.dLogf("... term out of date in RequestVote")
		cm.becomeFollower(args.Term)
	}

	// Проверка наличия известного лидера.
	// Если есть лидер, но это leadership transfer — голосуем (bypass).
	if cm.leaderID >= 0 && cm.leaderID != args.CandidateID && !args.LeadershipTransfer {
		cm.dLogf("... leader known, denying vote for %d", args.CandidateID)
		reply.VoteGranted = false
		reply.RPCHeader = RPCHeader{
			ProtocolVersion: ProtocolVersion,
			ServerID:        cm.id,
		}
		reply.Term = cm.currentTerm
		cm.persistToStorage()
		return nil
	}

	termOk := cm.currentTerm == args.Term
	voteOk := cm.votedFor == -1 || cm.votedFor == args.CandidateID
	logOk := args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)
	if !termOk || !voteOk || !logOk {
		cm.dLogf("... vote denied: termOk=%v (cur=%d, args=%d), voteOk=%v (votedFor=%d, cand=%d), logOk=%v (args=(%d,%d), local=(%d,%d))",
			termOk, cm.currentTerm, args.Term, voteOk, cm.votedFor, args.CandidateID,
			logOk, args.LastLogTerm, args.LastLogIndex, lastLogTerm, lastLogIndex)
	}
	if termOk && voteOk && logOk {
		reply.VoteGranted = true
		cm.votedFor = args.CandidateID
		cm.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.RPCHeader = RPCHeader{
		ProtocolVersion: ProtocolVersion,
		ServerID:        cm.id,
	}
	reply.Term = cm.currentTerm
	cm.persistToStorage()
	cm.dLogf("... RequestVote reply: %+v", reply)
	return nil
}

// RequestPreVote обрабатывает входящий PreVote RPC (§4 Pre-Vote).
//
// В отличие от RequestVote:
//   - НЕ меняет votedFor, currentTerm
//   - НЕ персистит состояние
//   - НЕ переводит в Follower при args.Term > cm.currentTerm
//   - Добавляет проверку: знает ли получатель о действующем лидере
//
// PreVote использует ту же log-safety проверку, что и RequestVote (§5.4.1).
// Если получатель недавно получал heartbeat от лидера или отстаёт по логу —
// PreVote отклоняется, чтобы предотвратить лишние выборы.
func (cm *ConsensusModule) RequestPreVote(args RequestPreVoteArgs, reply *RequestPreVoteReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.state == Dead {
		return nil
	}

	if err := checkRPCHeader(&args); err != nil {
		return err
	}

	lastLogIndex, lastLogTerm := cm.lastLogIndexAndTerm()
	cm.dLogf(
		"RequestPreVote: %+v [currentTerm=%d, log index/term=(%d, %d)]",
		args, cm.currentTerm, lastLogIndex, lastLogTerm,
	)

	// Отклоняем PreVote, если отправитель не является voter'ом в текущей
	// конфигурации. Это defense-in-depth: runPreCandidate уже фильтрует
	// voter'ов на стороне отправителя, но получатель тоже должен проверять.
	if !hasVote(cm.configurations.latest, args.GetRPCHeader().ServerID) {
		cm.dLogf("... RequestPreVote denied: not a voter")
		reply.RPCHeader = RPCHeader{
			ProtocolVersion: ProtocolVersion,
			ServerID:        cm.id,
		}
		reply.Term = cm.currentTerm
		reply.VoteGranted = false
		return nil
	}

	reply.RPCHeader = RPCHeader{
		ProtocolVersion: ProtocolVersion,
		ServerID:        cm.id,
	}
	reply.Term = cm.currentTerm
	reply.VoteGranted = false

	// Предлагаемый term должен быть не меньше текущего.
	if args.Term < cm.currentTerm {
		cm.dLogf("... RequestPreVote denied: older term")
		return nil
	}

	// Отклоняем PreVote, если недавно был контакт с лидером (leaderLastContact
	// обновлён в пределах election timeout). Это предотвращает выборы при
	// кратковременных сетевых сбоях — кандидат явно не актуальный лидер.
	if !cm.leaderLastContact.IsZero() {
		leaderKnown := time.Since(cm.leaderLastContact) < cm.electionTimeout()
		if leaderKnown && args.GetRPCHeader().ServerID != cm.leaderID {
			cm.dLogf("... RequestPreVote denied: leader known")
			return nil
		}
	}

	// Log-safety: отклонить, если лог получателя новее лога кандидата.
	if lastLogTerm > args.LastLogTerm ||
		(lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
		cm.dLogf("... RequestPreVote denied: log is more up-to-date")
		return nil
	}

	reply.VoteGranted = true
	cm.dLogf("... RequestPreVote granted")
	return nil
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

func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
		return nil
	}

	if err := checkRPCHeader(&args); err != nil {
		return err
	}

	cm.dLogf("AppendEntries: %+v", args)

	// Candidate от leadership transfer не должен делать step-down
	// при получении AppendEntries от старого лидера, если терм равен
	// текущему или на 1 меньше. Исключение: терм строго больше нашего.
	if cm.state == Candidate && cm.candidateFromLeadershipTransfer.Load() {
		if args.Term > cm.currentTerm {
			cm.dLogf("... term out of date in AppendEntries (candidate from LT)")
			cm.becomeFollower(args.Term)
			cm.candidateFromLeadershipTransfer.Store(false)
		}
		reply.RPCHeader = RPCHeader{
			ProtocolVersion: ProtocolVersion,
			ServerID:        cm.id,
		}
		reply.Term = cm.currentTerm
		reply.Success = false
		cm.persistToStorage()
		return nil
	}

	if args.Term > cm.currentTerm {
		cm.dLogf("... term out of date in AppendEntries")
		cm.becomeFollower(args.Term)
	}

	reply.Success = false
	if args.Term == cm.currentTerm {
		if cm.state != Follower {
			cm.becomeFollower(args.Term)
		}
		cm.electionResetEvent = time.Now()
		cm.leaderLastContact = time.Now()
		cm.leaderID = args.LeaderID

		// Проверяет, содержит ли наш журнал запись с индексом PrevLogIndex,
		// у которой терм совпадает с PrevLogTerm.
		// Обратите внимание, что в особом случае, когда PrevLogIndex == -1,
		// условие считается истинным автоматически.
		if args.PrevLogIndex == -1 ||
			(args.PrevLogIndex < len(cm.log) && args.PrevLogTerm == cm.log[args.PrevLogIndex].Term) {
			reply.Success = true

			// Находит точку вставки — место, где происходит несовпадение термов
			// между существующими записями журнала, начиная с PrevLogIndex+1,t
			// и новыми записями, отправленными лидером через RPC.
			logInsertIndex := args.PrevLogIndex + 1
			newEntriesIndex := 0

			for logInsertIndex < len(cm.log) && newEntriesIndex < len(args.Entries) {
				if cm.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}
			// После завершения этого цикла:
			// - logInsertIndex указывает на конец журнала
			//   или на индекс, где терм записи отличается от записи лидера.
			// - newEntriesIndex указывает на конец массива Entries
			//   или на индекс, где терм записи отличается от соответствующей записи журнала.
			if newEntriesIndex < len(args.Entries) {
				cm.dLogf("... inserting entries %v from index %d", args.Entries[newEntriesIndex:], logInsertIndex)
				cm.log = append(cm.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
				cm.dLogf("... log is now: %v", cm.log)
				// Если перезапись затронула конфигурацию, откатываем latest.
				cm.processLogConflict(logInsertIndex)
			}

			// Обработать LogConfiguration записи в полученном диапазоне.
			for _, entry := range args.Entries[newEntriesIndex:] {
				if entry.Type == LogConfiguration {
					cm.processConfigurationLogEntry(&entry)
				}
			}

			if args.LeaderCommit > cm.commitIndex {
				cm.commitIndex = min(args.LeaderCommit, len(cm.log)-1)
				cm.dLogf("... setting commitIndex=%d", cm.commitIndex)
				cm.persistToStorage()
				commitIdx := cm.commitIndex
				cm.mu.Unlock()
				cm.processLogs(commitIdx)
				cm.mu.Lock()
			}
		} else {
			// Не найдено совпадение для PrevLogIndex/PrevLogTerm.
			// Заполнить ConflictIndex и ConflictTerm, чтобы помочь лидеру
			// быстрее привести наш журнал в актуальное состояние.
			if args.PrevLogIndex >= len(cm.log) {
				reply.ConflictIndex = len(cm.log)
				reply.ConflictTerm = -1
			} else {
				// PrevLogIndex указывает на существующую запись в нашем журнале,
				// но PrevLogTerm не совпадает с термом записи
				// cm.log[PrevLogIndex].
				reply.ConflictTerm = cm.log[args.PrevLogIndex].Term

				var i int
				for i = args.PrevLogIndex - 1; i >= 0; i-- {
					if cm.log[i].Term != reply.ConflictTerm {
						break
					}
				}
				reply.ConflictIndex = i + 1
			}
		}
	}

	reply.RPCHeader = RPCHeader{
		ProtocolVersion: ProtocolVersion,
		ServerID:        cm.id,
	}
	reply.Term = cm.currentTerm
	cm.persistToStorage()
	cm.dLogf("AppendEntries reply: %+v", *reply)
	return nil
}

// electionTimeout генерирует псевдослучайную длительность тайм-аута выборов.
func (cm *ConsensusModule) electionTimeout() time.Duration {
	// Если установлен параметр RAFT_FORCE_MORE_REELECTION, проведите стресс-тест, намеренно
	// генерируя жестко заданное число очень часто. Это вызовет коллизии
	// между различными серверами и приведет к увеличению количества перевыборов.
	if os.Getenv("RAFT_FORCE_MORE_REELECTION") != "" && rand.Intn(3) == 0 {
		return time.Duration(ReelectionTimeoutMs) * time.Millisecond
	}
	return time.Duration(ReelectionTimeoutMs+rand.Intn(ReelectionTimeoutMs)) * time.Millisecond
}

// runElectionTimer реализует таймер выборов. Она должна запускаться всякий раз, когда
// мы хотим запустить таймер для выдвижения в качестве кандидата на новых выборах.
//
// Эта функция является блокирующей и должна запускаться в отдельной горутине;
// она предназначена для работы с одним (одноразовым) таймером выборов, поскольку она завершается
// всякий раз, когда состояние CM меняется с «ведомый/кандидат» или меняется срок полномочий.
func (cm *ConsensusModule) runElectionTimer() {
	timeoutDuration := cm.electionTimeout()
	cm.mu.Lock()
	termStarted := cm.currentTerm
	electionTimerDone := cm.electionTimerDone
	cm.mu.Unlock()
	cm.dLogf("election timer started (%v), term=%d", timeoutDuration, termStarted)

	// Этот цикл выполняется до тех пор, пока не произойдёт одно из двух:
	// - мы не обнаружим, что таймер выборов больше не нужен, или
	// - таймер выборов не истечёт и данный CM не станет кандидатом.
	// Для ведомого узла этот цикл обычно продолжает работать в фоновом режиме
	// в течение всего времени жизни CM.
	ticker := time.NewTicker(TickerTimeoutMs * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			cm.mu.Lock()
			if cm.state != Candidate && cm.state != Follower {
				cm.dLogf("in election timer state=%s, bailing out", cm.state)
				cm.mu.Unlock()
				return
			}

			if termStarted != cm.currentTerm {
				cm.dLogf("in election timer term changed from %d to %d, bailing out", termStarted, cm.currentTerm)
				cm.mu.Unlock()
				return
			}

			// Начать выборы (или Pre-Vote), если в течение времени ожидания мы
			// не получили сообщение от лидера или не проголосовали за кого-либо.
			if elapsed := time.Since(cm.electionResetEvent); elapsed >= timeoutDuration {
				if cm.preVoteDisabled || cm.candidateFromLeadershipTransfer.Load() {
					cm.startElection()
					cm.mu.Unlock()
					return
				}
				cm.mu.Unlock()
				go cm.runPreCandidate()
				return
			}
			cm.mu.Unlock()

		case <-electionTimerDone:
			return
		case <-cm.shutdownCh:
			return
		}
	}
}

// startElection запускает новые выборы с этим CM в качестве кандидата.
// Ожидается, что cm.mu будет заблокирован.
//
// Если candidateFromLeadershipTransfer установлен, в RequestVoteArgs
// передаётся LeadershipTransfer=true, что bypass проверку наличия лидера
// на узлах-получателях. Флаг сбрасывается после того, как все горутины
// отправили RequestVote, чтобы последующие обычные выборы не имели этого флага.
func (cm *ConsensusModule) startElection() {
	cm.state = Candidate
	cm.currentTerm += 1
	savedCurrentTerm := cm.currentTerm
	cm.electionResetEvent = time.Now()
	cm.votedFor = cm.id
	cm.persistToStorage()
	cm.dLogf("becomes Candidate (currentTerm=%d); log=%v", savedCurrentTerm, cm.log)

	// Сохраняем флаг в локальную переменную до его сброса.
	isLeadershipTransfer := cm.candidateFromLeadershipTransfer.Load()
	// Сбрасываем флаг, чтобы последующие обычные выборы не имели этого флага.
	cm.candidateFromLeadershipTransfer.Store(false)

	// Определяем состав кластера из текущей конфигурации.
	cfg := cm.configurations.latest
	voters := voterIDs(cfg)
	voterCount := len(voters)

	// Одиночный узел: побеждает на выборах немедленно.
	if voterCount <= 1 {
		cm.startLeader()
		return
	}

	var votesReceived atomic.Int32
	votesReceived.Store(1)

	// Параллельно отправить RPC-запросы RequestVote всем серверам из конфигурации.
	for _, s := range cfg.ConfigServers {
		peerID := int(s.ID)
		if peerID == cm.id {
			continue
		}
		go func(peerID int, lt bool) {
			cm.mu.Lock()
			savedLastLogIndex, savedLastLogTerm := cm.lastLogIndexAndTerm()
			cm.mu.Unlock()

			args := RequestVoteArgs{
				RPCHeader: RPCHeader{
					ProtocolVersion: ProtocolVersion,
					ServerID:        cm.id,
				},
				Term:               savedCurrentTerm,
				CandidateID:        cm.id,
				LastLogIndex:       savedLastLogIndex,
				LastLogTerm:        savedLastLogTerm,
				LeadershipTransfer: lt,
			}

			cm.dLogf("sending RequestVote to %d: %+v", peerID, args)
			reply, err := cm.transport.RequestVote(ServerID(peerID), args)
			if err == nil {
				cm.mu.Lock()
				cm.dLogf("received RequestVoteReply %+v", reply)

				if cm.state != Candidate {
					cm.dLogf("while waiting for reply, state = %v", cm.state)
					cm.mu.Unlock()
					return
				}

				if reply.Term > cm.currentTerm {
					cm.dLogf("term out of date in RequestVoteReply")
					cm.becomeFollower(reply.Term)
					cm.mu.Unlock()
					return
				} else if reply.Term == cm.currentTerm {
					if reply.VoteGranted {
						votesReceived.Add(1)
						if int(votesReceived.Load()) >= quorumSize(voterCount) {
							// Выиграл выборы!
							cm.dLogf("wins election with %d votes", votesReceived.Load())
							cm.startLeader()
							cm.mu.Unlock()
							slog.Info("wins election", slog.Int("votes", int(votesReceived.Load())))
							return
						}
					}
				}
				cm.mu.Unlock()
			}
		}(peerID, isLeadershipTransfer)
	}

	// Запустить новый таймер выборов на случай, если текущие выборы не завершатся успешно.
	go cm.runElectionTimer()
}

// runPreCandidate выполняет предварительное голосование (§4 Pre-Vote).
//
// Узел переходит в PreCandidate (не увеличивая currentTerm), отправляет
// RequestPreVote всем voter'ам и собирает ответы. Если получен кворум
// положительных ответов — вызывает startElection() для настоящих выборов.
// Если кворум не получен или обнаружен более высокий term — возвращается
// в Follower.
//
// Горутина завершается при:
//   - получении кворума PreVote → вызов startElection()
//   - истечении election timeout без кворума → becomeFollower()
//   - обнаружении более высокого term от другого узла → becomeFollower()
//
// Вызывается из runElectionTimer(), когда preVoteDisabled == false.
func (cm *ConsensusModule) runPreCandidate() {
	cm.mu.Lock()
	if cm.state != Follower && cm.state != PreCandidate {
		cm.mu.Unlock()
		return
	}
	cm.state = PreCandidate
	cm.dLogf("becomes PreCandidate; term=%d, log=%v", cm.currentTerm, cm.log)

	cfg := cm.configurations.latest
	voters := voterIDs(cfg)
	voterCount := len(voters)

	// Одиночный узел: PreVote не нужен, сразу выборы.
	if voterCount <= 1 {
		cm.startElection()
		cm.mu.Unlock()
		return
	}

	proposedTerm := cm.currentTerm + 1
	cm.mu.Unlock()

	// Канал для сбора ответов.
	preVoteRespCh := make(chan *RequestPreVoteReply, voterCount-1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Отправить RequestPreVote параллельно всем voter'ам (кроме себя).
	for _, s := range cfg.ConfigServers {
		peerID := int(s.ID)
		if peerID == cm.id {
			continue
		}
		if s.Suffrage != Voter {
			continue
		}
		go func(peerID int) {
			cm.mu.Lock()
			lastLogIndex, lastLogTerm := cm.lastLogIndexAndTerm()
			savedTerm := cm.currentTerm
			cm.mu.Unlock()

			args := RequestPreVoteArgs{
				RPCHeader: RPCHeader{
					ProtocolVersion: ProtocolVersion,
					ServerID:        cm.id,
				},
				Term:         proposedTerm,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			cm.dLogf("sending RequestPreVote to %d: %+v", peerID, args)
			reply, err := cm.transport.RequestPreVote(ServerID(peerID), args)
			if err != nil {
				cm.dLogf("RequestPreVote to %d failed: %v", peerID, err)
				var resp *RequestPreVoteReply
				if err == ErrNotImplemented {
					// Если транспорт не поддерживает PreVote, считаем голос
					// предоставленным (как в hashicorp/raft).
					resp = &RequestPreVoteReply{
						RPCHeader:   RPCHeader{ProtocolVersion: ProtocolVersion, ServerID: cm.id},
						Term:        savedTerm,
						VoteGranted: true,
					}
				} else {
					resp = &RequestPreVoteReply{
						RPCHeader:   RPCHeader{ProtocolVersion: ProtocolVersion, ServerID: cm.id},
						Term:        savedTerm,
						VoteGranted: false,
					}
				}
				select {
				case preVoteRespCh <- resp:
				case <-ctx.Done():
				}
				return
			}
			select {
			case preVoteRespCh <- &reply:
			case <-ctx.Done():
			}
		}(peerID)
	}

	grantedVotes := 1 // голос за себя
	neededVotes := quorumSize(voterCount)
	votersResponded := 0
	totalVoters := voterCount - 1

	timeout := time.After(cm.electionTimeout())

	// Собирать ответы от peer'ов.
	for votersResponded < totalVoters {
		select {
		case reply := <-preVoteRespCh:
			votersResponded++
			cm.mu.Lock()
			if cm.state != PreCandidate {
				cm.dLogf("runPreCandidate: state changed to %s, bailing out", cm.state)
				cm.mu.Unlock()
				return
			}
			if reply.Term > cm.currentTerm {
				cm.dLogf("runPreCandidate: found higher term %d", reply.Term)
				cm.becomeFollower(reply.Term)
				cm.mu.Unlock()
				return
			}
			cm.mu.Unlock()
			if reply.VoteGranted {
				grantedVotes++
				cm.dLogf("runPreCandidate: granted vote from peer, total=%d, needed=%d", grantedVotes, neededVotes)
				if grantedVotes >= neededVotes {
					// Jitter перед выборами, чтобы одновременно несколько узлов
					// не перешли в Candidate с одинаковым term (split vote).
					time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
					cm.mu.Lock()
					if cm.state == PreCandidate {
						cm.dLogf("runPreCandidate: won pre-vote with %d votes, starting election", grantedVotes)
						cm.startElection()
					}
					cm.mu.Unlock()
					return
				}
			}
		case <-timeout:
			cm.mu.Lock()
			if cm.state == PreCandidate {
				cm.dLogf("runPreCandidate: pre-vote timeout, returning to follower")
				cm.becomeFollower(cm.currentTerm)
			}
			cm.mu.Unlock()
			return
		case <-cm.shutdownCh:
			return
		}

		// После обработки всех ответов пересчитать необходимые голоса.
		if votersResponded >= totalVoters && grantedVotes < neededVotes {
			cm.mu.Lock()
			if cm.state == PreCandidate {
				cm.dLogf("runPreCandidate: pre-vote lost (%d/%d), returning to follower", grantedVotes, neededVotes)
				cm.becomeFollower(cm.currentTerm)
			}
			cm.mu.Unlock()
			return
		}
	}
}

// quorumSize возвращает минимальное количество голосов для достижения кворума.
func quorumSize(voterCount int) int {
	return voterCount/2 + 1
}

// becomeFollower делает cm последователем и сбрасывает его состояние.
// Если cm был лидером, отправляет сигнал в stepDown, чтобы leaderLoop
// завершил работу и разрешил все ожидающие future с ErrLeadershipLost.
// Ожидается, что cm.mu будет заблокирован.
func (cm *ConsensusModule) becomeFollower(term int) {
	wasLeader := cm.state == Leader
	cm.dLogf("becomes Follower with term=%d; log=%v", term, cm.log)
	cm.state = Follower
	if wasLeader {
		select {
		case cm.stepDown <- struct{}{}:
		default:
		}
	}
	if term > cm.currentTerm {
		cm.currentTerm = term
		cm.votedFor = -1
		cm.persistToStorage()
	}
	cm.leaderLastContact = time.Time{}
	cm.leaderID = -1
	cm.electionResetEvent = time.Now()

	select {
	case <-cm.electionTimerDone:
	default:
		close(cm.electionTimerDone)
	}
	cm.electionTimerDone = make(chan struct{})
	go cm.runElectionTimer()
}

// startLeader переводит cm в состояние лидера и запускает единый leaderLoop.
// Ожидается, что cm.mu будет заблокирован.
func (cm *ConsensusModule) startLeader() {
	cm.state = Leader
	cm.leaderLastContact = time.Now()
	cm.leaderID = cm.id

	cfg := cm.configurations.latest
	for _, s := range cfg.ConfigServers {
		peerID := int(s.ID)
		if peerID == cm.id {
			continue
		}
		cm.nextIndex[peerID] = len(cm.log)
		cm.matchIndex[peerID] = -1
	}
	cm.dLogf(
		"becomes Leader; term=%d, nextIndex=%v, matchIndex=%v; log=%v",
		cm.currentTerm, cm.nextIndex, cm.matchIndex, cm.log,
	)

	cm.leaderStartIndex = len(cm.log) - 1
	cm.commitmentTracker = newCommitmentTracker(
		cm.id, cm.commitCh, cm.currentTerm, len(cm.log)-1,
	)
	cm.commitmentTracker.setConfiguration(voterIDs(cfg), cm.lookupTerm)
	cm.commitmentTracker.setMatch(cm.id, len(cm.log)-1, cm.lookupTerm)

	// Noop entry при утверждении лидерства (§5.4.2).
	// Лидер должен закоммитить noop запись своего терма, чтобы
	// зафиксировать любые незакоммиченные записи из предыдущих термов.
	noop := &logFuture{
		log: LogEntry{
			Type: LogNoop,
		},
	}
	noop.init(cm.shutdownCh)
	cm.dispatchLogsUnsafe([]*logFuture{noop})

	go cm.runLeaderLoop()
}

func (cm *ConsensusModule) nextIndexArgsEntries(peerID, savedCurrentTerm int) (int, AppendEntriesArgs, []LogEntry) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	ni := cm.nextIndex[peerID]
	prevLogIndex := ni - 1
	prevLogTerm := -1
	if 0 <= prevLogIndex && prevLogIndex < len(cm.log) {
		prevLogTerm = cm.log[prevLogIndex].Term
	}
	var entries []LogEntry
	if ni < len(cm.log) {
		entries = append([]LogEntry{}, cm.log[ni:]...)
	} else {
		entries = nil
	}
	return ni, AppendEntriesArgs{
		RPCHeader: RPCHeader{
			ProtocolVersion: ProtocolVersion,
			ServerID:        cm.id,
		},
		Term:         savedCurrentTerm,
		LeaderID:     cm.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: cm.commitIndex,
	}, entries
}

// leaderSendAEsToPeer отправляет AppendEntries указанному peer-у,
// обрабатывает ответ и обновляет состояние CM. При успешной репликации
// использует commitmentTracker для вычисления нового commitIndex.
//
// Вызов происходит в отдельной goroutine для каждого peer-а.
// peerID — идентификатор получателя, savedCurrentTerm — терм на момент
// формирования запроса (для защиты от устаревших ответов).
func (cm *ConsensusModule) leaderSendAEsToPeer(peerID, savedCurrentTerm int) {
	ni, args, entries := cm.nextIndexArgsEntries(peerID, savedCurrentTerm)
	cm.dLogf("sending AppendEntries to %v: ni=%d, args=%+v", peerID, ni, args)
	reply, err := cm.transport.AppendEntries(ServerID(peerID), args)
	if err == nil {
		cm.mu.Lock()
		if reply.Term > cm.currentTerm {
			cm.dLogf("term out of date in heartbeat reply")
			cm.becomeFollower(reply.Term)
			cm.mu.Unlock()
			return
		}
		if cm.nextIndex[peerID] != ni {
			cm.dLogf("#44 ni out of date in heartbeat reply")
			cm.mu.Unlock()
			return
		}

		if cm.state == Leader && savedCurrentTerm == reply.Term {
			if reply.Success {
				cm.nextIndex[peerID] = ni + len(entries)
				cm.matchIndex[peerID] = cm.nextIndex[peerID] - 1

				cm.commitmentTracker.setMatch(peerID, cm.matchIndex[peerID], cm.lookupTerm)
				cm.dLogf(
					"AppendEntries reply from %d success: nextIndex := %v, matchIndex := %v; commitIndex := %d",
					peerID, cm.nextIndex, cm.matchIndex, cm.commitmentTracker.getCommitIndex(),
				)
				cm.persistToStorage()
				cm.mu.Unlock()
			} else {
				if reply.ConflictTerm >= 0 {
					lastIndexOfTerm := -1
					for i, v := range slices.Backward(cm.log) {
						if v.Term == reply.ConflictTerm {
							lastIndexOfTerm = i
							break
						}
					}
					if lastIndexOfTerm >= 0 {
						cm.nextIndex[peerID] = lastIndexOfTerm + 1
					} else {
						cm.nextIndex[peerID] = reply.ConflictIndex
					}
				} else {
					cm.nextIndex[peerID] = reply.ConflictIndex
				}
				cm.dLogf("AppendEntries reply from %d !success: nextIndex := %d", peerID, ni-1)
				cm.mu.Unlock()
			}
		} else {
			cm.mu.Unlock()
		}
	}
}

// leaderSendAEs отправляет очередной раунд сообщений AppendEntries всем
// соседям, обрабатывает их ответы и обновляет состояние CM.
func (cm *ConsensusModule) leaderSendAEs() {
	cm.mu.Lock()
	if cm.state != Leader {
		cm.mu.Unlock()
		return
	}
	savedCurrentTerm := cm.currentTerm

	peers := make(map[int]struct{})
	for _, s := range cm.configurations.committed.ConfigServers {
		if int(s.ID) != cm.id {
			peers[int(s.ID)] = struct{}{}
		}
	}
	for _, s := range cm.configurations.latest.ConfigServers {
		if int(s.ID) != cm.id {
			peers[int(s.ID)] = struct{}{}
		}
	}
	cm.mu.Unlock()

	for peerID := range peers {
		go cm.leaderSendAEsToPeer(peerID, savedCurrentTerm)
	}
}

// lookupTerm возвращает терм записи журнала по индексу.
// Используется commitmentTracker для проверки Raft safety.
func (cm *ConsensusModule) lookupTerm(index int) int {
	if index < 0 || index >= len(cm.log) {
		return -1
	}
	return cm.log[index].Term
}

// lastLogIndexAndTerm возвращает индекс и терм последней записи журнала.
func (cm *ConsensusModule) lastLogIndexAndTerm() (int, int) {
	if len(cm.log) > 0 {
		lastIndex := len(cm.log) - 1
		return lastIndex, cm.log[lastIndex].Term
	}
	return -1, -1
}

// Apply отправляет команду в Raft и возвращает ApplyFuture.
// Команда будет применена к FSM после коммита.
func (cm *ConsensusModule) Apply(command any, timeout time.Duration) ApplyFuture {
	select {
	case <-cm.shutdownCh:
		return errorFuture{ErrRaftShutdown}
	default:
	}

	future := &logFuture{
		log: LogEntry{
			Type:    LogCommand,
			Command: command,
		},
	}
	future.init(cm.shutdownCh)

	cm.mu.Lock()
	if cm.state != Leader {
		cm.mu.Unlock()
		return errorFuture{ErrNotLeader}
	}
	// Во время передачи лидерства Apply блокируется — клиент получает
	// ErrLeadershipTransferInProgress. Это предотвращает ситуации, когда
	// новые команды приходят после того, как таргет уже догнал журнал.
	if atomic.LoadInt32(&cm.leadershipTransferInProgress) != 0 {
		cm.mu.Unlock()
		return errorFuture{ErrLeadershipTransferInProgress}
	}
	cm.mu.Unlock()

	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	}
	select {
	case <-timer:
		return errorFuture{ErrEnqueueTimeout}
	case <-cm.shutdownCh:
		return errorFuture{ErrRaftShutdown}
	case cm.applyCh <- future:
		return future
	}
}

// LeadershipTransfer инициирует graceful передачу лидерства указанному узлу.
//
// Алгоритм:
//  1. Проверка: цель != cm.id (нельзя передать лидерство себе).
//  2. Проверка: цель — voter в текущей конфигурации.
//  3. Проверка: передача ещё не идёт (leadershipTransferInProgress == 0).
//  4. Создание leadershipTransferFuture, отправка в leadershipTransferCh.
//  5. Возврат future клиенту.
//
// Возвращает ErrNotLeader, если узел не является лидером.
// Возвращает ErrLeadershipTransferInProgress, если передача уже идёт.
func (cm *ConsensusModule) LeadershipTransfer(targetID ServerID) LeadershipTransferFuture {
	future := &leadershipTransferFuture{targetID: targetID}
	future.init(cm.shutdownCh)

	cm.mu.Lock()
	if cm.state != Leader {
		cm.mu.Unlock()
		return errorFuture{ErrNotLeader}
	}
	// Проверка, что цель — voter в текущей конфигурации.
	if !hasVote(cm.configurations.latest, int(targetID)) {
		cm.mu.Unlock()
		return errorFuture{ErrLeadershipLost}
	}
	if targetID == ServerID(cm.id) {
		cm.mu.Unlock()
		return errorFuture{ErrLeadershipLost}
	}
	// Проверка, что передача ещё не идёт.
	if atomic.LoadInt32(&cm.leadershipTransferInProgress) != 0 {
		cm.mu.Unlock()
		return errorFuture{ErrLeadershipTransferInProgress}
	}
	cm.mu.Unlock()

	select {
	case cm.leadershipTransferCh <- future:
		return future
	case <-cm.shutdownCh:
		return errorFuture{ErrRaftShutdown}
	}
}

// timeoutNow обрабатывает входящий TimeoutNowRequest.
// Устанавливает candidateFromLeadershipTransfer и форсирует немедленные
// выборы (без PreVote, без ожидания election timeout).
func (cm *ConsensusModule) timeoutNow(rpc RPC, req *TimeoutNowRequest) {
	cm.dLogf("received TimeoutNow from %d", req.ServerID)

	// Уже лидер — no-op.
	cm.mu.Lock()
	if cm.state == Leader {
		term := cm.currentTerm
		cm.mu.Unlock()
		rpc.RespChan <- RPCResponse{
			Reply: &TimeoutNowResponse{
				RPCHeader: RPCHeader{ProtocolVersion: ProtocolVersion, ServerID: cm.id},
				Success:   false,
				Term:      term,
			},
		}
		return
	}
	// Если не лидер, проверяем только состояние Follower (Candidate/PreCandidate
	// не могут получить TimeoutNow).
	if cm.state != Follower {
		cm.mu.Unlock()
		rpc.RespChan <- RPCResponse{
			Reply: &TimeoutNowResponse{
				RPCHeader: RPCHeader{ProtocolVersion: ProtocolVersion, ServerID: cm.id},
				Success:   false,
				Term:      cm.currentTerm,
			},
		}
		return
	}

	// Форсированная остановка текущего election timer.
	select {
	case <-cm.electionTimerDone:
	default:
		close(cm.electionTimerDone)
	}
	cm.electionTimerDone = make(chan struct{})
	cm.candidateFromLeadershipTransfer.Store(true)
	cm.dLogf("candidateFromLeadershipTransfer set to true (cm.id=%d)", cm.id)

	// Запускаем форсированные выборы немедленно, без ожидания election timeout.
	// cm.mu удерживается, что гарантирует атомарность.
	cm.startElection()
	newTerm := cm.currentTerm
	cm.dLogf("started immediate election after TimeoutNow, term=%d (cm.id=%d)", newTerm, cm.id)
	cm.mu.Unlock()

	rpc.RespChan <- RPCResponse{
		Reply: &TimeoutNowResponse{
			RPCHeader: RPCHeader{ProtocolVersion: ProtocolVersion, ServerID: cm.id},
			Success:   true,
			Term:      newTerm,
		},
	}
}

// handleLeadershipTransfer реализует логику передачи лидерства.
//
// Вызывается только из leaderLoop. Блокирует leaderLoop до завершения
// передачи или таймаута. Все новые Apply и confChange во время передачи
// отклоняются через флаг leadershipTransferInProgress.
func (cm *ConsensusModule) handleLeadershipTransfer(future *leadershipTransferFuture) {
	targetID := int(future.targetID)

	// 1. Устанавливаем флаг блокировки Apply.
	// Флаг будет снят в stepDown или в defer leaderLoop при ошибке.
	atomic.StoreInt32(&cm.leadershipTransferInProgress, 1)

	// 2. Проверяем, что цель — voter и достижима.
	cm.mu.Lock()
	if !hasVote(cm.configurations.latest, targetID) {
		cm.mu.Unlock()
		atomic.StoreInt32(&cm.leadershipTransferInProgress, 0)
		future.respond(ErrLeadershipLost)
		return
	}
	lastIdx := len(cm.log) - 1
	targetNextIdx := cm.nextIndex[targetID]
	savedCurrentTerm := cm.currentTerm
	cm.mu.Unlock()

	// 3. Если цель отстаёт по журналу — догоняем.
	if targetNextIdx <= lastIdx {
		done := make(chan error, 1)
		go func() {
			for {
				// Отправляем сигнал репликации целевому узлу.
				cm.leaderSendAEsToPeer(targetID, savedCurrentTerm)

				// Ждём короткий интервал, затем проверяем nextIndex.
				time.Sleep(10 * time.Millisecond)

				cm.mu.Lock()
				if cm.nextIndex[targetID] > lastIdx {
					cm.mu.Unlock()
					done <- nil
					return
				}
				// Если лидерство потеряно — выход.
				if cm.state != Leader {
					cm.mu.Unlock()
					done <- ErrLeadershipLost
					return
				}
				cm.mu.Unlock()
			}
		}()

		// Таймаут догоняющей репликации.
		select {
		case err := <-done:
			if err != nil {
				atomic.StoreInt32(&cm.leadershipTransferInProgress, 0)
				future.respond(err)
				return
			}
		case <-cm.shutdownCh:
			atomic.StoreInt32(&cm.leadershipTransferInProgress, 0)
			future.respond(ErrRaftShutdown)
			return
		case <-time.After(cm.electionTimeout()):
			atomic.StoreInt32(&cm.leadershipTransferInProgress, 0)
			future.respond(ErrLeadershipLost)
			return
		}
	}

	// 4. Отправляем TimeoutNow цели.
	reply, err := cm.transport.TimeoutNow(
		ServerID(targetID),
		TimeoutNowRequest{
			RPCHeader: RPCHeader{
				ProtocolVersion: ProtocolVersion,
				ServerID:        cm.id,
			},
		},
	)
	if err != nil {
		atomic.StoreInt32(&cm.leadershipTransferInProgress, 0)
		future.respond(err)
		return
	}
	if !reply.Success {
		// Цель уже была лидером или в процессе выборов.
		atomic.StoreInt32(&cm.leadershipTransferInProgress, 0)
		future.respond(ErrLeadershipLost)
		return
	}

	// 5. TimeoutNow отправлен успешно — сохраняем future для ответа
	// в stepDown, когда leaderLoop обнаружит более высокий term.
	// Флаг leadershipTransferInProgress остаётся установленным
	// до stepDown — так Apply блокируется на всё время передачи.
	cm.leadershipTransferFuture = future
}

// runFSM — отдельная горутина, применяющая закоммиченные записи к FSM.
// Если FSM реализует BatchingFSM, использует ApplyBatch для группового
// применения; иначе вызывает Apply для каждой записи по отдельности.
// Type assertion выполняется один раз при старте горутины.
// Завершает работу при закрытии shutdownCh.
func (cm *ConsensusModule) runFSM() {
	batchingFSM, supportsBatch := cm.fsm.(BatchingFSM)
	for {
		select {
		case batch := <-cm.fsmMutateCh:
			if supportsBatch {
				cm.applyBatch(batchingFSM, batch)
			} else {
				cm.applySingle(batch)
			}
		case <-cm.shutdownCh:
			return
		}
	}
}

// applySingle применяет каждую запись из батча по отдельности через
// cm.fsm.Apply. Используется для FSM, не реализующих BatchingFSM.
func (cm *ConsensusModule) applySingle(batch []*commitTuple) {
	for _, ct := range batch {
		if ct.log.Type != LogCommand {
			if ct.future != nil {
				ct.future.response = nil
				ct.future.respond(nil)
			}
			continue
		}
		resp := cm.fsm.Apply(ct.log)
		if ct.future != nil {
			ct.future.response = resp
			ct.future.respond(nil)
		}
	}
}

// applyBatch применяет все LogCommand записи из батча за один вызов
// ApplyBatch переданного BatchingFSM. Non-command записи (noop)
// разрешаются с nil-ответом без вызова FSM. Паникует при несовпадении
// длины возвращаемого среза — это нарушение контракта BatchingFSM.
func (cm *ConsensusModule) applyBatch(fsm BatchingFSM, batch []*commitTuple) {
	var logs []*LogEntry
	var futures []*logFuture
	var nonCmdFutures []*logFuture
	for _, ct := range batch {
		if ct.log.Type != LogCommand {
			if ct.future != nil {
				nonCmdFutures = append(nonCmdFutures, ct.future)
			}
			continue
		}
		logs = append(logs, ct.log)
		futures = append(futures, ct.future)
	}
	for _, f := range nonCmdFutures {
		f.response = nil
		f.respond(nil)
	}
	if len(logs) == 0 {
		return
	}
	responses := fsm.ApplyBatch(logs)
	if len(responses) != len(logs) {
		panic(fmt.Sprintf(
			"ApplyBatch returned %d responses, expected %d",
			len(responses), len(logs),
		))
	}
	for i, f := range futures {
		if f != nil {
			f.response = responses[i]
			f.respond(nil)
		}
	}
}

// processLogs собирает закоммиченные записи от lastApplied+1 до commitIndex
// и отправляет их в FSM goroutine. Диапазон делится на под-батчи размером
// не более maxApplyBatchSize, чтобы FSM goroutine не блокировалась на
// обработке тысяч записей (например, после восстановления лидера).
// Метод не требует удержания cm.mu при вызове, но сам захватывает её
// внутри для чтения журнала и поиска future в списке inflight.
func (cm *ConsensusModule) processLogs(commitIndex int) {
	cm.mu.Lock()
	if commitIndex <= cm.lastApplied {
		cm.mu.Unlock()
		return
	}
	lastApplied := cm.lastApplied
	cm.lastApplied = commitIndex
	cm.persistToStorage()
	cm.mu.Unlock()

	start := lastApplied + 1
	for start <= commitIndex {
		end := start + maxApplyBatchSize - 1
		if end > commitIndex {
			end = commitIndex
		}
		cm.sendBatch(start, end)
		start = end + 1
	}
}

// sendBatch собирает под-батч записей журнала от start до end включительно
// и отправляет в fsmMutateCh. Поиск future в inflight выполняется под
// блокировкой cm.mu, отправка — без блокировки.
func (cm *ConsensusModule) sendBatch(start, end int) {
	cm.mu.Lock()
	batch := make([]*commitTuple, 0, end-start+1)
	for idx := start; idx <= end; idx++ {
		var future *logFuture
		for e := cm.inflight.Front(); e != nil; e = e.Next() {
			if lf := e.Value.(*logFuture); lf.log.Index == idx {
				future = lf
				cm.inflight.Remove(e)
				break
			}
		}
		if idx >= len(cm.log) {
			continue
		}
		log := &cm.log[idx]
		batch = append(batch, &commitTuple{log: log, future: future})
	}
	cm.mu.Unlock()
	if len(batch) > 0 {
		cm.fsmMutateCh <- batch
	}
}

// dispatchLogs присваивает индексы и термы записям, добавляет их в журнал
// лидера (cm.log), помещает future в очередь inflight и инициирует
// репликацию через commitmentTracker.
//
// Вызывается только из runLeaderLoop после проверки, что сервер всё ещё
// является лидером. После возврата runLeaderLoop вызывает leaderSendAEs
// для немедленной репликации.
//
// Предполагается, что будущие ответы будут разрешены после коммита
// в processLogs, либо при потере лидерства в runLeaderLoop.
func (cm *ConsensusModule) dispatchLogs(applyLogs []*logFuture) {
	cm.mu.Lock()
	cm.dispatchLogsUnsafe(applyLogs)

	// Продвигаем commitIndex через commitmentTracker.
	// Для single-node кластера это приводит к немедленному коммиту.
	// Для multi-node кластера учитываются matchIndex всех peer-ов.
	cm.commitmentTracker.commit(len(cm.log)-1, cm.lookupTerm)
	savedCommitIndex := cm.commitIndex
	if newCommitIndex := cm.commitmentTracker.getCommitIndex(); newCommitIndex > cm.commitIndex {
		cm.dLogf("leader sets commitIndex := %d", newCommitIndex)
		cm.commitIndex = newCommitIndex
	}
	newCommitIndex := cm.commitIndex
	cm.mu.Unlock()

	if newCommitIndex > savedCommitIndex {
		cm.processLogs(newCommitIndex)
	}
}

// dispatchLogsUnsafe — внутренняя версия dispatchLogs без захвата блокировки.
// Вызывающий должен удерживать cm.mu.
func (cm *ConsensusModule) dispatchLogsUnsafe(applyLogs []*logFuture) {
	term := cm.currentTerm
	lastIndex := len(cm.log) - 1
	now := time.Now()
	for _, f := range applyLogs {
		f.dispatch = now
		lastIndex++
		f.log.Index = lastIndex
		f.log.Term = term
		cm.log = append(cm.log, f.log)
	}
	cm.persistToStorage()
	cm.matchIndex[cm.id] = lastIndex

	for _, f := range applyLogs {
		cm.inflight.PushBack(f)
	}
}

// runLeaderLoop — единый центральный цикл лидера. Запускается при переходе
// в состояние Leader и завершается при потере лидерства или остановке CM.
//
// Обрабатывает все события лидера в одном select:
//   - applyCh: новые команды от клиентов (групповой commit, §5.1)
//   - commitCh: уведомление от commitmentTracker об изменении commitIndex
//   - stepDown: обнаружение более высокого term из RPC-ответа
//   - confChangeCh: изменения конфигурации кластера (placeholder)
//   - heartbeatTicker: регулярная отправка Heartbeat/AppendEntries
//   - shutdownCh: остановка модуля
//
// После выхода из цикла все ожидающие future из списка inflight
// получают ошибку ErrLeadershipLost, после чего список очищается.
func (cm *ConsensusModule) runLeaderLoop() {
	defer func() {
		cm.mu.Lock()
		if cm.leadershipTransferFuture != nil {
			atomic.StoreInt32(&cm.leadershipTransferInProgress, 0)
			cm.leadershipTransferFuture.respond(ErrLeadershipLost)
			cm.leadershipTransferFuture = nil
		}
		inflightCount := cm.inflight.Len()
		cm.dLogf("leaderLoop exit: responding to %d inflight futures", inflightCount)
		for e := cm.inflight.Front(); e != nil; e = e.Next() {
			e.Value.(*logFuture).respond(ErrLeadershipLost)
		}
		cm.inflight = list.New()
		cm.mu.Unlock()
	}()

	heartbeatTicker := time.NewTicker(HeartbeatTimeoutMs * time.Millisecond)
	defer heartbeatTicker.Stop()

	for {
		select {
		case future := <-cm.applyCh:
			cm.mu.Lock()
			if cm.state != Leader {
				cm.mu.Unlock()
				future.respond(ErrNotLeader)
				return
			}
			cm.mu.Unlock()

			ready := []*logFuture{future}
		groupCommit:
			for i := 0; i < maxApplyBatchSize-1; i++ {
				select {
				case f := <-cm.applyCh:
					ready = append(ready, f)
				default:
					break groupCommit
				}
			}
			cm.dispatchLogs(ready)
			// Немедленная репликация после записи в журнал.
			cm.leaderSendAEs()

			heartbeatTicker.Stop()
			heartbeatTicker.Reset(HeartbeatTimeoutMs * time.Millisecond)

		case newCommitIndex := <-cm.commitCh:
			cm.mu.Lock()
			if newCommitIndex > cm.commitIndex {
				cm.dLogf("leader sets commitIndex := %d", newCommitIndex)
				cm.commitIndex = newCommitIndex

				// Обновить committed конфигурацию, если latest был закоммичен.
				if cm.configurations.latestIndex > cm.configurations.committedIndex &&
					newCommitIndex >= cm.configurations.latestIndex {
					cm.configurations.committed = cm.configurations.latest
					cm.configurations.committedIndex = cm.configurations.latestIndex
				}

				// Проверить, остался ли лидер в committed конфигурации.
				if !hasVote(cm.configurations.committed, cm.id) {
					cm.dLogf("leader stepping down: not in committed configuration")
					cm.mu.Unlock()
					cm.becomeFollower(cm.currentTerm)
					return
				}
				cm.mu.Unlock()
				cm.processLogs(newCommitIndex)
				cm.leaderSendAEs()
			} else {
				cm.mu.Unlock()
			}

		case <-cm.stepDown:
			cm.mu.Lock()
			cm.dLogf("leader stepping down")
			if cm.leadershipTransferFuture != nil {
				atomic.StoreInt32(&cm.leadershipTransferInProgress, 0)
				cm.leadershipTransferFuture.respond(nil)
				cm.leadershipTransferFuture = nil
			}
			cm.mu.Unlock()
			return

		case <-heartbeatTicker.C:
			cm.leaderSendAEs()

		case future := <-cm.configurationChangeChIfStable():
			cm.appendConfigurationEntry(future)

		case future := <-cm.leadershipTransferCh:
			// Запуск graceful передачи лидерства.
			// Блокирует leaderLoop до завершения передачи или таймаута.
			cm.handleLeadershipTransfer(future)

		case <-cm.shutdownCh:
			return
		}
	}
}

// configurationChangeChIfStable возвращает канал confChangeCh только когда
// конфигурация стабильна (latest == committed), noop лидера закоммичен
// и сервер всё ещё является лидером.
func (cm *ConsensusModule) configurationChangeChIfStable() chan *configurationChangeFuture {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state != Leader {
		return nil
	}
	if cm.configurations.latestIndex != cm.configurations.committedIndex {
		return nil
	}
	if cm.commitIndex < cm.leaderStartIndex {
		return nil
	}
	return cm.confChangeCh
}

// AddVoter добавляет новый сервер как voter или обновляет адрес существующего.
func (cm *ConsensusModule) AddVoter(id ServerID, addr ServerAddress) IndexFuture {
	future := &configurationChangeFuture{
		req: configurationChangeRequest{
			command:       AddVoter,
			serverID:      id,
			serverAddress: addr,
		},
	}
	future.init(cm.shutdownCh)

	if err := cm.enqueueConfigurationChange(future); err != nil {
		return errorFuture{err}
	}
	return future
}

// AddNonvoter добавляет новый сервер как nonvoter или обновляет адрес существующего.
func (cm *ConsensusModule) AddNonvoter(id ServerID, addr ServerAddress) IndexFuture {
	future := &configurationChangeFuture{
		req: configurationChangeRequest{
			command:       AddNonvoter,
			serverID:      id,
			serverAddress: addr,
		},
	}
	future.init(cm.shutdownCh)

	if err := cm.enqueueConfigurationChange(future); err != nil {
		return errorFuture{err}
	}
	return future
}

// DemoteVoter понижает voter до nonvoter.
func (cm *ConsensusModule) DemoteVoter(id ServerID) IndexFuture {
	future := &configurationChangeFuture{
		req: configurationChangeRequest{
			command:  DemoteVoter,
			serverID: id,
		},
	}
	future.init(cm.shutdownCh)

	if err := cm.enqueueConfigurationChange(future); err != nil {
		return errorFuture{err}
	}
	return future
}

// RemoveServer удаляет сервер из конфигурации кластера.
func (cm *ConsensusModule) RemoveServer(id ServerID) IndexFuture {
	future := &configurationChangeFuture{
		req: configurationChangeRequest{
			command:  RemoveServer,
			serverID: id,
		},
	}
	future.init(cm.shutdownCh)

	if err := cm.enqueueConfigurationChange(future); err != nil {
		return errorFuture{err}
	}
	return future
}

// GetConfiguration возвращает текущую конфигурацию кластера.
// Для лидера возвращает latest конфигурацию; для follower — committed.
func (cm *ConsensusModule) GetConfiguration() ConfigurationFuture {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return &configurationsFuture{
		config: cm.configurations.latest,
	}
}

// enqueueConfigurationChange отправляет запрос на изменение конфигурации
// в канал confChangeCh лидера. Возвращает ошибку, если сервер не лидер.
func (cm *ConsensusModule) enqueueConfigurationChange(future *configurationChangeFuture) error {
	cm.mu.Lock()
	if cm.state != Leader {
		cm.mu.Unlock()
		return ErrNotLeader
	}
	// Во время передачи лидерства изменения конфигурации блокируются.
	if atomic.LoadInt32(&cm.leadershipTransferInProgress) != 0 {
		cm.mu.Unlock()
		return ErrLeadershipTransferInProgress
	}
	cm.mu.Unlock()

	select {
	case cm.confChangeCh <- future:
		return nil
	case <-cm.shutdownCh:
		return ErrRaftShutdown
	}
}

// setInitialConfiguration создаёт начальную конфигурацию на основе peerIds.
// Все серверы из peerIds становятся voter'ами.
func (cm *ConsensusModule) setInitialConfiguration() {
	servers := make([]ConfigServer, 0, len(cm.peerIds)+1)
	servers = append(servers, ConfigServer{
		ID:       ServerID(cm.id),
		Address:  ServerAddress(fmt.Sprintf("%d", cm.id)),
		Suffrage: Voter,
	})
	for _, peerID := range cm.peerIds {
		servers = append(servers, ConfigServer{
			ID:       ServerID(peerID),
			Address:  ServerAddress(fmt.Sprintf("%d", peerID)),
			Suffrage: Voter,
		})
	}
	cfg := Configuration{ConfigServers: servers}
	cm.configurations = configurations{
		committed:      cfg,
		committedIndex: 0,
		latest:         cfg,
		latestIndex:    0,
	}
}

// appendConfigurationEntry записывает запись LogConfiguration в журнал лидера
// и запускает репликацию. Вызывается из leaderLoop.
func (cm *ConsensusModule) appendConfigurationEntry(future *configurationChangeFuture) {
	cm.mu.Lock()
	nextCfg, err := nextConfiguration(
		cm.configurations.committed,
		cm.configurations.committedIndex,
		future.req,
	)
	if err != nil {
		cm.mu.Unlock()
		future.respond(err)
		return
	}

	data, err := EncodeConfiguration(nextCfg)
	if err != nil {
		cm.mu.Unlock()
		future.respond(err)
		return
	}

	entry := &logFuture{
		log: LogEntry{
			Type:    LogConfiguration,
			Command: data,
		},
	}
	entry.init(cm.shutdownCh)

	cm.configurations.latest = nextCfg
	cm.configurations.latestIndex = len(cm.log)

	cm.dispatchLogsUnsafe([]*logFuture{entry})
	cm.configurations.latestIndex = entry.log.Index
	future.index = entry.log.Index

	cm.commitmentTracker.setConfiguration(voterIDs(nextCfg), cm.lookupTerm)

	cm.commitmentTracker.commit(len(cm.log)-1, cm.lookupTerm)
	savedCommitIndex := cm.commitIndex
	if newCI := cm.commitmentTracker.getCommitIndex(); newCI > cm.commitIndex {
		cm.dLogf("leader sets commitIndex := %d", newCI)
		cm.commitIndex = newCI
	}
	cm.mu.Unlock()

	if cm.commitIndex > savedCommitIndex {
		cm.processLogs(cm.commitIndex)
	}
	cm.leaderSendAEs()

	go func() {
		err := entry.Error()
		if err == nil {
			cm.mu.Lock()
			cm.startStopReplication(nextCfg)
			cm.configurations.committed = nextCfg
			cm.configurations.committedIndex = entry.log.Index
			cm.mu.Unlock()
		}
		future.respond(err)
	}()
}

// startStopReplication обновляет набор реплицируемых peer'ов в соответствии
// с новой конфигурацией. Добавляет новые серверы в nextIndex/matchIndex
// и удаляет отсутствующие.
func (cm *ConsensusModule) startStopReplication(cfg Configuration) {
	for peerID := range cm.nextIndex {
		if peerID == cm.id {
			continue
		}
		found := false
		for _, s := range cfg.ConfigServers {
			if int(s.ID) == peerID {
				found = true
				break
			}
		}
		if !found {
			delete(cm.nextIndex, peerID)
			delete(cm.matchIndex, peerID)
		}
	}
	for _, s := range cfg.ConfigServers {
		peerID := int(s.ID)
		if peerID == cm.id {
			continue
		}
		if _, ok := cm.nextIndex[peerID]; !ok {
			cm.nextIndex[peerID] = len(cm.log)
			cm.matchIndex[peerID] = -1
		}
	}
}

// rebuildConfigurations восстанавливает конфигурации из журнала после
// загрузки из хранилища. Сканирует лог в поиске LogConfiguration записей.
func (cm *ConsensusModule) rebuildConfigurations() {
	cm.setInitialConfiguration()
	for i, entry := range cm.log {
		if entry.Type == LogConfiguration {
			entryCmd, ok := entry.Command.([]byte)
			if !ok {
				continue
			}
			cfg, err := DecodeConfiguration(entryCmd)
			if err != nil {
				continue
			}
			cm.configurations.committed = cm.configurations.latest
			cm.configurations.committedIndex = cm.configurations.latestIndex
			cm.configurations.latest = cfg
			cm.configurations.latestIndex = i
		}
	}
}

// processConfigurationLogEntry обрабатывает LogConfiguration запись
// на follower при репликации.
func (cm *ConsensusModule) processConfigurationLogEntry(entry *LogEntry) {
	entryCmd, ok := entry.Command.([]byte)
	if !ok {
		return
	}
	cfg, err := DecodeConfiguration(entryCmd)
	if err != nil {
		return
	}
	cm.configurations.committed = cm.configurations.latest
	cm.configurations.committedIndex = cm.configurations.latestIndex
	cm.configurations.latest = cfg
	cm.configurations.latestIndex = entry.Index
}

// processLogConflict вызывается при конфликте логов (перезаписи) в
// AppendEntries. Если конфликт затрагивает latest конфигурацию,
// откатываем её до committed.
func (cm *ConsensusModule) processLogConflict(conflictIndex int) {
	if conflictIndex <= cm.configurations.latestIndex && conflictIndex > 0 {
		cm.configurations.latest = cm.configurations.committed
		cm.configurations.latestIndex = cm.configurations.committedIndex
	}
}

// persistToStorage сохраняет постоянное состояние CM в cm.storage.
func (cm *ConsensusModule) persistToStorage() {
	var termData bytes.Buffer
	if err := gob.NewEncoder(&termData).Encode(cm.currentTerm); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("currentTerm", termData.Bytes())

	var votedData bytes.Buffer
	if err := gob.NewEncoder(&votedData).Encode(cm.votedFor); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("votedFor", votedData.Bytes())

	var logData bytes.Buffer
	if err := gob.NewEncoder(&logData).Encode(cm.log); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("log", logData.Bytes())
}
