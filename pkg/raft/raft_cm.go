package raft

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"math/rand"
	"os"
	"sort"
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
	storage Storage

	transport Transport

	fsm         FSM
	fsmMutateCh chan []*commitTuple
	shutdownCh  chan struct{}

	applyCh  chan *logFuture
	verifyCh chan *verifyFuture

	// inflight — карта future по индексу записи в журнале.
	// Обеспечивает O(1) поиск при отправке батча в FSM.
	inflight map[int]*logFuture

	commitCh          chan int
	stepDown          chan struct{}
	confChangeCh      chan *configurationChangeFuture
	configurationsCh  chan *configurationsFuture
	commitmentTracker *commitmentTracker

	// Постоянное состояние Raft на всех серверах
	currentTerm int
	votedFor    int
	log         []LogEntry

	// lastLogIndex — кэш индекса последней записи в журнале.
	// Обновляется через setLastLog при любом изменении журнала.
	lastLogIndex int
	// lastLogTerm — кэш терма последней записи в журнале.
	lastLogTerm int

	// Непостоянное состояние Raft на всех серверах
	commitIndex        int
	lastApplied        int
	leaderStartIndex   int
	state              CMState
	electionResetEvent time.Time
	electionTimerDone  chan struct{}

	// Непостоянное состояние Raft на лидерах
	nextIndex  map[int]int
	matchIndex map[int]int

	// peerReplications — карта долгоживущих контекстов репликации
	// по идентификатору peer.
	//
	// Инициализируется в startLeader при старте лидерства. Каждый entry
	// содержит per-peer горутину replicateLoop с триггер-каналом для
	// коалесцирующей отправки AppendEntries.
	//
	// При потере лидерства (becomeFollower) или остановке leaderLoop
	// все горутины останавливаются через stopCh, карта очищается.
	//
	// Инвариант:
	//   peerReplications[peerID] != nil  — peer активен в конфигурации
	//   peerReplications[peerID] == nil  — peer не в конфигурации
	peerReplications map[int]*peerReplication

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

	// snapshotStore — хранилище снэпшотов. Если nil, снэпшоты отключены.
	snapshotStore SnapshotStore

	// lastSnapshotIndex — индекс последнего созданного снэпшота.
	// Инициализируется как -1 (нет снэпшота).
	lastSnapshotIndex int

	// lastSnapshotTerm — терм последнего созданного снэпшота.
	lastSnapshotTerm int

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

	// restoreCh — канал для восстановления из снэпшота.
	restoreCh chan *restoreFuture

	// pendingVerify — очередь verifyFuture, ожидающих подтверждения
	// от follower-ов. Каждый успешный AppendEntries ответ голосует за
	// все ожидающие verifyFuture.
	pendingVerify []*verifyFuture
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
			(args.PrevLogIndex <= cm.lastLogIndex && cm.lookupTerm(args.PrevLogIndex) == args.PrevLogTerm) {
			reply.Success = true

			// Находит точку вставки — место, где происходит несовпадение термов
			// между существующими записями журнала, начиная с PrevLogIndex+1,
			// и новыми записями, отправленными лидером через RPC.
			// Используем logPosition для поиска позиции в срезе после компактирования.
			logInsertIndex := args.PrevLogIndex + 1
			logInsertPos := cm.logPosition(logInsertIndex)
			newEntriesIndex := 0

			for logInsertPos < len(cm.log) && newEntriesIndex < len(args.Entries) {
				if cm.log[logInsertPos].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertPos++
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
				cm.log = append(cm.log[:logInsertPos], args.Entries[newEntriesIndex:]...)
				cm.dLogf("... log is now: %v", cm.log)
				cm.rebuildLastLog()
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
				cm.commitIndex = min(args.LeaderCommit, cm.lastLogIndex)
				cm.dLogf("... setting commitIndex=%d", cm.commitIndex)
				cm.persistToStorage()
				commitIdx := cm.commitIndex
				cm.mu.Unlock()
				cm.processLogs(commitIdx)
				cm.mu.Lock()
			}
		} else {
			// Не найдено совпадение для PrevLogIndex/PrevLogTerm.
			if args.PrevLogIndex > cm.lastLogIndex {
				reply.ConflictIndex = cm.lastLogIndex + 1
				reply.ConflictTerm = -1
			} else {
				reply.ConflictTerm = cm.lookupTerm(args.PrevLogIndex)

				// Ищем первую запись с данным термом, двигаясь назад от PrevLogIndex.
				// Используем бинарный поиск вместо линейного, так как лог может
				// быть компактирован, и prevLogIndex может быть вне среза.
				pos := cm.logPosition(args.PrevLogIndex)
				i := pos - 1
				for i >= 0 && cm.log[i].Term == reply.ConflictTerm {
					i--
				}
				reply.ConflictIndex = cm.log[i+1].Index
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
			Type: LogCommand,
			Data: command,
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

// GetConfiguration возвращает текущую конфигурацию кластера.
// Для лидера возвращает latest конфигурацию; для follower — committed.
func (cm *ConsensusModule) GetConfiguration() ConfigurationFuture {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return &configurationsFuture{
		config: cm.configurations.latest,
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

// Report отчет о состоянии данного CM.
func (cm *ConsensusModule) Report() (id, term int, isLeader bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.id, cm.currentTerm, cm.state == Leader
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

// RequestVote обрабатывает входящий запрос голосования (§5.4.1).
//
// Кандидаты, не являющиеся voter'ами в текущей конфигурации, отклоняются.
// Nonvoter не может быть избран лидером ни при каких обстоятельствах.
func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
		return nil
	}

	if err := checkRPCHeader(&args); err != nil {
		return err
	}

	// Nonvoter не может быть избран лидером — отклоняем запрос.
	if !hasVote(cm.configurations.latest, args.GetRPCHeader().ServerID) {
		cm.dLogf("... RequestVote denied: not a voter")
		reply.VoteGranted = false
		reply.RPCHeader = RPCHeader{
			ProtocolVersion: ProtocolVersion,
			ServerID:        cm.id,
		}
		reply.Term = cm.currentTerm
		return nil
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

// SetSnapshotConfig обновляет параметры снэпшотов и сигнализирует
// runSnapshots о необходимости проверить, нужно ли создать снэпшот.
func (cm *ConsensusModule) SetSnapshotConfig(threshold int, interval time.Duration, trailing int) {
	cm.mu.Lock()
	cm.snapshotThreshold = threshold
	cm.snapshotInterval = interval
	cm.trailingLogs = trailing
	cm.mu.Unlock()

	select {
	case cm.snapshotCh <- struct{}{}:
	default:
	}
}

// Stop останавливает этот CM. Безопасен для многократного вызова.
func (cm *ConsensusModule) Stop() {
	cm.mu.Lock()
	if cm.state == Dead {
		cm.mu.Unlock()
		return
	}
	cm.state = Dead
	cm.mu.Unlock()

	cm.dLogf("CM.Stop called / becomes Dead")
	close(cm.shutdownCh)
}

// VerifyLeader проверяет, что текущий узел всё ещё является лидером,
// используя механизм ReadIndex (Raft §8). Возвращает Future, которая
// завершится с ошибкой, если лидерство утеряно.
//
// Если узел не лидер, немедленно возвращает ошибку ErrNotLeader.
// Вызов на лидере проверяет кворум без записи в журнал (ReadIndex).
func (cm *ConsensusModule) VerifyLeader() Future {
	cm.mu.Lock()
	isLeader := cm.state == Leader
	cm.mu.Unlock()
	if !isLeader {
		return errorFuture{err: ErrNotLeader}
	}
	vf := &verifyFuture{
		quorumSize: len(cm.peerIds)/2 + 1,
		votes:      0,
		notifyCh:   make(chan *verifyFuture, len(cm.peerIds)),
	}
	vf.init(cm.shutdownCh)
	select {
	case cm.verifyCh <- vf:
		return vf
	case <-cm.shutdownCh:
		return errorFuture{err: ErrRaftShutdown}
	}
}

// Non exported

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
			Type: LogConfiguration,
			Data: data,
		},
	}
	entry.init(cm.shutdownCh)

	cm.configurations.latest = nextCfg
	cm.configurations.latestIndex = cm.lastLogIndex + 1

	cm.dispatchLogsUnsafe([]*logFuture{entry})
	cm.configurations.latestIndex = entry.log.Index
	future.index = entry.log.Index

	cm.commitmentTracker.setConfiguration(voterIDs(nextCfg), cm.lookupTerm)

	cm.commitmentTracker.commit(cm.lastLogIndex, cm.lookupTerm)
	savedCommitIndex := cm.commitIndex
	if newCI := cm.commitmentTracker.getCommitIndex(); newCI > cm.commitIndex {
		cm.dLogf("leader sets commitIndex := %d", newCI)
		cm.commitIndex = newCI
	}
	cm.mu.Unlock()

	if cm.commitIndex > savedCommitIndex {
		cm.processLogs(cm.commitIndex)
	}
	cm.triggerAllPeers()

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

// becomeFollower делает cm последователем и сбрасывает его состояние.
// Если cm был лидером, отправляет сигнал в stepDown, чтобы leaderLoop
// завершил работу и разрешил все ожидающие future с ErrLeadershipLost.
// Ожидается, что cm.mu будет заблокирован.
func (cm *ConsensusModule) becomeFollower(term int) {
	wasLeader := cm.state == Leader
	cm.dLogf("becomes Follower with term=%d; log=%v", term, cm.log)

	// Остановить все per-peer горутины репликации.
	// ВАЖНО: close(stopCh) без ожидания doneCh, так как ожидание
	// под cm.mu приводит к дедлоку: replicateLoop может быть внутри
	// leaderSendAEsToPeer, ожидающей cm.mu.
	for peerID, pr := range cm.peerReplications {
		close(pr.stopCh)
		delete(cm.peerReplications, peerID)
	}

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

// compactLogs удаляет записи из cm.log с Index < compactIndex.
// Оставляет как минимум trailingLogs записей.
// Вызывается под cm.mu.Lock().
func (cm *ConsensusModule) compactLogs(compactIndex int) {
	if compactIndex <= 0 {
		return
	}
	pos := cm.logPosition(compactIndex)
	if pos <= 0 {
		return
	}
	kept := make([]LogEntry, len(cm.log)-pos)
	copy(kept, cm.log[pos:])
	cm.log = kept
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
	cm.commitmentTracker.commit(cm.lastLogIndex, cm.lookupTerm)
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
	lastIndex := cm.lastLogIndex
	now := time.Now()
	for _, f := range applyLogs {
		f.dispatch = now
		lastIndex++
		f.log.Index = lastIndex
		f.log.Term = term
		cm.log = append(cm.log, f.log)
	}
	cm.setLastLog(lastIndex, term)
	cm.persistToStorage()
	cm.matchIndex[cm.id] = lastIndex

	for _, f := range applyLogs {
		cm.inflight[f.log.Index] = f
	}
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

// handleFsmSnapshot обрабатывает запрос на создание снэпшота от runFSM.
// Вызывается из горутины runFSM при получении reqSnapshotFuture.
// Захватывает текущий lastApplied и терм, вызывает fsm.Snapshot().
func (cm *ConsensusModule) handleFsmSnapshot(req *reqSnapshotFuture) {
	if cm.lastApplied < 0 {
		req.respond(ErrNothingNewToSnapshot)
		return
	}
	snap, err := cm.fsm.Snapshot()
	if err != nil {
		req.respond(err)
		return
	}
	req.index = cm.lastApplied
	req.term = cm.lookupTerm(cm.lastApplied)
	req.snapshot = snap
	req.respond(nil)
}

// Получает снэпшот от лидера, сохраняет в SnapshotStore,
// восстанавливает FSM и заменяет журнал.
func (cm *ConsensusModule) handleInstallSnapshot(rpc RPC, req *InstallSnapshotRequest) {
	resp := &InstallSnapshotResponse{
		RPCHeader: RPCHeader{
			ProtocolVersion: ProtocolVersion,
			ServerID:        cm.id,
		},
		Term:    cm.currentTerm,
		Success: false,
	}
	var rpcErr error
	defer func() {
		_, _ = io.Copy(io.Discard, rpc.Reader)
		rpc.RespChan <- RPCResponse{Reply: resp, Error: rpcErr}
	}()

	cm.mu.Lock()
	if req.Term < cm.currentTerm {
		cm.mu.Unlock()
		return
	}
	if req.Term > cm.currentTerm {
		cm.becomeFollower(req.Term)
		resp.Term = req.Term
	}
	cm.electionResetEvent = time.Now()
	cm.leaderLastContact = time.Now()
	cm.leaderID = req.LeaderID
	cm.mu.Unlock()

	cfg, err := DecodeConfiguration(req.Configuration)
	if err != nil {
		rpcErr = err
		return
	}

	sink, err := cm.snapshotStore.Create(req.LastLogIndex, req.LastLogTerm, cfg, req.ConfigIndex)
	if err != nil {
		rpcErr = err
		return
	}

	n, err := io.Copy(sink, rpc.Reader)
	if err != nil {
		_ = sink.Cancel()
		rpcErr = err
		return
	}
	if req.DataSize > 0 && n != req.DataSize {
		_ = sink.Cancel()
		rpcErr = fmt.Errorf("snapshot size mismatch: got %d, expected %d", n, req.DataSize)
		return
	}

	if err := sink.Close(); err != nil {
		rpcErr = err
		return
	}

	// Restore через прямой вызов fsm.Restore.
	meta, snapReader, err := cm.snapshotStore.Open(sink.ID())
	if err != nil {
		rpcErr = err
		return
	}
	defer snapReader.Close()

	if err := cm.fsm.Restore(snapReader); err != nil {
		rpcErr = err
		return
	}

	cm.mu.Lock()
	cm.lastLogIndex = meta.Index
	cm.lastLogTerm = meta.Term
	cm.lastSnapshotIndex = meta.Index
	cm.lastSnapshotTerm = meta.Term
	cm.lastApplied = meta.Index
	cm.commitIndex = meta.Index
	// Заменить журнал: удалить все записи <= LastLogIndex.
	pos := cm.logPosition(meta.Index)
	if pos < len(cm.log) && cm.log[pos].Index == meta.Index {
		kept := make([]LogEntry, len(cm.log)-pos-1)
		copy(kept, cm.log[pos+1:])
		cm.log = kept
	} else {
		cm.log = nil
	}
	cm.rebuildLastLog()
	cm.persistToStorage()
	cm.mu.Unlock()

	resp.Success = true
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
	lastIdx := cm.lastLogIndex
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

// lastLogIndexAndTerm возвращает кэшированный индекс и терм
// последней записи в журнале. Не вычисляет из len(cm.log).
// Требует удержания cm.mu (RLock или Lock).
func (cm *ConsensusModule) lastLogIndexAndTerm() (int, int) {
	return cm.lastLogIndex, cm.lastLogTerm
}

// logPosition возвращает позицию в срезе cm.log для записи с данным Index.
// Использует бинарный поиск. Если запись не найдена, возвращает
// позицию первой записи с Index >= заданного (место для вставки).
// Вызывается под cm.mu.Lock().
func (cm *ConsensusModule) logPosition(index int) int {
	return sort.Search(len(cm.log), func(i int) bool {
		return cm.log[i].Index >= index
	})
}

// lookupTerm возвращает терм для записи с указанным index.
// Использует бинарный поиск: после внедрения снэпшотов позиция
// записи в cm.log может не совпадать с её индексом.
// Требует удержания cm.mu (RLock или Lock).
func (cm *ConsensusModule) lookupTerm(index int) int {
	lo, hi := 0, len(cm.log)-1
	for lo <= hi {
		mid := (lo + hi) / 2
		if cm.log[mid].Index == index {
			return cm.log[mid].Term
		} else if cm.log[mid].Index < index {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return -1
}

// processConfigurationLogEntry обрабатывает LogConfiguration запись
// на follower при репликации.
func (cm *ConsensusModule) processConfigurationLogEntry(entry *LogEntry) {
	entryData, ok := entry.Data.([]byte)
	if !ok {
		return
	}
	cfg, err := DecodeConfiguration(entryData)
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

// processLogs собирает закоммиченные записи от lastApplied+1 до commitIndex
// и отправляет их в FSM goroutine. Диапазон делится на под-батчи размером
// не более maxApplyBatchSize для снижения latency FSM.
// Метод не требует удержания cm.mu при вызове, но сам захватывает её
// внутри для чтения журнала и поиска future в карте inflight.
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

// rebuildConfigurations восстанавливает конфигурации из журнала после
// загрузки из хранилища. Сканирует лог в поиске LogConfiguration записей.
func (cm *ConsensusModule) rebuildConfigurations() {
	cm.setInitialConfiguration()
	for i, entry := range cm.log {
		if entry.Type == LogConfiguration {
			entryData, ok := entry.Data.([]byte)
			if !ok {
				continue
			}
			cfg, err := DecodeConfiguration(entryData)
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

// rebuildLastLog восстанавливает кэш lastLogIndex/lastLogTerm
// из текущего содержимого cm.log.
// Вызывается из restoreFromStorage() и других мест, где журнал
// мог измениться без вызова setLastLog.
// Требует удержания cm.mu (Lock).
func (cm *ConsensusModule) rebuildLastLog() {
	if len(cm.log) > 0 {
		last := cm.log[len(cm.log)-1]
		cm.lastLogIndex = last.Index
		cm.lastLogTerm = last.Term
	} else {
		cm.lastLogIndex = -1
		cm.lastLogTerm = -1
	}
}

// runElectionTimer реализует таймер выборов. Она должна запускаться всякий раз, когда
// мы хотим запустить таймер для выдвижения в качестве кандидата на новых выборах.
//
// Эта функция является блокирующей и должна запускаться в отдельной горутине;
// она предназначена для работы с одним (одноразовым) таймером выборов, поскольку она завершается
// всякий раз, когда состояние CM меняется с «ведомый/кандидат» или меняется срок полномочий.
// runElectionTimer запускает таймер выборов для узла. Если узел не является
// voter'ом (Suffrage == Nonvoter), таймер немедленно завершается — nonvoter
// никогда не участвует в выборах и не может стать кандидатом.
func (cm *ConsensusModule) runElectionTimer() {
	timeoutDuration := cm.electionTimeout()
	cm.mu.Lock()
	termStarted := cm.currentTerm
	electionTimerDone := cm.electionTimerDone

	// Nonvoter не участвует в выборах — не запускаем таймер.
	if !hasVote(cm.configurations.latest, cm.id) {
		cm.mu.Unlock()
		return
	}

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
		case req := <-cm.fsmSnapshotCh:
			cm.handleFsmSnapshot(req)
		case <-cm.shutdownCh:
			return
		}
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
		if len(cm.pendingVerify) > 0 {
			for _, vf := range cm.pendingVerify {
				vf.respond(ErrLeadershipLost)
			}
			cm.pendingVerify = nil
		}
		inflightCount := len(cm.inflight)
		cm.dLogf("leaderLoop exit: responding to %d inflight futures", inflightCount)
		for _, future := range cm.inflight {
			future.respond(ErrLeadershipLost)
		}
		cm.inflight = make(map[int]*logFuture)

		// Остановить все per-peer горутины репликации.
		// ВАЖНО: close(stopCh) без ожидания doneCh — replicateLoop
		// может быть заблокирован в leaderSendAEsToPeer (ожидание RPC
		// или попытка захвата cm.mu), что приведёт к дедлоку.
		for peerID, pr := range cm.peerReplications {
			close(pr.stopCh)
			delete(cm.peerReplications, peerID)
		}
		cm.mu.Unlock()
	}()

	heartbeatTicker := time.NewTicker(HeartbeatTimeoutMs * time.Millisecond)
	defer heartbeatTicker.Stop()

	applyTicker := time.NewTicker(applyBatchInterval)
	defer applyTicker.Stop()

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
			for i := 0; i < leaderBatchSize-1; i++ {
				select {
				case f := <-cm.applyCh:
					ready = append(ready, f)
				default:
					break groupCommit
				}
			}
			cm.dispatchLogs(ready)
			// Немедленная репликация после записи в журнал.
			cm.triggerAllPeers()

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
				// Не вызываем processLogs здесь — накапливаем commitIndex
				// до следующего тика applyTicker для объединения батчей.
				cm.triggerAllPeers()
			} else {
				cm.mu.Unlock()
			}

		case <-applyTicker.C:
			cm.mu.Lock()
			ci := cm.commitIndex
			cm.mu.Unlock()
			if ci > cm.lastApplied {
				cm.processLogs(ci)
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
			cm.triggerAllPeers()

		case future := <-cm.configurationChangeChIfStable():
			cm.appendConfigurationEntry(future)

		case vf := <-cm.verifyCh:
			cm.mu.Lock()
			if cm.state != Leader {
				vf.respond(ErrNotLeader)
				cm.mu.Unlock()
				break
			}
			vf.vote(true)
			if vf.votes >= vf.quorumSize {
				cm.mu.Unlock()
				break
			}
			cm.pendingVerify = append(cm.pendingVerify, vf)
			cm.mu.Unlock()
			if len(cm.pendingVerify) == 1 {
				cm.triggerAllPeers()
			}

		case future := <-cm.leadershipTransferCh:
			// Запуск graceful передачи лидерства.
			// Блокирует leaderLoop до завершения передачи или таймаута.
			cm.handleLeadershipTransfer(future)

		case <-cm.shutdownCh:
			return
		}
	}
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
			case *InstallSnapshotRequest:
				cm.handleInstallSnapshot(rpc, cmd)
			default:
				rpc.RespChan <- RPCResponse{Error: ErrNotImplemented}
			}
		case <-cm.shutdownCh:
			return
		}
	}
}

// runSnapshots — фоновая горутина управления снэпшотами.
// Запускается только если cm.snapshotStore != nil.
// Проверяет необходимость снэпшота по таймеру или при сигнале из snapshotCh.
// Завершается при закрытии shutdownCh.
func (cm *ConsensusModule) runSnapshots() {
	for {
		cm.mu.Lock()
		interval := cm.snapshotInterval
		cm.mu.Unlock()

		select {
		case <-cm.snapshotCh:
			if cm.shouldSnapshot() {
				_ = cm.takeSnapshot()
			}
		case <-time.After(interval):
			if cm.shouldSnapshot() {
				_ = cm.takeSnapshot()
			}
		case <-cm.shutdownCh:
			return
		}
	}
}

// sendBatch собирает под-батч записей журнала от start до end включительно
// и отправляет в fsmMutateCh. Поиск future в inflight выполняется за O(1)
// через map[int]*logFuture. Блокировка cm.mu удерживается только на время
// поиска и чтения журнала, отправка — без блокировки.
func (cm *ConsensusModule) sendBatch(start, end int) {
	cm.mu.Lock()
	batch := make([]*commitTuple, 0, end-start+1)
	for idx := start; idx <= end; idx++ {
		future := cm.inflight[idx]
		delete(cm.inflight, idx)
		if idx > cm.lastLogIndex {
			continue
		}
		pos := cm.logPosition(idx)
		log := &cm.log[pos]
		batch = append(batch, &commitTuple{log: log, future: future})
	}
	cm.mu.Unlock()
	if len(batch) > 0 {
		cm.fsmMutateCh <- batch
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

// setLastLog обновляет кэш lastLogIndex/lastLogTerm.
// Вызывается после любого изменения журнала (добавление записей,
// удаление при конфликте, восстановление из storage).
// Требует удержания cm.mu (Lock).
func (cm *ConsensusModule) setLastLog(index, term int) {
	cm.lastLogIndex = index
	cm.lastLogTerm = term
}

// shouldSnapshot проверяет, нужно ли создать снэпшот.
// Снэпшот нужен, если с момента последнего снэпшота накопилось
// больше snapshotThreshold записей.
func (cm *ConsensusModule) shouldSnapshot() bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.lastLogIndex < 0 {
		return false
	}
	if cm.snapshotStore == nil {
		return false
	}
	delta := cm.lastLogIndex - cm.lastSnapshotIndex
	return delta >= cm.snapshotThreshold
}

// startElection запускает новые выборы с этим CM в качестве кандидата.
// Ожидается, что cm.mu будет заблокирован.
//
// Если candidateFromLeadershipTransfer установлен, в RequestVoteArgs
// передаётся LeadershipTransfer=true, что bypass проверку наличия лидера
// на узлах-получателях. Флаг сбрасывается после того, как все горутины
// отправили RequestVote, чтобы последующие обычные выборы не имели этого флага.
//
// Nonvoter'ы исключаются из рассылки RequestVote — они не участвуют
// в голосовании и не должны получать запросы на предоставление голоса.
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

	// Параллельно отправить RPC-запросы RequestVote всем voter'ам.
	for _, s := range cfg.ConfigServers {
		peerID := int(s.ID)
		if peerID == cm.id {
			continue
		}
		if s.Suffrage != Voter {
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
		cm.nextIndex[peerID] = cm.lastLogIndex + 1
		cm.matchIndex[peerID] = -1

		// Создать per-peer горутину репликации.
		pr := &peerReplication{
			peerID:    peerID,
			triggerCh: make(chan struct{}, 1),
			stopCh:    make(chan struct{}),
			doneCh:    make(chan struct{}),
		}
		cm.peerReplications[peerID] = pr
		go cm.replicateLoop(pr)
	}
	cm.dLogf(
		"becomes Leader; term=%d, nextIndex=%v, matchIndex=%v; log=%v",
		cm.currentTerm, cm.nextIndex, cm.matchIndex, cm.log,
	)

	cm.leaderStartIndex = cm.lastLogIndex
	cm.commitmentTracker = newCommitmentTracker(
		cm.id, cm.commitCh, cm.currentTerm, cm.lastLogIndex,
	)
	cm.commitmentTracker.setConfiguration(voterIDs(cfg), cm.lookupTerm)
	cm.commitmentTracker.setMatch(cm.id, cm.lastLogIndex, cm.lookupTerm)

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
			// Остановить горутину репликации для удалённого peer.
			// ВАЖНО: close(stopCh) без ожидания doneCh — см. becomeFollower.
			if pr, ok := cm.peerReplications[peerID]; ok {
				close(pr.stopCh)
				delete(cm.peerReplications, peerID)
			}
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
			cm.nextIndex[peerID] = cm.lastLogIndex + 1
			cm.matchIndex[peerID] = -1
		}
		if _, ok := cm.peerReplications[peerID]; !ok {
			// Создать горутину репликации для нового peer.
			pr := &peerReplication{
				peerID:    peerID,
				triggerCh: make(chan struct{}, 1),
				stopCh:    make(chan struct{}),
				doneCh:    make(chan struct{}),
			}
			cm.peerReplications[peerID] = pr
			go cm.replicateLoop(pr)
		}
	}
}

// takeSnapshot создаёт снэпшот и компактит лог.
// Вызывается только из runSnapshots.
func (cm *ConsensusModule) takeSnapshot() error {
	snapReq := &reqSnapshotFuture{}
	snapReq.init(cm.shutdownCh)

	select {
	case cm.fsmSnapshotCh <- snapReq:
	case <-cm.shutdownCh:
		return ErrRaftShutdown
	}

	if err := snapReq.Error(); err != nil {
		if err == ErrNothingNewToSnapshot {
			return nil
		}
		return err
	}
	defer snapReq.snapshot.Release()

	cm.mu.Lock()
	committed := cm.configurations.committed
	committedIndex := cm.configurations.committedIndex
	cm.mu.Unlock()

	sink, err := cm.snapshotStore.Create(snapReq.index, snapReq.term, committed, committedIndex)
	if err != nil {
		return fmt.Errorf("failed to create snapshot: %w", err)
	}

	if err := snapReq.snapshot.Persist(sink); err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("failed to persist snapshot: %w", err)
	}

	if err := sink.Close(); err != nil {
		return fmt.Errorf("failed to close snapshot: %w", err)
	}

	cm.mu.Lock()
	cm.lastSnapshotIndex = snapReq.index
	cm.lastSnapshotTerm = snapReq.term
	cm.compactLogs(snapReq.index - cm.trailingLogs)
	cm.persistToStorage()
	cm.mu.Unlock()

	return nil
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

// TODO переименовать

// dLogf выводит отладочное сообщение, если DebugCM > 0.
func (cm *ConsensusModule) dLogf(format string, args ...any) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d] ", cm.id) + format
		log.Printf(format, args...)
	}
}
