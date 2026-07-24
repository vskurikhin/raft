// Package raft Реализация ядра Raft — модуль консенсуса.
// This code is in the public domain.
package raft

import (
	"bytes"
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
	DebugCM = 0
	Quantum = 2

	HeartbeatTimeoutMs  = 5 * 13 * Quantum
	ReelectionTimeoutMs = 17 * 13 * Quantum
	TickerTimeoutMs     = 17 * Quantum

	//HeartbeatTimeoutMs  = 50 * Quantum
	//ReelectionTimeoutMs = 150 * Quantum
	//TickerTimeoutMs     = 10 * Quantum
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
	Candidate
	Leader
	Dead
)

func (s CMState) String() string {
	switch s {
	case Follower:
		return "Follower"
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

type LogEntry struct {
	Command any
	Term    int
}

// SnapshotHeader — метаданные, хранящиеся вместе с данными снепшота.
type SnapshotHeader struct {
	LastIncludedIndex int
	LastIncludedTerm  int
}

// InstallSnapshotArgs — аргументы RPC InstallSnapshot.
type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

// InstallSnapshotReply — ответ на InstallSnapshot RPC.
type InstallSnapshotReply struct {
	Term int
}

// ConsensusModule (CM) реализует единый узел консенсуса Raft.
type ConsensusModule struct {
	// mu защищает одновременный доступ к CM.
	mu sync.Mutex

	// id — идентификатор сервера этого экземпляра CM (ConsensusModule).
	id int

	// peerIds содержит список идентификаторов узлов-соседей в кластере.
	peerIds []int

	// server — это сервер, содержащий данный экземпляр CM.
	// Используется для отправки RPC-запросов другим узлам кластера.
	server *Server

	// storage is used to persist state.
	storage Storage

	// commitChan — канал, через который данный CM сообщает
	// о записях журнала, зафиксированных в кластере Raft.
	// Передаётся клиентом при создании CM.
	commitChan chan<- CommitEntry

	// snapshotChan — канал, через который CM передаёт данные снепшота
	// машине состояний. Создаётся клиентом и передаётся в конструктор.
	snapshotChan chan<- []byte

	// newCommitReadyChan — внутренний канал уведомлений, используемый
	// горутинами, которые фиксируют новые записи журнала, чтобы сообщить,
	// что эти записи могут быть отправлены в канал фиксации commitChan.
	// Отдельная горутина отслеживает этот канал и при получении уведомления
	// отправляет записи в commitChan; newCommitReadyChanWg используется для
	// ожидания завершения этой горутины, обеспечивая корректное завершение
	// работы.
	newCommitReadyChan   chan struct{}
	newCommitReadyChanWg sync.WaitGroup

	// triggerAEChan — внутренний канал уведомлений, используемый для запуска
	// отправки новых сообщений AppendEntries соседям, когда происходят
	// существенные изменения состояния.
	triggerAEChan chan struct{}

	// Постоянное состояние Raft на всех серверах
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Непостоянное состояние Raft на всех серверах
	commitIndex        int
	lastApplied        int
	state              CMState
	electionResetEvent time.Time
	electionTimerDone  chan struct{}

	// Непостоянное состояние Raft на лидерах
	nextIndex  map[int]int
	matchIndex map[int]int

	// Состояние снепшота
	lastIncludedIndex int
	lastIncludedTerm  int
	snapshotData      []byte

	// Параметры политики снепшотов
	snapshotThreshold int
	snapshotInterval  int
	snapshotDataFn    func() []byte
}

// NewConsensusModule создаёт новый экземпляр CM с указанными
// идентификатором, списком идентификаторов соседей и сервером.
// Канал ready уведомляет CM о том, что все соседи подключены
// и можно безопасно запускать его машину состояний.
// Канал фиксации будет использоваться CM для отправки записей журнала,
// зафиксированных кластером Raft.
func NewConsensusModule(
	id int,
	peerIds []int,
	server *Server,
	storage Storage,
	ready <-chan any,
	commitChan chan<- CommitEntry,
	snapshotChan chan<- []byte,
) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.id = id
	cm.peerIds = peerIds
	cm.server = server
	cm.storage = storage
	cm.commitChan = commitChan
	cm.snapshotChan = snapshotChan
	cm.newCommitReadyChan = make(chan struct{}, 16)
	cm.triggerAEChan = make(chan struct{}, 1)
	cm.state = Follower
	cm.votedFor = -1
	cm.commitIndex = -1
	cm.lastApplied = -1
	cm.nextIndex = make(map[int]int)
	cm.matchIndex = make(map[int]int)
	cm.electionTimerDone = make(chan struct{})

	if cm.storage.HasData() {
		cm.restoreFromStorage()
	}

	go func() {
		// CM находится в режиме ожидания, пока не будет подан сигнал готовности;
		// затем он запускает обратный отсчет до выборов лидера.
		<-ready
		cm.mu.Lock()
		cm.electionResetEvent = time.Now()
		cm.mu.Unlock()
		cm.runElectionTimer()
	}()

	cm.newCommitReadyChanWg.Add(1)
	go cm.commitChanSender()
	return cm
}

// Report отчет о состоянии данного CM.
func (cm *ConsensusModule) Report() (id, term int, isLeader bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.id, cm.currentTerm, cm.state == Leader
}

// Submit отправляет новую команду в CM. Этот метод не блокирует выполнение;
// клиенты читают канал фиксации, переданный в конструктор, чтобы получать
// уведомления о новых зафиксированных записях.
// Если данный CM является лидером, Submit возвращает индекс записи журнала,
// в который была добавлена команда. В противном случае возвращается -1.
func (cm *ConsensusModule) Submit(command any) int {
	cm.mu.Lock()
	if cm.state != Leader {
		cm.mu.Unlock()
		return -1
	}
	cm.dLogf("Submit received by %v: %v", cm.state, command)
	submitIndex := cm.getLogLength()
	cm.log = append(cm.log, LogEntry{Command: command, Term: cm.currentTerm})
	cm.persistToStorage()
	cm.dLogf("... log=%v", cm.log)
	cm.mu.Unlock()
	select {
	case cm.triggerAEChan <- struct{}{}:
	default:
	}
	return submitIndex
}

// TakeSnapshot сохраняет снепшот состояния машины состояний и обрезает журнал.
// stateMachineData — сериализованное состояние машины состояний.
// Предполагается, что cm.mu захвачен вызывающим.
func (cm *ConsensusModule) TakeSnapshot(stateMachineData []byte) {
	if stateMachineData == nil {
		panic("TakeSnapshot: stateMachineData is nil")
	}
	if cm.lastApplied < 0 {
		// Нет зафиксированных записей — снепшот бессмысленен.
		return
	}
	if cm.lastIncludedIndex > 0 && cm.lastApplied <= cm.lastIncludedIndex {
		// Снепшот уже покрывает последнее применение — ничего не делать.
		return
	}

	snapIndex := cm.lastApplied
	// snapIndex — логический индекс. Преобразуем в физический.
	snapTerm := cm.log[snapIndex-cm.logOffset()].Term

	cm.lastIncludedIndex = snapIndex
	cm.lastIncludedTerm = snapTerm
	cm.snapshotData = stateMachineData

	// Обрезать журнал: оставить записи с логическим индексом > snapIndex.
	// До снепшота физические индексы совпадают с логическими (offset был 1,
	// lastIncludedIndex был 0). После снепшота offset = snapIndex + 1.
	keepFrom := snapIndex + 1
	if keepFrom >= len(cm.log) {
		cm.log = cm.log[:0]
	} else {
		cm.log = append([]LogEntry{}, cm.log[keepFrom:]...)
	}

	cm.dLogf(
		"snapshot taken: lastIncludedIndex=%d, lastIncludedTerm=%d, log truncated to %d entries",
		snapIndex, snapTerm, len(cm.log),
	)

	cm.persistToStorage()
}

// SetSnapshotPolicy устанавливает параметры политики автоматических снепшотов.
// threshold > 0 включает автоматические снепшоты; dataFn вызывается для получения
// данных машины состояний при создании снепшота.
func (cm *ConsensusModule) SetSnapshotPolicy(threshold, interval int) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.snapshotThreshold = threshold
	cm.snapshotInterval = interval
}

// SetSnapshotDataFn устанавливает функцию, вызываемую для получения данных
// машины состояний при создании снепшота.
func (cm *ConsensusModule) SetSnapshotDataFn(dataFn func() []byte) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.snapshotDataFn = dataFn
}

// Stop останавливает этот CM, очищая его состояние. Этот метод быстро возвращает результат,
// но для завершения работы всех горутин может потребоваться некоторое время (до ~таймаута выборов).
func (cm *ConsensusModule) Stop() {
	cm.dLogf("CM.Stop called")
	cm.mu.Lock()
	cm.state = Dead
	cm.mu.Unlock()
	cm.dLogf("becomes Dead")

	// Close the commit notification channel, and wait for the goroutine that
	// monitors it to exit.
	close(cm.newCommitReadyChan)
	cm.newCommitReadyChanWg.Wait()
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

	// Новые ключи снепшота (необязательные — обратная совместимость)
	cm.lastIncludedIndex = 0
	cm.lastIncludedTerm = 0
	cm.snapshotData = nil

	if idxData, found := cm.storage.Get("lastIncludedIndex"); found {
		d := gob.NewDecoder(bytes.NewBuffer(idxData))
		if err := d.Decode(&cm.lastIncludedIndex); err != nil {
			log.Fatal(err)
		}
	}
	if termData, found := cm.storage.Get("lastIncludedTerm"); found {
		d := gob.NewDecoder(bytes.NewBuffer(termData))
		if err := d.Decode(&cm.lastIncludedTerm); err != nil {
			log.Fatal(err)
		}
	}
	if snapData, found := cm.storage.Get("snapshot"); found && len(snapData) > 0 {
		cm.snapshotData = snapData
	}
}

// persistToStorage сохраняет всё постоянное состояние CM в cm.storage.
// Предполагается, что cm.mu уже заблокирован.
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

	// Новые ключи снепшота
	var snapIdxBuf bytes.Buffer
	if err := gob.NewEncoder(&snapIdxBuf).Encode(cm.lastIncludedIndex); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("lastIncludedIndex", snapIdxBuf.Bytes())

	var snapTermBuf bytes.Buffer
	if err := gob.NewEncoder(&snapTermBuf).Encode(cm.lastIncludedTerm); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("lastIncludedTerm", snapTermBuf.Bytes())

	cm.storage.Set("snapshot", cm.snapshotData)
}

// dLogf выводит отладочное сообщение, если DebugCM > 0.
func (cm *ConsensusModule) dLogf(format string, args ...any) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d] ", cm.id) + format
		log.Printf(format, args...)
	}
}

// getLogEntry возвращает запись журнала по логическому индексу i.
// Если индекс выходит за пределы доступных записей (включая снепшот),
// возвращает false.
// Смещение: если снепшот не создавался (lastIncludedIndex == 0), offset = 0,
// и логические индексы совпадают с физическими для обратной совместимости.
// После снепшота offset = lastIncludedIndex + 1.
func (cm *ConsensusModule) getLogEntry(i int) (LogEntry, bool) {
	offset := cm.logOffset()
	if i < offset || i >= offset+len(cm.log) {
		return LogEntry{}, false
	}
	return cm.log[i-offset], true
}

// logOffset возвращает смещение между физическим и логическим индексом.
// Если снепшот не создавался — 0 (совпадают).
// После снепшота — lastIncludedIndex + 1.
func (cm *ConsensusModule) logOffset() int {
	if cm.lastIncludedIndex > 0 {
		return cm.lastIncludedIndex + 1
	}
	return 0
}

// getLogLength возвращает полную логическую длину журнала (с учётом снепшота).
// Для пустого журнала возвращает lastIncludedIndex.
func (cm *ConsensusModule) getLogLength() int {
	if cm.lastIncludedIndex > 0 {
		return cm.lastIncludedIndex + 1 + len(cm.log)
	}
	return len(cm.log)
}

// getLastLogIndex возвращает логический индекс последней записи журнала.
// Если журнал пуст, возвращает lastIncludedIndex.
func (cm *ConsensusModule) getLastLogIndex() int {
	if len(cm.log) > 0 {
		return cm.logOffset() + len(cm.log) - 1
	}
	return cm.lastIncludedIndex
}

// getLastLogTerm возвращает терм последней записи журнала.
// Если журнал пуст, возвращает lastIncludedTerm.
func (cm *ConsensusModule) getLastLogTerm() int {
	if len(cm.log) > 0 {
		return cm.log[len(cm.log)-1].Term
	}
	return cm.lastIncludedTerm
}

// RequestVoteArgs См. рисунок 2 в статье.
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote RPC.
func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
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

	if cm.currentTerm == args.Term &&
		(cm.votedFor == -1 || cm.votedFor == args.CandidateID) &&
		(args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		cm.votedFor = args.CandidateID
		cm.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = cm.currentTerm
	cm.persistToStorage()
	cm.dLogf("... RequestVote reply: %+v", reply)
	return nil
}

// AppendEntriesArgs См. рисунок 2 в статье.
type AppendEntriesArgs struct {
	Term     int
	LeaderID int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// Faster conflict resolution optimization (described near the end of section
	// 5.3 in the paper.)
	ConflictIndex int
	ConflictTerm  int
}

func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
		return nil
	}
	cm.dLogf("AppendEntries: %+v", args)

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

		// Проверяет, содержит ли наш журнал запись с индексом PrevLogIndex,
		// у которой терм совпадает с PrevLogTerm.
		// Обратите внимание, что в особом случае, когда PrevLogIndex == -1,
		// условие считается истинным автоматически.
		// Для PrevLogIndex, совпадающего с lastIncludedIndex, используем lastIncludedTerm.
		if args.PrevLogIndex == -1 {
			reply.Success = true
		} else if args.PrevLogIndex == cm.lastIncludedIndex && args.PrevLogTerm == cm.lastIncludedTerm {
			reply.Success = true
		} else if entry, ok := cm.getLogEntry(args.PrevLogIndex); ok && args.PrevLogTerm == entry.Term {
			reply.Success = true
		}

		if reply.Success {
			// Находит точку вставки — место, где происходит несовпадение термов
			// между существующими записями журнала, начиная с PrevLogIndex+1,
			// и новыми записями, отправленными лидером через RPC.
			offset := cm.logOffset()
			logInsertIndex := args.PrevLogIndex + 1
			newEntriesIndex := 0

			for logInsertIndex < offset+len(cm.log) && newEntriesIndex < len(args.Entries) {
				entry, ok := cm.getLogEntry(logInsertIndex)
				if !ok || entry.Term != args.Entries[newEntriesIndex].Term {
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
				// Обрезаем журнал до logInsertIndex (физический индекс)
				physIndex := logInsertIndex - offset
				physIndex = max(physIndex, 0)
				physIndex = min(physIndex, len(cm.log))
				cm.log = append(cm.log[:physIndex], args.Entries[newEntriesIndex:]...)
				cm.dLogf("... log is now: %v", cm.log)
			}

			// Устанавливает индекс фиксации.
			if args.LeaderCommit > cm.commitIndex {
				cm.commitIndex = min(args.LeaderCommit, cm.getLastLogIndex())
				cm.dLogf("... setting commitIndex=%d", cm.commitIndex)
				select {
				case cm.newCommitReadyChan <- struct{}{}:
				default:
				}
			}
		} else {
			// Не найдено совпадение для PrevLogIndex/PrevLogTerm.
			// Заполнить ConflictIndex и ConflictTerm, чтобы помочь лидеру
			// быстрее привести наш журнал в актуальное состояние.
			logLen := cm.getLogLength()
			if args.PrevLogIndex >= logLen {
				reply.ConflictIndex = logLen
				reply.ConflictTerm = -1
			} else {
				// PrevLogIndex указывает на существующую запись в нашем журнале,
				// но PrevLogTerm не совпадает с термом записи.
				entry, _ := cm.getLogEntry(args.PrevLogIndex)
				reply.ConflictTerm = entry.Term

				var i int
				for i = args.PrevLogIndex - 1; i >= cm.lastIncludedIndex; i-- {
					entry, ok := cm.getLogEntry(i)
					if !ok || entry.Term != reply.ConflictTerm {
						break
					}
				}
				reply.ConflictIndex = i + 1
			}
		}
	}

	reply.Term = cm.currentTerm
	cm.persistToStorage()
	cm.dLogf("AppendEntries reply: %+v", *reply)
	return nil
}

// InstallSnapshot — RPC, который лидер отправляет отстающему узлу.
// Получатель (follower) применяет снепшот, заменяет свой журнал и персистит.
func (cm *ConsensusModule) InstallSnapshot(
	args InstallSnapshotArgs,
	reply *InstallSnapshotReply,
) error {
	cm.mu.Lock()
	if cm.state == Dead {
		cm.mu.Unlock()
		return nil
	}

	cm.dLogf("InstallSnapshot: %+v [currentTerm=%d, lastIncludedIndex=%d]",
		args, cm.currentTerm, cm.lastIncludedIndex)

	// Шаг 1: проверка терма
	if args.Term < cm.currentTerm {
		reply.Term = cm.currentTerm
		cm.mu.Unlock()
		return nil
	}

	if args.Term > cm.currentTerm {
		cm.becomeFollower(args.Term)
	}

	reply.Term = cm.currentTerm

	// Шаг 2: если снепшот новее текущего
	if args.LastIncludedIndex <= cm.lastIncludedIndex {
		cm.dLogf("InstallSnapshot: ignoring, already have lastIncludedIndex=%d >= %d",
			cm.lastIncludedIndex, args.LastIncludedIndex)
		cm.mu.Unlock()
		return nil
	}

	// Шаг 3: принять снепшот
	cm.lastIncludedIndex = args.LastIncludedIndex
	cm.lastIncludedTerm = args.LastIncludedTerm
	cm.snapshotData = args.Data

	if cm.commitIndex < args.LastIncludedIndex {
		cm.commitIndex = args.LastIncludedIndex
	}
	if cm.lastApplied < args.LastIncludedIndex {
		cm.lastApplied = args.LastIncludedIndex
	}

	// Шаг 4: обрезать журнал.
	// До InstallSnapshot записи в cm.log имеют логические индексы
	// от cm.logOffset() до cm.logOffset()+len(cm.log)-1.
	// После установки нового lastIncludedIndex нужно удалить записи
	// с логическим индексом <= args.LastIncludedIndex.
	oldOffset := cm.logOffset()
	keepFrom := args.LastIncludedIndex + 1
	truncateFrom := keepFrom - oldOffset
	truncateFrom = max(truncateFrom, 0)
	if truncateFrom >= len(cm.log) {
		cm.log = cm.log[:0]
	} else {
		cm.log = append([]LogEntry{}, cm.log[truncateFrom:]...)
	}

	cm.dLogf("InstallSnapshot: log truncated to %d entries, lastIncludedIndex=%d",
		len(cm.log), cm.lastIncludedIndex)

	// Шаг 5: персистенция
	cm.persistToStorage()
	cm.mu.Unlock()

	// Шаг 6: уведомить snapshotChan и commitChanSender
	select {
	case cm.snapshotChan <- args.Data:
	default:
		cm.dLogf("InstallSnapshot: snapshotChan full, dropping snapshot notification")
	}

	select {
	case cm.newCommitReadyChan <- struct{}{}:
	default:
	}

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

			// Начать выборы, если в течение времени ожидания мы не получили
			// сообщение от лидера или не проголосовали за кого-либо.
			if elapsed := time.Since(cm.electionResetEvent); elapsed >= timeoutDuration {
				cm.startElection()
				cm.mu.Unlock()
				return
			}
			cm.mu.Unlock()

		case <-electionTimerDone:
			return
		}
	}
}

// startElection запускает новые выборы с этим CM в качестве кандидата.
// Ожидается, что cm.mu будет заблокирован.
func (cm *ConsensusModule) startElection() {
	cm.state = Candidate
	cm.currentTerm += 1
	savedCurrentTerm := cm.currentTerm
	cm.electionResetEvent = time.Now()
	cm.votedFor = cm.id
	cm.persistToStorage()
	cm.dLogf("becomes Candidate (currentTerm=%d); log=%v", savedCurrentTerm, cm.log)

	var votesReceived atomic.Int32
	votesReceived.Store(1)

	// Параллельно отправить RPC-запросы RequestVote всем остальным серверам.
	for _, peerID := range cm.peerIds {
		go func() {
			cm.mu.Lock()
			savedLastLogIndex, savedLastLogTerm := cm.lastLogIndexAndTerm()
			cm.mu.Unlock()

			args := RequestVoteArgs{
				Term:         savedCurrentTerm,
				CandidateID:  cm.id,
				LastLogIndex: savedLastLogIndex,
				LastLogTerm:  savedLastLogTerm,
			}

			cm.dLogf("sending RequestVote to %d: %+v", peerID, args)
			var reply RequestVoteReply
			if err := cm.server.Call(peerID, "ConsensusModule.RequestVote", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				cm.dLogf("received RequestVoteReply %+v", reply)

				if cm.state != Candidate {
					cm.dLogf("while waiting for reply, state = %v", cm.state)
					return
				}

				if reply.Term > cm.currentTerm {
					cm.dLogf("term out of date in RequestVoteReply")
					cm.becomeFollower(reply.Term)
					return
				} else if reply.Term == cm.currentTerm {
					if reply.VoteGranted {
						votesReceived.Add(1)
						if int(votesReceived.Load())*2 > len(cm.peerIds)+1 {
							// Выиграл выборы!
							cm.dLogf("wins election with %d votes", votesReceived.Load())
							cm.startLeader()
							slog.Info("wins election", slog.Int("votes", int(votesReceived.Load())))
							return
						}
					}
				}
			}
		}()
	}

	// Запустить новый таймер выборов на случай, если текущие выборы не завершатся успешно.
	go cm.runElectionTimer()
}

// becomeFollower делает cm последователем и сбрасывает его состояние.
// Ожидается, что cm.mu будет заблокирован.
func (cm *ConsensusModule) becomeFollower(term int) {
	cm.dLogf("becomes Follower with term=%d; log=%v", term, cm.log)
	cm.state = Follower
	if term > cm.currentTerm {
		cm.currentTerm = term
		cm.votedFor = -1
		cm.persistToStorage()
	}
	cm.electionResetEvent = time.Now()

	select {
	case <-cm.electionTimerDone:
	default:
		close(cm.electionTimerDone)
	}
	cm.electionTimerDone = make(chan struct{})
	go cm.runElectionTimer()
}

// startLeader переводит cm в состояние лидера и начинает процесс пульсации.
// Ожидается, что cm.mu будет заблокирован.
func (cm *ConsensusModule) startLeader() {
	cm.state = Leader

	logLen := cm.getLogLength()
	for _, peerID := range cm.peerIds {
		cm.nextIndex[peerID] = logLen
		cm.matchIndex[peerID] = -1
	}
	cm.dLogf(
		"becomes Leader; term=%d, nextIndex=%v, matchIndex=%v; log=%v",
		cm.currentTerm, cm.nextIndex, cm.matchIndex, cm.log,
	)

	// Эта горутина выполняется в фоновом режиме и отправляет сообщения
	// AppendEntries соседям:
	// * каждый раз, когда в triggerAEChan поступает уведомление;
	// * либо каждые HeartbeatTimeoutMs мс, если в triggerAEChan не происходит событий.
	go func(heartbeatTimeout time.Duration) {
		// Немедленно отправить сообщения AppendEntries всем соседям.
		cm.leaderSendAEs()

		t := time.NewTimer(heartbeatTimeout)
		defer t.Stop()
		for {
			doSend := false //nolint
			select {
			case <-t.C:
				doSend = true

				// Перезапустить таймер, чтобы он снова сработал через
				// heartbeatTimeout.
				t.Stop()
				t.Reset(heartbeatTimeout)
			case _, ok := <-cm.triggerAEChan:
				if ok {
					doSend = true
				} else {
					return
				}

				// Перезапустить таймер heartbeatTimeout.
				if !t.Stop() {
					<-t.C
				}
				t.Reset(heartbeatTimeout)
			}

			if doSend {
				// Если этот узел больше не является лидером,
				// остановить цикл отправки сообщений.
				cm.mu.Lock()
				if cm.state != Leader {
					cm.mu.Unlock()
					return
				}
				cm.mu.Unlock()
				cm.leaderSendAEs()
			}
		}
	}(HeartbeatTimeoutMs * time.Millisecond)
}

func (cm *ConsensusModule) nextIndexArgsEntries(peerID, savedCurrentTerm int) (int, AppendEntriesArgs, []LogEntry) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	ni := cm.nextIndex[peerID]
	prevLogIndex := ni - 1
	prevLogTerm := -1
	if prevLogIndex == cm.lastIncludedIndex {
		prevLogTerm = cm.lastIncludedTerm
	} else if entry, ok := cm.getLogEntry(prevLogIndex); ok {
		prevLogTerm = entry.Term
	}
	var entries []LogEntry
	logLen := cm.getLogLength()
	if ni < logLen {
		offset := cm.logOffset()
		from := ni - offset
		from = max(from, 0)
		if from < len(cm.log) {
			entries = append([]LogEntry{}, cm.log[from:]...)
		} else {
			entries = nil
		}
	} else {
		entries = nil
	}
	return ni, AppendEntriesArgs{
		Term:         savedCurrentTerm,
		LeaderID:     cm.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: cm.commitIndex,
	}, entries
}

// leaderSendAEsToPeer отправляет очередной раунд сообщений AppendEntries соседу,
// обрабатывает их ответы и обновляет состояние CM.
//
// peerID - сосед
// savedCurrentTerm - терм.
func (cm *ConsensusModule) leaderSendAEsToPeer(peerID, savedCurrentTerm int) {
	// Проверка: нужен ли InstallSnapshot
	cm.mu.Lock()
	if cm.state != Leader {
		cm.mu.Unlock()
		return
	}
	if cm.nextIndex[peerID] <= cm.lastIncludedIndex && cm.snapshotData != nil {
		cm.mu.Unlock()
		cm.sendInstallSnapshot(peerID, savedCurrentTerm)
		return
	}
	cm.mu.Unlock()

	ni, args, entries := cm.nextIndexArgsEntries(peerID, savedCurrentTerm)
	cm.dLogf("sending AppendEntries to %v: ni=%d, args=%+v", peerID, ni, args)
	var reply AppendEntriesReply
	if err := cm.server.Call(peerID, "ConsensusModule.AppendEntries", args, &reply); err == nil {
		cm.mu.Lock()
		// К сожалению, здесь нельзя просто использовать
		// defer cm.mu.Unlock(), поскольку в одной из ветвей
		// выполнения требуется отправка данных в каналы.
		// Поэтому необходимо явно вызывать cm.mu.Unlock()
		// во всех путях выхода, начиная с этого места.
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

				savedCommitIndex := cm.commitIndex
				logLen := cm.getLogLength()
				for i := cm.commitIndex + 1; i < logLen; i++ {
					entry, ok := cm.getLogEntry(i)
					if !ok {
						break
					}
					if entry.Term == cm.currentTerm {
						matchCount := 1
						for _, pid := range cm.peerIds {
							if cm.matchIndex[pid] >= i {
								matchCount++
							}
						}
						if matchCount*2 > len(cm.peerIds)+1 {
							cm.commitIndex = i
						}
					}
				}
				cm.dLogf(
					"AppendEntries reply from %d success: nextIndex := %v, matchIndex := %v; commitIndex := %d",
					peerID, cm.nextIndex, cm.matchIndex, cm.commitIndex,
				)
				if cm.commitIndex != savedCommitIndex {
					cm.dLogf("leader sets commitIndex := %d", cm.commitIndex)
					// Индекс фиксации изменился: лидер считает новые
					// записи журнала зафиксированными. Отправить новые
					// записи в канал фиксации клиентам этого лидера
					// и уведомить ведомых, отправив им сообщения
					// AppendEntries.
					cm.mu.Unlock()
					select {
					case cm.newCommitReadyChan <- struct{}{}:
					default:
					}
					select {
					case cm.triggerAEChan <- struct{}{}:
					default:
					}
				} else {
					cm.mu.Unlock()
				}
			} else {
				if reply.ConflictTerm >= 0 {
					lastIndexOfTerm := -1
					offset := cm.logOffset()
					for i := range slices.Backward(cm.log) {
						if cm.log[i].Term == reply.ConflictTerm {
							lastIndexOfTerm = i + offset
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

// sendInstallSnapshot отправляет InstallSnapshot узлу peerID.
// Предполагается, что cm.mu НЕ захвачен при входе.
func (cm *ConsensusModule) sendInstallSnapshot(peerID, savedCurrentTerm int) {
	cm.mu.Lock()
	if cm.state != Leader {
		cm.mu.Unlock()
		return
	}

	args := InstallSnapshotArgs{
		Term:              savedCurrentTerm,
		LeaderID:          cm.id,
		LastIncludedIndex: cm.lastIncludedIndex,
		LastIncludedTerm:  cm.lastIncludedTerm,
		Data:              cm.snapshotData,
	}
	cm.mu.Unlock()

	cm.dLogf(
		"sending InstallSnapshot to %d: lastIncludedIndex=%d, lastIncludedTerm=%d, dataLen=%d",
		peerID, args.LastIncludedIndex, args.LastIncludedTerm, len(args.Data),
	)

	var reply InstallSnapshotReply
	if err := cm.server.Call(peerID, "ConsensusModule.InstallSnapshot", args, &reply); err != nil {
		cm.dLogf("sendInstallSnapshot to %d failed: %v", peerID, err)
		return
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	if reply.Term > cm.currentTerm {
		cm.dLogf("term out of date in InstallSnapshot reply")
		cm.becomeFollower(reply.Term)
		return
	}

	if cm.state == Leader && savedCurrentTerm == reply.Term {
		cm.nextIndex[peerID] = cm.lastIncludedIndex + 1
		cm.matchIndex[peerID] = cm.lastIncludedIndex

		cm.dLogf("InstallSnapshot to %d success: nextIndex=%d, matchIndex=%d",
			peerID, cm.nextIndex[peerID], cm.matchIndex[peerID])
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
	cm.mu.Unlock()

	for _, peerID := range cm.peerIds {
		go cm.leaderSendAEsToPeer(peerID, savedCurrentTerm)
	}
}

// lastLogIndexAndTerm возвращает логический индекс последней записи журнала
// и терм последней записи журнала данного сервера.
// Если журнал пуст, возвращает lastIncludedIndex, lastIncludedTerm.
// Предполагается, что cm.mu уже заблокирован.
func (cm *ConsensusModule) lastLogIndexAndTerm() (int, int) {
	if len(cm.log) > 0 {
		lastIndex := cm.logOffset() + len(cm.log) - 1
		return lastIndex, cm.log[len(cm.log)-1].Term
	}
	return cm.lastIncludedIndex, cm.lastIncludedTerm
}

// commitChanSender отвечает за отправку зафиксированных записей журнала
// в cm.commitChan. Он отслеживает уведомления, поступающие через
// newCommitReadyChan, и определяет, какие новые записи журнала готовы
// к отправке. Этот метод должен выполняться в отдельной фоновой горутине;
// cm.commitChan может быть буферизированным, что будет ограничивать скорость,
// с которой клиент получает новые зафиксированные записи журнала.
// Метод завершает работу после закрытия newCommitReadyChan.
func (cm *ConsensusModule) commitChanSender() {
	defer cm.newCommitReadyChanWg.Done()

	for {
		// Ожидание сигнала о новых зафиксированных записях.
		_, ok := <-cm.newCommitReadyChan
		if !ok {
			cm.dLogf("commitChanSender done")
			return
		}

		// Определить, какие записи журнала необходимо применить.
		cm.mu.Lock()
		savedLastApplied := cm.lastApplied
		var entries []LogEntry

		if cm.commitIndex > cm.lastApplied {
			entries = cm.pendingCommittedEntries()
		}
		cm.mu.Unlock()
		cm.dLogf("commitChanSender entries=%v, savedLastApplied=%d", entries, savedLastApplied)

		for i, entry := range entries {
			cm.dLogf("send on commitchan i=%v, entry=%v", i, entry)
			ce := CommitEntry{
				Command: entry.Command,
				Index:   savedLastApplied + i + 1,
				Term:    entry.Term,
			}
			// Используем select, чтобы отправка могла быть прервана
			// закрытием newCommitReadyChan при остановке.
			select {
			case cm.commitChan <- ce:
			case <-cm.newCommitReadyChan:
				cm.dLogf("commitChanSender done")
				return
			}
		}

		// Проверить, не пора ли создать снепшот (только на лидере).
		cm.mu.Lock()
		if cm.shouldTakeSnapshot() {
			cm.dLogf("triggering snapshot: logLen=%d, threshold=%d, gap=%d",
				len(cm.log), cm.snapshotThreshold, cm.lastApplied-cm.lastIncludedIndex)
			data := cm.snapshotDataFn()
			cm.TakeSnapshot(data)
			cm.mu.Unlock()
		} else {
			cm.mu.Unlock()
		}
	}
}

func (cm *ConsensusModule) shouldTakeSnapshot() bool {
	return cm.state == Leader &&
		cm.snapshotThreshold > 0 &&
		len(cm.log) >= cm.snapshotThreshold &&
		cm.lastApplied-cm.lastIncludedIndex >= cm.snapshotInterval &&
		cm.snapshotDataFn != nil
}

// pendingCommittedEntries возвращает срез записей журнала, которые были
// зафиксированы, но ещё не применены к машине состояний.
//
// Метод вычисляет диапазон [lastApplied+1, commitIndex] с учётом смещения
// logOffset, обрезает границы по длине журнала и обновляет lastApplied
// до commitIndex. entries пуст, если новых зафиксированных записей нет.
//
// Предполагается, что cm.mu захвачен вызывающим.
func (cm *ConsensusModule) pendingCommittedEntries() (entries []LogEntry) {
	offset := cm.logOffset()
	from := cm.lastApplied + 1 - offset
	to := cm.commitIndex + 1 - offset
	if from < 0 {
		from = 0
	}
	if to > len(cm.log) {
		to = len(cm.log)
	}
	if to > from {
		entries = append([]LogEntry{}, cm.log[from:to]...)
	}
	cm.lastApplied = cm.commitIndex
	return entries
}
