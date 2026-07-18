// Package raft Реализация ядра Raft — модуль консенсуса.
// This code is in the public domain.
package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	DebugCM = 1
	Quantum = 1

	HeartbeatTimeoutMs  = 5 * 10 * Quantum
	ReelectionTimeoutMs = 15 * 10 * Quantum
	SnapshotThreshold   = 1000
	TickerTimeoutMs     = 10 * Quantum
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

	// Snapshot указывает, что данный CommitEntry является уведомлением
	// о применении snapshot, а не обычной зафиксированной записи журнала.
	// Когда Snapshot == true, Command содержит snapshot-данные ([]byte),
	// а Index содержит lastSnapshotIndex.
	Snapshot bool
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
		panic("unreachable")
	}
}

type LogEntry struct {
	Command any
	Term    int
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

	// Непостоянное состояние Raft на лидерах
	nextIndex  map[int]int
	matchIndex map[int]int

	// snapshotData — последний сохранённый snapshot в памяти.
	snapshotData []byte

	// lastSnapshotIndex — индекс последней записи, включённой в snapshot.
	// Если snapshot не создавался, равен -1.
	lastSnapshotIndex int

	// lastSnapshotTerm — терм последней записи, включённой в snapshot.
	// Если snapshot не создавался, равен -1.
	lastSnapshotTerm int

	// snapshotter — функция, вызываемая для получения snapshot-данных
	// от машины состояний (KVService). Устанавливается извне через
	// SetSnapshotter. Возвращает (data, lastIndex, lastTerm).
	snapshotter func() ([]byte, int, int)

	// takeSnapshotChan — канал, по которому CM запрашивает взятие snapshot
	// у машины состояний. Функция, переданная в канал, будет вызвана
	// отдельной горутиной для получения snapshot-данных.
	takeSnapshotChan chan func() ([]byte, int, int)

	// snapshotApplyWg — ожидание завершения применения snapshot на follower.
	snapshotApplyWg sync.WaitGroup

	// snapshotThreshold — минимальное количество зафиксированных записей
	// с момента последнего snapshot, после которого лидер инициирует
	// взятие нового snapshot.
	snapshotThreshold int
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
) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.id = id
	cm.peerIds = peerIds
	cm.server = server
	cm.storage = storage
	cm.commitChan = commitChan
	cm.newCommitReadyChan = make(chan struct{}, 16)
	cm.triggerAEChan = make(chan struct{}, 1)
	cm.state = Follower
	cm.votedFor = -1
	cm.commitIndex = -1
	cm.lastApplied = -1
	cm.lastSnapshotIndex = -1
	cm.lastSnapshotTerm = -1
	cm.snapshotThreshold = SnapshotThreshold
	cm.takeSnapshotChan = make(chan func() ([]byte, int, int), 16)
	cm.nextIndex = make(map[int]int)
	cm.matchIndex = make(map[int]int)

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
	go cm.runTakeSnapshotLoop()
	return cm
}

// SetSnapshotter устанавливает функцию snapshotter, которая будет
// вызываться для получения snapshot-данных от машины состояний.
// Должна быть вызвана до начала работы CM.
func (cm *ConsensusModule) SetSnapshotter(fn func() ([]byte, int, int)) {
	cm.snapshotter = fn
}

// SetSnapshotThreshold устанавливает порог snapshot (количество зафиксированных
// записей с момента последнего snapshot) для данного CM. По умолчанию
// используется константа SnapshotThreshold. Должна быть вызвана до начала
// работы CM.
func (cm *ConsensusModule) SetSnapshotThreshold(n int) {
	cm.snapshotThreshold = n
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
	cm.dLogf("Submit received by %v: %v", cm.state, command)
	if cm.state == Leader {
		submitIndex := len(cm.log)
		cm.log = append(cm.log, LogEntry{Command: command, Term: cm.currentTerm})
		cm.persistToStorage()
		cm.dLogf("... log=%v", cm.log)
		cm.mu.Unlock()
		cm.triggerAEChan <- struct{}{}
		return submitIndex
	}

	cm.mu.Unlock()
	return -1
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

	// Закрыть канал takeSnapshotChan, чтобы завершить горутину
	// runTakeSnapshotLoop.
	close(cm.takeSnapshotChan)
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

	// Восстановление snapshot metadata (обратная совместимость:
	// если ключей нет — lastSnapshotIndex = -1, lastSnapshotTerm = -1).
	if snapIdxData, found := cm.storage.Get("snapshotIndex"); found {
		d := gob.NewDecoder(bytes.NewBuffer(snapIdxData))
		if err := d.Decode(&cm.lastSnapshotIndex); err != nil {
			log.Fatal(err)
		}
	} else {
		cm.lastSnapshotIndex = -1
	}
	if snapTermData, found := cm.storage.Get("snapshotTerm"); found {
		d := gob.NewDecoder(bytes.NewBuffer(snapTermData))
		if err := d.Decode(&cm.lastSnapshotTerm); err != nil {
			log.Fatal(err)
		}
	} else {
		cm.lastSnapshotTerm = -1
	}
	if snapData, found := cm.storage.Get("snapshot"); found {
		cm.snapshotData = snapData
	} else {
		cm.snapshotData = nil
	}
}

// persistToStorage сохраняет всё постоянное состояние CM в cm.storage,
// включая snapshot metadata и snapshot data.
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

	// Сохранить snapshot metadata.
	var snapIdxData bytes.Buffer
	if err := gob.NewEncoder(&snapIdxData).Encode(cm.lastSnapshotIndex); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("snapshotIndex", snapIdxData.Bytes())

	var snapTermData bytes.Buffer
	if err := gob.NewEncoder(&snapTermData).Encode(cm.lastSnapshotTerm); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("snapshotTerm", snapTermData.Bytes())

	// Сохранить snapshot data.
	if cm.snapshotData != nil {
		snapCopy := make([]byte, len(cm.snapshotData))
		copy(snapCopy, cm.snapshotData)
		cm.storage.Set("snapshot", snapCopy)
	}
}

// dLogf выводит отладочное сообщение, если DebugCM > 0.
func (cm *ConsensusModule) dLogf(format string, args ...any) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d] ", cm.id) + format
		log.Printf(format, args...)
	}
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

// InstallSnapshotArgs — аргументы RPC InstallSnapshot, используемые лидером
// для передачи snapshot отстающему узлу. См. спецификацию Raft §7.
type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastSnapshotIndex int
	LastSnapshotTerm  int
	Data              []byte
}

// InstallSnapshotReply — ответ на RPC InstallSnapshot.
type InstallSnapshotReply struct {
	Term    int
	Success bool
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
		// Учитываем snapshot: если PrevLogIndex совпадает с lastSnapshotIndex,
		// сравниваем с lastSnapshotTerm.
		//nolint:gocritic
		if args.PrevLogIndex == -1 {
			reply.Success = true
		} else if args.PrevLogIndex == cm.lastSnapshotIndex && args.PrevLogTerm == cm.lastSnapshotTerm {
			// PrevLogIndex совпадает со snapshot.
			reply.Success = true
		} else if args.PrevLogIndex > cm.lastSnapshotIndex {
			sliceIdx := args.PrevLogIndex - cm.lastSnapshotIndex - 1
			if sliceIdx < len(cm.log) && cm.log[sliceIdx].Term == args.PrevLogTerm {
				reply.Success = true
			}
		} else {
			// PrevLogIndex < lastSnapshotIndex — отстающий узел,
			// ему нужен InstallSnapshot.
			reply.Success = false
			reply.ConflictIndex = cm.lastSnapshotIndex + 1
			reply.ConflictTerm = -1
		}

		if reply.Success {
			// Находит точку вставки — место, где происходит несовпадение термов
			// между существующими записями журнала, начиная с PrevLogIndex+1,
			// и новыми записями, отправленными лидером через RPC.
			var logInsertSliceIdx int
			if args.PrevLogIndex == -1 || args.PrevLogIndex == cm.lastSnapshotIndex {
				logInsertSliceIdx = 0
			} else {
				logInsertSliceIdx = args.PrevLogIndex - cm.lastSnapshotIndex
			}
			newEntriesIndex := 0

			for logInsertSliceIdx < len(cm.log) && newEntriesIndex < len(args.Entries) {
				if cm.log[logInsertSliceIdx].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertSliceIdx++
				newEntriesIndex++
			}

			if newEntriesIndex < len(args.Entries) {
				cm.dLogf("... inserting entries %v from slice index %d", args.Entries[newEntriesIndex:], logInsertSliceIdx)
				cm.log = append(cm.log[:logInsertSliceIdx], args.Entries[newEntriesIndex:]...)
				cm.dLogf("... log is now: %v", cm.log)
			}

			// Устанавливает индекс фиксации.
			// convert LeaderCommit from absolute to slice index
			if args.LeaderCommit > cm.commitIndex {
				newCommitIdx := args.LeaderCommit
				// Если лидер зафиксировал индекс, который уже в snapshot
				if newCommitIdx <= cm.lastSnapshotIndex {
					cm.commitIndex = newCommitIdx
				} else {
					// newCommitIdx > lastSnapshotIndex
					maxLogAbsIndex := cm.lastSnapshotIndex + len(cm.log)
					cm.commitIndex = min(newCommitIdx, maxLogAbsIndex)
				}
				cm.dLogf("... setting commitIndex=%d", cm.commitIndex)
				cm.newCommitReadyChan <- struct{}{}
			}
		} else {
			// Не найдено совпадение для PrevLogIndex/PrevLogTerm.
			// Заполнить ConflictIndex и ConflictTerm, чтобы помочь лидеру
			// быстрее привести наш журнал в актуальное состояние.
			//nolint:gocritic
			if args.PrevLogIndex < cm.lastSnapshotIndex {
				// PrevLogIndex меньше snapshot — отстающий узел.
				reply.ConflictIndex = cm.lastSnapshotIndex + 1
				reply.ConflictTerm = -1
			} else if args.PrevLogIndex >= cm.lastSnapshotIndex+len(cm.log) {
				sliceLen := len(cm.log)
				reply.ConflictIndex = cm.lastSnapshotIndex + sliceLen
				reply.ConflictTerm = -1
			} else {
				// PrevLogIndex указывает на существующую запись в нашем журнале,
				// но PrevLogTerm не совпадает с термом записи.
				sliceIdx := args.PrevLogIndex - cm.lastSnapshotIndex - 1
				reply.ConflictTerm = cm.log[sliceIdx].Term

				var i int
				for i = sliceIdx - 1; i >= 0; i-- {
					if cm.log[i].Term != reply.ConflictTerm {
						break
					}
				}
				reply.ConflictIndex = cm.lastSnapshotIndex + 1 + i
			}
		}
	}

	reply.Term = cm.currentTerm
	cm.persistToStorage()
	cm.dLogf("AppendEntries reply: %+v", *reply)
	return nil
}

// InstallSnapshot — RPC-обработчик для InstallSnapshot.
// Лидер передаёт snapshot отстающему follower, который применяет его
// и обрезает журнал. См. спецификацию Raft §7.
func (cm *ConsensusModule) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
		return nil
	}
	if args.Term > cm.currentTerm {
		cm.becomeFollower(args.Term)
	}
	reply.Term = cm.currentTerm
	reply.Success = false
	if args.Term == cm.currentTerm {
		cm.dLogf("InstallSnapshot: lastIndex=%d, lastTerm=%d", args.LastSnapshotIndex, args.LastSnapshotTerm)
		cm.lastSnapshotIndex = args.LastSnapshotIndex
		cm.lastSnapshotTerm = args.LastSnapshotTerm
		cm.snapshotData = args.Data
		cm.persistToStorage()

		// Обрезать log: удалить записи с индексом <= lastSnapshotIndex.
		cm.log = cm.log[:0]
		cm.dLogf("... log truncated, snapshot applied")
		// Обновить commitIndex и lastApplied.
		if cm.commitIndex < args.LastSnapshotIndex {
			cm.commitIndex = args.LastSnapshotIndex
		}
		if cm.lastApplied < args.LastSnapshotIndex {
			cm.lastApplied = args.LastSnapshotIndex
		}
		// Отправить snapshot в commitChan для применения к state machine.
		cm.snapshotApplyWg.Add(1)
		go func() {
			defer cm.snapshotApplyWg.Done()
			cm.dLogf("sending snapshot to commitChan (index=%d)", args.LastSnapshotIndex)
			cm.commitChan <- CommitEntry{
				Command:  args.Data,
				Index:    args.LastSnapshotIndex,
				Term:     args.LastSnapshotTerm,
				Snapshot: true,
			}
		}()
		reply.Success = true
	}
	cm.dLogf("InstallSnapshot reply: %+v", *reply)
	return nil
}

// sendInstallSnapshot отправляет InstallSnapshot RPC указанному узлу.
// Предполагается, что cm.mu не заблокирован (вызывается из leaderSendAEs).
func (cm *ConsensusModule) sendInstallSnapshot(peerID int) {
	cm.mu.Lock()
	savedTerm := cm.currentTerm
	args := InstallSnapshotArgs{
		Term:              savedTerm,
		LeaderID:          cm.id,
		LastSnapshotIndex: cm.lastSnapshotIndex,
		LastSnapshotTerm:  cm.lastSnapshotTerm,
		Data:              cm.snapshotData,
	}
	cm.mu.Unlock()
	cm.dLogf("sending InstallSnapshot to %d: lastIndex=%d, lastTerm=%d", peerID, args.LastSnapshotIndex, args.LastSnapshotTerm)

	var reply InstallSnapshotReply
	if err := cm.server.Call(peerID, "ConsensusModule.InstallSnapshot", args, &reply); err == nil {
		cm.mu.Lock()
		if reply.Term > cm.currentTerm {
			cm.becomeFollower(reply.Term)
		} else if cm.state == Leader && savedTerm == reply.Term && reply.Success {
			cm.nextIndex[peerID] = args.LastSnapshotIndex + 1
			cm.matchIndex[peerID] = args.LastSnapshotIndex
			cm.dLogf("InstallSnapshot reply from %d success: nextIndex=%d", peerID, cm.nextIndex[peerID])
		}
		cm.mu.Unlock()
	} else {
		cm.dLogf("warning while sending InstallSnapshot to %d; error: %v", peerID, err)

		// Переподключаемся, если клиент был обнулён из-за разрыва TCP
		// (shut down) или если есть флаг peerWantsReconnect.
		shouldReconnect := strings.Contains(err.Error(), "shut down")
		if !shouldReconnect {
			cm.server.mu.Lock()
			shouldReconnect = cm.server.peerWantsReconnect[peerID]
			cm.server.mu.Unlock()
		}
		if shouldReconnect {
			cm.mu.Lock()
			if cm.state == Leader {
				cm.mu.Unlock()
				cm.dLogf("reconnecting to peer %d after InstallSnapshot error", peerID)
				if err := cm.server.ReconnectToPeer(peerID); err != nil {
					cm.dLogf("failed to reconnect to peer %d: %v", peerID, err)
				}
			} else {
				cm.mu.Unlock()
			}
		}
	}
}

// takeSnapshot сохраняет snapshot-данные, обрезает журнал и обновляет
// метаданные. Предполагается, что cm.mu уже заблокирован.
func (cm *ConsensusModule) takeSnapshot(data []byte, index, term int) {
	cm.dLogf("takeSnapshot: index=%d, term=%d", index, term)
	cm.lastSnapshotIndex = index
	cm.lastSnapshotTerm = term
	cm.snapshotData = data

	// Обрезать log: все записи с индексом <= lastSnapshotIndex удаляются.
	cm.log = cm.log[:0]
	cm.persistToStorage()

	// Обновить nextIndex/matchIndex для всех peers.
	for _, peerID := range cm.peerIds {
		cm.nextIndex[peerID] = cm.lastSnapshotIndex + 1
		cm.matchIndex[peerID] = cm.lastSnapshotIndex
	}
	cm.dLogf("snapshot taken, log truncated, nextIndex updated")
}

// runTakeSnapshotLoop — горутина, обрабатывающая запросы на взятие snapshot.
// Получает функции через takeSnapshotChan, вызывает их для получения
// snapshot-данных от машины состояний, затем сохраняет snapshot.
func (cm *ConsensusModule) runTakeSnapshotLoop() {
	for fn := range cm.takeSnapshotChan {
		data, index, term := fn()
		cm.mu.Lock()
		cm.takeSnapshot(data, index, term)
		cm.mu.Unlock()
		cm.dLogf("snapshot loop: snapshot saved (index=%d)", index)
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
		<-ticker.C

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

	votesReceived := 1

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
						votesReceived += 1
						if votesReceived*2 > len(cm.peerIds)+1 {
							// Выиграл выборы!
							cm.dLogf("wins election with %d votes", votesReceived)
							cm.startLeader()
							return
						}
					}
				}
			} else {
				cm.dLogf("warning while sending RequestVote to %v; error: %v", peerID, err)
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

	go cm.runElectionTimer()
}

// startLeader переводит cm в состояние лидера и начинает процесс пульсации.
// Ожидается, что cm.mu будет заблокирован.
func (cm *ConsensusModule) startLeader() {
	cm.state = Leader

	for _, peerID := range cm.peerIds {
		cm.nextIndex[peerID] = cm.lastSnapshotIndex + 1 + len(cm.log)
		cm.matchIndex[peerID] = -1
	}
	cm.dLogf("becomes Leader; term=%d, nextIndex=%v, matchIndex=%v; log=%v", cm.currentTerm, cm.nextIndex, cm.matchIndex, cm.log)

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

// leaderSendAEs отправляет очередной раунд сообщений AppendEntries всем
// соседям, обрабатывает их ответы и обновляет состояние CM.
// Если узел отстаёт настолько, что его nextIndex <= lastSnapshotIndex,
// вместо AppendEntries отправляется InstallSnapshot.
func (cm *ConsensusModule) leaderSendAEs() {
	cm.mu.Lock()
	if cm.state != Leader {
		cm.mu.Unlock()
		return
	}
	savedCurrentTerm := cm.currentTerm
	cm.mu.Unlock()

	for _, peerID := range cm.peerIds {
		go func() {
			cm.mu.Lock()
			ni := cm.nextIndex[peerID]

			// Если узел отстаёт за snapshot, отправить InstallSnapshot.
			if ni <= cm.lastSnapshotIndex {
				cm.mu.Unlock()
				cm.sendInstallSnapshot(peerID)
				return
			}

			prevLogIndex := ni - 1
			prevLogTerm := -1
			if prevLogIndex == cm.lastSnapshotIndex {
				prevLogTerm = cm.lastSnapshotTerm
			} else if prevLogIndex > cm.lastSnapshotIndex {
				sliceIdx := prevLogIndex - cm.lastSnapshotIndex - 1
				if sliceIdx < len(cm.log) {
					prevLogTerm = cm.log[sliceIdx].Term
				}
			}
			entriesSliceStart := max(0, ni-cm.lastSnapshotIndex-1)
			if entriesSliceStart > len(cm.log) {
				entriesSliceStart = len(cm.log)
			}
			entries := cm.log[entriesSliceStart:]

			args := AppendEntriesArgs{
				Term:         savedCurrentTerm,
				LeaderID:     cm.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: cm.commitIndex,
			}
			cm.mu.Unlock()
			cm.dLogf("sending AppendEntries to %v: ni=%d, args=%+v", peerID, ni, args)
			var reply AppendEntriesReply
			if err := cm.server.Call(peerID, "ConsensusModule.AppendEntries", args, &reply); err == nil {
				cm.mu.Lock()
				if reply.Term > cm.currentTerm {
					cm.dLogf("term out of date in heartbeat reply")
					cm.becomeFollower(reply.Term)
					cm.mu.Unlock()
					return
				}

				if cm.state == Leader && savedCurrentTerm == reply.Term {
					if reply.Success {
						cm.nextIndex[peerID] = ni + len(entries)
						// Ограничить nextIndex, чтобы он не выходил за текущий журнал после snapshot
						maxNextIndex := cm.lastSnapshotIndex + len(cm.log) + 1
						if cm.nextIndex[peerID] > maxNextIndex {
							cm.nextIndex[peerID] = maxNextIndex
						}
						cm.matchIndex[peerID] = cm.nextIndex[peerID] - 1

						savedCommitIndex := cm.commitIndex
						// Итерация по абсолютным индексам журнала.
						maxAbsIndex := cm.lastSnapshotIndex + len(cm.log)
						for absIdx := cm.commitIndex + 1; absIdx <= maxAbsIndex; absIdx++ {
							sliceIdx := absIdx - cm.lastSnapshotIndex - 1
							if sliceIdx >= 0 && sliceIdx < len(cm.log) && cm.log[sliceIdx].Term == cm.currentTerm {
								matchCount := 1
								for _, pID := range cm.peerIds {
									if cm.matchIndex[pID] >= absIdx {
										matchCount++
									}
								}
								if matchCount*2 > len(cm.peerIds)+1 {
									cm.commitIndex = absIdx
								}
							}
						}
						cm.dLogf(
							"AppendEntries reply from %d success: nextIndex := %v, matchIndex := %v; commitIndex := %d",
							peerID, cm.nextIndex, cm.matchIndex, cm.commitIndex,
						)
						if cm.commitIndex != savedCommitIndex {
							cm.dLogf("leader sets commitIndex := %d", cm.commitIndex)
							cm.mu.Unlock()
							cm.newCommitReadyChan <- struct{}{}
							cm.triggerAEChan <- struct{}{}

							// Проверить, не пора ли взять snapshot.
							cm.mu.Lock()
							if cm.snapshotter != nil && cm.commitIndex-cm.lastSnapshotIndex >= cm.snapshotThreshold {
								cm.dLogf("triggering snapshot: commitIndex=%d, lastSnapshotIndex=%d", cm.commitIndex, cm.lastSnapshotIndex)
								cm.takeSnapshotChan <- cm.snapshotter
							}
							cm.mu.Unlock()
						} else {
							cm.mu.Unlock()
						}
					} else {
						if reply.ConflictTerm >= 0 {
							// Поиск последнего вхождения ConflictTerm в журнале
							// с учётом snapshot offset.
							lastIndexOfTerm := -1
							for i := len(cm.log) - 1; i >= 0; i-- {
								if cm.log[i].Term == reply.ConflictTerm {
									lastIndexOfTerm = cm.lastSnapshotIndex + 1 + i
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
			} else {
				cm.dLogf("warning while sending AppendEntries to %v; error: %v", peerID, err)

				// Переподключаемся, если клиент был обнулён из-за разрыва TCP
				// (shut down) или если есть флаг peerWantsReconnect
				// (предыдущий shut down). Не переподключаемся, если клиент
				// был явно отключён через DisconnectPeer (флаг сброшен).
				shouldReconnect := strings.Contains(err.Error(), "shut down")
				if !shouldReconnect {
					cm.server.mu.Lock()
					shouldReconnect = cm.server.peerWantsReconnect[peerID]
					cm.server.mu.Unlock()
				}
				if shouldReconnect {
					cm.mu.Lock()
					if cm.state == Leader {
						cm.mu.Unlock()
						cm.dLogf("reconnecting to peer %d", peerID)
						if err := cm.server.ReconnectToPeer(peerID); err != nil {
							cm.dLogf("failed to reconnect to peer %d: %v", peerID, err)
						}
					} else {
						cm.mu.Unlock()
					}
				}
			}
		}()
	}
}

// lastLogIndexAndTerm возвращает индекс последней записи журнала
// и терм последней записи журнала данного сервера
// (или -1, -1, если журнал пуст и snapshot отсутствует).
// Учитывает snapshot: если журнал не пуст, реальный последний индекс
// равен lastSnapshotIndex + len(cm.log).
// Предполагается, что cm.mu уже заблокирован.
func (cm *ConsensusModule) lastLogIndexAndTerm() (int, int) {
	if len(cm.log) > 0 {
		lastIndex := cm.lastSnapshotIndex + len(cm.log)
		return lastIndex, cm.log[len(cm.log)-1].Term
	}
	if cm.lastSnapshotIndex >= 0 {
		return cm.lastSnapshotIndex, cm.lastSnapshotTerm
	}
	return -1, -1
}

// logIndexToSliceIdx преобразует абсолютный индекс журнала в индекс
// в срезе cm.log. Требует, чтобы absoluteIndex > cm.lastSnapshotIndex.
// Если absoluteIndex <= cm.lastSnapshotIndex, возвращает -1.
// Предполагается, что cm.mu уже заблокирован.
//
//nolint:unused
func (cm *ConsensusModule) logIndexToSliceIdx(absoluteIndex int) int {
	if absoluteIndex <= cm.lastSnapshotIndex {
		return -1
	}
	return absoluteIndex - cm.lastSnapshotIndex - 1
}

// commitChanSender отвечает за отправку зафиксированных записей журнала
// в cm.commitChan. Он отслеживает уведомления, поступающие через
// newCommitReadyChan, и определяет, какие новые записи журнала готовы
// к отправке. Этот метод должен выполняться в отдельной фоновой горутине;
// cm.commitChan может быть буферизированным, что будет ограничивать скорость,
// с которой клиент получает новые зафиксированные записи журнала.
// Метод завершает работу после закрытия newCommitReadyChan.
// Учитывает snapshot: lastApplied и commitIndex — абсолютные индексы,
// поэтому для доступа к cm.log используется преобразование через
// lastSnapshotIndex.
func (cm *ConsensusModule) commitChanSender() {
	defer cm.newCommitReadyChanWg.Done()

	for range cm.newCommitReadyChan {
		cm.mu.Lock()
		savedTerm := cm.currentTerm
		savedLastApplied := cm.lastApplied
		var entries []LogEntry
		if cm.commitIndex > cm.lastApplied {
			// Преобразование абсолютных индексов в индексы среза cm.log.
			startSliceIdx := max(0, cm.lastApplied+1-cm.lastSnapshotIndex-1)
			endSliceIdx := min(cm.commitIndex+1-cm.lastSnapshotIndex-1, len(cm.log))
			if startSliceIdx < endSliceIdx && startSliceIdx < len(cm.log) {
				entries = cm.log[startSliceIdx:endSliceIdx]
			}
			cm.lastApplied = cm.commitIndex
		}
		cm.mu.Unlock()
		cm.dLogf("commitChanSender entries=%v, savedLastApplied=%d", entries, savedLastApplied)

		for i, entry := range entries {
			cm.dLogf("send on commitchan i=%v, entry=%v", i, entry)
			cm.commitChan <- CommitEntry{
				Command: entry.Command,
				Index:   savedLastApplied + i + 1,
				Term:    savedTerm,
			}
		}
	}
	cm.dLogf("commitChanSender done")
}
