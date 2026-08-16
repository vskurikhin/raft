package raft

import (
	"bytes"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mockTransportAE — минимальная реализация Transport для тестов,
// подсчитывающая вызовы AppendEntries и InstallSnapshot с возможностью
// задания задержки AppendEntries.
type mockTransportAE struct {
	Transport
	callCount        atomic.Int32
	installCallCount atomic.Int32
	delay            time.Duration
	blockCh          chan struct{}
}

func (m *mockTransportAE) AppendEntries(_ ServerID, _ AppendEntriesArgs) (AppendEntriesReply, error) {
	m.callCount.Add(1)
	if m.blockCh != nil {
		<-m.blockCh
	}
	if m.delay > 0 {
		time.Sleep(m.delay)
	}
	return AppendEntriesReply{
		Term:    0,
		Success: true,
	}, nil
}

func (m *mockTransportAE) InstallSnapshot(_ ServerID, _ InstallSnapshotRequest, _ io.Reader) (InstallSnapshotResponse, error) {
	m.installCallCount.Add(1)
	return InstallSnapshotResponse{
		Success: true,
	}, nil
}

func (m *mockTransportAE) Consumer() <-chan RPC {
	return nil
}

func (m *mockTransportAE) RequestVote(_ ServerID, _ RequestVoteArgs) (RequestVoteReply, error) {
	return RequestVoteReply{}, nil
}

func (m *mockTransportAE) RequestPreVote(_ ServerID, _ RequestPreVoteArgs) (RequestPreVoteReply, error) {
	return RequestPreVoteReply{}, nil
}

func (m *mockTransportAE) TimeoutNow(_ ServerID, _ TimeoutNowRequest) (TimeoutNowResponse, error) {
	return TimeoutNowResponse{}, nil
}

func (m *mockTransportAE) AppendEntriesPipeline(_ ServerID) (AppendPipeline, error) {
	return nil, nil
}

func (m *mockTransportAE) Close() error {
	return nil
}

func (m *mockTransportAE) SetHeartbeatHandler(_ func(RPC)) {
}

func (m *mockTransportAE) LocalAddr() ServerAddress {
	return ""
}

func (m *mockTransportAE) EncodePeer(_ ServerID, _ ServerAddress) string {
	return ""
}

func (m *mockTransportAE) DecodePeer(_ string) ServerAddress {
	return ""
}

// TestLeaderSendAEs_Deduplication проверяет, что leaderSendAEs
// запускает не более одной горутины на peer.
//
// Сценарий:
//  1. ConsensusModule в состоянии Leader с inflightAE для peer 1.
//  2. Установить inflightAE[1] = true (имитация активной горутины).
//  3. Вызвать leaderSendAEs.
//  4. Проверить, что AppendEntries НЕ вызван (дедупликация сработала).
//  5. Сбросить inflightAE[1] = false.
//  6. Вызвать leaderSendAEs.
//  7. Проверить, что AppendEntries вызван (дедупликация разрешена).
func TestLeaderSendAEs_Deduplication(t *testing.T) {
	mock := &mockTransportAE{}
	cm := &ConsensusModule{
		leaderState: leaderState{
			nextIndex:  map[int]int{1: 0},
			matchIndex: map[int]int{1: -1},
			inflightAE: map[int]*atomic.Bool{1: new(atomic.Bool)},
		},
		cmState: cmState{
			state:        Leader,
			log:          make([]LogEntry, 0),
			lastLogIndex: -1,
			currentTerm:  1,
			commitIndex:  -1,
			configurations: configurations{
				committed: Configuration{
					ConfigServers: []ConfigServer{
						{ID: 0, Suffrage: Voter},
						{ID: 1, Suffrage: Voter},
					},
				},
				latest: Configuration{
					ConfigServers: []ConfigServer{
						{ID: 0, Suffrage: Voter},
						{ID: 1, Suffrage: Voter},
					},
				},
			},
		},

		id:        0,
		transport: mock,
	}
	cm.leaderState.inflightAE[1].Store(false)

	// Установить inflightAE[1] = true — имитация активной горутины.
	cm.leaderState.inflightAE[1].Store(true)
	time.Sleep(10 * time.Millisecond)

	// Вызвать leaderSendAEs — должен пропустить peer 1 (CAS=false).
	cm.leaderSendAEs()
	time.Sleep(30 * time.Millisecond)

	calls := mock.callCount.Load()
	if calls > 0 {
		t.Errorf("expected 0 AppendEntries calls (dedup active), got %d", calls)
	}

	// Сбросить флаг — теперь leaderSendAEs должен запустить горутину.
	cm.leaderState.inflightAE[1].Store(false)
	cm.leaderSendAEs()
	time.Sleep(30 * time.Millisecond)

	calls = mock.callCount.Load()
	if calls == 0 {
		t.Error("expected AppendEntries call after inflightAE reset, got 0")
	}
}

// TestInflightAE_DeferReset проверяет, что leaderSendAEsToPeer
// сбрасывает inflightAE[peerID] через defer при любом завершении.
//
// Сценарий:
//  1. ConsensusModule в состоянии Leader, inflightAE[1] = true.
//  2. Вызвать leaderSendAEsToPeer (mock transport с успешным ответом).
//  3. После завершения проверить, что inflightAE[1] == false.
func TestInflightAE_DeferReset(t *testing.T) {
	mock := &mockTransportAE{}
	cm := &ConsensusModule{
		leaderState: leaderState{
			nextIndex:  map[int]int{1: 0},
			matchIndex: map[int]int{1: -1},
			inflightAE: map[int]*atomic.Bool{1: new(atomic.Bool)},
		},
		cmState: cmState{
			state:        Leader,
			log:          make([]LogEntry, 0),
			lastLogIndex: -1,
			currentTerm:  1,
			commitIndex:  -1,
			configurations: configurations{
				committed: Configuration{
					ConfigServers: []ConfigServer{
						{ID: 0, Suffrage: Voter},
						{ID: 1, Suffrage: Voter},
					},
				},
				latest: Configuration{
					ConfigServers: []ConfigServer{
						{ID: 0, Suffrage: Voter},
						{ID: 1, Suffrage: Voter},
					},
				},
			},
		},

		id:        0,
		transport: mock,
	}
	cm.leaderState.inflightAE[1].Store(false)

	// Установить флаг и вызвать leaderSendAEsToPeer.
	cm.leaderState.inflightAE[1].Store(true)
	cm.leaderSendAEsToPeer(1, 1, 0)
	time.Sleep(20 * time.Millisecond)

	// После завершения флаг должен быть сброшен.
	if cm.leaderState.inflightAE[1].Load() {
		t.Error("inflightAE[1] should be false after leaderSendAEsToPeer completes")
	}
}

// TestInflightAE_DeferResetOnSnapshotPath проверяет, что defer
// сбрасывает inflightAE при выходе через snapshot path.
//
// Сценарий:
//  1. ConsensusModule в состоянии Leader, snapshotStore != nil,
//     nextIndex[1] <= lastSnapshotIndex.
//  2. Установить inflightAE[1] = true.
//  3. Вызвать leaderSendAEsToPeer — она идёт по snapshot path.
//  4. После завершения inflightAE[1] == false.
func TestInflightAE_DeferResetOnSnapshotPath(t *testing.T) {
	cm := &ConsensusModule{
		leaderState: leaderState{
			nextIndex:        map[int]int{1: -1},
			matchIndex:       map[int]int{1: -1},
			leaderStartIndex: -1,
			inflightAE:       map[int]*atomic.Bool{1: new(atomic.Bool)},
		},
		cmState: cmState{
			state:             Leader,
			log:               make([]LogEntry, 0),
			lastLogIndex:      -1,
			lastSnapshotIndex: -1,
			currentTerm:       1,
			commitIndex:       -1,
			votedFor:          -1,
			configurations: configurations{
				committed: Configuration{
					ConfigServers: []ConfigServer{
						{ID: 0, Suffrage: Voter},
						{ID: 1, Suffrage: Voter},
					},
				},
				latest: Configuration{
					ConfigServers: []ConfigServer{
						{ID: 0, Suffrage: Voter},
						{ID: 1, Suffrage: Voter},
					},
				},
			},
		},

		id:            0,
		transport:     &mockTransportAE{},
		storage:       NewMapStorage(),
		snapshotStore: &mockSnapshotStore{},
	}
	cm.leaderState.inflightAE[1].Store(false)

	cm.leaderState.inflightAE[1].Store(true)
	cm.leaderSendAEsToPeer(1, 1, 0)
	time.Sleep(20 * time.Millisecond)

	if cm.leaderState.inflightAE[1].Load() {
		t.Error("inflightAE[1] should be false after snapshot path exit")
	}
}

// TestLeaderSendAEs_StateCheck проверяет, что leaderSendAEs
// ничего не делает, если сервер не лидер.
//
// Сценарий:
//  1. ConsensusModule в состоянии Follower.
//  2. Вызвать leaderSendAEs.
//  3. Проверить, что AppendEntries не вызван.
func TestLeaderSendAEs_StateCheck(t *testing.T) {
	mock := &mockTransportAE{}
	cm := &ConsensusModule{
		leaderState: leaderState{
			inflightAE: map[int]*atomic.Bool{1: new(atomic.Bool)},
		},
		cmState: cmState{
			state: Follower,
		},

		id:        0,
		transport: mock,
	}

	cm.leaderSendAEs()
	time.Sleep(20 * time.Millisecond)

	if calls := mock.callCount.Load(); calls > 0 {
		t.Errorf("expected 0 AppendEntries calls for follower, got %d", calls)
	}
}

// TestInflightAE_NilSafe проверяет, что leaderSendAEs и
// leaderSendAEsToPeer безопасны при nil в inflightAE.
//
// Сценарий:
//  1. ConsensusModule с inflightAE = nil.
//  2. Вызвать leaderSendAEs — не должно быть panic.
//  3. Вызвать leaderSendAEsToPeer — не должно быть panic.
func TestInflightAE_NilSafe(t *testing.T) {
	cm := &ConsensusModule{
		leaderState: leaderState{
			nextIndex:  map[int]int{1: 0},
			matchIndex: map[int]int{1: -1},
			inflightAE: nil,
		},
		cmState: cmState{
			state:        Leader,
			log:          make([]LogEntry, 0),
			lastLogIndex: -1,
			currentTerm:  1,
			commitIndex:  -1,
		},

		id:        0,
		transport: &mockTransportAE{},
	}

	// Не должно быть panic.
	cm.leaderSendAEs()
	cm.leaderSendAEsToPeer(1, 1, 0)
}

// TestBecomeFollower_InflightAE_NilEntry проверяет, что becomeFollower
// безопасна при наличии nil-элемента в inflightAE. Тот же защищённый
// код используется и в defer блока runLeaderLoop при выходе из цикла
// лидера, поэтому тест покрывает оба места сброса флагов.
//
// Сценарий:
//  1. ConsensusModule в состоянии Leader, inflightAE = {1: nil, 2: true}.
//     Элемент 1 (nil) имитирует peer, добавленного в конфигурацию, но
//     не инициализированного через startStopReplication до момента
//     завершения лидерства (реконфигурация не успела обработаться).
//  2. Вызвать becomeFollower(1) — не должно быть panic: ранее вызов
//     Store(false) на nil *atomic.Bool давал nil dereference.
//  3. Проверить, что inflightAE[2] сброшен в false, а nil-элемент
//     пропущен без паники.
func TestBecomeFollower_InflightAE_NilEntry(t *testing.T) {
	cm := &ConsensusModule{
		leaderState: leaderState{
			inflightAE: map[int]*atomic.Bool{
				1: nil,              // peer вне конфигурации — элемент не инициализирован
				2: new(atomic.Bool), // активный peer, флаг установлен
			},
		},
		cmState: cmState{
			state:             Leader,
			electionTimerDone: make(chan struct{}),
			currentTerm:       1,
		},

		id:         0,
		stepDown:   make(chan struct{}, 1),
		shutdownCh: make(chan struct{}),
	}
	cm.leaderState.inflightAE[2].Store(true)

	// becomeFollower ожидает, что cm.mu заблокирован (по контракту).
	cm.mu.Lock()
	cm.becomeFollower(1)
	cm.mu.Unlock()

	if cm.cmState.state != Follower {
		t.Errorf("state = %v, want Follower", cm.cmState.state)
	}
	if cm.leaderState.inflightAE[2].Load() {
		t.Error("inflightAE[2] should be reset to false")
	}
}

// TestInflightAE_CASConcurrent проверяет, что atomic CAS
// не вызывает data race при конкурентном доступе.
//
// Сценарий:
//  1. Создать inflightAE с одним peer.
//  2. Запустить 10 горутин, каждая делает CAS(false→true) и Store(false).
//  3. Проверить, что нет panic и все операции завершаются.
//
// Запуск: только с -race флагом.
func TestInflightAE_CASConcurrent(t *testing.T) {
	flag := &atomic.Bool{}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if flag.CompareAndSwap(false, true) {
				flag.Store(false)
			}
		}()
	}
	wg.Wait()
}

// mockSnapshotStore — минимальная реализация SnapshotStore для тестов.
type mockSnapshotStore struct {
	SnapshotStore
}

func (m *mockSnapshotStore) List() ([]*SnapshotMeta, error) {
	return []*SnapshotMeta{
		{Index: 0, Term: 1, Size: 0},
	}, nil
}

func (m *mockSnapshotStore) Open(id string) (*SnapshotMeta, io.ReadCloser, error) {
	return &SnapshotMeta{Index: 0, Term: 1, Size: 0}, io.NopCloser(bytes.NewReader(nil)), nil
}

// mockSnapshotStoreCfg — конфигурируемая реализация SnapshotStore для
// тестов leaderSendSnapshot. Позволяет имитировать повреждённое хранилище:
// nil-элемент в List(), пустой ID, ошибку List() и т.д.
type mockSnapshotStoreCfg struct {
	SnapshotStore
	listResult []*SnapshotMeta // результат List(); nil означает «снэпшотов нет»
	listErr    error           // ошибка, возвращаемая List()
	openMeta   *SnapshotMeta   // метаданные, возвращаемые Open()
}

// List возвращает предзаданный список снэпшотов или ошибку.
func (m *mockSnapshotStoreCfg) List() ([]*SnapshotMeta, error) {
	return m.listResult, m.listErr
}

// Open возвращает предзаданные метаданные и пустой ридер — для проверки
// корректности вызова важны лишь метаданные, содержимое не читается.
func (m *mockSnapshotStoreCfg) Open(id string) (*SnapshotMeta, io.ReadCloser, error) {
	return m.openMeta, io.NopCloser(bytes.NewReader(nil)), nil
}

// TestLeaderSendSnapshot_NilSnapshotStore проверяет, что leaderSendSnapshot
// безопасна при nil-хранилище снэпшотов.
//
// Сценарий:
//  1. ConsensusModule в состоянии Leader, snapshotStore == nil.
//  2. Вызвать leaderSendSnapshot — не должно быть panic.
//  3. Проверить, что InstallSnapshot НЕ вызван.
//
// Функция выполняется в отдельной горутине (через leaderSendAEsToPeer),
// поэтому паника здесь без recover() убила бы весь процесс.
func TestLeaderSendSnapshot_NilSnapshotStore(t *testing.T) {
	transport := &mockTransportAE{}
	cm := &ConsensusModule{
		leaderState: leaderState{
			nextIndex:  map[int]int{1: -1},
			matchIndex: map[int]int{1: -1},
		},
		cmState: cmState{
			state:       Leader,
			currentTerm: 1,
		},

		id:            0,
		transport:     transport,
		snapshotStore: nil,
	}

	// Никаких обращений к хранилищу не должно быть.
	cm.leaderSendSnapshot(1, 1)

	if n := transport.installCallCount.Load(); n != 0 {
		t.Errorf("expected 0 InstallSnapshot calls, got %d", n)
	}
}

// TestLeaderSendSnapshot_NilMetaInList проверяет, что leaderSendSnapshot
// безопасна, когда List() возвращает слайс с nil-элементом.
//
// Сценарий:
//  1. snapshotStore.List() возвращает []*SnapshotMeta{nil}.
//  2. Вызвать leaderSendSnapshot — не должно быть panic, иначе обращение
//     snapshots[0].ID привело бы к nil dereference.
//  3. Проверить, что InstallSnapshot НЕ вызван.
func TestLeaderSendSnapshot_NilMetaInList(t *testing.T) {
	transport := &mockTransportAE{}
	cm := &ConsensusModule{
		leaderState: leaderState{
			nextIndex:  map[int]int{1: -1},
			matchIndex: map[int]int{1: -1},
		},
		cmState: cmState{
			state:       Leader,
			currentTerm: 1,
		},

		id:        0,
		transport: transport,
		snapshotStore: &mockSnapshotStoreCfg{
			listResult: []*SnapshotMeta{nil},
		},
	}

	cm.leaderSendSnapshot(1, 1)

	if n := transport.installCallCount.Load(); n != 0 {
		t.Errorf("expected 0 InstallSnapshot calls, got %d", n)
	}
}

// TestLeaderSendSnapshot_EmptyID проверяет, что leaderSendSnapshot
// безопасна, когда snapshots[0].ID пуст.
//
// Сценарий:
//  1. snapshotStore.List() возвращает снэпшот с пустым ID.
//  2. Вызвать leaderSendSnapshot — не должно быть вызова snapshotStore.Open
//     с некорректным ID (реализации хранилища могут паниковать на нём).
//  3. Проверить, что InstallSnapshot НЕ вызван.
func TestLeaderSendSnapshot_EmptyID(t *testing.T) {
	transport := &mockTransportAE{}
	cm := &ConsensusModule{
		leaderState: leaderState{
			nextIndex:  map[int]int{1: -1},
			matchIndex: map[int]int{1: -1},
		},
		cmState: cmState{
			state:       Leader,
			currentTerm: 1,
		},

		id:        0,
		transport: transport,
		snapshotStore: &mockSnapshotStoreCfg{
			listResult: []*SnapshotMeta{{ID: ""}},
		},
	}

	cm.leaderSendSnapshot(1, 1)

	if n := transport.installCallCount.Load(); n != 0 {
		t.Errorf("expected 0 InstallSnapshot calls, got %d", n)
	}
}

// TestLeaderSendSnapshot_Success проверяет, что leaderSendSnapshot при
// корректном хранилище отправляет InstallSnapshot и обновляет nextIndex
// и matchIndex.
//
// Сценарий:
//  1. snapshotStore.List() возвращает один снэпшот (Index=5, Term=2, Size=100).
//  2. transport.InstallSnapshot возвращает Success=true, Term=0 (не выше term=1).
//  3. Проверить, что InstallSnapshot вызван ровно один раз.
//  4. Проверить, что nextIndex[1] == 6 и matchIndex[1] == 5.
func TestLeaderSendSnapshot_Success(t *testing.T) {
	transport := &mockTransportAE{}
	cm := &ConsensusModule{
		leaderState: leaderState{
			nextIndex:  map[int]int{1: -1},
			matchIndex: map[int]int{1: -1},
		},
		cmState: cmState{
			state:       Leader,
			currentTerm: 1,
		},

		id:        0,
		transport: transport,
		snapshotStore: &mockSnapshotStoreCfg{
			listResult: []*SnapshotMeta{{ID: "snap-5", Index: 5, Term: 2, Size: 100}},
			openMeta:   &SnapshotMeta{Index: 5, Term: 2, Size: 100},
		},
	}

	cm.leaderSendSnapshot(1, 1)

	if n := transport.installCallCount.Load(); n != 1 {
		t.Fatalf("expected 1 InstallSnapshot call, got %d", n)
	}
	if cm.leaderState.nextIndex[1] != 6 {
		t.Errorf("nextIndex[1] = %d, want 6", cm.leaderState.nextIndex[1])
	}
	if cm.leaderState.matchIndex[1] != 5 {
		t.Errorf("matchIndex[1] = %d, want 5", cm.leaderState.matchIndex[1])
	}
}
