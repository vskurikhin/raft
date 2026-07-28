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
// подсчитывающая вызовы AppendEntries с возможностью задания задержки.
type mockTransportAE struct {
	Transport
	callCount atomic.Int32
	delay     time.Duration
	blockCh   chan struct{}
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
		id:           0,
		state:        Leader,
		transport:    mock,
		log:          make([]LogEntry, 0),
		nextIndex:    map[int]int{1: 0},
		matchIndex:   map[int]int{1: -1},
		lastLogIndex: -1,
		currentTerm:  1,
		commitIndex:  -1,
		inflightAE:   map[int]*atomic.Bool{1: new(atomic.Bool)},
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
	}
	cm.inflightAE[1].Store(false)

	// Установить inflightAE[1] = true — имитация активной горутины.
	cm.inflightAE[1].Store(true)
	time.Sleep(10 * time.Millisecond)

	// Вызвать leaderSendAEs — должен пропустить peer 1 (CAS=false).
	cm.leaderSendAEs()
	time.Sleep(30 * time.Millisecond)

	calls := mock.callCount.Load()
	if calls > 0 {
		t.Errorf("expected 0 AppendEntries calls (dedup active), got %d", calls)
	}

	// Сбросить флаг — теперь leaderSendAEs должен запустить горутину.
	cm.inflightAE[1].Store(false)
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
		id:           0,
		state:        Leader,
		transport:    mock,
		log:          make([]LogEntry, 0),
		nextIndex:    map[int]int{1: 0},
		matchIndex:   map[int]int{1: -1},
		lastLogIndex: -1,
		currentTerm:  1,
		commitIndex:  -1,
		inflightAE:   map[int]*atomic.Bool{1: new(atomic.Bool)},
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
	}
	cm.inflightAE[1].Store(false)

	// Установить флаг и вызвать leaderSendAEsToPeer.
	cm.inflightAE[1].Store(true)
	cm.leaderSendAEsToPeer(1, 1)
	time.Sleep(20 * time.Millisecond)

	// После завершения флаг должен быть сброшен.
	if cm.inflightAE[1].Load() {
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
		id:                0,
		state:             Leader,
		transport:         &mockTransportAE{},
		log:               make([]LogEntry, 0),
		nextIndex:         map[int]int{1: -1},
		matchIndex:        map[int]int{1: -1},
		lastLogIndex:      -1,
		lastSnapshotIndex: -1,
		currentTerm:       1,
		commitIndex:       -1,
		votedFor:          -1,
		leaderStartIndex:  -1,
		storage:           NewMapStorage(),
		snapshotStore:     &mockSnapshotStore{},
		inflightAE:        map[int]*atomic.Bool{1: new(atomic.Bool)},
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
	}
	cm.inflightAE[1].Store(false)

	cm.inflightAE[1].Store(true)
	cm.leaderSendAEsToPeer(1, 1)
	time.Sleep(20 * time.Millisecond)

	if cm.inflightAE[1].Load() {
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
		id:         0,
		state:      Follower,
		transport:  mock,
		inflightAE: map[int]*atomic.Bool{1: new(atomic.Bool)},
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
		id:           0,
		state:        Leader,
		transport:    &mockTransportAE{},
		log:          make([]LogEntry, 0),
		nextIndex:    map[int]int{1: 0},
		matchIndex:   map[int]int{1: -1},
		lastLogIndex: -1,
		currentTerm:  1,
		commitIndex:  -1,
		inflightAE:   nil,
	}

	// Не должно быть panic.
	cm.leaderSendAEs()
	cm.leaderSendAEsToPeer(1, 1)
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
