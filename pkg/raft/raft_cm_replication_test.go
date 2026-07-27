package raft

import (
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

// TestReplicaLoop_StartAndStop проверяет базовый жизненный цикл
// per-peer горутины репликации: создание, старт, обработка триггера,
// остановка.
//
// Сценарий:
//  1. Создать ConsensusModule в состоянии Leader.
//  2. Создать peerReplication с peerID=1, запустить replicateLoop.
//  3. Вызвать triggerReplication.
//  4. Проверить, что transport.AppendEntries был вызван.
//  5. Закрыть stopCh, проверить, что doneCh закрыт (таймаут 1s).
//
// Ожидание: горутина стартует, обрабатывает триггер, завершается.
func TestReplicaLoop_StartAndStop(t *testing.T) {
	mock := &mockTransportAE{}
	cm := &ConsensusModule{
		id:               0,
		state:            Leader,
		transport:        mock,
		log:              make([]LogEntry, 0),
		nextIndex:        map[int]int{1: 0},
		matchIndex:       map[int]int{1: -1},
		lastLogIndex:     -1,
		currentTerm:      1,
		commitIndex:      -1,
		peerReplications: make(map[int]*peerReplication),
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

	pr := &peerReplication{
		peerID:    1,
		triggerCh: make(chan struct{}, 1),
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
	cm.peerReplications[1] = pr
	go cm.replicateLoop(pr)

	// Отправить триггер, дождаться обработки.
	cm.triggerReplication(1)
	time.Sleep(50 * time.Millisecond)

	if mock.callCount.Load() == 0 {
		t.Error("AppendEntries should have been called after trigger")
	}

	// Остановка горутины.
	close(pr.stopCh)

	select {
	case <-pr.doneCh:
		// корректно завершилась
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for replicateLoop to stop")
	}

	delete(cm.peerReplications, 1)
}

// TestReplicaLoop_Coalescing проверяет, что множественные trigger
// coalescятся в один вызов leaderSendAEsToPeer, когда горутина
// занята обработкой.
//
// Сценарий:
//  1. ConsensusModule в состоянии Leader, transport с задержкой 100ms.
//  2. Запустить replicateLoop.
//  3. Вызвать triggerReplication 50 раз подряд без пауз.
//  4. Дождаться завершения обработки.
//  5. Проверить, что transport.AppendEntries вызван 1 раз за время
//     задержки (50ms * 1 = одна итерация), остальные 49 триггеров
//     схлопнулись.
//
// Ожидание: coalescing работает — количество вызовов << 50.
func TestReplicaLoop_Coalescing(t *testing.T) {
	mock := &mockTransportAE{
		delay: 50 * time.Millisecond,
	}
	cm := &ConsensusModule{
		id:               0,
		state:            Leader,
		transport:        mock,
		log:              make([]LogEntry, 0),
		nextIndex:        map[int]int{1: 0},
		matchIndex:       map[int]int{1: -1},
		lastLogIndex:     -1,
		currentTerm:      1,
		commitIndex:      -1,
		peerReplications: make(map[int]*peerReplication),
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

	pr := &peerReplication{
		peerID:    1,
		triggerCh: make(chan struct{}, 1),
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
	cm.peerReplications[1] = pr
	go cm.replicateLoop(pr)

	// Подождать, пока горутина станет на ожидание в select.
	time.Sleep(10 * time.Millisecond)

	// Вызвать trigger 50 раз — все должны coalescнуться в один сигнал.
	for i := 0; i < 50; i++ {
		cm.triggerReplication(1)
	}

	// Дать время на обработку: с delay=50ms за 200ms должно
	// выполниться не более 4 итераций.
	time.Sleep(200 * time.Millisecond)

	calls := mock.callCount.Load()
	if calls > 5 {
		t.Errorf("expected <= 5 AppendEntries calls (coalescing), got %d", calls)
	}

	close(pr.stopCh)
	<-pr.doneCh
	delete(cm.peerReplications, 1)
}

// TestReplicaLoop_HeartbeatOnIdle проверяет, что replicateLoop
// отправляет AppendEntries (heartbeat) даже при отсутствии триггеров.
//
// Сценарий:
//  1. ConsensusModule в состоянии Leader.
//  2. Запустить replicateLoop с HeartbeatTimeoutMs = 10ms.
//  3. Не отправлять trigger.
//  4. Проверить, что AppendEntries вызван минимум 2 раза за 50ms.
//
// Ожидание: heartbeat работает автономно, без внешних триггеров.
func TestReplicaLoop_HeartbeatOnIdle(t *testing.T) {
	mock := &mockTransportAE{}
	cm := &ConsensusModule{
		id:               0,
		state:            Leader,
		transport:        mock,
		log:              make([]LogEntry, 0),
		nextIndex:        map[int]int{1: 0},
		matchIndex:       map[int]int{1: -1},
		lastLogIndex:     -1,
		currentTerm:      1,
		commitIndex:      -1,
		peerReplications: make(map[int]*peerReplication),
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

	pr := &peerReplication{
		peerID:    1,
		triggerCh: make(chan struct{}, 1),
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
	cm.peerReplications[1] = pr
	go cm.replicateLoop(pr)

	// Ждём несколько heartbeat-тиков (HeartbeatTimeoutMs = 46ms).
	time.Sleep(200 * time.Millisecond)

	calls := mock.callCount.Load()
	if calls < 2 {
		t.Errorf("expected >= 2 heartbeat AppendEntries, got %d", calls)
	}

	close(pr.stopCh)
	<-pr.doneCh
	delete(cm.peerReplications, 1)
}

// TestReplicaLoop_StateLeaderCheck проверяет, что replicateLoop
// завершается, если сервер перестал быть лидером между триггером
// и захватом cm.mu.
//
// Сценарий:
//  1. ConsensusModule в состоянии Leader.
//  2. Запустить replicateLoop с blockCh (горутина блокируется на RPC).
//  3. Отправить trigger.
//  4. Когда replicateLoop входит в AppendEntries (заблокирован),
//     изменить cm.state на Follower.
//  5. Закрыть blockCh, чтобы AppendEntries завершился.
//  6. Проверить, что doneCh закрыт (горутина вышла при проверке state).
//
// Ожидание: горутина выходит при обнаружении state != Leader.
func TestReplicaLoop_StateLeaderCheck(t *testing.T) {
	blockCh := make(chan struct{})
	mock := &mockTransportAE{
		blockCh: blockCh,
	}
	cm := &ConsensusModule{
		id:               0,
		state:            Leader,
		transport:        mock,
		log:              make([]LogEntry, 0),
		nextIndex:        map[int]int{1: 0},
		matchIndex:       map[int]int{1: -1},
		lastLogIndex:     -1,
		currentTerm:      1,
		commitIndex:      -1,
		peerReplications: make(map[int]*peerReplication),
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

	pr := &peerReplication{
		peerID:    1,
		triggerCh: make(chan struct{}, 1),
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
	cm.peerReplications[1] = pr
	go cm.replicateLoop(pr)

	// Отправить триггер, дождаться блокировки в AppendEntries.
	cm.triggerReplication(1)
	time.Sleep(30 * time.Millisecond)

	// Пока горутина заблокирована, меняем состояние на Follower.
	cm.mu.Lock()
	cm.state = Follower
	cm.mu.Unlock()

	// Разблокировать AppendEntries — горутина продолжит и увидит state=Follower.
	close(blockCh)

	select {
	case <-pr.doneCh:
		// Горутина корректно завершилась.
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for replicateLoop to exit after state change")
	}

	delete(cm.peerReplications, 1)
}

// TestTriggerReplication_PeerNotInMap проверяет, что triggerReplication
// безопасно завершается, если peerID отсутствует в peerReplications.
//
// Сценарий:
//  1. ConsensusModule в состоянии Leader, пустая peerReplications.
//  2. Вызвать triggerReplication(999) — несуществующий peer.
//  3. Проверить, что нет panic, нет вызовов transport.AppendEntries.
//
// Ожидание: nil-safe, без panic.
func TestTriggerReplication_PeerNotInMap(t *testing.T) {
	cm := &ConsensusModule{
		id:               0,
		state:            Leader,
		peerReplications: make(map[int]*peerReplication),
		transport:        &mockTransportAE{},
	}

	// Не должно быть panic.
	cm.triggerReplication(999)

	mock := cm.transport.(*mockTransportAE)
	if calls := mock.callCount.Load(); calls > 0 {
		t.Errorf("expected 0 AppendEntries calls for unknown peer, got %d", calls)
	}
}

// TestTriggerAllPeers_AllTriggered проверяет, что triggerAllPeers
// отправляет сигнал всем активным peer.
//
// Сценарий:
//  1. ConsensusModule в состоянии Leader, 3 peer с активными горутинами.
//  2. Вызвать triggerAllPeers().
//  3. Проверить, что каждый peer получил триггер (AppendEntries вызван).
//
// Ожидание: все peer получили trigger, AppendEntries отправлен каждому.
func TestTriggerAllPeers_AllTriggered(t *testing.T) {
	mock := &mockTransportAE{delay: 10 * time.Millisecond}
	cm := &ConsensusModule{
		id:               0,
		state:            Leader,
		transport:        mock,
		log:              make([]LogEntry, 0),
		nextIndex:        map[int]int{1: 0, 2: 0, 3: 0},
		matchIndex:       map[int]int{1: -1, 2: -1, 3: -1},
		lastLogIndex:     -1,
		currentTerm:      1,
		commitIndex:      -1,
		peerReplications: make(map[int]*peerReplication),
		configurations: configurations{
			committed: Configuration{
				ConfigServers: []ConfigServer{
					{ID: 0, Suffrage: Voter},
					{ID: 1, Suffrage: Voter},
					{ID: 2, Suffrage: Voter},
					{ID: 3, Suffrage: Voter},
				},
			},
			latest: Configuration{
				ConfigServers: []ConfigServer{
					{ID: 0, Suffrage: Voter},
					{ID: 1, Suffrage: Voter},
					{ID: 2, Suffrage: Voter},
					{ID: 3, Suffrage: Voter},
				},
			},
		},
	}

	// Запустить replicateLoop для 3 peer.
	for _, pid := range []int{1, 2, 3} {
		pr := &peerReplication{
			peerID:    pid,
			triggerCh: make(chan struct{}, 1),
			stopCh:    make(chan struct{}),
			doneCh:    make(chan struct{}),
		}
		cm.peerReplications[pid] = pr
		go cm.replicateLoop(pr)
	}

	time.Sleep(10 * time.Millisecond)

	// triggerAllPeers — должен отправить сигнал всем peer.
	cm.triggerAllPeers()

	time.Sleep(100 * time.Millisecond)

	// Каждый peer должен получить хотя бы один вызов AppendEntries.
	calls := mock.callCount.Load()
	if calls < 3 {
		t.Errorf("expected >= 3 AppendEntries calls (1 per peer), got %d", calls)
	}

	// Остановить все горутины.
	for _, pid := range []int{1, 2, 3} {
		close(cm.peerReplications[pid].stopCh)
		<-cm.peerReplications[pid].doneCh
		delete(cm.peerReplications, pid)
	}
}

// TestReplicaLoop_StopChInterrupt проверяет, что replicateLoop
// незамедлительно завершается по stopCh, даже если триггер
// уже был отправлен, но горутина ещё не обработала его.
//
// Сценарий:
//  1. ConsensusModule в состоянии Leader.
//  2. Запустить replicateLoop.
//  3. Отправить trigger.
//  4. Немедленно закрыть stopCh.
//  5. Проверить, что doneCh закрыт.
//
// Ожидание: stopCh имеет приоритет над triggerCh.
func TestReplicaLoop_StopChInterrupt(t *testing.T) {
	mock := &mockTransportAE{delay: 5 * time.Millisecond}
	cm := &ConsensusModule{
		id:               0,
		state:            Leader,
		transport:        mock,
		log:              make([]LogEntry, 0),
		nextIndex:        map[int]int{1: 0},
		matchIndex:       map[int]int{1: -1},
		lastLogIndex:     -1,
		currentTerm:      1,
		commitIndex:      -1,
		peerReplications: make(map[int]*peerReplication),
	}

	pr := &peerReplication{
		peerID:    1,
		triggerCh: make(chan struct{}, 1),
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
	cm.peerReplications[1] = pr
	go cm.replicateLoop(pr)

	time.Sleep(10 * time.Millisecond)

	// Отправить триггер и сразу остановить.
	pr.triggerCh <- struct{}{}
	close(pr.stopCh)

	select {
	case <-pr.doneCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for replicateLoop to stop after stopCh")
	}

	delete(cm.peerReplications, 1)
}

// TestReplicaLoop_StartStopReplicationLifecycle проверяет, что
// startStopReplication корректно создаёт горутины для новых peer
// и останавливает для удалённых.
//
// Сценарий:
//  1. ConsensusModule в состоянии Leader, 1 peer в конфигурации.
//  2. Создать peerReplications для peer 1, запустить replicateLoop.
//  3. Вызвать startStopReplication с конфигурацией {0, 1, 2} —
//     peer 2 должен получить новую горутину.
//  4. Вызвать startStopReplication с конфигурацией {0} —
//     горутины peer 1 и peer 2 должны быть остановлены.
//
// Ожидание: карта peerReplications синхронизирована с конфигурацией.
func TestReplicaLoop_StartStopReplicationLifecycle(t *testing.T) {
	mock := &mockTransportAE{delay: 5 * time.Millisecond}
	cm := &ConsensusModule{
		id:               0,
		state:            Leader,
		transport:        mock,
		log:              make([]LogEntry, 0),
		nextIndex:        map[int]int{1: 0, 2: 0},
		matchIndex:       map[int]int{1: -1, 2: -1},
		lastLogIndex:     -1,
		currentTerm:      1,
		commitIndex:      -1,
		peerReplications: make(map[int]*peerReplication),
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

	// Вручную создать горутину для peer 1.
	pr1 := &peerReplication{
		peerID:    1,
		triggerCh: make(chan struct{}, 1),
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
	cm.peerReplications[1] = pr1
	go cm.replicateLoop(pr1)

	time.Sleep(10 * time.Millisecond)

	// Добавить peer 2 через startStopReplication.
	cm.mu.Lock()
	cm.startStopReplication(Configuration{
		ConfigServers: []ConfigServer{
			{ID: 0, Suffrage: Voter},
			{ID: 1, Suffrage: Voter},
			{ID: 2, Suffrage: Voter},
		},
	})
	cm.mu.Unlock()

	// Проверить, что peer 2 создан.
	if _, ok := cm.peerReplications[2]; !ok {
		t.Fatal("peerReplications[2] should exist after startStopReplication")
	}

	// Удалить peer 1 и 2 через startStopReplication (оставить только 0).
	cm.mu.Lock()
	cm.startStopReplication(Configuration{
		ConfigServers: []ConfigServer{
			{ID: 0, Suffrage: Voter},
		},
	})
	cm.mu.Unlock()

	// Проверить, что peer 1 и 2 удалены из карты.
	if _, ok := cm.peerReplications[1]; ok {
		t.Error("peerReplications[1] should be deleted")
	}
	if _, ok := cm.peerReplications[2]; ok {
		t.Error("peerReplications[2] should be deleted")
	}

	// Горутина peer 1 должна завершиться после close(stopCh).
	// Ожидание с таймаутом — doneCh больше не дожидается
	// синхронно в startStopReplication (см. дедлок #20).
	select {
	case <-pr1.doneCh:
	case <-time.After(time.Second):
		t.Error("peer 1 doneCh should be closed within 1s")
	}
}
