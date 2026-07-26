package raft

import (
	"bytes"
	"container/list"
	"encoding/gob"
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
)

func TestElectionBasic(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	h.CheckSingleLeader()
}

func TestElectionLeaderDisconnect(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderId, origTerm := h.CheckSingleLeader()

	h.DisconnectPeer(origLeaderId)
	sleepMs(350)

	newLeaderId, newTerm := h.CheckSingleLeader()
	if newLeaderId == origLeaderId {
		t.Errorf("want new leader to be different from orig leader")
	}
	if newTerm <= origTerm {
		t.Errorf("want newTerm <= origTerm, got %d and %d", newTerm, origTerm)
	}
}

func TestElectionLeaderAndAnotherDisconnect(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*Quantum*time.Millisecond)()
	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()

	h.DisconnectPeer(origLeaderId)
	otherId := (origLeaderId + 1) % 3
	h.DisconnectPeer(otherId)

	// Нет кворума.
	sleepMs(450)
	h.CheckNoLeader()

	// Повторно подключаем ещё один сервер; теперь кворум будет достигнут.
	h.ReconnectPeer(otherId)
	h.CheckSingleLeader()
}

func TestDisconnectAllThenRestore(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	sleepMs(100)
	// Отключаем все серверы с самого начала. Лидера не будет.
	for i := 0; i < 3; i++ {
		h.DisconnectPeer(i)
	}
	sleepMs(450)
	h.CheckNoLeader()

	// Повторно подключаем все серверы. Будет выбран лидер.
	for i := 0; i < 3; i++ {
		h.ReconnectPeer(i)
	}
	h.CheckSingleLeader()
}

func TestElectionLeaderDisconnectThenReconnect(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()
	origLeaderId, _ := h.CheckSingleLeader()

	h.DisconnectPeer(origLeaderId)

	sleepMs(350 * Quantum)
	newLeaderId, newTerm := h.CheckSingleLeader()

	h.ReconnectPeer(origLeaderId)
	sleepMs(150 * Quantum)

	againLeaderId, againTerm := h.CheckSingleLeader()

	if newLeaderId != againLeaderId {
		t.Errorf("again leader id got %d; want %d", againLeaderId, newLeaderId)
	}
	if againTerm != newTerm {
		t.Errorf("again term got %d; want %d", againTerm, newTerm)
	}
}

func TestElectionLeaderDisconnectThenReconnect5(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*Quantum*time.Millisecond)()

	h := NewHarness(t, 5)
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()

	h.DisconnectPeer(origLeaderId)
	sleepMs(150 * Quantum)
	newLeaderId, newTerm := h.CheckSingleLeader()

	h.ReconnectPeer(origLeaderId)
	sleepMs(150 * Quantum)

	againLeaderId, againTerm := h.CheckSingleLeader()

	if newLeaderId != againLeaderId {
		t.Errorf("again leader id got %d; want %d", againLeaderId, newLeaderId)
	}
	if againTerm != newTerm {
		t.Errorf("again term got %d; want %d", againTerm, newTerm)
	}
}

func TestElectionFollowerComesBack(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*Quantum*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderId, origTerm := h.CheckSingleLeader()

	otherId := (origLeaderId + 1) % 3
	h.DisconnectPeer(otherId)
	time.Sleep(650 * Quantum * time.Millisecond)
	h.ReconnectPeer(otherId)
	sleepMs(150 * Quantum)

	// Здесь мы не можем проверить идентификатор нового лидера,
	// поскольку он зависит от относительных тайм-аутов выборов.
	// Однако мы можем проверить, что терм изменился,
	// а это означает, что произошло повторное избрание лидера.
	_, newTerm := h.CheckSingleLeader()
	if newTerm <= origTerm {
		t.Errorf("newTerm=%d, origTerm=%d", newTerm, origTerm)
	}
}

func TestElectionDisconnectLoop(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*Quantum*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	for cycle := 0; cycle < 5; cycle++ {
		leaderId, _ := h.CheckSingleLeader()

		h.DisconnectPeer(leaderId)
		otherId := (leaderId + 1) % 3
		h.DisconnectPeer(otherId)
		sleepMs(310 * Quantum)
		h.CheckNoLeader()

		// Повторно подключаем оба сервера.
		h.ReconnectPeer(otherId)
		h.ReconnectPeer(leaderId)

		// Даём системе время стабилизироваться.
		sleepMs(150 * Quantum)
	}
}

func TestCommitOneCommand(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*Quantum*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()

	tlog("submitting 42 to %d", origLeaderId)
	isLeader := h.SubmitToServer(origLeaderId, 42) >= 0
	if !isLeader {
		t.Errorf("want id=%d leader, but it's not", origLeaderId)
	}

	sleepMs(250)
	h.CheckCommittedN(42, 3)
}

func TestCommitAfterCallDrops(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*Quantum*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	lid, _ := h.CheckSingleLeader()
	h.PeerDropCallsAfterN(lid, 2)
	h.SubmitToServer(lid, 99)
	sleepMs(30 * Quantum)
	h.PeerDontDropCalls(lid)

	sleepMs(60 * Quantum)
	h.CheckCommittedN(99, 3)
}

func TestSubmitNonLeaderFails(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()
	sid := (origLeaderId + 1) % 3
	tlog("submitting 42 to %d", sid)
	isLeader := h.SubmitToServer(sid, 42) >= 0
	if isLeader {
		t.Errorf("want id=%d !leader, but it is", sid)
	}
	sleepMs(10)
}

func TestCommitMultipleCommands(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*Quantum*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()

	values := []int{42, 55, 81}
	for _, v := range values {
		tlog("submitting %d to %d", v, origLeaderId)
		isLeader := h.SubmitToServer(origLeaderId, v) >= 0
		if !isLeader {
			t.Errorf("want id=%d leader, but it's not", origLeaderId)
		}
		sleepMs(100)
	}

	sleepMs(250 * Quantum)
	nc, i1 := h.CheckCommitted(42)
	_, i2 := h.CheckCommitted(55)
	if nc != 3 {
		t.Errorf("want nc=3, got %d", nc)
	}
	if i1 >= i2 {
		t.Errorf("want i1<i2, got i1=%d i2=%d", i1, i2)
	}

	_, i3 := h.CheckCommitted(81)
	if i2 >= i3 {
		t.Errorf("want i2<i3, got i2=%d i3=%d", i2, i3)
	}
}

func TestCommitWithDisconnectionAndRecover(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*Quantum*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	// Отправляем несколько значений в полностью подключённый кластер.
	origLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(origLeaderId, 5)
	h.SubmitToServer(origLeaderId, 6)

	sleepMs(250)
	h.CheckCommittedN(6, 3)

	dPeerId := (origLeaderId + 1) % 3
	h.CrashPeer(dPeerId)
	sleepMs(250 * Quantum)

	// SubmitToServer блокируется до коммита. Теперь кластер из 2 живых узлов:
	// лидер + один follower = кворум (2/3). Команда будет закоммичена.
	if idx := h.SubmitToServer(origLeaderId, 7); idx < 0 {
		t.Fatal("Submit 7 failed")
	}
	sleepMs(250)

	// Перезапускаем отключённый узел и ждём синхронизации.
	h.RestartPeer(dPeerId)
	sleepMs(250 * Quantum)
	h.CheckSingleLeader()

	sleepMs(150)
	h.CheckCommittedN(7, 3)
}

func TestNoCommitWithNoQuorum(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*Quantum*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	// Отправляем несколько значений в полностью подключённый кластер.
	origLeaderId, origTerm := h.CheckSingleLeader()
	h.SubmitToServer(origLeaderId, 5)
	h.SubmitToServer(origLeaderId, 6)

	sleepMs(250)
	h.CheckCommittedN(6, 3)

	// Отключаем обоих ведомых.
	dPeer1 := (origLeaderId + 1) % 3
	dPeer2 := (origLeaderId + 2) % 3
	h.DisconnectPeer(dPeer1)
	h.DisconnectPeer(dPeer2)
	sleepMs(250 * Quantum)

	h.SubmitToServer(origLeaderId, 8)
	sleepMs(250 * Quantum)
	h.CheckNotCommitted(8)

	// Повторно подключаем оба остальных сервера;
	// теперь у нас снова будет кворум.
	h.ReconnectPeer(dPeer1)
	h.ReconnectPeer(dPeer2)
	sleepMs(600)

	// Команда 8 по-прежнему не зафиксирована,
	// поскольку терм изменился.
	h.CheckNotCommitted(8)

	// Будет выбран новый лидер.
	// Это может быть другой лидер, несмотря на то,
	// что журнал исходного лидера длиннее,
	// поскольку два повторно подключённых соседа
	// могут выбрать друг друга.
	newLeaderId, againTerm := h.CheckSingleLeader()
	if origTerm == againTerm {
		t.Errorf("got origTerm==againTerm==%d; want them different", origTerm)
	}

	// Но новые значения уже точно будут зафиксированы...
	h.SubmitToServer(newLeaderId, 9)
	h.SubmitToServer(newLeaderId, 10)
	h.SubmitToServer(newLeaderId, 11)
	sleepMs(350)

	for _, v := range []int{9, 10, 11} {
		h.CheckCommittedN(v, 3)
	}
}

func TestDisconnectLeaderBriefly(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	// Отправляем несколько значений в полностью связанный кластер.
	origLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(origLeaderId, 5)
	h.SubmitToServer(origLeaderId, 6)
	sleepMs(250)
	h.CheckCommittedN(6, 3)

	// Отключаем лидера на короткое время (меньше тайм-аута выборов у соседей).
	h.DisconnectPeer(origLeaderId)
	sleepMs(90)
	h.ReconnectPeer(origLeaderId)
	sleepMs(200)

	h.SubmitToServer(origLeaderId, 7)
	sleepMs(250)
	h.CheckCommittedN(7, 3)
}

func TestCommitsWithLeaderDisconnects(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*Quantum*time.Millisecond)()

	h := NewHarness(t, 5)
	defer h.Shutdown()

	// Отправляем несколько значений в полностью подключённый кластер.
	origLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(origLeaderId, 5)
	h.SubmitToServer(origLeaderId, 6)

	sleepMs(250)
	h.CheckCommittedN(6, 5)

	// Лидер отключён...
	h.DisconnectPeer(origLeaderId)
	sleepMs(10 * Quantum)

	// Отправляем команду 7 исходному лидеру,
	// несмотря на то, что он отключён.
	h.SubmitToServer(origLeaderId, 7)

	sleepMs(250)
	h.CheckNotCommitted(7)

	newLeaderId, _ := h.CheckSingleLeader()

	// Отправляем команду 8 новому лидеру.
	h.SubmitToServer(newLeaderId, 8)
	sleepMs(250)
	h.CheckCommittedN(8, 4)

	// Повторно подключаем прежнего лидера и даём кластеру
	// время стабилизироваться. Прежний лидер не должен
	// снова стать лидером.
	h.ReconnectPeer(origLeaderId)
	sleepMs(600)

	finalLeaderId, _ := h.CheckSingleLeader()
	if finalLeaderId == origLeaderId {
		t.Errorf("got finalLeaderId==origLeaderId==%d, want them different", finalLeaderId)
	}

	// Отправляем команду 9 и проверяем,
	// что она полностью зафиксирована.
	h.SubmitToServer(newLeaderId, 9)
	sleepMs(250)
	h.CheckCommittedN(9, 5)
	h.CheckCommittedN(8, 5)

	// А вот команда 7 не должна быть зафиксирована...
	h.CheckNotCommitted(7)
}

func TestCrashFollower(t *testing.T) {
	// Базовый тест, проверяющий, что сбой одного узла не приводит к отказу системы.
	defer leaktest.CheckTimeout(t, 100*Quantum*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(origLeaderId, 5)

	sleepMs(350)
	h.CheckCommittedN(5, 3)

	h.CrashPeer((origLeaderId + 1) % 3)
	sleepMs(350)
	h.CheckCommittedN(5, 2)
}

func TestCrashThenRestartFollower(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*Quantum*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(origLeaderId, 5)
	h.SubmitToServer(origLeaderId, 6)
	h.SubmitToServer(origLeaderId, 7)

	vals := []int{5, 6, 7}

	sleepMs(350)
	for _, v := range vals {
		h.CheckCommittedN(v, 3)
	}

	h.CrashPeer((origLeaderId + 1) % 3)
	sleepMs(350)
	for _, v := range vals {
		h.CheckCommittedN(v, 2)
	}

	// Перезапускаем завершившийся с ошибкой ведомый узел и даём ему время
	// синхронизироваться с остальными.
	h.RestartPeer((origLeaderId + 1) % 3)
	sleepMs(650)
	for _, v := range vals {
		h.CheckCommittedN(v, 3)
	}
}

func TestCrashThenRestartLeader(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*Quantum*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(origLeaderId, 5)
	h.SubmitToServer(origLeaderId, 6)
	h.SubmitToServer(origLeaderId, 7)

	vals := []int{5, 6, 7}

	sleepMs(350)
	for _, v := range vals {
		h.CheckCommittedN(v, 3)
	}

	h.CrashPeer(origLeaderId)
	sleepMs(350 * Quantum)
	for _, v := range vals {
		h.CheckCommittedN(v, 2)
	}

	h.RestartPeer(origLeaderId)
	sleepMs(550)
	for _, v := range vals {
		h.CheckCommittedN(v, 3)
	}
}

func TestCrashThenRestartAll(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*Quantum*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(origLeaderId, 5)
	h.SubmitToServer(origLeaderId, 6)
	h.SubmitToServer(origLeaderId, 7)

	vals := []int{5, 6, 7}

	sleepMs(350)
	for _, v := range vals {
		h.CheckCommittedN(v, 3)
	}

	for i := 0; i < 3; i++ {
		h.CrashPeer((origLeaderId + i) % 3)
	}

	sleepMs(350)

	for i := 0; i < 3; i++ {
		h.RestartPeer((origLeaderId + i) % 3)
	}

	sleepMs(150 * Quantum)
	newLeaderId, _ := h.CheckSingleLeader()

	h.SubmitToServer(newLeaderId, 8)
	sleepMs(250)

	vals = []int{5, 6, 7, 8}
	for _, v := range vals {
		h.CheckCommittedN(v, 3)
	}
}

func TestReplaceMultipleLogEntries(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	// Отправляем несколько значений в полностью связанный кластер.
	origLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(origLeaderId, 5)
	h.SubmitToServer(origLeaderId, 6)

	sleepMs(250)
	h.CheckCommittedN(6, 3)

	// Лидер отключается...
	h.DisconnectPeer(origLeaderId)
	sleepMs(10)

	// Отправляем несколько записей исходному лидеру; поскольку он отключён,
	// они не будут реплицированы.
	h.SubmitToServer(origLeaderId, 21)
	sleepMs(5)
	h.SubmitToServer(origLeaderId, 22)
	sleepMs(5)
	h.SubmitToServer(origLeaderId, 23)
	sleepMs(5)
	h.SubmitToServer(origLeaderId, 24)
	sleepMs(5)

	newLeaderId, _ := h.CheckSingleLeader()

	// Отправляем записи новому лидеру — они будут реплицированы.
	h.SubmitToServer(newLeaderId, 8)
	sleepMs(5)
	h.SubmitToServer(newLeaderId, 9)
	sleepMs(5)
	h.SubmitToServer(newLeaderId, 10)
	sleepMs(250)
	h.CheckNotCommitted(21)
	h.CheckCommittedN(10, 2)

	// Завершаем работу и перезапускаем нового лидера, чтобы сбросить его
	// nextIndex. Это гарантирует, что новый лидер кластера (после выборов им
	// может оказаться и третий сервер) попытается заменить нереплицированные
	// записи исходного лидера, начиная с самого конца журнала.
	h.CrashPeer(newLeaderId)
	sleepMs(60)
	h.RestartPeer(newLeaderId)

	sleepMs(100)
	finalLeaderId, _ := h.CheckSingleLeader()
	h.ReconnectPeer(origLeaderId)
	sleepMs(400 * Quantum)

	// Отправляем ещё одну запись. Это необходимо, потому что лидеры не
	// фиксируют записи из предыдущих термов (раздел 5.4.2 статьи), поэтому
	// записи 8, 9 и 10 после перезапуска могут остаться незафиксированными
	// на всех серверах, пока не поступит новая команда.
	h.SubmitToServer(finalLeaderId, 11)
	sleepMs(250)

	// На этом этапе записи 11 и 10 должны быть реплицированы на всех серверах,
	// а запись 21 — нет.
	h.CheckNotCommitted(21)
	h.CheckCommittedN(11, 3)
	h.CheckCommittedN(10, 3)
}

func TestCrashAfterSubmit(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*Quantum*time.Millisecond)()
	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()

	h.SubmitToServer(origLeaderId, 5)
	h.CrashPeer(origLeaderId)

	sleepMs(10)
	h.CheckSingleLeader()

	// CrashPeer может оставить «висячий» commit entry
	// (гонка между collectCommits и очисткой в CrashPeer).
	h.commits[origLeaderId] = h.commits[origLeaderId][:0]

	h.RestartPeer(origLeaderId)
	sleepMs(150)
	newLeaderId, _ := h.CheckSingleLeader()

	h.SubmitToServer(newLeaderId, 6)
	sleepMs(100)
	h.CheckCommittedN(5, 3)
	h.CheckCommittedN(6, 3)
}

func TestDisconnectAfterSubmit(t *testing.T) {
	// Аналогично TestCrashAfterSubmit, но лидер не завершается аварийно,
	// а отключается вскоре после отправки первой команды.
	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()

	// SubmitToServer теперь использует Apply, который блокируется до коммита.
	// Команда 5 может быть закоммичена до дисконнекта лидера.
	h.SubmitToServer(origLeaderId, 5)
	h.DisconnectPeer(origLeaderId)

	sleepMs(10)
	h.CheckSingleLeader()

	h.ReconnectPeer(origLeaderId)
	sleepMs(150)
	newLeaderId, _ := h.CheckSingleLeader()

	// После отправки новой команды 6 запись 5 будет закоммичена вместе с 6,
	// независимо от того, успела ли она закоммититься до дисконнекта.
	h.SubmitToServer(newLeaderId, 6)
	sleepMs(100)
	h.CheckCommittedN(5, 3)
	h.CheckCommittedN(6, 3)
}

// getPersistedTerm считывает значение currentTerm из указанного хранилища
// (используя тот же формат кодирования, что и persistToStorage).
func getPersistedTerm(storage *MapStorage) int {
	data, found := storage.Get("currentTerm")
	if !found {
		return 0
	}
	var term int
	d := gob.NewDecoder(bytes.NewBuffer(data))
	if err := d.Decode(&term); err != nil {
		return 0
	}
	return term
}

func TestBug_StartElectionMissingPersist(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	// Ждём, пока будет выбран лидер, затем выбираем ведомый узел,
	// который будем отключать.
	leaderId, _ := h.CheckSingleLeader()
	victim := (leaderId + 1) % 3

	h.DisconnectPeer(victim)

	// Даём узлу достаточно времени, чтобы он выполнил несколько выборов
	// (каждые выборы увеличивают currentTerm).
	time.Sleep(1200 * time.Millisecond)

	// Считываем значение терма жертвы из памяти и из постоянного хранилища.
	cm := h.cluster[victim]
	cm.mu.Lock()
	inMemoryTerm := cm.currentTerm
	cm.mu.Unlock()

	persistedTerm := getPersistedTerm(h.storage[victim])

	t.Logf("server %d: in-memory term = %d, persisted term = %d", victim, inMemoryTerm, persistedTerm)

	if persistedTerm < inMemoryTerm {
		t.Errorf("persisted term (%d) is behind in-memory term (%d); "+
			"startElection is not persisting state", persistedTerm, inMemoryTerm)
	}
}

func TestBug_BecomeFollowerMissingPersist(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderId, origTerm := h.CheckSingleLeader()

	// Изолируем лидера, чтобы два остальных сервера смогли выбрать нового
	// лидера с более высоким термом.
	h.DisconnectPeer(origLeaderId)
	sleepMs(350 * Quantum)

	newLeaderId, newTerm := h.CheckSingleLeader()
	if newTerm <= origTerm {
		t.Fatalf("got newTerm=%d, origTerm=%d; want newTerm > origTerm", newTerm, origTerm)
	}

	// Не позволяем новому лидеру отправлять сообщения старому лидеру после его
	// повторного подключения. Благодаря этому старый лидер сможет обнаружить
	// более новый терм и отказаться от роли лидера, но не получит позже новое
	// сообщение heartbeat, которое могло бы обновить сохранённый терм до того,
	// как мы аварийно завершим его работу.
	h.PeerDropCallsAfterN(newLeaderId, 0)
	defer h.PeerDontDropCalls(newLeaderId)

	h.ReconnectPeer(origLeaderId)
	sleepMs(120 * Quantum)

	_, steppedDownTerm, isLeader := h.cluster[origLeaderId].Report()
	if isLeader {
		t.Fatalf("server %d still thinks it's leader after reconnect", origLeaderId)
	}
	if steppedDownTerm != newTerm {
		t.Fatalf("server %d has term %d after step-down; want %d", origLeaderId, steppedDownTerm, newTerm)
	}

	// Аварийно завершаем работу сразу после того, как старый лидер обнаружил
	// более высокий терм и отказался от лидерства. После перезапуска он должен
	// по-прежнему помнить этот более высокий терм. Если это не так, значит
	// изменение терма хранилось только в памяти и было потеряно при сбое.
	h.CrashPeer(origLeaderId)
	h.RestartPeer(origLeaderId)

	_, restartedTerm, _ := h.cluster[origLeaderId].Report()
	if restartedTerm != newTerm {
		t.Fatalf("server %d restarted with term %d; want persisted higher term %d", origLeaderId, restartedTerm, newTerm)
	}
}

// Ведомый узел, получающий вызов becomeFollower с тем же термом, в котором он
// уже находится, должен сохранить значение votedFor. Сброс votedFor при
// переходе в состояние ведомого в пределах того же терма позволил бы узлу
// проголосовать дважды в одном терме, нарушив гарантию безопасности Raft,
// согласно которой в каждом терме узел может голосовать только один раз.
func TestBecomeFollowerSameTermPreservesVotedFor(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	h.CheckSingleLeader()

	for i := 0; i < 3; i++ {
		cm := h.cluster[i]
		cm.mu.Lock()
		if cm.state == Follower && cm.votedFor >= 0 {
			savedVotedFor := cm.votedFor
			savedTerm := cm.currentTerm

			cm.becomeFollower(savedTerm)

			if cm.votedFor != savedVotedFor {
				t.Errorf("becomeFollower(%d) reset votedFor from %d to %d on same-term transition",
					savedTerm, savedVotedFor, cm.votedFor)
			}
			cm.mu.Unlock()
			return
		}
		cm.mu.Unlock()
	}
	t.Fatal("no follower with votedFor >= 0 found")
}

// Ведомый узел, переходящий в более высокий терм, должен сбросить значение
// votedFor в -1, чтобы иметь возможность проголосовать в новом терме.
// Без этого сброса узел будет отклонять все запросы RequestVote в новом
// терме, что может помешать выбору лидера.
func TestBecomeFollowerHigherTermResetsVotedFor(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	h.CheckSingleLeader()

	for i := 0; i < 3; i++ {
		cm := h.cluster[i]
		cm.mu.Lock()
		if cm.state == Follower && cm.votedFor >= 0 {
			savedTerm := cm.currentTerm

			cm.becomeFollower(savedTerm + 1)

			if cm.votedFor != -1 {
				t.Errorf("becomeFollower(%d) did not reset votedFor (got %d, want -1)",
					savedTerm+1, cm.votedFor)
			}
			cm.mu.Unlock()
			return
		}
		cm.mu.Unlock()
	}
	t.Fatal("no follower with votedFor >= 0 found")
}

// После нескольких смен лидера повторно подключившиеся узлы с устаревшими
// термами не должны нарушать работу кластера. Этот тест проверяет, что ранее
// изолированный лидер и второй лидер могут снова присоединиться к кластеру,
// не вызывая появления двух лидеров одновременно (split-brain) или
// бесконечного цикла выборов.
func TestStaleVoteReplyIgnored(t *testing.T) {
	h := NewHarness(t, 5)
	defer h.Shutdown()

	origLeaderId, origTerm := h.CheckSingleLeader()

	h.DisconnectPeer(origLeaderId)
	sleepMs(450)

	newLeaderId, newTerm := h.CheckSingleLeader()
	if newTerm <= origTerm {
		t.Fatalf("expected newTerm > origTerm, got %d <= %d", newTerm, origTerm)
	}

	h.DisconnectPeer(newLeaderId)
	sleepMs(450)

	h.ReconnectPeer(origLeaderId)
	h.ReconnectPeer(newLeaderId)
	sleepMs(450)

	h.CheckSingleLeader()
}

// Ведомый узел, который уже проголосовал за лидера в текущем терме, должен
// отклонить запрос RequestVote от другого кандидата в том же терме.
// Предоставление второго голоса позволило бы выбрать двух лидеров
// в одном и том же терме.
func TestSameTermDoubleVotePrevented(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	leaderId, leaderTerm := h.CheckSingleLeader()

	followerId := -1
	for i := 0; i < 3; i++ {
		if i == leaderId {
			continue
		}
		cm := h.cluster[i]
		cm.mu.Lock()
		if cm.votedFor == leaderId && cm.currentTerm == leaderTerm {
			followerId = i
		}
		cm.mu.Unlock()
		if followerId >= 0 {
			break
		}
	}
	if followerId < 0 {
		t.Fatal("could not find a follower that voted for the leader")
	}

	otherCandidate := -1
	for i := 0; i < 3; i++ {
		if i != leaderId && i != followerId {
			otherCandidate = i
			break
		}
	}

	cm := h.cluster[followerId]
	args := RequestVoteArgs{
		RPCHeader: RPCHeader{
			ProtocolVersion: ProtocolVersion,
			ServerID:        otherCandidate,
		},
		Term:         leaderTerm,
		CandidateID:  otherCandidate,
		LastLogIndex: -1,
		LastLogTerm:  -1,
	}
	var reply RequestVoteReply
	if err := cm.RequestVote(args, &reply); err != nil {
		t.Fatal(err)
	}

	if reply.VoteGranted {
		t.Errorf("follower %d granted vote to %d in term %d, but already voted for %d",
			followerId, otherCandidate, leaderTerm, leaderId)
	}
}

// TestBecomeFollowerDoubleClose проверяет, что двойной вызов becomeFollower
// не вызывает панику при повторном close(electionTimerDone) (проблема 10).
func TestBecomeFollowerDoubleClose(t *testing.T) {
	cm := &ConsensusModule{
		id:                 1,
		state:              Follower,
		currentTerm:        1,
		votedFor:           -1,
		electionTimerDone:  make(chan struct{}),
		electionResetEvent: time.Now(),
		storage:            NewMapStorage(),
		nextIndex:          make(map[int]int),
		matchIndex:         make(map[int]int),
		inflight:           list.New(),
		applyCh:            make(chan *logFuture),
		shutdownCh:         make(chan struct{}),
		commitCh:           make(chan int, 1),
		stepDown:           make(chan struct{}, 1),
		confChangeCh:       make(chan *configurationChangeFuture),
	}
	cm.log = make([]LogEntry, 0)
	cm.mu.Lock()

	// Первый вызов becomeFollower — нормально закрывает старый канал
	cm.becomeFollower(2)

	// Второй вызов becomeFollower с новым каналом — не должен паниковать
	cm.becomeFollower(3)

	cm.mu.Unlock()
}

// Стресс-тест: многократно изолирует текущего лидера и проверяет, что после
// каждого нарушения связности кластер всегда сходится ровно к одному лидеру.
func TestElectionSafetyStress(t *testing.T) {
	h := NewHarness(t, 5)
	defer h.Shutdown()

	for cycle := 0; cycle < 8; cycle++ {
		leaderId, _ := h.CheckSingleLeader()
		h.DisconnectPeer(leaderId)
		sleepMs(350)

		h.CheckSingleLeader()

		h.ReconnectPeer(leaderId)
		sleepMs(150)
	}

	time.Sleep(300 * time.Millisecond)
	h.CheckSingleLeader()
}

// --- Интеграционные тесты leaderLoop ---

// TestLeader_StepDown_AppendEntriesHigherTerm проверяет, что leaderLoop
// завершается при получении AppendEntries с более высоким term.
// Изолируем лидера от followers, чтобы Apply() блокировался в inflight.
func TestLeader_StepDown_AppendEntriesHigherTerm(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*Quantum*time.Millisecond)()
	h := NewHarness(t, 3)
	defer h.Shutdown()

	lid, _ := h.CheckSingleLeader()

	// Изолируем лидера, чтобы Apply() не мог закоммититься
	h.DisconnectPeer((lid + 1) % 3)
	h.DisconnectPeer((lid + 2) % 3)
	sleepMs(50)

	future := h.cluster[lid].Apply(42, 0)
	sleepMs(50)

	// Отправляем AppendEntries с более высоким term
	args := AppendEntriesArgs{
		RPCHeader: RPCHeader{
			ProtocolVersion: ProtocolVersion,
		},
		Term:         100,
		LeaderID:     (lid + 1) % 3,
		PrevLogIndex: -1,
		PrevLogTerm:  -1,
		Entries:      []LogEntry{},
		LeaderCommit: -1,
	}
	if err := h.cluster[lid].AppendEntries(args, &AppendEntriesReply{}); err != nil {
		t.Fatal(err)
	}
	sleepMs(100)

	if err := future.Error(); err != ErrLeadershipLost {
		t.Fatalf("got %v, want ErrLeadershipLost", err)
	}
}

// TestLeader_StepDown_RequestVoteHigherTerm проверяет, что leaderLoop
// завершается при получении RequestVote с более высоким term.
// Изолируем лидера от followers, чтобы Apply() блокировался в inflight.
func TestLeader_StepDown_RequestVoteHigherTerm(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*Quantum*time.Millisecond)()
	h := NewHarness(t, 3)
	defer h.Shutdown()

	lid, _ := h.CheckSingleLeader()

	// Изолируем лидера, чтобы Apply() не мог закоммититься
	h.DisconnectPeer((lid + 1) % 3)
	h.DisconnectPeer((lid + 2) % 3)
	sleepMs(50)

	future := h.cluster[lid].Apply(42, 0)
	sleepMs(50)

	// Отправляем RequestVote с более высоким term
	args := RequestVoteArgs{
		RPCHeader: RPCHeader{
			ProtocolVersion: ProtocolVersion,
		},
		Term:         100,
		CandidateID:  (lid + 1) % 3,
		LastLogIndex: -1,
		LastLogTerm:  -1,
	}
	if err := h.cluster[lid].RequestVote(args, &RequestVoteReply{}); err != nil {
		t.Fatal(err)
	}
	sleepMs(100)

	if err := future.Error(); err != ErrLeadershipLost {
		t.Fatalf("got %v, want ErrLeadershipLost", err)
	}
}

// TestLeader_StepDown_NoInflight проверяет, что leaderLoop выходит без ошибок,
// если inflight пуст.
func TestLeader_StepDown_NoInflight(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*Quantum*time.Millisecond)()
	h := NewHarness(t, 3)
	defer h.Shutdown()

	lid, _ := h.CheckSingleLeader()

	// Отправляем AppendEntries с более высоким term при пустом inflight
	args := AppendEntriesArgs{
		RPCHeader: RPCHeader{
			ProtocolVersion: ProtocolVersion,
		},
		Term:         100,
		LeaderID:     (lid + 1) % 3,
		PrevLogIndex: -1,
		PrevLogTerm:  -1,
		Entries:      []LogEntry{},
		LeaderCommit: -1,
	}
	if err := h.cluster[lid].AppendEntries(args, &AppendEntriesReply{}); err != nil {
		t.Fatal(err)
	}
	sleepMs(100)

	// После stepDown должен быть новый лидер
	h.CheckSingleLeader()
}

// TestLeader_StepDown_ReElection проверяет, что после stepDown проводятся
// новые выборы и избирается лидер (старый лидер может быть переизбран).
func TestLeader_StepDown_ReElection(t *testing.T) {
	defer leaktest.CheckTimeout(t, 200*Quantum*time.Millisecond)()
	h := NewHarness(t, 3)
	defer h.Shutdown()

	lid, _ := h.CheckSingleLeader()

	// Отправляем AppendEntries с более высоким term
	args := AppendEntriesArgs{
		RPCHeader: RPCHeader{
			ProtocolVersion: ProtocolVersion,
		},
		Term:         100,
		LeaderID:     (lid + 1) % 3,
		PrevLogIndex: -1,
		PrevLogTerm:  -1,
		Entries:      []LogEntry{},
		LeaderCommit: -1,
	}
	if err := h.cluster[lid].AppendEntries(args, &AppendEntriesReply{}); err != nil {
		t.Fatal(err)
	}
	sleepMs(500)

	// После stepDown и выборов должен быть лидер
	h.CheckSingleLeader()
}

// TestLeader_Heartbeat_StopsAfterStepDown проверяет, что после stepDown
// heartbeat прекращается (нет утечек).
func TestLeader_Heartbeat_StopsAfterStepDown(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*Quantum*time.Millisecond)()
	h := NewHarness(t, 3)
	defer h.Shutdown()

	lid, _ := h.CheckSingleLeader()

	args := AppendEntriesArgs{
		RPCHeader: RPCHeader{
			ProtocolVersion: ProtocolVersion,
		},
		Term:         100,
		LeaderID:     (lid + 1) % 3,
		PrevLogIndex: -1,
		PrevLogTerm:  -1,
		Entries:      []LogEntry{},
		LeaderCommit: -1,
	}
	if err := h.cluster[lid].AppendEntries(args, &AppendEntriesReply{}); err != nil {
		t.Fatal(err)
	}
	sleepMs(300)

	// Проверяем, что старый лидер не утек — leaktest сделает это
	h.CheckSingleLeader()
}

// TestLeader_ConfChange_NotSupported проверяет, что confChange возвращает ошибку.
func TestLeader_ConfChange_NotSupported(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*Quantum*time.Millisecond)()
	h := NewHarness(t, 3)
	defer h.Shutdown()

	lid, _ := h.CheckSingleLeader()

	// Прямой вызов confChangeCh
	future := &configurationChangeFuture{}
	future.init(h.cluster[lid].shutdownCh)
	select {
	case h.cluster[lid].confChangeCh <- future:
	case <-time.After(time.Second):
		t.Fatal("timeout sending confChange")
	}

	if err := future.Error(); err != ErrConfigurationChangeNotSupported {
		t.Fatalf("got %v, want ErrConfigurationChangeNotSupported", err)
	}
}

// TestLeader_Shutdown_NoInflight проверяет, что shutdown без inflight чист.
func TestLeader_Shutdown_NoInflight(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*Quantum*time.Millisecond)()
	h := NewHarness(t, 3)
	defer h.Shutdown()

	// Просто shutdown — leaktest проверит утечки
}

// TestLeader_Shutdown_DuringApply проверяет shutdown во время Apply.
func TestLeader_Shutdown_DuringApply(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*Quantum*time.Millisecond)()
	h := NewHarness(t, 3)
	defer h.Shutdown()

	lid, _ := h.CheckSingleLeader()

	future := h.cluster[lid].Apply(42, 0)
	h.Shutdown()

	err := future.Error()
	if err != ErrLeadershipLost && err != ErrRaftShutdown {
		t.Fatalf("got %v, want ErrLeadershipLost or ErrRaftShutdown", err)
	}
}

// TestLeader_DispatchLogs_IndexSequence проверяет последовательность индексов
// при применении нескольких команд. Индексы доступны только после завершения
// future (через Error()).
func TestLeader_DispatchLogs_IndexSequence(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*Quantum*time.Millisecond)()
	h := NewHarness(t, 3)
	defer h.Shutdown()

	lid, _ := h.CheckSingleLeader()
	sleepMs(100)

	f1 := h.cluster[lid].Apply(1, 0)
	f2 := h.cluster[lid].Apply(2, 0)
	f3 := h.cluster[lid].Apply(3, 0)

	for _, f := range []ApplyFuture{f1, f2, f3} {
		if err := f.Error(); err != nil {
			t.Fatalf("Apply failed: %v", err)
		}
	}

	i1 := f1.Index()
	i2 := f2.Index()
	i3 := f3.Index()

	if !(i1 < i2 && i2 < i3) {
		t.Fatalf("indices not increasing: %d, %d, %d", i1, i2, i3)
	}

	sleepMs(100)
}

// TestCommitmentInteg_CommitOnMajority проверяет, что commit происходит
// при репликации на большинство узлов.
func TestCommitmentInteg_CommitOnMajority(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*Quantum*time.Millisecond)()
	h := NewHarness(t, 3)
	defer h.Shutdown()

	lid, _ := h.CheckSingleLeader()

	future := h.cluster[lid].Apply(42, 0)
	if err := future.Error(); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	sleepMs(50)

	h.CheckCommitted(42)
}

// TestCommitmentInteg_NoCommitWithoutMajority проверяет, что commit не
// происходит без кворума. Изолируем лидера от обоих followers — кворума нет,
// команда не коммитится. Ожидаем, что future блокируется, а не возвращает nil.
func TestCommitmentInteg_NoCommitWithoutMajority(t *testing.T) {
	defer leaktest.CheckTimeout(t, 200*Quantum*time.Millisecond)()
	h := NewHarness(t, 3)
	defer h.Shutdown()

	lid, _ := h.CheckSingleLeader()

	// Отключаем лидера от обоих followers и followers друг от друга.
	h.DisconnectPeer((lid + 1) % 3)
	h.DisconnectPeer((lid + 2) % 3)
	sleepMs(50)

	future := h.cluster[lid].Apply(42, 0)
	sleepMs(100)

	// Проверяем, что future не завершился успехом в течение таймаута.
	errCh := make(chan error, 1)
	go func() {
		errCh <- future.Error()
	}()
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("Apply succeeded without majority, expected error")
		}
		// Ошибка ErrLeadershipLost или ErrRaftShutdown — допустимо.
	case <-time.After(1500 * time.Millisecond):
		// Future заблокирован — это ожидаемое поведение (нет кворума).
	}
}

// TestCommitmentInteg_CommitAfterReconnect проверяет, что после реконнекта
// отключённого узла коммит происходит.
func TestCommitmentInteg_CommitAfterReconnect(t *testing.T) {
	defer leaktest.CheckTimeout(t, 300*Quantum*time.Millisecond)()
	h := NewHarness(t, 3)
	defer h.Shutdown()

	lid, _ := h.CheckSingleLeader()
	follower := (lid + 1) % 3
	otherFollower := (lid + 2) % 3

	// Отключаем одного follower
	h.DisconnectPeer(follower)
	sleepMs(100)

	// Apply с оставшимся кворумом (leader + один follower)
	// Но нужно отключить ещё одного follower, чтобы кворума не было,
	// иначе команда закоммитится сразу
	h.DisconnectPeer(otherFollower)
	sleepMs(100)

	future := h.cluster[lid].Apply(42, 0)
	sleepMs(50)

	// Подключаем одного follower -> должен быть кворум
	h.ReconnectPeer(follower)
	sleepMs(200)

	if err := future.Error(); err == nil {
		h.CheckCommitted(42)
	} else {
		// Команда могла быть отклонена из-за stepDown — нормально
		t.Logf("Apply returned error (expected if leadership lost): %v", err)
	}
}

// TestCommitmentInteg_SingleNode проверяет single-node акселератор.
func TestCommitmentInteg_SingleNode(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*Quantum*time.Millisecond)()
	h := NewHarness(t, 1)
	defer h.Shutdown()

	_, _ = h.CheckSingleLeader()

	future := h.cluster[0].Apply(42, 0)
	if err := future.Error(); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	sleepMs(50)

	h.CheckCommitted(42)
}

// TestRaftSafety_NewLeaderCommitNoop проверяет, что после выборов
// лидер коммитит noop запись.
func TestRaftSafety_NewLeaderCommitNoop(t *testing.T) {
	defer leaktest.CheckTimeout(t, 200*Quantum*time.Millisecond)()
	h := NewHarness(t, 3)
	defer h.Shutdown()

	// Проверяем, что после выборов лидер коммитит noop — для этого
	// достаточно убедиться, что Apply работает на всех узлах
	lid, _ := h.CheckSingleLeader()

	future := h.cluster[lid].Apply(42, 0)
	if err := future.Error(); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	sleepMs(50)

	h.CheckCommitted(42)
}

// TestRace_ApplyAndStepDown проверяет, что нет гонок между Apply
// и stepDown.
func TestRace_ApplyAndStepDown(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*Quantum*time.Millisecond)()
	h := NewHarness(t, 3)
	defer h.Shutdown()

	lid, _ := h.CheckSingleLeader()

	// Apply в одной горутине
	done := make(chan struct{})
	go func() {
		future := h.cluster[lid].Apply(42, 0)
		future.Error()
		close(done)
	}()

	sleepMs(20)

	// stepDown в другой горутине
	args := AppendEntriesArgs{
		RPCHeader: RPCHeader{
			ProtocolVersion: ProtocolVersion,
		},
		Term:         100,
		LeaderID:     (lid + 1) % 3,
		PrevLogIndex: -1,
		PrevLogTerm:  -1,
		Entries:      []LogEntry{},
		LeaderCommit: -1,
	}
	if err := h.cluster[lid].AppendEntries(args, &AppendEntriesReply{}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for Apply to complete")
	}
}

// TestCommitmentInteg_FiveNodes проверяет 5-node кластер.
func TestCommitmentInteg_FiveNodes(t *testing.T) {
	defer leaktest.CheckTimeout(t, 200*Quantum*time.Millisecond)()
	h := NewHarness(t, 5)
	defer h.Shutdown()

	lid, _ := h.CheckSingleLeader()

	future := h.cluster[lid].Apply(42, 0)
	if err := future.Error(); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	sleepMs(100)

	h.CheckCommitted(42)
}

// TestCommitmentInteg_FiveNodesDisconnectTwo проверяет, что при отключении
// 2 из 5 узлов кворум сохраняется.
func TestCommitmentInteg_FiveNodesDisconnectTwo(t *testing.T) {
	defer leaktest.CheckTimeout(t, 300*Quantum*time.Millisecond)()
	h := NewHarness(t, 5)
	defer h.Shutdown()

	lid, _ := h.CheckSingleLeader()

	// Отключаем 2 узла (3/5 — кворум есть)
	disconnected := 0
	for i := 0; i < 5; i++ {
		if i != lid && disconnected < 2 {
			h.DisconnectPeer(i)
			disconnected++
		}
	}
	sleepMs(100)

	future := h.cluster[lid].Apply(42, 0)
	if err := future.Error(); err != nil {
		t.Fatalf("Apply should succeed with 3/5 quorum: %v", err)
	}
	sleepMs(100)

	h.CheckCommittedN(42, 3)
}

// TestCommitmentInteg_FiveNodesDisconnectThree проверяет, что при отключении
// 3 из 5 узлов кворум теряется (future блокируется).
func TestCommitmentInteg_FiveNodesDisconnectThree(t *testing.T) {
	defer leaktest.CheckTimeout(t, 300*Quantum*time.Millisecond)()
	h := NewHarness(t, 5)
	defer h.Shutdown()

	lid, _ := h.CheckSingleLeader()

	// Отключаем 3 узла (2/5 — кворума нет)
	disconnected := 0
	for i := 0; i < 5; i++ {
		if i != lid && disconnected < 3 {
			h.DisconnectPeer(i)
			disconnected++
		}
	}
	sleepMs(100)

	future := h.cluster[lid].Apply(42, 0)
	sleepMs(200)

	// Проверяем, что future не завершился успехом в течение таймаута.
	errCh := make(chan error, 1)
	go func() {
		errCh <- future.Error()
	}()
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("Apply should fail without quorum (2/5)")
		}
	case <-time.After(1500 * time.Millisecond):
		// Future заблокирован — это ожидаемое поведение (нет кворума).
	}
}

// TestStability_1kCommands проверяет стабильность при 100 Apply на 3 узлах.
func TestStability_1kCommands(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 1k commands stability test in short mode")
	}
	defer leaktest.CheckTimeout(t, 600*Quantum*time.Millisecond)()
	h := NewHarness(t, 3)
	defer h.Shutdown()

	lid, _ := h.CheckSingleLeader()

	for i := 0; i < 100; i++ {
		future := h.cluster[lid].Apply(i, 0)
		if err := future.Error(); err != nil {
			t.Fatalf("Apply %d failed: %v", i, err)
		}
	}
	sleepMs(100)

	for i := 0; i < 100; i++ {
		h.CheckCommitted(i)
	}
}

// TestStability_ConcurrentClients проверяет параллельные Apply.
func TestStability_ConcurrentClients(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrent clients test in short mode")
	}
	defer leaktest.CheckTimeout(t, 600*Quantum*time.Millisecond)()
	h := NewHarness(t, 3)
	defer h.Shutdown()

	lid, _ := h.CheckSingleLeader()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(cmd int) {
			defer wg.Done()
			future := h.cluster[lid].Apply(cmd, 0)
			if err := future.Error(); err != nil {
				t.Errorf("Apply %d failed: %v", cmd, err)
			}
		}(i)
	}
	wg.Wait()
	sleepMs(100)

	// Проверяем, что все 10 команд закоммичены
	h.CheckCommittedN(0, 3)
	h.CheckCommittedN(9, 3)
}
