package raft

import (
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vskurikhin/raft/pkg/raft/contract"
	"github.com/vskurikhin/raft/pkg/raft/protocol"
	"github.com/vskurikhin/raft/pkg/raft/transp"
)

// _freshV34Env переводит тестовый бинарник в режим вспомогательного процесса
// с большим числом зарегистрированных типов.
const _freshV34Env = "RAFT_V34_FRESH_HELPER"

// registerFreshUserTypes регистрирует count различных пользовательских типов
// под собственными именами, наполняя глобальный реестр gob заранее.
func registerFreshUserTypes(count int) {
	for i := range count {
		typ := reflect.StructOf([]reflect.StructField{{Name: fmt.Sprintf("F%d", i), Type: reflect.TypeFor[int]()}})
		gob.RegisterName(fmt.Sprintf("fresh.v34.T%d", i), reflect.New(typ).Elem().Interface())
	}
}

// TestV34FreshProcessManyTypes — V27/V30/V34: в отдельном процессе с более
// чем 64 заранее зарегистрированными типами проверяются аналитическая
// граница D на границе и смена лидера на узел того же процесса. Запись ровно
// по границе принимается, запись выше границы отвергается именованной
// ошибкой ErrCommandTooLarge без роста журнала — и до, и после смены лидера.
func TestV34FreshProcessManyTypes(t *testing.T) {
	if os.Getenv(_freshV34Env) == "1" {
		runFreshV34Helper(t)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestV34FreshProcessManyTypes$", "-test.count=1")
	cmd.Env = append(os.Environ(), _freshV34Env+"=1")
	out, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(out), "V34-FRESH-OK") {
		t.Fatalf("вспомогательный процесс: %v\n%s", err, out)
	}
	t.Logf("вывод дочернего процесса:\n%s", out)
}

// runFreshV34Helper выполняет сценарий границы D и смены лидера в процессе с
// широким реестром типов.
func runFreshV34Helper(t *testing.T) {
	registerFreshUserTypes(80)

	h := NewHarness(t, 3)
	defer h.Shutdown()
	leader, _ := h.CheckSingleLeader()
	boundary := boundaryCommand(t)
	checkBoundaryRejection(t, h.cluster[leader], boundary, "исходный лидер")

	target := (leader + 1) % 3
	if err := h.LeadershipTransfer(leader, ServerID(target)).Error(); err != nil {
		t.Fatalf("LeadershipTransfer: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if l, _ := h.CheckSingleLeader(); l == target {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if l, _ := h.CheckSingleLeader(); l != target {
		t.Fatalf("лидерство не перешло на узел %d, лидер %d", target, l)
	}
	checkBoundaryRejection(t, h.cluster[target], boundary, "новый лидер")
	runV34LegacyLimitScenario(t)
	fmt.Println("V34-FRESH-OK")
}

// runV34LegacyLimitScenario — V34: legacy-запись длиннее D не отправляется:
// именованный отказ ErrLimit, счётчик parameter=D по соседу, индекс записи в
// классификации, nextIndex и фиксация не меняются — молчаливого прогресса
// нет. Диагностика (счётчик/индекс) не содержит Data.
func runV34LegacyLimitScenario(t *testing.T) {
	t.Helper()
	limits := contract.Limits{MaxFrameBytes: 4128, MaxEntries: 3, MaxDataBytes: 64, MaxConfigurationBytes: 16}
	sender, err := transp.NewInmemTransportWithLimits("v34-l", limits)
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := transp.NewInmemTransportWithLimits("v34-r", limits)
	if err != nil {
		t.Fatal(err)
	}
	sender.Connect(1, receiver)
	defer sender.Close()
	defer receiver.Close()

	cm := commitRuleCM(1, []LogEntry{
		{Index: 0, Term: 1, Data: "ok"},
		{Index: 1, Term: 1, Data: strings.Repeat("x", 200)},
	}, -1)
	cm.limits = limits
	cm.transport = sender
	cm.cmState.state = Leader
	cm.leaderState.nextIndex = map[int]int{1: 1}
	cm.leaderState.matchIndex = map[int]int{1: 0}
	cm.leaderState.inflightAE = map[int]*atomic.Bool{1: new(atomic.Bool)}
	cm.leaderState.nextVerifyRedispatchAt = map[int]time.Time{}

	cm.mu.Lock()
	commitBefore := cm.cmState.commitIndex
	nextBefore := cm.leaderState.nextIndex[1]
	cm.mu.Unlock()

	cm.leaderSendAEsToPeer(1, 1, 0, true)

	cm.mu.Lock()
	counts := cm.limitRejectionsLocked()
	nextAfter := cm.leaderState.nextIndex[1]
	commitAfter := cm.cmState.commitIndex
	cm.mu.Unlock()
	if counts[limitRejectionKey{direction: limitDirectionSend, peer: "1", parameter: limitParameterD}] != 1 {
		t.Fatalf("счётчик parameter=D: %v", counts)
	}
	if nextAfter != nextBefore {
		t.Fatalf("nextIndex изменён: %d -> %d", nextBefore, nextAfter)
	}
	if commitAfter != commitBefore {
		t.Fatalf("фиксация продолжилась: %d -> %d", commitBefore, commitAfter)
	}
	parameter, index, actual := classifyAELimit(cm.cmState.log[1:], limits)
	if parameter != limitParameterD || index != 1 || actual <= 64 {
		t.Fatalf("классификация: %s %d %d", parameter, index, actual)
	}
	fmt.Printf("V34 legacy-limit parameter=%s index=%d actual=%d nextIndex=%d\n", parameter, index, actual, nextAfter)
}

// boundaryCommand возвращает строку, сетевая длина которой ровно равна
// MaxDataBytes действующего профиля.
func boundaryCommand(t *testing.T) string {
	t.Helper()
	const d = 8192
	n := d - 64
	for {
		size, err := protocol.MeasureData(strings.Repeat("y", n+1), 1<<20)
		if err != nil {
			t.Fatalf("MeasureData: %v", err)
		}
		if size > d {
			break
		}
		n++
	}
	return strings.Repeat("y", n)
}

// checkBoundaryRejection проверяет, что запись ровно по границе принимается,
// а на один байт длиннее — отвергается без роста журнала.
func checkBoundaryRejection(t *testing.T, cm *ConsensusModule, boundary, label string) {
	t.Helper()
	if err := cm.Apply(boundary, 2*time.Second).Error(); err != nil {
		t.Fatalf("%s: запись по границе D отвергнута: %v", label, err)
	}
	cm.mu.Lock()
	mid := cm.cmState.lastLogIndex
	cm.mu.Unlock()
	err := cm.Apply(boundary+"x", 2*time.Second).Error()
	if !errors.Is(err, ErrCommandTooLarge) {
		t.Fatalf("%s: запись выше границы D: %v", label, err)
	}
	cm.mu.Lock()
	after := cm.cmState.lastLogIndex
	cm.mu.Unlock()
	if after != mid {
		t.Fatalf("%s: отвергнутая запись изменила журнал %d -> %d", label, mid, after)
	}
}
