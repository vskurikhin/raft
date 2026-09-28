package raft

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/vskurikhin/raft/pkg/raft/contract"
	"github.com/vskurikhin/raft/pkg/raft/store"
	"github.com/vskurikhin/raft/pkg/raft/transp"
)

// statsCM — CM, собранный литералом для прямого выпуска статистики
// (без запущенной горутины stats).
func statsCM(trans Transport, limits contract.Limits) *ConsensusModule {
	cm := &ConsensusModule{limits: limits, transport: trans, storage: store.NewMapStorage()}
	cm.cmState.state = Follower
	cm.cmState.commitIndex = -1
	cm.cmState.lastLogIndex = -1
	return cm
}

// persistLine выпускает отчёт и возвращает документ PersistV2 и строку.
func persistLine(t *testing.T, cm *ConsensusModule) (map[string]any, string) {
	t.Helper()
	var out, diag bytes.Buffer
	cm.publishStats(&out, &diag)
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("stats lines = %d, want 3: %q", len(lines), out.String())
	}
	line := lines[2]
	var doc map[string]any
	if err := json.Unmarshal([]byte(line[strings.Index(line, "{"):]), &doc); err != nil {
		t.Fatalf("PersistV2: %v", err)
	}
	return doc, line
}

// rejectionRows извлекает строки protocol_limit_rejections_total.
func rejectionRows(doc map[string]any) []map[string]any {
	raw, _ := doc["ProtocolLimitRejectionsTotal"].([]any)
	rows := make([]map[string]any, 0, len(raw))
	for _, r := range raw {
		rows = append(rows, r.(map[string]any))
	}
	return rows
}

func hasRow(rows []map[string]any, direction, parameter, peer string, count float64) bool {
	for _, r := range rows {
		if r["Direction"] == direction && r["Parameter"] == parameter && r["Peer"] == peer && r["Count"] == count {
			return true
		}
	}
	return false
}

// TestLimitRejectionsPublishedTCPReceive — реальный TCP: получатель с
// меньшими D/N/C отвергает кадры; счётчик приёма по параметру и адресу
// соседа публикуется в строке PersistV2 (protocol_limit_rejections_total)
// без содержимого Data; при отсутствии отказов поле не публикуется.
func TestLimitRejectionsPublishedTCPReceive(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()
	small := contract.Limits{MaxFrameBytes: 262144, MaxEntries: 3, MaxDataBytes: 3000, MaxConfigurationBytes: 16}
	receiver, err := transp.NewTCPTransportWithLimits("127.0.0.1:0", uniformTCPTimeouts(time.Second), 2, small)
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	sender, err := transp.NewTCPTransport("127.0.0.1:0", uniformTCPTimeouts(time.Second), 2)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	sender.Connect(1, string(receiver.LocalAddr()))

	cm := statsCM(receiver, small)
	doc, line := persistLine(t, cm)
	if _, ok := doc["ProtocolLimitRejectionsTotal"]; ok || strings.Contains(line, "ProtocolLimitRejectionsTotal") {
		t.Fatalf("field published without rejections: %s", line)
	}

	payload := strings.Repeat("P", 4000)
	if _, err = sender.AppendEntries(1, AppendEntriesArgs{RPCHeader: hdr3(0), Term: 1,
		Entries: []LogEntry{{Index: 1, Term: 1, Data: payload}}}); err == nil {
		t.Fatal("receiver accepted Data > D")
	}
	if _, err = sender.AppendEntries(1, AppendEntriesArgs{RPCHeader: hdr3(0), Term: 1,
		Entries: make([]LogEntry, 4)}); err == nil {
		t.Fatal("receiver accepted N+1 entries")
	}
	if _, err = sender.InstallSnapshot(1, InstallSnapshotRequest{RPCHeader: hdr3(0), Term: 1, LastLogIndex: 1,
		LastLogTerm: 1, Configuration: make([]byte, 40), DataSize: 1}, strings.NewReader("x")); err == nil {
		t.Fatal("receiver accepted configuration > C")
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(receiver.LimitRejections()) < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	doc, line = persistLine(t, cm)
	rows := rejectionRows(doc)
	for _, parameter := range []string{"D", "N", "C"} {
		if !hasRow(rows, "recv", parameter, "127.0.0.1", 1) {
			t.Fatalf("recv %s not published: %s", parameter, line)
		}
	}
	if strings.Contains(line, "PPPP") {
		t.Fatal("payload leaked into statistics")
	}
}

// TestLimitRejectionsPublishedSendAndPreflight — отказ отправки по пределу
// (легаси-запись больше D) и локальный preflight Apply/конфигурации
// публикуются в той же строке с направлением send/preflight.
func TestLimitRejectionsPublishedSendAndPreflight(t *testing.T) {
	defer leaktest.CheckTimeout(t, LeaktestBudget)()
	limits := contract.Limits{MaxFrameBytes: 4128, MaxEntries: 3, MaxDataBytes: 64, MaxConfigurationBytes: 16}
	sender, err := transp.NewInmemTransportWithLimits("l", limits)
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := transp.NewInmemTransportWithLimits("r", limits)
	if err != nil {
		t.Fatal(err)
	}
	sender.Connect(1, receiver)
	defer sender.Close()
	defer receiver.Close()

	cm := commitRuleCM(1, []LogEntry{{Index: 0, Term: 1, Data: "ok"}, {Index: 1, Term: 1, Data: strings.Repeat("Q", 200)}}, -1)
	cm.limits = limits
	cm.transport = sender
	cm.cmState.state = Leader
	cm.leaderState.nextIndex = map[int]int{1: 1}
	cm.leaderState.matchIndex = map[int]int{1: 0}
	cm.leaderState.inflightAE = map[int]*atomic.Bool{1: new(atomic.Bool)}
	cm.leaderState.nextVerifyRedispatchAt = map[int]time.Time{}
	cm.leaderSendAEsToPeer(1, 1, 0, true)

	if err := cm.Apply(strings.Repeat("Q", 500), time.Second).Error(); err == nil {
		t.Fatal("oversized Apply accepted")
	}
	doc, line := persistLine(t, cm)
	rows := rejectionRows(doc)
	if !hasRow(rows, "send", "D", "1", 1) || !hasRow(rows, "preflight", "D", "local", 1) {
		t.Fatalf("send/preflight not published: %s", line)
	}
	if strings.Contains(line, "QQQQ") {
		t.Fatal("payload leaked into statistics")
	}
}

// TestInmemReceiverRejectionCounted — отказ приёма Inmem по пределам
// получателя учитывается транспортом получателя по параметру и отправителю.
func TestInmemReceiverRejectionCounted(t *testing.T) {
	small := contract.Limits{MaxFrameBytes: 262144, MaxEntries: 3, MaxDataBytes: 3000, MaxConfigurationBytes: 16}
	sender := transp.NewInmemTransport("sender")
	receiver, err := transp.NewInmemTransportWithLimits("receiver", small)
	if err != nil {
		t.Fatal(err)
	}
	sender.Connect(1, receiver)
	defer sender.Close()
	defer receiver.Close()
	if _, err = sender.AppendEntries(1, AppendEntriesArgs{RPCHeader: hdr3(0),
		Entries: []LogEntry{{Data: strings.Repeat("x", 4000)}}}); err == nil {
		t.Fatal("accepted")
	}
	if got := receiver.LimitRejections(); got["D|sender"] != 1 {
		t.Fatalf("receiver counts %v", got)
	}
}
