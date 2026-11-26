package pkg

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/vskurikhin/raft/pkg/api"
)

// expectedBasis — базис b51e637, от которого собирается baseline A.
const expectedBasis = "b51e6373f3fe8ecc2739f15f09dd8f092d3090ad"

// baselineManifest — ожидаемая структура provenance для baseline A.
type baselineManifest struct {
	Basis            string `json:"basis"`
	ExecutableSHA256 string `json:"executable_sha256"`
	GoVersion        string `json:"go_version"`
	Goos             string `json:"goos"`
	Goarch           string `json:"goarch"`
	Cgo              string `json:"cgo"`
	Recipe           string `json:"recipe"`
}

// stage4Node — один процесс raftkv в тесте.
type stage4Node struct {
	id       int
	httpAddr string
	rpcAddr  string
	dataDir  string
	logFile  string
	cmd      *exec.Cmd
	cancel   context.CancelFunc
}

// TestStage4HTTPParity сравнивает HTTP-семантику baseline A (b51e637) и
// текущей production-сборки на двух изолированных трёхузловых кластерах.
// A берётся из RAFT_STAGE4_BASELINE_BIN, provenance — из
// RAFT_STAGE4_BASELINE_MANIFEST. Без переменной тест пропускается; при
// заданном неверном artifact/manifest — FAIL.
func TestStage4HTTPParity(t *testing.T) {
	baselineBin := os.Getenv("RAFT_STAGE4_BASELINE_BIN")
	manifestPath := os.Getenv("RAFT_STAGE4_BASELINE_MANIFEST")
	if baselineBin == "" || manifestPath == "" {
		t.Skip("RAFT_STAGE4_BASELINE_BIN and RAFT_STAGE4_BASELINE_MANIFEST must be set for AC1")
	}

	if _, err := os.Stat(baselineBin); err != nil {
		t.Fatalf("baseline binary %s: %v", baselineBin, err)
	}
	if _, err := os.Stat(manifestPath); err != nil {
		t.Fatalf("baseline manifest %s: %v", manifestPath, err)
	}
	manifest := readBaselineManifest(t, manifestPath)
	verifyManifest(t, manifest, baselineBin)

	productionBin := buildProductionBinary(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	basePorts := allocatePorts(t, 6)
	prodPorts := allocatePorts(t, 6)

	baseDir := t.TempDir()
	prodDir := t.TempDir()

	baseNodes := startStage4Cluster(t, ctx, baselineBin, baseDir, basePorts, "base")
	prodNodes := startStage4Cluster(t, ctx, productionBin, prodDir, prodPorts, "prod")

	defer stopStage4Cluster(baseNodes)
	defer stopStage4Cluster(prodNodes)

	baseLeader := waitForStage4Leader(t, baseNodes)
	prodLeader := waitForStage4Leader(t, prodNodes)

	runDeterministicScenario(t, baseNodes, baseLeader, "base")
	runDeterministicScenario(t, prodNodes, prodLeader, "prod")

	compareFinalState(t, baseNodes, prodNodes)
}

// readBaselineManifest читает и разбирает JSON provenance.
func readBaselineManifest(t *testing.T, path string) baselineManifest {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var m baselineManifest
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	return m
}

// verifyManifest проверяет basis, SHA бинарника и совместимость окружения.
func verifyManifest(t *testing.T, m baselineManifest, binPath string) {
	t.Helper()
	if m.Basis != expectedBasis {
		t.Fatalf("manifest basis = %q, want %q", m.Basis, expectedBasis)
	}
	want := strings.ToLower(m.ExecutableSHA256)
	got := sha256File(t, binPath)
	if got != want {
		t.Fatalf("baseline SHA256 = %q, manifest wants %q", got, want)
	}
	if m.Goos != "" && m.Goos != runtime.GOOS {
		t.Fatalf("baseline GOOS = %q, current %q", m.Goos, runtime.GOOS)
	}
	if m.Goarch != "" && m.Goarch != runtime.GOARCH {
		t.Fatalf("baseline GOARCH = %q, current %q", m.Goarch, runtime.GOARCH)
	}
	if m.GoVersion != "" && m.GoVersion != runtime.Version() {
		t.Fatalf("baseline Go version = %q, current %q", m.GoVersion, runtime.Version())
	}
}

// sha256File возвращает SHA-256 файла в hex.
func sha256File(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// buildProductionBinary собирает текущий код ./cmd во временный бинарник.
func buildProductionBinary(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "raftkv")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd")
	cmd.Dir = ".." // пакет pkg находится в подкаталоге
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build production raftkv: %v\n%s", err, out)
	}
	return bin
}

// allocatePorts выделяет count свободных TCP-портов.
func allocatePorts(t *testing.T, count int) []int {
	t.Helper()
	ports := make([]int, 0, count)
	listeners := make([]net.Listener, 0, count)
	for i := 0; i < count; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		_, portStr, err := net.SplitHostPort(ln.Addr().String())
		if err != nil {
			t.Fatalf("split host port: %v", err)
		}
		port, err := net.LookupPort("tcp", portStr)
		if err != nil {
			t.Fatalf("lookup port: %v", err)
		}
		ports = append(ports, port)
		listeners = append(listeners, ln)
	}
	for _, ln := range listeners {
		_ = ln.Close()
	}
	return ports
}

// stage4PeerList формирует список соседей для узла ownID по шести портам
// кластера (HTTP и RPC каждого узла). Собственный узел исключается: иначе
// начальная конфигурация получит дубликат сервера и кворум станет
// недостижим при потере узла (см. setInitialConfiguration: сервер добавляет
// себя поверх peerIds).
func stage4PeerList(ports []int, ownID int) string {
	var sb strings.Builder
	for j := 0; j < 3; j++ {
		if j == ownID {
			continue
		}
		if sb.Len() > 0 {
			sb.WriteString(",")
		}
		fmt.Fprintf(&sb, "%d=127.0.0.1:%d", j, ports[j*2+1])
	}
	return sb.String()
}

// stage4PeerListByAddr формирует список соседей по RPC-адресам узлов,
// исключая собственный узел (см. stage4PeerList).
func stage4PeerListByAddr(rpcAddrs []string, ownID int) string {
	var sb strings.Builder
	for j, addr := range rpcAddrs {
		if j == ownID {
			continue
		}
		if sb.Len() > 0 {
			sb.WriteString(",")
		}
		fmt.Fprintf(&sb, "%d=%s", j, addr)
	}
	return sb.String()
}

// startStage4Cluster поднимает три процесса raftkv с одинаковыми флагами.
func startStage4Cluster(t *testing.T, ctx context.Context, bin, baseDir string, ports []int, label string) []*stage4Node {
	t.Helper()
	if len(ports) != 6 {
		t.Fatalf("need 6 ports, got %d", len(ports))
	}
	nodes := make([]*stage4Node, 3)

	for i := 0; i < 3; i++ {
		httpPort := ports[i*2]
		rpcPort := ports[i*2+1]
		dataDir := filepath.Join(baseDir, fmt.Sprintf("node-%d", i))
		if err := os.MkdirAll(dataDir, 0o750); err != nil {
			t.Fatalf("mkdir %s: %v", dataDir, err)
		}
		logFile := filepath.Join(baseDir, fmt.Sprintf("%s-node-%d.log", label, i))
		lf, err := os.Create(logFile)
		if err != nil {
			t.Fatalf("create log %s: %v", logFile, err)
		}

		nodeCtx, cancel := context.WithCancel(ctx)
		cmd := exec.CommandContext(nodeCtx, bin,
			"-number", fmt.Sprintf("%d", i),
			"-http-addr", fmt.Sprintf("127.0.0.1:%d", httpPort),
			"-rpc-addr", fmt.Sprintf("127.0.0.1:%d", rpcPort),
			"-peers", stage4PeerList(ports, i),
			"-data-dir", dataDir,
			"-heartbeat-timeout", "20ms",
			"-reelection-timeout", "430ms",
			"-ticker-timeout", "20ms",
			"-apply-batch-interval", "50ms",
			"-snapshot-interval", "3s",
			"-snapshot-threshold", "1024",
			"-max-pool", "16",
			"-trace-log-level", "0",
			"-tcp-connect-timeout", "165ms",
			"-tcp-rpc-timeout", "200ms",
			"-install-snapshot-timeout", "310ms",
			"-stats-output", "false",
		)
		cmd.Stdout = lf
		cmd.Stderr = lf
		if err := cmd.Start(); err != nil {
			cancel()
			t.Fatalf("start node %d: %v", i, err)
		}
		nodes[i] = &stage4Node{
			id:       i,
			httpAddr: fmt.Sprintf("127.0.0.1:%d", httpPort),
			rpcAddr:  fmt.Sprintf("127.0.0.1:%d", rpcPort),
			dataDir:  dataDir,
			logFile:  logFile,
			cmd:      cmd,
			cancel:   cancel,
		}
	}
	return nodes
}

// stopStage4Cluster останавливает все процессы кластера.
func stopStage4Cluster(nodes []*stage4Node) {
	for _, n := range nodes {
		if n != nil && n.cancel != nil {
			n.cancel()
		}
	}
	for _, n := range nodes {
		if n != nil && n.cmd != nil && n.cmd.Process != nil {
			_ = n.cmd.Wait()
		}
	}
}

// waitForStage4Leader ждёт, пока ровно один узел не вернёт /verifyleader/ StatusOK.
func waitForStage4Leader(t *testing.T, nodes []*stage4Node) int {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	client := stage4NoProxyHTTPClient(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		leader := -1
		multiple := false
		for _, n := range nodes {
			ok := stage4NodeIsLeader(t, client, n.httpAddr)
			if ok {
				if leader >= 0 {
					multiple = true
					break
				}
				leader = n.id
			}
		}
		if !multiple && leader >= 0 {
			return leader
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("no single leader within budget")
	return -1
}

// stage4NodeIsLeader возвращает true, если /verifyleader/ отвечает StatusOK.
func stage4NodeIsLeader(t *testing.T, client *http.Client, addr string) bool {
	t.Helper()
	resp, err := client.Get("http://" + addr + "/verifyleader/")
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	var sr api.StatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
		return false
	}
	return sr.RespStatus == api.StatusOK
}

// runDeterministicScenario выполняет фиксированный набор операций на кластере
// и проверяет статусы/значения.
func runDeterministicScenario(t *testing.T, nodes []*stage4Node, leaderID int, label string) {
	t.Helper()
	leader := nodes[leaderID]
	c := &stage4Client{t: t, addr: leader.httpAddr, label: label}

	c.put("parity", "alpha", api.PutResponse{RespStatus: api.StatusOK, KeyFound: false, PrevValue: ""})
	c.get("parity", api.GetResponse{RespStatus: api.StatusOK, KeyFound: true, Value: "alpha"})
	c.put("parity", "beta", api.PutResponse{RespStatus: api.StatusOK, KeyFound: true, PrevValue: "alpha"})
	c.cas("parity", "beta", "gamma", api.CASResponse{RespStatus: api.StatusOK, KeyFound: true, PrevValue: "beta"})
	c.cas("parity", "wrong", "x", api.CASResponse{RespStatus: api.StatusOK, KeyFound: true, PrevValue: "gamma"})
	c.get("parity", api.GetResponse{RespStatus: api.StatusOK, KeyFound: true, Value: "gamma"})
	c.delete("parity", api.DeleteResponse{RespStatus: api.StatusOK, KeyFound: true, PrevValue: "gamma"})
	c.delete("parity", api.DeleteResponse{RespStatus: api.StatusOK, KeyFound: false, PrevValue: ""})
	c.get("parity", api.GetResponse{RespStatus: api.StatusOK, KeyFound: false, Value: ""})
	c.put("empty", "", api.PutResponse{RespStatus: api.StatusOK, KeyFound: false, PrevValue: ""})
	c.get("empty", api.GetResponse{RespStatus: api.StatusOK, KeyFound: true, Value: ""})

	// Restart follower: останавливаем не-лидера и поднимаем с тем же data-dir.
	followerID := (leaderID + 1) % len(nodes)
	follower := nodes[followerID]
	follower.cancel()
	if follower.cmd.Process != nil {
		_ = follower.cmd.Wait()
	}
	newCtx, newCancel := context.WithCancel(context.Background())
	follower.cancel = newCancel
	cmd := exec.CommandContext(newCtx, leader.cmd.Path,
		"-number", fmt.Sprintf("%d", follower.id),
		"-http-addr", follower.httpAddr,
		"-rpc-addr", follower.rpcAddr,
		"-peers", stage4PeerListByAddr([]string{nodes[0].rpcAddr, nodes[1].rpcAddr, nodes[2].rpcAddr}, follower.id),
		"-data-dir", follower.dataDir,
		"-heartbeat-timeout", "20ms",
		"-reelection-timeout", "430ms",
		"-ticker-timeout", "20ms",
		"-apply-batch-interval", "50ms",
		"-snapshot-interval", "3s",
		"-snapshot-threshold", "1024",
		"-max-pool", "16",
		"-trace-log-level", "0",
		"-tcp-connect-timeout", "165ms",
		"-tcp-rpc-timeout", "200ms",
		"-install-snapshot-timeout", "310ms",
		"-stats-output", "false",
	)
	lf, err := os.OpenFile(follower.logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o640)
	if err != nil {
		t.Fatalf("reopen log: %v", err)
	}
	cmd.Stdout = lf
	cmd.Stderr = lf
	if err := cmd.Start(); err != nil {
		t.Fatalf("restart follower: %v", err)
	}
	follower.cmd = cmd

	deadline := time.Now().Add(10 * time.Second)
	client := stage4NoProxyHTTPClient(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if stage4NodeReady(t, client, follower.httpAddr) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !stage4NodeReady(t, client, follower.httpAddr) {
		t.Fatalf("restarted follower did not become ready")
	}

	c.put("after-restart", "ok", api.PutResponse{RespStatus: api.StatusOK, KeyFound: false, PrevValue: ""})

	// Смена лидера: останавливаем текущего лидера, ждём нового.
	leader.cancel()
	if leader.cmd.Process != nil {
		_ = leader.cmd.Wait()
	}
	newLeader := waitForStage4Leader(t, withoutNode(nodes, leaderID))
	c.addr = nodes[newLeader].httpAddr
	c.put("after-stepdown", "yes", api.PutResponse{RespStatus: api.StatusOK, KeyFound: false, PrevValue: ""})

	// Возвращаем старого лидера.
	oldLeader := nodes[leaderID]
	oldCtx, oldCancel := context.WithCancel(context.Background())
	oldLeader.cancel = oldCancel
	oldCmd := exec.CommandContext(oldCtx, leader.cmd.Path,
		"-number", fmt.Sprintf("%d", oldLeader.id),
		"-http-addr", oldLeader.httpAddr,
		"-rpc-addr", oldLeader.rpcAddr,
		"-peers", stage4PeerListByAddr([]string{nodes[0].rpcAddr, nodes[1].rpcAddr, nodes[2].rpcAddr}, oldLeader.id),
		"-data-dir", oldLeader.dataDir,
		"-heartbeat-timeout", "20ms",
		"-reelection-timeout", "430ms",
		"-ticker-timeout", "20ms",
		"-apply-batch-interval", "50ms",
		"-snapshot-interval", "3s",
		"-snapshot-threshold", "1024",
		"-max-pool", "16",
		"-trace-log-level", "0",
		"-tcp-connect-timeout", "165ms",
		"-tcp-rpc-timeout", "200ms",
		"-install-snapshot-timeout", "310ms",
		"-stats-output", "false",
	)
	oldLf, err := os.OpenFile(oldLeader.logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o640)
	if err != nil {
		t.Fatalf("reopen old leader log: %v", err)
	}
	oldCmd.Stdout = oldLf
	oldCmd.Stderr = oldLf
	if err := oldCmd.Start(); err != nil {
		t.Fatalf("restart old leader: %v", err)
	}
	oldLeader.cmd = oldCmd
}

// withoutNode возвращает срез узлов без узла с заданным id.
func withoutNode(nodes []*stage4Node, id int) []*stage4Node {
	out := make([]*stage4Node, 0, len(nodes))
	for _, n := range nodes {
		if n.id != id {
			out = append(out, n)
		}
	}
	return out
}

// stage4NoProxyHTTPClient возвращает клиента без прокси: запросы к
// 127.0.0.1 не должны уходить во внешний посредник окружения.
func stage4NoProxyHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout:   timeout,
		Transport: &http.Transport{Proxy: nil},
	}
}

// stage4NodeReady проверяет, что узел отвечает на /verifyleader/ (любой статус).
func stage4NodeReady(t *testing.T, client *http.Client, addr string) bool {
	t.Helper()
	resp, err := client.Get("http://" + addr + "/verifyleader/")
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode == http.StatusOK
}

// compareFinalState сверяет конечные значения ключей на текущих лидерах
// обоих кластеров. Сильное чтение /get/ обслуживается только лидером,
// поэтому после смены лидера узел 0 может быть ведомым.
func compareFinalState(t *testing.T, baseNodes, prodNodes []*stage4Node) {
	t.Helper()
	baseLeader := waitForStage4Leader(t, baseNodes)
	prodLeader := waitForStage4Leader(t, prodNodes)
	keys := []string{"parity", "empty", "after-restart", "after-stepdown"}
	want := map[string]api.GetResponse{
		"parity":         {RespStatus: api.StatusOK, KeyFound: false, Value: ""},
		"empty":          {RespStatus: api.StatusOK, KeyFound: true, Value: ""},
		"after-restart":  {RespStatus: api.StatusOK, KeyFound: true, Value: "ok"},
		"after-stepdown": {RespStatus: api.StatusOK, KeyFound: true, Value: "yes"},
	}
	for _, key := range keys {
		baseGot := stage4Get(t, baseNodes[baseLeader].httpAddr, key)
		prodGot := stage4Get(t, prodNodes[prodLeader].httpAddr, key)
		if baseGot != want[key] {
			t.Errorf("base final state key %q = %+v, want %+v", key, baseGot, want[key])
		}
		if prodGot != want[key] {
			t.Errorf("prod final state key %q = %+v, want %+v", key, prodGot, want[key])
		}
		if baseGot != prodGot {
			t.Errorf("final state mismatch for key %q: base %+v, prod %+v", key, baseGot, prodGot)
		}
	}
}

// stage4Get выполняет GET и возвращает структурированный ответ.
func stage4Get(t *testing.T, addr, key string) api.GetResponse {
	t.Helper()
	body, _ := json.Marshal(api.GetRequest{Key: key})
	resp := stage4HTTP(t, addr, "POST", "/get/", body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s HTTP %d", key, resp.StatusCode)
	}
	var gr api.GetResponse
	if err := json.NewDecoder(resp.Body).Decode(&gr); err != nil {
		t.Fatalf("decode GET %s: %v", key, err)
	}
	return gr
}

// stage4Client — HTTP-клиент для одного кластера stage4.
type stage4Client struct {
	t     *testing.T
	addr  string
	label string
}

func (c *stage4Client) put(key, value string, want api.PutResponse) {
	c.t.Helper()
	body, _ := json.Marshal(api.PutRequest{Key: key, Value: value})
	resp := stage4HTTP(c.t, c.addr, "POST", "/put/", body)
	c.compareStatus(resp.StatusCode, http.StatusOK)
	var pr api.PutResponse
	c.decode(resp.Body, &pr)
	if pr != want {
		c.t.Errorf("%s PUT %s=%s: got %+v, want %+v", c.label, key, value, pr, want)
	}
}

func (c *stage4Client) get(key string, want api.GetResponse) {
	c.t.Helper()
	body, _ := json.Marshal(api.GetRequest{Key: key})
	resp := stage4HTTP(c.t, c.addr, "POST", "/get/", body)
	c.compareStatus(resp.StatusCode, http.StatusOK)
	var gr api.GetResponse
	c.decode(resp.Body, &gr)
	if gr != want {
		c.t.Errorf("%s GET %s: got %+v, want %+v", c.label, key, gr, want)
	}
}

func (c *stage4Client) cas(key, compare, value string, want api.CASResponse) {
	c.t.Helper()
	body, _ := json.Marshal(api.CASRequest{Key: key, CompareValue: compare, Value: value})
	resp := stage4HTTP(c.t, c.addr, "POST", "/cas/", body)
	c.compareStatus(resp.StatusCode, http.StatusOK)
	var cr api.CASResponse
	c.decode(resp.Body, &cr)
	if cr != want {
		c.t.Errorf("%s CAS %s %s->%s: got %+v, want %+v", c.label, key, compare, value, cr, want)
	}
}

func (c *stage4Client) delete(key string, want api.DeleteResponse) {
	c.t.Helper()
	body, _ := json.Marshal(api.DeleteRequest{Key: key})
	resp := stage4HTTP(c.t, c.addr, "POST", "/delete/", body)
	c.compareStatus(resp.StatusCode, http.StatusOK)
	var dr api.DeleteResponse
	c.decode(resp.Body, &dr)
	if dr != want {
		c.t.Errorf("%s DELETE %s: got %+v, want %+v", c.label, key, dr, want)
	}
}

func (c *stage4Client) compareStatus(got, want int) {
	c.t.Helper()
	if got != want {
		c.t.Fatalf("%s HTTP status = %d, want %d", c.label, got, want)
	}
}

func (c *stage4Client) decode(r io.ReadCloser, v any) {
	c.t.Helper()
	defer func() { _ = r.Close() }()
	if err := json.NewDecoder(r).Decode(v); err != nil {
		c.t.Fatalf("%s decode: %v", c.label, err)
	}
}

// stage4HTTP выполняет HTTP-запрос и возвращает ответ.
// Прокси отключён, чтобы запросы к 127.0.0.1 не уходили во внешний
// посредник, настроенный в окружении CI/разработки.
func stage4HTTP(t *testing.T, addr, method, path string, body []byte) *http.Response {
	t.Helper()
	client := &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{Proxy: nil},
	}
	req, err := http.NewRequest(method, "http://"+addr+path, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request %s: %v", path, err)
	}
	return resp
}

// compile-time check: stage4HTTP не игнорирует ошибки.
var _ = errors.New
