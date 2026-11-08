package kvservice

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/vskurikhin/raft"
	"github.com/vskurikhin/raft/pkg/api"
	"github.com/vskurikhin/raft/pkg/raft/store"
	"github.com/vskurikhin/raft/pkg/raft/transp"
)

// TestCorruptCommandApplyFutureAndHTTP проверяет, что повреждённые байты
// KVC1, поданные через Apply, фиксируются без ошибки Raft, возвращают
// errCorruptCommand в Response, не меняют DataStore, следующая валидная
// команда применяется, а HTTP-операции продолжают работать.
func TestCorruptCommandApplyFutureAndHTTP(t *testing.T) {
	defer leaktest.CheckTimeout(t, raft.LeaktestBudget)()

	kvs := newSingleNodeKVService(t)
	defer kvs.Shutdown()
	if err := kvs.ServeHTTP("127.0.0.1:0"); err != nil {
		t.Fatalf("ServeHTTP: %v", err)
	}
	waitForHTTPReady(t, kvs.GetHTTPListenAddr())
	waitForKVLeader(t, []*KVService{kvs})

	corrupt := []byte("KVC1\x01\x02\x00\x00broken-payload")
	fut := kvs.rs.Apply(corrupt, 5*time.Second)
	if err := fut.Error(); err != nil {
		t.Fatalf("Raft error for corrupt command: %v", err)
	}
	resp := fut.Response()
	if resp == nil {
		t.Fatal("Response is nil for corrupt command")
	}
	err, ok := resp.(error)
	if !ok {
		t.Fatalf("Response type = %T, want error", resp)
	}
	if !errors.Is(err, errCorruptCommand) {
		t.Fatalf("Response error = %v, want errCorruptCommand", err)
	}

	// DataStore не изменилась повреждённой записью.
	if _, found := kvs.ds.Get("healthy"); found {
		t.Fatal("DataStore changed by corrupt command")
	}

	// Следующая валидная команда применяется.
	valid, err := EncodeCommand(Command{Kind: CommandPut, ID: 1, Key: "healthy", Value: "yes"})
	if err != nil {
		t.Fatalf("EncodeCommand: %v", err)
	}
	fut2 := kvs.rs.Apply(valid, 5*time.Second)
	if err := fut2.Error(); err != nil {
		t.Fatalf("Raft error for valid command: %v", err)
	}
	cmd, ok := fut2.Response().(Command)
	if !ok {
		t.Fatalf("Response type = %T, want Command", fut2.Response())
	}
	if cmd.Kind != CommandPut || cmd.ResultFound {
		t.Fatalf("unexpected valid response: %+v", cmd)
	}

	// HTTP-операция после повреждённой записи работает.
	addr := kvs.GetHTTPListenAddr()
	putResp := httpPut(t, addr, "http", "value")
	wantPut := api.PutResponse{RespStatus: api.StatusOK, KeyFound: false, PrevValue: ""}
	if putResp != wantPut {
		t.Fatalf("HTTP PUT response = %+v, want %+v", putResp, wantPut)
	}
	getResp := httpGet(t, addr, "http")
	wantGet := api.GetResponse{RespStatus: api.StatusOK, KeyFound: true, Value: "value"}
	if getResp != wantGet {
		t.Fatalf("HTTP GET response = %+v, want %+v", getResp, wantGet)
	}
}

// TestApplyBatchBadThenGood проверяет, что ApplyBatch обрабатывает
// повреждённую запись как результат errCorruptCommand, а следующая запись
// в том же batch применяется.
func TestApplyBatchBadThenGood(t *testing.T) {
	kvs := &KVService{ds: NewDataStore()}
	corrupt := []byte("KVC1\x01\x02\x00\x00broken-payload")
	valid, err := EncodeCommand(Command{Kind: CommandPut, ID: 1, Key: "good", Value: "value"})
	if err != nil {
		t.Fatalf("EncodeCommand: %v", err)
	}

	batch := []*raft.LogEntry{
		{Index: 1, Term: 1, Type: raft.LogCommand, Data: corrupt},
		{Index: 2, Term: 1, Type: raft.LogCommand, Data: valid},
	}
	results := kvs.ApplyBatch(batch)
	if len(results) != 2 {
		t.Fatalf("results len = %d, want 2", len(results))
	}
	if results[0] == nil {
		t.Fatal("result[0] is nil")
	}
	err0, ok := results[0].(error)
	if !ok || !errors.Is(err0, errCorruptCommand) {
		t.Fatalf("result[0] = %v, want errCorruptCommand", results[0])
	}
	cmd1, ok := results[1].(Command)
	if !ok || cmd1.Kind != CommandPut {
		t.Fatalf("result[1] = %v, want Command Put", results[1])
	}
	if _, found := kvs.ds.Get("good"); !found {
		t.Fatal("good command was not applied")
	}
}

// TestApplyCopyOwnership проверяет, что после возврата Apply изменение
// клиентского буфера не затрагивает FSM/журнал.
func TestApplyCopyOwnership(t *testing.T) {
	defer leaktest.CheckTimeout(t, raft.LeaktestBudget)()

	kvs := newSingleNodeKVService(t)
	defer kvs.Shutdown()
	waitForKVLeader(t, []*KVService{kvs})

	valid, err := EncodeCommand(Command{Kind: CommandPut, ID: 1, Key: "owned", Value: "original"})
	if err != nil {
		t.Fatalf("EncodeCommand: %v", err)
	}
	fut := kvs.rs.Apply(valid, 5*time.Second)
	// Изменяем буфер сразу после возврата Apply.
	valid[0] = 'X'
	if err := fut.Error(); err != nil {
		t.Fatalf("Raft error: %v", err)
	}
	if value, found := kvs.ds.Get("owned"); !found || value != "original" {
		t.Fatalf("DataStore = %q %v, want original true", value, found)
	}
}

// TestCorruptCommandLogsOnLeaderAndFollower проверяет при AC2, что
// безусловная строка "kvservice: corrupt command index=... term=... reason=..."
// появляется при trace=0 на каждом узле, применившем повреждённую запись:
// лидер и оба ведомых пишут строку с верными индексом, термом записи и
// причиной декодера, без payload. Повреждение — защитный сценарий:
// производственный инвариант «одна сборка на кластер» исключает посторонние
// байты в зафиксированной записи; соседний реалистичный сценарий HTTP→valid
// KVC1 проверяется TestCorruptCommandApplyFutureAndHTTP.
func TestCorruptCommandLogsOnLeaderAndFollower(t *testing.T) {
	defer leaktest.CheckTimeout(t, raft.LeaktestBudget)()

	var logBuf bytes.Buffer
	prev := log.Default().Writer()
	log.Default().SetOutput(&logBuf)
	defer func() { log.Default().SetOutput(prev) }()

	nodes, storages, ready, cleanup := newThreeNodeKVCluster(t)
	defer cleanup()
	close(ready)

	leaderID := waitForKVLeader(t, nodes)

	corrupt := []byte("KVC1\x01\x02\x00\x00broken-payload")
	fut := nodes[leaderID].rs.Apply(corrupt, 5*time.Second)
	if err := fut.Error(); err != nil {
		t.Fatalf("Raft error: %v", err)
	}
	// Валидная команда следом: её применение на всех узлах доказывает, что
	// каждый узел прошёл повреждённую запись и FSM продолжила работу.
	valid, err := EncodeCommand(Command{Kind: CommandPut, ID: 1, Key: "healthy", Value: "yes"})
	if err != nil {
		t.Fatalf("EncodeCommand: %v", err)
	}
	fut2 := nodes[leaderID].rs.Apply(valid, 5*time.Second)
	if err := fut2.Error(); err != nil {
		t.Fatalf("Raft error: %v", err)
	}
	waitFor(t, "the valid command on all nodes", func() bool {
		return storedOnAll(nodes, "healthy", "yes")
	})

	// Индекс и терм повреждённой записи берутся из журнала лидера.
	entries, err := storages[leaderID].LoadLog()
	if err != nil {
		t.Fatalf("LoadLog: %v", err)
	}
	var idx, term int
	for _, e := range entries {
		if e.Type == raft.LogCommand && bytes.Equal(e.Data, corrupt) {
			idx, term = e.Index, e.Term
		}
	}
	if idx == 0 {
		t.Fatal("corrupt entry not found in the leader log")
	}
	_, decodeErr := DecodeCommand(corrupt)
	if decodeErr == nil {
		t.Fatal("DecodeCommand accepted corrupt bytes")
	}
	wantLine := fmt.Sprintf("kvservice: corrupt command index=%d term=%d reason=%v", idx, term, decodeErr)

	cleanup()
	logged := logBuf.String()
	if got := strings.Count(logged, wantLine); got != 3 {
		t.Fatalf("corrupt command log count = %d, want 3 (leader + 2 followers); want line %q; log:\n%s", got, wantLine, logged)
	}
	if strings.Contains(logged, "broken-payload") {
		t.Fatal("log contains payload")
	}
}

// newSingleNodeKVService создаёт один узел KVService без соседей.
func newSingleNodeKVService(t *testing.T) *KVService {
	t.Helper()
	transport, err := transp.NewTCPTransport("127.0.0.1:0", transp.TCPTimeouts{}, 0)
	if err != nil {
		t.Fatalf("transport: %v", err)
	}
	ready := make(chan any)
	close(ready)
	return New(&Config{
		Config: raft.Config{
			ServerID:  1,
			PeerIds:   nil,
			Storage:   store.NewMapStorage(),
			Transport: transport,
		},
	}, ready)
}

// newThreeNodeKVCluster создаёт три KVService, соединённых друг с другом,
// и возвращает узлы с их хранилищами.
func newThreeNodeKVCluster(t *testing.T) ([]*KVService, []*store.MapStorage, chan any, func()) {
	t.Helper()
	n := 3
	nodes := make([]*KVService, n)
	storages := make([]*store.MapStorage, n)
	transports := make([]*transp.TCPTransport, n)
	ready := make(chan any)

	for i := 0; i < n; i++ {
		transport, err := transp.NewTCPTransport("127.0.0.1:0", transp.TCPTimeouts{}, 0)
		if err != nil {
			t.Fatalf("transport %d: %v", i, err)
		}
		transports[i] = transport
		peerIds := make([]int, 0, n-1)
		for p := 0; p < n; p++ {
			if p != i {
				peerIds = append(peerIds, p)
			}
		}
		storages[i] = store.NewMapStorage()
		nodes[i] = New(&Config{
			Config: raft.Config{
				ServerID:  i,
				PeerIds:   peerIds,
				Storage:   storages[i],
				Transport: transport,
			},
		}, ready)
	}
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j {
				rpcAddr, err := net.ResolveTCPAddr("tcp", string(transports[j].LocalAddr()))
				if err != nil {
					t.Fatalf("resolve %d: %v", j, err)
				}
				if err := nodes[i].ConnectToRaftPeer(j, rpcAddr); err != nil {
					t.Fatalf("connect %d->%d: %v", i, j, err)
				}
			}
		}
	}
	cleanup := func() {
		for _, n := range nodes {
			n.Shutdown()
		}
		for _, tr := range transports {
			tr.Close()
		}
	}
	return nodes, storages, ready, cleanup
}

// waitForKVLeader ждёт единственного лидера среди узлов.
func waitForKVLeader(t *testing.T, nodes []*KVService) int {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		leader := -1
		multiple := false
		for _, n := range nodes {
			if n.IsLeader() {
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
	t.Fatalf("no single leader")
	return -1
}

// waitForHTTPReady ждёт, пока /verifyleader/ начнёт отвечать.
// Прокси отключён, чтобы запросы к 127.0.0.1 не уходили во внешний
// посредник, настроенный в окружении CI/разработки.
func waitForHTTPReady(t *testing.T, addr string) {
	t.Helper()
	client := &http.Client{
		Timeout:   200 * time.Millisecond,
		Transport: &http.Transport{Proxy: nil},
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get("http://" + addr + "/verifyleader/")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("HTTP not ready at %s", addr)
}

// httpPut отправляет HTTP PUT и возвращает структурированный ответ.
func httpPut(t *testing.T, addr, key, value string) api.PutResponse {
	t.Helper()
	body, _ := json.Marshal(api.PutRequest{Key: key, Value: value})
	resp := httpRequest(t, addr, "POST", "/put/", body)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT HTTP %d", resp.StatusCode)
	}
	var pr api.PutResponse
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		t.Fatalf("decode PUT: %v", err)
	}
	return pr
}

// httpGet отправляет HTTP GET и возвращает структурированный ответ.
func httpGet(t *testing.T, addr, key string) api.GetResponse {
	t.Helper()
	body, _ := json.Marshal(api.GetRequest{Key: key})
	resp := httpRequest(t, addr, "POST", "/get/", body)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET HTTP %d", resp.StatusCode)
	}
	var gr api.GetResponse
	if err := json.NewDecoder(resp.Body).Decode(&gr); err != nil {
		t.Fatalf("decode GET: %v", err)
	}
	return gr
}

// httpRequest выполняет HTTP-запрос к KVService.
// Прокси отключён, чтобы запросы к 127.0.0.1 не уходили во внешний
// посредник, настроенный в окружении CI/разработки.
func httpRequest(t *testing.T, addr, method, path string, body []byte) *http.Response {
	t.Helper()
	client := &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{Proxy: nil},
	}
	url := fmt.Sprintf("http://%s%s", addr, path)
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
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

// TestStage4BytesOutliveFutureAcrossStepDown — преемник D
// TestCommandDataOutlivesFutureAcrossStepDown (матрица «Смена лидера/отказ»):
// живой кластер из трёх узлов. Команда фиксируется лидером L и ведомым F1,
// пока F2 отрезан; ответ HTTP получен, future завершён. После сборки мусора L
// изолируется; F1 становится лидером и передаёт F2 запись, восстановленную из
// журнала; неподтверждённая запись изолированного L при шаге вниз
// переписывается. Байты записи на диске всех узлов остаются побайтово равны
// единственному кодированию команды.
func TestStage4BytesOutliveFutureAcrossStepDown(t *testing.T) {
	defer leaktest.CheckTimeout(t, raft.LeaktestBudget)()
	const (
		key  = "step-down"
		lost = "step-down-lost"
	)
	nodes, storages, ready, cleanup := newThreeNodeKVCluster(t)
	defer cleanup()
	close(ready)
	l := waitForKVLeader(t, nodes)
	f1, f2 := (l+1)%3, (l+2)%3

	isolateStage4Node(t, nodes, f2)
	var putResp api.PutResponse
	postJSON(t, nodes[l].handlePut, "/put/", api.PutRequest{Key: key, Value: "v1"}, &putResp)
	if putResp != (api.PutResponse{RespStatus: api.StatusOK, KeyFound: false, PrevValue: ""}) {
		t.Fatalf("PUT = %+v", putResp)
	}
	wantBytes, err := EncodeCommand(Command{Kind: CommandPut, Key: key, Value: "v1", ID: l})
	if err != nil {
		t.Fatalf("EncodeCommand: %v", err)
	}
	runtime.GC()
	runtime.GC()

	// Изолированный L ещё лидер в своём сроке: команда попадает в его
	// журнал, но не фиксируется; future завершится отказом при шаге вниз.
	isolateStage4Node(t, nodes, l)
	lostBytes, err := EncodeCommand(Command{Kind: CommandPut, Key: lost, Value: "x"})
	if err != nil {
		t.Fatalf("EncodeCommand: %v", err)
	}
	lostFuture := nodes[l].rs.Apply(lostBytes, 5*time.Second)

	linkStage4Nodes(t, nodes, f1, f2)
	waitFor(t, "F1 to lead and replicate to F2", func() bool {
		return nodes[f1].IsLeader() && logHasKey(storages[f2], key)
	})
	if err := lostFuture.Error(); !errors.Is(err, raft.ErrLeadershipLost) && !errors.Is(err, raft.ErrNotLeader) {
		t.Fatalf("isolated leader Apply error = %v, want leadership lost or not leader", err)
	}
	runtime.GC()

	linkStage4Nodes(t, nodes, l, f1)
	linkStage4Nodes(t, nodes, l, f2)
	waitFor(t, "the old leader to step down and catch up", func() bool {
		return !nodes[l].IsLeader() && storedOnAll(nodes, key, "v1") && !logHasKey(storages[l], lost)
	})
	if _, found := nodes[l].ds.Get(lost); found {
		t.Fatal("uncommitted command of the old leader was applied")
	}

	// Байты подтверждённой команды на диске каждого узла равны единственному
	// кодированию: повторная передача, запись и переписывание журнала его не
	// меняют.
	for i, ms := range storages {
		if !logHasCommandBytes(ms, wantBytes) {
			t.Fatalf("node %d log has no command bytes equal to the single encoding %x", i, wantBytes)
		}
	}
}

// TestStage4RestartAfterSnapshot — преемник D TestCommandDataRestartAfterSnapshot
// (матрица «Снимок и restart»): кластер на FileStorage/FileSnapshot пишет
// команды, снимок со сжатием журнала, перезапуск всех узлов; снимок плюс
// v3-суффикс восстанавливаются, команды в суффиксе строго декодируются;
// следующие записи и смена лидера работают; сырые байты вне KVC1 дают
// errCorruptCommand в future без ошибки Raft.
func TestStage4RestartAfterSnapshot(t *testing.T) {
	defer leaktest.CheckTimeout(t, raft.LeaktestBudget)()
	const (
		before = 200
		after  = 10
	)
	fileNodes := []*fileNode{{dir: t.TempDir()}, {dir: t.TempDir()}, {dir: t.TempDir()}}
	key := func(i int) string { return fmt.Sprintf("k%03d", i) }
	val := func(i int) string { return fmt.Sprintf("v%d", i) }

	c := startFileCluster(t, fileNodes)
	leaderIdx := c.leaderIndex(t)
	for i := range before {
		putOK(t, c.nodes[leaderIdx], key(i), val(i))
	}
	waitFor(t, "a snapshot with log compaction on all nodes", func() bool {
		for _, node := range fileNodes {
			snaps, err := node.snaps.List()
			if err != nil || len(snaps) == 0 {
				return false
			}
			entries, err := node.storage.LoadLog()
			if err != nil || len(entries) == 0 || entries[0].Index == 0 {
				return false
			}
		}
		return true
	})
	for i := before; i < before+after; i++ {
		putOK(t, c.nodes[leaderIdx], key(i), val(i))
	}
	waitFor(t, "the last command on all nodes", func() bool {
		return storedOnAll(c.nodes, key(before+after-1), val(before+after-1))
	})
	c.shutdown(t)

	// Перезапуск: каждый узел восстанавливает снимок и v3-суффикс со
	// своими bytes; команды суффикса строго декодируются.
	c = startFileCluster(t, fileNodes)
	defer func() { c.shutdown(t) }()
	for i, node := range fileNodes {
		snaps, err := node.snaps.List()
		if err != nil || len(snaps) == 0 {
			t.Fatalf("node %d: snapshots %v, err %v", i, snaps, err)
		}
		entries, err := node.storage.LoadLog()
		if err != nil {
			t.Fatalf("node %d: LoadLog: %v", i, err)
		}
		last := entries[len(entries)-1]
		if entries[0].Index == 0 || last.Index <= snaps[0].Index {
			t.Fatalf("node %d: log [%d..%d], snapshot %d: want a compacted log with a suffix after the snapshot",
				i, entries[0].Index, last.Index, snaps[0].Index)
		}
		for _, e := range entries {
			if e.Type != raft.LogCommand {
				continue
			}
			if _, err := DecodeCommand(e.Data); err != nil {
				t.Fatalf("node %d: restored command %d does not decode: %v", i, e.Index, err)
			}
		}
	}
	waitFor(t, "restored KV state on all nodes", func() bool {
		for i := range before + after {
			if !storedOnAll(c.nodes, key(i), val(i)) {
				return false
			}
		}
		return true
	})

	leaderIdx = c.leaderIndex(t)
	var get api.GetResponse
	postJSON(t, c.nodes[leaderIdx].handleGet, "/get/", api.GetRequest{Key: key(0)}, &get)
	if get != (api.GetResponse{RespStatus: api.StatusOK, KeyFound: true, Value: val(0)}) {
		t.Fatalf("GET before snapshot key = %+v", get)
	}
	if put := putOK(t, c.nodes[leaderIdx], key(before), "after-restart"); !put.KeyFound || put.PrevValue != val(before) {
		t.Fatalf("PUT of a suffix key = %+v", put)
	}

	// Смена лидера: прежний лидер изолирован, новый принимает команды.
	c.isolate(t, leaderIdx)
	newIdx := c.otherLeader(t, leaderIdx)
	if put := putOK(t, c.nodes[newIdx], "after-failover", "x"); put.KeyFound {
		t.Fatalf("PUT on the new leader = %+v", put)
	}
	var cas api.CASResponse
	postJSON(t, c.nodes[newIdx].handleCAS, "/cas/", api.CASRequest{Key: key(1), CompareValue: val(1), Value: "cas-value"}, &cas)
	if cas != (api.CASResponse{RespStatus: api.StatusOK, KeyFound: true, PrevValue: val(1)}) {
		t.Fatalf("CAS on the new leader = %+v", cas)
	}
	for j := range c.nodes {
		if j != leaderIdx {
			c.link(t, leaderIdx, j)
		}
	}
	waitFor(t, "replication to the former leader", func() bool {
		return storedOnAll(c.nodes, "after-failover", "x") &&
			storedOnAll(c.nodes, key(1), "cas-value") &&
			storedOnAll(c.nodes, key(before), "after-restart")
	})

	// Не-команда в журнале: Raft фиксирует запись, результат —
	// errCorruptCommand, состояние не меняется.
	current := c.nodes[c.otherLeader(t, -1)]
	future := current.rs.Apply([]byte("not a KV command"), 5*time.Second)
	if err := future.Error(); err != nil {
		t.Fatalf("Apply of raw bytes: %v", err)
	}
	if resErr, ok := future.Response().(error); !ok || !errors.Is(resErr, errCorruptCommand) {
		t.Fatalf("future response %#v, want errCorruptCommand", future.Response())
	}
}

// waitFor опрашивает cond до успеха или истечения бюджета.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// storedOnAll сообщает, что каждый узел применил значение value ключа key.
func storedOnAll(nodes []*KVService, key, value string) bool {
	for _, kvs := range nodes {
		if v, ok := kvs.ds.Get(key); !ok || v != value {
			return false
		}
	}
	return true
}

// logHasKey сообщает, что журнал узла содержит декодируемую команду с
// ключом key.
func logHasKey(ms *store.MapStorage, key string) bool {
	entries, err := ms.LoadLog()
	if err != nil {
		return false
	}
	for _, e := range entries {
		if e.Type != raft.LogCommand {
			continue
		}
		cmd, err := DecodeCommand(e.Data)
		if err == nil && cmd.Key == key {
			return true
		}
	}
	return false
}

// logHasCommandBytes сообщает, что журнал узла содержит запись LogCommand,
// побайтово равную want.
func logHasCommandBytes(ms *store.MapStorage, want []byte) bool {
	entries, err := ms.LoadLog()
	if err != nil {
		return false
	}
	for _, e := range entries {
		if e.Type == raft.LogCommand && bytes.Equal(e.Data, want) {
			return true
		}
	}
	return false
}

// isolateStage4Node разрывает связь узла i со всеми в обе стороны.
func isolateStage4Node(t *testing.T, nodes []*KVService, i int) {
	t.Helper()
	nodes[i].DisconnectFromAllRaftPeers()
	for j, kvs := range nodes {
		if j != i {
			if err := kvs.DisconnectFromRaftPeer(i); err != nil {
				t.Fatalf("DisconnectFromRaftPeer(%d->%d): %v", j, i, err)
			}
		}
	}
}

// linkStage4Nodes соединяет узлы i и j в обе стороны.
func linkStage4Nodes(t *testing.T, nodes []*KVService, i, j int) {
	t.Helper()
	if err := nodes[i].ConnectToRaftPeer(j, nodes[j].GetRaftListenAddr()); err != nil {
		t.Fatalf("ConnectToRaftPeer(%d->%d): %v", i, j, err)
	}
	if err := nodes[j].ConnectToRaftPeer(i, nodes[i].GetRaftListenAddr()); err != nil {
		t.Fatalf("ConnectToRaftPeer(%d->%d): %v", j, i, err)
	}
}

// postJSON вызывает обработчик узла напрямую и декодирует ответ.
func postJSON(t *testing.T, handler http.HandlerFunc, path string, req, resp any) {
	t.Helper()
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	r := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("%s HTTP %d: %s", path, w.Code, w.Body.String())
	}
	if err := json.NewDecoder(w.Result().Body).Decode(resp); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
}

// putOK выполняет PUT через обработчик узла и требует успешного ответа.
func putOK(t *testing.T, kvs *KVService, key, value string) api.PutResponse {
	t.Helper()
	var resp api.PutResponse
	postJSON(t, kvs.handlePut, "/put/", api.PutRequest{Key: key, Value: value}, &resp)
	if resp.RespStatus != api.StatusOK {
		t.Fatalf("PUT %s: %+v", key, resp)
	}
	return resp
}

// fileNode — узел кластера на FileStorage и FileSnapshot в каталоге dir.
type fileNode struct {
	dir     string
	storage *store.FileStorage
	snaps   *store.FileSnapshot
}

// fileCluster — кластер KVService на файловых хранилищах.
type fileCluster struct {
	nodes      []*KVService
	fileNodes  []*fileNode
	transports []*transp.TCPTransport
}

// startFileCluster запускает (или перезапускает) узлы на каталогах nodes.
func startFileCluster(t *testing.T, nodes []*fileNode) *fileCluster {
	t.Helper()
	n := len(nodes)
	c := &fileCluster{nodes: make([]*KVService, n), fileNodes: nodes, transports: make([]*transp.TCPTransport, n)}
	ready := make(chan any)
	for i, node := range nodes {
		peerIds := make([]int, 0, n-1)
		for p := 0; p < n; p++ {
			if p != i {
				peerIds = append(peerIds, p)
			}
		}
		node.storage = store.NewFileStorage(node.dir)
		snaps, err := store.NewFileSnapshot(node.dir, 2)
		if err != nil {
			t.Fatalf("NewFileSnapshot: %v", err)
		}
		node.snaps = snaps
		transport, err := transp.NewTCPTransport("127.0.0.1:0", transp.TCPTimeouts{}, 0)
		if err != nil {
			t.Fatalf("transport %d: %v", i, err)
		}
		c.transports[i] = transport
		c.nodes[i] = New(&Config{
			Config: raft.Config{
				ServerID:          i,
				PeerIds:           peerIds,
				Storage:           node.storage,
				SnapshotStore:     snaps,
				SnapshotThreshold: 64,
				SnapshotInterval:  50 * time.Millisecond,
				Transport:         transport,
			},
		}, ready)
	}
	for i := range n {
		for j := range n {
			if i != j {
				if err := c.nodes[i].ConnectToRaftPeer(j, c.nodes[j].GetRaftListenAddr()); err != nil {
					t.Fatalf("ConnectToRaftPeer(%d->%d): %v", i, j, err)
				}
			}
		}
	}
	close(ready)
	return c
}

// shutdown останавливает узлы кластера и закрывает транспорты.
func (c *fileCluster) shutdown(t *testing.T) {
	for _, kvs := range c.nodes {
		kvs.DisconnectFromAllRaftPeers()
	}
	for i, kvs := range c.nodes {
		if err := kvs.Shutdown(); err != nil {
			t.Errorf("Shutdown(%d): %v", i, err)
		}
	}
	for _, tr := range c.transports {
		tr.Close()
	}
}

// isolate разрывает связь узла i со всеми в обе стороны.
func (c *fileCluster) isolate(t *testing.T, i int) {
	t.Helper()
	isolateStage4Node(t, c.nodes, i)
}

// link соединяет узлы i и j в обе стороны.
func (c *fileCluster) link(t *testing.T, i, j int) {
	t.Helper()
	linkStage4Nodes(t, c.nodes, i, j)
}

// leaderIndex ждёт единственного лидера и возвращает его индекс.
func (c *fileCluster) leaderIndex(t *testing.T) int {
	t.Helper()
	return waitForKVLeader(t, c.nodes)
}

// otherLeader ждёт лидера среди узлов, кроме old (-1 — любой лидер).
func (c *fileCluster) otherLeader(t *testing.T, old int) int {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for i, kvs := range c.nodes {
			if i != old && kvs.IsLeader() {
				return i
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no leader besides %d", old)
	return -1
}
