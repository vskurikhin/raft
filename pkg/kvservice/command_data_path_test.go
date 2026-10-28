package kvservice

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"

	"github.com/vskurikhin/raft"
	"github.com/vskurikhin/raft/pkg/api"
	"github.com/vskurikhin/raft/pkg/raft/contract"
	"github.com/vskurikhin/raft/pkg/raft/store"
	"github.com/vskurikhin/raft/pkg/raft/transp"
)

const (
	// _pathTestTimeout — бюджет ожидания в тестах путей сериализации.
	_pathTestTimeout = 10 * time.Second

	// _pathPollInterval — период опроса состояния узлов.
	_pathPollInterval = 10 * time.Millisecond

	// _testReelectionTimeout — база тайм-аута выборов тестового кластера.
	_testReelectionTimeout = 430 * time.Millisecond
)

// newEmptyMapStorage создаёт хранилище с созданным пустым журналом.
func newEmptyMapStorage() *store.MapStorage {
	ms := store.NewMapStorage()
	ms.RewriteLog(nil)
	return ms
}

func newTestTCPTransport(t *testing.T) *transp.TCPTransport {
	t.Helper()
	tr, err := transp.NewTCPTransport("127.0.0.1:0", transp.TCPTimeouts{}, 0)
	if err != nil {
		t.Fatalf("NewTCPTransport: %v", err)
	}
	return tr
}

// loadedCommand возвращает команду записи index из журнала и признак,
// что запись — носитель.
func loadedCommand(t *testing.T, ls contract.LogStorage, index int) (Command, bool) {
	t.Helper()
	entries, err := ls.LoadLog()
	if err != nil {
		t.Fatalf("LoadLog: %v", err)
	}
	for _, e := range entries {
		if e.Index != index {
			continue
		}
		data, ok := e.Data.(*commandData)
		if !ok {
			return Command{}, false
		}
		return data.cmd, true
	}
	return Command{}, false
}

// serveOneAppend принимает один AppendEntries, сохраняет записи ведомого и
// отвечает успехом.
func serveOneAppend(tr *transp.TCPTransport, ms *store.MapStorage, done chan<- struct{}, stop <-chan struct{}) {
	defer func() { done <- struct{}{} }()
	select {
	case rpc := <-tr.Consumer():
		args, ok := rpc.Command.(*contract.AppendEntriesArgs)
		if !ok {
			rpc.RespChan <- contract.RPCResponse{Error: contract.ErrNotImplemented}
			return
		}
		ms.StoreLogEntries(args.PrevLogIndex+1, args.Entries)
		rpc.RespChan <- contract.RPCResponse{Reply: &contract.AppendEntriesReply{Term: args.Term, Success: true}}
	case <-stop:
	}
}

// TestCommandDataTCPAndMapStorageEncodeCount — контролируемая доставка одной
// команды без повторов: лидер сохраняет запись и отправляет её двум
// соседям через TCPTransport, каждый сосед сохраняет её. Ровно 2 вызова
// кодека на TCP и 3 в MapStorage, прочих нет.
func TestCommandDataTCPAndMapStorageEncodeCount(t *testing.T) {
	defer leaktest.CheckTimeout(t, _pathTestTimeout)()
	const key = "tcp-map-controlled"
	rec := startEncodeRecorder(t, key)
	want := Command{Kind: CommandCAS, Key: key, Value: "new", CompareValue: "\x00old", ID: 0}
	entries := []contract.LogEntry{{Index: 1, Term: 1, Type: contract.LogCommand, Data: mustCommandData(t, want)}}

	leader := newTestTCPTransport(t)
	defer leader.Close()
	leaderStorage := newEmptyMapStorage()
	leaderStorage.StoreLogEntries(1, entries)

	stop := make(chan struct{})
	done := make(chan struct{}, 2)
	storages := []*store.MapStorage{newEmptyMapStorage(), newEmptyMapStorage()}
	followers := []*transp.TCPTransport{newTestTCPTransport(t), newTestTCPTransport(t)}
	defer func() {
		close(stop)
		for _, tr := range followers {
			tr.Close()
		}
	}()
	for i, tr := range followers {
		go serveOneAppend(tr, storages[i], done, stop)
		leader.Connect(contract.ServerID(i+1), string(tr.LocalAddr()))
	}

	for i := range storages {
		reply, err := leader.AppendEntries(contract.ServerID(i+1), contract.AppendEntriesArgs{
			Term: 1, Entries: entries, LeaderCommit: 1,
		})
		if err != nil || !reply.Success {
			t.Fatalf("AppendEntries to %d: reply=%+v err=%v", i+1, reply, err)
		}
	}
	for range storages {
		<-done
	}

	if wire, storage, other := rec.snapshot(); wire != 2 || storage != 3 || other != 0 {
		t.Fatalf("encode calls wire=%d storage=%d other=%d, want 2/3/0", wire, storage, other)
	}
	for i, ms := range append([]*store.MapStorage{leaderStorage}, storages...) {
		got, ok := loadedCommand(t, ms, 1)
		if !ok || got != want {
			t.Fatalf("storage %d holds %+v (carrier=%v), want %+v", i, got, ok, want)
		}
	}
}

// TestCommandDataFileStorageRepeatedSuffix: повтор совпадающего суффикса не
// пишет на диск, но кодек вызывается снова; после переоткрытия команда
// восстанавливается точно.
func TestCommandDataFileStorageRepeatedSuffix(t *testing.T) {
	const key = "file-storage-suffix"
	rec := startEncodeRecorder(t, key)
	dir := t.TempDir()
	want := Command{Kind: CommandPut, Key: key, Value: "\xff\x00value", ID: 2}
	entries := []contract.LogEntry{{Index: 1, Term: 1, Type: contract.LogCommand, Data: mustCommandData(t, want)}}

	fs := store.NewFileStorage(dir)
	fs.RewriteLog(nil)
	if first := fs.StoreLogEntries(1, entries); first.Writes == 0 {
		t.Fatalf("first store wrote nothing: %+v", first)
	}
	if second := fs.StoreLogEntries(1, entries); second.Writes != 0 || second.BytesWritten != 0 {
		t.Fatalf("repeated suffix wrote to disk: %+v", second)
	}
	if wire, storage, other := rec.snapshot(); wire != 0 || storage != 2 || other != 0 {
		t.Fatalf("encode calls wire=%d storage=%d other=%d, want 0/2/0", wire, storage, other)
	}

	got, ok := loadedCommand(t, store.NewFileStorage(dir), 1)
	if !ok || got != want {
		t.Fatalf("reopened storage holds %+v (carrier=%v), want %+v", got, ok, want)
	}
}

// TestLegacyLogDataBypassesCommandCodec: прежний Command, nil,
// конфигурационные и произвольные записи сохраняются прежним gob без
// нового кодека и восстанавливаются тем же типом.
func TestLegacyLogDataBypassesCommandCodec(t *testing.T) {
	const key = "legacy-data"
	rec := startEncodeRecorder(t, key)
	legacy := Command{Kind: CommandPut, Key: key, Value: "v", ResultValue: "r", ResultFound: true, ID: 1}
	entries := []contract.LogEntry{
		{Index: 1, Term: 1, Type: contract.LogCommand, Data: legacy},
		{Index: 2, Term: 1, Type: contract.LogNoop, Data: nil},
		{Index: 3, Term: 1, Type: contract.LogConfiguration, Data: []byte("configuration")},
		{Index: 4, Term: 1, Type: contract.LogCommand, Data: "text"},
	}
	ms := newEmptyMapStorage()
	ms.StoreLogEntries(1, entries)
	got, err := ms.LoadLog()
	if err != nil {
		t.Fatalf("LoadLog: %v", err)
	}
	if !reflect.DeepEqual(got, entries) {
		t.Fatalf("legacy entries changed:\n got %#v\nwant %#v", got, entries)
	}
	if wire, storage, other := rec.snapshot(); wire+storage+other != 0 {
		t.Fatalf("legacy data used the command codec %d times", wire+storage+other)
	}
}

// testCluster — три узла KVService на TCPTransport и MapStorage.
type testCluster struct {
	nodes    []*KVService
	storages []*store.MapStorage
}

func startTestCluster(t *testing.T, n int) *testCluster {
	t.Helper()
	c := &testCluster{nodes: make([]*KVService, n), storages: make([]*store.MapStorage, n)}
	ready := make(chan any)
	for i := range n {
		peers := make([]int, 0, n-1)
		for p := range n {
			if p != i {
				peers = append(peers, p)
			}
		}
		c.storages[i] = store.NewMapStorage()
		c.nodes[i] = New(&Config{Config: raft.Config{
			ReelectionTimeout: _testReelectionTimeout,
			ServerID:          i,
			PeerIds:           peers,
			Storage:           c.storages[i],
			Transport:         newTestTCPTransport(t),
		}}, ready)
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

func (c *testCluster) shutdown(t *testing.T) {
	for _, kvs := range c.nodes {
		kvs.DisconnectFromAllRaftPeers()
	}
	for i, kvs := range c.nodes {
		if err := kvs.Shutdown(); err != nil {
			t.Errorf("Shutdown(%d): %v", i, err)
		}
	}
}

// waitFor опрашивает cond до успеха или истечения бюджета.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(_pathTestTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(_pathPollInterval)
	}
}

func (c *testCluster) leader(t *testing.T) *KVService {
	t.Helper()
	var leader *KVService
	waitFor(t, "a leader", func() bool {
		for _, kvs := range c.nodes {
			if kvs.IsLeader() {
				leader = kvs
				return true
			}
		}
		return false
	})
	return leader
}

// postJSON вызывает обработчик лидера и декодирует ответ.
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
		t.Fatalf("%s: HTTP %d: %s", path, w.Code, w.Body.String())
	}
	if err := json.Unmarshal(w.Body.Bytes(), resp); err != nil {
		t.Fatalf("%s: decode %q: %v", path, w.Body.String(), err)
	}
}

// storedOnAll сообщает, что команда с ключом key есть в журнале каждого
// узла и применена к его хранилищу со значением value.
func (c *testCluster) storedOnAll(key, value string) bool {
	for i, ms := range c.storages {
		entries, err := ms.LoadLog()
		if err != nil {
			return false
		}
		found := false
		for _, e := range entries {
			if data, ok := e.Data.(*commandData); ok && data.cmd.Key == key {
				found = true
			}
		}
		if v, ok := c.nodes[i].ds.Get(key); !found || !ok || v != value {
			return false
		}
	}
	return true
}

// TestCommandDataLiveCluster — живой кластер из трёх узлов: команда PUT
// записана на всех узлах, кодек вызван не менее 2 раз на TCP и 3 раз в
// хранилище (повторы AppendEntries добавляют вызовы), прочих вызовов нет.
// Далее ответы HTTP для повторов, CAS, GET и DELETE совпадают с прежней
// семантикой.
func TestCommandDataLiveCluster(t *testing.T) {
	defer leaktest.CheckTimeout(t, _pathTestTimeout)()
	const key = "live-cluster"
	rec := startEncodeRecorder(t, key)
	c := startTestCluster(t, 3)
	defer c.shutdown(t)
	leader := c.leader(t)

	var put api.PutResponse
	postJSON(t, leader.handlePut, "/put/", api.PutRequest{Key: key, Value: "v1"}, &put)
	if put != (api.PutResponse{RespStatus: api.StatusOK}) {
		t.Fatalf("first PUT = %+v", put)
	}
	waitFor(t, "the command on all nodes", func() bool { return c.storedOnAll(key, "v1") })
	wire, storage, other := rec.snapshot()
	if wire < 2 || storage < 3 || wire+storage < 5 || other != 0 {
		t.Fatalf("encode calls wire=%d storage=%d other=%d, want wire>=2 storage>=3 other=0", wire, storage, other)
	}
	t.Logf("encode calls for one PUT: wire=%d storage=%d", wire, storage)

	postJSON(t, leader.handlePut, "/put/", api.PutRequest{Key: key, Value: "v2"}, &put)
	if put != (api.PutResponse{RespStatus: api.StatusOK, KeyFound: true, PrevValue: "v1"}) {
		t.Fatalf("repeated PUT = %+v", put)
	}
	var cas api.CASResponse
	postJSON(t, leader.handleCAS, "/cas/", api.CASRequest{Key: key, CompareValue: "nope", Value: "x"}, &cas)
	if cas != (api.CASResponse{RespStatus: api.StatusOK, KeyFound: true, PrevValue: "v2"}) {
		t.Fatalf("failed CAS = %+v", cas)
	}
	postJSON(t, leader.handleCAS, "/cas/", api.CASRequest{Key: key, CompareValue: "v2", Value: ""}, &cas)
	if cas != (api.CASResponse{RespStatus: api.StatusOK, KeyFound: true, PrevValue: "v2"}) {
		t.Fatalf("CAS = %+v", cas)
	}
	var get api.GetResponse
	postJSON(t, leader.handleGet, "/get/", api.GetRequest{Key: key}, &get)
	if get != (api.GetResponse{RespStatus: api.StatusOK, KeyFound: true, Value: ""}) {
		t.Fatalf("GET = %+v", get)
	}
	var del api.DeleteResponse
	postJSON(t, leader.handleDelete, "/delete/", api.DeleteRequest{Key: key}, &del)
	if del != (api.DeleteResponse{RespStatus: api.StatusOK, KeyFound: true, PrevValue: ""}) {
		t.Fatalf("DELETE = %+v", del)
	}
	var gone api.GetResponse
	postJSON(t, leader.handleGet, "/get/", api.GetRequest{Key: key}, &gone)
	if gone != (api.GetResponse{RespStatus: api.StatusOK}) {
		t.Fatalf("GET after DELETE = %+v", gone)
	}
	if _, _, other := rec.snapshot(); other != 0 {
		t.Fatalf("command encoded outside transport and storage %d times", other)
	}
}
