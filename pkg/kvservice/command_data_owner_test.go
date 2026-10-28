package kvservice

import (
	"bytes"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"errors"
	"runtime"
	"sync"
	"testing"

	"github.com/fortytw2/leaktest"

	"github.com/vskurikhin/raft"
	"github.com/vskurikhin/raft/pkg/api"
	"github.com/vskurikhin/raft/pkg/raft/contract"
	"github.com/vskurikhin/raft/pkg/raft/store"
	"github.com/vskurikhin/raft/pkg/raft/transp"
)

// _variantBGolden — результат GobEncode носителя варианта B (коммит 831c651)
// для тех же команд; снят до изменений C, журнал
// .ai/developer/TASK-1012/variant-c/b-golden.log.
var _variantBGolden = []struct {
	cmd Command
	hex string
}{
	{
		cmd: Command{Kind: CommandGet, Key: "k", ID: 1},
		hex: "4b5643310101000000000000000000010000000000000001000000000000000000000000000000006b",
	},
	{
		cmd: Command{Kind: CommandPut, Key: "key", Value: "value", ID: 7, ResultValue: "r", ResultFound: true},
		hex: "4b5643310102000000000000000000070000000000000003000000000000000500000000000000006b657976616c7565",
	},
	{
		cmd: Command{Kind: CommandCAS, Key: "\x00\xff", Value: "\xc3\x28", CompareValue: "old", ID: -3},
		hex: "4b56433101030000fffffffffffffffd00000000000000020000000000000002000000000000000300ffc3286f6c64",
	},
	{
		cmd: Command{Kind: CommandDelete, Key: "", ID: 0},
		hex: "4b564331010400000000000000000000000000000000000000000000000000000000000000000000",
	},
}

// TestCommandDataMatchesVariantB: байты GobEncode C совпадают с эталоном B.
func TestCommandDataMatchesVariantB(t *testing.T) {
	for i, tc := range _variantBGolden {
		want, err := hex.DecodeString(tc.hex)
		if err != nil {
			t.Fatalf("golden %d: %v", i, err)
		}
		got, err := mustCommandData(t, tc.cmd).GobEncode()
		if err != nil {
			t.Fatalf("golden %d: GobEncode: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("golden %d: GobEncode = %x, want variant B %x", i, got, want)
		}
	}
}

// encodedOf возвращает копию готового представления носителя.
func encodedOf(t *testing.T, data *commandData) []byte {
	t.Helper()
	b, err := data.GobEncode()
	if err != nil {
		t.Fatalf("GobEncode: %v", err)
	}
	return bytes.Clone(b)
}

// TestCommandDataGobDecodeCopiesInput: носитель после GobDecode не ссылается
// на вход; изменение входа не меняет команду и представление.
func TestCommandDataGobDecodeCopiesInput(t *testing.T) {
	want := Command{Kind: CommandCAS, Key: "key", Value: "new", CompareValue: "old", ID: 4}
	input, err := EncodeCommand(want)
	if err != nil {
		t.Fatalf("EncodeCommand: %v", err)
	}
	saved := bytes.Clone(input)

	var data commandData
	if err := data.GobDecode(input); err != nil {
		t.Fatalf("GobDecode: %v", err)
	}
	if &data.encoded[0] == &input[0] {
		t.Fatal("decoded carrier aliases the gob input buffer")
	}
	for i := range input {
		input[i] ^= 0xff
	}
	if data.cmd != want {
		t.Fatalf("command changed with the input: %+v", data.cmd)
	}
	if got := encodedOf(t, &data); !bytes.Equal(got, saved) {
		t.Fatalf("representation changed with the input: %x, want %x", got, saved)
	}
}

// TestCommandDataReceiverOwnsMemory: после передачи через gob получатель
// имеет собственную память, отправитель и получатель независимы.
func TestCommandDataReceiverOwnsMemory(t *testing.T) {
	src := mustCommandData(t, Command{Kind: CommandPut, Key: "k", Value: "v", ID: 1})
	entry := gobRoundTripEntry(t, raft.LogEntry{Index: 1, Term: 1, Data: src})
	dst, ok := entry.Data.(*commandData)
	if !ok {
		t.Fatalf("decoded Data type %T, want *commandData", entry.Data)
	}
	if dst == src || &dst.encoded[0] == &src.encoded[0] {
		t.Fatal("receiver shares the sender's representation")
	}
	if !bytes.Equal(dst.encoded, src.encoded) || dst.cmd != src.cmd {
		t.Fatalf("receiver %+v %x differs from sender %+v %x", dst.cmd, dst.encoded, src.cmd, src.encoded)
	}
}

// TestCommandDataIgnoresClientBuffers: носитель не ссылается ни на буфер
// HTTP-запроса, ни на исходную команду; их изменение после конструктора
// не меняет запись.
func TestCommandDataIgnoresClientBuffers(t *testing.T) {
	body := []byte(`{"key":"client-key","value":"client-value"}`)
	var req api.PutRequest
	if err := json.Unmarshal(body, &req); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	cmd := Command{Kind: CommandPut, Key: req.Key, Value: req.Value, ID: 3}
	want := cmd
	data, err := newCommandData(&cmd)
	if err != nil {
		t.Fatalf("newCommandData: %v", err)
	}
	saved := encodedOf(t, data)

	for i := range body {
		body[i] = 'x'
	}
	cmd.Key, cmd.Value, cmd.Kind = "changed", "changed", CommandDelete

	if data.cmd != want {
		t.Fatalf("carrier command changed: %+v, want %+v", data.cmd, want)
	}
	if got := encodedOf(t, data); !bytes.Equal(got, saved) {
		t.Fatalf("carrier representation changed: %x, want %x", got, saved)
	}
}

// TestFSMDoesNotMutateCarrier: FSM заполняет результат в копии, носитель
// остаётся прежним.
func TestFSMDoesNotMutateCarrier(t *testing.T) {
	kvs := &KVService{ds: NewDataStore()}
	kvs.ds.Put("k", "old")
	want := Command{Kind: CommandPut, Key: "k", Value: "new", ID: 1}
	data := mustCommandData(t, want)
	saved := encodedOf(t, data)

	got, ok := kvs.Apply(&raft.LogEntry{Data: data}).(Command)
	if !ok || !got.ResultFound || got.ResultValue != "old" {
		t.Fatalf("Apply = %+v", got)
	}
	got.Key = "mutated"
	if data.cmd != want {
		t.Fatalf("FSM changed the carrier: %+v, want %+v", data.cmd, want)
	}
	if enc := encodedOf(t, data); !bytes.Equal(enc, saved) {
		t.Fatalf("FSM changed the representation: %x, want %x", enc, saved)
	}
}

// serveAppends принимает AppendEntries до закрытия stop и сохраняет записи.
func serveAppends(tr *transp.TCPTransport, ms *store.MapStorage, stop <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case rpc := <-tr.Consumer():
			args, ok := rpc.Command.(*contract.AppendEntriesArgs)
			if !ok {
				rpc.RespChan <- contract.RPCResponse{Error: contract.ErrNotImplemented}
				continue
			}
			ms.StoreLogEntries(args.PrevLogIndex+1, args.Entries)
			rpc.RespChan <- contract.RPCResponse{Reply: &contract.AppendEntriesReply{Term: args.Term, Success: true}}
		case <-stop:
			return
		}
	}
}

// TestCommandDataConcurrentSendAndPersist: один носитель одновременно
// кодируют gob, отправляют двум соседям с повторами и сохраняют в
// MapStorage и FileStorage. Под -race гонок нет, кодек вызван один раз,
// представление не меняется.
func TestCommandDataConcurrentSendAndPersist(t *testing.T) {
	defer leaktest.CheckTimeout(t, _pathTestTimeout)()
	const (
		key     = "concurrent-carrier"
		workers = 4
		repeats = 20
	)
	rec := startEncodeRecorder(t, key)
	want := Command{Kind: CommandPut, Key: key, Value: "value", ID: 9}
	data := mustCommandData(t, want)
	saved := encodedOf(t, data)
	entries := []contract.LogEntry{{Index: 1, Term: 1, Type: contract.LogCommand, Data: data}}

	leader := newTestTCPTransport(t)
	defer leader.Close()
	followers := []*transp.TCPTransport{newTestTCPTransport(t), newTestTCPTransport(t)}
	followerStorages := []*store.MapStorage{newEmptyMapStorage(), newEmptyMapStorage()}
	stop := make(chan struct{})
	var servers sync.WaitGroup
	for i, tr := range followers {
		servers.Add(1)
		go serveAppends(tr, followerStorages[i], stop, &servers)
		leader.Connect(contract.ServerID(i+1), string(tr.LocalAddr()))
	}
	defer func() {
		close(stop)
		servers.Wait()
		for _, tr := range followers {
			tr.Close()
		}
	}()

	mapStorage := newEmptyMapStorage()
	dir := t.TempDir()
	fileStorage := store.NewFileStorage(dir)
	fileStorage.RewriteLog(nil)

	var wg sync.WaitGroup
	errs := make(chan error, 4*workers*repeats)
	for range workers {
		wg.Add(4)
		go func() {
			defer wg.Done()
			for range repeats {
				if err := gob.NewEncoder(&bytes.Buffer{}).Encode(raft.LogEntry{Data: data}); err != nil {
					errs <- err
				}
			}
		}()
		go func() {
			defer wg.Done()
			for range repeats {
				mapStorage.StoreLogEntries(1, entries)
			}
		}()
		go func() {
			defer wg.Done()
			for range repeats {
				fileStorage.StoreLogEntries(1, entries)
			}
		}()
		go func() {
			defer wg.Done()
			for range repeats {
				for i := range followers {
					reply, err := leader.AppendEntries(contract.ServerID(i+1), contract.AppendEntriesArgs{
						Term: 1, Entries: entries, LeaderCommit: 1,
					})
					if err == nil && !reply.Success {
						err = errors.New("append rejected")
					}
					if err != nil {
						errs <- err
					}
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent use failed: %v", err)
	}

	if wire, storage, other := rec.snapshot(); wire != 0 || storage != 0 || other != 1 {
		t.Fatalf("encode calls wire=%d storage=%d other=%d, want 0/0/1", wire, storage, other)
	}
	if got := encodedOf(t, data); !bytes.Equal(got, saved) || data.cmd != want {
		t.Fatalf("carrier changed: %+v %x, want %+v %x", data.cmd, got, want, saved)
	}
	all := []contract.LogStorage{mapStorage, fileStorage, followerStorages[0], followerStorages[1]}
	for i, ls := range all {
		if got, ok := loadedCommand(t, ls, 1); !ok || got != want {
			t.Fatalf("storage %d holds %+v (carrier=%v), want %+v", i, got, ok, want)
		}
	}
	if got, ok := loadedCommand(t, store.NewFileStorage(dir), 1); !ok || got != want {
		t.Fatalf("reopened file storage holds %+v (carrier=%v), want %+v", got, ok, want)
	}
}

// isolate разрывает связь узла i со всеми в обе стороны.
func (c *testCluster) isolate(t *testing.T, i int) {
	t.Helper()
	c.nodes[i].DisconnectFromAllRaftPeers()
	for j, kvs := range c.nodes {
		if j != i {
			if err := kvs.DisconnectFromRaftPeer(i); err != nil {
				t.Fatalf("DisconnectFromRaftPeer(%d->%d): %v", j, i, err)
			}
		}
	}
}

// link соединяет узлы i и j в обе стороны.
func (c *testCluster) link(t *testing.T, i, j int) {
	t.Helper()
	if err := c.nodes[i].ConnectToRaftPeer(j, c.nodes[j].GetRaftListenAddr()); err != nil {
		t.Fatalf("ConnectToRaftPeer(%d->%d): %v", i, j, err)
	}
	if err := c.nodes[j].ConnectToRaftPeer(i, c.nodes[i].GetRaftListenAddr()); err != nil {
		t.Fatalf("ConnectToRaftPeer(%d->%d): %v", j, i, err)
	}
}

func (c *testCluster) leaderIndex(t *testing.T) int {
	t.Helper()
	leader := c.leader(t)
	for i, kvs := range c.nodes {
		if kvs == leader {
			return i
		}
	}
	t.Fatal("leader is not a cluster node")
	return -1
}

// logHasKey сообщает, что журнал узла содержит команду с ключом key.
func logHasKey(ms *store.MapStorage, key string) bool {
	entries, err := ms.LoadLog()
	if err != nil {
		return false
	}
	for _, e := range entries {
		if cmd, ok := logEntryCommand(e.Data); ok && cmd.Key == key {
			return true
		}
	}
	return false
}

// TestCommandDataOutlivesFutureAcrossStepDown — живой кластер из трёх узлов.
// Команда фиксируется лидером L и ведомым F1, пока F2 отрезан; ответ HTTP
// получен, то есть future завершён. После сборки мусора L изолируется,
// F1 становится лидером и передаёт F2 запись, восстановленную из gob;
// неподтверждённая запись изолированного L при шаге вниз переписывается.
// Кодек вызван один раз — при приёме команды; повторная передача, запись
// и переписывание журнала его не вызывают.
func TestCommandDataOutlivesFutureAcrossStepDown(t *testing.T) {
	defer leaktest.CheckTimeout(t, 3*_pathTestTimeout)()
	const (
		key  = "step-down"
		lost = "step-down-lost"
	)
	rec := startEncodeRecorder(t, key)
	c := startTestCluster(t, 3)
	defer c.shutdown(t)
	l := c.leaderIndex(t)
	f1, f2 := (l+1)%3, (l+2)%3

	c.isolate(t, f2)
	var put api.PutResponse
	postJSON(t, c.nodes[l].handlePut, "/put/", api.PutRequest{Key: key, Value: "v1"}, &put)
	if put != (api.PutResponse{RespStatus: api.StatusOK}) {
		t.Fatalf("PUT = %+v", put)
	}
	runtime.GC()
	runtime.GC()

	// Изолированный L ещё лидер в своём сроке: команда попадает в его
	// журнал, но не фиксируется; future завершится отказом при шаге вниз.
	c.isolate(t, l)
	lostFuture := c.nodes[l].rs.Apply(mustCommandData(t, Command{Kind: CommandPut, Key: lost, Value: "x"}), _requestTimeout)

	c.link(t, f1, f2)
	waitFor(t, "F1 to lead and replicate to F2", func() bool {
		return c.nodes[f1].IsLeader() && logHasKey(c.storages[f2], key)
	})
	if err := lostFuture.Error(); !errors.Is(err, raft.ErrLeadershipLost) && !errors.Is(err, raft.ErrNotLeader) {
		t.Fatalf("isolated leader Apply error = %v, want leadership lost or not leader", err)
	}
	runtime.GC()

	c.link(t, l, f1)
	c.link(t, l, f2)
	waitFor(t, "the old leader to step down and catch up", func() bool {
		return !c.nodes[l].IsLeader() && c.storedOnAll(key, "v1") && !logHasKey(c.storages[l], lost)
	})
	if _, found := c.nodes[l].ds.Get(lost); found {
		t.Fatal("uncommitted command of the old leader was applied")
	}
	if wire, storage, other := rec.snapshot(); wire != 0 || storage != 0 || other != 1 {
		t.Fatalf("encode calls wire=%d storage=%d other=%d, want 0/0/1", wire, storage, other)
	}
}
