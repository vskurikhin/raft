package kvservice

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"

	"github.com/vskurikhin/raft"
	"github.com/vskurikhin/raft/pkg/api"
	"github.com/vskurikhin/raft/pkg/raft/contract"
	"github.com/vskurikhin/raft/pkg/raft/store"
)

// _logDataCommandPrefix — версия и тег Data журнала v3 для байтов команды.
var _logDataCommandPrefix = []byte{1, 3}

// TestRaftCommandBytesMatchesVariantC: хранилище получает ровно готовое
// представление C — те же байты, что GobEncode и EncodeCommand, без
// кодека, копии и выделений памяти.
func TestRaftCommandBytesMatchesVariantC(t *testing.T) {
	const key = "raft-command-bytes"
	rec := startEncodeRecorder(t, key)
	cmds := append(codecCommands(), Command{Kind: CommandPut, Key: key, Value: "v"})
	for i, cmd := range cmds {
		data := mustCommandData(t, cmd)
		got := data.RaftCommandBytes()
		wire, err := data.GobEncode()
		if err != nil {
			t.Fatalf("command %d: GobEncode: %v", i, err)
		}
		want, err := EncodeCommand(data.cmd)
		if err != nil {
			t.Fatalf("command %d: EncodeCommand: %v", i, err)
		}
		if !bytes.Equal(got, wire) || !bytes.Equal(got, want) {
			t.Fatalf("command %d: RaftCommandBytes %x, GobEncode %x, EncodeCommand %x", i, got, wire, want)
		}
		if &got[0] != &data.encoded[0] || cap(got) != len(got) {
			t.Fatalf("command %d: RaftCommandBytes is not the capped internal representation", i)
		}
		if allocs := testing.AllocsPerRun(100, func() { _ = data.RaftCommandBytes() }); allocs != 0 {
			t.Fatalf("command %d: RaftCommandBytes allocates %.0f times", i, allocs)
		}
	}
	if wire, storage, other := rec.snapshot(); wire != 0 || storage != 0 || other != 1 {
		t.Fatalf("encode calls wire=%d storage=%d other=%d, want 0/0/1", wire, storage, other)
	}
}

// TestFileStorageWritesCommandBytesWithoutGob: добавление, замена суффикса и
// полная перезапись журнала пишут KV-команду тегом 3 — байтами C после
// версии и тега; потока gob носителя в файле нет, кодек не вызывается.
func TestFileStorageWritesCommandBytesWithoutGob(t *testing.T) {
	const key = "file-v3-no-gob"
	rec := startEncodeRecorder(t, key)
	cmds := []Command{
		{Kind: CommandPut, Key: key, Value: "v1", ID: 1},
		{Kind: CommandCAS, Key: key, CompareValue: "v1", Value: "v2", ID: 1},
		{Kind: CommandDelete, Key: key, ID: 1},
		{Kind: CommandGet, Key: key, ID: 1},
	}
	carriers := make([]*commandData, len(cmds))
	entries := make([]contract.LogEntry, len(cmds))
	for i, cmd := range cmds {
		carriers[i] = mustCommandData(t, cmd)
		entries[i] = contract.LogEntry{Index: i + 1, Term: 1, Type: contract.LogCommand, Data: carriers[i]}
	}

	dir := t.TempDir()
	fs := store.NewFileStorage(dir)
	fs.RewriteLog(nil)
	fs.StoreLogEntries(1, entries[:2])
	fs.StoreLogEntries(3, entries[2:3])
	// Конфликт на индексе 2 с новым термом — замена суффикса файлом.
	rewritten := []contract.LogEntry{
		{Index: 2, Term: 2, Type: contract.LogCommand, Data: carriers[1]},
		{Index: 3, Term: 2, Type: contract.LogCommand, Data: carriers[2]},
		{Index: 4, Term: 2, Type: contract.LogCommand, Data: carriers[3]},
	}
	if res := fs.StoreLogEntries(2, rewritten); res.Writes != 1 {
		t.Fatalf("suffix replacement: %+v, want one write", res)
	}
	checkFile := func(stage string) {
		raw, err := os.ReadFile(filepath.Join(dir, "log.dat"))
		if err != nil {
			t.Fatalf("%s: read log: %v", stage, err)
		}
		for i, c := range carriers {
			frame := append(bytes.Clone(_logDataCommandPrefix), c.RaftCommandBytes()...)
			if !bytes.Contains(raw, frame) {
				t.Fatalf("%s: command %d is not written as tag 3 with C bytes", stage, i)
			}
		}
		for _, marker := range []string{_commandDataName, "logDataEnvelope"} {
			if bytes.Contains(raw, []byte(marker)) {
				t.Fatalf("%s: log file contains gob stream marker %q", stage, marker)
			}
		}
	}
	checkFile("append and suffix replacement")

	fs.RewriteLog(append(entries[:1:1], rewritten...))
	checkFile("rewrite")
	if wire, storage, other := rec.snapshot(); wire != 0 || storage != 0 || other != len(cmds) {
		t.Fatalf("encode calls wire=%d storage=%d other=%d, want 0/0/%d", wire, storage, other, len(cmds))
	}
	for i, cmd := range cmds {
		got, ok := loadedCommand(t, store.NewFileStorage(dir), i+1)
		if !ok || got != cmd {
			t.Fatalf("restored command %d = %+v (bytes=%v), want %+v", i+1, got, ok, cmd)
		}
	}
}

// TestRestartedCommandSuffixIsNoop (AC4, SA-004): ведомый получил команды
// живыми носителями, перезапущенный лидер повторяет тот же суффикс
// восстановленными []byte — записи нет, файл ведомого не изменён.
func TestRestartedCommandSuffixIsNoop(t *testing.T) {
	cmds := []Command{
		{Kind: CommandPut, Key: "suffix", Value: "\x00v", ID: 3},
		{Kind: CommandCAS, Key: "suffix", CompareValue: "\x00v", Value: "w", ID: 3},
		{Kind: CommandGet, Key: "suffix", ID: 3},
	}
	live := []contract.LogEntry{{Index: 0, Term: 0, Type: contract.LogNoop}}
	for i, cmd := range cmds {
		live = append(live, contract.LogEntry{Index: i + 1, Term: 1, Type: contract.LogCommand, Data: mustCommandData(t, cmd)})
	}

	leaderDir := t.TempDir()
	store.NewFileStorage(leaderDir).RewriteLog(live)
	restored, err := store.NewFileStorage(leaderDir).LoadLog()
	if err != nil {
		t.Fatalf("leader LoadLog: %v", err)
	}
	for i := 1; i < len(restored); i++ {
		raw, ok := restored[i].Data.([]byte)
		if !ok || !bytes.Equal(raw, live[i].Data.(*commandData).RaftCommandBytes()) {
			t.Fatalf("restored entry %d = %#v, want the carrier bytes", i, restored[i].Data)
		}
	}

	followerDir := t.TempDir()
	follower := store.NewFileStorage(followerDir)
	follower.RewriteLog(live[:1])
	follower.StoreLogEntries(1, live[1:])
	path := filepath.Join(followerDir, "log.dat")
	before := fileSum(t, path)
	for from := 1; from < len(restored); from++ {
		res := follower.StoreLogEntries(from, restored[from:])
		if res.Writes != 0 || res.BytesWritten != 0 {
			t.Fatalf("repeated suffix from %d: %+v, want Writes=0 BytesWritten=0", from, res)
		}
	}
	if fileSum(t, path) != before {
		t.Fatal("repeated suffix changed the follower log file")
	}

	changed := bytes.Clone(restored[3].Data.([]byte))
	changed[len(changed)-1] ^= 0x01
	res := follower.StoreLogEntries(3, []contract.LogEntry{{Index: 3, Term: 1, Type: contract.LogCommand, Data: changed}})
	if res.Writes != 1 || fileSum(t, path) == before {
		t.Fatalf("changed command byte: %+v, want a rewrite", res)
	}
}

func fileSum(t *testing.T, path string) [sha256.Size]byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return sha256.Sum256(raw)
}

// TestFSMDecodesRestoredCommandBytes: восстановленные байты команды дают те
// же результаты FSM, что живой носитель, по одиночке и пакетом.
func TestFSMDecodesRestoredCommandBytes(t *testing.T) {
	live := &KVService{ds: NewDataStore()}
	restored := &KVService{ds: NewDataStore()}
	var batch []*raft.LogEntry
	var wantBatch []any
	batched := &KVService{ds: NewDataStore()}
	for i, cmd := range fsmSequence() {
		cmd.ID = 4
		data := mustCommandData(t, cmd)
		want := live.Apply(&raft.LogEntry{Index: i, Type: raft.LogCommand, Data: data})
		got := restored.Apply(&raft.LogEntry{Index: i, Type: raft.LogCommand, Data: bytes.Clone(data.RaftCommandBytes())})
		if _, ok := got.(Command); !ok || !reflect.DeepEqual(got, want) {
			t.Fatalf("step %d: restored bytes give %#v, carrier gives %#v", i, got, want)
		}
		batch = append(batch, &raft.LogEntry{Index: i, Type: raft.LogCommand, Data: bytes.Clone(data.RaftCommandBytes())})
		wantBatch = append(wantBatch, want)
	}
	if got := batched.ApplyBatch(batch); !reflect.DeepEqual(got, wantBatch) {
		t.Fatalf("ApplyBatch = %#v, want %#v", got, wantBatch)
	}
}

// TestFSMRejectsCorruptCommandBytes: повреждённые байты команды, в том числе
// с неверными длинами, дают ошибку errCorruptCommand, а не Command; DataStore
// не меняется, соседние записи пакета применяются.
func TestFSMRejectsCorruptCommandBytes(t *testing.T) {
	valid, err := EncodeCommand(Command{Kind: CommandPut, Key: "k", Value: "v", ID: 1})
	if err != nil {
		t.Fatalf("EncodeCommand: %v", err)
	}
	mutate := func(f func(b []byte) []byte) []byte { return f(bytes.Clone(valid)) }
	const keyLength = _commandOffsetLengths
	cases := map[string][]byte{
		"nil bytes":          nil,
		"empty bytes":        {},
		"short header":       valid[:39],
		"bad magic":          mutate(func(b []byte) []byte { b[0] = 'X'; return b }),
		"bad version":        mutate(func(b []byte) []byte { b[4] = 9; return b }),
		"unknown kind":       mutate(func(b []byte) []byte { b[5] = 0; return b }),
		"key length too big": mutate(func(b []byte) []byte { binary.BigEndian.PutUint64(b[keyLength:], 1<<40); return b }),
		"max key length":     mutate(func(b []byte) []byte { binary.BigEndian.PutUint64(b[keyLength:], ^uint64(0)); return b }),
		"truncated value":    valid[:len(valid)-1],
		"extra tail byte":    append(bytes.Clone(valid), 0),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			kvs := &KVService{ds: NewDataStore()}
			good := mustCommandData(t, Command{Kind: CommandPut, Key: "other", Value: "x"})
			results := kvs.ApplyBatch([]*raft.LogEntry{
				{Index: 7, Type: raft.LogCommand, Data: data},
				{Index: 8, Type: raft.LogCommand, Data: good},
			})
			resErr, ok := results[0].(error)
			if !ok || !errors.Is(resErr, errCorruptCommand) {
				t.Fatalf("result %#v, want errCorruptCommand", results[0])
			}
			if _, isCmd := results[0].(Command); isCmd {
				t.Fatal("corrupt command bytes returned a Command")
			}
			if _, found := kvs.ds.Get("k"); found {
				t.Fatal("corrupt command changed the DataStore")
			}
			if _, ok := results[1].(Command); !ok {
				t.Fatalf("next entry result %#v, want Command", results[1])
			}
			if v, found := kvs.ds.Get("other"); !found || v != "x" {
				t.Fatalf("next entry not applied: %q/%v", v, found)
			}
		})
	}
}

// TestFSMCommandBytesOnlyForCommandEntries: байты вне LogCommand (например,
// конфигурационные) не проходят KV DecodeCommand даже при валидном формате.
func TestFSMCommandBytesOnlyForCommandEntries(t *testing.T) {
	valid, err := EncodeCommand(Command{Kind: CommandPut, Key: "cfg", Value: "v"})
	if err != nil {
		t.Fatalf("EncodeCommand: %v", err)
	}
	kvs := &KVService{ds: NewDataStore()}
	for _, typ := range []raft.LogType{raft.LogNoop, raft.LogConfiguration} {
		if got := kvs.Apply(&raft.LogEntry{Type: typ, Data: valid}); got != nil {
			t.Fatalf("type %v: Apply = %#v, want nil", typ, got)
		}
	}
	if _, found := kvs.ds.Get("cfg"); found {
		t.Fatal("non-command bytes were applied as a KV command")
	}
}

// fileNode — узел KVService на FileStorage и FileSnapshot в каталоге dir.
type fileNode struct {
	dir     string
	storage *store.FileStorage
	snaps   *store.FileSnapshot
}

// startFileCluster запускает (или перезапускает) узлы на каталогах nodes.
func startFileCluster(t *testing.T, nodes []*fileNode) *testCluster {
	t.Helper()
	n := len(nodes)
	c := &testCluster{nodes: make([]*KVService, n)}
	ready := make(chan any)
	for i, node := range nodes {
		peers := make([]int, 0, n-1)
		for p := range n {
			if p != i {
				peers = append(peers, p)
			}
		}
		node.storage = store.NewFileStorage(node.dir)
		snaps, err := store.NewFileSnapshot(node.dir, 2)
		if err != nil {
			t.Fatalf("NewFileSnapshot: %v", err)
		}
		node.snaps = snaps
		c.nodes[i] = New(&Config{Config: raft.Config{
			ReelectionTimeout: _testReelectionTimeout,
			ServerID:          i,
			PeerIds:           peers,
			Storage:           node.storage,
			SnapshotStore:     snaps,
			SnapshotThreshold: 64,
			SnapshotInterval:  50 * time.Millisecond,
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

// appliedOnAll сообщает, что каждый узел применил значение value ключа key.
func appliedOnAll(c *testCluster, key, value string) bool {
	for _, kvs := range c.nodes {
		if v, ok := kvs.ds.Get(key); !ok || v != value {
			return false
		}
	}
	return true
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

// otherLeader ждёт лидера среди узлов, кроме old.
func otherLeader(t *testing.T, c *testCluster, old int) int {
	t.Helper()
	leader := -1
	waitFor(t, "a new leader", func() bool {
		for i, kvs := range c.nodes {
			if i != old && kvs.IsLeader() {
				leader = i
				return true
			}
		}
		return false
	})
	return leader
}

// TestCommandDataRestartAfterSnapshot — сквозной сценарий D на FileStorage и
// FileSnapshot: команды, снимок с усечением журнала, суффикс после снимка,
// перезапуск всех узлов, восстановление KV, ответы клиента, смена лидера и
// репликация после неё. Восстановленные команды журнала — []byte тега 3.
// Байты, не являющиеся командой, дают в future ошибку, а не Command.
func TestCommandDataRestartAfterSnapshot(t *testing.T) {
	defer leaktest.CheckTimeout(t, 3*_pathTestTimeout)()
	const (
		before = 200
		after  = 10
	)
	nodes := []*fileNode{{dir: t.TempDir()}, {dir: t.TempDir()}, {dir: t.TempDir()}}
	key := func(i int) string { return fmt.Sprintf("k%03d", i) }

	c := startFileCluster(t, nodes)
	defer func() { c.shutdown(t) }()
	leader := c.leader(t)
	for i := range before {
		putOK(t, leader, key(i), fmt.Sprintf("v%d", i))
	}
	waitFor(t, "a snapshot with log compaction on all nodes", func() bool {
		for _, node := range nodes {
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
		putOK(t, leader, key(i), fmt.Sprintf("v%d", i))
	}
	waitFor(t, "the last command on all nodes", func() bool {
		return appliedOnAll(c, key(before+after-1), fmt.Sprintf("v%d", before+after-1))
	})
	c.shutdown(t)

	// Перезапуск: снимок + суффикс журнала v3 со своими []byte.
	c = startFileCluster(t, nodes)
	for i, node := range nodes {
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
			if _, ok := e.Data.([]byte); !ok {
				t.Fatalf("node %d: restored command %d has type %T, want []byte", i, e.Index, e.Data)
			}
			if _, ok := logEntryCommand(e.Data); !ok {
				t.Fatalf("node %d: restored command %d does not decode", i, e.Index)
			}
		}
	}
	leaderIdx := c.leaderIndex(t)
	waitFor(t, "restored KV state on all nodes", func() bool {
		for i := range before + after {
			if !appliedOnAll(c, key(i), fmt.Sprintf("v%d", i)) {
				return false
			}
		}
		return true
	})

	leader = c.nodes[leaderIdx]
	var get api.GetResponse
	postJSON(t, leader.handleGet, "/get/", api.GetRequest{Key: key(0)}, &get)
	if get != (api.GetResponse{RespStatus: api.StatusOK, KeyFound: true, Value: "v0"}) {
		t.Fatalf("GET before snapshot key = %+v", get)
	}
	if put := putOK(t, leader, key(before), "after-restart"); !put.KeyFound || put.PrevValue != fmt.Sprintf("v%d", before) {
		t.Fatalf("PUT of a suffix key = %+v", put)
	}

	// Смена лидера: прежний лидер изолирован, новый принимает команды.
	c.isolate(t, leaderIdx)
	newIdx := otherLeader(t, c, leaderIdx)
	newLeader := c.nodes[newIdx]
	if put := putOK(t, newLeader, "after-failover", "x"); put.KeyFound {
		t.Fatalf("PUT on the new leader = %+v", put)
	}
	var cas api.CASResponse
	postJSON(t, newLeader.handleCAS, "/cas/", api.CASRequest{Key: key(1), CompareValue: "v1", Value: "cas-value"}, &cas)
	if cas != (api.CASResponse{RespStatus: api.StatusOK, KeyFound: true, PrevValue: "v1"}) {
		t.Fatalf("CAS on the new leader = %+v", cas)
	}
	for j := range c.nodes {
		if j != leaderIdx {
			c.link(t, leaderIdx, j)
		}
	}
	waitFor(t, "replication to the former leader", func() bool {
		return appliedOnAll(c, "after-failover", "x") && appliedOnAll(c, key(1), "cas-value") &&
			appliedOnAll(c, key(before), "after-restart")
	})

	// Не-команда в журнале: future получает ошибку, состояние не меняется.
	current := c.nodes[otherLeader(t, c, -1)]
	future := current.rs.Apply([]byte("not a KV command"), _requestTimeout)
	if err := <-future.ErrorCh(); err != nil {
		t.Fatalf("Apply of raw bytes: %v", err)
	}
	if resErr, ok := future.Response().(error); !ok || !errors.Is(resErr, errCorruptCommand) {
		t.Fatalf("future response %#v, want errCorruptCommand", future.Response())
	}
}
