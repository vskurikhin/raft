package kvservice

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/vskurikhin/raft"
	"github.com/vskurikhin/raft/pkg/api"
	"github.com/vskurikhin/raft/pkg/raft/contract"
	"github.com/vskurikhin/raft/pkg/raft/protocol"
	"github.com/vskurikhin/raft/pkg/raft/store"
	"github.com/vskurikhin/raft/pkg/raft/transp"
)

// HTTP-маршруты команд KV-сервиса.
const (
	routePut    = "put"
	routeCas    = "cas"
	routeGet    = "get"
	routeDelete = "delete"
)

// TestEncodedMaxKV — K + 2V + 816 с проверкой переполнения и отрицательных
// входов; значения профилей владельца 5424 и 41776.
func TestEncodedMaxKV(t *testing.T) {
	for _, c := range []struct {
		k, v int
		want uint64
	}{{512, 2048, 5424}, {8192, 16384, 41776}, {0, 0, 816}, {1, 1, 819}} {
		got, err := EncodedMaxKV(c.k, c.v)
		if err != nil || got != c.want {
			t.Fatalf("EncodedMaxKV(%d,%d) = %d, %v; want %d", c.k, c.v, got, err, c.want)
		}
	}
	if _, err := EncodedMaxKV(-1, 1); err == nil {
		t.Fatal("negative accepted")
	}
}

// TestEncodedMaxKVBoundsActualEncoding — оценка не меньше фактической
// сетевой длины Command на предельных строках при широком ID, в том числе
// после регистрации большого числа типов gob (номера типов шире).
func TestEncodedMaxKVBoundsActualEncoding(t *testing.T) {
	gob.Register(Command{})
	check := func(k, v int) {
		bound, err := EncodedMaxKV(k, v)
		if err != nil {
			t.Fatal(err)
		}
		cmd := Command{Kind: CommandCAS, Key: strings.Repeat("k", k), Value: strings.Repeat("v", v),
			CompareValue: strings.Repeat("c", v), ID: -1 << 62}
		size, err := protocol.MeasureData(cmd, bound)
		if err != nil {
			t.Fatalf("K=%d V=%d: actual encoding exceeds bound %d: %v", k, v, bound, err)
		}
		if size > bound {
			t.Fatalf("size %d > bound %d", size, bound)
		}
	}
	check(512, 2048)
	check(8192, 16384)
	encodeManyGobTypes(t, 80)
	check(512, 2048)
	check(8192, 16384)
}

// TestValidateConfigKV — V29/V30: пределы полей и EncodedMaxKV(K,V) <= D
// до запуска; библиотечный ноль — умолчание, отрицательное и выше верхней
// границы — ошибка; диагностика содержит числа.
func TestValidateConfigKV(t *testing.T) {
	_, timing := transp.NormalizeTCPTimeouts(transp.TCPTimeouts{})
	cases := []struct {
		name    string
		k, v    int
		d       uint64
		wantErr string
	}{
		{"defaults", 0, 0, 0, ""},
		{"key 8192 at default D", 8192, 2048, 0, "EncodedMaxKV(8192,2048)=13104 exceeds max-data-bytes=8192"},
		{"key 8192", 8192, 2048, 13104, ""},
		{"key 8193", 8193, 2048, 0, "MaxKeyBytes"},
		{"value 16385", 512, 16385, 0, "MaxValueBytes"},
		{"negative key", -1, 0, 0, "MaxKeyBytes"},
		{"upper at default D", 8192, 16384, 0, "EncodedMaxKV(8192,16384)=41776 exceeds max-data-bytes=8192"},
		{"upper at 41984", 8192, 16384, 41984, ""},
		{"bound-1", 0, 0, 5423, "EncodedMaxKV(512,2048)=5424 exceeds max-data-bytes=5423"},
		{"bound", 0, 0, 5424, ""},
	}
	for _, c := range cases {
		cfg := &Config{MaxKeyBytes: c.k, MaxValueBytes: c.v}
		if c.d != 0 {
			cfg.Limits = contract.Limits{MaxFrameBytes: 262144, MaxEntries: 262056 / (32 + c.d),
				MaxDataBytes: c.d, MaxConfigurationBytes: 2560}
		}
		err := ValidateConfig(cfg, timing)
		if c.wantErr == "" && err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if c.wantErr != "" && (err == nil || !strings.Contains(err.Error(), c.wantErr)) {
			t.Fatalf("%s: err = %v, want %q", c.name, err, c.wantErr)
		}
	}
	// Библиотечный профиль 200/5/1 при TCP 200/200: 5 + 1 + 200 = 206 >= 200.
	cfg := &Config{}
	cfg.HeartbeatTimeout, cfg.TickerTimeout, cfg.ReelectionTimeout = 5*time.Millisecond, time.Millisecond, 200*time.Millisecond
	cfg.ApplyBatchInterval = time.Millisecond
	if err := ValidateConfig(cfg, timing); err == nil ||
		!strings.Contains(err.Error(), "invalid timing profile: heartbeat 5ms + ticker 1ms + max RPC window 200ms = 206ms >= reelection base 200ms") {
		t.Fatalf("200/5/1: %v", err)
	}
}

const _newInvalidHelperEnv = "KVSERVICE_NEW_INVALID_HELPER"

// TestNewRejectsBeforeSideEffects — V27/V34: неверная конфигурация
// отвергается New до регистрации типов и отметки создания сервиса: в
// отдельном процессе после отказа сторожевой флаг трассировки не выставлен.
func TestNewRejectsBeforeSideEffects(t *testing.T) {
	if os.Getenv(_newInvalidHelperEnv) == "1" {
		runNewInvalidHelper()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestNewRejectsBeforeSideEffects$", "-test.count=1")
	cmd.Env = append(os.Environ(), _newInvalidHelperEnv+"=1")
	out, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(out), "HELPER-OK") {
		t.Fatalf("helper: %v\n%s", err, out)
	}
}

func runNewInvalidHelper() {
	transport, err := transp.NewTCPTransport("127.0.0.1:0", transp.TCPTimeouts{}, 0)
	if err != nil {
		fmt.Println("HELPER-FAIL transport:", err)
		return
	}
	defer transport.Close()
	panicked := func() (msg string) {
		defer func() {
			if r := recover(); r != nil {
				msg = fmt.Sprint(r)
			}
		}()
		New(&Config{MaxKeyBytes: 8192, MaxValueBytes: 16384, Config: raft.Config{
			ServerID: 0, Storage: store.NewMapStorage(), Transport: transport,
		}}, make(chan any))
		return ""
	}()
	if !strings.Contains(panicked, "EncodedMaxKV(8192,16384)=41776 exceeds max-data-bytes=8192") {
		fmt.Println("HELPER-FAIL panic:", panicked)
		return
	}
	if _traceCMCreated.Load() {
		fmt.Println("HELPER-FAIL: set-once marker set before validation")
		return
	}
	// Транспорт с другим профилем пределов — отказ New по несовпадению.
	upper := contract.Limits{MaxFrameBytes: 262144, MaxEntries: 6, MaxDataBytes: 41984, MaxConfigurationBytes: 2560}
	other, err := transp.NewTCPTransportWithLimits("127.0.0.1:0", transp.TCPTimeouts{}, 0, upper)
	if err != nil {
		fmt.Println("HELPER-FAIL transport:", err)
		return
	}
	defer other.Close()
	mismatch := func() (msg string) {
		defer func() {
			if r := recover(); r != nil {
				msg = fmt.Sprint(r)
			}
		}()
		New(&Config{Config: raft.Config{ServerID: 0, Storage: store.NewMapStorage(), Transport: other}}, make(chan any))
		return ""
	}()
	if !strings.Contains(mismatch, "parameter MaxEntries node 31 transport 6") || _traceCMCreated.Load() {
		fmt.Println("HELPER-FAIL mismatch:", mismatch)
		return
	}
	fmt.Println("HELPER-OK")
}

// kvNode — одноузловой KVService с HTTP для проверок обработчиков.
type kvNode struct {
	kvs     *KVService
	storage *store.MapStorage
	base    string
}

func startSingleNode(t *testing.T) *kvNode {
	t.Helper()
	storage := store.NewMapStorage()
	ready := make(chan any)
	kvs := NewKVService("127.0.0.1:0", 0, nil, storage, ready)
	close(ready)
	if err := kvs.ServeHTTP("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = kvs.Shutdown() })
	deadline := time.Now().Add(5 * time.Second)
	for !kvs.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("single node did not become leader")
		}
		time.Sleep(20 * time.Millisecond)
	}
	return &kvNode{kvs: kvs, storage: storage, base: "http://" + kvs.GetHTTPListenAddr()}
}

func (n *kvNode) logLen(t *testing.T) int {
	t.Helper()
	entries, err := n.storage.LoadLog()
	if err != nil && !errors.Is(err, contract.ErrLogNotFound) {
		t.Fatal(err)
	}
	return len(entries)
}

func (n *kvNode) post(t *testing.T, route string, body []byte) (int, api.StatusResponse, string) {
	t.Helper()
	resp, err := http.Post(n.base+"/"+route+"/", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var sr api.StatusResponse
	_ = json.Unmarshal(raw, &sr)
	return resp.StatusCode, sr, string(raw)
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestHandlersFieldLimits — V29/V31: ключ, Value и CompareValue проверяются
// отдельно до построения команды (512/2048 по умолчанию); превышение — 413
// и StatusTooLarge, журнал не растёт; значения на границе принимаются.
// Слабое чтение проверяет ключ после URL-декодирования.
func TestHandlersFieldLimits(t *testing.T) {
	n := startSingleNode(t)
	key512, key513 := strings.Repeat("k", 512), strings.Repeat("k", 513)
	val2048, val2049 := strings.Repeat("v", 2048), strings.Repeat("v", 2049)

	ok := []struct {
		route string
		body  any
	}{
		{routePut, api.PutRequest{Key: key512, Value: val2048}},
		{routeCas, api.CASRequest{Key: key512, CompareValue: val2048, Value: val2048}},
		{routeGet, api.GetRequest{Key: key512}},
		{routeDelete, api.DeleteRequest{Key: key512}},
	}
	for _, c := range ok {
		code, sr, raw := n.post(t, c.route, mustJSON(t, c.body))
		if code != http.StatusOK || sr.RespStatus != api.StatusOK {
			t.Fatalf("%s at limit: %d %s", c.route, code, raw)
		}
	}
	tooLarge := []struct {
		name  string
		route string
		body  any
	}{
		{"put key", routePut, api.PutRequest{Key: key513, Value: "v"}},
		{"put value", routePut, api.PutRequest{Key: "k", Value: val2049}},
		{"cas compare", routeCas, api.CASRequest{Key: "k", CompareValue: val2049, Value: "v"}},
		{"cas value", routeCas, api.CASRequest{Key: "k", CompareValue: "c", Value: val2049}},
		{"cas key", routeCas, api.CASRequest{Key: key513}},
		{"get key", routeGet, api.GetRequest{Key: key513}},
		{"delete key", routeDelete, api.DeleteRequest{Key: key513}},
	}
	before := n.logLen(t)
	for _, c := range tooLarge {
		code, sr, raw := n.post(t, c.route, mustJSON(t, c.body))
		if code != http.StatusRequestEntityTooLarge || sr.RespStatus != api.StatusTooLarge {
			t.Fatalf("%s: %d %s", c.name, code, raw)
		}
	}
	if after := n.logLen(t); after != before {
		t.Fatalf("log grew %d -> %d on rejected requests", before, after)
	}

	weak := func(escaped string) int {
		resp, err := http.Get(n.base + "/weak-get/" + escaped)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	// 512 символов в процентном кодировании (1536 Б URL) — 512 Б после
	// декодирования.
	if code := weak(strings.Repeat("%41", 512)); code != http.StatusOK {
		t.Fatalf("weak-get decoded 512: %d", code)
	}
	if code := weak(strings.Repeat("%41", 513)); code != http.StatusRequestEntityTooLarge {
		t.Fatalf("weak-get decoded 513: %d", code)
	}
	if code := weak(url.PathEscape(key512)); code != http.StatusOK {
		t.Fatalf("weak-get 512: %d", code)
	}
}

// TestHandlersJSONBodyCap — V32: предел тела B = 6·(K+2V)+1024 = 28672 Б
// проверяется до декодирования (MaxBytesReader): B−1 и B принимаются, B+1 —
// 413; экранирование ×6 в пределах B принимается; второй объект или хвост
// после объекта — не TooLarge, а прежняя ошибка разбора 400; синтаксическая
// ошибка — 400; некорректный UTF-8 заменяется U+FFFD и предел применяется
// к результату замены.
func TestHandlersJSONBodyCap(t *testing.T) {
	n := startSingleNode(t)
	const capB = 6*(512+2*2048) + 1024
	obj := mustJSON(t, api.PutRequest{Key: "k", Value: "v"})
	padded := func(total int) []byte {
		return append(bytes.Clone(obj), bytes.Repeat([]byte(" "), total-len(obj))...)
	}
	for _, size := range []int{capB - 1, capB} {
		if code, _, raw := n.post(t, routePut, padded(size)); code != http.StatusOK {
			t.Fatalf("body %d: %d %s", size, code, raw)
		}
	}
	before := n.logLen(t)
	if code, sr, raw := n.post(t, routePut, padded(capB+1)); code != http.StatusRequestEntityTooLarge ||
		sr.RespStatus != api.StatusTooLarge {
		t.Fatalf("body B+1: %d %s", code, raw)
	}
	if n.logLen(t) != before {
		t.Fatal("log grew on oversized body")
	}
	// Ключ 512 символов, каждый экранирован A (6 Б): 3072 Б JSON,
	// 512 Б после декодирования.
	escaped := []byte(`{"Key":"` + strings.Repeat(`A`, 512) + `","Value":"v"}`)
	if code, _, raw := n.post(t, routePut, escaped); code != http.StatusOK {
		t.Fatalf("escaped key: %d %s", code, raw)
	}
	for name, body := range map[string][]byte{
		"second object": append(bytes.Clone(obj), obj...),
		"trailing data": append(bytes.Clone(obj), []byte(" x")...),
		"syntax":        []byte(`{"Key":`),
	} {
		if code, _, raw := n.post(t, routePut, body); code != http.StatusBadRequest {
			t.Fatalf("%s: %d %s", name, code, raw)
		}
	}
	// 170 некорректных байтов → 510 Б после замены U+FFFD; 171 → 513 Б.
	invalid := func(count int) []byte {
		return append(append([]byte(`{"Key":"`), bytes.Repeat([]byte{0xff}, count)...), []byte(`","Value":"v"}`)...)
	}
	if code, _, raw := n.post(t, routePut, invalid(170)); code != http.StatusOK {
		t.Fatalf("U+FFFD 510 B: %d %s", code, raw)
	}
	if code, _, raw := n.post(t, routePut, invalid(171)); code != http.StatusRequestEntityTooLarge {
		t.Fatalf("U+FFFD 513 B: %d %s", code, raw)
	}
}

// TestApplyOversizedCommand — V31/V35: прямой Apply команды, сетевая длина
// которой превышает D (включая ResultValue, измеряемый полностью), —
// ErrCommandTooLarge до журнала; FSM не меняет ResultValue исходной
// команды в записи журнала.
func TestApplyOversizedCommand(t *testing.T) {
	n := startSingleNode(t)
	before := n.logLen(t)
	cmd := Command{Kind: CommandPut, Key: "k", Value: "v", ResultValue: strings.Repeat("r", 9000), ID: 1}
	err := n.kvs.rs.Apply(cmd, time.Second).Error()
	if !errors.Is(err, raft.ErrCommandTooLarge) {
		t.Fatalf("oversized: %v", err)
	}
	if n.logLen(t) != before {
		t.Fatal("log grew on oversized Apply")
	}

	put := Command{Kind: CommandPut, Key: "k2", Value: "v2", ID: 1}
	future := n.kvs.rs.Apply(put, time.Second)
	if err = future.Error(); err != nil {
		t.Fatal(err)
	}
	entries, err := n.storage.LoadLog()
	if err != nil {
		t.Fatal(err)
	}
	last, ok := entries[len(entries)-1].Data.(Command)
	if !ok || last.ResultValue != "" || last.Key != "k2" {
		t.Fatalf("journal entry changed by FSM: %#v", entries[len(entries)-1].Data)
	}
	if put.ResultValue != "" {
		t.Fatal("caller command mutated")
	}
}

// encodeManyGobTypes кодирует значения count различных структурных типов:
// номера типов gob, назначаемые процессу, растут и становятся шире.
func encodeManyGobTypes(t *testing.T, count int) {
	t.Helper()
	for i := range count {
		typ := reflect.StructOf([]reflect.StructField{{Name: fmt.Sprintf("F%d", i), Type: reflect.TypeFor[int]()}})
		if err := gob.NewEncoder(io.Discard).EncodeValue(reflect.New(typ).Elem()); err != nil {
			t.Fatal(err)
		}
	}
}

// TestStoredOversizedFieldsNotTruncated — V33/V35: команда, принятая журналом
// в обход HTTP-пределов (прямой Apply, в пределах D), применяется FSM без
// проверки по новым HTTP-пределам; её значение возвращается целиком
// (ResultValue не усекается), а HTTP-ключ сверх предела отвергается 413 без
// изменения данных.
func TestStoredOversizedFieldsNotTruncated(t *testing.T) {
	n := startSingleNode(t)
	longValue := strings.Repeat("L", 3000) // > 2048, в пределах D
	if err := n.kvs.rs.Apply(Command{Kind: CommandPut, Key: "old", Value: longValue, ID: 1}, time.Second).Error(); err != nil {
		t.Fatal(err)
	}
	longKey := strings.Repeat("K", 600) // > 512, в пределах D
	if err := n.kvs.rs.Apply(Command{Kind: CommandPut, Key: longKey, Value: "x", ID: 1}, time.Second).Error(); err != nil {
		t.Fatal(err)
	}
	code, _, raw := n.post(t, routeGet, mustJSON(t, api.GetRequest{Key: "old"}))
	var gr api.GetResponse
	if err := json.Unmarshal([]byte(raw), &gr); err != nil || code != http.StatusOK || gr.Value != longValue {
		t.Fatalf("GET old: %d len %d %v", code, len(gr.Value), err)
	}
	if code, _, _ = n.post(t, routeDelete, mustJSON(t, api.DeleteRequest{Key: longKey})); code != http.StatusRequestEntityTooLarge {
		t.Fatalf("DELETE long key: %d", code)
	}
	if v, ok := n.kvs.ds.Get(longKey); !ok || v != "x" {
		t.Fatal("rejected request changed data")
	}
}
