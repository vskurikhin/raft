package kvservice

import (
	"bytes"
	"encoding/gob"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/vskurikhin/raft"
)

// gobRoundTripEntry кодирует запись журнала так же, как транспорт и
// хранилище: Data передаётся интерфейсным полем.
func gobRoundTripEntry(t *testing.T, entry raft.LogEntry) raft.LogEntry {
	t.Helper()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(entry); err != nil {
		t.Fatalf("gob encode: %v", err)
	}
	var got raft.LogEntry
	if err := gob.NewDecoder(&buf).Decode(&got); err != nil {
		t.Fatalf("gob decode: %v", err)
	}
	return got
}

func mustCommandData(t *testing.T, cmd Command) *commandData {
	t.Helper()
	data, err := newCommandData(&cmd)
	if err != nil {
		t.Fatalf("newCommandData: %v", err)
	}
	return data
}

func TestCommandDataGobRoundTrip(t *testing.T) {
	for _, want := range codecCommands() {
		got := gobRoundTripEntry(t, raft.LogEntry{Index: 3, Term: 2, Data: mustCommandData(t, want)})
		data, ok := got.Data.(*commandData)
		if !ok {
			t.Fatalf("decoded Data type %T, want *commandData", got.Data)
		}
		if data.cmd != want {
			t.Fatalf("round-trip mismatch: got %+v, want %+v", data.cmd, want)
		}
	}
}

// TestCommandDataEncodesEveryTime: каждая сериализация вызывает кодек
// заново и даёт новый буфер, кэша нет.
func TestCommandDataEncodesEveryTime(t *testing.T) {
	const key = "encode-every-time"
	rec := startEncodeRecorder(t, key)
	data := mustCommandData(t, Command{Kind: CommandPut, Key: key, Value: "v", ID: 1})

	first, err := data.GobEncode()
	if err != nil {
		t.Fatalf("first GobEncode: %v", err)
	}
	second, err := data.GobEncode()
	if err != nil {
		t.Fatalf("second GobEncode: %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("repeated encodings differ")
	}
	if &first[0] == &second[0] {
		t.Fatal("repeated encodings share a buffer")
	}
	gobRoundTripEntry(t, raft.LogEntry{Data: data})
	gobRoundTripEntry(t, raft.LogEntry{Data: data})

	if wire, storage, other := rec.snapshot(); wire != 0 || storage != 0 || other != 4 {
		t.Fatalf("encode calls wire=%d storage=%d other=%d, want 0/0/4", wire, storage, other)
	}
}

// TestNewCommandDataDropsResult: поля результата не входят в носитель.
func TestNewCommandDataDropsResult(t *testing.T) {
	data := mustCommandData(t, Command{Kind: CommandGet, Key: "k", ResultValue: "x", ResultFound: true})
	if data.cmd.ResultValue != "" || data.cmd.ResultFound {
		t.Fatalf("carrier keeps result fields: %+v", data.cmd)
	}
}

// TestCommandDataGobDecodeRejectsCorruption: ошибка декодера носителя
// останавливает чтение gob.
func TestCommandDataGobDecodeRejectsCorruption(t *testing.T) {
	var data commandData
	if err := data.GobDecode([]byte("KVC1 broken")); err == nil {
		t.Fatal("GobDecode accepted corrupt input")
	}
	if data.cmd != (Command{}) {
		t.Fatalf("carrier changed on failed decode: %+v", data.cmd)
	}
}

// TestCommandDataCorruptCarrierGuard — защитный путь: носитель, созданный
// в обход конструктора, не кодируется, и gob возвращает ошибку. Из HTTP
// такой носитель недостижим: обработчики создают его через newCommandData.
func TestCommandDataCorruptCarrierGuard(t *testing.T) {
	corrupt := &commandData{cmd: Command{Kind: CommandInvalid, Key: "k"}}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(raft.LogEntry{Data: corrupt}); err == nil {
		t.Fatal("gob encoded a carrier with an invalid kind")
	}
}

// TestApplyCommandValidationErrorReachesHTTP: ошибка проверки до Apply
// возвращается ответом 400, Apply и кодек не вызываются. Поле rs пустое:
// вызов Apply завершился бы паникой.
func TestApplyCommandValidationErrorReachesHTTP(t *testing.T) {
	const key = "invalid-http"
	rec := startEncodeRecorder(t, key)
	kvs := &KVService{id: 3}
	for _, kind := range []CommandKind{CommandInvalid, CommandDelete + 1, -7} {
		w := httptest.NewRecorder()
		future, ok := kvs.applyCommand(w, &Command{Kind: kind, Key: key, ID: kvs.id})
		if ok || future != nil {
			t.Fatalf("applyCommand(kind=%d) passed validation", kind)
		}
		if w.Code != http.StatusBadRequest {
			t.Fatalf("HTTP status %d, want %d", w.Code, http.StatusBadRequest)
		}
		if !strings.Contains(w.Body.String(), "unsupported command kind") {
			t.Fatalf("HTTP body %q lacks the validation error", w.Body.String())
		}
	}
	if wire, storage, other := rec.snapshot(); wire+storage+other != 0 {
		t.Fatalf("validation encoded the command %d times", wire+storage+other)
	}

	valid := mustCommandData(t, Command{Kind: CommandPut, Key: key, Value: "v", ID: kvs.id})
	for range 2 {
		if _, err := valid.GobEncode(); err != nil {
			t.Fatalf("valid carrier failed to encode: %v", err)
		}
	}
}

// fsmSequence — последовательность операций с повторами, CAS и Delete,
// пустыми строками и произвольными байтами.
func fsmSequence() []Command {
	return []Command{
		{Kind: CommandGet, Key: "a"},
		{Kind: CommandPut, Key: "a", Value: "1"},
		{Kind: CommandPut, Key: "a", Value: "2"},
		{Kind: CommandGet, Key: "a"},
		{Kind: CommandCAS, Key: "a", CompareValue: "x", Value: "3"},
		{Kind: CommandCAS, Key: "a", CompareValue: "2", Value: "3"},
		{Kind: CommandPut, Key: "", Value: ""},
		{Kind: CommandGet, Key: ""},
		{Kind: CommandPut, Key: "\x00\xff", Value: "\xc3\x28"},
		{Kind: CommandCAS, Key: "\x00\xff", CompareValue: "\xc3\x28", Value: ""},
		{Kind: CommandDelete, Key: "a"},
		{Kind: CommandDelete, Key: "a"},
		{Kind: CommandGet, Key: "\x00\xff"},
	}
}

// TestFSMResultsMatchLegacyCommand: результаты FSM для носителя после
// gob совпадают с результатами прежнего Command.
func TestFSMResultsMatchLegacyCommand(t *testing.T) {
	legacy := &KVService{ds: NewDataStore()}
	carrier := &KVService{ds: NewDataStore()}
	for i, cmd := range fsmSequence() {
		cmd.ID = 2
		want := legacy.Apply(&raft.LogEntry{Data: cmd})
		entry := gobRoundTripEntry(t, raft.LogEntry{Data: mustCommandData(t, cmd)})
		got := carrier.Apply(&entry)
		if _, ok := got.(Command); !ok {
			t.Fatalf("step %d: result type %T, want Command", i, got)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("step %d: got %+v, want %+v", i, got, want)
		}
	}
	batch := carrier.ApplyBatch([]*raft.LogEntry{{Data: mustCommandData(t, Command{Kind: CommandGet, Key: ""})}})
	if res, ok := batch[0].(Command); !ok || !res.ResultFound {
		t.Fatalf("ApplyBatch result %+v", batch[0])
	}
}

// TestFSMIgnoresForeignData: nil, произвольные значения и пустой носитель
// дают прежний ответ nil, без нулевой успешной команды.
func TestFSMIgnoresForeignData(t *testing.T) {
	kvs := &KVService{ds: NewDataStore()}
	for _, data := range []any{nil, "text", []byte("config"), 42, (*commandData)(nil), Command{Kind: CommandInvalid}} {
		if got := kvs.Apply(&raft.LogEntry{Data: data}); got != nil {
			t.Fatalf("Apply(%T) = %+v, want nil", data, got)
		}
	}
}

// TestSnapshotRestoreThenApplyCarrier: после восстановления из снимка
// носитель применяется к восстановленному состоянию.
func TestSnapshotRestoreThenApplyCarrier(t *testing.T) {
	src := &KVService{ds: NewDataStore()}
	src.Apply(&raft.LogEntry{Data: mustCommandData(t, Command{Kind: CommandPut, Key: "k", Value: "old"})})
	snap, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	var sink bytes.Buffer
	if err := snap.Persist(&bufferSink{Buffer: &sink}); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	dst := &KVService{ds: NewDataStore()}
	if err := dst.Restore(io.NopCloser(&sink)); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	entry := gobRoundTripEntry(t, raft.LogEntry{Data: mustCommandData(t, Command{Kind: CommandPut, Key: "k", Value: "new"})})
	got, ok := dst.Apply(&entry).(Command)
	if !ok || !got.ResultFound || got.ResultValue != "old" {
		t.Fatalf("Apply after restore = %+v", got)
	}
	if v, found := dst.ds.Get("k"); !found || v != "new" {
		t.Fatalf("restored store has %q/%v, want new", v, found)
	}
}

// bufferSink — приёмник снимка в памяти.
type bufferSink struct {
	*bytes.Buffer
}

func (*bufferSink) ID() string    { return "test" }
func (*bufferSink) Cancel() error { return nil }
func (*bufferSink) Close() error  { return nil }
