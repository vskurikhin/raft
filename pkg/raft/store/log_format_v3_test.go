package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/vskurikhin/raft/pkg/raft/contract"
)

// v2LogFixtureHex — реальные байты журнала версии 2, записанные сборкой C
// (7e27a06, публичный FileStorage.RewriteLog): noop с nil, LogCommand с
// []byte("cmd") и конфигурация []byte{1, 2}. Сборка D обязана их отвергать.
const v2LogFixtureHex = "52414654505354000002001800000000000000005a43f8c8524c4232" +
	"0000000000000000000000fd0000000000000003b32577f30000000000000000" +
	"0000000000000000000000000100000000000000000000000000002a257f0301" +
	"010f6c6f6744617461456e76656c6f706501ff80000101010444617461011000" +
	"000003ff8000000000000000000100000000000000010000000000000000000000" +
	"000000003a257f0301010f6c6f6744617461456e76656c6f706501ff80000101" +
	"010444617461011000000013ff8001075b5d75696e74380a050003636d640000" +
	"00000000000002000000000000000102000000000000000000000000000039257f" +
	"0301010f6c6f6744617461456e76656c6f706501ff8000010101044461746101" +
	"1000000012ff8001075b5d75696e74380a0400020102000dad9936524c434d49" +
	"543200"

// v2LogFixtureSHA — SHA-256 байтов v2LogFixtureHex.
const v2LogFixtureSHA = "bae917fd6db5705ea0d64b93342ac848fd0c53ee59650c791ee3b4e82d1770f2"

// v2LogFrame возвращает реальные байты журнала версии 2 из фикстуры.
func v2LogFrame(t *testing.T) []byte {
	t.Helper()
	raw, err := hex.DecodeString(v2LogFixtureHex)
	if err != nil {
		t.Fatalf("декодирование фикстуры v2: %v", err)
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != v2LogFixtureSHA {
		t.Fatalf("SHA-256 фикстуры v2 = %s, want %s", got, v2LogFixtureSHA)
	}
	return raw
}

// testCommandBytes — тестовый носитель готовых байтов команды. GobEncode
// считает вызовы: штатный путь журнала D не должен его вызывать.
type testCommandBytes struct {
	encoded []byte
}

// _testCommandGobCalls — число вызовов GobEncode тестового носителя.
var _testCommandGobCalls atomic.Int64

func (c *testCommandBytes) RaftCommandBytes() []byte {
	return c.encoded[:len(c.encoded):len(c.encoded)]
}

func (c *testCommandBytes) GobEncode() ([]byte, error) {
	_testCommandGobCalls.Add(1)
	return slices.Clone(c.encoded), nil
}

func (c *testCommandBytes) GobDecode(data []byte) error {
	c.encoded = slices.Clone(data)
	return nil
}

// registerTestCommandBytes регистрирует носитель для gob: нужен для
// проверки, что вне LogCommand он идёт общим тегом 4.
func registerTestCommandBytes() {
	gob.Register(&testCommandBytes{})
}

// dataTag возвращает тег закодированной Data.
func dataTag(t *testing.T, data []byte) byte {
	t.Helper()
	if len(data) < logDataHeaderSize || data[0] != logDataVersion {
		t.Fatalf("Data %x без заголовка версии 1", data)
	}
	return data[1]
}

// logDataTagCase — значение Data, ожидаемый тег и ожидаемое значение после
// чтения.
type logDataTagCase struct {
	name string
	typ  contract.LogType
	data any
	tag  byte
	want any
}

// logDataTagCases покрывает все теги, включая различие nil/empty и
// одинаковые байты под разными типами записи.
func logDataTagCases() []logDataTagCase {
	carrier := &testCommandBytes{encoded: []byte("KVC1-carrier")}
	return []logDataTagCase{
		{"nil interface", contract.LogNoop, nil, logDataTagNil, nil},
		{"nil interface у команды", contract.LogCommand, nil, logDataTagNil, nil},
		{"nil []byte у команды", contract.LogCommand, []byte(nil), logDataTagNilBytes, []byte(nil)},
		{"nil []byte у конфигурации", contract.LogConfiguration, []byte(nil), logDataTagNilBytes, []byte(nil)},
		{"пустой []byte у команды", contract.LogCommand, []byte{}, logDataTagBytes, []byte{}},
		{"пустой []byte у noop", contract.LogNoop, []byte{}, logDataTagBytes, []byte{}},
		{"непустой []byte конфигурации", contract.LogConfiguration, []byte{1, 2, 3}, logDataTagBytes, []byte{1, 2, 3}},
		{"непустой []byte noop", contract.LogNoop, []byte{0}, logDataTagBytes, []byte{0}},
		{"непустой []byte команды", contract.LogCommand, []byte{0, 0xff, 7}, logDataTagCommand, []byte{0, 0xff, 7}},
		{"носитель команды", contract.LogCommand, carrier, logDataTagCommand, []byte("KVC1-carrier")},
		{"носитель вне LogCommand", contract.LogNoop, carrier, logDataTagGob, carrier},
		{"строка", contract.LogCommand, "text", logDataTagGob, "text"},
		{"пустая строка", contract.LogCommand, "", logDataTagGob, ""},
		{"число", contract.LogCommand, 42, logDataTagGob, 42},
		{"зарегистрированный тип", contract.LogCommand, LogCodecTestPayload{ID: 5, Body: "b"}, logDataTagGob,
			LogCodecTestPayload{ID: 5, Body: "b"}},
		{"зарегистрированный тип конфигурации", contract.LogConfiguration, LogCodecTestPayload{ID: 6}, logDataTagGob,
			LogCodecTestPayload{ID: 6}},
	}
}

// requireSameData сравнивает значения Data с учётом различия nil
// interface, nil []byte и пустого []byte.
func requireSameData(t *testing.T, got, want any) {
	t.Helper()
	if reflect.TypeOf(got) != reflect.TypeOf(want) {
		t.Fatalf("тип Data = %T, want %T", got, want)
	}
	if wantBytes, ok := want.([]byte); ok {
		gotBytes := got.([]byte)
		if (gotBytes == nil) != (wantBytes == nil) || !bytes.Equal(gotBytes, wantBytes) {
			t.Fatalf("Data = %#v, want %#v", gotBytes, wantBytes)
		}
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Data = %#v, want %#v", got, want)
	}
}

// TestLogDataTagsRoundTrip: каждый тег кодируется канонически и читается
// тем же типом; nil interface, nil []byte и пустой []byte различаются;
// прочие any сохраняют тип через тег 4.
func TestLogDataTagsRoundTrip(t *testing.T) {
	registerLogCodecTestPayload()
	registerTestCommandBytes()
	for _, tc := range logDataTagCases() {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := encodeLogData(tc.typ, tc.data)
			if err != nil {
				t.Fatalf("encodeLogData: %v", err)
			}
			if tag := dataTag(t, encoded); tag != tc.tag {
				t.Fatalf("тег = %d, want %d", tag, tc.tag)
			}
			got, err := decodeLogData(tc.typ, encoded)
			if err != nil {
				t.Fatalf("decodeLogData: %v", err)
			}
			requireSameData(t, got, tc.want)
		})
	}
}

// TestLogDataBytesAreOwned: байтовые теги не делят память ни с входом
// кодировщика, ни с буфером, из которого читает декодер.
func TestLogDataBytesAreOwned(t *testing.T) {
	for _, typ := range []contract.LogType{contract.LogCommand, contract.LogConfiguration} {
		source := []byte{1, 2, 3}
		encoded, err := encodeLogData(typ, source)
		if err != nil {
			t.Fatalf("encodeLogData: %v", err)
		}
		source[0] = 0xee
		got, err := decodeLogData(typ, encoded)
		if err != nil {
			t.Fatalf("decodeLogData: %v", err)
		}
		gotBytes := got.([]byte)
		if !bytes.Equal(gotBytes, []byte{1, 2, 3}) {
			t.Fatalf("тип %d: Data = %x после изменения входа кодировщика", typ, gotBytes)
		}
		gotBytes[1] = 0xdd
		again, err := decodeLogData(typ, encoded)
		if err != nil {
			t.Fatalf("decodeLogData: %v", err)
		}
		if !bytes.Equal(again.([]byte), []byte{1, 2, 3}) {
			t.Fatalf("тип %d: изменение результата попало в закодированную Data", typ)
		}
	}
}

// TestLogDataDecodeRejects: неизвестные версия и тег, лишние байты у тегов
// 0/1, пустой тег 3, тег 3 вне LogCommand, неканонический непустой тег 2 у
// LogCommand, обрезанная и повреждённая Data — ошибка без значения, а не
// nil-команда.
func TestLogDataDecodeRejects(t *testing.T) {
	gobPayload, err := encodeLogData(contract.LogCommand, "text")
	if err != nil {
		t.Fatalf("encodeLogData: %v", err)
	}
	cases := []struct {
		name    string
		typ     contract.LogType
		data    []byte
		corrupt bool
	}{
		{"пустая Data", contract.LogCommand, nil, true},
		{"только версия", contract.LogCommand, []byte{logDataVersion}, true},
		{"версия 0", contract.LogCommand, []byte{0, logDataTagNil}, true},
		{"версия 2", contract.LogCommand, []byte{2, logDataTagNil}, true},
		{"неизвестный тег 5", contract.LogCommand, []byte{logDataVersion, 5}, true},
		{"неизвестный тег 255", contract.LogCommand, []byte{logDataVersion, 255, 1}, true},
		{"лишний байт у nil", contract.LogNoop, []byte{logDataVersion, logDataTagNil, 0}, true},
		{"лишний байт у nil []byte", contract.LogCommand, []byte{logDataVersion, logDataTagNilBytes, 0}, true},
		{"пустой тег 3", contract.LogCommand, []byte{logDataVersion, logDataTagCommand}, true},
		{"тег 3 у noop", contract.LogNoop, []byte{logDataVersion, logDataTagCommand, 'x'}, true},
		{"тег 3 у конфигурации", contract.LogConfiguration, []byte{logDataVersion, logDataTagCommand, 'x'}, true},
		{"неканонический тег 2 у команды", contract.LogCommand, []byte{logDataVersion, logDataTagBytes, 'x'}, true},
		{"лишний байт после gob", contract.LogCommand, append(slices.Clone(gobPayload), 0x7f), true},
		{"обрезанный gob", contract.LogCommand, gobPayload[:len(gobPayload)-1], false},
		{"не gob", contract.LogCommand, []byte{logDataVersion, logDataTagGob, 0xff, 0xff, 0xff}, false},
		{"пустой gob", contract.LogCommand, []byte{logDataVersion, logDataTagGob}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decodeLogData(tc.typ, tc.data)
			if err == nil {
				t.Fatalf("decodeLogData принял %x как %#v", tc.data, got)
			}
			if got != nil {
				t.Fatalf("вместе с ошибкой возвращено значение %#v", got)
			}
			if tc.corrupt && !errors.Is(err, errLogCorrupt) {
				t.Fatalf("ошибка %v не помечена как повреждение", err)
			}
		})
	}
}

// panicCommandBytes паникует при вызове метода: проверка нулевого носителя
// обязана сработать до вызова.
type panicCommandBytes struct {
	encoded []byte
}

func (c *panicCommandBytes) RaftCommandBytes() []byte {
	return c.encoded
}

// TestLogDataCarrierRejectsNilAndEmpty: нулевой носитель отвергается до
// вызова метода, пустое представление — до записи; ни один из них не
// становится тегом 0.
func TestLogDataCarrierRejectsNilAndEmpty(t *testing.T) {
	cases := []struct {
		name string
		data any
	}{
		{"нулевой указатель", (*panicCommandBytes)(nil)},
		{"пустое представление", &testCommandBytes{}},
		{"пустой, но не nil срез", &testCommandBytes{encoded: []byte{}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := encodeLogData(contract.LogCommand, tc.data)
			if err == nil {
				t.Fatalf("encodeLogData принял носитель: %x", encoded)
			}
			if _, err := encodeLogEntries([]contract.LogEntry{{Index: 1, Term: 1, Type: contract.LogCommand, Data: tc.data}}); err == nil {
				t.Fatal("encodeLogEntries принял носитель")
			}
		})
	}
}

// TestLogCommandBytesSkipGob: штатная запись носителя команды и непустых
// байтов команды идёт тегом 3 без gob — при добавлении, повторе суффикса,
// замене суффикса и полной перезаписи в FileStorage и MapStorage. Байты
// Data совпадают с готовым представлением носителя.
func TestLogCommandBytesSkipGob(t *testing.T) {
	carrier := func(s string) *testCommandBytes { return &testCommandBytes{encoded: []byte(s)} }
	first := []contract.LogEntry{
		{Index: 1, Term: 1, Type: contract.LogCommand, Data: carrier("c1")},
		{Index: 2, Term: 1, Type: contract.LogCommand, Data: carrier("c2")},
	}
	conflict := []contract.LogEntry{
		{Index: 2, Term: 2, Type: contract.LogCommand, Data: carrier("c2-new")},
		{Index: 3, Term: 2, Type: contract.LogCommand, Data: []byte("c3")},
	}

	before := _testCommandGobCalls.Load()
	fs := newTestFileStorage(t, t.TempDir())
	fs.RewriteLog(nil)
	fs.StoreLogEntries(1, first)
	if res := fs.StoreLogEntries(1, first); res.Writes != 0 || res.BytesWritten != 0 {
		t.Fatalf("повтор суффикса записал на диск: %+v", res)
	}
	if res := fs.StoreLogEntries(2, conflict); res.Writes != 1 {
		t.Fatalf("замена суффикса: %+v, want одну запись", res)
	}
	fs.RewriteLog(append(first[:1:1], conflict...))

	ms := NewMapStorage()
	ms.RewriteLog(nil)
	ms.StoreLogEntries(1, first)
	ms.StoreLogEntries(2, conflict)
	ms.RewriteLog(append(first[:1:1], conflict...))

	if calls := _testCommandGobCalls.Load() - before; calls != 0 {
		t.Fatalf("GobEncode носителя вызван %d раз на пути журнала", calls)
	}
	want := [][]byte{[]byte("c1"), []byte("c2-new"), []byte("c3")}
	for name, entries := range map[string][]encodedLogEntry{"file": fs.journal.entries, "map": ms.logEntries} {
		if len(entries) != len(want) {
			t.Fatalf("%s: %d записей, want %d", name, len(entries), len(want))
		}
		for i, e := range entries {
			wantData := append([]byte{logDataVersion, logDataTagCommand}, want[i]...)
			if !bytes.Equal(e.data, wantData) {
				t.Fatalf("%s: Data записи %d = %x, want %x", name, e.index, e.data, wantData)
			}
		}
	}
}

// newTestFileStorage открывает хранилище в dir штатными функциями
// ввода-вывода без log.Fatalf при загрузке.
func newTestFileStorage(t *testing.T, dir string) *FileStorage {
	t.Helper()
	fs := newFileStorage(dir, defaultWriteSeam(), defaultReadSeam())
	if err := fs.loadAll(); err != nil {
		t.Fatalf("loadAll: %v", err)
	}
	return fs
}

// tagSetEntries — записи со всеми тегами Data для сверки реализаций.
func tagSetEntries() []contract.LogEntry {
	cases := logDataTagCases()
	entries := make([]contract.LogEntry, len(cases))
	for i, tc := range cases {
		entries[i] = contract.LogEntry{Index: i + 1, Term: 1, Type: tc.typ, Data: tc.data}
	}
	return entries
}

// TestFileAndMapStorageSameTags (SA-006): один набор записей со всеми
// тегами даёт в FileStorage и MapStorage одинаковое кодированное
// представление и одинаковый результат LoadLog, в том числе после
// переоткрытия файла; произвольный зарегистрированный any сохраняет тип.
func TestFileAndMapStorageSameTags(t *testing.T) {
	registerLogCodecTestPayload()
	registerTestCommandBytes()
	entries := tagSetEntries()
	cases := logDataTagCases()

	dir := t.TempDir()
	fs := newTestFileStorage(t, dir)
	fs.RewriteLog(entries[:1])
	fs.StoreLogEntries(2, entries[1:])
	ms := NewMapStorage()
	ms.RewriteLog(entries[:1])
	ms.StoreLogEntries(2, entries[1:])

	if len(fs.journal.entries) != len(ms.logEntries) {
		t.Fatalf("число записей file=%d map=%d", len(fs.journal.entries), len(ms.logEntries))
	}
	for i := range ms.logEntries {
		f, m := fs.journal.entries[i], ms.logEntries[i]
		if f.index != m.index || f.term != m.term || f.typ != m.typ || !bytes.Equal(f.data, m.data) {
			t.Fatalf("запись %d: file %+v, map %+v", i, f, m)
		}
		if tag := dataTag(t, m.data); tag != cases[i].tag {
			t.Fatalf("%s: тег %d, want %d", cases[i].name, tag, cases[i].tag)
		}
	}

	reopened := newTestFileStorage(t, dir)
	for name, ls := range map[string]contract.LogStorage{"file": fs, "map": ms, "reopened": reopened} {
		got, err := ls.LoadLog()
		if err != nil {
			t.Fatalf("%s: LoadLog: %v", name, err)
		}
		if len(got) != len(cases) {
			t.Fatalf("%s: %d записей, want %d", name, len(got), len(cases))
		}
		for i := range got {
			if got[i].Index != entries[i].Index || got[i].Term != 1 || got[i].Type != entries[i].Type {
				t.Fatalf("%s: служебные поля записи %d = %+v", name, i, got[i])
			}
			requireSameData(t, got[i].Data, cases[i].want)
		}
	}
}

// TestRestoredCommandBytesMatchCarrier (SA-004): живой носитель и
// восстановленные из журнала байты той же команды дают побайтово одну Data.
func TestRestoredCommandBytesMatchCarrier(t *testing.T) {
	carrier := &testCommandBytes{encoded: []byte("KVC1\x01\x02-same")}
	live, err := encodeLogData(contract.LogCommand, carrier)
	if err != nil {
		t.Fatalf("encodeLogData(carrier): %v", err)
	}
	restored, err := decodeLogData(contract.LogCommand, live)
	if err != nil {
		t.Fatalf("decodeLogData: %v", err)
	}
	again, err := encodeLogData(contract.LogCommand, restored)
	if err != nil {
		t.Fatalf("encodeLogData(restored): %v", err)
	}
	if !bytes.Equal(live, again) {
		t.Fatalf("Data носителя %x и восстановленных байтов %x различаются", live, again)
	}
}

// fileState — содержимое и время изменения файла журнала.
type fileState struct {
	sum   [sha256.Size]byte
	mtime int64
	size  int64
}

func readFileState(t *testing.T, path string) fileState {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("чтение %s: %v", path, err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return fileState{sum: sha256.Sum256(raw), mtime: info.ModTime().UnixNano(), size: info.Size()}
}

// TestRestartedSuffixIsNoop (AC4, SA-004): ведомый с целым журналом получает
// от перезапущенного лидера тот же суффикс уже как восстановленные []byte:
// Writes=0, BytesWritten=0, файл не изменён. Реальное изменение байта,
// терма или типа записи совпадением не считается и переписывает журнал.
func TestRestartedSuffixIsNoop(t *testing.T) {
	carrier := func(s string) *testCommandBytes { return &testCommandBytes{encoded: []byte(s)} }
	live := []contract.LogEntry{
		{Index: 0, Term: 0, Type: contract.LogNoop, Data: nil},
		{Index: 1, Term: 1, Type: contract.LogConfiguration, Data: []byte{1, 2}},
		{Index: 2, Term: 1, Type: contract.LogCommand, Data: carrier("cmd-2")},
		{Index: 3, Term: 1, Type: contract.LogCommand, Data: carrier("cmd-3")},
	}

	// Лидер пишет живые носители и перезапускается.
	leaderDir := t.TempDir()
	leader := newTestFileStorage(t, leaderDir)
	leader.RewriteLog(live)
	restored, err := newTestFileStorage(t, leaderDir).LoadLog()
	if err != nil {
		t.Fatalf("LoadLog лидера: %v", err)
	}
	if _, ok := restored[2].Data.([]byte); !ok {
		t.Fatalf("восстановленная команда имеет тип %T, want []byte", restored[2].Data)
	}

	// Ведомый получил те же записи живыми носителями.
	followerDir := t.TempDir()
	path := filepath.Join(followerDir, _logFileName)
	follower := newTestFileStorage(t, followerDir)
	follower.RewriteLog(live[:1])
	follower.StoreLogEntries(1, live[1:])
	state := readFileState(t, path)

	for from := 1; from <= 3; from++ {
		res := follower.StoreLogEntries(from, restored[from:])
		if res.Writes != 0 || res.BytesWritten != 0 {
			t.Fatalf("повтор суффикса с %d: %+v, want Writes=0 BytesWritten=0", from, res)
		}
	}
	if got := readFileState(t, path); got != state {
		t.Fatal("повтор совпадающего суффикса изменил файл ведомого")
	}

	// LoadLog → StoreLogEntries того же хранилища — тоже no-op.
	own, err := follower.LoadLog()
	if err != nil {
		t.Fatalf("LoadLog ведомого: %v", err)
	}
	if res := follower.StoreLogEntries(0, own); res.Writes != 0 || res.BytesWritten != 0 {
		t.Fatalf("LoadLog→StoreLogEntries: %+v, want no-op", res)
	}

	changes := []struct {
		name   string
		mutate func(e *contract.LogEntry)
	}{
		{"байт команды", func(e *contract.LogEntry) { e.Data = []byte("cmd-X") }},
		{"терм", func(e *contract.LogEntry) { e.Term = 2 }},
		{"тип", func(e *contract.LogEntry) { e.Type = contract.LogNoop }},
	}
	for _, ch := range changes {
		t.Run(ch.name, func(t *testing.T) {
			dir := t.TempDir()
			fs := newTestFileStorage(t, dir)
			fs.RewriteLog(live)
			before := readFileState(t, filepath.Join(dir, _logFileName))
			suffix := slices.Clone(restored[3:])
			ch.mutate(&suffix[0])
			if res := fs.StoreLogEntries(3, suffix); res.Writes != 1 || res.BytesWritten == 0 {
				t.Fatalf("изменённый суффикс: %+v, want перезапись", res)
			}
			if readFileState(t, filepath.Join(dir, _logFileName)).sum == before.sum {
				t.Fatal("изменённый суффикс не изменил файл")
			}
			got, err := newTestFileStorage(t, dir).LoadLog()
			if err != nil {
				t.Fatalf("LoadLog: %v", err)
			}
			if !reflect.DeepEqual(got[3], suffix[0]) {
				t.Fatalf("запись после перезаписи = %#v, want %#v", got[3], suffix[0])
			}
		})
	}
}

// TestEncodedSuffixEqualDetectsEveryField: точное сравнение суффикса
// различает индекс, терм, тип и каждый байт Data.
func TestEncodedSuffixEqualDetectsEveryField(t *testing.T) {
	base, err := encodeLogEntries([]contract.LogEntry{
		{Index: 5, Term: 2, Type: contract.LogCommand, Data: []byte("abc")},
	})
	if err != nil {
		t.Fatalf("encodeLogEntries: %v", err)
	}
	if !encodedSuffixEqual(base, 0, []encodedLogEntry{{index: 5, term: 2, typ: contract.LogCommand, data: slices.Clone(base[0].data)}}) {
		t.Fatal("одинаковые записи не признаны совпадающими")
	}
	changes := map[string]encodedLogEntry{
		"индекс": {index: 6, term: 2, typ: contract.LogCommand, data: base[0].data},
		"терм":   {index: 5, term: 3, typ: contract.LogCommand, data: base[0].data},
		"тип":    {index: 5, term: 2, typ: contract.LogNoop, data: base[0].data},
	}
	for i := range base[0].data {
		data := slices.Clone(base[0].data)
		data[i] ^= 0x01
		changes["байт "+strconv.Itoa(i)] = encodedLogEntry{index: 5, term: 2, typ: contract.LogCommand, data: data}
	}
	for name, proposed := range changes {
		if encodedSuffixEqual(base, 0, []encodedLogEntry{proposed}) {
			t.Fatalf("изменение (%s) признано совпадением", name)
		}
	}
}

// TestMapStorageLogMemoryIsIndependent: MapStorage не делит память с
// записанными и загруженными срезами; LoadLog→StoreLogEntries оставляет
// журнал прежним.
func TestMapStorageLogMemoryIsIndependent(t *testing.T) {
	source := []byte("command")
	ms := NewMapStorage()
	ms.RewriteLog([]contract.LogEntry{{Index: 1, Term: 1, Type: contract.LogCommand, Data: source}})
	source[0] = 'X'

	loaded, err := ms.LoadLog()
	if err != nil {
		t.Fatalf("LoadLog: %v", err)
	}
	ms.StoreLogEntries(1, loaded)
	loaded[0].Data.([]byte)[1] = 'Y'

	again, err := ms.LoadLog()
	if err != nil {
		t.Fatalf("LoadLog: %v", err)
	}
	if got := again[0].Data.([]byte); string(got) != "command" {
		t.Fatalf("журнал MapStorage = %q, want command", got)
	}
}

// TestLoadAllRejectsLogV2Unchanged (AC1): реальный журнал версии 2 сборки C
// отвергается до применения записей; файл не изменяется ни по байтам, ни
// по времени изменения.
func TestLoadAllRejectsLogV2Unchanged(t *testing.T) {
	dir := t.TempDir()
	writeDatFile(t, dir, _logFileName, v2LogFrame(t))
	path := filepath.Join(dir, _logFileName)
	before := readFileState(t, path)

	fs := newFileStorage(dir, defaultWriteSeam(), defaultReadSeam())
	err := fs.loadAll()
	if err == nil {
		t.Fatal("loadAll принял журнал версии 2")
	}
	if !errors.Is(err, errLogCorrupt) || !strings.Contains(err.Error(), "unsupported log version 2") {
		t.Fatalf("ошибка %v не описывает версию 2", err)
	}
	if fs.journal.exists || len(fs.journal.entries) != 0 {
		t.Fatalf("после отказа журнал загружен: %+v", fs.journal)
	}
	if got := readFileState(t, path); got != before {
		t.Fatal("отказ версии 2 изменил файл")
	}
}

// TestLoadAllRejectsUnknownLogVersions: версии 0, 1, 2, 4 и 0xffff с
// целой CRC заголовка отвергаются; поддерживается только 3.
func TestLoadAllRejectsUnknownLogVersions(t *testing.T) {
	file := mustEncodeFile(t, []contract.LogEntry{{Index: 0, Term: 0, Type: contract.LogNoop}})
	for _, version := range []uint16{0, 1, 2, 4, 0xffff} {
		bad := slices.Clone(file)
		binary.BigEndian.PutUint16(bad[8:10], version)
		binary.BigEndian.PutUint32(bad[20:24], crc32.Checksum(bad[0:20], _crc32c))
		_, _, err := decodeLogFile(bad)
		requireCorruption(t, err)
		if !strings.Contains(err.Error(), "unsupported log version") {
			t.Fatalf("версия %d: ошибка %v", version, err)
		}
	}
}

// TestLogV2BatchInV3FileRejected: пакет с сигнатурой и маркером версии 2
// после заголовка версии 3 — повреждение, а не fallback к старому формату.
func TestLogV2BatchInV3FileRejected(t *testing.T) {
	v2 := v2LogFrame(t)
	header := encodeLogFileHeader()
	mixed := append(header[:], v2[logFileHeaderSize:]...)
	_, _, err := decodeLogFile(mixed)
	requireCorruption(t, err)
}

// TestNewFileStorageFatalOnLogV2: публичный конструктор на журнале версии 2
// завершает процесс с сообщением о версии и не изменяет файл.
func TestNewFileStorageFatalOnLogV2(t *testing.T) {
	if os.Getenv("STORE_LOG_V2_HELPER") == "1" {
		NewFileStorage(os.Getenv("STORE_LOG_V2_DIR"))
		return
	}
	dir := t.TempDir()
	writeDatFile(t, dir, _logFileName, v2LogFrame(t))
	path := filepath.Join(dir, _logFileName)
	before := readFileState(t, path)

	cmd := exec.Command(os.Args[0], "-test.run=^TestNewFileStorageFatalOnLogV2$")
	cmd.Env = append(os.Environ(), "STORE_LOG_V2_HELPER=1", "STORE_LOG_V2_DIR="+dir)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("дочерний процесс завершился успешно, want отказ:\n%s", out)
	}
	if !strings.Contains(string(out), "unsupported log version 2") {
		t.Fatalf("сообщение не описывает версию 2:\n%s", out)
	}
	if got := readFileState(t, path); got != before {
		t.Fatal("отказ версии 2 изменил файл")
	}
}

// rawLogFile собирает файл версии 3 из одного пакета с записями заданного
// типа и произвольной Data; CRC согласованы.
func rawLogFile(t *testing.T, typ contract.LogType, data ...[]byte) []byte {
	t.Helper()
	var body []byte
	for i, d := range data {
		body = append(body, buildEntryRecord(i, 1, typ, uint64(len(d)))...)
		body = append(body, d...)
	}
	header := encodeLogFileHeader()
	return append(header[:], buildLogPacket(t, body, uint64(len(data)))...)
}

// TestLoadLogRejectsBadDataTags (AC5): структурно целый пакет с Data,
// нарушающей теги, открывается (Data декодируется отложенно, как в
// версии 2), но LoadLog возвращает ошибку повреждения и ни одной записи;
// неизвестное значение не превращается в nil-команду.
func TestLoadLogRejectsBadDataTags(t *testing.T) {
	cases := []struct {
		name string
		typ  contract.LogType
		data []byte
	}{
		{"неканонический тег 2 у команды", contract.LogCommand, []byte{logDataVersion, logDataTagBytes, 'x'}},
		{"неизвестный тег", contract.LogCommand, []byte{logDataVersion, 9}},
		{"неизвестная версия Data", contract.LogCommand, []byte{7, logDataTagNil}},
		{"пустой тег 3", contract.LogCommand, []byte{logDataVersion, logDataTagCommand}},
		{"тег 3 у конфигурации", contract.LogConfiguration, []byte{logDataVersion, logDataTagCommand, 1}},
		{"Data из одного байта", contract.LogCommand, []byte{logDataVersion}},
		{"пустая Data", contract.LogCommand, []byte{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			file := rawLogFile(t, tc.typ, []byte{logDataVersion, logDataTagNil}, tc.data)
			writeDatFile(t, dir, _logFileName, file)
			fs := newTestFileStorage(t, dir)
			got, err := fs.LoadLog()
			if err == nil || !errors.Is(err, errLogCorrupt) {
				t.Fatalf("LoadLog = %#v, %v; want ошибку повреждения", got, err)
			}
			if got != nil {
				t.Fatalf("вместе с ошибкой возвращены записи %#v", got)
			}
			_, _, err = decodeLogFile(file)
			requireCorruption(t, err)
		})
	}
}

// TestLogV3CorruptTagByteFailsCRC: порча байта тега в записанном файле без
// пересчёта CRC отвергается при загрузке как повреждение пакета.
func TestLogV3CorruptTagByteFailsCRC(t *testing.T) {
	dir := t.TempDir()
	fs := newTestFileStorage(t, dir)
	fs.RewriteLog([]contract.LogEntry{{Index: 1, Term: 1, Type: contract.LogCommand, Data: []byte("cmd")}})
	raw := readDatFile(t, dir, _logKey)
	tagOffset := logFileHeaderSize + logBatchHeaderSize + logEntryHeaderSize + 1
	if raw[tagOffset] != logDataTagCommand {
		t.Fatalf("байт %d = %d, want тег 3", tagOffset, raw[tagOffset])
	}
	raw[tagOffset] = logDataTagBytes
	writeDatFile(t, dir, _logFileName, raw)
	if err := loadAllWithDefaultSeams(dir); err == nil || !errors.Is(err, errLogCorrupt) {
		t.Fatalf("loadAll = %v, want повреждение", err)
	}
}
