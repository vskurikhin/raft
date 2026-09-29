package protocol

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/vskurikhin/raft/pkg/raft/contract"
)

// registeredPoint — сторонний зарегистрированный тип Data.
type registeredPoint struct {
	X, Y  int
	Label string
}

// upperCodec — Data с собственными GobEncode/GobDecode.
type upperCodec struct{ value string }

func (u upperCodec) GobEncode() ([]byte, error) { return []byte(strings.ToUpper(u.value)), nil }

func (u *upperCodec) GobDecode(b []byte) error {
	u.value = strings.ToLower(string(b))

	return nil
}

var errCodecFailure = errors.New("codec failure")

// failingDecoder кодируется, но отказывает при декодировании.
type failingDecoder struct{}

func (failingDecoder) GobEncode() ([]byte, error) { return []byte{1}, nil }
func (*failingDecoder) GobDecode([]byte) error    { return errCodecFailure }

// failingEncoder отказывает при кодировании.
type failingEncoder struct{}

func (failingEncoder) GobEncode() ([]byte, error) { return nil, errCodecFailure }
func (*failingEncoder) GobDecode([]byte) error    { return nil }

// unregisteredData не регистрируется в gob.
type unregisteredData struct{ A int }

// unregisteredGob — замороженный независимый gob-поток struct{ Data any } со
// значением типа, зарегистрированного под именем
// "protocol.test/unregistered.Type", которое в тестовом процессе неизвестно.
const unregisteredGob = "147f030102ff80000101010444617461011000000043ff80011f70726f746f636f6c2e746573742f756e72" +
	"6567697374657265642e54797065ff810301010c556e7265676973746572656401ff82000101010141010400000007ff820301020000"

// stringGob — замороженный независимый gob-поток struct{ Data any }{"hi"}.
const stringGob = "147f030102ff80000101010444617461011000000011ff800106737472696e670c040002686900"

func registerTestTypes() {
	gob.Register(registeredPoint{})
	gob.Register(upperCodec{})
	gob.Register(failingDecoder{})
	gob.Register(failingEncoder{})
}

func roundTripData(t *testing.T, data any) any {
	t.Helper()

	args := &contract.AppendEntriesArgs{
		RPCHeader: header3(1), Entries: []contract.LogEntry{{Index: 1, Term: 1, Data: data}},
	}
	frame, err := AppendRequest(nil, args, DefaultLimits())
	if err != nil {
		t.Fatalf("AppendRequest %T: %v", data, err)
	}
	_, command, err := readRequestBytes(frame, DefaultLimits())
	if err != nil {
		t.Fatalf("ReadRequest %T: %v", data, err)
	}

	return command.(*contract.AppendEntriesArgs).Entries[0].Data //nolint:forcetypeassert // err == nil
}

// TestDataRoundTrip — V10: конкретный тип Data сохраняется: встроенные
// типы, пустые значения, зарегистрированный сторонний тип, GobEncoder и nil.
func TestDataRoundTrip(t *testing.T) {
	registerTestTypes()
	values := []any{
		"hello", "", 42, 0, -5, []byte{1, 2, 3}, []byte{},
		registeredPoint{X: 1, Y: -2, Label: "p"}, registeredPoint{},
		upperCodec{value: "abc"}, nil,
	}
	for _, value := range values {
		got := roundTripData(t, value)
		if bytes, ok := value.([]byte); ok && len(bytes) == 0 {
			if decoded, ok := got.([]byte); !ok || len(decoded) != 0 {
				t.Fatalf("empty []byte decoded as %#v", got)
			}

			continue
		}
		if !reflect.DeepEqual(got, value) {
			t.Fatalf("Data %#v decoded as %#v", value, got)
		}
	}
}

// TestDataTypedNil — V10: typed nil, допустимый для gob, кодируется видом
// gob и не превращается в nil; неподдерживаемый typed nil pointer — ErrData.
func TestDataTypedNil(t *testing.T) {
	registerTestTypes()
	var nilSlice []byte
	encoded, err := EncodeData(nilSlice, DefaultLimits().MaxDataBytes)
	if err != nil || len(encoded) == 0 {
		t.Fatalf("typed nil slice: %d bytes, %v", len(encoded), err)
	}
	if got, ok := roundTripData(t, nilSlice).([]byte); !ok || len(got) != 0 {
		t.Fatalf("typed nil slice decoded as %#v", got)
	}

	var nilPointer *registeredPoint
	_, err = EncodeData(nilPointer, DefaultLimits().MaxDataBytes)
	requireIs(t, err, ErrData)
}

// TestDataErrors — V10: незарегистрированный тип, ошибка пользовательского
// кодировщика и декодировщика, неизвестная регистрация — ErrData; RPC не
// возвращается даже частично.
func TestDataErrors(t *testing.T) {
	registerTestTypes()
	for _, data := range []any{unregisteredData{A: 1}, failingEncoder{}} {
		args := &contract.AppendEntriesArgs{RPCHeader: header3(1), Entries: []contract.LogEntry{{Data: data}}}
		frame, err := AppendRequest([]byte("p"), args, DefaultLimits())
		requireIs(t, err, ErrData)
		if string(frame) != "p" {
			t.Fatalf("%T: frame %q", data, frame)
		}
	}

	failing, err := EncodeData(failingDecoder{}, DefaultLimits().MaxDataBytes)
	if err != nil {
		t.Fatalf("encode failingDecoder: %v", err)
	}
	for name, payload := range map[string][]byte{
		"GobDecode error":     failing,
		"unknown type name":   mustHex(t, unregisteredGob),
		"trailing byte":       append(mustHex(t, stringGob), 0),
		"second gob value":    append(mustHex(t, stringGob), mustHex(t, stringGob)...),
		"truncated gob value": mustHex(t, stringGob)[:20],
	} {
		_, command, err := readRequestBytes(singleEntryFrame(payload, contract.LogCommand), DefaultLimits())
		requireIs(t, err, ErrData)
		if command != nil {
			t.Fatalf("%s: command %v", name, command)
		}
	}
}

// TestEncodeMeasureData — V27 (чистая часть): MeasureData равна длине
// EncodeData для разных типов и после регистрации многих других gob-типов;
// maxBytes=0 — ErrLimit, в том числе для nil; настоящий nil — длина 0.
func TestEncodeMeasureData(t *testing.T) {
	registerTestTypes()
	values := []any{"s", []byte("bytes"), 7, registeredPoint{X: 1}, upperCodec{value: "x"}, nil}
	check := func(stage string) {
		for _, value := range values {
			encoded, err := EncodeData(value, 8192)
			if err != nil {
				t.Fatalf("%s EncodeData %T: %v", stage, value, err)
			}
			measured, err := MeasureData(value, 8192)
			if err != nil || measured != uint64(len(encoded)) {
				t.Fatalf("%s MeasureData %T = %d, %v; want %d", stage, value, measured, err, len(encoded))
			}
		}
	}
	check("fresh")
	registerManyTypes(t, 140)
	check("after registrations")

	for _, value := range []any{nil, "s"} {
		_, err := EncodeData(value, 0)
		requireIs(t, err, ErrLimit)
		_, err = MeasureData(value, 0)
		requireIs(t, err, ErrLimit)
	}

	encoded, err := EncodeData(nil, 1)
	if err != nil || encoded != nil {
		t.Fatalf("nil data: %v, %v", encoded, err)
	}
	if n, err := MeasureData(nil, 1); err != nil || n != 0 {
		t.Fatalf("nil measure: %d, %v", n, err)
	}

	exact, err := MeasureData("limit", 8192)
	if err != nil {
		t.Fatalf("measure: %v", err)
	}
	if _, err = EncodeData("limit", exact); err != nil {
		t.Fatalf("exact limit: %v", err)
	}
	_, err = EncodeData("limit", exact-1)
	requireIs(t, err, ErrLimit)
	_, err = MeasureData("limit", exact-1)
	requireIs(t, err, ErrLimit)
}

// registerManyTypes кодирует count различных структурных типов, продвигая
// номера типов gob процесса.
func registerManyTypes(t *testing.T, count int) {
	t.Helper()

	for i := range count {
		fields := []reflect.StructField{{Name: fmt.Sprintf("Field%dN%d", i, count), Type: reflect.TypeFor[int]()}}
		value := reflect.New(reflect.StructOf(fields)).Elem().Interface()
		if err := gob.NewEncoder(io.Discard).Encode(value); err != nil {
			t.Fatalf("encode generated type %d: %v", i, err)
		}
	}
}

// TestDecodedDataOwnership — V11: результат декодирования не ссылается на
// входной буфер; повторное декодирование и изменение входа не меняют
// прежний результат; кодирование не меняет Data вызывающего.
func TestDecodedDataOwnership(t *testing.T) {
	payload := []byte{1, 2, 3, 4}
	args := &contract.AppendEntriesArgs{RPCHeader: header3(1), Entries: []contract.LogEntry{
		{Index: 1, Term: 1, Type: contract.LogConfiguration, Data: payload},
	}}
	frame, err := AppendRequest(nil, args, DefaultLimits())
	if err != nil {
		t.Fatalf("AppendRequest: %v", err)
	}
	if !bytes.Equal(payload, []byte{1, 2, 3, 4}) {
		t.Fatalf("encode changed caller Data: %v", payload)
	}

	_, first, err := readRequestBytes(frame, DefaultLimits())
	if err != nil {
		t.Fatalf("first decode: %v", err)
	}
	_, second, err := readRequestBytes(frame, DefaultLimits())
	if err != nil {
		t.Fatalf("second decode: %v", err)
	}
	firstData := first.(*contract.AppendEntriesArgs).Entries[0].Data.([]byte)   //nolint:forcetypeassert // проверено
	secondData := second.(*contract.AppendEntriesArgs).Entries[0].Data.([]byte) //nolint:forcetypeassert // проверено

	firstData[0] = 99
	clear(frame)
	if !bytes.Equal(secondData, []byte{1, 2, 3, 4}) || firstData[1] != 2 {
		t.Fatalf("decoded data aliased: first=%v second=%v", firstData, secondData)
	}

	snapshotFrame := loadGolden(t, "is_request")
	_, request, err := readRequestBytes(snapshotFrame, DefaultLimits())
	if err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	clear(snapshotFrame)
	if got := request.(*contract.InstallSnapshotRequest).Configuration; string(got) != "abc" { //nolint:forcetypeassert,lll // проверено
		t.Fatalf("configuration aliased frame: %q", got)
	}
}

// TestAppendPreservesPrefix — V11: Append* дописывают кадр к dst, не меняя
// префикс; при ошибке возвращают dst[:len(dst)] с прежним содержимым.
func TestAppendPreservesPrefix(t *testing.T) {
	dst := make([]byte, 3, 1024)
	copy(dst, "abc")
	frame, err := AppendRequest(dst, &contract.TimeoutNowRequest{RPCHeader: header3(1)}, DefaultLimits())
	if err != nil || !bytes.Equal(frame[:3], []byte("abc")) || !bytes.Equal(frame[3:], loadGolden(t, "tn_request")) {
		t.Fatalf("frame %x, %v", frame, err)
	}

	frame, err = AppendResponse(dst, rpcTimeoutNow, contract.RPCResponse{}, DefaultLimits())
	requireIs(t, err, ErrFormat)
	if !bytes.Equal(frame, []byte("abc")) || len(frame) != 3 {
		t.Fatalf("error result %q", frame)
	}
}

// TestAllocationProfile — V11: профиль выделений декодирования и кодирования
// коротких сообщений ограничен; кодек не удерживает буферы между вызовами.
func TestAllocationProfile(t *testing.T) {
	frame := loadGolden(t, "rv_request")
	reader := bytes.NewReader(frame)
	decodeAllocs := testing.AllocsPerRun(100, func() {
		reader.Reset(frame)
		if _, _, err := ReadRequest(reader, DefaultLimits()); err != nil {
			t.Fatal(err)
		}
	})
	args := &contract.RequestVoteArgs{RPCHeader: header3(1), Term: 1}
	encodeAllocs := testing.AllocsPerRun(100, func() {
		if _, err := AppendRequest(nil, args, DefaultLimits()); err != nil {
			t.Fatal(err)
		}
	})
	t.Logf("RequestVote decode allocs=%v encode allocs=%v", decodeAllocs, encodeAllocs)
	if decodeAllocs > 4 || encodeAllocs > 2 {
		t.Fatalf("allocations decode=%v encode=%v", decodeAllocs, encodeAllocs)
	}
}

// TestSnapshotConfigurationOwnsMemory — V11: Configuration декодированного
// InstallSnapshot не ссылается на тело кадра. Тело собирается так же, как в
// readFrame, — отдельный буфер длины BodyLength с копией тела независимого
// эталона после проверок пределов и заголовка, которые ReadRequestWithHeader
// выполняет до decodeRequest; тот же кадр через ReadRequest даёт равный запрос.
func TestSnapshotConfigurationOwnsMemory(t *testing.T) {
	limits := DefaultLimits()
	frame := loadGolden(t, "is_request")
	if err := checkLimits(limits); err != nil {
		t.Fatalf("limits: %v", err)
	}
	header, err := parseHeader((*[headerSize]byte)(frame[:headerSize]))
	if err != nil {
		t.Fatalf("header: %v", err)
	}
	if err = requestCheck(limits)(header); err != nil || header.Type != rpcInstallSnapshot ||
		header.BodyLength != uint64(len(frame)-headerSize) {
		t.Fatalf("header %+v: %v", header, err)
	}
	body := make([]byte, header.BodyLength)
	copy(body, frame[headerSize:])

	decoded, err := decodeInstallSnapshotRequest(&bodyDecoder{body: body}, limits)
	if err != nil {
		t.Fatalf("decode body: %v", err)
	}
	_, public, err := readRequestBytes(frame, limits)
	if err != nil || !reflect.DeepEqual(public, decoded) {
		t.Fatalf("ReadRequest %#v, %v; body decode %#v", public, err, decoded)
	}

	clear(body)
	if string(decoded.Configuration) != "abc" {
		t.Fatalf("configuration aliased frame body: %q", decoded.Configuration)
	}
}

// TestAppendRequestErrorRestoresLength — V07/V13: ошибка кодирования второй
// записи AppendEntries при dst длины 3 и ёмкости 1024 возвращает срез длины 3
// с прежним префиксом. Содержимое свободной ёмкости dst не проверяется.
func TestAppendRequestErrorRestoresLength(t *testing.T) {
	dst := make([]byte, 3, 1024)
	copy(dst, "abc")
	args := &contract.AppendEntriesArgs{RPCHeader: header3(1), Term: 1, Entries: []contract.LogEntry{
		{Index: 1, Term: 1, Type: contract.LogCommand, Data: "first"},
		{Index: 2, Term: 1, Type: maxLogType + 1},
	}}
	frame, err := AppendRequest(dst, args, DefaultLimits())
	requireIs(t, err, ErrFormat)
	if len(frame) != 3 || !bytes.Equal(frame, []byte("abc")) {
		t.Fatalf("error result len=%d %q", len(frame), frame)
	}
}
