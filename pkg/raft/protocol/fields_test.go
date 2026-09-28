package protocol

import (
	"math"
	"reflect"
	"testing"

	"github.com/vskurikhin/raft/pkg/raft/contract"
)

// Смещения полей в эталонных кадрах.
const (
	requestBody  = headerSize
	responseBody = headerSize + errorEnvelopeBytes
)

// TestProtocolVersionDecode — V03: ProtocolVersion всех 10 сообщений: 3
// допустимо, 0/2/4/−1 несовместимы. Запрос возвращает надёжный RPCType и
// contract.ErrUnsupportedProtocol без команды; ответ — ошибку вторым
// результатом.
func TestProtocolVersionDecode(t *testing.T) {
	versions := []int64{0, 2, 4, -1}
	for _, tc := range goldenRequests() {
		for _, version := range versions {
			frame := patchUint64(loadGolden(t, tc.name), requestBody, uint64(version))
			typ, command, err := readRequestBytes(frame, DefaultLimits())
			requireIs(t, err, contract.ErrUnsupportedProtocol)
			if typ != tc.typ || command != nil {
				t.Fatalf("%s PV %d: typ=%d command=%v", tc.name, version, typ, command)
			}
		}
	}
	for _, tc := range goldenResponses() {
		if tc.response.Error != nil {
			continue
		}
		for _, version := range versions {
			frame := patchUint64(loadGolden(t, tc.name), responseBody, uint64(version))
			response, err := readResponseBytes(frame, tc.typ, DefaultLimits())
			requireIs(t, err, contract.ErrUnsupportedProtocol)
			if response != (contract.RPCResponse{}) {
				t.Fatalf("%s PV %d: response=%#v", tc.name, version, response)
			}
		}
	}
}

// TestProtocolVersionEncode — V03: кодировщик отвергает версию протокола,
// отличную от 3, у всех 10 сообщений до записи кадра.
func TestProtocolVersionEncode(t *testing.T) {
	for _, version := range []int{0, 2, 4, -1} {
		for _, tc := range goldenRequests() {
			command := withProtocolVersion(t, tc.command, version)
			frame, err := AppendRequest([]byte("prefix"), command, DefaultLimits())
			requireIs(t, err, contract.ErrUnsupportedProtocol)
			if string(frame) != "prefix" {
				t.Fatalf("%s: frame %q", tc.name, frame)
			}
		}
		for _, tc := range goldenResponses() {
			if tc.response.Error != nil {
				continue
			}
			reply := withProtocolVersion(t, tc.response.Reply, version)
			frame, err := AppendResponse(nil, tc.typ, contract.RPCResponse{Reply: reply}, DefaultLimits())
			requireIs(t, err, contract.ErrUnsupportedProtocol)
			if len(frame) != 0 {
				t.Fatalf("%s: frame %x", tc.name, frame)
			}
		}
	}
}

// withProtocolVersion возвращает копию сообщения-указателя с заданной версией.
func withProtocolVersion(t *testing.T, message any, version int) any {
	t.Helper()

	value := reflect.ValueOf(message).Elem()
	clone := reflect.New(value.Type())
	clone.Elem().Set(value)
	header := clone.Elem().FieldByName("RPCHeader")
	if !header.IsValid() {
		t.Fatalf("%T has no RPCHeader", message)
	}
	header.FieldByName("ProtocolVersion").SetInt(int64(version))

	return clone.Interface()
}

type rangeField struct {
	golden   string
	offset   int
	response bool
	name     string
	valid    intRange
}

func decodeRangeFields() []rangeField {
	return []rangeField{
		{"rv_request", requestBody + 8, false, "ServerID", rangeAnyInt},
		{"rv_request", requestBody + 16, false, "Term", rangeNonNegative},
		{"rv_request", requestBody + 24, false, "CandidateID", rangeAnyInt},
		{"rv_request", requestBody + 32, false, "LastLogIndex", rangeMinusOne},
		{"rv_request", requestBody + 40, false, "LastLogTerm", rangeMinusOne},
		{"pv_request", requestBody + 16, false, "Term", rangeNonNegative},
		{"pv_request", requestBody + 24, false, "LastLogIndex", rangeMinusOne},
		{"pv_request", requestBody + 32, false, "LastLogTerm", rangeMinusOne},
		{"tn_request", requestBody + 8, false, "ServerID", rangeAnyInt},
		{"is_request", requestBody + 16, false, "Term", rangeNonNegative},
		{"is_request", requestBody + 24, false, "LeaderID", rangeAnyInt},
		{"is_request", requestBody + 32, false, "LastLogIndex", rangeNonNegative},
		{"is_request", requestBody + 40, false, "LastLogTerm", rangeNonNegative},
		{"is_request", requestBody + 48, false, "ConfigIndex", rangeMinusOne},
		{"ae_request_noop", requestBody + 16, false, "Term", rangeNonNegative},
		{"ae_request_noop", requestBody + 24, false, "LeaderID", rangeAnyInt},
		{"ae_request_noop", requestBody + 32, false, "PrevLogIndex", rangeMinusOne},
		{"ae_request_noop", requestBody + 40, false, "PrevLogTerm", rangeMinusOne},
		{"ae_request_noop", requestBody + 48, false, "LeaderCommit", rangeMinusOne},
		{"ae_request_noop", requestBody + 64, false, "LogEntry.Index", rangeNonNegative},
		{"ae_request_noop", requestBody + 72, false, "LogEntry.Term", rangeNonNegative},
		{"ae_reply", responseBody + 8, true, "ServerID", rangeAnyInt},
		{"ae_reply", responseBody + 16, true, "Term", rangeNonNegative},
		{"ae_reply", responseBody + 25, true, "ConflictIndex", rangeNonNegative},
		{"ae_reply", responseBody + 33, true, "ConflictTerm", rangeMinusOne},
		{"rv_reply", responseBody + 16, true, "Term", rangeNonNegative},
		{"is_reply", responseBody + 16, true, "Term", rangeNonNegative},
		{"tn_reply", responseBody + 17, true, "Term", rangeNonNegative},
		{"pv_reply", responseBody + 16, true, "Term", rangeNonNegative},
	}
}

func boundaryValues(valid intRange) []int64 {
	values := []int64{math.MinInt64, -2, -1, 0, 1, math.MaxInt64 - 1, math.MaxInt64}
	for _, edge := range []int64{valid.low, valid.high} {
		values = append(values, edge)
		if edge > math.MinInt64 {
			values = append(values, edge-1)
		}
		if edge < math.MaxInt64 {
			values = append(values, edge+1)
		}
	}

	return values
}

// TestIntegerRangesDecode — V04: каждое целое поле §6 декодером проверяется
// на границах min−1/min/min+1, −2/−1/0/1, I−1/I/I+1; допустимое значение
// сохраняется, остальное — ErrRange.
func TestIntegerRangesDecode(t *testing.T) {
	for _, field := range decodeRangeFields() {
		frame := loadGolden(t, field.golden)
		typ := RPCType(frame[offsetRPCType])
		for _, value := range boundaryValues(field.valid) {
			patched := patchUint64(frame, field.offset, uint64(value))
			var (
				decoded any
				err     error
			)
			if field.response {
				var response contract.RPCResponse
				response, err = readResponseBytes(patched, typ, DefaultLimits())
				decoded = response.Reply
			} else {
				_, decoded, err = readRequestBytes(patched, DefaultLimits())
			}
			if value < field.valid.low || value > field.valid.high {
				requireIs(t, err, ErrRange)

				continue
			}
			if err != nil {
				t.Fatalf("%s %s=%d: %v", field.golden, field.name, value, err)
			}
			if got := findIntField(decoded, field.name); got != value {
				t.Fatalf("%s %s=%d decoded as %d", field.golden, field.name, value, got)
			}
		}
	}
}

// findIntField находит значение целого поля по имени в сообщении или первой
// записи журнала.
func findIntField(message any, name string) int64 {
	value := reflect.ValueOf(message).Elem()
	if entryField, ok := cutPrefix(name, "LogEntry."); ok {
		return value.FieldByName("Entries").Index(0).FieldByName(entryField).Int()
	}

	return value.FieldByName(name).Int()
}

func cutPrefix(s, prefix string) (string, bool) {
	if len(s) >= len(prefix) && s[:len(prefix)] == prefix {
		return s[len(prefix):], true
	}

	return s, false
}

// TestSnapshotDataSizeRange — V04: DataSize — 1…1 GiB с обеих сторон.
func TestSnapshotDataSizeRange(t *testing.T) {
	frame := loadGolden(t, "is_request")
	const offset = requestBody + 56
	for _, value := range []int64{math.MinInt64, -1, 0, 1, 262144, maxSnapshotDataSize, maxSnapshotDataSize + 1,
		math.MaxInt64} {
		valid := value >= 1 && value <= maxSnapshotDataSize

		_, command, err := readRequestBytes(patchUint64(frame, offset, uint64(value)), DefaultLimits())
		request := &contract.InstallSnapshotRequest{
			RPCHeader: header3(1), Term: 1, LastLogIndex: 1, LastLogTerm: 1, DataSize: value,
		}
		_, encodeErr := AppendRequest(nil, request, DefaultLimits())
		if !valid {
			requireIs(t, err, ErrRange)
			requireIs(t, encodeErr, ErrRange)

			continue
		}
		if err != nil || encodeErr != nil {
			t.Fatalf("DataSize %d: decode %v, encode %v", value, err, encodeErr)
		}
		if got := command.(*contract.InstallSnapshotRequest).DataSize; got != value { //nolint:forcetypeassert // тип проверен err
			t.Fatalf("DataSize %d decoded as %d", value, got)
		}
	}
}

// TestIntegerRangesEncode — V04: симметричная проверка кодировщиком: поля с
// суженным диапазоном вне §6 отвергаются ErrRange до записи кадра.
func TestIntegerRangesEncode(t *testing.T) {
	type encodeCase struct {
		name    string
		command any
		typ     RPCType
		reply   any
	}
	entries := func(index, term int) []contract.LogEntry {
		return []contract.LogEntry{{Index: index, Term: term}}
	}
	cases := []encodeCase{
		{name: "AE Term", command: &contract.AppendEntriesArgs{RPCHeader: header3(1), Term: -1}},
		{name: "AE PrevLogIndex", command: &contract.AppendEntriesArgs{RPCHeader: header3(1), PrevLogIndex: -2}},
		{name: "AE PrevLogTerm", command: &contract.AppendEntriesArgs{RPCHeader: header3(1), PrevLogTerm: -2}},
		{name: "AE LeaderCommit", command: &contract.AppendEntriesArgs{RPCHeader: header3(1), LeaderCommit: -2}},
		{name: "AE entry Index", command: &contract.AppendEntriesArgs{RPCHeader: header3(1), Entries: entries(-1, 0)}},
		{name: "AE entry Term", command: &contract.AppendEntriesArgs{RPCHeader: header3(1), Entries: entries(0, -1)}},
		{name: "RV Term", command: &contract.RequestVoteArgs{RPCHeader: header3(1), Term: -1}},
		{name: "RV LastLogIndex", command: &contract.RequestVoteArgs{RPCHeader: header3(1), LastLogIndex: -2}},
		{name: "RV LastLogTerm", command: &contract.RequestVoteArgs{RPCHeader: header3(1), LastLogTerm: -2}},
		{name: "PV Term", command: &contract.RequestPreVoteArgs{RPCHeader: header3(1), Term: math.MinInt}},
		{name: "PV LastLogIndex", command: &contract.RequestPreVoteArgs{RPCHeader: header3(1), LastLogIndex: -2}},
		{name: "IS LastLogIndex", command: &contract.InstallSnapshotRequest{
			RPCHeader: header3(1), LastLogIndex: -1, DataSize: 1,
		}},
		{name: "IS LastLogTerm", command: &contract.InstallSnapshotRequest{
			RPCHeader: header3(1), LastLogTerm: -1, DataSize: 1,
		}},
		{name: "IS ConfigIndex", command: &contract.InstallSnapshotRequest{
			RPCHeader: header3(1), ConfigIndex: -2, DataSize: 1,
		}},
		{name: "AE reply Term", typ: rpcAppendEntries, reply: &contract.AppendEntriesReply{RPCHeader: header3(1), Term: -1}},
		{name: "AE reply ConflictIndex", typ: rpcAppendEntries, reply: &contract.AppendEntriesReply{
			RPCHeader: header3(1), ConflictIndex: -1,
		}},
		{name: "AE reply ConflictTerm", typ: rpcAppendEntries, reply: &contract.AppendEntriesReply{
			RPCHeader: header3(1), ConflictTerm: -2,
		}},
		{name: "RV reply Term", typ: rpcRequestVote, reply: &contract.RequestVoteReply{RPCHeader: header3(1), Term: -1}},
		{name: "IS reply Term", typ: rpcInstallSnapshot, reply: &contract.InstallSnapshotResponse{
			RPCHeader: header3(1), Term: -1,
		}},
		{name: "TN reply Term", typ: rpcTimeoutNow, reply: &contract.TimeoutNowResponse{RPCHeader: header3(1), Term: -1}},
		{name: "PV reply Term", typ: rpcRequestPreVote, reply: &contract.RequestPreVoteReply{
			RPCHeader: header3(1), Term: -1,
		}},
	}
	for _, tc := range cases {
		var (
			frame []byte
			err   error
		)
		if tc.command != nil {
			frame, err = AppendRequest(nil, tc.command, DefaultLimits())
		} else {
			frame, err = AppendResponse(nil, tc.typ, contract.RPCResponse{Reply: tc.reply}, DefaultLimits())
		}
		requireIs(t, err, ErrRange)
		if len(frame) != 0 {
			t.Fatalf("%s: frame written", tc.name)
		}
	}

	extremes := &contract.AppendEntriesArgs{
		RPCHeader: header3(math.MinInt), Term: math.MaxInt, LeaderID: math.MinInt,
		PrevLogIndex: -1, PrevLogTerm: math.MaxInt, LeaderCommit: -1,
	}
	frame, err := AppendRequest(nil, extremes, DefaultLimits())
	if err != nil {
		t.Fatalf("extremes: %v", err)
	}
	if _, decoded, err := readRequestBytes(frame, DefaultLimits()); err != nil || !reflect.DeepEqual(decoded, extremes) {
		t.Fatalf("extremes round trip: %#v, %v", decoded, err)
	}
}

// TestRange32Bit — V04: на 32-битной платформе i64 вне [MinInt32, MaxInt32]
// отвергается до преобразования в int; проверка выполняется над теми же
// правилами с границами 32-битного int.
func TestRange32Bit(t *testing.T) {
	anyInt32 := intRange{low: math.MinInt32, high: math.MaxInt32}
	nonNegative32 := intRange{low: 0, high: math.MaxInt32}
	minusOne32 := intRange{low: -1, high: math.MaxInt32}
	cases := []struct {
		value int64
		valid intRange
		ok    bool
	}{
		{math.MaxInt32 - 1, nonNegative32, true},
		{math.MaxInt32, nonNegative32, true},
		{math.MaxInt32 + 1, nonNegative32, false},
		{math.MaxInt32 + 1, minusOne32, false},
		{math.MinInt32, anyInt32, true},
		{math.MinInt32 - 1, anyInt32, false},
		{math.MaxInt32 + 1, anyInt32, false},
		{-1, minusOne32, true},
		{-2, minusOne32, false},
	}
	for _, tc := range cases {
		err := checkRange("field", tc.value, tc.valid)
		if tc.ok != (err == nil) {
			t.Fatalf("value %d range %+v: err = %v", tc.value, tc.valid, err)
		}
		if err != nil {
			requireIs(t, err, ErrRange)
		}
	}
	if int64(math.MaxInt) != rangeNonNegative.high || int64(math.MinInt) != rangeAnyInt.low {
		t.Fatalf("platform ranges do not follow int: %+v %+v", rangeNonNegative, rangeAnyInt)
	}
}

// TestBooleanFields — V05: все шесть булевых полей: 0/1 сохраняют значение,
// 2/255 отвергаются.
func TestBooleanFields(t *testing.T) {
	cases := []struct {
		golden   string
		offset   int
		response bool
		field    string
	}{
		{"rv_request", requestBody + 48, false, "LeadershipTransfer"},
		{"rv_reply", responseBody + 24, true, "VoteGranted"},
		{"pv_reply", responseBody + 24, true, "VoteGranted"},
		{"ae_reply", responseBody + 24, true, "Success"},
		{"is_reply", responseBody + 24, true, "Success"},
		{"tn_reply", responseBody + 16, true, "Success"},
	}
	for _, tc := range cases {
		frame := loadGolden(t, tc.golden)
		typ := RPCType(frame[offsetRPCType])
		for _, value := range []byte{0, 1, 2, 255} {
			patched := patchByte(frame, tc.offset, value)
			var (
				decoded any
				err     error
			)
			if tc.response {
				var response contract.RPCResponse
				response, err = readResponseBytes(patched, typ, DefaultLimits())
				decoded = response.Reply
			} else {
				_, decoded, err = readRequestBytes(patched, DefaultLimits())
			}
			if value > 1 {
				requireIs(t, err, ErrFormat)

				continue
			}
			if err != nil {
				t.Fatalf("%s %s=%d: %v", tc.golden, tc.field, value, err)
			}
			if got := reflect.ValueOf(decoded).Elem().FieldByName(tc.field).Bool(); got != (value == 1) {
				t.Fatalf("%s %s=%d decoded as %v", tc.golden, tc.field, value, got)
			}
		}
	}
}
