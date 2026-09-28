package protocol

import (
	"bytes"
	"encoding/binary"
	"math"
	"reflect"
	"testing"

	"github.com/vskurikhin/raft/pkg/raft/contract"
)

// Смещения полей AppendEntries и первой записи в ae_request_noop.
const (
	offsetEntryCount = requestBody + 56
	offsetFirstEntry = requestBody + appendEntriesMinBody
	offsetLogType    = offsetFirstEntry + 16
	offsetDataKind   = offsetFirstEntry + 17
	offsetEntryRsv   = offsetFirstEntry + 18
	offsetDataLength = offsetFirstEntry + 24
)

// buildAppendEntries строит кадр AppendEntries вручную с count записями nil
// Data и заданным объявленным EntryCount.
func buildAppendEntries(declared uint64, entries int) []byte {
	body := make([]byte, 0, appendEntriesMinBody+entries*entryHeaderBytes)
	for _, v := range []uint64{3, 1, 1, 1, 0, 0, 0, declared} {
		body = binary.BigEndian.AppendUint64(body, v)
	}
	for i := range entries {
		body = binary.BigEndian.AppendUint64(body, uint64(i+1))
		body = binary.BigEndian.AppendUint64(body, 1)
		body = append(body, 0, 0, 0, 0, 0, 0, 0, 0)
		body = binary.BigEndian.AppendUint64(body, 0)
	}

	return append(appendHeader(nil, directionRequest, rpcAppendEntries, uint64(len(body))), body...)
}

// TestEntryCountDecode — V06: EntryCount 0/1/N−1/N допустимы, N+1 и
// MaxUint64 — ErrLimit; EntryCount больше remaining/32 — ErrFormat; отказ
// до выделения Entries.
func TestEntryCountDecode(t *testing.T) {
	limits := smallLimits
	n := int(limits.MaxEntries)
	for _, count := range []int{0, 1, n - 1, n} {
		_, command, err := readRequestBytes(buildAppendEntries(uint64(count), count), limits)
		if err != nil {
			t.Fatalf("count %d: %v", count, err)
		}
		if got := len(command.(*contract.AppendEntriesArgs).Entries); got != count { //nolint:forcetypeassert // err == nil
			t.Fatalf("count %d decoded %d entries", count, got)
		}
	}

	for _, count := range []uint64{uint64(n) + 1, math.MaxUint64} {
		_, _, err := readRequestBytes(buildAppendEntries(count, n), limits)
		requireIs(t, err, ErrLimit)
	}

	_, _, err := readRequestBytes(buildAppendEntries(uint64(n), n-1), limits)
	requireIs(t, err, ErrFormat)
}

// TestEntryCountOverflow — V06: count·sizeof(LogEntry) проверяется на
// переполнение до make, даже если count согласован с N и остатком тела.
func TestEntryCountOverflow(t *testing.T) {
	huge := Limits{MaxFrameBytes: math.MaxInt, MaxEntries: (math.MaxInt - 88) / 33, MaxDataBytes: 1,
		MaxConfigurationBytes: 1}
	if err := huge.Validate(); err != nil {
		t.Fatalf("limits: %v", err)
	}
	count := uint64(math.MaxInt)/logEntrySize + 1
	if count > huge.MaxEntries {
		t.Skipf("count %d exceeds N on this platform", count)
	}
	requireIs(t, checkEntryCount(count, math.MaxInt, huge), ErrLimit)
	if err := checkEntryCount(count-1, math.MaxInt, huge); err != nil {
		t.Fatalf("count-1: %v", err)
	}
}

// TestEntryCountEncode — V06/V07: кодировщик отвергает больше N записей и
// Data больше D до записи кадра.
func TestEntryCountEncode(t *testing.T) {
	limits := smallLimits
	args := &contract.AppendEntriesArgs{RPCHeader: header3(1)}
	for i := range int(limits.MaxEntries) {
		args.Entries = append(args.Entries, contract.LogEntry{Index: i + 1, Term: 1})
	}
	if _, err := AppendRequest(nil, args, limits); err != nil {
		t.Fatalf("N entries: %v", err)
	}
	args.Entries = append(args.Entries, contract.LogEntry{Index: 9, Term: 1})
	frame, err := AppendRequest(nil, args, limits)
	requireIs(t, err, ErrLimit)
	if len(frame) != 0 {
		t.Fatalf("frame written")
	}

	fitting := payloadWithEncodedLength(t, limits.MaxDataBytes, limits.MaxDataBytes)
	args.Entries = []contract.LogEntry{{Index: 1, Term: 1, Data: fitting}}
	if _, err = AppendRequest(nil, args, limits); err != nil {
		t.Fatalf("D bytes: %v", err)
	}
	args.Entries[0].Data = append(bytes.Clone(fitting), 0)
	_, err = AppendRequest(nil, args, limits)
	requireIs(t, err, ErrLimit)
}

// TestEmptyEntriesAndConfiguration — V08: nil и пустые Entries/Configuration
// кодируются одинаково и декодируются в nil; пустые AppendEntries подряд.
func TestEmptyEntriesAndConfiguration(t *testing.T) {
	withNil := &contract.AppendEntriesArgs{RPCHeader: header3(1), Term: 1}
	withEmpty := &contract.AppendEntriesArgs{RPCHeader: header3(1), Term: 1, Entries: []contract.LogEntry{}}
	first, err := AppendRequest(nil, withNil, DefaultLimits())
	if err != nil {
		t.Fatalf("nil entries: %v", err)
	}
	second, err := AppendRequest(nil, withEmpty, DefaultLimits())
	if err != nil || !bytes.Equal(first, second) {
		t.Fatalf("empty entries: %v, equal=%v", err, bytes.Equal(first, second))
	}

	stream := bytes.NewReader(bytes.Repeat(first, 5))
	for range 5 {
		_, command, err := ReadRequest(stream, DefaultLimits())
		if err != nil || command.(*contract.AppendEntriesArgs).Entries != nil { //nolint:forcetypeassert // err == nil
			t.Fatalf("empty AE: %#v, %v", command, err)
		}
	}

	nilConfig := &contract.InstallSnapshotRequest{RPCHeader: header3(1), DataSize: 1}
	emptyConfig := &contract.InstallSnapshotRequest{RPCHeader: header3(1), DataSize: 1, Configuration: []byte{}}
	a, err := AppendRequest(nil, nilConfig, DefaultLimits())
	if err != nil {
		t.Fatalf("nil configuration: %v", err)
	}
	b, err := AppendRequest(nil, emptyConfig, DefaultLimits())
	if err != nil || !bytes.Equal(a, b) {
		t.Fatalf("empty configuration: %v", err)
	}
	_, command, err := readRequestBytes(a, DefaultLimits())
	if err != nil || command.(*contract.InstallSnapshotRequest).Configuration != nil { //nolint:forcetypeassert // err == nil
		t.Fatalf("configuration decoded as %#v, %v", command, err)
	}
}

// TestEntryFields — V09: LogType 0/1/2 допустимы, 3/255 — ErrFormat;
// DataKind 2/255, nil-kind с длиной, gob-kind с нулевой длиной, ненулевые
// reserved — ErrFormat; gob-поток, декодирующийся в nil, — ErrData.
func TestEntryFields(t *testing.T) {
	frame := loadGolden(t, "ae_request_noop")
	for _, logType := range []byte{0, 1, 2} {
		_, command, err := readRequestBytes(patchByte(frame, offsetLogType, logType), DefaultLimits())
		if err != nil {
			t.Fatalf("LogType %d: %v", logType, err)
		}
		if got := command.(*contract.AppendEntriesArgs).Entries[0].Type; got != contract.LogType(logType) { //nolint:forcetypeassert,lll // err == nil
			t.Fatalf("LogType %d decoded as %d", logType, got)
		}
	}
	cases := []struct {
		name  string
		frame []byte
		want  error
	}{
		{"LogType 3", patchByte(frame, offsetLogType, 3), ErrFormat},
		{"LogType 255", patchByte(frame, offsetLogType, 255), ErrFormat},
		{"DataKind 2", patchByte(frame, offsetDataKind, 2), ErrFormat},
		{"DataKind 255", patchByte(frame, offsetDataKind, 255), ErrFormat},
		{"nil kind with length", patchUint64(frame, offsetDataLength, 1), ErrFormat},
		{"gob kind with zero length", patchByte(frame, offsetDataKind, dataKindGob), ErrFormat},
		{"reserved first", patchByte(frame, offsetEntryRsv, 1), ErrFormat},
		{"reserved last", patchByte(frame, offsetEntryRsv+entryReservedBytes-1, 1), ErrFormat},
	}
	for _, tc := range cases {
		_, command, err := readRequestBytes(tc.frame, DefaultLimits())
		requireIs(t, err, tc.want)
		if command != nil {
			t.Fatalf("%s: command %v", tc.name, command)
		}
	}

	gobKind := patchByte(frame, offsetDataKind, dataKindGob)
	for _, length := range []uint64{DefaultLimits().MaxDataBytes + 1, math.MaxUint64} {
		_, _, err := readRequestBytes(patchUint64(gobKind, offsetDataLength, length), DefaultLimits())
		requireIs(t, err, ErrLimit)
	}
	_, _, err := readRequestBytes(patchUint64(gobKind, offsetDataLength, 33), DefaultLimits())
	requireIs(t, err, ErrFormat)

	nilEnvelope := mustHex(t, "147f030102ff80000101010444617461011000000003ff8000")
	_, _, err = readRequestBytes(singleEntryFrame(nilEnvelope, contract.LogCommand), DefaultLimits())
	requireIs(t, err, ErrData)

	for _, logType := range []contract.LogType{-1, 3} {
		args := &contract.AppendEntriesArgs{RPCHeader: header3(1), Entries: []contract.LogEntry{{Type: logType}}}
		_, err = AppendRequest(nil, args, DefaultLimits())
		requireIs(t, err, ErrFormat)
	}
}

// singleEntryFrame строит AppendEntries с одной записью gob-вида и заданным
// сетевым потоком Data.
func singleEntryFrame(payload []byte, logType contract.LogType) []byte {
	body := make([]byte, 0, appendEntriesMinBody+entryHeaderBytes+len(payload))
	for _, v := range []uint64{3, 1, 1, 1, 0, 0, 0, 1, 1, 1} {
		body = binary.BigEndian.AppendUint64(body, v)
	}
	body = append(body, byte(logType), dataKindGob, 0, 0, 0, 0, 0, 0)
	body = binary.BigEndian.AppendUint64(body, uint64(len(payload)))
	body = append(body, payload...)

	return append(appendHeader(nil, directionRequest, rpcAppendEntries, uint64(len(body))), body...)
}

// TestNoopData — V09: noop с nil успешен, ненулевая Data у noop и
// конфигурации не теряется.
func TestNoopData(t *testing.T) {
	args := &contract.AppendEntriesArgs{RPCHeader: header3(1), Entries: []contract.LogEntry{
		{Index: 1, Term: 1, Type: contract.LogNoop},
		{Index: 2, Term: 1, Type: contract.LogNoop, Data: "kept"},
		{Index: 3, Term: 1, Type: contract.LogConfiguration, Data: []byte{1, 2, 3}},
	}}
	frame, err := AppendRequest(nil, args, DefaultLimits())
	if err != nil {
		t.Fatalf("AppendRequest: %v", err)
	}
	if frame[offsetDataKind] != dataKindNil {
		t.Fatalf("noop nil encoded with kind %d", frame[offsetDataKind])
	}
	_, decoded, err := readRequestBytes(frame, DefaultLimits())
	if err != nil || !reflect.DeepEqual(decoded, args) {
		t.Fatalf("decoded %#v, %v", decoded, err)
	}
}
