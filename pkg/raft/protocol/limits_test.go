package protocol

import (
	"bytes"
	"math"
	"strings"
	"testing"

	"github.com/vskurikhin/raft/pkg/raft/contract"
)

// TestDefaultLimits — V27/AC5:
// DefaultLimits возвращает {262144, 31, 8192, 2560} новой копией;
// изменение копии не влияет на следующий вызов.
func TestDefaultLimits(t *testing.T) {
	want := contract.Limits{MaxFrameBytes: 262144, MaxEntries: 31, MaxDataBytes: 8192, MaxConfigurationBytes: 2560}
	got := DefaultLimits()
	if got != want {
		t.Fatalf("DefaultLimits = %+v, want %+v", got, want)
	}
	got.MaxEntries = 1
	if DefaultLimits() != want {
		t.Fatalf("DefaultLimits is not a fresh copy")
	}
	if err := want.Validate(); err != nil {
		t.Fatalf("default limits invalid: %v", err)
	}
}

// TestNormalizeLimits — V27/AC5: только целиком нулевой профиль заменяется
// профилем по умолчанию; частичный ноль и несогласованный профиль —
// ErrLimit; явный N не пересчитывается.
func TestNormalizeLimits(t *testing.T) {
	got, err := NormalizeLimits(contract.Limits{})
	if err != nil || got != DefaultLimits() {
		t.Fatalf("zero limits: %+v, %v", got, err)
	}

	explicit := contract.Limits{MaxFrameBytes: 262144, MaxEntries: 6, MaxDataBytes: 41984, MaxConfigurationBytes: 2560}
	got, err = NormalizeLimits(explicit)
	if err != nil || got != explicit {
		t.Fatalf("explicit limits: %+v, %v", got, err)
	}

	partial := []contract.Limits{
		{MaxFrameBytes: 262144},
		{MaxEntries: 31, MaxDataBytes: 8192, MaxConfigurationBytes: 2560},
		{MaxFrameBytes: 262144, MaxEntries: 31, MaxDataBytes: 8192},
		{MaxFrameBytes: 262144, MaxDataBytes: 8192, MaxConfigurationBytes: 2560},
		{MaxFrameBytes: 262144, MaxEntries: 32, MaxDataBytes: 8192, MaxConfigurationBytes: 2560},
	}
	for _, limits := range partial {
		got, err = NormalizeLimits(limits)
		requireIs(t, err, ErrLimit)
		if got != (contract.Limits{}) {
			t.Fatalf("%+v normalized to %+v", limits, got)
		}
	}
}

// TestCodecRejectsInvalidLimits — V07: нулевой и несогласованный профиль не
// означает отсутствия пределов: все функции кодека возвращают ErrLimit, не
// читая и не записывая байтов.
func TestCodecRejectsInvalidLimits(t *testing.T) {
	invalid := []contract.Limits{
		{},
		{MaxFrameBytes: 262144, MaxEntries: 32, MaxDataBytes: 8192, MaxConfigurationBytes: 2560},
	}
	for _, limits := range invalid {
		spy := &countingReader{r: bytes.NewReader(loadGolden(t, "rv_request"))}
		_, _, err := ReadRequest(spy, limits)
		requireIs(t, err, ErrLimit)
		_, err = ReadResponse(spy, rpcRequestVote, limits)
		requireIs(t, err, ErrLimit)
		if spy.consumed != 0 {
			t.Fatalf("consumed %d bytes with invalid limits", spy.consumed)
		}

		frame, err := AppendRequest([]byte("p"), &contract.TimeoutNowRequest{RPCHeader: header3(1)}, limits)
		requireIs(t, err, ErrLimit)
		if string(frame) != "p" {
			t.Fatalf("frame %q", frame)
		}
		_, err = AppendResponse(nil, rpcTimeoutNow, contract.RPCResponse{Error: contract.ErrRaftShutdown}, limits)
		requireIs(t, err, ErrLimit)
	}
}

// TestHugeBodyRejectedBeforeRead — V07: заголовок с огромной длиной тела
// отвергается после чтения ровно 24 Б, без чтения и выделения тела.
func TestHugeBodyRejectedBeforeRead(t *testing.T) {
	limits := DefaultLimits()
	header := appendHeader(nil, directionRequest, rpcAppendEntries, 1<<40)
	spy := &countingReader{r: endlessReader{header: header}}
	// Запрещающий BeforeBody: BASE обязан отвергнуть заголовок до вызова
	// hook; на мутанте без проверки длины hook возвращает маркерную ошибку
	// до выделения тела.
	hook := &hookRecorder{err: errHookMustNotRun}
	_, _, err := ReadRequestWithHeader(spy, limits, hook.hook)
	requireIs(t, err, ErrLimit)
	if hook.calls != 0 {
		t.Fatalf("BeforeBody called %d times", hook.calls)
	}
	if spy.consumed != headerSize {
		t.Fatalf("consumed %d bytes", spy.consumed)
	}

	for _, length := range []uint64{limits.MaxFrameBytes - headerSize, limits.MaxFrameBytes - headerSize + 1} {
		frame := append(appendHeader(nil, directionRequest, rpcAppendEntries, length), make([]byte, length)...)
		_, _, err = readRequestBytes(frame, limits)
		if length+headerSize > limits.MaxFrameBytes {
			requireIs(t, err, ErrLimit)
		} else {
			// Кадр ровно F байт прочитан целиком; нулевое тело несёт версию 0.
			requireIs(t, err, contract.ErrUnsupportedProtocol)
		}
	}
}

// endlessReader — Reader, отдающий заголовок, а затем бесконечные нули.
type endlessReader struct{ header []byte }

func (e endlessReader) Read(p []byte) (int, error) {
	n := copy(p, e.header)
	if n < len(p) {
		clear(p[n:])
	}

	return len(p), nil
}

// TestFrameCapacityBounded — V07/AC5: ёмкость кадра, выделенного кодеком,
// не превышает F, ёмкость кодированной Data — D, в том числе при росте и
// при ошибке.
func TestFrameCapacityBounded(t *testing.T) {
	limits := DefaultLimits()
	frame, err := AppendRequest(nil, largeAppendEntries(t, int(limits.MaxEntries)), limits)
	if err != nil {
		t.Fatalf("AppendRequest: %v", err)
	}
	if uint64(cap(frame)) > limits.MaxFrameBytes {
		t.Fatalf("frame capacity %d exceeds F %d", cap(frame), limits.MaxFrameBytes)
	}

	prefix := []byte("prefix")
	frame, err = AppendRequest(prefix, largeAppendEntries(t, int(limits.MaxEntries)), limits)
	if err != nil || !bytes.HasPrefix(frame, prefix) {
		t.Fatalf("with prefix: %v", err)
	}
	if uint64(cap(frame)) > uint64(len(prefix))+limits.MaxFrameBytes {
		t.Fatalf("frame capacity %d exceeds prefix+F", cap(frame))
	}

	for _, size := range []int{0, 1, 100, 8000, 8192} {
		data, err := EncodeData(bytes.Repeat([]byte{7}, size), limits.MaxDataBytes)
		if err == nil && uint64(cap(data)) > limits.MaxDataBytes {
			t.Fatalf("data capacity %d exceeds D", cap(data))
		}
	}

	buffer := boundedBuffer{limit: 100}
	for range 10 {
		_, _ = buffer.Write(bytes.Repeat([]byte{1}, 30))
		if cap(buffer.buf) > buffer.limit {
			t.Fatalf("scratch capacity %d exceeds limit", cap(buffer.buf))
		}
	}
	if !buffer.exceeded || len(buffer.buf) != 90 {
		t.Fatalf("bounded buffer: exceeded=%v len=%d", buffer.exceeded, len(buffer.buf))
	}
}

// TestFrameBuilderBounds — V07: рост кадра останавливается до расширения
// буфера сверх F; переполнение префикса вызывающего — ErrRange.
func TestFrameBuilderBounds(t *testing.T) {
	builder, err := newFrameBuilder(nil, smallLimits)
	if err != nil {
		t.Fatalf("newFrameBuilder: %v", err)
	}
	if err = builder.reserve(smallLimits.MaxFrameBytes); err != nil {
		t.Fatalf("reserve F: %v", err)
	}
	builder.buf = builder.buf[:1]
	requireIs(t, builder.reserve(smallLimits.MaxFrameBytes), ErrLimit)
	if uint64(cap(builder.buf)) > smallLimits.MaxFrameBytes {
		t.Fatalf("capacity %d exceeds F", cap(builder.buf))
	}

	huge := Limits{MaxFrameBytes: math.MaxInt, MaxEntries: 1, MaxDataBytes: 1, MaxConfigurationBytes: 1}
	_, err = newFrameBuilder([]byte{1}, huge)
	requireIs(t, err, ErrRange)
}

// TestConfigurationLimit — V07: конфигурация C допустима, C+1 отвергается
// кодировщиком и декодером (по заголовку до тела).
func TestConfigurationLimit(t *testing.T) {
	limits := smallLimits
	request := &contract.InstallSnapshotRequest{
		RPCHeader: header3(1), DataSize: 1,
		Configuration: []byte(strings.Repeat("c", int(limits.MaxConfigurationBytes))),
	}
	frame, err := AppendRequest(nil, request, limits)
	if err != nil {
		t.Fatalf("C bytes: %v", err)
	}
	if _, _, err = readRequestBytes(frame, limits); err != nil {
		t.Fatalf("decode C bytes: %v", err)
	}

	request.Configuration = append(request.Configuration, 'c')
	_, err = AppendRequest(nil, request, limits)
	requireIs(t, err, ErrLimit)

	wider := limits
	wider.MaxConfigurationBytes++
	frame, err = AppendRequest(nil, request, wider)
	if err != nil {
		t.Fatalf("C+1 with wider limits: %v", err)
	}
	_, _, err = readRequestBytes(frame, limits)
	requireIs(t, err, ErrLimit)
}

// TestDataLengthLimitDecode — V07: DataLength больше D отвергается ErrLimit
// до gob-декодирования.
func TestDataLengthLimitDecode(t *testing.T) {
	wider := smallLimits
	wider.MaxDataBytes = 128
	payload := payloadWithEncodedLength(t, smallLimits.MaxDataBytes+1, wider.MaxDataBytes)
	args := &contract.AppendEntriesArgs{RPCHeader: header3(1), Entries: []contract.LogEntry{{Data: payload}}}
	frame, err := AppendRequest(nil, args, wider)
	if err != nil {
		t.Fatalf("AppendRequest: %v", err)
	}
	_, _, err = readRequestBytes(frame, smallLimits)
	requireIs(t, err, ErrLimit)
}

// TestConfigurationLengthFieldLimit — V07: поле ConfigurationLength больше C
// (C+1 и MaxUint64) при корректной внешней длине кадра отвергается ErrLimit,
// а не более поздней сверкой с остатком тела.
func TestConfigurationLengthFieldLimit(t *testing.T) {
	limits := DefaultLimits()
	frame := loadGolden(t, "is_request")
	offset := requestBody + installSnapshotMin - 8
	for _, length := range []uint64{limits.MaxConfigurationBytes + 1, math.MaxUint64} {
		_, command, err := readRequestBytes(patchUint64(frame, offset, length), limits)
		requireIs(t, err, ErrLimit)
		if command != nil {
			t.Fatalf("ConfigurationLength %d: command %v", length, command)
		}
	}
}
