package protocol

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"math"
	"math/rand"
	"testing"
	"testing/iotest"

	"github.com/vskurikhin/raft/pkg/raft/contract"
)

// TestHeaderFieldCorruption — V02: изменение каждого байта заголовка даёт
// именованную ошибку до чтения тела; BeforeBody не вызывается.
func TestHeaderFieldCorruption(t *testing.T) {
	frame := loadGolden(t, "tn_request")
	wantByOffset := func(offset int) error {
		switch {
		case offset < offsetFormatVersion:
			return ErrFormat
		case offset < offsetHeaderSize:
			return ErrUnsupportedVersion
		case offset >= offsetBodyLength && offset < offsetReservedTail:
			return nil // длина тела проверяется отдельно
		default:
			return ErrFormat
		}
	}
	for offset := range headerSize {
		want := wantByOffset(offset)
		if want == nil {
			continue
		}
		for _, value := range []byte{frame[offset] ^ 0x01, frame[offset] ^ 0xff} {
			spy := &countingReader{r: bytes.NewReader(patchByte(frame, offset, value))}
			hook := &hookRecorder{}
			typ, command, err := ReadRequestWithHeader(spy, DefaultLimits(), hook.hook)
			requireIs(t, err, want)
			if typ != 0 || command != nil || hook.calls != 0 || spy.consumed != headerSize {
				t.Fatalf("offset %d value %#x: typ=%d command=%v hook=%d consumed=%d",
					offset, value, typ, command, hook.calls, spy.consumed)
			}
		}
	}
}

// TestRPCTypeValues — V02: известные типы 0…4 читаются, 5 и 255 отвергаются.
func TestRPCTypeValues(t *testing.T) {
	for _, tc := range goldenRequests() {
		typ, _, err := readRequestBytes(loadGolden(t, tc.name), DefaultLimits())
		if err != nil || typ != tc.typ {
			t.Fatalf("%s: typ=%d err=%v", tc.name, typ, err)
		}
	}
	frame := loadGolden(t, "tn_request")
	for _, value := range []byte{5, 255} {
		_, _, err := readRequestBytes(patchByte(frame, offsetRPCType, value), DefaultLimits())
		requireIs(t, err, ErrFormat)
		_, err = readResponseBytes(patchByte(loadGolden(t, "tn_reply"), offsetRPCType, value), RPCType(value),
			DefaultLimits())
		requireIs(t, err, ErrFormat)
	}
}

// TestDirectionAndExpectedType — V02: кадр другого направления и ответ
// другого RPC отвергаются.
func TestDirectionAndExpectedType(t *testing.T) {
	_, _, err := readRequestBytes(loadGolden(t, "rv_reply"), DefaultLimits())
	requireIs(t, err, ErrFormat)

	_, err = readResponseBytes(loadGolden(t, "rv_request"), rpcRequestVote, DefaultLimits())
	requireIs(t, err, ErrFormat)

	for _, tc := range goldenResponses() {
		for expected := range rpcTypeCount {
			if expected == tc.typ {
				continue
			}
			_, err = readResponseBytes(loadGolden(t, tc.name), expected, DefaultLimits())
			requireIs(t, err, ErrFormat)
		}
	}

	for _, expected := range []RPCType{rpcTypeCount, 255} {
		_, err = readResponseBytes(loadGolden(t, "rv_reply"), expected, DefaultLimits())
		requireIs(t, err, ErrFormat)
	}
}

// TestBodyLengthRules — V02/V07: фиксированные, минимальные и наибольшие
// длины тела проверяются по заголовку до чтения тела.
func TestBodyLengthRules(t *testing.T) {
	limits := DefaultLimits()
	cases := []struct {
		name   string
		golden string
		length uint64
		want   error
	}{
		{"RequestVote shorter", "rv_request", requestVoteBody - 1, ErrFormat},
		{"RequestVote longer", "rv_request", requestVoteBody + 1, ErrFormat},
		{"RequestPreVote longer", "pv_request", requestPreVoteBody + 1, ErrFormat},
		{"TimeoutNow shorter", "tn_request", timeoutNowBody - 1, ErrFormat},
		{"AppendEntries below minimum", "ae_request_empty", appendEntriesMinBody - 1, ErrFormat},
		{"InstallSnapshot below minimum", "is_request", installSnapshotMin - 1, ErrFormat},
		{"InstallSnapshot above C", "is_request", installSnapshotMin + limits.MaxConfigurationBytes + 1, ErrLimit},
		{"frame above F", "ae_request_empty", limits.MaxFrameBytes - headerSize + 1, ErrLimit},
		{"MaxUint64", "ae_request_empty", math.MaxUint64, ErrLimit},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spy := &countingReader{r: bytes.NewReader(withBodyLength(loadGolden(t, tc.golden), tc.length))}
			hook := &hookRecorder{}
			_, command, err := ReadRequestWithHeader(spy, limits, hook.hook)
			requireIs(t, err, tc.want)
			if command != nil || hook.calls != 0 || spy.consumed != headerSize {
				t.Fatalf("command=%v hook=%d consumed=%d", command, hook.calls, spy.consumed)
			}
		})
	}

	responseCases := []struct {
		length uint64
		want   error
	}{
		{errorEnvelopeBytes - 1, ErrFormat},
		{maxErrorBodyBytes + 1, ErrLimit},
		{math.MaxUint64, ErrLimit},
	}
	for _, tc := range responseCases {
		spy := &countingReader{r: bytes.NewReader(withBodyLength(loadGolden(t, "rv_reply"), tc.length))}
		hook := &hookRecorder{}
		_, err := ReadResponseWithHeader(spy, rpcRequestVote, limits, hook.hook)
		requireIs(t, err, tc.want)
		if hook.calls != 0 || spy.consumed != headerSize {
			t.Fatalf("length %d: hook=%d consumed=%d", tc.length, hook.calls, spy.consumed)
		}
	}
}

// allGoldenFrames — все эталоны с функцией чтения соответствующего кадра.
func allGoldenFrames(t *testing.T) map[string]func(io.Reader) error {
	t.Helper()

	readers := make(map[string]func(io.Reader) error)
	for _, tc := range goldenRequests() {
		readers[tc.name] = func(r io.Reader) error {
			_, _, err := ReadRequest(r, DefaultLimits())

			return err
		}
	}
	for _, tc := range goldenResponses() {
		readers[tc.name] = func(r io.Reader) error {
			_, err := ReadResponse(r, tc.typ, DefaultLimits())

			return err
		}
	}
	readers["ae_request_gob_string"] = readers["ae_request_empty"]

	return readers
}

// TestFramePrefixes — V13: каждый префикс кадра: пустой — чистый io.EOF,
// частичный заголовок или тело — io.ErrUnexpectedEOF; частичный результат
// не возвращается.
func TestFramePrefixes(t *testing.T) {
	frames := allGoldenFrames(t)
	for name, read := range frames {
		frame := loadGolden(t, name)
		if err := read(bytes.NewReader(nil)); err != io.EOF { //nolint:errorlint // чистый EOF возвращается без обёртки
			t.Fatalf("%s: empty stream err = %v, want io.EOF", name, err)
		}
		for n := 1; n < len(frame); n++ {
			err := read(bytes.NewReader(frame[:n]))
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("%s: prefix %d err = %v, want io.ErrUnexpectedEOF", name, n, err)
			}
		}
	}
}

// TestLargeAppendEntriesPrefixes — V13: все границы полей большого
// AppendEntries: каждый префикс даёт io.ErrUnexpectedEOF.
func TestLargeAppendEntriesPrefixes(t *testing.T) {
	limits := DefaultLimits()
	frame, err := AppendRequest(nil, largeAppendEntries(t, int(limits.MaxEntries)), limits)
	if err != nil {
		t.Fatalf("AppendRequest: %v", err)
	}
	for n := 1; n < len(frame); n++ {
		_, command, err := readRequestBytes(frame[:n], limits)
		if !errors.Is(err, io.ErrUnexpectedEOF) || command != nil {
			t.Fatalf("prefix %d: command=%v err=%v", n, command, err)
		}
	}
	if _, _, err = readRequestBytes(frame, limits); err != nil {
		t.Fatalf("full frame: %v", err)
	}
}

func largeAppendEntries(t *testing.T, count int) *contract.AppendEntriesArgs {
	t.Helper()

	args := &contract.AppendEntriesArgs{
		RPCHeader: header3(1), Term: 3, LeaderID: 1, PrevLogIndex: 9, PrevLogTerm: 2, LeaderCommit: 8,
	}
	for i := range count {
		var data any
		switch i % 3 {
		case 0:
			data = bytes.Repeat([]byte{byte(i)}, 100+i)
		case 1:
			data = string(bytes.Repeat([]byte{'a' + byte(i%26)}, 50+i))
		}
		args.Entries = append(args.Entries, contract.LogEntry{
			Index: 10 + i, Term: 3, Type: contract.LogType(i % 3), Data: data,
		})
	}

	return args
}

// TestPartialReaders — V13: чтение по одному байту и n+EOF на последнем
// чтении восстанавливают кадр; ошибка Reader передаётся с сохранением.
func TestPartialReaders(t *testing.T) {
	for name, read := range allGoldenFrames(t) {
		frame := loadGolden(t, name)
		if err := read(iotest.OneByteReader(bytes.NewReader(frame))); err != nil {
			t.Fatalf("%s one byte: %v", name, err)
		}
		if err := read(iotest.DataErrReader(bytes.NewReader(frame))); err != nil {
			t.Fatalf("%s data+EOF: %v", name, err)
		}
	}

	failure := errors.New("connection reset")
	frame := loadGolden(t, "rv_request")
	for _, n := range []int{0, 10, headerSize, headerSize + 5} {
		reader := io.MultiReader(bytes.NewReader(frame[:n]), iotest.ErrReader(failure))
		_, command, err := ReadRequest(reader, DefaultLimits())
		if !errors.Is(err, failure) || command != nil {
			t.Fatalf("prefix %d: command=%v err=%v", n, command, err)
		}
	}
}

type scriptedWriter struct {
	written []byte
	step    func(p []byte) (int, error)
}

func (w *scriptedWriter) Write(p []byte) (int, error) {
	n, err := w.step(p)
	if n > 0 && n <= len(p) {
		w.written = append(w.written, p[:n]...)
	}

	return n, err
}

// TestWriteFrame — V13: запись по одному байту повторяется до конца;
// n+ошибка учитывает n и возвращает ошибку; 0+nil — io.ErrShortWrite.
func TestWriteFrame(t *testing.T) {
	frame := loadGolden(t, "rv_request")

	oneByte := &scriptedWriter{step: func([]byte) (int, error) { return 1, nil }}
	if err := WriteFrame(oneByte, frame); err != nil || !bytes.Equal(oneByte.written, frame) {
		t.Fatalf("one byte writer: err=%v written=%d", err, len(oneByte.written))
	}

	failure := errors.New("broken pipe")
	calls := 0
	partial := &scriptedWriter{step: func([]byte) (int, error) {
		calls++
		if calls == 2 {
			return 3, failure
		}

		return 5, nil
	}}
	err := WriteFrame(partial, frame)
	if !errors.Is(err, failure) || calls != 2 || !bytes.Equal(partial.written, frame[:8]) {
		t.Fatalf("n+error writer: err=%v calls=%d written=%d", err, calls, len(partial.written))
	}

	stalled := &scriptedWriter{step: func([]byte) (int, error) { return 0, nil }}
	requireIs(t, WriteFrame(stalled, frame), io.ErrShortWrite)

	invalid := &scriptedWriter{step: func(p []byte) (int, error) { return len(p) + 1, nil }}
	requireIs(t, WriteFrame(invalid, frame), errInvalidWrite)

	untouched := &scriptedWriter{step: func([]byte) (int, error) { t.Fatal("unexpected write"); return 0, nil }}
	if err = WriteFrame(untouched, nil); err != nil {
		t.Fatalf("empty frame: %v", err)
	}
}

// TestConsecutiveFrames — V14: подряд идущие кадры читаются ровно по одному,
// в том числе случайными порциями; после последнего кадра — io.EOF.
func TestConsecutiveFrames(t *testing.T) {
	first := loadGolden(t, "rv_request")
	second := loadGolden(t, "ae_request_noop")
	stream := append(bytes.Clone(first), second...)

	rng := rand.New(rand.NewSource(1))
	for range 20 {
		reader := &chunkReader{data: stream, rng: rng}
		typ, _, err := ReadRequest(reader, DefaultLimits())
		if err != nil || typ != rpcRequestVote {
			t.Fatalf("first: typ=%d err=%v", typ, err)
		}
		typ, _, err = ReadRequest(reader, DefaultLimits())
		if err != nil || typ != rpcAppendEntries {
			t.Fatalf("second: typ=%d err=%v", typ, err)
		}
		if _, _, err = ReadRequest(reader, DefaultLimits()); err != io.EOF { //nolint:errorlint // чистый EOF
			t.Fatalf("third: err=%v, want io.EOF", err)
		}
	}
}

type chunkReader struct {
	data []byte
	rng  *rand.Rand
}

func (c *chunkReader) Read(p []byte) (int, error) {
	if len(c.data) == 0 {
		return 0, io.EOF
	}
	n := min(len(p), len(c.data), 1+c.rng.Intn(7))
	copy(p, c.data[:n])
	c.data = c.data[n:]

	return n, nil
}

// TestExtraBytesInsideBody — V14: лишний байт внутри объявленного тела
// отвергается; полный следующий кадр снаружи тела допустим.
func TestExtraBytesInsideBody(t *testing.T) {
	cases := []string{"ae_request_empty", "ae_request_noop", "is_request"}
	for _, name := range cases {
		frame := loadGolden(t, name)
		grown := append(withBodyLength(frame, uint64(len(frame)-headerSize+1)), 0)
		_, command, err := readRequestBytes(grown, DefaultLimits())
		requireIs(t, err, ErrFormat)
		if command != nil {
			t.Fatalf("%s: command %v", name, command)
		}
	}

	for _, name := range []string{"ae_reply", "rv_reply", "rv_error4_response"} {
		frame := loadGolden(t, name)
		grown := append(withBodyLength(frame, uint64(len(frame)-headerSize+1)), 0)
		typ := RPCType(frame[offsetRPCType])
		_, err := readResponseBytes(grown, typ, DefaultLimits())
		requireIs(t, err, ErrFormat)
	}

	frame := loadGolden(t, "ae_request_noop")
	shortened := withBodyLength(frame, uint64(len(frame)-headerSize-1))
	_, _, err := readRequestBytes(shortened, DefaultLimits())
	requireIs(t, err, ErrFormat)
}

// TestNoResynchronization — V14: после мусорного байта кадр не ищется по
// magic; следующие чтения также завершаются ошибкой.
func TestNoResynchronization(t *testing.T) {
	frame := loadGolden(t, "tn_request")
	stream := bytes.NewReader(append([]byte{0}, append(bytes.Clone(frame), frame...)...))
	_, _, err := ReadRequest(stream, DefaultLimits())
	requireIs(t, err, ErrFormat)
	for {
		_, command, err := ReadRequest(stream, DefaultLimits())
		if command != nil {
			t.Fatalf("resynchronized on %v", command)
		}
		if err != nil && !errors.Is(err, ErrFormat) && !errors.Is(err, io.ErrUnexpectedEOF) && err != io.EOF { //nolint:errorlint // чистый EOF
			t.Fatalf("unexpected error %v", err)
		}
		if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) { //nolint:errorlint // чистый EOF
			break
		}
	}
}

// TestBeforeBodyHook — V19: BeforeBody вызывается ровно один раз с проверенным
// заголовком, когда прочитаны ровно 24 Б; тело до его возврата не читается.
func TestBeforeBodyHook(t *testing.T) {
	frame := loadGolden(t, "rv_request")
	spy := &countingReader{r: bytes.NewReader(frame)}
	hook := &hookRecorder{spy: spy}
	typ, command, err := ReadRequestWithHeader(spy, DefaultLimits(), hook.hook)
	if err != nil || typ != rpcRequestVote || command == nil {
		t.Fatalf("typ=%d command=%v err=%v", typ, command, err)
	}
	want := FrameHeader{Direction: directionRequest, Type: rpcRequestVote, BodyLength: requestVoteBody}
	if hook.calls != 1 || hook.header != want || hook.consumedAt != headerSize {
		t.Fatalf("hook calls=%d header=%+v consumedAt=%d", hook.calls, hook.header, hook.consumedAt)
	}

	responseSpy := &countingReader{r: bytes.NewReader(loadGolden(t, "ae_reply"))}
	responseHook := &hookRecorder{spy: responseSpy}
	if _, err = ReadResponseWithHeader(responseSpy, rpcAppendEntries, DefaultLimits(), responseHook.hook); err != nil {
		t.Fatalf("ReadResponseWithHeader: %v", err)
	}
	wantResponse := FrameHeader{Direction: directionResponse, Type: rpcAppendEntries, BodyLength: 49}
	if responseHook.calls != 1 || responseHook.header != wantResponse || responseHook.consumedAt != headerSize {
		t.Fatalf("response hook calls=%d header=%+v consumedAt=%d",
			responseHook.calls, responseHook.header, responseHook.consumedAt)
	}
}

// TestBeforeBodyHookWithBufferedReader — V19: единый bufio.Reader вызывающего
// может заранее держать байты тела в буфере, но кодек потребляет только 24 Б
// до возврата BeforeBody и не оборачивает Reader вторым буфером.
func TestBeforeBodyHookWithBufferedReader(t *testing.T) {
	frame := loadGolden(t, "ae_request_noop")
	buffered := bufio.NewReader(bytes.NewReader(append(bytes.Clone(frame), 0xAA)))
	var bufferedAtHook int
	hook := func(FrameHeader) error {
		bufferedAtHook = buffered.Buffered()

		return nil
	}
	if _, _, err := ReadRequestWithHeader(buffered, DefaultLimits(), hook); err != nil {
		t.Fatalf("ReadRequestWithHeader: %v", err)
	}
	if bufferedAtHook != len(frame)+1-headerSize {
		t.Fatalf("buffered at hook = %d, want %d", bufferedAtHook, len(frame)+1-headerSize)
	}
	next, err := buffered.ReadByte()
	if err != nil || next != 0xAA {
		t.Fatalf("byte after frame = %#x, %v", next, err)
	}
}

type deadlineConn struct {
	readErr  error
	writeErr error
}

func (c deadlineConn) SetReadDeadline() error  { return c.readErr }
func (c deadlineConn) SetWriteDeadline() error { return c.writeErr }

// TestBeforeBodyHookErrors — V19: ошибка BeforeBody, в том числе ошибка
// установки срока чтения или записи, оборачивается с %w, немедленно
// прекращает чтение без потребления тела и без результата.
func TestBeforeBodyHookErrors(t *testing.T) {
	readFailure := errors.New("set read deadline: use of closed connection")
	writeFailure := errors.New("set write deadline: use of closed connection")
	hooks := map[string]struct {
		hook BeforeBody
		want error
	}{
		"callback": {func(FrameHeader) error { return readFailure }, readFailure},
		"SetReadDeadline": {func(FrameHeader) error {
			return deadlineConn{readErr: readFailure}.SetReadDeadline()
		}, readFailure},
		"SetWriteDeadline": {func(FrameHeader) error {
			conn := deadlineConn{writeErr: writeFailure}
			if err := conn.SetReadDeadline(); err != nil {
				return err
			}

			return conn.SetWriteDeadline()
		}, writeFailure},
	}
	for name, tc := range hooks {
		t.Run(name, func(t *testing.T) {
			spy := &countingReader{r: bytes.NewReader(loadGolden(t, "ae_request_noop"))}
			typ, command, err := ReadRequestWithHeader(spy, DefaultLimits(), tc.hook)
			requireIs(t, err, tc.want)
			if typ != 0 || command != nil || spy.consumed != headerSize {
				t.Fatalf("typ=%d command=%v consumed=%d", typ, command, spy.consumed)
			}

			responseSpy := &countingReader{r: bytes.NewReader(loadGolden(t, "ae_reply"))}
			response, err := ReadResponseWithHeader(responseSpy, rpcAppendEntries, DefaultLimits(), tc.hook)
			requireIs(t, err, tc.want)
			if response != (contract.RPCResponse{}) || responseSpy.consumed != headerSize {
				t.Fatalf("response=%#v consumed=%d", response, responseSpy.consumed)
			}
		})
	}
}

// TestBeforeBodyHookNotCalled — V19: чистый EOF, частичный заголовок и
// ошибки заголовка не вызывают BeforeBody; частичное тело — после одного
// вызова BeforeBody даёт io.ErrUnexpectedEOF.
func TestBeforeBodyHookNotCalled(t *testing.T) {
	frame := loadGolden(t, "rv_request")
	cases := []struct {
		name  string
		input []byte
		want  error
		calls int
	}{
		{"clean EOF", nil, io.EOF, 0},
		{"partial header", frame[:headerSize-1], io.ErrUnexpectedEOF, 0},
		{"bad magic", patchByte(frame, 0, 'X'), ErrFormat, 0},
		{"bad version", patchByte(frame, offsetFormatVersion+1, 2), ErrUnsupportedVersion, 0},
		{"frame above F", withBodyLength(frame, math.MaxUint64), ErrLimit, 0},
		{"partial body", frame[:len(frame)-1], io.ErrUnexpectedEOF, 1},
	}
	for _, tc := range cases {
		hook := &hookRecorder{}
		_, command, err := ReadRequestWithHeader(bytes.NewReader(tc.input), DefaultLimits(), hook.hook)
		requireIs(t, err, tc.want)
		if command != nil || hook.calls != tc.calls {
			t.Fatalf("%s: command=%v hook calls=%d, want %d", tc.name, command, hook.calls, tc.calls)
		}
	}
}

// TestBeforeBodyProtocolVersionAfterHook — V19/V03: версия протокола лежит в
// теле и проверяется после BeforeBody; RPCType при этом надёжен.
func TestBeforeBodyProtocolVersionAfterHook(t *testing.T) {
	frame := patchUint64(loadGolden(t, "tn_request"), headerSize, 2)
	hook := &hookRecorder{}
	typ, command, err := ReadRequestWithHeader(bytes.NewReader(frame), DefaultLimits(), hook.hook)
	requireIs(t, err, contract.ErrUnsupportedProtocol)
	if typ != rpcTimeoutNow || command != nil || hook.calls != 1 {
		t.Fatalf("typ=%d command=%v hook calls=%d", typ, command, hook.calls)
	}
}

// TestBodyStepBoundary — V19: тела 262144 и 262145 Б — граница ступени
// масштабирования; кадр 262169 Б при F=262168 отвергается до BeforeBody.
func TestBodyStepBoundary(t *testing.T) {
	limits := Limits{MaxFrameBytes: 262170, MaxEntries: 2, MaxDataBytes: 131009, MaxConfigurationBytes: 2560}
	for _, bodyLength := range []uint64{262144, 262145} {
		args := appendEntriesWithBody(t, bodyLength, limits)
		frame, err := AppendRequest(nil, args, limits)
		if err != nil {
			t.Fatalf("AppendRequest body %d: %v", bodyLength, err)
		}
		if uint64(len(frame)) != headerSize+bodyLength {
			t.Fatalf("frame %d bytes, want %d", len(frame), headerSize+bodyLength)
		}

		spy := &countingReader{r: bytes.NewReader(frame)}
		hook := &hookRecorder{spy: spy}
		if _, _, err = ReadRequestWithHeader(spy, limits, hook.hook); err != nil {
			t.Fatalf("read body %d: %v", bodyLength, err)
		}
		wantFactor := uint64(1)
		if bodyLength > frameStepBytes {
			wantFactor = 2
		}
		if hook.header.BodyLength != bodyLength || hook.consumedAt != headerSize ||
			frameFactor(headerSize+hook.header.BodyLength) != wantFactor {
			t.Fatalf("body %d: header=%+v consumedAt=%d", bodyLength, hook.header, hook.consumedAt)
		}

		tight := limits
		tight.MaxFrameBytes = 262168
		tight.MaxDataBytes = 131008
		tightSpy := &countingReader{r: bytes.NewReader(frame)}
		tightHook := &hookRecorder{}
		_, _, err = ReadRequestWithHeader(tightSpy, tight, tightHook.hook)
		if bodyLength == 262144 {
			if err != nil {
				t.Fatalf("F=262168 body 262144: %v", err)
			}

			continue
		}
		requireIs(t, err, ErrLimit)
		if tightHook.calls != 0 || tightSpy.consumed != headerSize {
			t.Fatalf("F=262168 body 262145: hook=%d consumed=%d", tightHook.calls, tightSpy.consumed)
		}
	}
}

// appendEntriesWithBody строит AppendEntries из двух записей []byte с телом
// кадра ровно bodyLength байт.
func appendEntriesWithBody(t *testing.T, bodyLength uint64, limits Limits) *contract.AppendEntriesArgs {
	t.Helper()

	dataTotal := bodyLength - appendEntriesMinBody - 2*entryHeaderBytes
	first := payloadWithEncodedLength(t, dataTotal/2, limits.MaxDataBytes)
	second := payloadWithEncodedLength(t, dataTotal-dataTotal/2, limits.MaxDataBytes)

	return &contract.AppendEntriesArgs{
		RPCHeader: header3(1), Term: 1, LeaderID: 1, PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []contract.LogEntry{
			{Index: 1, Term: 1, Type: contract.LogCommand, Data: first},
			{Index: 2, Term: 1, Type: contract.LogCommand, Data: second},
		},
	}
}

// payloadWithEncodedLength подбирает []byte, сетевая длина которого ровно want.
func payloadWithEncodedLength(t *testing.T, want, limit uint64) []byte {
	t.Helper()

	for size := want; size > want-64; size-- {
		payload := make([]byte, size)
		n, err := MeasureData(payload, limit)
		if err == nil && n == want {
			return payload
		}
	}
	t.Fatalf("no []byte with encoded length %d", want)

	return nil
}
