package kvservice

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"strings"
	"testing"
)

// codecCommands — входы точного round-trip: все виды, крайние ID, пустые
// строки и произвольные байты, включая невалидный UTF-8.
func codecCommands() []Command {
	return []Command{
		{Kind: CommandGet, Key: "k", ID: 0},
		{Kind: CommandPut, Key: "k", Value: "v", ID: 1},
		{Kind: CommandCAS, Key: "k", Value: "new", CompareValue: "old", ID: 2},
		{Kind: CommandDelete, Key: "k", ID: -1},
		{Kind: CommandPut, ID: math.MaxInt},
		{Kind: CommandCAS, Key: "\x00\xff\xfe", Value: "\xc3\x28", CompareValue: "\x00", ID: math.MinInt},
		{Kind: CommandGet, Key: "ключ", Value: "ignored", CompareValue: "ignored too", ID: 5},
		{Kind: CommandPut, Key: strings.Repeat("a", 4096), Value: strings.Repeat("\x00", 70000), ID: 9},
	}
}

func TestCommandCodecRoundTrip(t *testing.T) {
	for _, want := range codecCommands() {
		data, err := EncodeCommand(want)
		if err != nil {
			t.Fatalf("EncodeCommand(%v): %v", want.Kind, err)
		}
		wantSize := _commandHeaderSize + len(want.Key) + len(want.Value) + len(want.CompareValue)
		if len(data) != wantSize || cap(data) != wantSize {
			t.Fatalf("encoded len=%d cap=%d, want %d", len(data), cap(data), wantSize)
		}
		got, err := DecodeCommand(data)
		if err != nil {
			t.Fatalf("DecodeCommand: %v", err)
		}
		if got != want {
			t.Fatalf("round-trip mismatch: got %+v, want %+v", got, want)
		}
	}
}

// TestCommandCodecLayout закрепляет байты заголовка версии 1.
func TestCommandCodecLayout(t *testing.T) {
	data, err := EncodeCommand(Command{Kind: CommandCAS, Key: "ab", Value: "c", CompareValue: "def", ID: 258})
	if err != nil {
		t.Fatalf("EncodeCommand: %v", err)
	}
	want := []byte("KVC1")
	want = append(want, 1, 3, 0, 0)
	want = binary.BigEndian.AppendUint64(want, 258)
	want = binary.BigEndian.AppendUint64(want, 2)
	want = binary.BigEndian.AppendUint64(want, 1)
	want = binary.BigEndian.AppendUint64(want, 3)
	want = append(want, "abcdef"...)
	if !bytes.Equal(data, want) {
		t.Fatalf("layout mismatch:\n got %x\nwant %x", data, want)
	}
}

// TestCommandCodecResultNotEncoded: поля результата не входят в формат.
func TestCommandCodecResultNotEncoded(t *testing.T) {
	base := Command{Kind: CommandPut, Key: "k", Value: "v", ID: 1}
	withResult := base
	withResult.ResultValue, withResult.ResultFound = "prev", true
	a, err := EncodeCommand(base)
	if err != nil {
		t.Fatalf("EncodeCommand: %v", err)
	}
	b, err := EncodeCommand(withResult)
	if err != nil {
		t.Fatalf("EncodeCommand: %v", err)
	}
	if !bytes.Equal(a, b) {
		t.Fatal("result fields changed the encoding")
	}
	got, err := DecodeCommand(b)
	if err != nil {
		t.Fatalf("DecodeCommand: %v", err)
	}
	if got.ResultValue != "" || got.ResultFound {
		t.Fatalf("decoded result fields %q/%v, want zero", got.ResultValue, got.ResultFound)
	}
}

// TestCommandCodecDecodeCopiesInput: изменение входа после декодирования
// не меняет результат.
func TestCommandCodecDecodeCopiesInput(t *testing.T) {
	want := Command{Kind: CommandCAS, Key: "key", Value: "value", CompareValue: "cmp", ID: 4}
	data, err := EncodeCommand(want)
	if err != nil {
		t.Fatalf("EncodeCommand: %v", err)
	}
	got, err := DecodeCommand(data)
	if err != nil {
		t.Fatalf("DecodeCommand: %v", err)
	}
	for i := _commandHeaderSize; i < len(data); i++ {
		data[i] = 'X'
	}
	if got != want {
		t.Fatalf("decoded command shares input buffer: got %+v", got)
	}
}

func TestCommandCodecEncodeRejectsInvalidKind(t *testing.T) {
	for _, kind := range []CommandKind{CommandInvalid, CommandDelete + 1, -1, 256} {
		if _, err := EncodeCommand(Command{Kind: kind, Key: "k"}); err == nil {
			t.Fatalf("EncodeCommand(kind=%d) succeeded, want error", kind)
		}
	}
}

func TestEncodedCommandSizeOverflow(t *testing.T) {
	if _, err := encodedCommandSize(math.MaxInt-_commandHeaderSize, 0, 0); err != nil {
		t.Fatalf("size at limit: %v", err)
	}
	cases := [][]int{
		{math.MaxInt - _commandHeaderSize + 1, 0, 0},
		{math.MaxInt / 2, math.MaxInt / 2, 0},
		{math.MaxInt, math.MaxInt, math.MaxInt},
	}
	for _, lengths := range cases {
		if _, err := encodedCommandSize(lengths...); err == nil {
			t.Fatalf("encodedCommandSize(%v) succeeded, want overflow error", lengths)
		}
	}
}

func TestIDFitsInt(t *testing.T) {
	if !idFitsInt(math.MaxInt64, 64) || !idFitsInt(math.MinInt64, 64) {
		t.Fatal("64-bit int must hold any int64")
	}
	if !idFitsInt(math.MaxInt32, 32) || !idFitsInt(math.MinInt32, 32) {
		t.Fatal("32-bit int must hold int32 bounds")
	}
	if idFitsInt(math.MaxInt32+1, 32) || idFitsInt(math.MinInt32-1, 32) {
		t.Fatal("32-bit int must reject values outside int32")
	}
}

// validEncoded возвращает корректное представление для порчи в тестах.
func validEncoded(t *testing.T) []byte {
	t.Helper()
	data, err := EncodeCommand(Command{Kind: CommandCAS, Key: "key", Value: "val", CompareValue: "cmp", ID: 7})
	if err != nil {
		t.Fatalf("EncodeCommand: %v", err)
	}
	return data
}

func TestCommandCodecDecodeRejectsCorruption(t *testing.T) {
	mutate := func(fn func([]byte) []byte) func(*testing.T) []byte {
		return func(t *testing.T) []byte { return fn(validEncoded(t)) }
	}
	setLength := func(i int, n uint64) func([]byte) []byte {
		return func(d []byte) []byte {
			binary.BigEndian.PutUint64(d[_commandOffsetLengths+8*i:], n)
			return d
		}
	}
	cases := []struct {
		name string
		data func(*testing.T) []byte
	}{
		{"empty", func(*testing.T) []byte { return nil }},
		{"short header", mutate(func(d []byte) []byte { return d[:_commandHeaderSize-1] })},
		{"bad magic", mutate(func(d []byte) []byte { d[0] = 'X'; return d })},
		{"version 0", mutate(func(d []byte) []byte { d[_commandOffsetVersion] = 0; return d })},
		{"version 2", mutate(func(d []byte) []byte { d[_commandOffsetVersion] = 2; return d })},
		{"kind 0", mutate(func(d []byte) []byte { d[_commandOffsetKind] = 0; return d })},
		{"kind 5", mutate(func(d []byte) []byte { d[_commandOffsetKind] = 5; return d })},
		{"kind 255", mutate(func(d []byte) []byte { d[_commandOffsetKind] = 255; return d })},
		{"reserved low", mutate(func(d []byte) []byte { d[_commandOffsetReserved+1] = 1; return d })},
		{"reserved high", mutate(func(d []byte) []byte { d[_commandOffsetReserved] = 0x80; return d })},
		{"truncated body", mutate(func(d []byte) []byte { return d[:len(d)-1] })},
		{"trailing byte", mutate(func(d []byte) []byte { return append(d, 0) })},
		{"key too long", mutate(setLength(0, 100))},
		{"value too long", mutate(setLength(1, 100))},
		{"compare too long", mutate(setLength(2, 100))},
		{"length max uint64", mutate(setLength(0, math.MaxUint64))},
		{"lengths sum overflow", mutate(func(d []byte) []byte {
			setLength(0, math.MaxUint64)(d)
			return setLength(1, math.MaxUint64)(d)
		})},
		{"short lengths", mutate(setLength(2, 0))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeCommand(tc.data(t))
			if !errors.Is(err, errCommandEncoding) {
				t.Fatalf("DecodeCommand error = %v, want errCommandEncoding", err)
			}
		})
	}
}

// TestCommandCodecDecodeEveryTruncation: любой обрезанный префикс
// отвергается без паники.
func TestCommandCodecDecodeEveryTruncation(t *testing.T) {
	data := validEncoded(t)
	for n := range len(data) {
		if _, err := DecodeCommand(data[:n]); err == nil {
			t.Fatalf("DecodeCommand(prefix %d of %d) succeeded", n, len(data))
		}
	}
}

// TestCommandCodecDecodeNoAllocationByDeclaredLength: огромная заявленная
// длина отвергается до выделения памяти под строки.
func TestCommandCodecDecodeNoAllocationByDeclaredLength(t *testing.T) {
	data := validEncoded(t)
	binary.BigEndian.PutUint64(data[_commandOffsetLengths:], 1<<40)
	allocs := testing.AllocsPerRun(100, func() {
		if _, err := DecodeCommand(data); err == nil {
			t.Fatal("DecodeCommand succeeded")
		}
	})
	// Допустимы только выделения на текст ошибки.
	if allocs > 4 {
		t.Fatalf("DecodeCommand allocated %.0f times on rejected input", allocs)
	}
}

// TestEncodeCommandSingleAllocation: кодек выделяет только буфер результата.
func TestEncodeCommandSingleAllocation(t *testing.T) {
	cmd := Command{Kind: CommandCAS, Key: "key", Value: "value", CompareValue: "cmp", ID: 3}
	allocs := testing.AllocsPerRun(100, func() {
		if _, err := EncodeCommand(cmd); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 1 {
		t.Fatalf("EncodeCommand allocated %.0f times, want 1", allocs)
	}
}

// FuzzDecodeCommand: декодер не паникует, а принятый вход канонический —
// повторное кодирование даёт те же байты.
func FuzzDecodeCommand(f *testing.F) {
	for _, cmd := range codecCommands()[:7] {
		data, err := EncodeCommand(cmd)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(data)
	}
	f.Add([]byte("KVC1"))
	f.Fuzz(func(t *testing.T, data []byte) {
		cmd, err := DecodeCommand(data)
		if err != nil {
			return
		}
		again, err := EncodeCommand(cmd)
		if err != nil {
			t.Fatalf("re-encode of accepted command: %v", err)
		}
		if !bytes.Equal(again, data) {
			t.Fatalf("non-canonical input accepted:\n got %x\nwant %x", again, data)
		}
	})
}
