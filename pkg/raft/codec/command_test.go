package codec

import (
	"bytes"
	"encoding/hex"
	"errors"
	"math"
	"strings"
	"testing"
)

// validCommands — команды всех видов с крайними значениями полей.
func validCommands() []Command {
	return []Command{
		{Kind: KindGet, ID: 1, Key: "key"},
		{Kind: KindPut, ID: 2, Key: "k", Value: "v"},
		{Kind: KindCAS, ID: 3, Key: "k", Value: "new", CompareValue: "old"},
		{Kind: KindDelete, ID: 4, Key: "k"},
		{Kind: KindPut, ID: 0, Key: "", Value: "", CompareValue: ""},
		{Kind: KindPut, ID: math.MaxInt32, Key: strings.Repeat("a", 300), Value: strings.Repeat("b", 1000)},
	}
}

// TestCommandRoundTrip: каждая команда кодируется и строго декодируется в
// то же значение; результат не ссылается на вход.
func TestCommandRoundTrip(t *testing.T) {
	for _, cmd := range validCommands() {
		encoded, err := EncodeCommand(cmd)
		if err != nil {
			t.Fatalf("EncodeCommand(%+v): %v", cmd, err)
		}
		decoded, err := DecodeCommand(encoded)
		if err != nil {
			t.Fatalf("DecodeCommand: %v", err)
		}
		if decoded != cmd {
			t.Fatalf("команда = %+v, want %+v", decoded, cmd)
		}
		// Изменение буфера после декодирования не меняет результат.
		for i := range encoded {
			encoded[i] = 0
		}
		if decoded.Key != cmd.Key || decoded.Value != cmd.Value || decoded.CompareValue != cmd.CompareValue {
			t.Fatalf("результат ссылается на вход: %+v", decoded)
		}
	}
}

// TestCommandLayoutGolden: побайтовое представление команды со всеми
// заполненными полями — заголовок 40 байт, big-endian длины, строки
// подряд. Вектор зафиксирован независимо от реализации формата.
func TestCommandLayoutGolden(t *testing.T) {
	encoded, err := EncodeCommand(Command{
		Kind: KindPut, ID: 7, Key: "K", Value: "VV", CompareValue: "C",
	})
	if err != nil {
		t.Fatalf("EncodeCommand: %v", err)
	}
	if len(encoded) != 40+1+2+1 {
		t.Fatalf("размер = %d, want 44", len(encoded))
	}
	want := "4b564331" + // KVC1
		"01" + // version
		"02" + // kind Put
		"0000" + // reserved
		"0000000000000007" + // ID
		"0000000000000001" + // len(Key)
		"0000000000000002" + // len(Value)
		"0000000000000001" + // len(CompareValue)
		"4b" + "5656" + "43" // K VV C
	if got := hex.EncodeToString(encoded); got != want {
		t.Fatalf("представление =\n%s\nwant\n%s", got, want)
	}
}

// TestDecodeCommandRejects: короткий вход, неверная магическая метка,
// версия, вид, резерв, непомещающийся ID, длины больше остатка и лишние
// байты — ошибка без паники и выделения по ложной длине.
func TestDecodeCommandRejects(t *testing.T) {
	valid, err := EncodeCommand(Command{Kind: KindGet, ID: 1, Key: "abc"})
	if err != nil {
		t.Fatalf("EncodeCommand: %v", err)
	}

	cases := []struct {
		name string
		data []byte
	}{
		{"пустой вход", nil},
		{"короче заголовка", valid[:39]},
		{"неверная магическая метка", append([]byte("XVC1"), valid[4:]...)},
		{"неизвестная версия", withByte(t, valid, 4, 2)},
		{"нулевая версия", withByte(t, valid, 4, 0)},
		{"неизвестный вид", withByte(t, valid, 5, 5)},
		{"нулевой вид", withByte(t, valid, 5, 0)},
		{"ненулевой резерв", withByte(t, valid, 7, 1)},
		{"длина больше остатка", withLen(t, valid, 16, math.MaxUint64)},
		{"хвост", append(append([]byte{}, valid...), 0)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd, err := DecodeCommand(tc.data)
			if err == nil {
				t.Fatalf("DecodeCommand принял %x как %+v", tc.data, cmd)
			}
			if !errors.Is(err, errCommandEncoding) {
				t.Fatalf("ошибка %v не помечена как ошибка кодирования", err)
			}
		})
	}
}

// withByte возвращает копию data с заменённым байтом на offset.
func withByte(t *testing.T, data []byte, offset int, value byte) []byte {
	t.Helper()
	out := bytes.Clone(data)
	out[offset] = value
	return out
}

// withLen возвращает копию data с заменённой 8-байтовой длиной на offset.
func withLen(t *testing.T, data []byte, offset int, value uint64) []byte {
	t.Helper()
	out := bytes.Clone(data)
	for i := 0; i < 8; i++ {
		out[offset+i] = byte(value >> (56 - 8*i))
	}
	return out
}

// TestEncodeCommandRejectsKind: неизвестный вид не кодируется.
func TestEncodeCommandRejectsKind(t *testing.T) {
	if _, err := EncodeCommand(Command{Kind: 5, ID: 1}); err == nil {
		t.Fatal("EncodeCommand принял вид 5")
	}
	if _, err := EncodeCommand(Command{Kind: 0, ID: 1}); err == nil {
		t.Fatal("EncodeCommand принял вид 0")
	}
}

// TestDecodeCommandIDRange: ID представим в int платформы.
func TestDecodeCommandIDRange(t *testing.T) {
	encoded, err := EncodeCommand(Command{Kind: KindGet, ID: 1})
	if err != nil {
		t.Fatalf("EncodeCommand: %v", err)
	}
	// Максимальный int64 представим на 64-битной платформе.
	big := withLen(t, encoded, 8, math.MaxInt64)
	if _, err := DecodeCommand(big); err != nil {
		t.Fatalf("DecodeCommand(MaxInt64): %v", err)
	}
}
