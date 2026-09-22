package store

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

// readDatFile читает сырые байты файла данных ключа.
func readDatFile(t *testing.T, dir, key string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, key+_dataFileSuffix))
	if err != nil {
		t.Fatalf("чтение файла данных %q: %v", key, err)
	}
	return raw
}

// mustHex декодирует шестнадцатеричную строку ожидаемого вектора.
func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("декодирование вектора: %v", err)
	}
	return b
}

// TestFileStorageFrame_GoldenVectors проверяет побайтовое представление
// кадра: magic, version, reserved, длину payload, контрольную сумму
// CRC-32C и сами байты payload. Векторы зафиксированы заранее, включая
// пустой payload, и не вычисляются из реализации.
func TestFileStorageFrame_GoldenVectors(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		value   []byte
		wantHex string
	}{
		{
			name:  "пустой payload",
			key:   "empty",
			value: []byte{},
			// magic RAFTPST\0, version 1, reserved 0, length 0, CRC-32C 0.
			wantHex: "5241465450535400" + "0001" + "0000" +
				"0000000000000000" + "00000000",
		},
		{
			name:  "ascii hello",
			key:   "hello",
			value: []byte("hello"),
			// length 5, CRC-32C(hello) = 0x9A71BB4C, payload 68 65 6C 6C 6F.
			wantHex: "5241465450535400" + "0001" + "0000" +
				"0000000000000005" + "9a71bb4c" + "68656c6c6f",
		},
		{
			name:  "ключ currentTerm",
			key:   "currentTerm",
			value: []byte("currentTerm"),
			// length 11, CRC-32C(currentTerm) = 0x13D59AA5.
			wantHex: "5241465450535400" + "0001" + "0000" +
				"000000000000000b" + "13d59aa5" + "63757272656e745465726d",
		},
		{
			name:  "нулевые и старшие байты",
			key:   "mixed",
			value: []byte{0x00, 0x01, 0x02, 0xfe, 0xff, 0x00, 0x00},
			// length 7, CRC-32C(mixed payload) = 0x406FB77E.
			wantHex: "5241465450535400" + "0001" + "0000" +
				"0000000000000007" + "406fb77e" + "000102feff0000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			fs := NewFileStorage(dir)
			fs.Set(tt.key, tt.value)

			raw := readDatFile(t, dir, tt.key)
			want := mustHex(t, tt.wantHex)
			if !bytes.Equal(raw, want) {
				t.Fatalf("кадр =\n%x\nwant\n%x", raw, want)
			}
			if len(raw) != _headerSize+len(tt.value) {
				t.Fatalf("размер файла = %d, want %d", len(raw), _headerSize+len(tt.value))
			}
		})
	}
}

// TestFileStorageFrame_PayloadBytesEqualArgument проверяет, что байты
// payload на диске побайтово совпадают с аргументом Set, а заголовок
// занимает ровно 24 байта.
func TestFileStorageFrame_PayloadBytesEqualArgument(t *testing.T) {
	dir := t.TempDir()
	fs := NewFileStorage(dir)

	value := []byte{0x00, 0x00, 0xff, 0x7f, 0x80, 0x00, 0x0a}
	fs.Set("log", value)

	raw := readDatFile(t, dir, "log")
	if !bytes.Equal(raw[_headerSize:], value) {
		t.Fatalf("payload = %x, want %x", raw[_headerSize:], value)
	}
	if !bytes.Equal(raw[:8], _magic[:]) {
		t.Fatalf("magic = %x, want %x", raw[:8], _magic[:])
	}
}

// TestFileStorageFrame_LengthFieldBigEndian проверяет разбор полей
// заголовка записанного кадра: version, reserved и длину большой порядок
// байт.
func TestFileStorageFrame_LengthFieldBigEndian(t *testing.T) {
	dir := t.TempDir()
	fs := NewFileStorage(dir)
	value := make([]byte, 300)
	for i := range value {
		value[i] = byte(i)
	}
	fs.Set("log", value)

	raw := readDatFile(t, dir, "log")
	if got := binary.BigEndian.Uint16(raw[8:10]); got != _formatVersion {
		t.Fatalf("version = %d, want %d", got, _formatVersion)
	}
	if got := binary.BigEndian.Uint16(raw[10:12]); got != 0 {
		t.Fatalf("reserved = %d, want 0", got)
	}
	if got := binary.BigEndian.Uint64(raw[12:20]); got != uint64(len(value)) {
		t.Fatalf("length = %d, want %d", got, len(value))
	}
}

// TestFileStorageFrame_EmptyPayloadRoundtrip проверяет круговой путь для
// пустого значения: кадр состоит из одного заголовка, а загрузчик
// возвращает пустой, но присутствующий ключ.
func TestFileStorageFrame_EmptyPayloadRoundtrip(t *testing.T) {
	dir := t.TempDir()
	fs := NewFileStorage(dir)
	fs.Set("empty", []byte{})

	raw := readDatFile(t, dir, "empty")
	if len(raw) != _headerSize {
		t.Fatalf("размер кадра = %d, want %d", len(raw), _headerSize)
	}

	restarted := NewFileStorage(dir)
	got, ok := restarted.Get("empty")
	if !ok {
		t.Fatal("ключ empty не восстановлен")
	}
	if len(got) != 0 {
		t.Fatalf("значение = %x, want пустое", got)
	}
}
