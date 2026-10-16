package store

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"reflect"
	"slices"
	"testing"

	"github.com/vskurikhin/raft/pkg/raft/contract"
)

// LogCodecTestPayload — зарегистрированный пользовательский тип Data для
// проверки кругового пути. Регистрация типа остаётся обязанностью
// потребителя; пакет store не импортирует пакеты потребителей.
type LogCodecTestPayload struct {
	ID   int
	Body string
}

// registerLogCodecTestPayload регистрирует тестовый тип в gob. Повторная
// регистрация того же типа безопасна.
func registerLogCodecTestPayload() {
	gob.Register(LogCodecTestPayload{})
}

// mustEncodeLogData кодирует одну Data потоком gob и завершает тест при
// ошибке.
func mustEncodeLogData(t *testing.T, data any) []byte {
	t.Helper()
	encoded, err := encodeLogData(data)
	if err != nil {
		t.Fatalf("кодирование Data: %v", err)
	}
	return encoded
}

// mustEncodeFile кодирует новый файл журнала и завершает тест при ошибке.
func mustEncodeFile(t *testing.T, entries []contract.LogEntry) []byte {
	t.Helper()
	file, err := encodeLogFile(entries)
	if err != nil {
		t.Fatalf("кодирование файла: %v", err)
	}
	return file
}

// mustEncodeBatch кодирует один пакет и завершает тест при ошибке.
func mustEncodeBatch(t *testing.T, entries []contract.LogEntry) []byte {
	t.Helper()
	packet, err := encodeLogBatch(entries)
	if err != nil {
		t.Fatalf("кодирование пакета: %v", err)
	}
	return packet
}

// buildLogPacket собирает пакет из готового тела и числа записей, используя
// производственные кодировщики заголовка и footer. Нужен там, где тело
// формируется вручную.
func buildLogPacket(t *testing.T, body []byte, entryCount uint64) []byte {
	t.Helper()
	header := encodeLogBatchHeader(uint64(len(body)), entryCount)
	footer := encodeLogFooter(header[:], body)
	packet := make([]byte, 0, len(header)+len(body)+len(footer))
	packet = append(packet, header[:]...)
	packet = append(packet, body...)
	packet = append(packet, footer[:]...)
	return packet
}

// buildEntryRecord собирает служебные 32 байта одной записи без Data.
func buildEntryRecord(index, term int, logType contract.LogType, dataLength uint64) []byte {
	var record [logEntryHeaderSize]byte
	binary.BigEndian.PutUint64(record[0:8], uint64(index))
	binary.BigEndian.PutUint64(record[8:16], uint64(term))
	record[16] = byte(logType)
	binary.BigEndian.PutUint64(record[24:32], uint64(dataLength))
	return record[:]
}

// repairBatchHeaderCRC пересчитывает CRC заголовка пакета после правки его
// полей, чтобы проверка доходила до последующих уровней.
func repairBatchHeaderCRC(packet []byte) {
	sum := crc32.Checksum(packet[0:24], _crc32c)
	sum = crc32.Update(sum, _crc32c, packet[28:32])
	binary.BigEndian.PutUint32(packet[24:28], sum)
}

// repairBatchFooterCRC пересчитывает CRC пакета после правки тела, чтобы
// проверка доходила до декодирования Data.
func repairBatchFooterCRC(packet []byte) {
	bodyLength := int(binary.BigEndian.Uint64(packet[8:16]))
	body := packet[logBatchHeaderSize : logBatchHeaderSize+bodyLength]
	sum := crc32.Checksum(packet[:logBatchHeaderSize], _crc32c)
	sum = crc32.Update(sum, _crc32c, body)
	footerStart := logBatchHeaderSize + bodyLength
	binary.BigEndian.PutUint32(packet[footerStart:footerStart+4], sum)
}

// requireCorruption проверяет, что ошибка помечена как повреждение журнала.
func requireCorruption(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("ожидалась ошибка повреждения, получено успешное чтение")
	}
	if !errors.Is(err, errLogCorrupt) {
		t.Fatalf("ошибка %v не помечена как повреждение", err)
	}
}

// requireSameEntries сравнивает два среза записей журнала.
func requireSameEntries(t *testing.T, got, want []contract.LogEntry) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("записи =\n%#v\nwant\n%#v", got, want)
	}
}

// TestLogFileHeaderGolden проверяет побайтовое представление неизменяемого
// заголовка файла: сигнатуру, версию, размер заголовка, зарезервированное
// поле и CRC байт 0..19. Вектор зафиксирован заранее и не вычисляется из
// реализации.
func TestLogFileHeaderGolden(t *testing.T) {
	wantHex := "5241465450535400" + "0002" + "0018" +
		"0000000000000000" + "5a43f8c8"

	header := encodeLogFileHeader()
	want := mustDecodeHex(t, wantHex)
	if !bytes.Equal(header[:], want) {
		t.Fatalf("заголовок файла =\n%x\nwant\n%x", header[:], want)
	}
	if got := binary.BigEndian.Uint16(header[8:10]); got != logFileVersion {
		t.Fatalf("version = %d, want %d", got, logFileVersion)
	}
	if got := binary.BigEndian.Uint16(header[10:12]); got != logFileHeaderSize {
		t.Fatalf("headerSize = %d, want %d", got, logFileHeaderSize)
	}
	if hasNonZero(header[12:20]) {
		t.Fatalf("reserved = %x, want нули", header[12:20])
	}
	if got, want := crc32.Checksum(header[0:20], _crc32c), binary.BigEndian.Uint32(header[20:24]); got != want {
		t.Fatalf("CRC заголовка = %08x, want %08x", got, want)
	}
}

// TestLogFileEmptyGolden проверяет кодирование пустого журнала: заголовок
// файла и один пустой пакет без gob-данных.
func TestLogFileEmptyGolden(t *testing.T) {
	wantHex := "5241465450535400" + "0002" + "0018" + "0000000000000000" + "5a43f8c8" +
		"524c4232" + "00000000" + "0000000000000000" + "0000000000000000" +
		"9a2c13fe" + "00000000" +
		"a899011d" + "524c434d49543200"

	file := mustEncodeFile(t, nil)
	want := mustDecodeHex(t, wantHex)
	if !bytes.Equal(file, want) {
		t.Fatalf("пустой журнал =\n%x\nwant\n%x", file, want)
	}
}

// TestLogBatchGoldenNonEmpty проверяет пакет с непустыми Data и числовыми
// полями: смещения, big-endian и независимо пересчитанные CRC заголовка
// пакета и footer. Байты потока gob не фиксируются: их идентификаторы типов
// зависят от регистраций пакета, поэтому вектор намеренно не побайтовый.
func TestLogBatchGoldenNonEmpty(t *testing.T) {
	registerLogCodecTestPayload()
	entries := []contract.LogEntry{{Index: 7, Term: 3, Type: contract.LogCommand, Data: []byte("gold")}}
	packet := mustEncodeBatch(t, entries)

	if !bytes.Equal(packet[0:4], logBatchMagic[:]) {
		t.Fatalf("magic пакета = %x, want %x", packet[0:4], logBatchMagic[:])
	}
	if hasNonZero(packet[4:8]) || hasNonZero(packet[28:32]) {
		t.Fatalf("reserved пакета не нулевой: %x %x", packet[4:8], packet[28:32])
	}
	bodyLength := binary.BigEndian.Uint64(packet[8:16])
	entryCount := binary.BigEndian.Uint64(packet[16:24])
	if entryCount != 1 {
		t.Fatalf("entryCount = %d, want 1", entryCount)
	}
	if int(bodyLength) != len(packet)-logBatchHeaderSize-logFooterSize {
		t.Fatalf("bodyLength = %d, want %d", bodyLength, len(packet)-logBatchHeaderSize-logFooterSize)
	}
	if got := int64(binary.BigEndian.Uint64(packet[32:40])); got != 7 {
		t.Fatalf("index = %d, want 7", got)
	}
	if got := int64(binary.BigEndian.Uint64(packet[40:48])); got != 3 {
		t.Fatalf("term = %d, want 3", got)
	}
	if got := contract.LogType(packet[48]); got != contract.LogCommand {
		t.Fatalf("type = %d, want %d", got, contract.LogCommand)
	}
	if hasNonZero(packet[49:56]) {
		t.Fatalf("reserved записи не нулевой: %x", packet[49:56])
	}
	dataLength := binary.BigEndian.Uint64(packet[56:64])
	if dataLength != uint64(len(packet)-logBatchHeaderSize-32-logFooterSize) {
		t.Fatalf("dataLength = %d, want %d", dataLength, len(packet)-logBatchHeaderSize-32-logFooterSize)
	}
	body := packet[logBatchHeaderSize : len(packet)-logFooterSize]
	footerStart := len(packet) - logFooterSize
	wantHeaderCRC := crc32.Checksum(packet[0:24], _crc32c)
	wantHeaderCRC = crc32.Update(wantHeaderCRC, _crc32c, packet[28:32])
	if got := binary.BigEndian.Uint32(packet[24:28]); got != wantHeaderCRC {
		t.Fatalf("CRC заголовка пакета = %08x, want %08x", got, wantHeaderCRC)
	}
	wantFooterCRC := crc32.Checksum(packet[:logBatchHeaderSize], _crc32c)
	wantFooterCRC = crc32.Update(wantFooterCRC, _crc32c, body)
	if got := binary.BigEndian.Uint32(packet[footerStart : footerStart+4]); got != wantFooterCRC {
		t.Fatalf("CRC пакета = %08x, want %08x", got, wantFooterCRC)
	}
	if !bytes.Equal(packet[footerStart+4:], logCommitMarker[:]) {
		t.Fatalf("маркер конца = %x, want %x", packet[footerStart+4:], logCommitMarker[:])
	}
}

// TestLogFileRoundTrip проверяет круговой путь nil, []byte и
// зарегистрированного пользовательского типа; каждое значение декодируется
// новым декодером.
func TestLogFileRoundTrip(t *testing.T) {
	registerLogCodecTestPayload()
	entries := []contract.LogEntry{
		{Index: 0, Term: 0, Type: contract.LogNoop, Data: nil},
		{Index: 1, Term: 1, Type: contract.LogConfiguration, Data: []byte{1, 2, 3}},
		{Index: 2, Term: 2, Type: contract.LogCommand, Data: LogCodecTestPayload{ID: 42, Body: "строка"}},
	}
	file := mustEncodeFile(t, entries)

	got, validEnd, err := decodeLogFile(file)
	if err != nil {
		t.Fatalf("декодирование файла: %v", err)
	}
	if validEnd != len(file) {
		t.Fatalf("validEnd = %d, want %d", validEnd, len(file))
	}
	requireSameEntries(t, got, entries)
	if got[0].Data != nil {
		t.Fatalf("nil Data декодирована как %#v", got[0].Data)
	}
}

// TestLogDataStreamsAreIndependent проверяет, что Data каждой записи — это
// самостоятельный поток: сырые данные, полученные структурным разбором,
// декодируются по отдельности без общего состояния декодера.
func TestLogDataStreamsAreIndependent(t *testing.T) {
	registerLogCodecTestPayload()
	entries := []contract.LogEntry{
		{Index: 0, Term: 0, Type: contract.LogCommand, Data: []byte{0xde, 0xad}},
		{Index: 1, Term: 0, Type: contract.LogCommand, Data: LogCodecTestPayload{ID: 7, Body: "b"}},
	}
	scan, err := scanLogFile(mustEncodeFile(t, entries))
	if err != nil {
		t.Fatalf("структурный разбор: %v", err)
	}
	if len(scan.entries) != len(entries) {
		t.Fatalf("число записей = %d, want %d", len(scan.entries), len(entries))
	}
	for i := range scan.entries {
		value, err := decodeLogData(scan.entries[i].data)
		if err != nil {
			t.Fatalf("независимое декодирование записи %d: %v", i, err)
		}
		if !reflect.DeepEqual(value, entries[i].Data) {
			t.Fatalf("Data записи %d = %#v, want %#v", i, value, entries[i].Data)
		}
	}
}

// TestScanLogFileKeepsEncodedData проверяет, что структурный разбор не
// декодирует gob, а сохраняет закодированные байты Data точно такими,
// какими их создал кодировщик.
func TestScanLogFileKeepsEncodedData(t *testing.T) {
	entries := []contract.LogEntry{
		{Index: 0, Term: 0, Type: contract.LogCommand, Data: []byte{1, 2, 3}},
		{Index: 1, Term: 1, Type: contract.LogNoop, Data: nil},
	}
	scan, err := scanLogFile(mustEncodeFile(t, entries))
	if err != nil {
		t.Fatalf("структурный разбор: %v", err)
	}
	if scan.tail {
		t.Fatal("структурный разбор вернул хвост на целом файле")
	}
	for i := range scan.entries {
		want := mustEncodeLogData(t, entries[i].Data)
		if !bytes.Equal(scan.entries[i].data, want) {
			t.Fatalf("закодированные Data записи %d = %x, want %x", i, scan.entries[i].data, want)
		}
	}
}

// TestDecodeLogFileCutAtEveryPositionLastPacket проверяет, что разрез в
// каждой позиции последнего пакета не выдаёт ни одной его записи: годный
// префикс кончается на начале отброшенного пакета.
func TestDecodeLogFileCutAtEveryPositionLastPacket(t *testing.T) {
	base := mustEncodeFile(t, []contract.LogEntry{
		{Index: 0, Term: 0, Type: contract.LogNoop, Data: nil},
		{Index: 1, Term: 1, Type: contract.LogCommand, Data: []byte{0xaa}},
	})
	second := mustEncodeBatch(t, []contract.LogEntry{
		{Index: 2, Term: 1, Type: contract.LogCommand, Data: []byte{0xbb}},
		{Index: 3, Term: 1, Type: contract.LogCommand, Data: []byte{0xcc}},
	})
	file := append(slices.Clone(base), second...)
	wantBase, _, err := decodeLogFile(base)
	if err != nil {
		t.Fatalf("декодирование базы: %v", err)
	}

	for cut := len(base); cut < len(file); cut++ {
		got, validEnd, err := decodeLogFile(file[:cut])
		if err != nil {
			t.Fatalf("разрез %d из %d: %v", cut, len(file), err)
		}
		if validEnd != len(base) {
			t.Fatalf("разрез %d: validEnd = %d, want %d", cut, validEnd, len(base))
		}
		requireSameEntries(t, got, wantBase)
	}
}

// TestDecodeLogFileCutFirstPacketFails проверяет, что разрез внутри первого
// базового пакета всегда ошибка: неполный базовый пакет не становится хвостом.
func TestDecodeLogFileCutFirstPacketFails(t *testing.T) {
	base := mustEncodeFile(t, []contract.LogEntry{
		{Index: 0, Term: 0, Type: contract.LogNoop, Data: nil},
		{Index: 1, Term: 1, Type: contract.LogCommand, Data: []byte{0xaa}},
	})
	for cut := 1; cut < len(base); cut++ {
		_, _, err := decodeLogFile(base[:cut])
		requireCorruption(t, err)
	}
}

// TestDecodeLogFileBadHeaderCRCIsNotTail проверяет, что неверная CRC
// заголовка пакета с завышенной объявленной длиной — повреждение, а не
// «обрыв»: длина не должна маскировать порчу заголовка.
func TestDecodeLogFileBadHeaderCRCIsNotTail(t *testing.T) {
	base := mustEncodeFile(t, []contract.LogEntry{
		{Index: 0, Term: 0, Type: contract.LogNoop, Data: nil},
	})
	bad := slices.Clone(base)
	binary.BigEndian.PutUint64(bad[logFileHeaderSize+8:logFileHeaderSize+16], ^uint64(0))
	bad[logFileHeaderSize+24] ^= 0xff
	_, _, err := decodeLogFile(bad)
	requireCorruption(t, err)
}

// TestDecodeLogFileCorruption проверяет защитные сценарии повреждения:
// каждая строка соответствует одному классу нарушения формата. Сценарии
// достижимы внешним повреждением носителя, а не штатной записью.
func TestDecodeLogFileCorruption(t *testing.T) {
	base := mustEncodeFile(t, []contract.LogEntry{
		{Index: 0, Term: 0, Type: contract.LogNoop, Data: nil},
		{Index: 1, Term: 1, Type: contract.LogCommand, Data: []byte{0xaa}},
	})
	second := mustEncodeBatch(t, []contract.LogEntry{
		{Index: 2, Term: 1, Type: contract.LogCommand, Data: []byte{0xbb}},
	})
	file := append(slices.Clone(base), second...)
	firstEnd := len(base)
	pktStart := logFileHeaderSize
	entryType := pktStart + logEntryHeaderSize + 16
	entryIndex := pktStart + logEntryHeaderSize
	entryTerm := pktStart + logEntryHeaderSize + 8
	entryReserved := pktStart + logEntryHeaderSize + 17
	entryDataLength := pktStart + logEntryHeaderSize + 24

	cases := []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{"сигнатура файла", func(f []byte) []byte { f[0] ^= 0xff; return f }},
		{"версия файла", func(f []byte) []byte { f[8] ^= 0xff; return f }},
		{"размер заголовка файла", func(f []byte) []byte { f[10] ^= 0xff; return f }},
		{"зарезервированное поле файла", func(f []byte) []byte { f[12] = 1; return f }},
		{"CRC заголовка файла", func(f []byte) []byte { f[20] ^= 0xff; return f }},
		{"сигнатура пакета", func(f []byte) []byte { f[pktStart] ^= 0xff; return f }},
		{"reserved заголовка пакета 4..7", func(f []byte) []byte { f[pktStart+4] = 1; return f }},
		{"reserved заголовка пакета 28..31", func(f []byte) []byte { f[pktStart+28] = 1; return f }},
		{"CRC заголовка пакета", func(f []byte) []byte { f[pktStart+24] ^= 0xff; return f }},
		{"нулевой файл без пакета", func(f []byte) []byte { return f[:logFileHeaderSize] }},
		{"файл короче заголовка", func(f []byte) []byte { return f[:8] }},
		{"пустой не первый пакет", func(f []byte) []byte {
			base := mustEncodeFile(t, []contract.LogEntry{
				{Index: 0, Term: 0, Type: contract.LogNoop, Data: nil},
			})
			packet := mustEncodeBatch(t, nil)
			return append(base, packet...)
		}},
		{"разрыв индексов между пакетами", func(f []byte) []byte {
			packet := mustEncodeBatch(t, []contract.LogEntry{
				{Index: 9, Term: 1, Type: contract.LogCommand, Data: []byte{0xbb}},
			})
			return append(slices.Clone(base), packet...)
		}},
		{"завышенная длина тела с целой CRC", func(f []byte) []byte {
			binary.BigEndian.PutUint64(f[pktStart+8:pktStart+16], ^uint64(0))
			repairBatchHeaderCRC(f[pktStart:firstEnd])
			return f
		}},
		{"завышенное число записей", func(f []byte) []byte {
			binary.BigEndian.PutUint64(f[pktStart+16:pktStart+24], 1000)
			repairBatchHeaderCRC(f[pktStart:firstEnd])
			return f
		}},
		{"неизвестный тип записи", func(f []byte) []byte {
			f[entryType] = 9
			repairBatchFooterCRC(f[pktStart:firstEnd])
			return f
		}},
		{"ненулевое reserved записи", func(f []byte) []byte {
			f[entryReserved] = 1
			repairBatchFooterCRC(f[pktStart:firstEnd])
			return f
		}},
		{"отрицательный индекс", func(f []byte) []byte {
			binary.BigEndian.PutUint64(f[entryIndex:entryIndex+8], ^uint64(0))
			repairBatchFooterCRC(f[pktStart:firstEnd])
			return f
		}},
		{"отрицательный терм", func(f []byte) []byte {
			binary.BigEndian.PutUint64(f[entryTerm:entryTerm+8], ^uint64(0))
			repairBatchFooterCRC(f[pktStart:firstEnd])
			return f
		}},
		{"завышенная длина Data", func(f []byte) []byte {
			binary.BigEndian.PutUint64(f[entryDataLength:entryDataLength+8], ^uint64(0)>>1)
			repairBatchFooterCRC(f[pktStart:firstEnd])
			return f
		}},
		{"неверная CRC пакета", func(f []byte) []byte { f[firstEnd-12] ^= 0xff; return f }},
		{"неверный маркер конца", func(f []byte) []byte { f[firstEnd-1] ^= 0xff; return f }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bad := tc.mutate(slices.Clone(file))
			_, _, err := decodeLogFile(bad)
			requireCorruption(t, err)
		})
	}
}

// TestDecodeLogFileExtraDataBytes проверяет, что лишние байты в области Data
// (gob декодировал одно значение, но область не исчерпана) — повреждение.
func TestDecodeLogFileExtraDataBytes(t *testing.T) {
	data := mustEncodeLogData(t, nil)
	data = append(data, 0x7f)
	body := append(buildEntryRecord(0, 0, contract.LogNoop, uint64(len(data))), data...)
	packet := buildLogPacket(t, body, 1)
	header := encodeLogFileHeader()
	file := append(header[:], packet...)
	_, _, err := decodeLogFile(file)
	requireCorruption(t, err)
}

// TestDecodeLogFileEmptyJournal проверяет валидный пустой журнал: заголовок
// файла и один пустой первый пакет.
func TestDecodeLogFileEmptyJournal(t *testing.T) {
	file := mustEncodeFile(t, nil)
	entries, validEnd, err := decodeLogFile(file)
	if err != nil {
		t.Fatalf("декодирование пустого журнала: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("число записей = %d, want 0", len(entries))
	}
	if validEnd != len(file) {
		t.Fatalf("validEnd = %d, want %d", validEnd, len(file))
	}
	scan, err := scanLogFile(file)
	if err != nil {
		t.Fatalf("структурный разбор пустого журнала: %v", err)
	}
	if scan.tail {
		t.Fatal("пустой журнал помечен как хвост")
	}
}

// TestEncodeLogBatchValidation проверяет отказ кодирования на недопустимых
// записях: индекс, терм, тип и разрыв индексов.
func TestEncodeLogBatchValidation(t *testing.T) {
	cases := []struct {
		name    string
		entries []contract.LogEntry
	}{
		{"отрицательный индекс", []contract.LogEntry{{Index: -1, Term: 0, Type: contract.LogNoop}}},
		{"отрицательный терм", []contract.LogEntry{{Index: 0, Term: -1, Type: contract.LogNoop}}},
		{"неизвестный тип", []contract.LogEntry{{Index: 0, Term: 0, Type: contract.LogType(9)}}},
		{"разрыв индексов", []contract.LogEntry{
			{Index: 0, Term: 0, Type: contract.LogNoop},
			{Index: 2, Term: 0, Type: contract.LogNoop},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := encodeLogBatch(tc.entries); err == nil {
				t.Fatal("ожидалась ошибка кодирования")
			}
		})
	}
}

// mustDecodeHex декодирует шестнадцатеричный вектор, завершая тест при
// ошибке.
func mustDecodeHex(t *testing.T, value string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	if err != nil {
		t.Fatalf("декодирование вектора: %v", err)
	}
	return decoded
}
