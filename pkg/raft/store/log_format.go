package store

// Файл пакетного журнала версии 2. Формат состоит из неизменяемого
// заголовка файла и последовательности пакетов; каждое число записано
// в порядке big-endian, контрольная сумма — CRC-32C (полином Кастаньоли).
//
//	заголовок файла, 24 байта:
//	  0..7   magic = "RAFTPST\0"
//	  8..9   version = 2 (uint16)
//	  10..11 headerSize = 24 (uint16)
//	  12..19 reserved = 0 (8 байт)
//	  20..23 CRC-32C байт 0..19
//	пакет: заголовок 32 байта, тело и footer 12 байт
//	  заголовок пакета, 32 байта:
//	    0..3   magic = "RLB2"
//	    4..7   reserved = 0
//	    8..15  bodyLength (uint64)
//	    16..23 entryCount (uint64)
//	    24..27 CRC-32C(байты 0..23 || байты 28..31)
//	    28..31 reserved = 0
//	  тело: entryCount записей по 32 байта плюс Data:
//	    index int64 | term int64 | type uint8 | reserved[7]=0 |
//	    dataLength uint64 | data
//	  footer, 12 байт:
//	    CRC-32C(весь заголовок пакета 32 байта || тело) uint32 |
//	    marker[8] = "RLCMIT2\0"
//
// Data кодируется отдельным потоком gob как обёртка struct{ Data any }:
// каждая запись независима, nil и встроенный []byte сохраняются, а типы
// пользователя регистрирует потребитель. Кодек не выполняет ввода-вывода
// и не зависит от корневого пакета или пакета службы KV.

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"slices"

	"github.com/vskurikhin/raft/pkg/raft/contract"
)

const (
	// logFileHeaderSize — размер неизменяемого заголовка файла журнала.
	logFileHeaderSize = 24

	// logBatchHeaderSize — размер заголовка пакета.
	logBatchHeaderSize = 32

	// logEntryHeaderSize — служебная часть одной записи без Data.
	logEntryHeaderSize = 32

	// logFooterSize — завершающая часть пакета: CRC и маркер.
	logFooterSize = 12

	// logFileVersion — версия формата журнала.
	logFileVersion uint16 = 2
)

// logFileMagic — сигнатура заголовка файла: "RAFTPST" и завершающий ноль.
var logFileMagic = [8]byte{'R', 'A', 'F', 'T', 'P', 'S', 'T', 0x00}

// logBatchMagic — сигнатура заголовка пакета.
var logBatchMagic = [4]byte{'R', 'L', 'B', '2'}

// logCommitMarker — маркер конца пакета: "RLCMIT2" и завершающий ноль.
// Смысл слова COMMIT в маркере не связан с индексом фиксации Raft.
var logCommitMarker = [8]byte{'R', 'L', 'C', 'M', 'I', 'T', '2', 0x00}

var (
	// errLogCorrupt — маркерная ошибка повреждения журнала. Сюда относятся
	// неверные сигнатуры, версия, зарезервированные поля и контрольные суммы,
	// несогласованные длины и число записей, разрыв индексов, недопустимый
	// тип записи и недекодируемая Data.
	errLogCorrupt = errors.New("повреждение журнала")

	// errLogIncomplete — внутренняя ошибка обрыва пакета: до конца файла не
	// хватает байт, обещанных полным и валидным заголовком пакета. Применима
	// только к последнему пакету после хотя бы одного целого базового пакета.
	errLogIncomplete = errors.New("незавершённый пакет журнала")
)

// logPacketClass — классификация разбора одного пакета.
type logPacketClass int

const (
	// logPacketComplete — пакет разобран и проверен целиком.
	logPacketComplete logPacketClass = iota

	// logPacketIncompleteTail — пакет оборван; годный префикс кончается на
	// его начале.
	logPacketIncompleteTail

	// logPacketCorrupt — полный, но неверный пакет либо недопустимое
	// сочетание полей.
	logPacketCorrupt
)

// logEntryRaw — запись после структурной проверки пакета: числовые поля
// разобраны, Data оставлена в закодированном виде. data ссылается на
// исходный буфер файла и годна только в пределах его времени жизни.
type logEntryRaw struct {
	index int
	term  int
	typ   contract.LogType
	data  []byte
}

// logFileScan — результат структурного разбора файла журнала.
type logFileScan struct {
	// entries — записи всех целых пакетов.
	entries []logEntryRaw

	// validEnd — смещение конца годного префикса: равно длине файла, если
	// файл кончается на границе пакета, и началу отброшенного неполного
	// пакета иначе.
	validEnd int

	// tail — признак отброшенного неполного последнего пакета.
	tail bool
}

// logDataEnvelope — обёртка одной записи Data. Поле экспортировано: gob
// кодирует только экспортированные поля и через интерфейсное поле сохраняет
// конкретный тип.
type logDataEnvelope struct {
	Data any
}

// encodeLogFileHeader возвращает неизменяемый 24-байтовый заголовок файла
// журнала. Заголовок не зависит от содержимого и никогда не перезаписывается
// после публикации файла.
func encodeLogFileHeader() [logFileHeaderSize]byte {
	var header [logFileHeaderSize]byte
	copy(header[0:8], logFileMagic[:])
	binary.BigEndian.PutUint16(header[8:10], logFileVersion)
	binary.BigEndian.PutUint16(header[10:12], logFileHeaderSize)
	// Байты 12..19 — зарезервированное поле, остаётся нулевым.
	binary.BigEndian.PutUint32(header[20:24], crc32.Checksum(header[0:20], _crc32c))
	return header
}

// encodeLogBatch собирает один полный пакет: заголовок, тело записей и
// footer. Валидация записей выполняется до выделения и кодирования Data.
func encodeLogBatch(entries []contract.LogEntry) ([]byte, error) {
	if err := checkLogEntries(entries); err != nil {
		return nil, err
	}
	body, err := encodeLogBody(entries)
	if err != nil {
		return nil, err
	}
	header := encodeLogBatchHeader(uint64(len(body)), uint64(len(entries)))
	footer := encodeLogFooter(header[:], body)
	packet := make([]byte, 0, logBatchHeaderSize+len(body)+logFooterSize)
	packet = append(packet, header[:]...)
	packet = append(packet, body...)
	packet = append(packet, footer[:]...)
	return packet, nil
}

// encodeLogFile собирает новый файл журнала: заголовок и один пакет. Пустой
// набор записей даёт валидный пустой журнал из заголовка и пустого пакета.
func encodeLogFile(entries []contract.LogEntry) ([]byte, error) {
	header := encodeLogFileHeader()
	packet, err := encodeLogBatch(entries)
	if err != nil {
		return nil, err
	}
	file := make([]byte, 0, len(header)+len(packet))
	file = append(file, header[:]...)
	file = append(file, packet...)
	return file, nil
}

// encodeLogBatchHeader собирает заголовок пакета: сигнатуру, нулевые
// зарезервированные поля, длину тела, число записей и CRC байт 0..23 и
// 28..31.
func encodeLogBatchHeader(bodyLength, entryCount uint64) [logBatchHeaderSize]byte {
	var header [logBatchHeaderSize]byte
	copy(header[0:4], logBatchMagic[:])
	// Байты 4..7 — зарезервированное поле, остаётся нулевым.
	binary.BigEndian.PutUint64(header[8:16], bodyLength)
	binary.BigEndian.PutUint64(header[16:24], entryCount)
	// Байты 28..31 — зарезервированное поле, остаётся нулевым.
	sum := crc32.Checksum(header[0:24], _crc32c)
	sum = crc32.Update(sum, _crc32c, header[28:32])
	binary.BigEndian.PutUint32(header[24:28], sum)
	return header
}

// encodeLogBody собирает тело пакета: для каждой записи служебные поля и
// отдельный поток gob с Data.
func encodeLogBody(entries []contract.LogEntry) ([]byte, error) {
	var body bytes.Buffer
	for i := range entries {
		entry := &entries[i]
		data, err := encodeLogData(entry.Data)
		if err != nil {
			return nil, fmt.Errorf("кодирование записи с индексом %d: %w", entry.Index, err)
		}
		var record [logEntryHeaderSize]byte
		binary.BigEndian.PutUint64(record[0:8], uint64(entry.Index))
		binary.BigEndian.PutUint64(record[8:16], uint64(entry.Term))
		record[16] = byte(entry.Type)
		// Байты 17..23 — зарезервированное поле, остаётся нулевым.
		binary.BigEndian.PutUint64(record[24:32], uint64(len(data)))
		body.Write(record[:])
		body.Write(data)
	}
	return body.Bytes(), nil
}

// encodeLogFooter собирает footer пакета: CRC всего заголовка пакета и тела,
// затем маркер конца.
func encodeLogFooter(header, body []byte) [logFooterSize]byte {
	var footer [logFooterSize]byte
	sum := crc32.Checksum(header, _crc32c)
	sum = crc32.Update(sum, _crc32c, body)
	binary.BigEndian.PutUint32(footer[0:4], sum)
	copy(footer[4:12], logCommitMarker[:])
	return footer
}

// encodeLogData кодирует одну Data отдельным потоком gob в обёртке с
// интерфейсным полем. Новый кодировщик на запись исключает межзаписное
// состояние; nil кодируется как пустое интерфейсное значение.
func encodeLogData(data any) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(logDataEnvelope{Data: data}); err != nil {
		return nil, fmt.Errorf("кодирование Data журнала: %w", err)
	}
	return buf.Bytes(), nil
}

// decodeLogData декодирует одну Data тем же потоком gob. Новый декодер на
// каждую запись сохраняет независимость потоков; Data-область обязана быть
// исчерпана ровно одним значением, иначе это повреждение.
func decodeLogData(data []byte) (any, error) {
	reader := bytes.NewReader(data)
	var envelope logDataEnvelope
	if err := gob.NewDecoder(reader).Decode(&envelope); err != nil {
		return nil, fmt.Errorf("декодирование Data журнала: %w", err)
	}
	if reader.Len() != 0 {
		return nil, fmt.Errorf("%w: Data содержит %d лишних байт", errLogCorrupt, reader.Len())
	}
	return envelope.Data, nil
}

// checkLogEntries проверяет записи перед кодированием: неотрицательные
// индекс и терм, допустимый тип и непрерывность индексов внутри пакета.
func checkLogEntries(entries []contract.LogEntry) error {
	for i := range entries {
		entry := &entries[i]
		if entry.Index < 0 {
			return fmt.Errorf("недопустимый индекс записи журнала %d", entry.Index)
		}
		if entry.Term < 0 {
			return fmt.Errorf("недопустимый терм записи журнала %d", entry.Term)
		}
		if entry.Type < contract.LogCommand || entry.Type > contract.LogConfiguration {
			return fmt.Errorf("недопустимый тип записи журнала %d", entry.Type)
		}
		if i > 0 && entry.Index != entries[i-1].Index+1 {
			return fmt.Errorf("разрыв индексов записей журнала: %d после %d",
				entry.Index, entries[i-1].Index)
		}
	}
	return nil
}

// scanLogFile структурно разбирает всё содержимое файла журнала, не
// декодируя gob Data: проверяются заголовок файла, заголовки и footer
// пакетов, контрольные суммы, длины, число записей, тип, зарезервированные
// поля, непрерывность индексов и полнота пакета. Ошибка возвращается при
// повреждении; неполный последний пакет после целой базы отбрасывается
// целиком и отмечается в результате. Неполный первый пакет и файл без
// пакета — повреждение.
func scanLogFile(data []byte) (logFileScan, error) {
	if len(data) < logFileHeaderSize {
		return logFileScan{}, fmt.Errorf("%w: заголовок файла короче %d байт",
			errLogCorrupt, logFileHeaderSize)
	}
	if err := checkLogFileHeader(data[:logFileHeaderSize]); err != nil {
		return logFileScan{}, err
	}

	var scan logFileScan
	scan.validEnd = len(data)
	offset := logFileHeaderSize
	base := false
	var lastIndex int
	hasIndex := false
	for offset < len(data) {
		entries, size, class := decodeLogPacket(data[offset:])
		switch class {
		case logPacketIncompleteTail:
			if !base {
				return logFileScan{}, fmt.Errorf("%w: неполный первый пакет по смещению %d",
					errLogCorrupt, offset)
			}
			scan.tail = true
			scan.validEnd = offset
			return scan, nil
		case logPacketCorrupt:
			return logFileScan{}, fmt.Errorf("%w: пакет по смещению %d", errLogCorrupt, offset)
		}
		if base && len(entries) == 0 {
			return logFileScan{}, fmt.Errorf("%w: пустой пакет после первого по смещению %d",
				errLogCorrupt, offset)
		}
		updated, updatedHasIndex, err := checkLogIndexes(entries, lastIndex, hasIndex)
		if err != nil {
			return logFileScan{}, err
		}
		lastIndex = updated
		hasIndex = updatedHasIndex
		scan.entries = append(scan.entries, entries...)
		base = true
		offset += size
	}
	if !base {
		return logFileScan{}, fmt.Errorf("%w: файл не содержит ни одного пакета", errLogCorrupt)
	}
	return scan, nil
}

// checkLogIndexes проверяет непрерывность индексов записей пакета: первый
// индекс любой (≥0), каждый следующий ровно на единицу больше. lastIndex и
// hasIndex описывают последнюю индекс-запись, встреченную в файле до этого
// пакета. Возвращает обновлённое состояние.
func checkLogIndexes(entries []logEntryRaw, lastIndex int, hasIndex bool) (int, bool, error) {
	for i := range entries {
		if hasIndex && entries[i].index != lastIndex+1 {
			return 0, false, fmt.Errorf("%w: разрыв индексов: %d после %d",
				errLogCorrupt, entries[i].index, lastIndex)
		}
		lastIndex = entries[i].index
		hasIndex = true
	}
	return lastIndex, hasIndex, nil
}

// decodeLogFile структурно разбирает файл и декодирует Data всех целых
// пакетов. Возвращает записи, смещение конца годного префикса и ошибку.
// Неполный последний пакет не даёт ни одной записи, а его начало — это
// validEnd. Неполный первый пакет — ошибка.
func decodeLogFile(data []byte) ([]contract.LogEntry, int, error) {
	scan, err := scanLogFile(data)
	if err != nil {
		return nil, 0, err
	}
	entries := make([]contract.LogEntry, 0, len(scan.entries))
	for i := range scan.entries {
		raw := &scan.entries[i]
		value, err := decodeLogData(raw.data)
		if err != nil {
			return nil, 0, fmt.Errorf("%w: запись с индексом %d: %w", errLogCorrupt, raw.index, err)
		}
		entries = append(entries, contract.LogEntry{
			Index: raw.index,
			Term:  raw.term,
			Type:  raw.typ,
			Data:  value,
		})
	}
	return entries, scan.validEnd, nil
}

// decodeLogPacket разбирает один пакет в начале data и возвращает класс
// результата. complete требует валидных записей и размера; incompleteTail
// означает обрыв по концу файла; corrupt — полный, но неверный пакет.
func decodeLogPacket(data []byte) ([]logEntryRaw, int, logPacketClass) {
	entries, size, err := parseLogPacket(data)
	switch {
	case err == nil:
		return entries, size, logPacketComplete
	case errors.Is(err, errLogIncomplete):
		return nil, 0, logPacketIncompleteTail
	default:
		return nil, 0, logPacketCorrupt
	}
}

// parseLogPacket проверяет полный заголовок пакета и при достатке байт
// разбирает тело и footer. Заголовок проверяется целиком до чтения объявленной
// длины: неверная контрольная сумма заголовка — повреждение, а не обрыв.
// errLogIncomplete возвращается только тогда, когда полный и валидный
// заголовок обещает больше байт, чем осталось до конца.
func parseLogPacket(data []byte) ([]logEntryRaw, int, error) {
	if len(data) < logBatchHeaderSize {
		return nil, 0, errLogIncomplete
	}
	header := data[:logBatchHeaderSize]
	if err := checkLogBatchHeader(header); err != nil {
		return nil, 0, err
	}
	bodyLength := binary.BigEndian.Uint64(header[8:16])
	entryCount := binary.BigEndian.Uint64(header[16:24])
	if err := checkLogBatchSizes(bodyLength, entryCount); err != nil {
		return nil, 0, err
	}
	total := logBatchHeaderSize + int(bodyLength) + logFooterSize
	if total > len(data) {
		return nil, 0, errLogIncomplete
	}
	body := data[logBatchHeaderSize : logBatchHeaderSize+int(bodyLength)]
	footer := data[logBatchHeaderSize+int(bodyLength) : total]
	// Контрольные суммы полного кадра проверяются до декодирования Data.
	if err := checkLogFooter(header, body, footer); err != nil {
		return nil, 0, err
	}
	entries, err := parseLogBody(body, entryCount)
	if err != nil {
		return nil, 0, err
	}
	return entries, total, nil
}

// checkLogFileHeader проверяет сигнатуру, версию, размер заголовка,
// зарезервированное поле и контрольную сумму заголовка файла.
func checkLogFileHeader(header []byte) error {
	if !bytes.Equal(header[0:8], logFileMagic[:]) {
		return fmt.Errorf("%w: неверная сигнатура файла журнала", errLogCorrupt)
	}
	if version := binary.BigEndian.Uint16(header[8:10]); version != logFileVersion {
		return fmt.Errorf("%w: неподдерживаемая версия журнала %d", errLogCorrupt, version)
	}
	if size := binary.BigEndian.Uint16(header[10:12]); size != logFileHeaderSize {
		return fmt.Errorf("%w: неверный размер заголовка файла %d", errLogCorrupt, size)
	}
	if hasNonZero(header[12:20]) {
		return fmt.Errorf("%w: ненулевое зарезервированное поле заголовка файла", errLogCorrupt)
	}
	want := binary.BigEndian.Uint32(header[20:24])
	if got := crc32.Checksum(header[0:20], _crc32c); got != want {
		return fmt.Errorf("%w: несовпадение CRC заголовка файла", errLogCorrupt)
	}
	return nil
}

// checkLogBatchHeader проверяет сигнатуру, зарезервированные поля и CRC
// заголовка пакета.
func checkLogBatchHeader(header []byte) error {
	if !bytes.Equal(header[0:4], logBatchMagic[:]) {
		return fmt.Errorf("%w: неверная сигнатура пакета журнала", errLogCorrupt)
	}
	if binary.BigEndian.Uint32(header[4:8]) != 0 {
		return fmt.Errorf("%w: ненулевое зарезервированное поле заголовка пакета", errLogCorrupt)
	}
	if binary.BigEndian.Uint32(header[28:32]) != 0 {
		return fmt.Errorf("%w: ненулевое зарезервированное поле заголовка пакета", errLogCorrupt)
	}
	want := binary.BigEndian.Uint32(header[24:28])
	sum := crc32.Checksum(header[0:24], _crc32c)
	sum = crc32.Update(sum, _crc32c, header[28:32])
	if got := sum; got != want {
		return fmt.Errorf("%w: несовпадение CRC заголовка пакета", errLogCorrupt)
	}
	return nil
}

// checkLogBatchSizes проверяет длину тела и число записей до выделения и
// преобразования в int: тело должно помещаться в предел платформы вместе с
// заголовком и footer, а число записей не превосходить тело, делённое на
// минимальный размер записи, и предел платформы.
func checkLogBatchSizes(bodyLength, entryCount uint64) error {
	if bodyLength > uint64(math.MaxInt)-logBatchHeaderSize-logFooterSize {
		return fmt.Errorf("%w: длина тела пакета %d превышает предел платформы",
			errLogCorrupt, bodyLength)
	}
	if entryCount > bodyLength/logEntryHeaderSize {
		return fmt.Errorf("%w: число записей %d не соответствует длине тела %d",
			errLogCorrupt, entryCount, bodyLength)
	}
	if entryCount > uint64(math.MaxInt) {
		return fmt.Errorf("%w: число записей %d превышает предел платформы",
			errLogCorrupt, entryCount)
	}
	return nil
}

// checkLogFooter проверяет CRC всего заголовка пакета вместе с телом и
// маркер конца пакета.
func checkLogFooter(header, body, footer []byte) error {
	want := binary.BigEndian.Uint32(footer[0:4])
	sum := crc32.Checksum(header, _crc32c)
	sum = crc32.Update(sum, _crc32c, body)
	if got := sum; got != want {
		return fmt.Errorf("%w: несовпадение CRC пакета", errLogCorrupt)
	}
	if !bytes.Equal(footer[4:12], logCommitMarker[:]) {
		return fmt.Errorf("%w: отсутствует маркер конца пакета", errLogCorrupt)
	}
	return nil
}

// parseLogBody разбирает тело пакета: ровно entryCount записей, каждая со
// служебными полями и Data объявленной длины. Индексы и типы проверяются до
// возврата записей; лишние или недостающие байты тела — повреждение.
func parseLogBody(body []byte, entryCount uint64) ([]logEntryRaw, error) {
	entries := make([]logEntryRaw, 0, entryCount)
	offset := 0
	for i := range entryCount {
		if len(body)-offset < logEntryHeaderSize {
			return nil, fmt.Errorf("%w: запись %d обрезана", errLogCorrupt, i)
		}
		record := body[offset : offset+logEntryHeaderSize]
		index := int64(binary.BigEndian.Uint64(record[0:8]))
		term := int64(binary.BigEndian.Uint64(record[8:16]))
		logType := contract.LogType(record[16])
		dataLength := binary.BigEndian.Uint64(record[24:32])
		if err := checkLogEntryFields(index, term, logType, record[17:24]); err != nil {
			return nil, err
		}
		offset += logEntryHeaderSize
		if dataLength > uint64(len(body)-offset) {
			return nil, fmt.Errorf("%w: длина Data записи %d превышает остаток тела",
				errLogCorrupt, i)
		}
		end := offset + int(dataLength)
		entries = append(entries, logEntryRaw{
			index: int(index),
			term:  int(term),
			typ:   logType,
			data:  body[offset:end],
		})
		offset = end
	}
	if offset != len(body) {
		return nil, fmt.Errorf("%w: тело пакета содержит лишние байты", errLogCorrupt)
	}
	return entries, nil
}

// checkLogEntryFields проверяет служебные поля записи: неотрицательные и
// представимые int индекс и терм, известный тип и нулевое зарезервированное
// поле.
func checkLogEntryFields(index, term int64, logType contract.LogType, reserved []byte) error {
	if index < 0 || index > math.MaxInt {
		return fmt.Errorf("%w: недопустимый индекс записи %d", errLogCorrupt, index)
	}
	if term < 0 || term > math.MaxInt {
		return fmt.Errorf("%w: недопустимый терм записи %d", errLogCorrupt, term)
	}
	if logType < contract.LogCommand || logType > contract.LogConfiguration {
		return fmt.Errorf("%w: недопустимый тип записи %d", errLogCorrupt, logType)
	}
	if hasNonZero(reserved) {
		return fmt.Errorf("%w: ненулевое зарезервированное поле записи", errLogCorrupt)
	}
	return nil
}

// hasNonZero сообщает, есть ли среди байт хотя бы один ненулевой.
func hasNonZero(data []byte) bool {
	return slices.ContainsFunc(data, func(value byte) bool { return value != 0 })
}
