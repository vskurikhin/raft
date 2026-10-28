package store

// Файл пакетного журнала версии 3 (экспериментальный формат варианта D).
// Формат состоит из неизменяемого заголовка файла и последовательности
// пакетов; все многобайтовые числовые поля заголовков записаны в порядке
// big-endian, контрольная сумма — CRC-32C. От версии 2 отличаются только
// номер версии, сигнатуры пакета и footer и представление Data.
//
//	заголовок файла, 24 байта:
//	  0..7   magic = "RAFTPST\0"
//	  8..9   version = 3 (uint16)
//	  10..11 headerSize = 24 (uint16)
//	  12..19 reserved = 0 (8 байт)
//	  20..23 CRC-32C байт 0..19
//	пакет: заголовок 32 байта, тело и footer 12 байт
//	  заголовок пакета, 32 байта:
//	    0..3   magic = "RLB3"
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
//	    marker[8] = "RLCMIT3\0"
//
// Data версии 1: version uint8 = 1, tag uint8, затем payload; длину payload
// задаёт dataLength записи. Теги:
//
//	0 — nil interface, payload пуст;
//	1 — nil []byte, payload пуст;
//	2 — не-nil []byte (в том числе пустой); непустой только у записей,
//	    отличных от LogCommand;
//	3 — непустые байты LogCommand: []byte или носитель RaftCommandBytes;
//	4 — поток gob обёртки struct{ Data any } для прочих типов.
//
// Кодирование каноническое: одно и то же значение с тем же типом записи
// всегда даёт одни и те же байты, поэтому живой носитель и восстановленные
// байты команды сравниваются побайтово. Тип записи хранится в заголовке
// записи и при чтении Data проверяется. Байты тега 3 store не разбирает:
// их формат проверяет FSM приложения. Типы для тега 4 регистрирует
// потребитель. Кодек не выполняет ввода-вывода и не зависит от корневого
// пакета или пакета kvservice.
//
// CRC-32C — полином Кастаньоли:
// f(x) = x³² + x²⁸ + x²⁷ + x²⁶ + x²⁵ + x²³ + x²² + x²⁰ + x¹⁹ + x¹⁸ + x¹⁴ + x¹³ + x¹¹ + x¹⁰ + x⁹ + x⁸ + x⁶ + 1

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"reflect"
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
	logFileVersion uint16 = 3

	// logDataVersion — версия представления Data внутри записи.
	logDataVersion byte = 1

	// logDataHeaderSize — версия и тег перед payload Data.
	logDataHeaderSize = 2
)

// Теги Data версии 1.
const (
	// logDataTagNil — nil interface.
	logDataTagNil byte = 0

	// logDataTagNilBytes — nil []byte.
	logDataTagNilBytes byte = 1

	// logDataTagBytes — не-nil []byte.
	logDataTagBytes byte = 2

	// logDataTagCommand — непустые байты команды записи LogCommand.
	logDataTagCommand byte = 3

	// logDataTagGob — поток gob обёртки logDataEnvelope.
	logDataTagGob byte = 4
)

// commandBytesCarrier — узкий структурный контракт носителя готовых байтов
// команды. Метод вызывается под блокировками консенсус-модуля и хранилища,
// поэтому обязан только читать неизменяемое представление: без блокировок,
// каналов, ввода-вывода и вызовов Server/CM. Возвращённый срез хранилище
// только копирует в кадр и не изменяет.
type commandBytesCarrier interface {
	RaftCommandBytes() []byte
}

// logFileMagic — сигнатура заголовка файла: "RAFTPST" и завершающий ноль.
var logFileMagic = [8]byte{'R', 'A', 'F', 'T', 'P', 'S', 'T', 0x00}

// logBatchMagic — сигнатура заголовка пакета.
var logBatchMagic = [4]byte{'R', 'L', 'B', '3'}

// logCommitMarker — маркер конца пакета: "RLCMIT3" и завершающий ноль.
// Смысл слова COMMIT в маркере не связан с индексом фиксации Raft.
var logCommitMarker = [8]byte{'R', 'L', 'C', 'M', 'I', 'T', '3', 0x00}

var (
	// errLogCorrupt — маркерная ошибка повреждения журнала. Сюда относятся
	// неверные сигнатуры, версия, зарезервированные поля и контрольные суммы,
	// несогласованные длины и число записей, разрыв индексов, недопустимый
	// тип записи и недекодируемая Data.
	errLogCorrupt = errors.New("log corruption")

	// errLogIncomplete — ошибка, сигнализирующая об обрыве пакета:
	// объём данных в файле меньше, чем заявлено в заголовке пакета.
	// Допустима только для последнего пакета при условии, что до него
	// был успешно прочитан минимум один целый базовый пакет.
	// Позволяет отличать частичный хвост от повреждённых/неполных основных пакетов.
	errLogIncomplete = errors.New("incomplete log batch")
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

	// validEnd — граница валидного префикса в файле (в байтах).
	// Если файл заканчивается ровно на границе пакета, validEnd == len(file).
	// Иначе validEnd указывает на начало неполного пакета, который не был включён в entries.
	// Это значение позволяет безопасно обрезать файл или продолжить чтение.
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
	encoded, err := encodeLogEntries(entries)
	if err != nil {
		return nil, err
	}
	return encodeLogBatchEncoded(encoded), nil
}

// encodeLogFile собирает новый файл журнала: заголовок и один пакет. Пустой
// набор записей даёт валидный пустой журнал из заголовка и пустого пакета.
func encodeLogFile(entries []contract.LogEntry) ([]byte, error) {
	encoded, err := encodeLogEntries(entries)
	if err != nil {
		return nil, err
	}
	return encodeLogFileEncoded(encoded), nil
}

// encodedLogEntry — запись журнала с уже закодированной Data. Кэш
// реализаций хранит такое представление и переиспользует его при
// пересборке пакета без повторного кодирования Data.
type encodedLogEntry struct {
	index int
	term  int
	typ   contract.LogType
	data  []byte
}

// encodeLogEntries проверяет записи и кодирует Data каждой из них с учётом
// типа записи. Валидация выполняется до выделения и кодирования.
func encodeLogEntries(entries []contract.LogEntry) ([]encodedLogEntry, error) {
	if err := checkLogEntries(entries); err != nil {
		return nil, err
	}
	encoded := make([]encodedLogEntry, len(entries))
	for i := range entries {
		entry := &entries[i]
		data, err := encodeLogData(entry.Type, entry.Data)
		if err != nil {
			return nil, fmt.Errorf("encode entry at index %d: %w", entry.Index, err)
		}
		encoded[i] = encodedLogEntry{
			index: entry.Index,
			term:  entry.Term,
			typ:   entry.Type,
			data:  data,
		}
	}
	return encoded, nil
}

// encodeLogBatchEncoded собирает полный пакет из записей с готовой Data.
// Вызывающий обязан заранее проверить записи: индексы идут подряд, тип и
// длины допустимы.
func encodeLogBatchEncoded(entries []encodedLogEntry) []byte {
	body := encodeLogBodyEncoded(entries)
	header := encodeLogBatchHeader(uint64(len(body)), uint64(len(entries)))
	footer := encodeLogFooter(header[:], body)
	packet := make([]byte, 0, logBatchHeaderSize+len(body)+logFooterSize)
	packet = append(packet, header[:]...)
	packet = append(packet, body...)
	packet = append(packet, footer[:]...)
	return packet
}

// encodeLogFileEncoded собирает файл журнала из заголовка и одного пакета
// записей с готовой Data.
func encodeLogFileEncoded(entries []encodedLogEntry) []byte {
	header := encodeLogFileHeader()
	packet := encodeLogBatchEncoded(entries)
	file := make([]byte, 0, len(header)+len(packet))
	file = append(file, header[:]...)
	file = append(file, packet...)
	return file
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

// encodeLogBodyEncoded собирает тело пакета из записей с готовой Data:
// для каждой записи служебные поля и уже закодированная Data.
func encodeLogBodyEncoded(entries []encodedLogEntry) []byte {
	size := 0
	for i := range entries {
		size += logEntryHeaderSize + len(entries[i].data)
	}
	body := make([]byte, 0, size)
	for i := range entries {
		entry := &entries[i]
		var record [logEntryHeaderSize]byte
		binary.BigEndian.PutUint64(record[0:8], uint64(entry.index))
		binary.BigEndian.PutUint64(record[8:16], uint64(entry.term))
		record[16] = byte(entry.typ)
		// Байты 17..23 — зарезервированное поле, остаётся нулевым.
		binary.BigEndian.PutUint64(record[24:32], uint64(len(entry.data)))
		body = append(body, record[:]...)
		body = append(body, entry.data...)
	}
	return body
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

// encodeLogData кодирует одну Data канонически по типу записи: nil, nil
// []byte и прочие []byte — без gob; непустые байты LogCommand и носитель
// готовых байтов — тегом 3; остальные значения — отдельным потоком gob
// (тег 4). Новый кодировщик на запись исключает межзаписное состояние.
func encodeLogData(typ contract.LogType, data any) ([]byte, error) {
	switch value := data.(type) {
	case nil:
		return logDataFrame(logDataTagNil, nil), nil
	case []byte:
		switch {
		case value == nil:
			return logDataFrame(logDataTagNilBytes, nil), nil
		case typ == contract.LogCommand && len(value) > 0:
			return logDataFrame(logDataTagCommand, value), nil
		default:
			return logDataFrame(logDataTagBytes, value), nil
		}
	}
	if carrier, ok := data.(commandBytesCarrier); ok && typ == contract.LogCommand {
		return encodeCommandBytes(carrier)
	}
	return encodeLogDataGob(data)
}

// encodeCommandBytes кодирует носитель готовых байтов тегом 3. Нулевой
// указатель и пустое представление отвергаются до вызова и до записи: они
// не превращаются ни в nil, ни в пустую команду.
func encodeCommandBytes(carrier commandBytesCarrier) ([]byte, error) {
	if isNilValue(carrier) {
		return nil, fmt.Errorf("encode log Data: nil %T command bytes carrier", carrier)
	}
	payload := carrier.RaftCommandBytes()
	if len(payload) == 0 {
		return nil, fmt.Errorf("encode log Data: %T has empty command bytes", carrier)
	}
	return logDataFrame(logDataTagCommand, payload), nil
}

// encodeLogDataGob кодирует прочую Data тегом 4: поток gob в обёртке с
// интерфейсным полем после версии и тега.
func encodeLogDataGob(data any) ([]byte, error) {
	var buf bytes.Buffer
	buf.Write([]byte{logDataVersion, logDataTagGob})
	if err := gob.NewEncoder(&buf).Encode(logDataEnvelope{Data: data}); err != nil {
		return nil, fmt.Errorf("encode log Data: %w", err)
	}
	return buf.Bytes(), nil
}

// logDataFrame собирает Data из версии, тега и копии payload.
func logDataFrame(tag byte, payload []byte) []byte {
	data := make([]byte, logDataHeaderSize+len(payload))
	data[0] = logDataVersion
	data[1] = tag
	copy(data[logDataHeaderSize:], payload)
	return data
}

// isNilValue сообщает, что интерфейс хранит нулевой указатель или иное
// нулевое ссылочное значение.
func isNilValue(value any) bool {
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan, reflect.Interface:
		return v.IsNil()
	default:
		return false
	}
}

// decodeLogData декодирует одну Data с учётом типа записи. Байтовые теги
// возвращают собственную копию payload. Неизвестная версия или тег, лишние
// байты у тегов 0/1, пустой тег 3, тег 3 вне LogCommand и неканонический
// непустой тег 2 у LogCommand — повреждение. Поток тега 4 декодируется
// новым декодером и обязан исчерпать payload ровно одним значением.
func decodeLogData(typ contract.LogType, data []byte) (any, error) {
	if len(data) < logDataHeaderSize {
		return nil, fmt.Errorf("decode log Data: %w: Data is %d bytes, shorter than the %d-byte header",
			errLogCorrupt, len(data), logDataHeaderSize)
	}
	if data[0] != logDataVersion {
		return nil, fmt.Errorf("decode log Data: %w: unsupported Data version %d", errLogCorrupt, data[0])
	}
	tag, payload := data[1], data[logDataHeaderSize:]
	switch tag {
	case logDataTagNil, logDataTagNilBytes:
		if len(payload) != 0 {
			return nil, fmt.Errorf("decode log Data: %w: Data tag %d has %d extra bytes", errLogCorrupt, tag, len(payload))
		}
		if tag == logDataTagNil {
			return nil, nil //nolint:nilnil // nil interface — допустимое значение Data тега 0, а не отсутствие результата
		}
		return []byte(nil), nil
	case logDataTagBytes:
		if typ == contract.LogCommand && len(payload) > 0 {
			return nil, fmt.Errorf("decode log Data: %w: non-canonical Data tag %d for non-empty command bytes",
				errLogCorrupt, tag)
		}
		return cloneBytes(payload), nil
	case logDataTagCommand:
		if typ != contract.LogCommand {
			return nil, fmt.Errorf("decode log Data: %w: Data tag %d in entry of type %d", errLogCorrupt, tag, typ)
		}
		if len(payload) == 0 {
			return nil, fmt.Errorf("decode log Data: %w: empty command bytes", errLogCorrupt)
		}
		return cloneBytes(payload), nil
	case logDataTagGob:
		return decodeLogDataGob(payload)
	default:
		return nil, fmt.Errorf("decode log Data: %w: unknown Data tag %d", errLogCorrupt, tag)
	}
}

// decodeLogDataGob декодирует поток gob тега 4. Новый декодер на каждую
// запись сохраняет независимость потоков; payload обязан быть исчерпан
// ровно одним значением, иначе это повреждение.
func decodeLogDataGob(payload []byte) (any, error) {
	reader := bytes.NewReader(payload)
	var envelope logDataEnvelope
	if err := gob.NewDecoder(reader).Decode(&envelope); err != nil {
		return nil, fmt.Errorf("decode log Data: %w", err)
	}
	if reader.Len() != 0 {
		return nil, fmt.Errorf("%w: Data contains %d extra bytes", errLogCorrupt, reader.Len())
	}
	return envelope.Data, nil
}

// cloneBytes возвращает не-nil копию: пустой срез остаётся пустым, а не nil.
func cloneBytes(payload []byte) []byte {
	out := make([]byte, len(payload))
	copy(out, payload)
	return out
}

// checkLogEntries проверяет записи перед кодированием: неотрицательные
// индекс и терм, допустимый тип и непрерывность индексов внутри пакета.
func checkLogEntries(entries []contract.LogEntry) error {
	for i := range entries {
		entry := &entries[i]
		if entry.Index < 0 {
			return fmt.Errorf("invalid log entry index %d", entry.Index)
		}
		if entry.Term < 0 {
			return fmt.Errorf("invalid log entry term %d", entry.Term)
		}
		if entry.Type < contract.LogCommand || entry.Type > contract.LogConfiguration {
			return fmt.Errorf("invalid log entry type %d", entry.Type)
		}
		if i > 0 && entry.Index != entries[i-1].Index+1 {
			return fmt.Errorf("log entry index gap: %d after %d",
				entry.Index, entries[i-1].Index)
		}
	}
	return nil
}

// scanLogFile структурно разбирает всё содержимое файла журнала, не
// декодируя Data: проверяются заголовок файла, заголовки и footer
// пакетов, контрольные суммы, длины, число записей, тип, зарезервированные
// поля, непрерывность индексов и полнота пакета. Ошибка возвращается при
// повреждении; неполный последний пакет после целой базы отбрасывается
// целиком и отмечается в результате. Неполный первый пакет и файл без
// пакета — повреждение.
func scanLogFile(data []byte) (logFileScan, error) {
	if len(data) < logFileHeaderSize {
		return logFileScan{}, fmt.Errorf("%w: file header is shorter than %d bytes",
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
				return logFileScan{}, fmt.Errorf("%w: incomplete first batch at offset %d",
					errLogCorrupt, offset)
			}
			scan.tail = true
			scan.validEnd = offset
			return scan, nil
		case logPacketCorrupt:
			return logFileScan{}, fmt.Errorf("%w: batch at offset %d", errLogCorrupt, offset)
		}
		if base && len(entries) == 0 {
			return logFileScan{}, fmt.Errorf("%w: empty batch after first batch at offset %d",
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
		return logFileScan{}, fmt.Errorf("%w: file contains no batches", errLogCorrupt)
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
			return 0, false, fmt.Errorf("%w: index gap: %d after %d",
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
		value, err := decodeLogData(raw.typ, raw.data)
		if err != nil {
			return nil, 0, fmt.Errorf("%w: entry at index %d: %w", errLogCorrupt, raw.index, err)
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

// parseLogPacket выполняет структурный разбор пакета: сначала полностью валидирует заголовок,
// затем (при достаточном объёме данных) разбирает тело и footer.
//
// Критическое разграничение ошибок:
//   - неверная контрольная сумма заголовка трактуется как повреждение данных (необработанный кейс),
//     а не как обрыв потока;
//   - errLogIncomplete сигнализирует строго о нехватке байт после валидного заголовка.
//
// Такой подход позволяет отличать повреждённые пакеты от естественного хвоста файла.
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
		return fmt.Errorf("%w: invalid log file signature", errLogCorrupt)
	}
	if version := binary.BigEndian.Uint16(header[8:10]); version != logFileVersion {
		return fmt.Errorf("%w: unsupported log version %d", errLogCorrupt, version)
	}
	if size := binary.BigEndian.Uint16(header[10:12]); size != logFileHeaderSize {
		return fmt.Errorf("%w: invalid file header size %d", errLogCorrupt, size)
	}
	if hasNonZero(header[12:20]) {
		return fmt.Errorf("%w: non-zero reserved file header field", errLogCorrupt)
	}
	want := binary.BigEndian.Uint32(header[20:24])
	if got := crc32.Checksum(header[0:20], _crc32c); got != want {
		return fmt.Errorf("%w: file header CRC mismatch", errLogCorrupt)
	}
	return nil
}

// checkLogBatchHeader проверяет сигнатуру, зарезервированные поля и CRC
// заголовка пакета.
func checkLogBatchHeader(header []byte) error {
	if !bytes.Equal(header[0:4], logBatchMagic[:]) {
		return fmt.Errorf("%w: invalid log batch signature", errLogCorrupt)
	}
	if binary.BigEndian.Uint32(header[4:8]) != 0 {
		return fmt.Errorf("%w: non-zero reserved batch header field", errLogCorrupt)
	}
	if binary.BigEndian.Uint32(header[28:32]) != 0 {
		return fmt.Errorf("%w: non-zero reserved batch header field", errLogCorrupt)
	}
	want := binary.BigEndian.Uint32(header[24:28])
	sum := crc32.Checksum(header[0:24], _crc32c)
	sum = crc32.Update(sum, _crc32c, header[28:32])
	if got := sum; got != want {
		return fmt.Errorf("%w: batch header CRC mismatch", errLogCorrupt)
	}
	return nil
}

// checkLogBatchSizes выполняет валидацию размеров батча ДО любых преобразований в int
// и до выделения памяти, чтобы исключить переполнение и некорректные операции.
//
// «Предел платформы» здесь — это math.MaxInt для текущей архитектуры (int32/int64).
// Проверки гарантируют, что:
//   - суммарный объём пакета (заголовок + тело + footer) ≤ math.MaxInt;
//   - число записей ≤ math.MaxInt, чтобы безопасно использовать его в индексах и слайсах.
//
// Это защищает от паник при аллокации и обеспечивает предсказуемую работу
// на любой поддерживаемой архитектуре.
func checkLogBatchSizes(bodyLength, entryCount uint64) error {
	if bodyLength > uint64(math.MaxInt)-logBatchHeaderSize-logFooterSize {
		return fmt.Errorf("%w: batch body length %d exceeds platform limit",
			errLogCorrupt, bodyLength)
	}
	if entryCount > bodyLength/logEntryHeaderSize {
		return fmt.Errorf("%w: entry count %d is inconsistent with body length %d",
			errLogCorrupt, entryCount, bodyLength)
	}
	if entryCount > uint64(math.MaxInt) {
		return fmt.Errorf("%w: entry count %d exceeds platform limit",
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
		return fmt.Errorf("%w: batch CRC mismatch", errLogCorrupt)
	}
	if !bytes.Equal(footer[4:12], logCommitMarker[:]) {
		return fmt.Errorf("%w: missing batch end marker", errLogCorrupt)
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
			return nil, fmt.Errorf("%w: entry %d is truncated", errLogCorrupt, i)
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
			return nil, fmt.Errorf("%w: entry %d Data length exceeds remaining body",
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
		return nil, fmt.Errorf("%w: batch body contains extra bytes", errLogCorrupt)
	}
	return entries, nil
}

// checkLogEntryFields проверяет служебные поля записи: неотрицательные и
// представимые int индекс и терм, известный тип и нулевое зарезервированное
// поле.
func checkLogEntryFields(index, term int64, logType contract.LogType, reserved []byte) error {
	if index < 0 || index > math.MaxInt {
		return fmt.Errorf("%w: invalid entry index %d", errLogCorrupt, index)
	}
	if term < 0 || term > math.MaxInt {
		return fmt.Errorf("%w: invalid entry term %d", errLogCorrupt, term)
	}
	if logType < contract.LogCommand || logType > contract.LogConfiguration {
		return fmt.Errorf("%w: invalid entry type %d", errLogCorrupt, logType)
	}
	if hasNonZero(reserved) {
		return fmt.Errorf("%w: non-zero reserved entry field", errLogCorrupt)
	}
	return nil
}

// hasNonZero сообщает, есть ли среди байт хотя бы один ненулевой.
func hasNonZero(data []byte) bool {
	return slices.ContainsFunc(data, func(value byte) bool { return value != 0 })
}
