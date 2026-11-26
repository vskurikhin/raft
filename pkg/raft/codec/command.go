// Package codec содержит независимый кодек данных команд журнала Raft.
// Пакет зависит только от стандартной библиотеки и не знает ни доменных
// типов сервиса, ни журнала, ни транспорта: одни и те же готовые байты
// команды используются и для диска, и для сети.
package codec

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
)

// Виды команд кодека. Значения закреплены форматом версии 1.
const (
	KindGet    uint8 = 1
	KindPut    uint8 = 2
	KindCAS    uint8 = 3
	KindDelete uint8 = 4
)

// Command — транспортное представление полей команды без результатов:
// прикладной результат вычисляет машина состояний и в форму не входит.
type Command struct {
	Kind         uint8
	ID           int
	Key          string
	Value        string
	CompareValue string
}

// Формат команды версии 1. Заголовок: магическое число, версия, вид
// команды, резерв, ID и три длины строк в порядке Key, Value,
// CompareValue. Следом идут ровно байты этих строк. Целые — big-endian.
// Поля результата не входят в формат: их вычисляет машина состояний.
const (
	commandMagic          = "KVC1"
	commandVersion        = 1
	commandHeaderSize     = 40
	commandOffsetVersion  = 4
	commandOffsetKind     = 5
	commandOffsetReserved = 6
	commandOffsetID       = 8
	commandOffsetLengths  = 16
)

// errCommandEncoding — маркерная ошибка повреждённого или неподдерживаемого
// представления команды при декодировании.
var errCommandEncoding = errors.New("codec: invalid command encoding")

// EncodeCommand кодирует входные поля cmd в формат версии 1. Возвращает
// новый буфер, которым владеет вызывающий. Ошибка возможна только для
// неизвестного вида команды или размера, не помещающегося в int.
func EncodeCommand(cmd Command) ([]byte, error) {
	if !ValidKind(cmd.Kind) {
		return nil, fmt.Errorf("codec: unsupported command kind %d", cmd.Kind)
	}
	size, err := encodedCommandSize(len(cmd.Key), len(cmd.Value), len(cmd.CompareValue))
	if err != nil {
		return nil, err
	}
	buf := make([]byte, commandHeaderSize, size)
	copy(buf, commandMagic)
	buf[commandOffsetVersion] = commandVersion
	buf[commandOffsetKind] = cmd.Kind
	binary.BigEndian.PutUint64(buf[commandOffsetID:], uint64(int64(cmd.ID)))
	lengths := buf[commandOffsetLengths:]
	binary.BigEndian.PutUint64(lengths[0:], uint64(len(cmd.Key)))
	binary.BigEndian.PutUint64(lengths[8:], uint64(len(cmd.Value)))
	binary.BigEndian.PutUint64(lengths[16:], uint64(len(cmd.CompareValue)))
	buf = append(buf, cmd.Key...)
	buf = append(buf, cmd.Value...)
	buf = append(buf, cmd.CompareValue...)
	return buf, nil
}

// DecodeCommand строго декодирует представление версии 1. Все проверки
// выполняются до выделения памяти под строки; строки копируются, поэтому
// результат не ссылается на data.
func DecodeCommand(data []byte) (Command, error) {
	if len(data) < commandHeaderSize {
		return Command{}, fmt.Errorf("%w: %d bytes is shorter than the %d-byte header",
			errCommandEncoding, len(data), commandHeaderSize)
	}
	if string(data[:len(commandMagic)]) != commandMagic {
		return Command{}, fmt.Errorf("%w: bad magic", errCommandEncoding)
	}
	if version := data[commandOffsetVersion]; version != commandVersion {
		return Command{}, fmt.Errorf("%w: unsupported version %d", errCommandEncoding, version)
	}
	kind := data[commandOffsetKind]
	if !ValidKind(kind) {
		return Command{}, fmt.Errorf("%w: unsupported command kind %d", errCommandEncoding, kind)
	}
	if reserved := binary.BigEndian.Uint16(data[commandOffsetReserved:]); reserved != 0 {
		return Command{}, fmt.Errorf("%w: reserved field is %d, want 0", errCommandEncoding, reserved)
	}
	id := int64(binary.BigEndian.Uint64(data[commandOffsetID:]))
	if !idFitsInt(id, strconv.IntSize) {
		return Command{}, fmt.Errorf("%w: ID %d does not fit in int", errCommandEncoding, id)
	}

	var lengths [3]int
	remaining := uint64(len(data) - commandHeaderSize)
	for i := range lengths {
		n := binary.BigEndian.Uint64(data[commandOffsetLengths+8*i:])
		if n > remaining {
			return Command{}, fmt.Errorf("%w: string %d length %d exceeds remaining %d bytes",
				errCommandEncoding, i, n, remaining)
		}
		remaining -= n
		lengths[i] = int(n)
	}
	if remaining != 0 {
		return Command{}, fmt.Errorf("%w: %d trailing bytes", errCommandEncoding, remaining)
	}

	body := data[commandHeaderSize:]
	key, body := body[:lengths[0]], body[lengths[0]:]
	value, compare := body[:lengths[1]], body[lengths[1]:]
	return Command{
		Kind:         kind,
		Key:          string(key),
		Value:        string(value),
		CompareValue: string(compare),
		ID:           int(id),
	}, nil
}

// ValidKind сообщает, что kind — один из четырёх поддерживаемых видов.
func ValidKind(kind uint8) bool {
	return kind >= KindGet && kind <= KindDelete
}

// encodedCommandSize возвращает размер представления со строками заданных
// длин. Сумма проверяется вычитанием, без переполнения.
func encodedCommandSize(lengths ...int) (int, error) {
	size := commandHeaderSize
	for _, n := range lengths {
		if n > math.MaxInt-size {
			return 0, errors.New("codec: command encoding size overflows int")
		}
		size += n
	}
	return size, nil
}

// idFitsInt сообщает, что id помещается в знаковое целое разрядности bits.
func idFitsInt(id int64, bits int) bool {
	if bits >= 64 {
		return true
	}
	limit := int64(1) << (bits - 1)
	return id >= -limit && id < limit
}
