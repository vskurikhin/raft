package kvservice

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
)

// Формат команды версии 1. Заголовок: магическое число, версия, вид
// команды, резерв, ID и три длины строк в порядке Key, Value,
// CompareValue. Следом идут ровно байты этих строк. Целые — big-endian.
// Поля результата не входят в формат: их вычисляет машина состояний.
const (
	_commandMagic          = "KVC1"
	_commandVersion        = 1
	_commandHeaderSize     = 40
	_commandOffsetVersion  = 4
	_commandOffsetKind     = 5
	_commandOffsetReserved = 6
	_commandOffsetID       = 8
	_commandOffsetLengths  = 16
)

// errCommandEncoding — маркерная ошибка повреждённого или неподдерживаемого
// представления команды при декодировании.
var errCommandEncoding = errors.New("kvservice: invalid command encoding")

// EncodeCommand кодирует входные поля cmd в формат версии 1. Возвращает
// новый буфер, которым владеет вызывающий. Ошибка возможна только для
// команды, не прошедшей проверку validateCommand.
//
//nolint:gocritic // сигнатура по значению: кодек принимает неизменяемую копию команды.
func EncodeCommand(cmd Command) ([]byte, error) {
	size, err := validateCommand(&cmd)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, _commandHeaderSize, size)
	copy(buf, _commandMagic)
	buf[_commandOffsetVersion] = _commandVersion
	buf[_commandOffsetKind] = byte(cmd.Kind)
	binary.BigEndian.PutUint64(buf[_commandOffsetID:], uint64(int64(cmd.ID)))
	lengths := buf[_commandOffsetLengths:]
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
// результат не ссылается на data. Поля результата остаются нулевыми.
func DecodeCommand(data []byte) (Command, error) {
	if len(data) < _commandHeaderSize {
		return Command{}, fmt.Errorf("%w: %d bytes is shorter than the %d-byte header",
			errCommandEncoding, len(data), _commandHeaderSize)
	}
	if string(data[:len(_commandMagic)]) != _commandMagic {
		return Command{}, fmt.Errorf("%w: bad magic", errCommandEncoding)
	}
	if version := data[_commandOffsetVersion]; version != _commandVersion {
		return Command{}, fmt.Errorf("%w: unsupported version %d", errCommandEncoding, version)
	}
	kind := CommandKind(data[_commandOffsetKind])
	if !validCommandKind(kind) {
		return Command{}, fmt.Errorf("%w: unsupported command kind %d", errCommandEncoding, kind)
	}
	if reserved := binary.BigEndian.Uint16(data[_commandOffsetReserved:]); reserved != 0 {
		return Command{}, fmt.Errorf("%w: reserved field is %d, want 0", errCommandEncoding, reserved)
	}
	id := int64(binary.BigEndian.Uint64(data[_commandOffsetID:]))
	if !idFitsInt(id, strconv.IntSize) {
		return Command{}, fmt.Errorf("%w: ID %d does not fit in int", errCommandEncoding, id)
	}

	var lengths [3]int
	remaining := uint64(len(data) - _commandHeaderSize)
	for i := range lengths {
		n := binary.BigEndian.Uint64(data[_commandOffsetLengths+8*i:])
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

	body := data[_commandHeaderSize:]
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

// validateCommand проверяет вид команды и размер её представления, не
// создавая буфера. Возвращает полный размер представления. ID типа int
// на любой платформе Go представим в int64, отдельной проверки не нужно.
func validateCommand(cmd *Command) (int, error) {
	if !validCommandKind(cmd.Kind) {
		return 0, fmt.Errorf("kvservice: unsupported command kind %d", cmd.Kind)
	}
	return encodedCommandSize(len(cmd.Key), len(cmd.Value), len(cmd.CompareValue))
}

// encodedCommandSize возвращает размер представления со строками заданных
// длин. Сумма проверяется вычитанием, без переполнения.
func encodedCommandSize(lengths ...int) (int, error) {
	size := _commandHeaderSize
	for _, n := range lengths {
		if n > math.MaxInt-size {
			return 0, errors.New("kvservice: command encoding size overflows int")
		}
		size += n
	}
	return size, nil
}

// validCommandKind сообщает, что kind — одна из четырёх команд KV.
func validCommandKind(kind CommandKind) bool {
	return kind >= CommandGet && kind <= CommandDelete
}

// idFitsInt сообщает, что id помещается в знаковое целое разрядности bits.
func idFitsInt(id int64, bits int) bool {
	if bits >= 64 {
		return true
	}
	limit := int64(1) << (bits - 1)
	return id >= -limit && id < limit
}
