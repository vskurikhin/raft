package protocol

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"math"
)

// Вид Data записи журнала в сетевом формате.
const (
	dataKindNil uint8 = 0
	dataKindGob uint8 = 1
)

// minDataCapacity — начальная ёмкость буфера кодированной Data.
const minDataCapacity = 64

// dataEnvelope — анонимная обёртка ненулевой Data: gob-поток содержит ровно
// одно значение этой обёртки, поэтому конкретный тип Data сохраняется.
type dataEnvelope = struct{ Data any }

var errDataTooLarge = errors.New("protocol: encoded data limit reached")

// EncodeData кодирует ненулевую Data новым gob-кодировщиком в независимый
// поток не длиннее maxBytes. Настоящий nil даёт пустой результат (вид nil).
// maxBytes = 0 не означает отсутствия предела: возвращается ErrLimit, в том
// числе для nil. Превышение предела — ErrLimit, ошибка gob — ErrData.
func EncodeData(data any, maxBytes uint64) ([]byte, error) {
	if maxBytes == 0 {
		return nil, fmt.Errorf("%w: data limit must be positive", ErrLimit)
	}
	if data == nil {
		return nil, nil
	}

	out := boundedBuffer{limit: clampToInt(maxBytes)}
	if err := gob.NewEncoder(&out).Encode(dataEnvelope{Data: data}); err != nil {
		if out.exceeded {
			return nil, fmt.Errorf("%w: encoded data exceeds %d bytes", ErrLimit, maxBytes)
		}

		return nil, fmt.Errorf("%w: encode %T: %w", ErrData, data, err)
	}

	return out.buf, nil
}

// MeasureData возвращает сетевую длину Data, вычисленную тем же кодированием,
// что и EncodeData, с теми же ошибками.
func MeasureData(data any, maxBytes uint64) (uint64, error) {
	encoded, err := EncodeData(data, maxBytes)
	if err != nil {
		return 0, err
	}

	return uint64(len(encoded)), nil
}

// decodeData восстанавливает ненулевую Data из единственного gob-потока.
// Успех требует одновременно: стандартный декодер прочитал значение без
// ошибки, Data — ненулевой interface, а bytes.Reader исчерпан после первого
// значения; иначе возвращается ErrData. Внутренняя грамматика gob (точка
// конца значения во внутреннем буфере декодера, необязательные терминаторы
// и игнорируемые им счётчики) не проверяется: байты, поглощённые декодером,
// не считаются внешним остатком. Результат не ссылается на payload.
func decodeData(payload []byte) (any, error) {
	reader := bytes.NewReader(payload)

	var envelope dataEnvelope
	if err := gob.NewDecoder(reader).Decode(&envelope); err != nil {
		return nil, fmt.Errorf("%w: decode: %w", ErrData, err)
	}
	if reader.Len() != 0 {
		return nil, fmt.Errorf("%w: %d trailing bytes after data value", ErrData, reader.Len())
	}
	if envelope.Data == nil {
		return nil, fmt.Errorf("%w: gob data kind decodes to nil", ErrData)
	}

	return envelope.Data, nil
}

// boundedBuffer — writer, который отказывает до расширения буфера сверх
// limit; ёмкость буфера никогда не превышает limit.
type boundedBuffer struct {
	buf      []byte
	limit    int
	exceeded bool
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if len(p) > b.limit-len(b.buf) {
		b.exceeded = true

		return 0, errDataTooLarge
	}

	need := len(b.buf) + len(p)
	if need > cap(b.buf) {
		b.grow(need)
	}
	b.buf = append(b.buf, p...)

	return len(p), nil
}

func (b *boundedBuffer) grow(need int) {
	newCap := max(need, minDataCapacity)
	if doubled := cap(b.buf); doubled <= b.limit/2 {
		newCap = max(newCap, 2*doubled)
	} else {
		newCap = b.limit
	}
	newCap = min(newCap, b.limit)

	grown := make([]byte, len(b.buf), newCap)
	copy(grown, b.buf)
	b.buf = grown
}

func clampToInt(value uint64) int {
	if value > math.MaxInt {
		return math.MaxInt
	}

	return int(value)
}
