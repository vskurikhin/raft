package protocol

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/vskurikhin/raft/pkg/raft/contract"
)

// Ширина полей тела кадра.
const (
	int64Bytes  = 8
	uint32Bytes = 4
	uint16Bytes = 2
)

// intRange — допустимый диапазон целого поля; верхняя и нижняя границы
// учитывают разрядность int текущей платформы.
type intRange struct {
	low  int64
	high int64
}

var (
	rangeAnyInt      = intRange{low: math.MinInt, high: math.MaxInt}
	rangeNonNegative = intRange{low: 0, high: math.MaxInt}
	rangeMinusOne    = intRange{low: -1, high: math.MaxInt}
)

func checkRange(field string, value int64, valid intRange) error {
	if value < valid.low || value > valid.high {
		return fmt.Errorf("%w: %s = %d outside [%d, %d]", ErrRange, field, value, valid.low, valid.high)
	}

	return nil
}

// Имена полей, общих для нескольких сообщений.
const (
	fieldTerm         = "Term"
	fieldLastLogIndex = "LastLogIndex"
	fieldLastLogTerm  = "LastLogTerm"
)

// intField — целое поле доменной структуры для проверки перед кодированием.
// Поля без сужения диапазона (идентификаторы) не проверяются: любое значение
// Go int представимо в i64.
type intField struct {
	name  string
	value int
	valid intRange
}

func checkIntFields(fields ...intField) error {
	for _, field := range fields {
		if err := checkRange(field.name, int64(field.value), field.valid); err != nil {
			return err
		}
	}

	return nil
}

// checkEncodeHeader проверяет общий заголовок RPC-сообщения перед
// кодированием: версия протокола несовместима — contract.ErrUnsupportedProtocol.
func checkEncodeHeader(message string, header contract.RPCHeader) error {
	if header.ProtocolVersion != contract.ProtocolVersion {
		return fmt.Errorf("protocol: %s: ProtocolVersion %d: %w", message, header.ProtocolVersion, contract.ErrUnsupportedProtocol)
	}

	return nil
}

func appendInt(dst []byte, value int) []byte {
	return binary.BigEndian.AppendUint64(dst, uint64(int64(value)))
}

func appendUint64(dst []byte, value uint64) []byte {
	return binary.BigEndian.AppendUint64(dst, value)
}

func appendBool(dst []byte, value bool) []byte {
	if value {
		return append(dst, 1)
	}

	return append(dst, 0)
}

func appendRPCHeader(dst []byte, header contract.RPCHeader) []byte {
	dst = appendInt(dst, header.ProtocolVersion)

	return appendInt(dst, header.ServerID)
}

// bodyDecoder последовательно читает поля тела кадра. Первая ошибка
// сохраняется; последующие чтения возвращают нулевые значения.
type bodyDecoder struct {
	body   []byte
	offset int
	err    error
}

func (d *bodyDecoder) remaining() int {
	return len(d.body) - d.offset
}

func (d *bodyDecoder) take(field string, n int) []byte {
	if d.err != nil {
		return nil
	}
	if n < 0 || n > d.remaining() {
		d.err = fmt.Errorf("%w: %s needs %d bytes, %d remain", ErrFormat, field, n, d.remaining())

		return nil
	}

	chunk := d.body[d.offset : d.offset+n : d.offset+n]
	d.offset += n

	return chunk
}

func (d *bodyDecoder) readByte(field string) uint8 {
	chunk := d.take(field, 1)
	if chunk == nil {
		return 0
	}

	return chunk[0]
}

func (d *bodyDecoder) readUint16(field string) uint16 {
	chunk := d.take(field, uint16Bytes)
	if chunk == nil {
		return 0
	}

	return binary.BigEndian.Uint16(chunk)
}

func (d *bodyDecoder) readUint32(field string) uint32 {
	chunk := d.take(field, uint32Bytes)
	if chunk == nil {
		return 0
	}

	return binary.BigEndian.Uint32(chunk)
}

func (d *bodyDecoder) readUint64(field string) uint64 {
	chunk := d.take(field, int64Bytes)
	if chunk == nil {
		return 0
	}

	return binary.BigEndian.Uint64(chunk)
}

// readInt читает i64 и проверяет диапазон до преобразования в int.
func (d *bodyDecoder) readInt(field string, valid intRange) int {
	chunk := d.take(field, int64Bytes)
	if chunk == nil {
		return 0
	}

	value := int64(binary.BigEndian.Uint64(chunk))
	if err := checkRange(field, value, valid); err != nil {
		d.err = err

		return 0
	}

	return int(value)
}

func (d *bodyDecoder) readBool(field string) bool {
	value := d.readByte(field)
	if d.err != nil {
		return false
	}
	if value > 1 {
		d.err = fmt.Errorf("%w: %s = %d is not a boolean", ErrFormat, field, value)

		return false
	}

	return value == 1
}

// readRPCHeader читает общий заголовок RPC-сообщения. Несовместимая версия
// протокола — contract.ErrUnsupportedProtocol.
func (d *bodyDecoder) readRPCHeader() contract.RPCHeader {
	version := d.readUint64("ProtocolVersion")
	if d.err != nil {
		return contract.RPCHeader{}
	}
	if int64(version) != contract.ProtocolVersion {
		d.err = fmt.Errorf("protocol: ProtocolVersion %d: %w", int64(version), contract.ErrUnsupportedProtocol)

		return contract.RPCHeader{}
	}

	return contract.RPCHeader{
		ProtocolVersion: contract.ProtocolVersion,
		ServerID:        d.readInt("ServerID", rangeAnyInt),
	}
}

// finish требует точного исчерпания тела.
func (d *bodyDecoder) finish() error {
	if d.err != nil {
		return d.err
	}
	if rest := d.remaining(); rest != 0 {
		return fmt.Errorf("%w: %d trailing body bytes", ErrFormat, rest)
	}

	return nil
}

// frameBuilder дописывает кадр к префиксу вызывающего; ёмкость, выделенная
// кодеком, не превышает start+MaxFrameBytes.
type frameBuilder struct {
	buf   []byte
	start int
	limit int
}

func newFrameBuilder(dst []byte, limits Limits) (frameBuilder, error) {
	start := len(dst)
	if uint64(start) > math.MaxInt-limits.MaxFrameBytes {
		return frameBuilder{}, fmt.Errorf("%w: prefix %d bytes plus MaxFrameBytes %d overflows int",
			ErrRange, start, limits.MaxFrameBytes)
	}

	return frameBuilder{buf: dst, start: start, limit: start + int(limits.MaxFrameBytes)}, nil
}

// reserve гарантирует ёмкость ещё для n байт кадра, не выходя за предел кадра:
// первое выделение точно по запросу, дальше — удвоение, ограниченное F.
func (b *frameBuilder) reserve(n uint64) error {
	used := len(b.buf) - b.start
	if n > uint64(b.limit-len(b.buf)) {
		return fmt.Errorf("%w: frame %d+%d bytes exceeds MaxFrameBytes %d",
			ErrLimit, used, n, b.limit-b.start)
	}

	need := len(b.buf) + int(n)
	if need <= cap(b.buf) {
		return nil
	}

	maxFrame := b.limit - b.start
	frameCap := need - b.start
	if current := cap(b.buf) - b.start; current > maxFrame/2 {
		frameCap = maxFrame
	} else {
		frameCap = max(frameCap, 2*current)
	}
	newCap := b.start + min(frameCap, maxFrame)

	grown := make([]byte, len(b.buf), newCap)
	copy(grown, b.buf)
	b.buf = grown

	return nil
}

// finishFrame записывает фактическую длину тела в заголовок кадра.
func (b *frameBuilder) finishFrame() []byte {
	bodyLength := len(b.buf) - b.start - headerSize
	binary.BigEndian.PutUint64(b.buf[b.start+offsetBodyLength:], uint64(bodyLength))

	return b.buf
}
