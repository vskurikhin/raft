package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// RPCType — тип RPC в заголовке кадра.
type RPCType uint8

// Типы RPC сохраняют прежние назначения.
const (
	rpcAppendEntries RPCType = iota
	rpcRequestVote
	rpcInstallSnapshot
	rpcTimeoutNow
	rpcRequestPreVote
	rpcTypeCount
)

// Направление кадра.
const (
	directionRequest  uint8 = 0
	directionResponse uint8 = 1
)

// Раскладка заголовка кадра.
const (
	headerSize           = 24
	formatVersion        = 1
	offsetFormatVersion  = 4
	offsetHeaderSize     = 6
	offsetDirection      = 8
	offsetRPCType        = 9
	offsetReserved       = 10
	offsetBodyLength     = 12
	offsetReservedTail   = 20
	reservedTailBytes    = 4
	reservedHeaderBytes  = 2
	magicBytes           = 4
	maxErrorTextBytes    = 4096
	errorEnvelopeBytes   = 8
	maxErrorBodyBytes    = errorEnvelopeBytes + maxErrorTextBytes
	appendEntriesMinBody = 64
	requestVoteBody      = 49
	requestPreVoteBody   = 40
	timeoutNowBody       = 16
	installSnapshotMin   = 72
)

var frameMagic = [magicBytes]byte{'R', 'R', 'P', 'C'}

var errInvalidWrite = errors.New("protocol: writer returned invalid byte count")

// FrameHeader — проверенные поля заголовка кадра, передаваемые BeforeBody
// до чтения тела.
type FrameHeader struct {
	Direction  uint8
	Type       RPCType
	BodyLength uint64
}

// BeforeBody вызывается ровно один раз после проверки заголовка и до
// потребления первого байта тела. Ошибка прекращает чтение кадра.
type BeforeBody func(header FrameHeader) error

// WriteFrame пишет готовый кадр целиком, повторяя положительные короткие
// записи. Семантику кадра не проверяет и Flush не выполняет.
func WriteFrame(w io.Writer, frame []byte) error {
	for len(frame) > 0 {
		n, err := w.Write(frame)
		if n < 0 || n > len(frame) {
			return errInvalidWrite
		}
		frame = frame[n:]
		if err != nil {
			return fmt.Errorf("protocol: write frame: %w", err)
		}
		if n == 0 {
			return fmt.Errorf("protocol: write frame: %w", io.ErrShortWrite)
		}
	}

	return nil
}

func appendHeader(dst []byte, direction uint8, typ RPCType, bodyLength uint64) []byte {
	dst = append(dst, frameMagic[:]...)
	dst = binary.BigEndian.AppendUint16(dst, formatVersion)
	dst = binary.BigEndian.AppendUint16(dst, headerSize)
	dst = append(dst, direction, byte(typ))
	dst = binary.BigEndian.AppendUint16(dst, 0)
	dst = binary.BigEndian.AppendUint64(dst, bodyLength)

	return binary.BigEndian.AppendUint32(dst, 0)
}

// frameCheck проверяет направление, тип и длину тела заголовка до чтения тела.
type frameCheck func(header FrameHeader) error

// readFrame читает заголовок, проверяет его, вызывает beforeBody и только
// затем читает тело. Тело выделяется по уже проверенной длине.
func readFrame(r io.Reader, check frameCheck, beforeBody BeforeBody) (FrameHeader, []byte, error) {
	var raw [headerSize]byte
	if _, err := io.ReadFull(r, raw[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return FrameHeader{}, nil, io.EOF
		}

		return FrameHeader{}, nil, fmt.Errorf("protocol: read frame header: %w", err)
	}

	header, err := parseHeader(&raw)
	if err != nil {
		return FrameHeader{}, nil, err
	}
	if err = check(header); err != nil {
		return FrameHeader{}, nil, err
	}
	if beforeBody != nil {
		if err = beforeBody(header); err != nil {
			return FrameHeader{}, nil, fmt.Errorf("protocol: before body: %w", err)
		}
	}

	body := make([]byte, header.BodyLength)
	if _, err = io.ReadFull(r, body); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}

		return FrameHeader{}, nil, fmt.Errorf("protocol: read frame body: %w", err)
	}

	return header, body, nil
}

func parseHeader(raw *[headerSize]byte) (FrameHeader, error) {
	if [magicBytes]byte(raw[:magicBytes]) != frameMagic {
		return FrameHeader{}, fmt.Errorf("%w: bad magic % x", ErrFormat, raw[:magicBytes])
	}
	if version := binary.BigEndian.Uint16(raw[offsetFormatVersion:]); version != formatVersion {
		return FrameHeader{}, fmt.Errorf("%w: FormatVersion %d", ErrUnsupportedVersion, version)
	}
	if size := binary.BigEndian.Uint16(raw[offsetHeaderSize:]); size != headerSize {
		return FrameHeader{}, fmt.Errorf("%w: HeaderSize %d", ErrFormat, size)
	}
	if !isZero(raw[offsetReserved:offsetReserved+reservedHeaderBytes]) ||
		!isZero(raw[offsetReservedTail:offsetReservedTail+reservedTailBytes]) {
		return FrameHeader{}, fmt.Errorf("%w: non-zero reserved header bytes", ErrFormat)
	}

	header := FrameHeader{
		Direction:  raw[offsetDirection],
		Type:       RPCType(raw[offsetRPCType]),
		BodyLength: binary.BigEndian.Uint64(raw[offsetBodyLength:]),
	}
	if header.Direction != directionRequest && header.Direction != directionResponse {
		return FrameHeader{}, fmt.Errorf("%w: Direction %d", ErrFormat, header.Direction)
	}
	if header.Type >= rpcTypeCount {
		return FrameHeader{}, fmt.Errorf("%w: RPCType %d", ErrFormat, header.Type)
	}

	return header, nil
}

// checkFrameLength проверяет 24+BodyLength <= MaxFrameBytes без переполнения.
func checkFrameLength(header FrameHeader, limits Limits) error {
	if header.BodyLength > limits.MaxFrameBytes-headerSize {
		return fmt.Errorf("%w: frame %d+%d bytes exceeds MaxFrameBytes %d",
			ErrLimit, headerSize, header.BodyLength, limits.MaxFrameBytes)
	}

	return nil
}

func requestCheck(limits Limits) frameCheck {
	return func(header FrameHeader) error {
		if header.Direction != directionRequest {
			return fmt.Errorf("%w: expected request, got Direction %d", ErrFormat, header.Direction)
		}
		if err := checkFrameLength(header, limits); err != nil {
			return err
		}

		return checkRequestBodyLength(header, limits)
	}
}

func checkRequestBodyLength(header FrameHeader, limits Limits) error {
	length := header.BodyLength
	switch header.Type {
	case rpcAppendEntries:
		return checkMinBody(header, appendEntriesMinBody)
	case rpcRequestVote:
		return checkExactBody(header, requestVoteBody)
	case rpcRequestPreVote:
		return checkExactBody(header, requestPreVoteBody)
	case rpcTimeoutNow:
		return checkExactBody(header, timeoutNowBody)
	case rpcInstallSnapshot:
		if err := checkMinBody(header, installSnapshotMin); err != nil {
			return err
		}
		if length-installSnapshotMin > limits.MaxConfigurationBytes {
			return fmt.Errorf("%w: InstallSnapshot body %d bytes exceeds %d+MaxConfigurationBytes %d",
				ErrLimit, length, installSnapshotMin, limits.MaxConfigurationBytes)
		}

		return nil
	default:
		return fmt.Errorf("%w: RPCType %d", ErrFormat, header.Type)
	}
}

func responseCheck(expected RPCType, limits Limits) frameCheck {
	return func(header FrameHeader) error {
		if header.Direction != directionResponse {
			return fmt.Errorf("%w: expected response, got Direction %d", ErrFormat, header.Direction)
		}
		if header.Type != expected {
			return fmt.Errorf("%w: expected response RPCType %d, got %d", ErrFormat, expected, header.Type)
		}
		if err := checkFrameLength(header, limits); err != nil {
			return err
		}
		if err := checkMinBody(header, errorEnvelopeBytes); err != nil {
			return err
		}
		if header.BodyLength > maxErrorBodyBytes {
			return fmt.Errorf("%w: response body %d bytes exceeds %d",
				ErrLimit, header.BodyLength, maxErrorBodyBytes)
		}

		return nil
	}
}

func checkMinBody(header FrameHeader, minimum uint64) error {
	if header.BodyLength < minimum {
		return fmt.Errorf("%w: RPCType %d body %d bytes is shorter than %d",
			ErrFormat, header.Type, header.BodyLength, minimum)
	}

	return nil
}

func checkExactBody(header FrameHeader, exact uint64) error {
	if header.BodyLength != exact {
		return fmt.Errorf("%w: RPCType %d body %d bytes, want %d",
			ErrFormat, header.Type, header.BodyLength, exact)
	}

	return nil
}

func isZero(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}

	return true
}
