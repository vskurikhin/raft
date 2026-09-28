package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/vskurikhin/raft/pkg/raft/contract"
)

// Коды ошибки в оболочке тела ответа.
const (
	errorCodeNone        uint16 = 0
	errorCodeShutdown    uint16 = 1
	errorCodeEnqueue     uint16 = 2
	errorCodeUnsupported uint16 = 3
	errorCodeRemote      uint16 = 4
)

// Длины успешного тела ответа без оболочки ошибки.
const (
	appendEntriesReplyBytes = 41
	shortReplyBytes         = 25
)

// Усечение слишком длинного текста удалённой ошибки.
const (
	truncationSuffix     = "...[truncated]"
	truncatedPrefixBytes = maxErrorTextBytes - len(truncationSuffix)
)

// errorMarkers — маркерные ошибки кодов 1–3 в порядке выбора кода.
var errorMarkers = [...]struct {
	code   uint16
	marker error
}{
	{errorCodeShutdown, contract.ErrRaftShutdown},
	{errorCodeEnqueue, contract.ErrEnqueueTimeout},
	{errorCodeUnsupported, contract.ErrUnsupportedProtocol},
}

// AppendResponse дописывает к dst полный кадр ответа типа typ. Заданная
// Error имеет приоритет: код 1–3 выбирается через errors.Is по маркерам
// contract, прочие ошибки — код 4 с текстом, Reply при этом не кодируется.
// Без Error обязателен ненулевой указатель на ответ типа typ с совместимой
// версией протокола. При ошибке возвращается dst[:len(dst)].
func AppendResponse(dst []byte, typ RPCType, response contract.RPCResponse, limits Limits) ([]byte, error) {
	oldLen := len(dst)
	if err := checkLimits(limits); err != nil {
		return dst[:oldLen], err
	}
	if typ >= rpcTypeCount {
		return dst[:oldLen], fmt.Errorf("%w: RPCType %d", ErrFormat, typ)
	}

	var (
		frame []byte
		err   error
	)
	if response.Error != nil {
		frame, err = appendErrorResponse(dst, typ, response.Error, limits)
	} else {
		frame, err = appendReplyResponse(dst, typ, response.Reply, limits)
	}
	if err != nil {
		return dst[:oldLen], err
	}

	return frame, nil
}

func appendErrorResponse(dst []byte, typ RPCType, cause error, limits Limits) ([]byte, error) {
	code, text := errorCodeAndText(cause)
	builder, err := startFixedFrame(dst, directionResponse, typ, errorEnvelopeBytes+uint64(len(text)), limits)
	if err != nil {
		return nil, err
	}
	builder.buf = appendErrorEnvelope(builder.buf, code, len(text))
	builder.buf = append(builder.buf, text...)

	return builder.buf, nil
}

func errorCodeAndText(cause error) (uint16, string) {
	for _, known := range errorMarkers {
		if errors.Is(cause, known.marker) {
			return known.code, known.marker.Error()
		}
	}

	text := cause.Error()
	if len(text) > maxErrorTextBytes {
		text = text[:truncatedPrefixBytes] + truncationSuffix
	}

	return errorCodeRemote, text
}

func appendErrorEnvelope(dst []byte, code uint16, textLength int) []byte {
	dst = binary.BigEndian.AppendUint16(dst, code)
	dst = binary.BigEndian.AppendUint16(dst, 0)

	return binary.BigEndian.AppendUint32(dst, uint32(textLength))
}

func appendReplyResponse(dst []byte, typ RPCType, reply any, limits Limits) ([]byte, error) {
	if reply == nil {
		return nil, fmt.Errorf("%w: response has neither Reply nor Error", ErrFormat)
	}

	switch typ {
	case rpcAppendEntries:
		return appendAppendEntriesReply(dst, reply, limits)
	case rpcRequestVote:
		return appendVoteReply(dst, typ, reply, limits)
	case rpcInstallSnapshot:
		return appendInstallSnapshotReply(dst, reply, limits)
	case rpcTimeoutNow:
		return appendTimeoutNowReply(dst, reply, limits)
	case rpcRequestPreVote:
		return appendVoteReply(dst, typ, reply, limits)
	default:
		return nil, fmt.Errorf("%w: RPCType %d", ErrFormat, typ)
	}
}

func replyTypeError(typ RPCType, reply any) error {
	return fmt.Errorf("%w: RPCType %d cannot carry Reply %T", ErrFormat, typ, reply)
}

func nilReplyError(reply any) error {
	return fmt.Errorf("%w: nil Reply %T", ErrFormat, reply)
}

// startSuccessFrame проверяет заголовок и терм ответа и дописывает начало
// кадра успешного ответа: заголовок кадра, пустую оболочку ошибки и H.
func startSuccessFrame(dst []byte, typ RPCType, header contract.RPCHeader, term int, replyBytes uint64,
	limits Limits,
) (frameBuilder, error) {
	if err := checkEncodeHeader("response", header); err != nil {
		return frameBuilder{}, err
	}
	err := checkIntFields(
		intField{fieldTerm, term, rangeNonNegative},
	)
	if err != nil {
		return frameBuilder{}, err
	}

	builder, err := startFixedFrame(dst, directionResponse, typ, errorEnvelopeBytes+replyBytes, limits)
	if err != nil {
		return frameBuilder{}, err
	}
	builder.buf = appendErrorEnvelope(builder.buf, errorCodeNone, 0)
	builder.buf = appendRPCHeader(builder.buf, header)

	return builder, nil
}

func appendAppendEntriesReply(dst []byte, reply any, limits Limits) ([]byte, error) {
	typed, ok := reply.(*contract.AppendEntriesReply)
	if !ok {
		return nil, replyTypeError(rpcAppendEntries, reply)
	}
	if typed == nil {
		return nil, nilReplyError(reply)
	}
	if err := checkEncodeHeader("AppendEntries reply", typed.RPCHeader); err != nil {
		return nil, err
	}
	err := checkIntFields(
		intField{"ConflictIndex", typed.ConflictIndex, rangeNonNegative},
		intField{"ConflictTerm", typed.ConflictTerm, rangeMinusOne},
	)
	if err != nil {
		return nil, err
	}

	builder, err := startSuccessFrame(dst, rpcAppendEntries, typed.RPCHeader, typed.Term, appendEntriesReplyBytes, limits)
	if err != nil {
		return nil, err
	}
	builder.buf = appendInt(builder.buf, typed.Term)
	builder.buf = appendBool(builder.buf, typed.Success)
	builder.buf = appendInt(builder.buf, typed.ConflictIndex)
	builder.buf = appendInt(builder.buf, typed.ConflictTerm)

	return builder.buf, nil
}

// appendVoteReply кодирует ответ RequestVote или RequestPreVote: у обоих
// одинаковая раскладка H, Term, VoteGranted.
func appendVoteReply(dst []byte, typ RPCType, reply any, limits Limits) ([]byte, error) {
	var (
		header  contract.RPCHeader
		term    int
		granted bool
	)
	switch typed := reply.(type) {
	case *contract.RequestVoteReply:
		if typ != rpcRequestVote {
			return nil, replyTypeError(typ, reply)
		}
		if typed == nil {
			return nil, nilReplyError(reply)
		}
		header, term, granted = typed.RPCHeader, typed.Term, typed.VoteGranted
	case *contract.RequestPreVoteReply:
		if typ != rpcRequestPreVote {
			return nil, replyTypeError(typ, reply)
		}
		if typed == nil {
			return nil, nilReplyError(reply)
		}
		header, term, granted = typed.RPCHeader, typed.Term, typed.VoteGranted
	default:
		return nil, replyTypeError(typ, reply)
	}

	builder, err := startSuccessFrame(dst, typ, header, term, shortReplyBytes, limits)
	if err != nil {
		return nil, err
	}
	builder.buf = appendInt(builder.buf, term)
	builder.buf = appendBool(builder.buf, granted)

	return builder.buf, nil
}

func appendInstallSnapshotReply(dst []byte, reply any, limits Limits) ([]byte, error) {
	typed, ok := reply.(*contract.InstallSnapshotResponse)
	if !ok {
		return nil, replyTypeError(rpcInstallSnapshot, reply)
	}
	if typed == nil {
		return nil, nilReplyError(reply)
	}

	builder, err := startSuccessFrame(dst, rpcInstallSnapshot, typed.RPCHeader, typed.Term, shortReplyBytes, limits)
	if err != nil {
		return nil, err
	}
	builder.buf = appendInt(builder.buf, typed.Term)
	builder.buf = appendBool(builder.buf, typed.Success)

	return builder.buf, nil
}

// appendTimeoutNowReply кодирует ответ TimeoutNow: Success предшествует Term.
func appendTimeoutNowReply(dst []byte, reply any, limits Limits) ([]byte, error) {
	typed, ok := reply.(*contract.TimeoutNowResponse)
	if !ok {
		return nil, replyTypeError(rpcTimeoutNow, reply)
	}
	if typed == nil {
		return nil, nilReplyError(reply)
	}

	builder, err := startSuccessFrame(dst, rpcTimeoutNow, typed.RPCHeader, typed.Term, shortReplyBytes, limits)
	if err != nil {
		return nil, err
	}
	builder.buf = appendBool(builder.buf, typed.Success)
	builder.buf = appendInt(builder.buf, typed.Term)

	return builder.buf, nil
}

// ReadResponse читает и полностью проверяет один кадр ответа ожидаемого
// типа. Это ReadResponseWithHeader без проверки заголовка вызывающим.
func ReadResponse(r io.Reader, expected RPCType, limits Limits) (contract.RPCResponse, error) {
	return ReadResponseWithHeader(r, expected, limits, nil)
}

// ReadResponseWithHeader читает ровно 24 Б заголовка ответа, проверяет его,
// вызывает beforeBody ровно один раз с копией проверенного заголовка и только
// затем читает тело. Транспортная или форматная ошибка возвращается вторым
// результатом; удалённая ошибка — в RPCResponse.Error: коды 1–3 точно в
// маркеры contract, код 4 — ошибка с текстом удалённой стороны. Успешный
// ответ с несовместимой версией протокола — contract.ErrUnsupportedProtocol
// вторым результатом.
func ReadResponseWithHeader(r io.Reader, expected RPCType, limits Limits,
	beforeBody BeforeBody,
) (contract.RPCResponse, error) {
	if err := checkLimits(limits); err != nil {
		return contract.RPCResponse{}, err
	}
	if expected >= rpcTypeCount {
		return contract.RPCResponse{}, fmt.Errorf("%w: expected RPCType %d", ErrFormat, expected)
	}

	_, body, err := readFrame(r, responseCheck(expected, limits), beforeBody)
	if err != nil {
		return contract.RPCResponse{}, err
	}

	response, err := decodeResponse(expected, body)
	if err != nil {
		return contract.RPCResponse{}, err
	}

	return response, nil
}

func decodeResponse(typ RPCType, body []byte) (contract.RPCResponse, error) {
	d := &bodyDecoder{body: body}
	code := d.readUint16("ErrorCode")
	reserved := d.readUint16("error reserved")
	textLength := d.readUint32("ErrorLength")
	if d.err != nil {
		return contract.RPCResponse{}, d.err
	}
	if reserved != 0 {
		return contract.RPCResponse{}, fmt.Errorf("%w: non-zero error reserved bytes", ErrFormat)
	}

	if code == errorCodeNone {
		if textLength != 0 {
			return contract.RPCResponse{}, fmt.Errorf("%w: ErrorCode 0 with ErrorLength %d", ErrFormat, textLength)
		}
		if want := successReplyBytes(typ); d.remaining() != want {
			return contract.RPCResponse{}, fmt.Errorf("%w: RPCType %d success reply %d bytes, want %d",
				ErrFormat, typ, d.remaining(), want)
		}
		reply, err := decodeReply(typ, d)
		if err != nil {
			return contract.RPCResponse{}, err
		}

		return contract.RPCResponse{Reply: reply}, nil
	}

	if uint64(textLength) != uint64(d.remaining()) {
		return contract.RPCResponse{}, fmt.Errorf("%w: ErrorCode %d body has %d bytes after envelope, ErrorLength %d",
			ErrFormat, code, d.remaining(), textLength)
	}

	return remoteErrorResponse(code, d.take("ErrorBytes", int(textLength)))
}

func successReplyBytes(typ RPCType) int {
	if typ == rpcAppendEntries {
		return appendEntriesReplyBytes
	}

	return shortReplyBytes
}

// remoteErrorResponse восстанавливает удалённую ошибку: коды 1–3 требуют
// канонический текст маркера и возвращают сам маркер.
func remoteErrorResponse(code uint16, text []byte) (contract.RPCResponse, error) {
	if code == errorCodeRemote {
		return contract.RPCResponse{Error: errors.New(string(text))}, nil
	}
	for _, known := range errorMarkers {
		if known.code != code {
			continue
		}
		if string(text) != known.marker.Error() {
			return contract.RPCResponse{}, fmt.Errorf("%w: ErrorCode %d with non-canonical text %q", ErrFormat, code, text)
		}

		return contract.RPCResponse{Error: known.marker}, nil
	}

	return contract.RPCResponse{}, fmt.Errorf("%w: unknown ErrorCode %d", ErrFormat, code)
}

func decodeReply(typ RPCType, d *bodyDecoder) (any, error) {
	var reply any
	switch typ {
	case rpcAppendEntries:
		reply = decodeAppendEntriesReply(d)
	case rpcRequestVote:
		reply = decodeRequestVoteReply(d)
	case rpcInstallSnapshot:
		reply = decodeInstallSnapshotReply(d)
	case rpcTimeoutNow:
		reply = decodeTimeoutNowReply(d)
	case rpcRequestPreVote:
		reply = decodeRequestPreVoteReply(d)
	default:
		return nil, fmt.Errorf("%w: RPCType %d", ErrFormat, typ)
	}
	if err := d.finish(); err != nil {
		return nil, err
	}

	return reply, nil
}

func decodeRequestVoteReply(d *bodyDecoder) *contract.RequestVoteReply {
	reply := &contract.RequestVoteReply{RPCHeader: d.readRPCHeader()}
	reply.Term = d.readInt(fieldTerm, rangeNonNegative)
	reply.VoteGranted = d.readBool("VoteGranted")

	return reply
}

func decodeInstallSnapshotReply(d *bodyDecoder) *contract.InstallSnapshotResponse {
	reply := &contract.InstallSnapshotResponse{RPCHeader: d.readRPCHeader()}
	reply.Term = d.readInt(fieldTerm, rangeNonNegative)
	reply.Success = d.readBool("Success")

	return reply
}

func decodeTimeoutNowReply(d *bodyDecoder) *contract.TimeoutNowResponse {
	reply := &contract.TimeoutNowResponse{RPCHeader: d.readRPCHeader()}
	reply.Success = d.readBool("Success")
	reply.Term = d.readInt(fieldTerm, rangeNonNegative)

	return reply
}

func decodeRequestPreVoteReply(d *bodyDecoder) *contract.RequestPreVoteReply {
	reply := &contract.RequestPreVoteReply{RPCHeader: d.readRPCHeader()}
	reply.Term = d.readInt(fieldTerm, rangeNonNegative)
	reply.VoteGranted = d.readBool("VoteGranted")

	return reply
}

func decodeAppendEntriesReply(d *bodyDecoder) *contract.AppendEntriesReply {
	reply := &contract.AppendEntriesReply{RPCHeader: d.readRPCHeader()}
	reply.Term = d.readInt(fieldTerm, rangeNonNegative)
	reply.Success = d.readBool("Success")
	reply.ConflictIndex = d.readInt("ConflictIndex", rangeNonNegative)
	reply.ConflictTerm = d.readInt("ConflictTerm", rangeMinusOne)

	return reply
}
