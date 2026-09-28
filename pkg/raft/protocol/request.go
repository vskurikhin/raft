package protocol

import (
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"

	"github.com/vskurikhin/raft/pkg/raft/contract"
)

// Раскладка записи журнала в теле AppendEntries.
const (
	entryHeaderBytes    = 32
	entryReservedBytes  = 6
	maxLogType          = contract.LogConfiguration
	maxSnapshotDataSize = 1 << 30
)

// logEntrySize — размер contract.LogEntry на текущей платформе; используется
// для проверки выделения Entries до make.
var logEntrySize = uint64(reflect.TypeFor[contract.LogEntry]().Size())

var errNilCommand = fmt.Errorf("%w: nil command", ErrFormat)

// AppendRequest дописывает к dst полный кадр запроса. Тип RPC выводится из
// конкретного типа command: допустимы только ненулевые указатели на пять
// типов запросов contract. Перед кодированием проверяются версия протокола,
// диапазоны полей и пределы. При ошибке возвращается dst[:len(dst)]; префикс
// dst не изменяется.
func AppendRequest(dst []byte, command any, limits Limits) ([]byte, error) {
	oldLen := len(dst)
	if err := checkLimits(limits); err != nil {
		return dst[:oldLen], err
	}

	frame, err := appendRequest(dst, command, limits)
	if err != nil {
		return dst[:oldLen], err
	}

	return frame, nil
}

func appendRequest(dst []byte, command any, limits Limits) ([]byte, error) {
	switch args := command.(type) {
	case *contract.AppendEntriesArgs:
		if args == nil {
			return nil, errNilCommand
		}

		return appendAppendEntriesRequest(dst, args, limits)
	case *contract.RequestVoteArgs:
		if args == nil {
			return nil, errNilCommand
		}

		return appendRequestVoteRequest(dst, args, limits)
	case *contract.InstallSnapshotRequest:
		if args == nil {
			return nil, errNilCommand
		}

		return appendInstallSnapshotRequest(dst, args, limits)
	case *contract.TimeoutNowRequest:
		if args == nil {
			return nil, errNilCommand
		}

		return appendTimeoutNowRequest(dst, args, limits)
	case *contract.RequestPreVoteArgs:
		if args == nil {
			return nil, errNilCommand
		}

		return appendRequestPreVoteRequest(dst, args, limits)
	default:
		return nil, fmt.Errorf("%w: unsupported command type %T", ErrFormat, command)
	}
}

// startFixedFrame проверяет пределы кадра известной длины и дописывает его
// заголовок, выделяя ровно одну ёмкость под весь кадр.
func startFixedFrame(dst []byte, direction uint8, typ RPCType, bodyLength uint64, limits Limits) (frameBuilder, error) {
	builder, err := newFrameBuilder(dst, limits)
	if err != nil {
		return frameBuilder{}, err
	}
	if err = builder.reserve(headerSize + bodyLength); err != nil {
		return frameBuilder{}, err
	}
	builder.buf = appendHeader(builder.buf, direction, typ, bodyLength)

	return builder, nil
}

func appendAppendEntriesRequest(dst []byte, args *contract.AppendEntriesArgs, limits Limits) ([]byte, error) {
	if err := checkEncodeHeader("AppendEntries request", args.RPCHeader); err != nil {
		return nil, err
	}
	err := checkIntFields(
		intField{fieldTerm, args.Term, rangeNonNegative},
		intField{"PrevLogIndex", args.PrevLogIndex, rangeMinusOne},
		intField{"PrevLogTerm", args.PrevLogTerm, rangeMinusOne},
		intField{"LeaderCommit", args.LeaderCommit, rangeMinusOne},
	)
	if err != nil {
		return nil, err
	}
	count := uint64(len(args.Entries))
	if count > limits.MaxEntries {
		return nil, fmt.Errorf("%w: AppendEntries %d entries exceeds MaxEntries %d", ErrLimit, count, limits.MaxEntries)
	}

	builder, err := newFrameBuilder(dst, limits)
	if err != nil {
		return nil, err
	}
	if err = builder.reserve(headerSize + appendEntriesMinBody + count*entryHeaderBytes); err != nil {
		return nil, err
	}
	builder.buf = appendHeader(builder.buf, directionRequest, rpcAppendEntries, 0)
	builder.buf = appendRPCHeader(builder.buf, args.RPCHeader)
	builder.buf = appendInt(builder.buf, args.Term)
	builder.buf = appendInt(builder.buf, args.LeaderID)
	builder.buf = appendInt(builder.buf, args.PrevLogIndex)
	builder.buf = appendInt(builder.buf, args.PrevLogTerm)
	builder.buf = appendInt(builder.buf, args.LeaderCommit)
	builder.buf = appendUint64(builder.buf, count)

	for i := range args.Entries {
		if err = appendEntry(&builder, &args.Entries[i], limits); err != nil {
			return nil, fmt.Errorf("AppendEntries entry %d: %w", i, err)
		}
	}

	return builder.finishFrame(), nil
}

func appendEntry(builder *frameBuilder, entry *contract.LogEntry, limits Limits) error {
	err := checkIntFields(
		intField{"LogEntry.Index", entry.Index, rangeNonNegative},
		intField{"LogEntry.Term", entry.Term, rangeNonNegative},
	)
	if err != nil {
		return err
	}
	if entry.Type < 0 || entry.Type > maxLogType {
		return fmt.Errorf("%w: LogEntry.Type %d", ErrFormat, entry.Type)
	}

	data, err := EncodeData(entry.Data, limits.MaxDataBytes)
	if err != nil {
		return err
	}
	kind := dataKindGob
	if entry.Data == nil {
		kind = dataKindNil
	}

	if err = builder.reserve(entryHeaderBytes + uint64(len(data))); err != nil {
		return err
	}
	builder.buf = appendInt(builder.buf, entry.Index)
	builder.buf = appendInt(builder.buf, entry.Term)
	builder.buf = append(builder.buf, byte(entry.Type), kind, 0, 0, 0, 0, 0, 0)
	builder.buf = appendUint64(builder.buf, uint64(len(data)))
	builder.buf = append(builder.buf, data...)

	return nil
}

func appendRequestVoteRequest(dst []byte, args *contract.RequestVoteArgs, limits Limits) ([]byte, error) {
	if err := checkEncodeHeader("RequestVote request", args.RPCHeader); err != nil {
		return nil, err
	}
	err := checkIntFields(
		intField{fieldTerm, args.Term, rangeNonNegative},
		intField{fieldLastLogIndex, args.LastLogIndex, rangeMinusOne},
		intField{fieldLastLogTerm, args.LastLogTerm, rangeMinusOne},
	)
	if err != nil {
		return nil, err
	}

	builder, err := startFixedFrame(dst, directionRequest, rpcRequestVote, requestVoteBody, limits)
	if err != nil {
		return nil, err
	}
	builder.buf = appendRPCHeader(builder.buf, args.RPCHeader)
	builder.buf = appendInt(builder.buf, args.Term)
	builder.buf = appendInt(builder.buf, args.CandidateID)
	builder.buf = appendInt(builder.buf, args.LastLogIndex)
	builder.buf = appendInt(builder.buf, args.LastLogTerm)
	builder.buf = appendBool(builder.buf, args.LeadershipTransfer)

	return builder.buf, nil
}

func appendInstallSnapshotRequest(dst []byte, args *contract.InstallSnapshotRequest, limits Limits) ([]byte, error) {
	if err := checkEncodeHeader("InstallSnapshot request", args.RPCHeader); err != nil {
		return nil, err
	}
	err := checkIntFields(
		intField{fieldTerm, args.Term, rangeNonNegative},
		intField{fieldLastLogIndex, args.LastLogIndex, rangeNonNegative},
		intField{fieldLastLogTerm, args.LastLogTerm, rangeNonNegative},
		intField{"ConfigIndex", args.ConfigIndex, rangeMinusOne},
	)
	if err != nil {
		return nil, err
	}
	if err = checkSnapshotDataSize(args.DataSize); err != nil {
		return nil, err
	}
	configLength := uint64(len(args.Configuration))
	if configLength > limits.MaxConfigurationBytes {
		return nil, fmt.Errorf("%w: InstallSnapshot configuration %d bytes exceeds MaxConfigurationBytes %d",
			ErrLimit, configLength, limits.MaxConfigurationBytes)
	}

	builder, err := startFixedFrame(dst, directionRequest, rpcInstallSnapshot, installSnapshotMin+configLength, limits)
	if err != nil {
		return nil, err
	}
	builder.buf = appendRPCHeader(builder.buf, args.RPCHeader)
	builder.buf = appendInt(builder.buf, args.Term)
	builder.buf = appendInt(builder.buf, args.LeaderID)
	builder.buf = appendInt(builder.buf, args.LastLogIndex)
	builder.buf = appendInt(builder.buf, args.LastLogTerm)
	builder.buf = appendInt(builder.buf, args.ConfigIndex)
	builder.buf = appendUint64(builder.buf, uint64(args.DataSize))
	builder.buf = appendUint64(builder.buf, configLength)
	builder.buf = append(builder.buf, args.Configuration...)

	return builder.buf, nil
}

func appendTimeoutNowRequest(dst []byte, args *contract.TimeoutNowRequest, limits Limits) ([]byte, error) {
	if err := checkEncodeHeader("TimeoutNow request", args.RPCHeader); err != nil {
		return nil, err
	}
	builder, err := startFixedFrame(dst, directionRequest, rpcTimeoutNow, timeoutNowBody, limits)
	if err != nil {
		return nil, err
	}
	builder.buf = appendRPCHeader(builder.buf, args.RPCHeader)

	return builder.buf, nil
}

func appendRequestPreVoteRequest(dst []byte, args *contract.RequestPreVoteArgs, limits Limits) ([]byte, error) {
	if err := checkEncodeHeader("RequestPreVote request", args.RPCHeader); err != nil {
		return nil, err
	}
	err := checkIntFields(
		intField{fieldTerm, args.Term, rangeNonNegative},
		intField{fieldLastLogIndex, args.LastLogIndex, rangeMinusOne},
		intField{fieldLastLogTerm, args.LastLogTerm, rangeMinusOne},
	)
	if err != nil {
		return nil, err
	}

	builder, err := startFixedFrame(dst, directionRequest, rpcRequestPreVote, requestPreVoteBody, limits)
	if err != nil {
		return nil, err
	}
	builder.buf = appendRPCHeader(builder.buf, args.RPCHeader)
	builder.buf = appendInt(builder.buf, args.Term)
	builder.buf = appendInt(builder.buf, args.LastLogIndex)
	builder.buf = appendInt(builder.buf, args.LastLogTerm)

	return builder.buf, nil
}

func checkSnapshotDataSize(size int64) error {
	if size < 1 || size > maxSnapshotDataSize {
		return fmt.Errorf("%w: InstallSnapshot DataSize = %d outside [1, %d]", ErrRange, size, maxSnapshotDataSize)
	}

	return nil
}

// ReadRequest читает и полностью проверяет один кадр запроса. Это
// ReadRequestWithHeader без проверки заголовка вызывающим.
func ReadRequest(r io.Reader, limits Limits) (RPCType, any, error) {
	return ReadRequestWithHeader(r, limits, nil)
}

// ReadRequestWithHeader читает ровно 24 Б заголовка, проверяет его и пределы,
// вызывает beforeBody ровно один раз с копией проверенного заголовка и только
// после его успешного возврата читает и декодирует тело. Возвращает новый
// указатель на запрос contract. Несовместимая версия протокола известного
// запроса возвращается как contract.ErrUnsupportedProtocol вместе с надёжным
// RPCType; при остальных ошибках RPCType не используется. Чистый конец потока
// до первого байта заголовка — io.EOF, частичный кадр — io.ErrUnexpectedEOF.
func ReadRequestWithHeader(r io.Reader, limits Limits, beforeBody BeforeBody) (RPCType, any, error) {
	if err := checkLimits(limits); err != nil {
		return 0, nil, err
	}

	header, body, err := readFrame(r, requestCheck(limits), beforeBody)
	if err != nil {
		return 0, nil, err
	}

	command, err := decodeRequest(header.Type, body, limits)
	if err != nil {
		if errors.Is(err, contract.ErrUnsupportedProtocol) {
			return header.Type, nil, err
		}

		return 0, nil, err
	}

	return header.Type, command, nil
}

func decodeRequest(typ RPCType, body []byte, limits Limits) (any, error) {
	decoder := &bodyDecoder{body: body}
	switch typ {
	case rpcAppendEntries:
		return decodeAppendEntriesRequest(decoder, limits)
	case rpcRequestVote:
		return decodeRequestVoteRequest(decoder)
	case rpcInstallSnapshot:
		return decodeInstallSnapshotRequest(decoder, limits)
	case rpcTimeoutNow:
		return decodeTimeoutNowRequest(decoder)
	case rpcRequestPreVote:
		return decodeRequestPreVoteRequest(decoder)
	default:
		return nil, fmt.Errorf("%w: RPCType %d", ErrFormat, typ)
	}
}

func decodeAppendEntriesRequest(d *bodyDecoder, limits Limits) (*contract.AppendEntriesArgs, error) {
	args := &contract.AppendEntriesArgs{RPCHeader: d.readRPCHeader()}
	args.Term = d.readInt(fieldTerm, rangeNonNegative)
	args.LeaderID = d.readInt("LeaderID", rangeAnyInt)
	args.PrevLogIndex = d.readInt("PrevLogIndex", rangeMinusOne)
	args.PrevLogTerm = d.readInt("PrevLogTerm", rangeMinusOne)
	args.LeaderCommit = d.readInt("LeaderCommit", rangeMinusOne)
	count := d.readUint64("EntryCount")
	if d.err != nil {
		return nil, d.err
	}
	if err := checkEntryCount(count, d.remaining(), limits); err != nil {
		return nil, err
	}

	if count > 0 {
		args.Entries = make([]contract.LogEntry, count)
	}
	for i := range args.Entries {
		if err := decodeEntry(d, &args.Entries[i], limits); err != nil {
			return nil, fmt.Errorf("AppendEntries entry %d: %w", i, err)
		}
	}
	if err := d.finish(); err != nil {
		return nil, err
	}

	return args, nil
}

// checkEntryCount проверяет недоверенное число записей до выделения Entries.
func checkEntryCount(count uint64, remaining int, limits Limits) error {
	if count > limits.MaxEntries {
		return fmt.Errorf("%w: EntryCount %d exceeds MaxEntries %d", ErrLimit, count, limits.MaxEntries)
	}
	if count > uint64(remaining)/entryHeaderBytes {
		return fmt.Errorf("%w: EntryCount %d exceeds remaining body %d bytes / %d",
			ErrFormat, count, remaining, entryHeaderBytes)
	}
	if count > math.MaxInt/logEntrySize {
		return fmt.Errorf("%w: EntryCount %d x %d bytes overflows int", ErrLimit, count, logEntrySize)
	}

	return nil
}

func decodeEntry(d *bodyDecoder, entry *contract.LogEntry, limits Limits) error {
	if d.remaining() < entryHeaderBytes {
		return fmt.Errorf("%w: entry header needs %d bytes, %d remain", ErrFormat, entryHeaderBytes, d.remaining())
	}
	entry.Index = d.readInt("LogEntry.Index", rangeNonNegative)
	entry.Term = d.readInt("LogEntry.Term", rangeNonNegative)
	logType := d.readByte("LogType")
	kind := d.readByte("DataKind")
	reserved := d.take("entry reserved", entryReservedBytes)
	length := d.readUint64("DataLength")
	if d.err != nil {
		return d.err
	}
	if contract.LogType(logType) > maxLogType {
		return fmt.Errorf("%w: LogType %d", ErrFormat, logType)
	}
	entry.Type = contract.LogType(logType)
	if !isZero(reserved) {
		return fmt.Errorf("%w: non-zero entry reserved bytes", ErrFormat)
	}

	switch kind {
	case dataKindNil:
		if length != 0 {
			return fmt.Errorf("%w: nil DataKind with DataLength %d", ErrFormat, length)
		}

		return nil
	case dataKindGob:
		return decodeEntryData(d, entry, length, limits)
	default:
		return fmt.Errorf("%w: DataKind %d", ErrFormat, kind)
	}
}

func decodeEntryData(d *bodyDecoder, entry *contract.LogEntry, length uint64, limits Limits) error {
	if length == 0 {
		return fmt.Errorf("%w: gob DataKind with zero DataLength", ErrFormat)
	}
	if length > limits.MaxDataBytes {
		return fmt.Errorf("%w: DataLength %d exceeds MaxDataBytes %d", ErrLimit, length, limits.MaxDataBytes)
	}
	if length > uint64(d.remaining()) {
		return fmt.Errorf("%w: DataLength %d exceeds remaining body %d bytes", ErrFormat, length, d.remaining())
	}

	data, err := decodeData(d.take("Data", int(length)))
	if err != nil {
		return err
	}
	entry.Data = data

	return nil
}

func decodeRequestVoteRequest(d *bodyDecoder) (*contract.RequestVoteArgs, error) {
	args := &contract.RequestVoteArgs{RPCHeader: d.readRPCHeader()}
	args.Term = d.readInt(fieldTerm, rangeNonNegative)
	args.CandidateID = d.readInt("CandidateID", rangeAnyInt)
	args.LastLogIndex = d.readInt(fieldLastLogIndex, rangeMinusOne)
	args.LastLogTerm = d.readInt(fieldLastLogTerm, rangeMinusOne)
	args.LeadershipTransfer = d.readBool("LeadershipTransfer")
	if err := d.finish(); err != nil {
		return nil, err
	}

	return args, nil
}

func decodeInstallSnapshotRequest(d *bodyDecoder, limits Limits) (*contract.InstallSnapshotRequest, error) {
	args := &contract.InstallSnapshotRequest{RPCHeader: d.readRPCHeader()}
	args.Term = d.readInt(fieldTerm, rangeNonNegative)
	args.LeaderID = d.readInt("LeaderID", rangeAnyInt)
	args.LastLogIndex = d.readInt(fieldLastLogIndex, rangeNonNegative)
	args.LastLogTerm = d.readInt(fieldLastLogTerm, rangeNonNegative)
	args.ConfigIndex = d.readInt("ConfigIndex", rangeMinusOne)
	dataSize := int64(d.readUint64("DataSize"))
	configLength := d.readUint64("ConfigurationLength")
	if d.err != nil {
		return nil, d.err
	}
	if err := checkSnapshotDataSize(dataSize); err != nil {
		return nil, err
	}
	args.DataSize = dataSize
	if configLength > limits.MaxConfigurationBytes {
		return nil, fmt.Errorf("%w: ConfigurationLength %d exceeds MaxConfigurationBytes %d",
			ErrLimit, configLength, limits.MaxConfigurationBytes)
	}
	if configLength > uint64(d.remaining()) {
		return nil, fmt.Errorf("%w: ConfigurationLength %d exceeds remaining body %d bytes",
			ErrFormat, configLength, d.remaining())
	}
	if configLength > 0 {
		args.Configuration = append([]byte(nil), d.take("Configuration", int(configLength))...)
	}
	if err := d.finish(); err != nil {
		return nil, err
	}

	return args, nil
}

func decodeTimeoutNowRequest(d *bodyDecoder) (*contract.TimeoutNowRequest, error) {
	args := &contract.TimeoutNowRequest{RPCHeader: d.readRPCHeader()}
	if err := d.finish(); err != nil {
		return nil, err
	}

	return args, nil
}

func decodeRequestPreVoteRequest(d *bodyDecoder) (*contract.RequestPreVoteArgs, error) {
	args := &contract.RequestPreVoteArgs{RPCHeader: d.readRPCHeader()}
	args.Term = d.readInt(fieldTerm, rangeNonNegative)
	args.LastLogIndex = d.readInt(fieldLastLogIndex, rangeMinusOne)
	args.LastLogTerm = d.readInt(fieldLastLogTerm, rangeMinusOne)
	if err := d.finish(); err != nil {
		return nil, err
	}

	return args, nil
}
