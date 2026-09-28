package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/vskurikhin/raft/pkg/raft/contract"
)

// TestErrorCodes — V03/V12: код выбирается через errors.Is, в том числе
// для обёрнутого маркера; ErrorBytes кодов 1–3 — канонический текст маркера;
// декодирование возвращает сам маркер contract.
func TestErrorCodes(t *testing.T) {
	cases := []struct {
		cause  error
		code   uint16
		marker error
	}{
		{contract.ErrRaftShutdown, errorCodeShutdown, contract.ErrRaftShutdown},
		{fmt.Errorf("handler: %w", contract.ErrRaftShutdown), errorCodeShutdown, contract.ErrRaftShutdown},
		{contract.ErrEnqueueTimeout, errorCodeEnqueue, contract.ErrEnqueueTimeout},
		{fmt.Errorf("wait: %w", contract.ErrEnqueueTimeout), errorCodeEnqueue, contract.ErrEnqueueTimeout},
		{contract.ErrUnsupportedProtocol, errorCodeUnsupported, contract.ErrUnsupportedProtocol},
		{fmt.Errorf("version: %w", contract.ErrUnsupportedProtocol), errorCodeUnsupported, contract.ErrUnsupportedProtocol},
		{errors.New("disk full"), errorCodeRemote, nil},
	}
	for typ := range rpcTypeCount {
		for _, tc := range cases {
			frame, err := AppendResponse(nil, typ, contract.RPCResponse{Error: tc.cause}, DefaultLimits())
			if err != nil {
				t.Fatalf("AppendResponse %v: %v", tc.cause, err)
			}
			if code := binary.BigEndian.Uint16(frame[headerSize:]); code != tc.code {
				t.Fatalf("%v encoded with code %d, want %d", tc.cause, code, tc.code)
			}
			got, err := readResponseBytes(frame, typ, DefaultLimits())
			if err != nil || got.Reply != nil {
				t.Fatalf("decode %v: %#v, %v", tc.cause, got, err)
			}
			if tc.marker != nil {
				if got.Error != tc.marker || !errors.Is(got.Error, tc.marker) {
					t.Fatalf("decoded %#v, want marker %v", got.Error, tc.marker)
				}
				if text := string(frame[headerSize+errorEnvelopeBytes:]); text != tc.marker.Error() {
					t.Fatalf("ErrorBytes %q, want canonical %q", text, tc.marker.Error())
				}

				continue
			}
			if got.Error.Error() != tc.cause.Error() {
				t.Fatalf("remote text %q", got.Error.Error())
			}
			for _, marker := range []error{contract.ErrRaftShutdown, contract.ErrEnqueueTimeout, contract.ErrUnsupportedProtocol} {
				if errors.Is(got.Error, marker) {
					t.Fatalf("remote error matches marker %v", marker)
				}
			}
		}
	}
}

// TestErrorPriorityOverReply — V12: при заданной Error Reply не кодируется,
// даже некорректный или нулевой.
func TestErrorPriorityOverReply(t *testing.T) {
	replies := []any{nil, (*contract.AppendEntriesReply)(nil), &contract.AppendEntriesReply{}, "garbage",
		&contract.AppendEntriesReply{RPCHeader: header3(1), Term: 1, Success: true}}
	want := loadGolden(t, "ae_error1_response")
	for _, reply := range replies {
		frame, err := AppendResponse(nil, rpcAppendEntries,
			contract.RPCResponse{Reply: reply, Error: contract.ErrRaftShutdown}, DefaultLimits())
		if err != nil || !bytes.Equal(frame, want) {
			t.Fatalf("reply %#v: frame %x, %v", reply, frame, err)
		}
	}
}

// TestReplyWithoutError — SA-1156/V12: при Error=nil: nil interface и
// typed nil — ErrFormat без разыменования; указатель на нулевую структуру —
// локальная contract.ErrUnsupportedProtocol; неверный конкретный тип —
// ErrFormat; кадр не пишется.
func TestReplyWithoutError(t *testing.T) {
	typedNils := map[RPCType]any{
		rpcAppendEntries:   (*contract.AppendEntriesReply)(nil),
		rpcRequestVote:     (*contract.RequestVoteReply)(nil),
		rpcInstallSnapshot: (*contract.InstallSnapshotResponse)(nil),
		rpcTimeoutNow:      (*contract.TimeoutNowResponse)(nil),
		rpcRequestPreVote:  (*contract.RequestPreVoteReply)(nil),
	}
	zeros := map[RPCType]any{
		rpcAppendEntries:   &contract.AppendEntriesReply{},
		rpcRequestVote:     &contract.RequestVoteReply{},
		rpcInstallSnapshot: &contract.InstallSnapshotResponse{},
		rpcTimeoutNow:      &contract.TimeoutNowResponse{},
		rpcRequestPreVote:  &contract.RequestPreVoteReply{},
	}
	for typ := range rpcTypeCount {
		assertEncodeError(t, typ, nil, ErrFormat)
		assertEncodeError(t, typ, typedNils[typ], ErrFormat)
		assertEncodeError(t, typ, zeros[typ], contract.ErrUnsupportedProtocol)
		for other := range rpcTypeCount {
			if other != typ {
				assertEncodeError(t, typ, zeros[other], ErrFormat)
			}
		}
		assertEncodeError(t, typ, contract.AppendEntriesReply{RPCHeader: header3(1)}, ErrFormat)
	}

	_, err := AppendResponse(nil, rpcTypeCount, contract.RPCResponse{Error: contract.ErrRaftShutdown}, DefaultLimits())
	requireIs(t, err, ErrFormat)

	normal := &contract.RequestVoteReply{RPCHeader: header3(1), Term: 3, VoteGranted: false}
	frame, err := AppendResponse(nil, rpcRequestVote, contract.RPCResponse{Reply: normal}, DefaultLimits())
	if err != nil || binary.BigEndian.Uint16(frame[headerSize:]) != errorCodeNone {
		t.Fatalf("VoteGranted=false must be code 0: %x, %v", frame, err)
	}
}

func assertEncodeError(t *testing.T, typ RPCType, reply any, want error) {
	t.Helper()

	frame, err := AppendResponse([]byte("p"), typ, contract.RPCResponse{Reply: reply}, DefaultLimits())
	requireIs(t, err, want)
	if string(frame) != "p" {
		t.Fatalf("RPCType %d reply %#v: frame %q", typ, reply, frame)
	}
	if want == ErrFormat && errors.Is(err, contract.ErrUnsupportedProtocol) {
		t.Fatalf("RPCType %d reply %#v: format error masked as unsupported protocol", typ, reply)
	}
}

// TestErrorTextLength — V12: текст 4095/4096 Б передаётся как есть, 4097 Б
// усекается до 4082 Б + "...[truncated]"; не-UTF-8 и пустой текст сохраняются.
func TestErrorTextLength(t *testing.T) {
	for _, n := range []int{0, 1, 4095, 4096, 4097, 10000} {
		text := strings.Repeat("e", n)
		frame, err := AppendResponse(nil, rpcRequestVote, contract.RPCResponse{Error: errors.New(text)}, DefaultLimits())
		if err != nil {
			t.Fatalf("length %d: %v", n, err)
		}
		want := text
		if n > maxErrorTextBytes {
			want = text[:4082] + "...[truncated]"
		}
		got, err := readResponseBytes(frame, rpcRequestVote, DefaultLimits())
		if err != nil || got.Error.Error() != want || len(got.Error.Error()) > maxErrorTextBytes {
			t.Fatalf("length %d: got %d bytes, %v", n, len(got.Error.Error()), err)
		}
	}

	invalidUTF8 := string([]byte{0xff, 0xfe, 'x', 0x80})
	frame, err := AppendResponse(nil, rpcAppendEntries, contract.RPCResponse{Error: errors.New(invalidUTF8)}, DefaultLimits())
	if err != nil {
		t.Fatalf("non-UTF-8: %v", err)
	}
	got, err := readResponseBytes(frame, rpcAppendEntries, DefaultLimits())
	if err != nil || got.Error.Error() != invalidUTF8 {
		t.Fatalf("non-UTF-8 decoded %q, %v", got.Error, err)
	}
}

// buildErrorResponse строит ответ вручную с заданной оболочкой и хвостом.
func buildErrorResponse(typ RPCType, code, reserved uint16, length uint32, tail []byte) []byte {
	body := binary.BigEndian.AppendUint16(nil, code)
	body = binary.BigEndian.AppendUint16(body, reserved)
	body = binary.BigEndian.AppendUint32(body, length)
	body = append(body, tail...)

	return append(appendHeader(nil, directionResponse, typ, uint64(len(body))), body...)
}

// TestErrorEnvelopeDecode — V12: неизвестный код, reserved, лишние байты
// ответа на ошибке, несоответствие ErrorLength, текст кода 1–3 не того маркера
// и код 0 с ErrorLength — ErrFormat.
func TestErrorEnvelopeDecode(t *testing.T) {
	shutdown := []byte(contract.ErrRaftShutdown.Error())
	reply := loadGolden(t, "rv_reply")[responseBody:]
	cases := map[string][]byte{
		"unknown code 5":         buildErrorResponse(rpcRequestVote, 5, 0, 1, []byte("x")),
		"unknown code 65535":     buildErrorResponse(rpcRequestVote, 65535, 0, 0, nil),
		"reserved":               buildErrorResponse(rpcRequestVote, errorCodeRemote, 1, 1, []byte("x")),
		"reply after error":      buildErrorResponse(rpcRequestVote, errorCodeRemote, 0, 1, append([]byte("x"), reply...)),
		"length beyond body":     buildErrorResponse(rpcRequestVote, errorCodeRemote, 0, 2, []byte("x")),
		"length shorter":         buildErrorResponse(rpcRequestVote, errorCodeRemote, 0, 0, []byte("x")),
		"code 1 other text":      buildErrorResponse(rpcRequestVote, errorCodeShutdown, 0, 1, []byte("x")),
		"code 2 shutdown text":   buildErrorResponse(rpcRequestVote, errorCodeEnqueue, 0, uint32(len(shutdown)), shutdown),
		"code 3 empty text":      buildErrorResponse(rpcRequestVote, errorCodeUnsupported, 0, 0, nil),
		"code 0 with length":     buildErrorResponse(rpcRequestVote, errorCodeNone, 0, 1, append([]byte("x"), reply...)),
		"code 0 short reply":     buildErrorResponse(rpcRequestVote, errorCodeNone, 0, 0, reply[:len(reply)-1]),
		"code 0 without reply":   buildErrorResponse(rpcRequestVote, errorCodeNone, 0, 0, nil),
		"code 0 AE-length reply": buildErrorResponse(rpcRequestVote, errorCodeNone, 0, 0, make([]byte, 41)),
	}
	for name, frame := range cases {
		response, err := readResponseBytes(frame, rpcRequestVote, DefaultLimits())
		if !errors.Is(err, ErrFormat) {
			t.Fatalf("%s: err = %v, want ErrFormat", name, err)
		}
		if response != (contract.RPCResponse{}) {
			t.Fatalf("%s: response %#v", name, response)
		}
	}

	longest := buildErrorResponse(rpcRequestVote, errorCodeRemote, 0, maxErrorTextBytes,
		bytes.Repeat([]byte{'z'}, maxErrorTextBytes))
	if _, err := readResponseBytes(longest, rpcRequestVote, DefaultLimits()); err != nil {
		t.Fatalf("4096-byte text: %v", err)
	}
	tooLong := buildErrorResponse(rpcRequestVote, errorCodeRemote, 0, maxErrorTextBytes+1,
		bytes.Repeat([]byte{'z'}, maxErrorTextBytes+1))
	_, err := readResponseBytes(tooLong, rpcRequestVote, DefaultLimits())
	requireIs(t, err, ErrLimit)
}
