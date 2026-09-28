package protocol

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/vskurikhin/raft/pkg/raft/contract"
)

// goldenRequests — доменные значения независимых эталонов запросов.
func goldenRequests() []struct {
	name    string
	typ     RPCType
	command any
} {
	return []struct {
		name    string
		typ     RPCType
		command any
	}{
		{"tn_request", rpcTimeoutNow, &contract.TimeoutNowRequest{RPCHeader: header3(1)}},
		{"rv_request", rpcRequestVote, &contract.RequestVoteArgs{
			RPCHeader: header3(2), Term: 5, CandidateID: 2, LastLogIndex: -1, LastLogTerm: -1,
			LeadershipTransfer: true,
		}},
		{"pv_request", rpcRequestPreVote, &contract.RequestPreVoteArgs{
			RPCHeader: header3(-7), Term: 9, LastLogIndex: 10, LastLogTerm: 8,
		}},
		{"is_request", rpcInstallSnapshot, &contract.InstallSnapshotRequest{
			RPCHeader: header3(1), Term: 4, LeaderID: 1, LastLogIndex: 100, LastLogTerm: 3,
			Configuration: []byte("abc"), ConfigIndex: -1, DataSize: 262145,
		}},
		{"ae_request_empty", rpcAppendEntries, &contract.AppendEntriesArgs{
			RPCHeader: header3(3), Term: 7, LeaderID: 3, PrevLogIndex: 5, PrevLogTerm: 6, LeaderCommit: 4,
		}},
		{"ae_request_noop", rpcAppendEntries, &contract.AppendEntriesArgs{
			RPCHeader: header3(1), Term: 2, LeaderID: 1, PrevLogIndex: -1, PrevLogTerm: -1, LeaderCommit: -1,
			Entries: []contract.LogEntry{
				{Index: 1, Term: 2, Type: contract.LogNoop},
				{Index: 2, Term: 2, Type: contract.LogCommand},
			},
		}},
	}
}

// goldenResponses — доменные значения независимых эталонов ответов.
func goldenResponses() []struct {
	name     string
	typ      RPCType
	response contract.RPCResponse
} {
	return []struct {
		name     string
		typ      RPCType
		response contract.RPCResponse
	}{
		{"ae_reply", rpcAppendEntries, contract.RPCResponse{Reply: &contract.AppendEntriesReply{
			RPCHeader: header3(2), Term: 7, ConflictTerm: -1,
		}}},
		{"rv_reply", rpcRequestVote, contract.RPCResponse{Reply: &contract.RequestVoteReply{
			RPCHeader: header3(2), Term: 5, VoteGranted: true,
		}}},
		{"is_reply", rpcInstallSnapshot, contract.RPCResponse{Reply: &contract.InstallSnapshotResponse{
			RPCHeader: header3(2), Term: 4, Success: true,
		}}},
		{"tn_reply", rpcTimeoutNow, contract.RPCResponse{Reply: &contract.TimeoutNowResponse{
			RPCHeader: header3(2), Success: true, Term: 9,
		}}},
		{"pv_reply", rpcRequestPreVote, contract.RPCResponse{Reply: &contract.RequestPreVoteReply{
			RPCHeader: header3(-1), Term: 9,
		}}},
		{"ae_error1_response", rpcAppendEntries, contract.RPCResponse{Error: contract.ErrRaftShutdown}},
		{"is_error2_response", rpcInstallSnapshot, contract.RPCResponse{Error: contract.ErrEnqueueTimeout}},
		{"tn_error3_response", rpcTimeoutNow, contract.RPCResponse{Error: contract.ErrUnsupportedProtocol}},
		{"rv_error4_response", rpcRequestVote, contract.RPCResponse{Error: errors.New("x")}},
		{"pv_error4_empty_response", rpcRequestPreVote, contract.RPCResponse{Error: errors.New("")}},
	}
}

// TestGoldenRequestEncode — V01: кодирование запросов совпадает с
// независимыми эталонами побайтно.
func TestGoldenRequestEncode(t *testing.T) {
	for _, tc := range goldenRequests() {
		t.Run(tc.name, func(t *testing.T) {
			want := loadGolden(t, tc.name)
			got, err := AppendRequest(nil, tc.command, DefaultLimits())
			if err != nil {
				t.Fatalf("AppendRequest: %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("frame mismatch\n got % x\nwant % x", got, want)
			}
		})
	}
}

// TestGoldenRequestDecode — V01: декодирование эталонов запросов даёт
// ожидаемые доменные значения и тип RPC.
func TestGoldenRequestDecode(t *testing.T) {
	for _, tc := range goldenRequests() {
		t.Run(tc.name, func(t *testing.T) {
			typ, command, err := readRequestBytes(loadGolden(t, tc.name), DefaultLimits())
			if err != nil {
				t.Fatalf("ReadRequest: %v", err)
			}
			if typ != tc.typ {
				t.Fatalf("RPCType = %d, want %d", typ, tc.typ)
			}
			if !reflect.DeepEqual(command, tc.command) {
				t.Fatalf("command = %#v, want %#v", command, tc.command)
			}
		})
	}
}

// TestGoldenResponseEncode — V01: кодирование ответов совпадает с
// независимыми эталонами побайтно.
func TestGoldenResponseEncode(t *testing.T) {
	for _, tc := range goldenResponses() {
		t.Run(tc.name, func(t *testing.T) {
			want := loadGolden(t, tc.name)
			got, err := AppendResponse(nil, tc.typ, tc.response, DefaultLimits())
			if err != nil {
				t.Fatalf("AppendResponse: %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("frame mismatch\n got % x\nwant % x", got, want)
			}
		})
	}
}

// TestGoldenResponseDecode — V01/V12: декодирование эталонов ответов; коды
// 1–3 возвращают сами маркеры contract, код 4 — текст удалённой стороны.
func TestGoldenResponseDecode(t *testing.T) {
	for _, tc := range goldenResponses() {
		t.Run(tc.name, func(t *testing.T) {
			got, err := readResponseBytes(loadGolden(t, tc.name), tc.typ, DefaultLimits())
			if err != nil {
				t.Fatalf("ReadResponse: %v", err)
			}
			assertResponse(t, got, tc.response)
		})
	}
}

func assertResponse(t *testing.T, got, want contract.RPCResponse) {
	t.Helper()

	if want.Error == nil {
		if got.Error != nil || !reflect.DeepEqual(got.Reply, want.Reply) {
			t.Fatalf("response = %#v, want %#v", got, want)
		}

		return
	}
	if got.Reply != nil || got.Error == nil {
		t.Fatalf("response = %#v, want error %v", got, want.Error)
	}
	for _, marker := range []error{contract.ErrRaftShutdown, contract.ErrEnqueueTimeout, contract.ErrUnsupportedProtocol} {
		if want.Error == marker {
			if got.Error != marker {
				t.Fatalf("error = %#v, want marker identity %v", got.Error, marker)
			}

			return
		}
	}
	if got.Error.Error() != want.Error.Error() {
		t.Fatalf("remote error text = %q, want %q", got.Error.Error(), want.Error.Error())
	}
}

// TestGoldenGobDataDecode — V01/V10: замороженный независимый gob-поток
// Data декодируется в конкретный тип; кодировщик проверяется неизменяемой
// оболочкой и равным восстановленным значением, а не байтами gob.
func TestGoldenGobDataDecode(t *testing.T) {
	frame := loadGolden(t, "ae_request_gob_string")
	_, command, err := readRequestBytes(frame, DefaultLimits())
	if err != nil {
		t.Fatalf("ReadRequest: %v", err)
	}
	args, ok := command.(*contract.AppendEntriesArgs)
	if !ok || len(args.Entries) != 1 {
		t.Fatalf("command = %#v", command)
	}
	data, ok := args.Entries[0].Data.(string)
	if !ok || data != "hi" {
		t.Fatalf("Data = %#v, want string \"hi\"", args.Entries[0].Data)
	}

	encoded, err := AppendRequest(nil, args, DefaultLimits())
	if err != nil {
		t.Fatalf("AppendRequest: %v", err)
	}
	const shellEnd = headerSize + appendEntriesMinBody + entryHeaderBytes - int64Bytes
	if !bytes.Equal(encoded[:offsetBodyLength], frame[:offsetBodyLength]) ||
		!bytes.Equal(encoded[offsetReservedTail:shellEnd], frame[offsetReservedTail:shellEnd]) {
		t.Fatalf("envelope mismatch\n got % x\nwant % x", encoded[:shellEnd], frame[:shellEnd])
	}
	_, again, err := readRequestBytes(encoded, DefaultLimits())
	if err != nil || !reflect.DeepEqual(again, args) {
		t.Fatalf("re-decoded = %#v, %v; want %#v", again, err, args)
	}
}
