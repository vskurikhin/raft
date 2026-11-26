package transp

import (
	"bytes"
	"encoding/gob"
	"net"
	"testing"
	"time"

	"github.com/vskurikhin/raft/pkg/raft/contract"
)

// wireRoundTrip прогоняет аргументы AppendEntries через сетевое
// представление и возвращает восстановленный публичный запрос.
func wireRoundTrip(t *testing.T, args contract.AppendEntriesArgs) *contract.AppendEntriesArgs {
	t.Helper()
	wire := appendEntriesToWire(&args)

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&tcpRPCRequest{Type: _rpcAppendEntries, Args: &wire}); err != nil {
		t.Fatalf("gob.Encode: %v", err)
	}
	var decoded tcpRPCRequest
	if err := gob.NewDecoder(&buf).Decode(&decoded); err != nil {
		t.Fatalf("gob.Decode: %v", err)
	}
	wireBack, ok := decoded.Args.(appendEntriesBytesV1)
	if !ok {
		t.Fatalf("сетевое представление декодировано как %T", decoded.Args)
	}
	restored, err := appendEntriesFromWire(&wireBack)
	if err != nil {
		t.Fatalf("appendEntriesFromWire: %v", err)
	}
	return restored
}

// requireSameEntryData сравнивает байты записей с различением nil и
// непустого пустого среза.
func requireSameEntryData(t *testing.T, got, want []contract.LogEntry) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("число записей = %d, want %d", len(got), len(want))
	}
	for i := range got {
		if got[i].Index != want[i].Index || got[i].Term != want[i].Term || got[i].Type != want[i].Type {
			t.Fatalf("запись %d: полей метаданных = %+v, want %+v", i, got[i], want[i])
		}
		if (got[i].Data == nil) != (want[i].Data == nil) || !bytes.Equal(got[i].Data, want[i].Data) {
			t.Fatalf("запись %d: Data = %#v, want %#v", i, got[i].Data, want[i].Data)
		}
	}
}

// TestAppendEntriesWireRoundTripNilEmptyBytes: nil, непустой пустой и
// непустой срезы Data проходят круговой путь без потери различия; байты
// получателя не делят память с входом.
func TestAppendEntriesWireRoundTripNilEmptyBytes(t *testing.T) {
	source := []byte{1, 2, 3}
	args := contract.AppendEntriesArgs{
		RPCHeader:    contract.RPCHeader{ProtocolVersion: contract.ProtocolVersion},
		Term:         5,
		LeaderID:     1,
		PrevLogIndex: 3,
		PrevLogTerm:  4,
		LeaderCommit: 2,
		Entries: []contract.LogEntry{
			{Index: 4, Term: 5, Type: contract.LogCommand, Data: nil},
			{Index: 5, Term: 5, Type: contract.LogCommand, Data: []byte{}},
			{Index: 6, Term: 5, Type: contract.LogCommand, Data: source},
			{Index: 7, Term: 5, Type: contract.LogConfiguration, Data: []byte{9}},
		},
	}

	restored := wireRoundTrip(t, args)
	if restored.Term != args.Term || restored.LeaderID != args.LeaderID ||
		restored.PrevLogIndex != args.PrevLogIndex || restored.PrevLogTerm != args.PrevLogTerm ||
		restored.LeaderCommit != args.LeaderCommit ||
		restored.RPCHeader.ProtocolVersion != args.RPCHeader.ProtocolVersion {
		t.Fatalf("метаданные запроса = %+v, want %+v", restored, args)
	}
	requireSameEntryData(t, restored.Entries, args.Entries)

	// Изменение входа после конверсии не затрагивает полученные байты.
	source[0] = 0xee
	if got := restored.Entries[2].Data; !bytes.Equal(got, []byte{1, 2, 3}) {
		t.Fatalf("байты получателя изменились вместе со входом: %x", got)
	}
	// Изменение полученных байтов не затрагивает вход.
	restored.Entries[2].Data[0] = 0xdd
	if source[0] != 0xee {
		t.Fatalf("изменение полученных байтов затронуло вход: %x", source)
	}
}

// TestAppendEntriesWireEmptyEntriesHeartbeat: пустой Entries (heartbeat)
// проходит круговой путь и остаётся пустым.
func TestAppendEntriesWireEmptyEntriesHeartbeat(t *testing.T) {
	args := contract.AppendEntriesArgs{
		Term:         7,
		LeaderID:     2,
		PrevLogIndex: 10,
		PrevLogTerm:  6,
		LeaderCommit: 0,
		Entries:      nil,
	}
	restored := wireRoundTrip(t, args)
	if len(restored.Entries) != 0 {
		t.Fatalf("heartbeat Entries = %#v, want пустой", restored.Entries)
	}
	if restored.Term != 7 || restored.LeaderCommit != 0 {
		t.Fatalf("heartbeat поля = %+v", restored)
	}
}

// TestAppendEntriesWireRejectsVersionAndPresence: нулевая и неизвестная
// версия, противоречивый признак присутствия Data — ошибка сетевого
// представления.
func TestAppendEntriesWireRejectsVersionAndPresence(t *testing.T) {
	valid := appendEntriesToWire(&contract.AppendEntriesArgs{
		Term: 1, Entries: []contract.LogEntry{{Index: 1, Term: 1, Data: []byte("x")}},
	})

	zero := valid
	zero.EncodingVersion = 0
	if _, err := appendEntriesFromWire(&zero); err == nil {
		t.Fatal("нулевая версия принята")
	}

	future := valid
	future.EncodingVersion = appendEntriesWireVersion + 1
	if _, err := appendEntriesFromWire(&future); err == nil {
		t.Fatal("неизвестная версия принята")
	}

	contradictory := valid
	contradictory.Entries[0].DataPresent = false // payload остался
	if _, err := appendEntriesFromWire(&contradictory); err == nil {
		t.Fatal("противоречивый DataPresent принят")
	}
}

// TestTCPHandleCommandRejectsOldAppendEntriesArgs: прежняя метка
// AppendEntriesArgs распознаётся в обеих формах (value и pointer), но
// отвергается ошибкой из handleCommand до обработчиков: соединение
// закрывается, обычный ответ не отправляется.
func TestTCPHandleCommandRejectsOldAppendEntriesArgs(t *testing.T) {
	cases := []struct {
		name string
		args any
	}{
		{"pointer", &contract.AppendEntriesArgs{Term: 1, LeaderID: 0}},
		{"value", contract.AppendEntriesArgs{Term: 1, LeaderID: 0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server, _, cleanup := newWireTestPair(t)
			defer cleanup()

			conn := dialWireTest(t, string(server.LocalAddr()))
			defer func() { _ = conn.Close() }()
			if err := gob.NewEncoder(conn).Encode(&tcpRPCRequest{Type: _rpcAppendEntries, Args: tc.args}); err != nil {
				t.Fatalf("gob.Encode: %v", err)
			}

			_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			var rpcError string
			if err := gob.NewDecoder(conn).Decode(&rpcError); err == nil {
				// Приёмник обязан закрыть соединение без обычного ответа:
				// декодирование либо обрыв соединения, но не Success=false.
				t.Fatalf("получен обычный ответ на старую метку: %q", rpcError)
			}
		})
	}
}

// TestTCPAppendEntriesHeartbeatFastPath: корректное DTO конвертируется до
// heartbeat fast-path — при зарегистрированном обработчике пульс
// обслуживается им и не попадает в consumerCh.
func TestTCPAppendEntriesHeartbeatFastPath(t *testing.T) {
	server, client, cleanup := newWireTestPair(t)
	defer cleanup()

	heartbeatSeen := make(chan struct{}, 1)
	server.SetHeartbeatHandler(func(rpc contract.RPC) {
		select {
		case heartbeatSeen <- struct{}{}:
		default:
		}
		rpc.RespChan <- contract.RPCResponse{Reply: &contract.AppendEntriesReply{Term: 3, Success: true}}
	})

	if _, err := client.AppendEntries(1, contract.AppendEntriesArgs{Term: 3, LeaderCommit: 0}); err != nil {
		t.Fatalf("heartbeat AppendEntries: %v", err)
	}
	select {
	case <-heartbeatSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("heartbeat не обслужен fast-path обработчиком")
	}

	// consumerCh пуст: пульс не дошёл до общего потребителя.
	select {
	case rpc := <-server.Consumer():
		t.Fatalf("heartbeat попал в consumerCh: %+v", rpc)
	default:
	}
}

// dialWireTest открывает прямое TCP-соединение к транспорту.
func dialWireTest(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	return conn
}

// newWireTestPair создаёт пару транспортов с одним соседом.
func newWireTestPair(t *testing.T) (*TCPTransport, *TCPTransport, func()) {
	t.Helper()
	server, err := NewTCPTransport("127.0.0.1:0", TCPTimeouts{}, 0)
	if err != nil {
		t.Fatalf("server transport: %v", err)
	}
	client, err := NewTCPTransport("127.0.0.1:0", TCPTimeouts{}, 0)
	if err != nil {
		t.Fatalf("client transport: %v", err)
	}
	client.Connect(1, string(server.LocalAddr()))
	cleanup := func() {
		client.Close()
		server.Close()
	}
	return server, client, cleanup
}
