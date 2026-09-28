package transp

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/vskurikhin/raft"
	"github.com/vskurikhin/raft/pkg/raft/contract"
	"github.com/vskurikhin/raft/pkg/raft/protocol"
)

// testRPCHeader — заголовок RPC совместимой версии протокола для тестовых
// запросов и ответов.
var testRPCHeader = contract.RPCHeader{ProtocolVersion: contract.ProtocolVersion, ServerID: 1}

// Раскладка заголовка кадра по таблице спецификации формата: смещения
// полей и размер. Значения задаются тестом независимо от кодека.
const (
	wireHeaderBytes     = 24
	wireOffsetDirection = 8
	wireOffsetType      = 9
	wireOffsetBodyLen   = 12
)

// wireHeader строит 24 байта заголовка кадра вручную по таблице формата.
func wireHeader(direction, typ byte, bodyLength uint64) []byte {
	h := make([]byte, wireHeaderBytes)
	copy(h, "RRPC")
	binary.BigEndian.PutUint16(h[4:], 1)
	binary.BigEndian.PutUint16(h[6:], wireHeaderBytes)
	h[wireOffsetDirection] = direction
	h[wireOffsetType] = typ
	binary.BigEndian.PutUint64(h[wireOffsetBodyLen:], bodyLength)
	return h
}

func appendI64(b []byte, v int64) []byte { return binary.BigEndian.AppendUint64(b, uint64(v)) }

// wireTimeoutNowRequest — запрос TimeoutNow вручную: H(PV, ServerID).
func wireTimeoutNowRequest(pv, serverID int64) []byte {
	body := appendI64(appendI64(nil, pv), serverID)
	return append(wireHeader(0, 3, uint64(len(body))), body...)
}

// wireAppendEntriesRequest — запрос AppendEntries с одной gob-записью Data
// вручную. extraBody дописывается в тело после записи внутри BodyLength.
func wireAppendEntriesRequest(data, extraBody []byte) []byte {
	body := appendI64(appendI64(nil, 3), 1)       // H
	body = appendI64(body, 1)                     // Term
	body = appendI64(body, 0)                     // LeaderID
	body = appendI64(body, -1)                    // PrevLogIndex
	body = appendI64(body, -1)                    // PrevLogTerm
	body = appendI64(body, -1)                    // LeaderCommit
	body = binary.BigEndian.AppendUint64(body, 1) // EntryCount
	body = appendI64(body, 0)                     // Index
	body = appendI64(body, 1)                     // Term
	body = append(body, 0, 1, 0, 0, 0, 0, 0, 0)   // LogType, DataKind, reserved
	body = binary.BigEndian.AppendUint64(body, uint64(len(data)))
	body = append(body, data...)
	body = append(body, extraBody...)
	return append(wireHeader(0, 0, uint64(len(body))), body...)
}

// TestTCPRPCTypeNamedValues — именованные типы RPC транспорта совпадают
// с таблицей формата: 0 AppendEntries, 1 RequestVote, 2 InstallSnapshot,
// 3 TimeoutNow, 4 RequestPreVote.
func TestTCPRPCTypeNamedValues(t *testing.T) {
	want := map[protocol.RPCType]struct {
		value uint8
		name  string
	}{
		rpcTypeAppendEntries:   {0, "AppendEntries"},
		rpcTypeRequestVote:     {1, "RequestVote"},
		rpcTypeInstallSnapshot: {2, "InstallSnapshot"},
		rpcTypeTimeoutNow:      {3, "TimeoutNow"},
		rpcTypeRequestPreVote:  {4, "RequestPreVote"},
	}
	if len(want) != 5 {
		t.Fatalf("named RPC types are not distinct: %d", len(want))
	}
	for typ, w := range want {
		if uint8(typ) != w.value {
			t.Fatalf("%s = %d, want %d", w.name, typ, w.value)
		}
		if got := rpcTypeName(typ); got != w.name {
			t.Fatalf("rpcTypeName(%d) = %q, want %q", typ, got, w.name)
		}
	}
	if got := rpcTypeName(5); got != "RPCType(5)" {
		t.Fatalf("rpcTypeName(5) = %q", got)
	}
}

// rawPeer — сырой TCP-сосед: принимает соединения и отдаёт их тесту.
// close закрывает слушатель и все принятые соединения; вызывается
// отложенно после проверки утечек горутин в порядке defer теста.
type rawPeer struct {
	ln       net.Listener
	conns    chan net.Conn
	mu       sync.Mutex
	accepted []net.Conn
	done     chan struct{}
}

func newRawPeer(t *testing.T) *rawPeer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	p := &rawPeer{ln: ln, conns: make(chan net.Conn, 16), done: make(chan struct{})}
	go func() {
		defer close(p.done)
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			p.mu.Lock()
			p.accepted = append(p.accepted, c)
			p.mu.Unlock()
			p.conns <- c
		}
	}()
	return p
}

func (p *rawPeer) close() {
	_ = p.ln.Close()
	<-p.done
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.accepted {
		_ = c.Close()
	}
}

func (p *rawPeer) accept(t *testing.T) net.Conn {
	t.Helper()
	select {
	case c := <-p.conns:
		return c
	case <-time.After(2 * time.Second):
		t.Fatal("no connection accepted")
		return nil
	}
}

// noConnection проверяет, что за время ожидания соединение не открывалось.
func (p *rawPeer) noConnection(t *testing.T, wait time.Duration) {
	t.Helper()
	select {
	case c := <-p.conns:
		_ = c.Close()
		t.Fatal("unexpected connection")
	case <-time.After(wait):
	}
}

// TestTCPWireTypeByteAndGolden — на реальном TCP-соединении каждый из пяти
// RPC пишет в поле RPCType заголовка своё значение из таблицы формата
// (V02/V15); запрос TimeoutNow совпадает побайтово с независимым эталоном
// спецификации (PV=3, ServerID=1). Ответ другого типа отвергается
// отправителем и не возвращает соединение в пул.
func TestTCPWireTypeByteAndGolden(t *testing.T) {
	defer leaktest.CheckTimeout(t, raft.LeaktestBudget)()
	peer := newRawPeer(t)
	defer peer.close()
	client, err := NewTCPTransport("127.0.0.1:0", uniformTCPTimeouts(time.Second), 2)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.Connect(1, peer.ln.Addr().String())

	golden, err := hex.DecodeString(strings.ReplaceAll(
		"52525043000100180003000000000000000000100000000000000000000000030000000000000001", " ", ""))
	if err != nil {
		t.Fatal(err)
	}

	calls := []struct {
		name string
		want byte
		call func() error
	}{
		{rpcTypeName(rpcTypeAppendEntries), 0, func() error {
			_, err := client.AppendEntries(1, contract.AppendEntriesArgs{RPCHeader: testRPCHeader, Term: 1})
			return err
		}},
		{rpcTypeName(rpcTypeRequestVote), 1, func() error {
			_, err := client.RequestVote(1, contract.RequestVoteArgs{RPCHeader: testRPCHeader, Term: 1})
			return err
		}},
		{rpcTypeName(rpcTypeInstallSnapshot), 2, func() error {
			_, err := client.InstallSnapshot(1, contract.InstallSnapshotRequest{
				RPCHeader: testRPCHeader, Term: 1, LastLogIndex: 1, LastLogTerm: 1, DataSize: 1,
			}, bytes.NewReader([]byte{7}))
			return err
		}},
		{rpcTypeName(rpcTypeTimeoutNow), 3, func() error {
			_, err := client.TimeoutNow(1, contract.TimeoutNowRequest{RPCHeader: testRPCHeader})
			return err
		}},
		{rpcTypeName(rpcTypeRequestPreVote), 4, func() error {
			_, err := client.RequestPreVote(1, contract.RequestPreVoteArgs{RPCHeader: testRPCHeader, Term: 1})
			return err
		}},
	}
	for _, c := range calls {
		t.Run(c.name, func(t *testing.T) {
			done := make(chan error, 1)
			go func() { done <- c.call() }()
			conn := peer.accept(t)
			head := make([]byte, wireHeaderBytes)
			if _, err := io.ReadFull(conn, head); err != nil {
				t.Fatalf("read header: %v", err)
			}
			if head[wireOffsetType] != c.want || head[wireOffsetDirection] != 0 {
				t.Fatalf("type byte = %d direction = %d, want %d/0", head[wireOffsetType], head[wireOffsetDirection], c.want)
			}
			body := make([]byte, binary.BigEndian.Uint64(head[wireOffsetBodyLen:]))
			if _, err := io.ReadFull(conn, body); err != nil {
				t.Fatalf("read body: %v", err)
			}
			if c.want == 2 {
				// Тело снимка DataSize=1 следует за управляющим кадром.
				if _, err := io.ReadFull(conn, make([]byte, 1)); err != nil {
					t.Fatalf("read snapshot data: %v", err)
				}
			}
			if c.want == 3 && !bytes.Equal(append(head, body...), golden) {
				t.Fatalf("TimeoutNow frame % x, want golden % x", append(head, body...), golden)
			}
			// Ответ другого типа (RequestVote вместо ожидаемого; для
			// RequestVote — AppendEntries): отправитель обязан отвергнуть.
			other := byte(1)
			if c.want == 1 {
				other = 0
			}
			reply := wireHeader(1, other, 8)
			reply = append(reply, 0, 4, 0, 0, 0, 0, 0, 0) // код 4, пустой текст
			_, _ = conn.Write(reply)
			err := <-done
			if !errors.Is(err, protocol.ErrFormat) {
				t.Fatalf("response of another type: err = %v, want ErrFormat", err)
			}
			// Соединение закрыто отправителем: чтение даёт EOF.
			_ = conn.SetReadDeadline(time.Now().Add(time.Second))
			if n, err := conn.Read(make([]byte, 1)); err == nil || n != 0 {
				t.Fatalf("connection not closed by sender: n=%d err=%v", n, err)
			}
			if pooled := client.pooledCount(contract.ServerAddress(peer.ln.Addr().String())); pooled != 0 {
				t.Fatalf("pool = %d after rejected response, want 0", pooled)
			}
		})
	}
}

// pooledCount — число соединений пула к адресу (для тестов судьбы соединения).
func (t *TCPTransport) pooledCount(target contract.ServerAddress) int {
	t.connPoolLock.Lock()
	defer t.connPoolLock.Unlock()
	return len(t.connPool[target])
}

// fiveRPCHandler отвечает на пять RPC: ответ формирует reply по команде.
func fiveRPCHandler(t *testing.T, trans *TCPTransport, reply func(rpc contract.RPC) contract.RPCResponse) (
	*atomic.Int64, func(),
) {
	t.Helper()
	var delivered atomic.Int64
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			select {
			case rpc := <-trans.Consumer():
				delivered.Add(1)
				if rpc.Reader != nil {
					_, _ = io.Copy(io.Discard, rpc.Reader)
				}
				rpc.RespChan <- reply(rpc)
			case <-done:
				return
			}
		}
	}()
	return &delivered, func() { close(done); <-stopped }
}

// TestTCPFivePairsRoundTrip — V15: реальный TCP, все пять пар запрос/ответ
// со всеми значениями флагов, Success=false/VoteGranted=false (штатный код
// 0) и удалённая ошибка кода 4. Потребитель получает ровно один полностью
// заполненный указатель на команду; соединение переиспользуется.
func TestTCPFivePairsRoundTrip(t *testing.T) {
	defer leaktest.CheckTimeout(t, raft.LeaktestBudget)()
	client, server, cleanup := newTCPPair(t, uniformTCPTimeouts(time.Second))
	defer cleanup()

	var mu sync.Mutex
	var got []any
	flag := false
	delivered, stop := fiveRPCHandler(t, server, func(rpc contract.RPC) contract.RPCResponse {
		mu.Lock()
		got = append(got, rpc.Command)
		f := flag
		mu.Unlock()
		switch cmd := rpc.Command.(type) {
		case *contract.AppendEntriesArgs:
			return contract.RPCResponse{Reply: &contract.AppendEntriesReply{
				RPCHeader: testRPCHeader, Term: cmd.Term, Success: f, ConflictIndex: 7, ConflictTerm: -1,
			}}
		case *contract.RequestVoteArgs:
			return contract.RPCResponse{Reply: &contract.RequestVoteReply{RPCHeader: testRPCHeader, Term: cmd.Term, VoteGranted: f}}
		case *contract.RequestPreVoteArgs:
			return contract.RPCResponse{Reply: &contract.RequestPreVoteReply{RPCHeader: testRPCHeader, Term: cmd.Term, VoteGranted: f}}
		case *contract.TimeoutNowRequest:
			return contract.RPCResponse{Reply: &contract.TimeoutNowResponse{RPCHeader: testRPCHeader, Success: f, Term: 9}}
		case *contract.InstallSnapshotRequest:
			return contract.RPCResponse{Reply: &contract.InstallSnapshotResponse{RPCHeader: testRPCHeader, Term: cmd.Term, Success: f}}
		}
		return contract.RPCResponse{Error: errors.New("unexpected")}
	})
	defer stop()

	addr := contract.ServerAddress(server.LocalAddr())
	for _, f := range []bool{false, true} {
		mu.Lock()
		flag = f
		got = nil
		mu.Unlock()
		ae := contract.AppendEntriesArgs{
			RPCHeader: testRPCHeader, Term: 5, LeaderID: -3, PrevLogIndex: -1, PrevLogTerm: -1, LeaderCommit: 2,
			Entries: []contract.LogEntry{
				{Index: 1, Term: 5, Type: contract.LogNoop},
				{Index: 2, Term: 5, Type: contract.LogCommand, Data: "cmd"},
				{Index: 3, Term: 5, Type: contract.LogConfiguration, Data: []byte{1, 2}},
			},
		}
		aeReply, err := client.AppendEntries(1, ae)
		if err != nil || aeReply.Success != f || aeReply.ConflictIndex != 7 || aeReply.ConflictTerm != -1 {
			t.Fatalf("AE: %+v %v", aeReply, err)
		}
		rv := contract.RequestVoteArgs{RPCHeader: testRPCHeader, Term: 6, CandidateID: 2, LastLogIndex: -1, LastLogTerm: -1,
			LeadershipTransfer: f}
		if r, err := client.RequestVote(1, rv); err != nil || r.VoteGranted != f || r.Term != 6 {
			t.Fatalf("RV: %+v %v", r, err)
		}
		pv := contract.RequestPreVoteArgs{RPCHeader: testRPCHeader, Term: 7, LastLogIndex: 4, LastLogTerm: 2}
		if r, err := client.RequestPreVote(1, pv); err != nil || r.VoteGranted != f || r.Term != 7 {
			t.Fatalf("PreVote: %+v %v", r, err)
		}
		if r, err := client.TimeoutNow(1, contract.TimeoutNowRequest{RPCHeader: testRPCHeader}); err != nil ||
			r.Success != f || r.Term != 9 {
			t.Fatalf("TimeoutNow: %+v %v", r, err)
		}
		if client.pooledCount(addr) != 1 {
			t.Fatalf("pool = %d, want 1 reused connection", client.pooledCount(addr))
		}
		is := contract.InstallSnapshotRequest{RPCHeader: testRPCHeader, Term: 8, LeaderID: 1, LastLogIndex: 3,
			LastLogTerm: 2, ConfigIndex: -1, Configuration: []byte{9}, DataSize: 3}
		if r, err := client.InstallSnapshot(1, is, bytes.NewReader([]byte{1, 2, 3})); err != nil || r.Success != f || r.Term != 8 {
			t.Fatalf("InstallSnapshot: %+v %v", r, err)
		}
		mu.Lock()
		want := []any{&ae, &rv, &pv, &contract.TimeoutNowRequest{RPCHeader: testRPCHeader}, &is}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("delivered commands:\n%#v\nwant\n%#v", got, want)
		}
		mu.Unlock()
	}
	if delivered.Load() != 10 {
		t.Fatalf("delivered %d commands, want exactly 10", delivered.Load())
	}
}

// TestTCPRemoteErrorCodesAndPoolFate — V12/V20 и маркер остановки трёх RPC:
// ответ с Error кодируется кодом 1/2/3 независимо от вида Reply (nil, typed
// nil, нулевая структура, заполненный ответ), errors.Is на стороне
// отправителя истинен, соединение закрывается; код 4 — текстовая ошибка,
// соединение переиспользуется; штатный отрицательный ответ кода 0 — тоже.
func TestTCPRemoteErrorCodesAndPoolFate(t *testing.T) {
	defer leaktest.CheckTimeout(t, raft.LeaktestBudget)()
	client, server, cleanup := newTCPPair(t, uniformTCPTimeouts(time.Second))
	defer cleanup()
	addr := contract.ServerAddress(server.LocalAddr())

	var next atomic.Value
	_, stop := fiveRPCHandler(t, server, func(contract.RPC) contract.RPCResponse {
		return next.Load().(contract.RPCResponse)
	})
	defer stop()

	replies := map[string]any{
		"nil":        nil,
		"typed nil":  (*contract.AppendEntriesReply)(nil),
		"zero":       &contract.AppendEntriesReply{},
		"full reply": &contract.AppendEntriesReply{RPCHeader: testRPCHeader, Term: 3, Success: true},
	}
	rpcs := map[string]func() error{
		rpcTypeName(rpcTypeAppendEntries): func() error {
			_, err := client.AppendEntries(1, contract.AppendEntriesArgs{RPCHeader: testRPCHeader, Term: 1})
			return err
		},
		rpcTypeName(rpcTypeRequestVote): func() error {
			_, err := client.RequestVote(1, contract.RequestVoteArgs{RPCHeader: testRPCHeader, Term: 1})
			return err
		},
		rpcTypeName(rpcTypeRequestPreVote): func() error {
			_, err := client.RequestPreVote(1, contract.RequestPreVoteArgs{RPCHeader: testRPCHeader, Term: 1})
			return err
		},
	}
	markers := []error{contract.ErrRaftShutdown, contract.ErrEnqueueTimeout, contract.ErrUnsupportedProtocol}
	for rpcName, call := range rpcs {
		for _, marker := range markers {
			for replyName, reply := range replies {
				next.Store(contract.RPCResponse{Reply: reply, Error: marker})
				client.CloseStreams()
				err := call()
				if !errors.Is(err, marker) {
					t.Fatalf("%s/%v/%s: err = %v, want %v", rpcName, marker, replyName, err, marker)
				}
				if n := client.pooledCount(addr); n != 0 {
					t.Fatalf("%s/%v/%s: pooled %d, want closed connection", rpcName, marker, replyName, n)
				}
			}
		}
		// Код 4: текст удалённой стороны, соединение живо.
		next.Store(contract.RPCResponse{Error: errors.New("remote failure")})
		client.CloseStreams()
		if err := call(); err == nil || !strings.Contains(err.Error(), "remote failure") || closesConnection(err) {
			t.Fatalf("%s code 4: err = %v", rpcName, err)
		}
		if n := client.pooledCount(addr); n != 1 {
			t.Fatalf("%s code 4: pooled %d, want 1", rpcName, n)
		}
	}
	// Штатный отрицательный ответ с PV=3 — код 0, не остановка.
	next.Store(contract.RPCResponse{Reply: &contract.RequestVoteReply{RPCHeader: testRPCHeader, Term: 4}})
	reply, err := client.RequestVote(1, contract.RequestVoteArgs{RPCHeader: testRPCHeader, Term: 1})
	if err != nil || reply.VoteGranted || reply.Term != 4 {
		t.Fatalf("negative vote: %+v %v", reply, err)
	}
	if n := client.pooledCount(addr); n != 1 {
		t.Fatalf("negative vote: pooled %d, want 1", n)
	}
}

// TestTCPInvalidReplyWithoutError — ответ без Error с nil, typed nil или
// нулевой структурой правильного типа: получатель не пишет кадр (локальная
// ошибка кодировщика) и закрывает соединение; отправитель получает ошибку
// ввода-вывода, не успех, и не возвращает соединение в пул.
func TestTCPInvalidReplyWithoutError(t *testing.T) {
	defer leaktest.CheckTimeout(t, raft.LeaktestBudget)()
	client, server, cleanup := newTCPPair(t, uniformTCPTimeouts(time.Second))
	defer cleanup()
	addr := contract.ServerAddress(server.LocalAddr())

	var next atomic.Value
	_, stop := fiveRPCHandler(t, server, func(contract.RPC) contract.RPCResponse {
		return next.Load().(contract.RPCResponse)
	})
	defer stop()

	for name, reply := range map[string]any{
		"nil":       nil,
		"typed nil": (*contract.RequestVoteReply)(nil),
		"zero":      &contract.RequestVoteReply{},
		"wrong":     &contract.AppendEntriesReply{RPCHeader: testRPCHeader},
	} {
		next.Store(contract.RPCResponse{Reply: reply})
		_, err := client.RequestVote(1, contract.RequestVoteArgs{RPCHeader: testRPCHeader, Term: 1})
		if err == nil || closesConnection(err) {
			t.Fatalf("%s: err = %v, want I/O error", name, err)
		}
		if n := client.pooledCount(addr); n != 0 {
			t.Fatalf("%s: pooled %d", name, n)
		}
	}
}

// TestTCPEncodeBeforeConnection — V07/V13: ошибка кодирования запроса
// (некодируемая Data, несовместимая версия, Data больше D) обнаруживается до
// установки соединения: поток не открывается и частичный кадр не пишется.
func TestTCPEncodeBeforeConnection(t *testing.T) {
	defer leaktest.CheckTimeout(t, raft.LeaktestBudget)()
	peer := newRawPeer(t)
	defer peer.close()
	client, err := NewTCPTransportWithLimits("127.0.0.1:0", uniformTCPTimeouts(time.Second), 2,
		contract.Limits{MaxFrameBytes: 4128, MaxEntries: 3, MaxDataBytes: 64, MaxConfigurationBytes: 16})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.Connect(1, peer.ln.Addr().String())

	cases := map[string]contract.AppendEntriesArgs{
		"gob error": {RPCHeader: testRPCHeader, Entries: []contract.LogEntry{{Data: func() {}}}},
		"version":   {RPCHeader: contract.RPCHeader{ProtocolVersion: 2}},
		"data > D":  {RPCHeader: testRPCHeader, Entries: []contract.LogEntry{{Data: strings.Repeat("x", 100)}}},
		"count > N": {RPCHeader: testRPCHeader, Entries: make([]contract.LogEntry, 4)},
	}
	for name, args := range cases {
		if _, err := client.AppendEntries(1, args); err == nil {
			t.Fatalf("%s: accepted", name)
		}
	}
	if _, err := client.InstallSnapshot(1, contract.InstallSnapshotRequest{
		RPCHeader: testRPCHeader, Term: 1, LastLogIndex: 1, LastLogTerm: 1, Configuration: make([]byte, 17), DataSize: 1,
	}, bytes.NewReader([]byte{1})); !errors.Is(err, protocol.ErrLimit) {
		t.Fatalf("snapshot configuration > C: err = %v", err)
	}
	peer.noConnection(t, 100*time.Millisecond)
}

// TestTCPMalformedRequestNotDispatched — V02/V13/V14 на реальном TCP:
// неизвестный RPCType (5, 255), лишний байт тела вне DataLength (ErrFormat),
// лишний байт внутри DataLength после gob-сообщения (ErrData) и повреждённый
// magic приводят к закрытию соединения без dispatch и без ответа; следующий
// корректный запрос на новом соединении обслуживается.
func TestTCPMalformedRequestNotDispatched(t *testing.T) {
	defer leaktest.CheckTimeout(t, raft.LeaktestBudget)()
	server, err := NewTCPTransport("127.0.0.1:0", uniformTCPTimeouts(time.Second), 2)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	delivered, stop := fiveRPCHandler(t, server, func(rpc contract.RPC) contract.RPCResponse {
		return contract.RPCResponse{Reply: &contract.TimeoutNowResponse{RPCHeader: testRPCHeader}}
	})
	defer stop()

	gobHi, err := hex.DecodeString("147f030102ff80000101010444617461011000000011ff800106737472696e670c040002686900")
	if err != nil {
		t.Fatal(err)
	}
	valid := wireTimeoutNowRequest(3, 1)
	badType := func(typ byte) []byte { f := bytes.Clone(valid); f[wireOffsetType] = typ; return f }
	badMagic := bytes.Clone(valid)
	badMagic[0] = 'X'
	cases := map[string][]byte{
		"type 5":             badType(5),
		"type 255":           badType(255),
		"magic":              badMagic,
		"body outside data":  wireAppendEntriesRequest(gobHi, []byte{0}),
		"data after gob msg": wireAppendEntriesRequest(append(bytes.Clone(gobHi), 0), nil),
	}
	for name, frame := range cases {
		conn, err := net.Dial("tcp", string(server.LocalAddr()))
		if err != nil {
			t.Fatal(err)
		}
		// Следующий корректный кадр в том же потоке не обслуживается:
		// ресинхронизации нет.
		if _, err = conn.Write(append(bytes.Clone(frame), valid...)); err != nil {
			t.Fatal(err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		if n, err := conn.Read(make([]byte, 64)); err == nil || n != 0 {
			t.Fatalf("%s: got response (%d bytes, %v), want close", name, n, err)
		}
		_ = conn.Close()
		if delivered.Load() != 0 {
			t.Fatalf("%s: dispatched %d", name, delivered.Load())
		}
	}
	// Корректный запрос на новом соединении обслуживается ровно один раз.
	conn, err := net.Dial("tcp", string(server.LocalAddr()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err = conn.Write(valid); err != nil {
		t.Fatal(err)
	}
	resp, err := protocol.ReadResponse(conn, rpcTypeTimeoutNow, protocol.DefaultLimits())
	if err != nil || resp.Error != nil {
		t.Fatalf("valid request: %+v %v", resp, err)
	}
	if delivered.Load() != 1 {
		t.Fatalf("delivered %d, want 1", delivered.Load())
	}
}

// TestTCPIncompatibleVersionCode3 — V03: запрос известного типа
// с несовместимой ProtocolVersion получает код 3 до dispatch, затем
// соединение закрывается; errors.Is на стороне отправителя истинен для
// маркера contract и корневого алиаса.
func TestTCPIncompatibleVersionCode3(t *testing.T) {
	defer leaktest.CheckTimeout(t, raft.LeaktestBudget)()
	server, err := NewTCPTransport("127.0.0.1:0", uniformTCPTimeouts(time.Second), 2)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	delivered, stop := fiveRPCHandler(t, server, func(contract.RPC) contract.RPCResponse {
		return contract.RPCResponse{Error: errors.New("must not dispatch")}
	})
	defer stop()

	for _, pv := range []int64{0, 2, 4, -1} {
		conn, err := net.Dial("tcp", string(server.LocalAddr()))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = conn.Write(wireTimeoutNowRequest(pv, 1)); err != nil {
			t.Fatal(err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		resp, err := protocol.ReadResponse(conn, rpcTypeTimeoutNow, protocol.DefaultLimits())
		if err != nil || !errors.Is(resp.Error, contract.ErrUnsupportedProtocol) ||
			!errors.Is(resp.Error, raft.ErrUnsupportedProtocol) {
			t.Fatalf("PV %d: %+v %v", pv, resp, err)
		}
		if n, err := conn.Read(make([]byte, 1)); err == nil || n != 0 {
			t.Fatalf("PV %d: connection not closed", pv)
		}
		_ = conn.Close()
	}
	if delivered.Load() != 0 {
		t.Fatalf("dispatched %d", delivered.Load())
	}
}

// TestTCPAcceptedNoncanonicalDataGo1264 — AR00101: внутренне неканоничная, но
// принятая стандартным gob Data (увеличенный счётчик delimited-значения
// interface) проходит транспорт без дополнительной проверки каноничности:
// потребитель получает ровно одну команду с Data "hi". Ожидание привязано
// к go1.26.4.
func TestTCPAcceptedNoncanonicalDataGo1264(t *testing.T) {
	if v := runtime.Version(); v != "go1.26.4" {
		t.Fatalf("toolchain %s: требуется переаттестация gob-корпуса ARCHITECT", v)
	}
	defer leaktest.CheckTimeout(t, raft.LeaktestBudget)()
	server, err := NewTCPTransport("127.0.0.1:0", uniformTCPTimeouts(time.Second), 2)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	var got atomic.Value
	delivered, stop := fiveRPCHandler(t, server, func(rpc contract.RPC) contract.RPCResponse {
		got.Store(rpc.Command)
		return contract.RPCResponse{Reply: &contract.AppendEntriesReply{RPCHeader: testRPCHeader, Term: 1}}
	})
	defer stop()

	noncanonical, err := hex.DecodeString("147f030102ff80000101010444617461011000000011ff800106737472696e670c050002686900")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial("tcp", string(server.LocalAddr()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err = conn.Write(wireAppendEntriesRequest(noncanonical, nil)); err != nil {
		t.Fatal(err)
	}
	resp, err := protocol.ReadResponse(conn, rpcTypeAppendEntries, protocol.DefaultLimits())
	if err != nil || resp.Error != nil {
		t.Fatalf("response: %+v %v", resp, err)
	}
	args, _ := got.Load().(*contract.AppendEntriesArgs)
	if delivered.Load() != 1 || args == nil || len(args.Entries) != 1 || args.Entries[0].Data != "hi" {
		t.Fatalf("delivered %d, command %#v", delivered.Load(), got.Load())
	}
}

// TestTCPDeadlinesScaleWithBody — V19: окно получателя обычного RPC —
// BaseRecv × max(1, ⌈BodyLength/256 КиБ⌉). Тело 262144 Б (factor 1) и
// 262145 Б (factor 2) при F, допускающем оба кадра; задержка обработчика
// между base и scaled проходит только для большого тела. Окно отправителя
// масштабируется так же.
func TestTCPDeadlinesScaleWithBody(t *testing.T) {
	defer leaktest.CheckTimeout(t, 2*time.Second)()
	const base = 300 * time.Millisecond
	limits := contract.Limits{MaxFrameBytes: 600000, MaxEntries: 2, MaxDataBytes: 262200, MaxConfigurationBytes: 16}
	server, err := NewTCPTransportWithLimits("127.0.0.1:0", uniformTCPTimeouts(base), 2, limits)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client, err := NewTCPTransportWithLimits("127.0.0.1:0", uniformTCPTimeouts(base), 2, limits)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.Connect(1, string(server.LocalAddr()))

	const delay = 400 * time.Millisecond // base < delay < 2·base
	_, stop := fiveRPCHandler(t, server, func(rpc contract.RPC) contract.RPCResponse {
		time.Sleep(delay)
		return contract.RPCResponse{Reply: &contract.AppendEntriesReply{RPCHeader: testRPCHeader, Term: 1}}
	})
	defer stop()

	// Длина Data, при которой тело кадра равно заданному: 64 Б
	// фиксированной части + 32 Б записи + Data (gob string) — подбирается
	// измерением.
	bodyWith := func(n int) contract.AppendEntriesArgs {
		return contract.AppendEntriesArgs{RPCHeader: testRPCHeader, Term: 1,
			Entries: []contract.LogEntry{{Index: 1, Term: 1, Data: strings.Repeat("a", n)}}}
	}
	sizeOf := func(n int) int {
		args := bodyWith(n)
		frame, err := protocol.AppendRequest(nil, &args, limits)
		if err != nil {
			t.Fatal(err)
		}
		return len(frame) - wireHeaderBytes
	}
	n := 262144 - 200
	for sizeOf(n) < 262144 {
		n++
	}
	if sizeOf(n) != 262144 {
		t.Fatalf("cannot build exact body: %d", sizeOf(n))
	}
	if _, err := client.AppendEntries(1, bodyWith(n)); err == nil {
		t.Fatal("body 262144 B (factor 1) with handler delay > base: accepted")
	}
	// Обработчик отвергнутого обмена освобождает потребителя.
	time.Sleep(delay)
	if _, err := client.AppendEntries(1, bodyWith(n+1)); err != nil {
		t.Fatalf("body 262145 B (factor 2) with handler delay < 2·base: %v", err)
	}
}

// TestTCPBodyWindowNotExtended — V19: одно абсолютное окно D_body на тело и
// обработчик: медленное тело порциями и задержка обработчика, каждая короче
// окна, в сумме превышают его — отказ без ответа. Непрерывные малые порции
// тела не продлевают окно; частичный заголовок закрывается по сроку.
func TestTCPBodyWindowNotExtended(t *testing.T) {
	defer leaktest.CheckTimeout(t, 2*time.Second)()
	const base = 300 * time.Millisecond
	server, err := NewTCPTransport("127.0.0.1:0", uniformTCPTimeouts(base), 2)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	delivered, stop := fiveRPCHandler(t, server, func(rpc contract.RPC) contract.RPCResponse {
		time.Sleep(200 * time.Millisecond)
		return contract.RPCResponse{Reply: &contract.TimeoutNowResponse{RPCHeader: testRPCHeader}}
	})
	defer stop()

	frame := wireTimeoutNowRequest(3, 1)
	t.Run("body plus handler exceed D_body", func(t *testing.T) {
		conn, err := net.Dial("tcp", string(server.LocalAddr()))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close() }()
		start := time.Now()
		if _, err = conn.Write(frame[:wireHeaderBytes]); err != nil {
			t.Fatal(err)
		}
		// Тело по байту каждые 10 мс: ≈160 мс < base, затем обработчик
		// 200 мс < base; сумма > base.
		for _, b := range frame[wireHeaderBytes:] {
			time.Sleep(10 * time.Millisecond)
			if _, err = conn.Write([]byte{b}); err != nil {
				t.Fatal(err)
			}
		}
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		resp, err := protocol.ReadResponse(conn, rpcTypeTimeoutNow, protocol.DefaultLimits())
		if err == nil && resp.Error == nil {
			t.Fatalf("response after %v beyond D_body: %+v", time.Since(start), resp)
		}
		if elapsed := time.Since(start); elapsed > base+250*time.Millisecond {
			t.Fatalf("closed after %v, window not bounded by D_body", elapsed)
		}
	})
	t.Run("partial header", func(t *testing.T) {
		conn, err := net.Dial("tcp", string(server.LocalAddr()))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close() }()
		start := time.Now()
		// Частичный заголовок порциями: поступление байтов срок не продлевает.
		for i := range 20 {
			if _, err = conn.Write(frame[i : i+1]); err != nil {
				break
			}
			time.Sleep(25 * time.Millisecond)
		}
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		if n, err := conn.Read(make([]byte, 1)); err == nil || n != 0 {
			t.Fatal("partial header: connection not closed")
		}
		if elapsed := time.Since(start); elapsed > 2*base+250*time.Millisecond {
			t.Fatalf("partial header held %v", elapsed)
		}
	})
	before := delivered.Load()
	if before > 1 {
		t.Fatalf("delivered %d", before)
	}
}

// TestTCPIdleBetweenFramesUnbounded — между кадрами нового срока нет:
// соединение, простоявшее дольше BaseRecv после ответа, обслуживает
// следующий запрос.
func TestTCPIdleBetweenFramesUnbounded(t *testing.T) {
	defer leaktest.CheckTimeout(t, raft.LeaktestBudget)()
	const base = 100 * time.Millisecond
	client, server, cleanup := newTCPPair(t, uniformTCPTimeouts(base))
	defer cleanup()
	delivered, stop := fiveRPCHandler(t, server, func(rpc contract.RPC) contract.RPCResponse {
		return contract.RPCResponse{Reply: &contract.TimeoutNowResponse{RPCHeader: testRPCHeader}}
	})
	defer stop()
	for range 2 {
		if _, err := client.TimeoutNow(1, contract.TimeoutNowRequest{RPCHeader: testRPCHeader}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(3 * base)
	}
	if delivered.Load() != 2 || client.pooledCount(contract.ServerAddress(server.LocalAddr())) != 1 {
		t.Fatalf("delivered %d pooled %d", delivered.Load(), client.pooledCount(contract.ServerAddress(server.LocalAddr())))
	}
}

// TestTCPScaleTimeoutOverflow — переполнение base × factor — ошибка, не
// отрицательный или насыщенный срок; factor по нормативным формулам.
func TestTCPScaleTimeoutOverflow(t *testing.T) {
	if _, err := scaleTimeout("x", time.Hour, 1<<40); err == nil {
		t.Fatal("overflow accepted")
	}
	if _, err := scaleTimeout("x", 0, 1); err == nil {
		t.Fatal("zero base accepted")
	}
	if d, err := scaleTimeout("x", contract.InstallSnapshotTimeout, snapshotFactor(1<<30)); err != nil ||
		d != 4096*contract.InstallSnapshotTimeout {
		t.Fatalf("1 GiB snapshot window %v %v", d, err)
	}
	for body, want := range map[uint64]uint64{0: 1, 1: 1, 262144: 1, 262145: 2, 524288: 2, 524289: 3} {
		if got := bodyFactor(body); got != want {
			t.Fatalf("bodyFactor(%d) = %d, want %d", body, got, want)
		}
	}
	for size, want := range map[int64]uint64{1: 1, 262143: 1, 262144: 1, 524287: 1, 524288: 2} {
		if got := snapshotFactor(size); got != want {
			t.Fatalf("snapshotFactor(%d) = %d, want %d", size, got, want)
		}
	}
}

// TestNormalizeTCPTimeouts — V27: сырые сроки <= 0 получают прежние
// умолчания каждого из четырёх полей, положительные сохраняются, функция
// идемпотентна, профиль TCP — InProcess=false; accessor транспорта
// возвращает тот же профиль, что и функция.
func TestNormalizeTCPTimeouts(t *testing.T) {
	cases := []TCPTimeouts{
		{},
		{ConnectionTimeout: -1, GenericRPCTimeout: -time.Second, InstallSnapshotTimeout: -1, ResponseTimeout: -5},
		{ConnectionTimeout: 10 * time.Millisecond, GenericRPCTimeout: 0, InstallSnapshotTimeout: 390 * time.Millisecond,
			ResponseTimeout: -1},
		uniformTCPTimeouts(500 * time.Millisecond),
	}
	for _, in := range cases {
		out, timing := NormalizeTCPTimeouts(in)
		pick := func(raw, def time.Duration) time.Duration {
			if raw > 0 {
				return raw
			}
			return def
		}
		want := TCPTimeouts{
			ConnectionTimeout:      pick(in.ConnectionTimeout, contract.ConnectionTCPRPCTimeout),
			GenericRPCTimeout:      pick(in.GenericRPCTimeout, contract.TCPRPCTimeout),
			InstallSnapshotTimeout: pick(in.InstallSnapshotTimeout, contract.InstallSnapshotTimeout),
			ResponseTimeout:        pick(in.ResponseTimeout, contract.TCPRPCTimeout),
		}
		if out != want {
			t.Fatalf("NormalizeTCPTimeouts(%+v) = %+v, want %+v", in, out, want)
		}
		wantTiming := contract.TransportTiming{BaseSend: want.GenericRPCTimeout, BaseRecv: want.ResponseTimeout,
			Dial: want.ConnectionTimeout, InProcess: false}
		if timing != wantTiming {
			t.Fatalf("timing %+v, want %+v", timing, wantTiming)
		}
		again, againTiming := NormalizeTCPTimeouts(out)
		if again != out || againTiming != timing {
			t.Fatal("not idempotent")
		}
		trans, err := NewTCPTransport("127.0.0.1:0", in, 2)
		if err != nil {
			t.Fatal(err)
		}
		if trans.TransportTiming() != timing || trans.Limits() != protocol.DefaultLimits() {
			t.Fatalf("accessor %+v / %+v", trans.TransportTiming(), trans.Limits())
		}
		trans.Close()
	}
}

// TestTransportLimitsAccessors — WithLimits-конструкторы TCP и Inmem:
// явный профиль сохраняется, целиком нулевой — умолчание, частично нулевой
// и недопустимый отвергаются до открытия слушателя. Профиль сроков Inmem —
// 500/500 мс, Dial=0, InProcess=true.
func TestTransportLimitsAccessors(t *testing.T) {
	defer leaktest.CheckTimeout(t, raft.LeaktestBudget)()
	upper := contract.Limits{MaxFrameBytes: 262144, MaxEntries: 6, MaxDataBytes: 41984, MaxConfigurationBytes: 2560}
	tcp, err := NewTCPTransportWithLimits("127.0.0.1:0", TCPTimeouts{}, 2, upper)
	if err != nil || tcp.Limits() != upper {
		t.Fatalf("TCP upper: %v", err)
	}
	tcp.Close()
	tcp, err = NewTCPTransportWithLimits("127.0.0.1:0", TCPTimeouts{}, 2, contract.Limits{})
	if err != nil || tcp.Limits() != protocol.DefaultLimits() {
		t.Fatalf("TCP zero: %v", err)
	}
	tcp.Close()
	if _, err = NewTCPTransportWithLimits("127.0.0.1:0", TCPTimeouts{}, 2,
		contract.Limits{MaxFrameBytes: 262144}); !errors.Is(err, protocol.ErrLimit) {
		t.Fatalf("TCP partial zero: %v", err)
	}
	inmem, err := NewInmemTransportWithLimits("a", upper)
	if err != nil || inmem.Limits() != upper {
		t.Fatalf("Inmem upper: %v", err)
	}
	if got := inmem.TransportTiming(); got != (contract.TransportTiming{BaseSend: InmemTransportTimeout,
		BaseRecv: InmemTransportTimeout, Dial: 0, InProcess: true}) {
		t.Fatalf("Inmem timing %+v", got)
	}
	if NewInmemTransport("b").Limits() != protocol.DefaultLimits() {
		t.Fatal("Inmem default limits")
	}
	if _, err = NewInmemTransportWithLimits("c", contract.Limits{MaxEntries: 1}); err == nil {
		t.Fatal("Inmem partial zero accepted")
	}
}

// TestInmemLimitsCheckedBeforeDispatch — Inmem защитно проверяет тот же
// профиль без сетевого кадра: число записей, сетевая длина Data
// (protocol.MeasureData, некодируемая Data — ошибка) и длина конфигурации
// снимка. Отказ — до dispatch.
func TestInmemLimitsCheckedBeforeDispatch(t *testing.T) {
	defer leaktest.CheckTimeout(t, raft.LeaktestBudget)()
	limits := contract.Limits{MaxFrameBytes: 4128, MaxEntries: 2, MaxDataBytes: 64, MaxConfigurationBytes: 16}
	a, err := NewInmemTransportWithLimits("a", limits)
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewInmemTransportWithLimits("b", limits)
	if err != nil {
		t.Fatal(err)
	}
	a.Connect(1, b)
	defer a.Close()
	defer b.Close()
	var delivered atomic.Int64
	defer startInmemCounting(b, &delivered)()

	for name, args := range map[string]contract.AppendEntriesArgs{
		"count > N": {RPCHeader: testRPCHeader, Entries: make([]contract.LogEntry, 3)},
		"data > D":  {RPCHeader: testRPCHeader, Entries: []contract.LogEntry{{Data: strings.Repeat("x", 80)}}},
		"gob error": {RPCHeader: testRPCHeader, Entries: []contract.LogEntry{{Data: make(chan int)}}},
	} {
		if _, err := a.AppendEntries(1, args); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
	if _, err := a.InstallSnapshot(1, contract.InstallSnapshotRequest{RPCHeader: testRPCHeader,
		Configuration: make([]byte, 17), DataSize: 1}, bytes.NewReader([]byte{1})); !errors.Is(err, protocol.ErrLimit) {
		t.Fatalf("configuration > C: %v", err)
	}
	if _, err := a.AppendEntries(1, contract.AppendEntriesArgs{RPCHeader: testRPCHeader,
		Entries: []contract.LogEntry{{Data: "ok"}, {}}}); err != nil {
		t.Fatalf("valid: %v", err)
	}
	if delivered.Load() != 1 {
		t.Fatalf("delivered %d, want 1", delivered.Load())
	}
}

func startInmemCounting(trans *InmemTransport, delivered *atomic.Int64) func() {
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			select {
			case rpc := <-trans.Consumer():
				delivered.Add(1)
				rpc.RespChan <- contract.RPCResponse{Reply: &contract.AppendEntriesReply{RPCHeader: testRPCHeader}}
			case <-done:
				return
			}
		}
	}()
	return func() { close(done); <-stopped }
}

// snapshotServer — транспорт-получатель и обработчик снимка по функции.
func snapshotPair(t *testing.T, handle func(rpc contract.RPC) contract.RPCResponse) (*TCPTransport, *TCPTransport, func()) {
	t.Helper()
	client, server, cleanup := newTCPPair(t, uniformTCPTimeouts(time.Second))
	_, stop := fiveRPCHandlerRaw(server, handle)
	return client, server, func() { stop(); cleanup() }
}

// fiveRPCHandlerRaw — обработчик без автоматического чтения тела снимка.
func fiveRPCHandlerRaw(trans *TCPTransport, handle func(rpc contract.RPC) contract.RPCResponse) (*atomic.Int64, func()) {
	var delivered atomic.Int64
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			select {
			case rpc := <-trans.Consumer():
				delivered.Add(1)
				rpc.RespChan <- handle(rpc)
			case <-done:
				return
			}
		}
	}()
	return &delivered, func() { close(done); <-stopped }
}

func snapshotArgs(size int64) contract.InstallSnapshotRequest {
	return contract.InstallSnapshotRequest{RPCHeader: testRPCHeader, Term: 1, LeaderID: 0, LastLogIndex: 5,
		LastLogTerm: 1, ConfigIndex: -1, DataSize: size}
}

// TestTCPSnapshotStreamBoundaries — V16/V17/V18: локальный отказ DataSize
// вне [1, 1 ГиБ] и nil Reader без соединения; источник N−1 — ошибка;
// источник N+1 — успех, лишний байт остаётся у владельца Reader; тело по
// байту через общий reader сверяется целиком; соединение снимка не
// возвращается в пул ни при успехе, ни при ошибке.
func TestTCPSnapshotStreamBoundaries(t *testing.T) {
	defer leaktest.CheckTimeout(t, raft.LeaktestBudget)()
	var received atomic.Value
	client, server, cleanup := snapshotPair(t, func(rpc contract.RPC) contract.RPCResponse {
		var buf bytes.Buffer
		one := make([]byte, 1)
		for {
			n, err := rpc.Reader.Read(one)
			buf.Write(one[:n])
			if err != nil {
				break
			}
		}
		received.Store(buf.Bytes())
		req := rpc.Command.(*contract.InstallSnapshotRequest)
		return contract.RPCResponse{Reply: &contract.InstallSnapshotResponse{RPCHeader: testRPCHeader, Term: req.Term,
			Success: int64(buf.Len()) == req.DataSize}}
	})
	defer cleanup()
	addr := contract.ServerAddress(server.LocalAddr())

	for _, size := range []int64{0, -1, 1<<30 + 1, 1<<63 - 1} {
		if _, err := client.InstallSnapshot(1, snapshotArgs(size), bytes.NewReader(nil)); !errors.Is(err, protocol.ErrRange) {
			t.Fatalf("DataSize %d: err = %v, want ErrRange", size, err)
		}
	}
	if _, err := client.InstallSnapshot(1, snapshotArgs(1), nil); err == nil {
		t.Fatal("nil reader accepted")
	}

	payload := make([]byte, 3000)
	for i := range payload {
		payload[i] = byte(i * 7)
	}
	// Источник короче DataSize — ошибка, не ожидание успешного ответа.
	if _, err := client.InstallSnapshot(1, snapshotArgs(3000), bytes.NewReader(payload[:2999])); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("short source: %v", err)
	}
	// Источник длиннее: читается ровно DataSize, остаток — у владельца.
	src := bytes.NewReader(append(bytes.Clone(payload), 0xEE))
	reply, err := client.InstallSnapshot(1, snapshotArgs(3000), src)
	if err != nil || !reply.Success {
		t.Fatalf("exact: %+v %v", reply, err)
	}
	if src.Len() != 1 {
		t.Fatalf("source remainder = %d, want 1", src.Len())
	}
	if got, _ := received.Load().([]byte); !bytes.Equal(got, payload) {
		t.Fatal("received body differs")
	}
	if n := client.pooledCount(addr); n != 0 {
		t.Fatalf("snapshot connection pooled: %d", n)
	}
}

// TestTCPSnapshotControlAndBodyOneWrite — V17: управляющий кадр и начало
// тела одной TCP-записью (предварительное чтение bufio) и посторонний кадр
// после тела: тело доходит без потерь, посторонний кадр не dispatch,
// соединение закрывается после ответа.
func TestTCPSnapshotControlAndBodyOneWrite(t *testing.T) {
	defer leaktest.CheckTimeout(t, raft.LeaktestBudget)()
	server, err := NewTCPTransport("127.0.0.1:0", uniformTCPTimeouts(time.Second), 2)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	var body atomic.Value
	delivered, stop := fiveRPCHandlerRaw(server, func(rpc contract.RPC) contract.RPCResponse {
		b, _ := io.ReadAll(rpc.Reader)
		body.Store(b)
		return contract.RPCResponse{Reply: &contract.InstallSnapshotResponse{RPCHeader: testRPCHeader, Term: 1, Success: true}}
	})
	defer stop()

	args := snapshotArgs(5)
	control, err := protocol.AppendRequest(nil, &args, protocol.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	stream := append(append(bytes.Clone(control), 1, 2, 3, 4, 5), wireTimeoutNowRequest(3, 1)...)
	conn, err := net.Dial("tcp", string(server.LocalAddr()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err = conn.Write(stream); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	r := bufio.NewReader(conn)
	resp, err := protocol.ReadResponse(r, rpcTypeInstallSnapshot, protocol.DefaultLimits())
	if err != nil || resp.Error != nil {
		t.Fatalf("snapshot response: %+v %v", resp, err)
	}
	if n, err := r.Read(make([]byte, 1)); err == nil || n != 0 {
		t.Fatal("connection kept open after snapshot")
	}
	if got, _ := body.Load().([]byte); !bytes.Equal(got, []byte{1, 2, 3, 4, 5}) {
		t.Fatalf("body %v", got)
	}
	if delivered.Load() != 1 {
		t.Fatalf("delivered %d: stray frame after snapshot dispatched", delivered.Load())
	}
}

// TestTCPSnapshotUnreadBodyIsError — V18: обработчик ответил успехом, не
// дочитав тело: транспорт не читает остаток конкурентно и не выдаёт успех —
// отправитель получает ошибку.
func TestTCPSnapshotUnreadBodyIsError(t *testing.T) {
	defer leaktest.CheckTimeout(t, raft.LeaktestBudget)()
	client, _, cleanup := snapshotPair(t, func(rpc contract.RPC) contract.RPCResponse {
		_, _ = rpc.Reader.Read(make([]byte, 2))
		return contract.RPCResponse{Reply: &contract.InstallSnapshotResponse{RPCHeader: testRPCHeader, Term: 1, Success: true}}
	})
	defer cleanup()
	reply, err := client.InstallSnapshot(1, snapshotArgs(4096), bytes.NewReader(make([]byte, 4096)))
	if err == nil || !raft.IsNilInterface(reply) && reply.Success {
		t.Fatalf("unread body: %+v %v", reply, err)
	}
}

// TestTCPParallelExclusiveOwnership — V20: параллельные обмены при пуле 2
// и 16: каждый ответ принадлежит своему запросу (терм запроса возвращается
// в ответе), idle-соединений в пуле не больше maxPool.
func TestTCPParallelExclusiveOwnership(t *testing.T) {
	for _, maxPool := range []int{2, 16} {
		t.Run(fmt.Sprintf("pool %d", maxPool), func(t *testing.T) {
			defer leaktest.CheckTimeout(t, raft.LeaktestBudget)()
			server, err := NewTCPTransport("127.0.0.1:0", uniformTCPTimeouts(time.Second), maxPool)
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			client, err := NewTCPTransport("127.0.0.1:0", uniformTCPTimeouts(time.Second), maxPool)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			client.Connect(1, string(server.LocalAddr()))
			_, stop := fiveRPCHandlerConcurrent(server)
			defer stop()

			var wg sync.WaitGroup
			for i := range 32 {
				wg.Add(1)
				go func(term int) {
					defer wg.Done()
					reply, err := client.AppendEntries(1, contract.AppendEntriesArgs{RPCHeader: testRPCHeader, Term: term})
					if err != nil || reply.Term != term {
						t.Errorf("term %d: reply %+v err %v", term, reply, err)
					}
				}(i + 1)
			}
			wg.Wait()
			if n := client.pooledCount(contract.ServerAddress(server.LocalAddr())); n > maxPool {
				t.Fatalf("pooled %d > maxPool %d", n, maxPool)
			}
		})
	}
}

// fiveRPCHandlerConcurrent отвечает на каждый AppendEntries в отдельной
// горутине с небольшой задержкой, возвращая терм запроса.
func fiveRPCHandlerConcurrent(trans *TCPTransport) (*atomic.Int64, func()) {
	var delivered atomic.Int64
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case rpc := <-trans.Consumer():
				delivered.Add(1)
				wg.Add(1)
				go func() {
					defer wg.Done()
					time.Sleep(2 * time.Millisecond)
					args := rpc.Command.(*contract.AppendEntriesArgs)
					rpc.RespChan <- contract.RPCResponse{Reply: &contract.AppendEntriesReply{RPCHeader: testRPCHeader, Term: args.Term}}
				}()
			case <-done:
				return
			}
		}
	}()
	return &delivered, func() { close(done); wg.Wait() }
}

// TestTCPNextRPCAfterReceiverWindow — V20: после истечения окна получателя
// (соединение закрыто получателем) следующий RPC не использует мёртвое
// соединение из пула и проходит; поздний ответ обработчика не достаётся
// следующему обмену.
func TestTCPNextRPCAfterReceiverWindow(t *testing.T) {
	defer leaktest.CheckTimeout(t, raft.LeaktestBudget)()
	server, err := NewTCPTransport("127.0.0.1:0", TCPTimeouts{ConnectionTimeout: time.Second,
		GenericRPCTimeout: time.Second, InstallSnapshotTimeout: time.Second, ResponseTimeout: 50 * time.Millisecond}, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client, err := NewTCPTransport("127.0.0.1:0", uniformTCPTimeouts(time.Second), 2)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.Connect(1, string(server.LocalAddr()))

	var calls atomic.Int64
	_, stop := fiveRPCHandlerRaw(server, func(rpc contract.RPC) contract.RPCResponse {
		args := rpc.Command.(*contract.AppendEntriesArgs)
		if calls.Add(1) == 1 {
			time.Sleep(120 * time.Millisecond) // поздний ответ после окна 50 мс
		}
		return contract.RPCResponse{Reply: &contract.AppendEntriesReply{RPCHeader: testRPCHeader, Term: args.Term}}
	})
	defer stop()

	if _, err = client.AppendEntries(1, contract.AppendEntriesArgs{RPCHeader: testRPCHeader, Term: 1}); err == nil {
		t.Fatal("first exchange succeeded beyond receiver window")
	}
	time.Sleep(150 * time.Millisecond)
	reply, err := client.AppendEntries(1, contract.AppendEntriesArgs{RPCHeader: testRPCHeader, Term: 2})
	if err != nil || reply.Term != 2 {
		t.Fatalf("next exchange: %+v %v", reply, err)
	}
}

// TestTCPCloseDuringExchange — V19/V20: Close получателя во время приёма и
// ожидания ответа обработчика закрывает входящие соединения: отправитель
// получает ошибку, горутины транспорта завершаются (leaktest), обработчик
// может ответить поздно без блокировки.
func TestTCPCloseDuringExchange(t *testing.T) {
	defer leaktest.CheckTimeout(t, raft.LeaktestBudget)()
	server, err := NewTCPTransport("127.0.0.1:0", uniformTCPTimeouts(5*time.Second), 2)
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewTCPTransport("127.0.0.1:0", uniformTCPTimeouts(5*time.Second), 2)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.Connect(1, string(server.LocalAddr()))

	got := make(chan contract.RPC, 1)
	go func() { got <- <-server.Consumer() }()
	done := make(chan error, 1)
	go func() {
		_, err := client.AppendEntries(1, contract.AppendEntriesArgs{RPCHeader: testRPCHeader, Term: 1})
		done <- err
	}()
	rpc := <-got
	start := time.Now()
	server.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("exchange succeeded after receiver Close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sender blocked after receiver Close")
	}
	if time.Since(start) > time.Second {
		t.Fatal("receiver Close did not interrupt the exchange promptly")
	}
	rpc.RespChan <- contract.RPCResponse{Reply: &contract.AppendEntriesReply{RPCHeader: testRPCHeader}}
}

// legacyRequest — gob-конверт запроса прежнего формата: байт типа и
// аргументы в интерфейсном поле.
type legacyRequest struct {
	Type byte
	Args any
}

// TestTCPLegacyGobEnvelopeNoFalseSuccess — V23 (сетевая часть): прежний
// gob-конверт и новый формат не общаются и не дают ложного успеха. Запрос
// прежнего формата новый получатель отвергает по magic без dispatch и без
// ответа; ответ прежнего формата новый отправитель отвергает как ошибку
// формата.
func TestTCPLegacyGobEnvelopeNoFalseSuccess(t *testing.T) {
	defer leaktest.CheckTimeout(t, raft.LeaktestBudget)()
	gob.RegisterName("github.com/vskurikhin/raft.AppendEntriesArgs", contract.AppendEntriesArgs{})
	gob.RegisterName("github.com/vskurikhin/raft.AppendEntriesReply", contract.AppendEntriesReply{})

	server, err := NewTCPTransport("127.0.0.1:0", uniformTCPTimeouts(time.Second), 2)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	delivered, stop := fiveRPCHandler(t, server, func(contract.RPC) contract.RPCResponse {
		return contract.RPCResponse{Reply: &contract.AppendEntriesReply{RPCHeader: testRPCHeader, Success: true}}
	})
	defer stop()

	conn, err := net.Dial("tcp", string(server.LocalAddr()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	legacy := legacyRequest{Type: 0, Args: contract.AppendEntriesArgs{RPCHeader: testRPCHeader, Term: 1}}
	var legacyFrame bytes.Buffer
	if err = gob.NewEncoder(&legacyFrame).Encode(legacy); err != nil {
		t.Fatal(err)
	}
	// Получатель закрывает соединение по первым байтам (magic), поэтому
	// ошибка записи хвоста (EPIPE/RST) — ожидаемый исход, а не отказ теста.
	_, _ = conn.Write(legacyFrame.Bytes())
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if n, err := conn.Read(make([]byte, 64)); err == nil || n != 0 {
		t.Fatalf("legacy request answered: %d %v", n, err)
	}
	if delivered.Load() != 0 {
		t.Fatalf("legacy request dispatched")
	}

	// Сосед прежнего формата отвечает gob-потоком: строка ошибки и ответ.
	peer := newRawPeer(t)
	defer peer.close()
	client, err := NewTCPTransport("127.0.0.1:0", uniformTCPTimeouts(time.Second), 2)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.Connect(1, peer.ln.Addr().String())
	done := make(chan error, 1)
	go func() {
		_, err := client.AppendEntries(1, contract.AppendEntriesArgs{RPCHeader: testRPCHeader, Term: 1})
		done <- err
	}()
	legacyConn := peer.accept(t)
	_, _ = legacyConn.Read(make([]byte, 1024))
	enc := gob.NewEncoder(legacyConn)
	_ = enc.Encode("")
	var reply any = &contract.AppendEntriesReply{Success: true, Term: 1}
	_ = enc.Encode(reply)
	if err := <-done; !errors.Is(err, protocol.ErrFormat) {
		t.Fatalf("legacy response: err = %v, want ErrFormat", err)
	}
}
