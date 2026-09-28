package transp

import (
	"bufio"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/vskurikhin/raft"
	"github.com/vskurikhin/raft/pkg/raft/contract"
	"github.com/vskurikhin/raft/pkg/raft/protocol"
)

// readyConsumer постоянно принимает RPC из consumerCh и считает их.
func readyConsumer(trans *TCPTransport) (*atomic.Int64, func()) {
	var got atomic.Int64
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			select {
			case rpc := <-trans.consumerCh:
				got.Add(1)
				rpc.RespChan <- contract.RPCResponse{Reply: &contract.TimeoutNowResponse{RPCHeader: testRPCHeader}}
			case <-done:
				return
			}
		}
	}()
	return &got, func() { close(done); <-stopped }
}

// TestTCPExpiredDeadlineNoDispatch — истёкшая граница означает отказ до
// постановки запроса: при готовом потребителе и сроке в прошлом ни один
// запрос не доставляется.
func TestTCPExpiredDeadlineNoDispatch(t *testing.T) {
	defer leaktest.CheckTimeout(t, raft.LeaktestBudget)()
	trans, err := NewTCPTransport("127.0.0.1:0", TCPTimeouts{}, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer trans.Close()
	got, stop := readyConsumer(trans)
	defer stop()
	time.Sleep(10 * time.Millisecond)
	for range 64 {
		rpc := contract.RPC{Command: &contract.TimeoutNowRequest{RPCHeader: testRPCHeader},
			RespChan: make(chan contract.RPCResponse, 1)}
		if err := trans.dispatch(rpc, time.Now().Add(-time.Second)); !errors.Is(err, contract.ErrEnqueueTimeout) {
			t.Fatalf("dispatch after expired deadline: %v", err)
		}
	}
	if got.Load() != 0 {
		t.Fatalf("dispatched %d requests after expired deadline", got.Load())
	}
	// Своевременный обмен сохраняется.
	rpc := contract.RPC{Command: &contract.TimeoutNowRequest{RPCHeader: testRPCHeader},
		RespChan: make(chan contract.RPCResponse, 1)}
	if err := trans.dispatch(rpc, time.Now().Add(time.Second)); err != nil {
		t.Fatalf("timely dispatch: %v", err)
	}
}

// TestTCPExpiredDeadlineNoLateResponse — готовый, но поздний ответ после
// истечения границы не принимается.
func TestTCPExpiredDeadlineNoLateResponse(t *testing.T) {
	defer leaktest.CheckTimeout(t, raft.LeaktestBudget)()
	trans, err := NewTCPTransport("127.0.0.1:0", TCPTimeouts{}, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer trans.Close()
	for range 64 {
		respCh := make(chan contract.RPCResponse, 1)
		respCh <- contract.RPCResponse{Reply: &contract.TimeoutNowResponse{RPCHeader: testRPCHeader}}
		if _, err := trans.awaitResponse(respCh, time.Now().Add(-time.Second)); !errors.Is(err, contract.ErrEnqueueTimeout) {
			t.Fatalf("late ready response accepted: %v", err)
		}
	}
	respCh := make(chan contract.RPCResponse, 1)
	respCh <- contract.RPCResponse{Reply: &contract.TimeoutNowResponse{RPCHeader: testRPCHeader}}
	if _, err := trans.awaitResponse(respCh, time.Now().Add(time.Second)); err != nil {
		t.Fatalf("timely response: %v", err)
	}
}

// TestTCPServeRegularAfterLateDecode — разбор запроса завершился после
// D_body (поздний decode): запрос не передаётся потребителю, ответ не
// пишется, соединение не продолжается.
func TestTCPServeRegularAfterLateDecode(t *testing.T) {
	defer leaktest.CheckTimeout(t, raft.LeaktestBudget)()
	trans, err := NewTCPTransport("127.0.0.1:0", TCPTimeouts{}, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer trans.Close()
	got, stop := readyConsumer(trans)
	defer stop()
	time.Sleep(10 * time.Millisecond)
	for range 32 {
		server, client := net.Pipe()
		deadline := time.Now().Add(-time.Millisecond)
		// Сокетные сроки уже выставлены хуком на D_body.
		_ = server.SetDeadline(deadline)
		written := make(chan int, 1)
		go func() {
			buf := make([]byte, 64)
			_ = client.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
			n, _ := client.Read(buf)
			written <- n
		}()
		keep, err := trans.serveRegular(server, bufio.NewWriter(server), rpcTypeTimeoutNow,
			&contract.TimeoutNowRequest{RPCHeader: testRPCHeader}, deadline)
		if keep || err == nil {
			t.Fatalf("serveRegular after expired D_body: keep=%v err=%v", keep, err)
		}
		_ = server.Close()
		if n := <-written; n != 0 {
			t.Fatalf("response of %d bytes written after expiry", n)
		}
		_ = client.Close()
	}
	if got.Load() != 0 {
		t.Fatalf("dispatched %d after expired D_body", got.Load())
	}
}

// TestInmemReceiverLimits — защитная проверка пределов получателя Inmem:
// меньшие N/D/C получателя отвергаются до Consumer с errors.Is ErrLimit;
// значения на границе получателя проходят.
func TestInmemReceiverLimits(t *testing.T) {
	defer leaktest.CheckTimeout(t, raft.LeaktestBudget)()
	senderLimits := protocol.DefaultLimits()
	receiverLimits := contract.Limits{MaxFrameBytes: 262144, MaxEntries: 3, MaxDataBytes: 3000, MaxConfigurationBytes: 16}
	sender, err := NewInmemTransportWithLimits("s", senderLimits)
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := NewInmemTransportWithLimits("r", receiverLimits)
	if err != nil {
		t.Fatal(err)
	}
	sender.Connect(1, receiver)
	defer sender.Close()
	defer receiver.Close()
	var delivered atomic.Int64
	defer startInmemCounting(receiver, &delivered)()

	// Наибольшая строка с сетевой длиной <= 3000.
	n := 2900
	for {
		size, err := protocol.MeasureData(strings.Repeat("x", n+1), 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		if size > 3000 {
			break
		}
		n++
	}
	neg := map[string]contract.AppendEntriesArgs{
		"D receiver":   {RPCHeader: testRPCHeader, Entries: []contract.LogEntry{{Data: strings.Repeat("x", 4000)}}},
		"D+1 receiver": {RPCHeader: testRPCHeader, Entries: []contract.LogEntry{{Data: strings.Repeat("x", n+1)}}},
		"N receiver":   {RPCHeader: testRPCHeader, Entries: make([]contract.LogEntry, 4)},
	}
	for name, args := range neg {
		if _, err := sender.AppendEntries(1, args); !errors.Is(err, protocol.ErrLimit) {
			t.Fatalf("%s: err = %v, want ErrLimit", name, err)
		}
	}
	if _, err := sender.InstallSnapshot(1, contract.InstallSnapshotRequest{RPCHeader: testRPCHeader,
		Configuration: make([]byte, 17), DataSize: 1}, strings.NewReader("x")); !errors.Is(err, protocol.ErrLimit) {
		t.Fatalf("C receiver: %v", err)
	}
	if delivered.Load() != 0 {
		t.Fatalf("receiver-exceeding requests delivered: %d", delivered.Load())
	}
	if _, err := sender.AppendEntries(1, contract.AppendEntriesArgs{RPCHeader: testRPCHeader,
		Entries: []contract.LogEntry{{Data: strings.Repeat("x", n)}, {}, {}}}); err != nil {
		t.Fatalf("at receiver bound: %v", err)
	}
	if delivered.Load() != 1 {
		t.Fatalf("delivered %d, want 1", delivered.Load())
	}
}
