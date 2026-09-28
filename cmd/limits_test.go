package main

import (
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/vskurikhin/raft"
	"github.com/vskurikhin/raft/pkg/raft/transp"
)

// TestRunWithRejectsTimingProfileBeforeListener — V29 CLI-путь: значения,
// прошедшие все существующие проверки флагов (reelection 401, heartbeat 40,
// ticker 40, connect 1, rpc 399, snapshot 399), отвергаются ValidateConfig
// по временному профилю 40 + 40 + 399 = 479 >= 401 до создания хранилищ и
// открытия слушателя: адрес RPC остаётся свободным, каталог данных пуст.
func TestRunWithRejectsTimingProfileBeforeListener(t *testing.T) {
	t.Cleanup(leaktest.CheckTimeout(t, raft.LeaktestBudget))

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	rpcAddr := ln.Addr()
	_ = ln.Close()

	values := newTestValues(t)
	values.Peers = map[int]net.Addr{}
	values.RPCAddress = rpcAddr
	values.ReelectionTimeout = 401 * time.Millisecond
	values.HeartbeatTimeout = 40 * time.Millisecond
	values.TickerTimeout = 40 * time.Millisecond
	values.TCPConnectTimeout = time.Millisecond
	values.TCPRPCTimeout = 399 * time.Millisecond
	values.InstallSnapshotTimeout = 399 * time.Millisecond

	if err = raft.ValidateTiming(raft.TimerConfig{ApplyBatch: raft.DefaultApplyBatchInterval,
		Heartbeat: values.HeartbeatTimeout, Reelection: values.ReelectionTimeout, Ticker: values.TickerTimeout}); err != nil {
		t.Fatalf("example must pass the existing range checks: %v", err)
	}

	stop, err := runWith(&values)
	if err == nil {
		stop()
		t.Fatal("runWith accepted 479ms >= 401ms")
	}
	want := "invalid timing profile: heartbeat 40ms + ticker 40ms + max RPC window 399ms = 479ms >= reelection base 401ms"
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("error %q does not contain %q", err, want)
	}
	check, err := net.Listen("tcp", rpcAddr.String())
	if err != nil {
		t.Fatalf("RPC listener left open: %v", err)
	}
	_ = check.Close()
	entries, err := os.ReadDir(values.DataDir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("side effects in data dir: %v %v", entries, err)
	}
}

// TestRunWithRejectsKVProfile — V29/V30 CLI-путь: EncodedMaxKV(K,V) > D
// отвергается до старта с диагностикой; верхний профиль владельца
// (8192/16384 при D=41984, N_eff=6) запускается.
func TestRunWithRejectsKVProfile(t *testing.T) {
	t.Cleanup(leaktest.CheckTimeout(t, raft.LeaktestBudget))

	values := newTestValues(t)
	values.Peers = map[int]net.Addr{}
	values.MaxKeyBytes = 8192
	values.MaxValueBytes = 16384
	values.MaxDataBytes = 41775
	if stop, err := runWith(&values); err == nil {
		stop()
		t.Fatal("runWith accepted EncodedMaxKV > D")
	} else if !strings.Contains(err.Error(), "EncodedMaxKV(8192,16384)=41776 exceeds max-data-bytes=41775") {
		t.Fatalf("error: %v", err)
	}

	values = newTestValues(t)
	values.Peers = map[int]net.Addr{}
	values.MaxKeyBytes = 8192
	values.MaxValueBytes = 16384
	values.MaxDataBytes = 41984
	stop, err := runWith(&values)
	if err != nil {
		t.Fatalf("upper profile: %v", err)
	}
	t.Cleanup(stop)
	if limits, err := nodeLimits(&values); err != nil || limits.MaxEntries != 6 {
		t.Fatalf("N_eff: %+v %v", limits, err)
	}
}

// TestCLITimingProfileMatchesTransport — V27: профиль сроков CLI-точки
// (NormalizeTCPTimeouts над transportTimeouts(Values)) совпадает с профилем
// конструктора и accessor транспорта для нулевых, частичных и полных
// значений флагов.
func TestCLITimingProfileMatchesTransport(t *testing.T) {
	for _, v := range []struct{ connect, rpc, snapshot time.Duration }{
		{0, 0, 0}, {10 * time.Millisecond, 0, 390 * time.Millisecond}, {1, 399 * time.Millisecond, 399 * time.Millisecond},
	} {
		values := newTestValues(t)
		values.TCPConnectTimeout, values.TCPRPCTimeout, values.InstallSnapshotTimeout = v.connect, v.rpc, v.snapshot
		timeouts, timing := transp.NormalizeTCPTimeouts(transportTimeouts(&values))
		trans, err := transp.NewTCPTransport("127.0.0.1:0", transportTimeouts(&values), 0)
		if err != nil {
			t.Fatal(err)
		}
		if trans.TransportTiming() != timing || timing.InProcess || timing.Dial != timeouts.ConnectionTimeout {
			t.Fatalf("%+v: accessor %+v, CLI %+v", v, trans.TransportTiming(), timing)
		}
		trans.Close()
	}
}
