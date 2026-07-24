package main

import (
	"fmt"
	"log"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/vskurikhin/raft/internal/config"
	"github.com/vskurikhin/raft/pkg/kvservice"
	"github.com/vskurikhin/raft/pkg/raft"
)

const (
	DurationModulus = 30 * 1000
	MinimalDuration = 500
	Try             = 4096
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	return runWith(config.ParseFlags())
}

var wg sync.WaitGroup

func runWith(values config.Values) error {
	raft.DisableRPCProxy(true)
	raft.TraceCM(1)
	kvservice.TraceKV(0)
	nums := slices.Collect(maps.Keys(values.Peers))
	done := make(chan any)
	ready := make(chan any)

	cfg := kvservice.Config{
		HTTPAddress: values.HTTPAddress.String(),
		Config: raft.Config{
			PeerAddresses: values.Peers,
			PeerIds:       nums,
			RPCAddress:    values.RPCAddress.String(),
			ServerID:      values.Number,
		},
	}
	storage := raft.NewMapStorage()
	kvs := kvservice.New(cfg, storage, ready)
	// Для готовности достаточно N/2 + 1 серверов
	wg.Add(len(nums) / 2)
	for _, num := range nums {
		go connect(num, kvs, values, nums)
	}
	wg.Wait()
	close(ready)
	kvs.ServeHTTP(values.HTTPAddress.String())
	<-done
	return nil
}

var (
	count int
	mu    sync.Mutex
)

func connect(n int, kvs *kvservice.KVService, values config.Values, nums []int) {
	err := kvs.ConnectToRaftPeer(n, values.Peers[n])
	for i := 0; i < Try && err != nil; i++ {
		duration := (i * MinimalDuration) % DurationModulus
		time.Sleep(time.Duration(duration+MinimalDuration) * time.Millisecond)
		err = kvs.ConnectToRaftPeer(n, values.Peers[n])
	}
	if err != nil {
		slog.Warn(fmt.Sprintf("warning connect to peer %d: error: %v", n, err))
	} else {
		slog.Info(fmt.Sprintf("connected to peer %d", n))
		if count < len(nums)/2 {
			mu.Lock()
			count++
			wg.Done()
			mu.Unlock()
		}
	}
}
