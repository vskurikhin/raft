package main

import (
	"fmt"
	"log"
	"maps"
	"path/filepath"
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

// configureTraceLogging применяет параметры трассировки командной строки:
// --trace-log-level задаёт пороги raft.TraceCM и kvservice.TraceKV,
// --trace-log-file — файл, в который пишутся отладочные сообщения
// консенсус-модуля и KV-сервиса (пустая строка — stderr).
func configureTraceLogging(values config.Values) error {
	raft.TraceCM = values.TraceLogLevel
	kvservice.TraceKV = values.TraceLogLevel
	if values.TraceLogFile == "" {
		return nil
	}
	if err := raft.SetTraceLogFile(values.TraceLogFile); err != nil {
		return err
	}
	return kvservice.SetTraceLogFile(values.TraceLogFile)
}

func runWith(values config.Values) error {
	if err := configureTraceLogging(values); err != nil {
		return err
	}
	nums := slices.Collect(maps.Keys(values.Peers))
	done := make(chan any)
	ready := make(chan any)

	dataDir := values.DataDir
	if dataDir == "" {
		dataDir = filepath.Join("data", fmt.Sprintf("node-%d", values.Number))
	}

	// Durable-хранилище снапшотов: log compaction усекает durable-лог,
	// поэтому снапшот обязан переживать рестарт процесса. retain=2 —
	// запас на случай повреждения последнего снапшота.
	snapshotStore, err := raft.NewFileSnapshotStore(dataDir, 2)
	if err != nil {
		return fmt.Errorf("failed to create file snapshot store in %s: %w", dataDir, err)
	}

	cfg := kvservice.Config{
		HTTPAddress: values.HTTPAddress.String(),
		Config: raft.Config{
			PeerAddresses: values.Peers,
			PeerIds:       nums,
			RPCAddress:    values.RPCAddress.String(),
			ServerID:      values.Number,
			SnapshotStore: snapshotStore,
			Storage:       raft.NewFileStorage(dataDir),
			TcpRpcTimeout: raft.TcpRpcTimeoutMs,
			MaxPool:       4,
		},
	}
	kvs := kvservice.New(cfg, ready)
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
	log.Printf("connect to peer %d", n)
	err := kvs.ConnectToRaftPeer(n, values.Peers[n])
	for i := 0; i < Try && err != nil; i++ {
		duration := (i * MinimalDuration) % DurationModulus
		time.Sleep(time.Duration(duration+MinimalDuration) * time.Millisecond)
		log.Printf("try connect to peer %d", n)
		err = kvs.ConnectToRaftPeer(n, values.Peers[n])
	}
	if err != nil {
		log.Printf("warning connect to peer %d: error: %v", n, err)
	} else if count < len(nums)/2 {
		mu.Lock()
		count++
		wg.Done()
		mu.Unlock()
	}
}
