package main

import (
	"log"
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
	nums := slices.Collect(maps.Keys(values.Peers))
	done := make(chan any)
	ready := make(chan any)

	cfg := kvservice.Config{
		HTTPAddress: values.HTTPAddress.String(),
		Config: raft.Config{
			PeerAddresses:     values.Peers,
			PeerIds:           nums,
			RPCAddress:        values.RPCAddress.String(),
			ServerID:          values.Number,
			SnapshotThreshold: 96,
			SnapshotInterval:  64,
		},
	}
	storage := raft.NewMapStorage()
	kvs := kvservice.New(cfg, storage, ready)
	wg.Add(len(nums))
	for _, num := range nums {
		go connect(num, kvs, values)
	}
	waitReady(ready, 30*time.Second)
	kvs.ServeHTTP(values.HTTPAddress.String())
	<-done
	return nil
}

func waitReady(ready chan any, timeout time.Duration) {
	c := make(chan any)
	go func() {
		wg.Wait()
		close(c)
	}()
	select {
	case <-c:
	case <-time.After(timeout):
		log.Printf("startup timeout reached, starting with partial connectivity")
	}
	close(ready)
}

func connect(n int, kvs *kvservice.KVService, values config.Values) {
	defer wg.Done()
	log.Printf("connect to peer %d", n)
	err := kvs.ConnectToRaftPeer(n, values.Peers[n])
	for i := 0; i < Try && err != nil; i++ {
		duration := (i * MinimalDuration) % DurationModulus
		time.Sleep(time.Duration(duration+MinimalDuration) * time.Millisecond)
		err = kvs.ConnectToRaftPeer(n, values.Peers[n])
		if err == nil {
			log.Printf("connected to peer %d", n)
		}
	}
	if err != nil {
		log.Printf("warning connect to peer %d: error: %v", n, err)
	}
}
