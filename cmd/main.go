package main

import (
	"log"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
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

func runWith(values config.Values) error {
	nums := slices.Collect(maps.Keys(values.Peers))
	done := make(chan any)
	ready := make(chan any)

	storage := raft.NewMapStorage()
	kvs := kvservice.New(values.RPCAddress.String(), values.Number, nums, storage, ready)
	var wg sync.WaitGroup
	var count atomic.Int64
	wg.Add(len(nums) / 2)
	for _, num := range nums {
		go func(n int) {
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
			} else if c := count.Load(); c < int64(len(nums)/2) {
				count.CompareAndSwap(c, c+1)
				wg.Done()
			}
		}(num)
	}
	wg.Wait()
	close(ready)
	kvs.ServeHTTP(values.HTTPAddress.String())
	<-done
	return nil
}
