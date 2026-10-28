package kvservice

import (
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// encodePath — путь, на котором gob вызвал кодек команды.
type encodePath int

const (
	encodePathWire encodePath = iota
	encodePathStorage
	encodePathOther
	encodePathCount
)

// encodeRecorder считает вызовы кодека для команд с ключом key по путям.
// Только для тестов: в рабочей сборке счётчиков нет.
type encodeRecorder struct {
	key string

	mu     sync.Mutex
	counts [encodePathCount]int
}

// _activeEncodeRecorder — счётчик текущего теста; nil, если не включён.
var _activeEncodeRecorder atomic.Pointer[encodeRecorder]

// recordingEncodeCommand подставляется в _encodeCommandFunc в TestMain
// до запуска тестов и далее не меняется.
func recordingEncodeCommand(cmd Command) ([]byte, error) {
	if r := _activeEncodeRecorder.Load(); r != nil && cmd.Key == r.key {
		r.add(callerEncodePath())
	}
	return EncodeCommand(cmd)
}

// startEncodeRecorder включает счётчик для ключа key до конца теста.
func startEncodeRecorder(t *testing.T, key string) *encodeRecorder {
	t.Helper()
	r := &encodeRecorder{key: key}
	if !_activeEncodeRecorder.CompareAndSwap(nil, r) {
		t.Fatal("another encode recorder is active")
	}
	t.Cleanup(func() { _activeEncodeRecorder.Store(nil) })
	return r
}

func (r *encodeRecorder) add(path encodePath) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counts[path]++
}

// snapshot возвращает счётчики wire, storage и прочих путей.
func (r *encodeRecorder) snapshot() (wire, storage, other int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counts[encodePathWire], r.counts[encodePathStorage], r.counts[encodePathOther]
}

// callerEncodePath определяет путь по стеку вызова: транспорт или
// хранилище журнала; всё остальное — прочие вызовы.
func callerEncodePath() encodePath {
	pcs := make([]uintptr, 256)
	n := runtime.Callers(3, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	for {
		frame, more := frames.Next()
		switch {
		case strings.Contains(frame.Function, "/pkg/raft/transp."):
			return encodePathWire
		case strings.Contains(frame.Function, "/pkg/raft/store."):
			return encodePathStorage
		}
		if !more {
			return encodePathOther
		}
	}
}
