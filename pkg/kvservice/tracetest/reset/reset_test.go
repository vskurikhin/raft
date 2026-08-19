// Package reset_test — test-бинарник контракта трассировки
// pkg/kvservice: сценарий вывода по умолчанию (stderr-путь). Один
// бинарник на один сценарий конфигурации (ADR-004, NEW-14);
// симметрично pkg/raft/tracetest/reset. Бинарник конфигурируется
// значением "" однократно с самого начала — единственная успешная
// конфигурация процесса; reset-through-re-set запрещён строгим
// set-once. Эмиттер трассировки — kvs.ServeHTTP.
package reset_test

import (
	"bytes"
	"log"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/vskurikhin/raft/pkg/kvservice"
	"github.com/vskurikhin/raft/pkg/raft"
)

// buf — синхронизированный приёмник стандартного логгера: запись
// ведётся из HTTP-горутины через log.Default(), чтение — из теста.
var buf syncBuffer

// TestMain выполняет log.SetOutput ДО конфигурации (безопасно в
// выделенном бинарнике) и единственную успешную конфигурацию
// трассировки процесса: SetTraceLogFile("").
func TestMain(m *testing.M) {
	log.SetOutput(&buf)
	if err := kvservice.SetTraceLogFile(""); err != nil {
		_, _ = os.Stderr.WriteString("kvservice: trace configuration failed: " + err.Error() + "\n")
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func TestTraceReset(t *testing.T) {
	// Порядок cleanup (LIFO): остановка сервиса → проверка leaktest.
	t.Cleanup(leaktest.CheckTimeout(t, raft.LeaktestBudget))

	// Строгий set-once: повторный вызов — ошибка контракта.
	if err := kvservice.SetTraceLogFile(""); err == nil {
		t.Fatal("SetTraceLogFile: want contract error, got nil")
	}

	// Создаём сервис; эмиттер — kvs.ServeHTTP; запись идёт в
	// стандартный логгер (путь конфигурации ""), файл трассировки
	// не создаётся.
	kvs := newTestService(t)
	if err := kvs.ServeHTTP(":0"); err != nil {
		t.Fatalf("ServeHTTP: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for !strings.Contains(buf.String(), "serving HTTP on") {
		if time.Now().After(deadline) {
			t.Fatalf("trace buffer does not contain the ServeHTTP entry: %q", buf.String())
		}
		// poll-интервал condition-wait (не фиксированная пауза).
		time.Sleep(5 * time.Millisecond)
	}
}

// newTestService создаёт KVService с хранилищем во временном каталоге
// и останавливает его в t.Cleanup.
func newTestService(t *testing.T) *kvservice.KVService {
	t.Helper()
	ready := make(chan any)
	cfg := &kvservice.Config{
		HTTPAddress: ":0",
		Config: raft.Config{
			PeerIds:       []int{},
			RPCAddress:    ":0",
			ServerID:      7,
			Storage:       raft.NewMapStorage(),
			TCPRPCTimeout: raft.TCPRPCTimeout,
		},
	}
	kvs := kvservice.New(cfg, ready)
	close(ready)
	t.Cleanup(func() {
		if err := kvs.Shutdown(); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	})
	return kvs
}

// syncBuffer — потокобезопасный буфер для перехвата вывода
// стандартного логгера.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}
