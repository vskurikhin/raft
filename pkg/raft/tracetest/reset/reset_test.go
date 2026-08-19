// Package reset_test — test-бинарник контракта трассировки pkg/raft:
// сценарий вывода по умолчанию (stderr-путь). Один бинарник на один
// сценарий конфигурации (ADR-004, NEW-14). Бинарник конфигурируется
// значением "" однократно с самого начала — единственная успешная
// конфигурация процесса; сценарий reset-through-re-set
// (SetTraceLogFile(tmp) → SetTraceLogFile("")) запрещён строгим
// set-once и в тестах отсутствует.
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
	"github.com/vskurikhin/raft/pkg/raft"
)

// buf — синхронизированный приёмник стандартного логгера: запись
// ведётся из горутин CM через log.Default(), чтение — из теста.
var buf syncBuffer

// TestMain выполняет log.SetOutput ДО конфигурации (безопасно в
// выделенном бинарнике) и единственную успешную конфигурацию
// трассировки процесса: SetTraceLogFile("").
func TestMain(m *testing.M) {
	log.SetOutput(&buf)
	if err := raft.SetTraceLogFile(""); err != nil {
		os.Stderr.WriteString("raft: trace configuration failed: " + err.Error() + "\n")
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func TestTraceReset(t *testing.T) {
	// Порядок cleanup (LIFO): остановка CM → проверка leaktest.
	t.Cleanup(leaktest.CheckTimeout(t, raft.LeaktestBudget))

	// Строгий set-once: повторный вызов — ошибка контракта.
	if err := raft.SetTraceLogFile(""); err == nil {
		t.Fatal("SetTraceLogFile: want contract error, got nil")
	}

	// Создаём CM (InmemTransport + MapStorage, без кластера).
	commitCh := make(chan raft.CommitEntry)
	readerDone := make(chan any)
	go func() {
		defer close(readerDone)
		for range commitCh {
		}
	}()
	ready := make(chan any)
	cm := raft.NewConsensusModule(
		0, []int{}, raft.NewInmemTransport("trace-reset"),
		raft.NewMapStorage(), raft.NewCommitChannelFSM(commitCh), ready,
	)
	close(ready)
	t.Cleanup(func() {
		cm.Stop()
		close(commitCh)
		<-readerDone
	})

	// Эмиттер трассировки — cm.Stop(); запись идёт в стандартный
	// логгер (путь конфигурации ""), файл трассировки не создаётся.
	cm.Stop()

	deadline := time.Now().Add(2 * time.Second)
	for !strings.Contains(buf.String(), "CM.Stop called / becomes Dead") {
		if time.Now().After(deadline) {
			t.Fatalf("trace buffer does not contain the CM.Stop entry: %q", buf.String())
		}
		// poll-интервал condition-wait (не фиксированная пауза).
		time.Sleep(5 * time.Millisecond)
	}
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
