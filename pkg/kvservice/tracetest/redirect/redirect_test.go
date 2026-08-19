// Package redirect_test — test-бинарник контракта трассировки
// pkg/kvservice: сценарий перенаправления в файл. Один бинарник на один
// сценарий конфигурации (ADR-004, NEW-14); симметрично
// pkg/raft/tracetest/redirect. Строгий set-once: ровно один успешный
// вызов SetTraceLogFile на процесс; любой повторный вызов — ошибка
// контракта. Эмиттер трассировки — kvs.ServeHTTP.
package redirect_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/vskurikhin/raft/pkg/kvservice"
	"github.com/vskurikhin/raft/pkg/raft"
)

func TestTraceRedirect(t *testing.T) {
	// Порядок cleanup (LIFO): остановка сервиса → проверка leaktest.
	t.Cleanup(leaktest.CheckTimeout(t, raft.LeaktestBudget))

	// err-path: вызов с недоступным путём ДО успешной конфигурации —
	// ошибка I/O; окно конфигурации не расходуется.
	// При -count>1 строгое set-once окно уже израсходовано первым
	// прогоном процесса (процессный и необратимый контракт) — полный
	// сценарий выполняется один раз на процесс.
	bad := filepath.Join(t.TempDir(), "no-such-dir", "trace.log")
	err := kvservice.SetTraceLogFile(bad)
	if err == nil {
		t.Fatalf("SetTraceLogFile(%q): want error, got nil", bad)
	}
	if strings.Contains(err.Error(), "exactly once") {
		t.Skip("trace configuration window consumed by a previous run")
	}

	// Ошибка I/O не израсходовала окно: немедленный повтор с корректным
	// путём обязан быть успешным и является ЕДИНСТВЕННОЙ успешной
	// конфигурацией процесса.
	path := filepath.Join(t.TempDir(), "kv-trace.log")
	if err := kvservice.SetTraceLogFile(path); err != nil {
		t.Fatalf("SetTraceLogFile: %v", err)
	}

	// Строгий set-once: повторный вызов ДО создания сервиса — ошибка контракта.
	if err := kvservice.SetTraceLogFile(path); err == nil {
		t.Fatal("SetTraceLogFile: want contract error before service creation, got nil")
	}

	// Создаём KVService; сторожевой флаг traceCMCreated поднимается
	// конструктором.
	kvs := newTestService(t)

	// Эмиттер трассировки — kvs.ServeHTTP (traceLogf("serving HTTP on %s"),
	// печатается при TraceKV=1 > 0): после запуска файл не пуст.
	if err := kvs.ServeHTTP(":0"); err != nil {
		t.Fatalf("ServeHTTP: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		data, err := os.ReadFile(path)
		if err == nil && strings.Contains(string(data), "serving HTTP on") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("trace file %q does not contain the ServeHTTP entry: %v", path, err)
		}
		// poll-интервал condition-wait (не фиксированная пауза).
		time.Sleep(5 * time.Millisecond)
	}

	// fail-fast: повторный вызов ПОСЛЕ создания сервиса — ошибка контракта.
	if err := kvservice.SetTraceLogFile(path); err == nil {
		t.Fatal("SetTraceLogFile: want contract error after service creation, got nil")
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
