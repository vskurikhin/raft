// Package redirect_test — test-бинарник контракта трассировки pkg/raft:
// сценарий перенаправления в файл. Один бинарник на один сценарий
// конфигурации (ADR-004, NEW-14): окно конфигурации процессное и
// необратимое, сценарии redirect и reset взаимоисключащи в одном
// процессе. Строгий set-once: ровно один успешный вызов
// SetTraceLogFile на процесс; любой повторный вызов — ошибка контракта.
package redirect_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fortytw2/leaktest"
	"github.com/vskurikhin/raft/pkg/raft"
)

func TestTraceRedirect(t *testing.T) {
	// Порядок cleanup (LIFO): остановка CM → проверка leaktest.
	t.Cleanup(leaktest.CheckTimeout(t, raft.LeaktestBudget))

	// err-path: вызов с недоступным путём ДО успешной конфигурации —
	// ошибка I/O; окно конфигурации не расходуется.
	// При -count>1 строгое set-once окно уже израсходовано первым
	// прогоном процесса (процессный и необратимый контракт) — полный
	// сценарий выполняется один раз на процесс.
	bad := filepath.Join(t.TempDir(), "no-such-dir", "trace.log")
	err := raft.SetTraceLogFile(bad)
	if err == nil {
		t.Fatalf("SetTraceLogFile(%q): want error, got nil", bad)
	}
	if strings.Contains(err.Error(), "exactly once") {
		t.Skip("trace configuration window consumed by a previous run")
	}

	// Ошибка I/O не израсходовала окно: немедленный повтор с корректным
	// путём обязан быть успешным и является ЕДИНСТВЕННОЙ успешной
	// конфигурацией процесса.
	path := filepath.Join(t.TempDir(), "raft-trace.log")
	if err := raft.SetTraceLogFile(path); err != nil {
		t.Fatalf("SetTraceLogFile: %v", err)
	}

	// Строгий set-once: повторный вызов ДО создания CM — ошибка контракта.
	if err := raft.SetTraceLogFile(path); err == nil {
		t.Fatal("SetTraceLogFile: want contract error before CM creation, got nil")
	}

	// Создаём CM (InmemTransport + MapStorage, без кластера).
	// CommitChannelFSM требует читателя канала при живом узле.
	commitCh := make(chan raft.CommitEntry)
	readerDone := make(chan any)
	go func() {
		defer close(readerDone)
		for range commitCh {
		}
	}()
	ready := make(chan any)
	cm := raft.NewConsensusModule(
		0, []int{}, raft.NewInmemTransport("trace-redirect"),
		raft.NewMapStorage(), raft.NewCommitChannelFSM(commitCh), ready,
	)
	close(ready)
	t.Cleanup(func() {
		cm.Stop()
		close(commitCh)
		<-readerDone
	})

	// Эмиттер трассировки — cm.Stop() (traceLogf(0, "CM.Stop called /
	// becomes Dead"), печатается при TraceCM=1 > 0): после Stop файл
	// не пуст.
	cm.Stop()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !strings.Contains(string(data), "CM.Stop called / becomes Dead") {
		t.Errorf("trace file %q does not contain the CM.Stop entry: %q", path, data)
	}

	// fail-fast: повторный вызов ПОСЛЕ создания CM — ошибка контракта.
	if err := raft.SetTraceLogFile(path); err == nil {
		t.Fatal("SetTraceLogFile: want contract error after CM creation, got nil")
	}
}
