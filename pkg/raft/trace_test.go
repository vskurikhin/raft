package raft

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
)

// TestSetTraceLogFile проверяет, что отладочные сообщения traceLogf
// записываются в файл, заданный SetTraceLogFile, и возвращаются
// в стандартный логгер при пустом пути.
//
// Значение TraceCM не изменяется (по умолчанию 1): уровень 0
// заведомо ниже порога, поэтому запись гарантированно происходит,
// а сама глобальная переменная остаётся доступной только на чтение
// из фоновых горутин — изменение её из теста породило бы data race.
func TestSetTraceLogFile(t *testing.T) {
	defer leaktest.CheckTimeout(t, Quantum*150*time.Millisecond)()

	path := filepath.Join(t.TempDir(), "raft-trace.log")
	cm := new(ConsensusModule)

	if err := SetTraceLogFile(path); err != nil {
		t.Fatalf("SetTraceLogFile: %v", err)
	}
	defer func() { _ = SetTraceLogFile("") }()

	cm.traceLogf(0, "trace-test %d", 42)

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !strings.Contains(string(data), "trace-test 42") {
		t.Errorf("trace file %q does not contain 'trace-test 42': %q", path, data)
	}
}

// TestSetTraceLogFileBadPath проверяет, что SetTraceLogFile возвращает
// ошибку для некорректного пути и не ломает существующий логгер.
func TestSetTraceLogFileBadPath(t *testing.T) {
	defer leaktest.CheckTimeout(t, Quantum*150*time.Millisecond)()

	if err := SetTraceLogFile(""); err != nil {
		t.Fatalf("SetTraceLogFile(\"\"): %v", err)
	}

	bad := filepath.Join(t.TempDir(), "no-such-dir", "trace.log")
	if err := SetTraceLogFile(bad); err == nil {
		t.Fatalf("SetTraceLogFile(%q): want error, got nil", bad)
	}
}
