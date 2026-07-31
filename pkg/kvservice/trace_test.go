package kvservice

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestSetTraceLogFile проверяет, что отладочные сообщения traceLogf
// записываются в файл, заданный SetTraceLogFile, и возвращаются
// в стандартный логгер при пустом пути.
func TestSetTraceLogFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kv-trace.log")
	kvs := &KVService{id: 7}

	oldLevel := TraceKV
	defer func() { TraceKV = oldLevel }()
	TraceKV = 1

	if err := SetTraceLogFile(path); err != nil {
		t.Fatalf("SetTraceLogFile: %v", err)
	}
	defer func() { _ = SetTraceLogFile("") }()

	kvs.traceLogf("kv-trace-test %d", 42)

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !strings.Contains(string(data), "kv-trace-test 42") {
		t.Errorf("trace file %q does not contain 'kv-trace-test 42': %q", path, data)
	}
}

// TestSetTraceLogFileBadPath проверяет, что SetTraceLogFile возвращает
// ошибку для некорректного пути и не ломает существующий логгер.
func TestSetTraceLogFileBadPath(t *testing.T) {
	if err := SetTraceLogFile(""); err != nil {
		t.Fatalf("SetTraceLogFile(\"\"): %v", err)
	}

	bad := filepath.Join(t.TempDir(), "no-such-dir", "trace.log")
	if err := SetTraceLogFile(bad); err == nil {
		t.Fatalf("SetTraceLogFile(%q): want error, got nil", bad)
	}
}
