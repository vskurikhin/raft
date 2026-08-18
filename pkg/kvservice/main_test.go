package kvservice

import (
	"os"
	"testing"
)

// TestMain выполняет единственную статическую конфигурацию трассировки
// тестового бинарника (вывод по умолчанию — stderr) до создания первых
// KVService. Контракт SetTraceLogFile — строгий set-once: вызовы из
// тестов этого бинарника запрещены. Покрытие конфигурации —
// в пакетах pkg/kvservice/tracetest/{redirect,reset}.
func TestMain(m *testing.M) {
	if err := SetTraceLogFile(""); err != nil {
		os.Stderr.WriteString("kvservice: trace configuration failed: " + err.Error() + "\n")
		os.Exit(1)
	}
	os.Exit(m.Run())
}
