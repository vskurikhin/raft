package raft

import (
	"os"
	"testing"
)

// TestMain выполняет единственную статическую конфигурацию трассировки
// тестового бинарника (вывод по умолчанию — stderr) до создания первых
// ConsensusModule. Контракт SetTraceLogFile — строгий set-once (ADR-004):
// вызовы из тестов этого бинарника запрещены (окно уже израсходовано,
// любой вызов вернул бы ошибку контракта). Покрытие конфигурации —
// в пакетах pkg/raft/tracetest/{redirect,reset}.
func TestMain(m *testing.M) {
	if err := SetTraceLogFile(""); err != nil {
		os.Stderr.WriteString("raft: trace configuration failed: " + err.Error() + "\n")
		os.Exit(1)
	}
	os.Exit(m.Run())
}
