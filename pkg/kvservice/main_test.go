package kvservice

import (
	"os"
	"testing"
)

// TestMain выполняет единственную статическую конфигурацию трассировки
// тестового бинарника (уровень 1, вывод по умолчанию — stderr) до создания
// первых KVService. Уровень задаётся явно: он равен эффективному порогу,
// действовавшему до перехода на SetTrace (инициализатор var traceKV = 1),
// и нулевое значение TraceConfig{} молча выключило бы трассировку тестов.
// Контракт SetTrace — строгий set-once: вызовы из тестов этого бинарника
// запрещены. Покрытие конфигурации — в пакетах
// pkg/kvservice/tracetest/{redirect,reset}.
func TestMain(m *testing.M) {
	if err := SetTrace(TraceConfig{Level: 1}); err != nil {
		_, _ = os.Stderr.WriteString("kvservice: trace configuration failed: " + err.Error() + "\n")
		os.Exit(1)
	}
	os.Exit(m.Run())
}
