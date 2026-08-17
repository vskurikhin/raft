package _init

import (
	"log"

	"github.com/vskurikhin/raft/internal/config"
	"github.com/vskurikhin/raft/pkg/kvservice"
	"github.com/vskurikhin/raft/pkg/raft"
)

var Values config.Values

// configureTraceLogging применяет параметры трассировки командной строки:
// --trace-log-level задаёт пороги raft.TraceCM и kvservice.TraceKV,
// --trace-log-file — файл, в который пишутся отладочные сообщения
// консенсус-модуля и KV-сервиса (пустая строка — stderr).
func configureTraceLogging(values config.Values) error {
	raft.TraceCM = values.TraceLogLevel
	kvservice.TraceKV = values.TraceLogLevel
	if values.TraceLogFile == "" {
		return nil
	}
	if err := raft.SetTraceLogFile(values.TraceLogFile); err != nil {
		return err
	}
	return kvservice.SetTraceLogFile(values.TraceLogFile)
}

//nolint:gochecknoinits
func init() {
	Values = config.ParseFlags()
	if err := configureTraceLogging(Values); err != nil {
		log.Fatal(err)
	}
}
