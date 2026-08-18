package raft

import (
	"fmt"
	"log"
	"os"
	"sync/atomic"
)

// TraceCM — порог детализации отладочных сообщений консенсус-модуля
// (traceLogf, traceLogLocked). Сообщение с уровнем level выводится,
// если TraceCM > level. Значение задаётся флагом --trace-log-level
// командной строки, по умолчанию 1. Изменяется только до старта горутин.
var TraceCM = 1

// _traceLogger — логгер отладочных сообщений консенсус-модуля (traceLogf,
// traceLogfLocked). По умолчанию выводит в стандартный логгер (stderr).
// После единственной успешной конфигурации SetTraceLogFile — read-only.
var _traceLogger = log.Default()

// traceEnabled сообщает, будет ли сообщение уровня level напечатано
// трассировкой. Единственный источник истины для порога TraceCM.
func traceEnabled(level int) bool { return level < TraceCM }

// _traceLogFile — открытый файл трассировки; nil, если вывод в stderr.
//
//nolint:unused
var _traceLogFile *os.File

// traceConfigured — сторожевой флаг строгого set-once: единственная
// успешная конфигурация трассировки на процесс уже выполнена.
// Устанавливается только успешным вызовом SetTraceLogFile
// (вызов, завершившийся ошибкой I/O, окно не расходует).
var traceConfigured atomic.Bool

// traceCMCreated — сторожевой флаг: в процессе уже создавался
// ConsensusModule. Устанавливается конструктором.
var traceCMCreated atomic.Bool

// SetTraceLogFile конфигурирует трассировку (traceLogf, traceLogfLocked):
// путь к файлу журнала; пустая строка — вывод в стандартный логгер.
// Файл открывается в режиме добавления и создаётся при необходимости.
//
// Контракт (строгий set-once-before-first-CM): функция успешна ровно
// один раз на процесс, до создания первого ConsensusModule (и, тем
// самым, до входа CM в Candidate/Candidate-Modifier). Первый успешный
// вызов выигрывает и замораживает конфигурацию; состояние трассировки
// после этого read-only. Любой повторный вызов — детерминированная
// ошибка контракта, в том числе до создания CM; вызов после создания
// CM — та же ошибка. Вызов, завершившийся ошибкой I/O, окно
// конфигурации не расходует.
func SetTraceLogFile(path string) error {
	if traceConfigured.Load() || traceCMCreated.Load() {
		return fmt.Errorf(
			"raft: trace configuration must be set exactly once, " +
				"before the first ConsensusModule is created; repeated calls are forbidden",
		)
	}
	if path == "" {
		_traceLogger = log.Default()
		traceConfigured.Store(true)
		return nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	_traceLogFile = f
	_traceLogger = log.New(f, "", log.LstdFlags|log.Lmicroseconds)
	traceConfigured.Store(true)
	return nil
}
