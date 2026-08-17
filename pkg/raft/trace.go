package raft

import (
	"log"
	"os"
)

// TraceCM — порог детализации отладочных сообщений консенсус-модуля
// (traceLogf, traceLogLocked). Сообщение с уровнем level выводится,
// если TraceCM > level. Значение задаётся флагом --trace-log-level
// командной строки, по умолчанию 1.
var TraceCM = 1

// _traceLogger — логгер отладочных сообщений консенсус-модуля (traceLogf,
// traceLogfLocked). По умолчанию выводит в стандартный логгер (stderr).
// Перенаправляется в файл функцией SetTraceLogFile.
var _traceLogger = log.Default()

// traceEnabled сообщает, будет ли сообщение уровня level напечатано
// трассировкой. Единственный источник истины для порога TraceCM.
func traceEnabled(level int) bool { return level < TraceCM }

// _traceLogFile — открытый файл трассировки; nil, если вывод в stderr.
var _traceLogFile *os.File

// SetTraceLogFile перенаправляет отладочные сообщения (traceLogf,
// traceLogfLocked) в файл с указанным путём. Файл открывается в режиме
// добавления и создаётся при необходимости. Пустая строка возвращает
// вывод в стандартный логгер.
//
// Должна вызываться до старта горутин, использующих трассировку.
func SetTraceLogFile(path string) error {
	if _traceLogFile != nil {
		_ = _traceLogFile.Close()
		_traceLogFile = nil
	}
	if path == "" {
		_traceLogger = log.Default()
		return nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	_traceLogFile = f
	_traceLogger = log.New(f, "", log.LstdFlags|log.Lmicroseconds)
	return nil
}
