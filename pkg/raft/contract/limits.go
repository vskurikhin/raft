package contract

import (
	"errors"
	"fmt"
	"math"
)

// Байтовые величины сетевого формата RPC, от которых зависят отношения
// пределов. Значения задаются раскладкой кадра и продублированы здесь,
// потому что contract не зависит от пакета кодека.
const (
	// limitsMinFrameBytes — наибольший кадр ответа с ошибкой: заголовок 24 Б,
	// оболочка ошибки 8 Б и текст до 4096 Б.
	limitsMinFrameBytes = 4128
	// limitsAppendEntriesOverhead — заголовок кадра и фиксированная часть
	// тела AppendEntries.
	limitsAppendEntriesOverhead = 88
	// limitsSingleEntryOverhead — AppendEntries с одной записью без Data.
	limitsSingleEntryOverhead = limitsAppendEntriesOverhead + limitsEntryHeaderBytes
	// limitsSnapshotControlOverhead — заголовок кадра и фиксированная часть
	// управляющего тела InstallSnapshot.
	limitsSnapshotControlOverhead = 96
	// limitsEntryHeaderBytes — служебная часть одной записи журнала.
	limitsEntryHeaderBytes = 32
)

// errInvalidLimits — общий префикс ошибок Limits.Validate.
var errInvalidLimits = errors.New("contract: invalid limits")

// Limits — байтовые и количественные пределы сетевого формата RPC.
// Одинаковый профиль обязателен отправителю и получателю. Нулевое значение
// не означает отсутствия пределов: Validate его отвергает.
type Limits struct {
	// MaxFrameBytes — наибольший полный кадр RPC, включая заголовок 24 Б;
	// тело снимка сюда не входит.
	MaxFrameBytes uint64
	// MaxEntries — наибольшее число записей журнала в одном AppendEntries.
	MaxEntries uint64
	// MaxDataBytes — наибольшая сетевая длина Data одной записи журнала.
	MaxDataBytes uint64
	// MaxConfigurationBytes — наибольшая длина конфигурации в управляющем
	// сообщении InstallSnapshot.
	MaxConfigurationBytes uint64
}

// Validate проверяет, что пределы положительны, представимы в int текущей
// платформы и согласованы между собой: наибольший ответ с ошибкой помещается
// в кадр, AppendEntries с одной записью наибольшей Data и управляющее
// сообщение снимка с наибольшей конфигурацией помещаются в кадр, конфигурация
// помещается в Data, а MaxEntries записей наибольшей Data помещаются в один
// AppendEntries. Нулевые поля Validate не нормализует.
func (l Limits) Validate() error {
	if err := l.validateRepresentable(); err != nil {
		return err
	}

	return l.validateRelations()
}

func (l Limits) validateRepresentable() error {
	fields := [...]struct {
		name  string
		value uint64
	}{
		{"MaxFrameBytes", l.MaxFrameBytes},
		{"MaxEntries", l.MaxEntries},
		{"MaxDataBytes", l.MaxDataBytes},
		{"MaxConfigurationBytes", l.MaxConfigurationBytes},
	}
	for _, field := range fields {
		if field.value == 0 {
			return fmt.Errorf("%w: %s must be positive", errInvalidLimits, field.name)
		}
		if field.value > math.MaxInt {
			return fmt.Errorf("%w: %s %d exceeds platform MaxInt %d",
				errInvalidLimits, field.name, field.value, uint64(math.MaxInt))
		}
	}

	return nil
}

func (l Limits) validateRelations() error {
	frame := l.MaxFrameBytes
	if frame < limitsMinFrameBytes {
		return fmt.Errorf("%w: MaxFrameBytes %d is less than largest error frame %d",
			errInvalidLimits, frame, limitsMinFrameBytes)
	}
	if l.MaxDataBytes > frame-limitsSingleEntryOverhead {
		return fmt.Errorf("%w: MaxDataBytes %d exceeds MaxFrameBytes-%d = %d",
			errInvalidLimits, l.MaxDataBytes, limitsSingleEntryOverhead, frame-limitsSingleEntryOverhead)
	}
	if l.MaxConfigurationBytes > frame-limitsSnapshotControlOverhead {
		return fmt.Errorf("%w: MaxConfigurationBytes %d exceeds MaxFrameBytes-%d = %d",
			errInvalidLimits, l.MaxConfigurationBytes, limitsSnapshotControlOverhead,
			frame-limitsSnapshotControlOverhead)
	}
	if l.MaxConfigurationBytes > l.MaxDataBytes {
		return fmt.Errorf("%w: MaxConfigurationBytes %d exceeds MaxDataBytes %d",
			errInvalidLimits, l.MaxConfigurationBytes, l.MaxDataBytes)
	}

	entriesSpace := frame - limitsAppendEntriesOverhead
	if l.MaxEntries > entriesSpace/limitsEntryHeaderBytes {
		return fmt.Errorf("%w: MaxEntries %d exceeds (MaxFrameBytes-%d)/%d = %d",
			errInvalidLimits, l.MaxEntries, limitsAppendEntriesOverhead, limitsEntryHeaderBytes,
			entriesSpace/limitsEntryHeaderBytes)
	}

	entryBytes := limitsEntryHeaderBytes + l.MaxDataBytes
	if l.MaxEntries > entriesSpace/entryBytes {
		return fmt.Errorf("%w: MaxEntries %d x (%d+MaxDataBytes %d) exceeds MaxFrameBytes-%d = %d",
			errInvalidLimits, l.MaxEntries, limitsEntryHeaderBytes, l.MaxDataBytes,
			limitsAppendEntriesOverhead, entriesSpace)
	}

	return nil
}

// LimitsProvider — дополнительный интерфейс транспорта, сообщающего свой
// действующий профиль пределов. Базовый Transport им не расширяется.
type LimitsProvider interface {
	Limits() Limits
}
