package protocol

import (
	"fmt"

	"github.com/vskurikhin/raft/pkg/raft/contract"
)

// Limits — пределы сетевого формата; единственный владелец типа — contract.
type Limits = contract.Limits

// Профиль пределов по умолчанию.
const (
	defaultMaxFrameBytes         = 262144
	defaultMaxEntries            = 31
	defaultMaxDataBytes          = 8192
	defaultMaxConfigurationBytes = 2560
)

// DefaultLimits возвращает новую копию профиля пределов по умолчанию.
func DefaultLimits() contract.Limits {
	return contract.Limits{
		MaxFrameBytes:         defaultMaxFrameBytes,
		MaxEntries:            defaultMaxEntries,
		MaxDataBytes:          defaultMaxDataBytes,
		MaxConfigurationBytes: defaultMaxConfigurationBytes,
	}
}

// NormalizeLimits заменяет целиком нулевой профиль профилем по умолчанию и
// проверяет результат. Частично нулевой профиль — ошибка; MaxEntries никогда
// не пересчитывается. Ошибка проверки оборачивается ErrLimit.
func NormalizeLimits(limits contract.Limits) (contract.Limits, error) {
	if limits == (contract.Limits{}) {
		limits = DefaultLimits()
	}
	if err := checkLimits(limits); err != nil {
		return contract.Limits{}, err
	}

	return limits, nil
}

func checkLimits(limits Limits) error {
	if err := limits.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrLimit, err)
	}

	return nil
}
