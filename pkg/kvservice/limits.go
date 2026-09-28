package kvservice

import (
	"errors"
	"fmt"
	"math"

	"github.com/vskurikhin/raft"
	"github.com/vskurikhin/raft/pkg/raft/contract"
	"github.com/vskurikhin/raft/pkg/raft/protocol"
)

// Пределы входных полей KV-сервиса по умолчанию и их верхние границы, байт.
// Размер строки — длина после JSON-декодирования (UTF-8).
const (
	_defaultMaxKeyBytes   = 512
	_defaultMaxValueBytes = 2048
	_maxKeyBytesLimit     = 8192
	_maxValueBytesLimit   = 16384
)

// _encodedCommandOverhead — G_KV: консервативная верхняя оценка накладных
// байтов сетевого gob-кодирования Command в анонимной обёртке Data сверх
// строк Key, Value и CompareValue. Оценка выведена из фиксированного графа
// типов (полные описания типов, имена типов и полей, префиксы кадров, номера
// типов и длины шириной 9 байт, значение ID — 10 байт) и не зависит от
// истории реестра типов процесса.
const _encodedCommandOverhead = 816

// _jsonBodyOverhead — запас на JSON-обвязку в пределе тела запроса.
const _jsonBodyOverhead = 1024

// _jsonEscapeFactor — множитель худшего экранирования строки в JSON.
const _jsonEscapeFactor = 6

// kvLimits — нормализованные пределы входных полей и тела запроса.
type kvLimits struct {
	maxKeyBytes   int
	maxValueBytes int
	maxBodyBytes  int64
}

// EncodedMaxKV — верхняя оценка сетевой длины Data HTTP-команды при пределах
// ключа maxKeyBytes и значения maxValueBytes: K + 2V + G_KV, где 2V покрывает
// Value и CompareValue команды CAS. Отрицательные пределы и переполнение —
// ошибка.
func EncodedMaxKV(maxKeyBytes, maxValueBytes int) (uint64, error) {
	if maxKeyBytes < 0 || maxValueBytes < 0 {
		return 0, fmt.Errorf("kvservice: EncodedMaxKV: negative limits %d/%d", maxKeyBytes, maxValueBytes)
	}
	key, value := uint64(maxKeyBytes), uint64(maxValueBytes)
	if value > (math.MaxUint64-_encodedCommandOverhead-key)/2 {
		return 0, fmt.Errorf("kvservice: EncodedMaxKV(%d,%d) overflows", maxKeyBytes, maxValueBytes)
	}
	return key + 2*value + _encodedCommandOverhead, nil
}

// normalizeKVLimits нормализует библиотечные пределы полей: ноль — умолчание
// 512/2048, отрицательное значение и выход за верхнюю границу 8192/16384 —
// ошибка. Вычисляет предел тела JSON-запроса 6·(K+2V)+1024.
func normalizeKVLimits(maxKeyBytes, maxValueBytes int) (kvLimits, error) {
	if maxKeyBytes == 0 {
		maxKeyBytes = _defaultMaxKeyBytes
	}
	if maxValueBytes == 0 {
		maxValueBytes = _defaultMaxValueBytes
	}
	if maxKeyBytes < 0 || maxKeyBytes > _maxKeyBytesLimit {
		return kvLimits{}, fmt.Errorf("kvservice: MaxKeyBytes must be between 1 and %d, got %d",
			_maxKeyBytesLimit, maxKeyBytes)
	}
	if maxValueBytes < 0 || maxValueBytes > _maxValueBytesLimit {
		return kvLimits{}, fmt.Errorf("kvservice: MaxValueBytes must be between 1 and %d, got %d",
			_maxValueBytesLimit, maxValueBytes)
	}
	fields := int64(maxKeyBytes) + 2*int64(maxValueBytes)
	return kvLimits{
		maxKeyBytes:   maxKeyBytes,
		maxValueBytes: maxValueBytes,
		maxBodyBytes:  _jsonEscapeFactor*fields + _jsonBodyOverhead,
	}, nil
}

// ValidateConfig проверяет конфигурацию сервиса до создания транспорта и
// запуска служб: пределы полей MaxKeyBytes/MaxValueBytes, условие
// EncodedMaxKV(K,V) <= MaxDataBytes нормализованного профиля пределов и
// профиль узла raft.ValidateConfig с профилем сроков timing планируемого
// транспорта.
func ValidateConfig(cfg *Config, timing contract.TransportTiming) error {
	_, err := validateConfig(cfg, timing)
	return err
}

func validateConfig(cfg *Config, timing contract.TransportTiming) (kvLimits, error) {
	if cfg == nil {
		return kvLimits{}, errors.New("kvservice: ValidateConfig: nil config")
	}
	kv, err := normalizeKVLimits(cfg.MaxKeyBytes, cfg.MaxValueBytes)
	if err != nil {
		return kvLimits{}, err
	}
	limits, err := protocol.NormalizeLimits(cfg.Limits)
	if err != nil {
		return kvLimits{}, fmt.Errorf("kvservice: invalid limits profile: %w", err)
	}
	encoded, err := EncodedMaxKV(kv.maxKeyBytes, kv.maxValueBytes)
	if err != nil {
		return kvLimits{}, err
	}
	if encoded > limits.MaxDataBytes {
		return kvLimits{}, fmt.Errorf("kvservice: EncodedMaxKV(%d,%d)=%d exceeds max-data-bytes=%d;"+
			" increase max-data-bytes within F/N bounds or lower KV limits",
			kv.maxKeyBytes, kv.maxValueBytes, encoded, limits.MaxDataBytes)
	}
	if err = raft.ValidateConfig(&cfg.Config, timing); err != nil {
		return kvLimits{}, err
	}
	return kv, nil
}

// validateServiceConfig проверяет конфигурацию с профилем сроков уже
// переданного транспорта. Транспорт без contract.TransportTimingProvider —
// ошибка конфигурации.
func validateServiceConfig(cfg *Config) (kvLimits, error) {
	if cfg == nil {
		return kvLimits{}, errors.New("kvservice: nil config")
	}
	provider, ok := cfg.Transport.(contract.TransportTimingProvider)
	if !ok {
		return kvLimits{}, fmt.Errorf("kvservice: transport %T does not implement contract.TransportTimingProvider",
			cfg.Transport)
	}
	return validateConfig(cfg, provider.TransportTiming())
}
