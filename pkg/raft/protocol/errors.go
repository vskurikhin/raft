package protocol

import "errors"

var (
	// ErrFormat — маркерная ошибка нарушения раскладки кадра: magic, размер
	// заголовка, направление, тип RPC, reserved, длина тела, булевы и
	// перечислимые поля, лишние или недостающие байты.
	ErrFormat = errors.New("protocol: invalid frame format")
	// ErrUnsupportedVersion — маркерная ошибка неизвестной версии формата
	// кадра (FormatVersion).
	ErrUnsupportedVersion = errors.New("protocol: unsupported frame format version")
	// ErrLimit — маркерная ошибка нарушения пределов Limits, недопустимого
	// профиля пределов или непройденного временного неравенства.
	ErrLimit = errors.New("protocol: limit exceeded")
	// ErrRange — маркерная ошибка скалярного значения вне допустимого
	// диапазона, непредставимого значения или переполнения.
	ErrRange = errors.New("protocol: value out of range")
	// ErrData — маркерная ошибка кодирования или декодирования Data записи
	// журнала.
	ErrData = errors.New("protocol: data encoding error")
)
