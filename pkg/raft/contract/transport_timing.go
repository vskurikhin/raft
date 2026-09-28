package contract

import "time"

// TransportTiming — действующие сроки транспорта, от которых зависит
// временная модель обычного RPC.
type TransportTiming struct {
	// BaseSend — базовое окно обмена отправителя, масштабируемое размером кадра.
	BaseSend time.Duration
	// BaseRecv — базовое окно получателя, масштабируемое размером кадра.
	BaseRecv time.Duration
	// Dial — лимит времени установки соединения; 0 допустим только
	// для транспорта без сетевых соединений.
	Dial time.Duration
	// InProcess — true только у транспорта внутри процесса: обмена по сети нет.
	InProcess bool
}

// TransportTimingProvider — дополнительный интерфейс транспорта, сообщающего
// свои действующие сроки. Базовый Transport им не расширяется.
type TransportTimingProvider interface {
	TransportTiming() TransportTiming
}
