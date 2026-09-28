package transp

import (
	"errors"
	"maps"
	"net"
	"strings"
	"sync"

	"github.com/vskurikhin/raft/pkg/raft/protocol"
)

// Параметры профиля пределов в учёте отказов приёма.
const (
	limitParameterF            = "F"
	limitParameterN            = "N"
	limitParameterD            = "D"
	limitParameterC            = "C"
	limitParameterUnclassified = "unclassified"
)

// limitRejections — счётчик отказов приёма по пределам сетевого формата:
// по параметру профиля и адресу соседа. Содержимое Data не хранится.
type limitRejections struct {
	mu     sync.Mutex
	counts map[string]uint64
}

// record учитывает отказ по пределу, если err — protocol.ErrLimit, и
// возвращает параметр отказа; иначе возвращает пустую строку.
func (l *limitRejections) record(peer string, err error) string {
	if !errors.Is(err, protocol.ErrLimit) {
		return ""
	}
	parameter := limitParameterOf(err)
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.counts == nil {
		l.counts = make(map[string]uint64)
	}
	l.counts[parameter+"|"+peer]++
	return parameter
}

// snapshot возвращает копию счётчика.
func (l *limitRejections) snapshot() map[string]uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return maps.Clone(l.counts)
}

// limitParameterOf определяет параметр профиля по имени предела в тексте
// ошибки кодека: MaxConfigurationBytes — C, MaxDataBytes — D, MaxEntries —
// N, MaxFrameBytes — F. Формат ошибок кодека не меняется.
func limitParameterOf(err error) string {
	text := err.Error()
	switch {
	case strings.Contains(text, "MaxConfigurationBytes"):
		return limitParameterC
	case strings.Contains(text, "MaxDataBytes"):
		return limitParameterD
	case strings.Contains(text, "MaxEntries"):
		return limitParameterN
	case strings.Contains(text, "MaxFrameBytes"):
		return limitParameterF
	default:
		return limitParameterUnclassified
	}
}

// remoteHost — адрес соседа входящего соединения без эфемерного порта:
// достоверный до разбора кадра контекст отправителя.
func remoteHost(addr net.Addr) string {
	if addr == nil {
		return "unknown"
	}
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return addr.String()
	}
	return host
}
