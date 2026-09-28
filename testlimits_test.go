package raft

import (
	"github.com/vskurikhin/raft/pkg/raft/contract"
	"github.com/vskurikhin/raft/pkg/raft/protocol"
	"github.com/vskurikhin/raft/pkg/raft/transp"
)

// testLimits — профиль пределов по умолчанию для CM, собранных литералом:
// конструктор записывает в CM нормализованный профиль, равный профилю
// транспорта, и литерал воспроизводит это достижимое состояние.
var testLimits = protocol.DefaultLimits()

// delegatedLimits возвращает профиль пределов встроенного транспорта
// тестового двойника; двойник без внутреннего транспорта доставляет
// сообщения внутри процесса и сообщает профиль по умолчанию.
func delegatedLimits(inner Transport) contract.Limits {
	if provider, ok := inner.(contract.LimitsProvider); ok && !IsNilInterface(inner) {
		return provider.Limits()
	}
	return protocol.DefaultLimits()
}

// delegatedTiming возвращает сроки встроенного транспорта тестового
// двойника; двойник без внутреннего транспорта — внутрипроцессный:
// InProcess=true, окно обмена InmemTransportTimeout, без установки
// соединения.
func delegatedTiming(inner Transport) contract.TransportTiming {
	if provider, ok := inner.(contract.TransportTimingProvider); ok && !IsNilInterface(inner) {
		return provider.TransportTiming()
	}
	return contract.TransportTiming{
		BaseSend:  transp.InmemTransportTimeout,
		BaseRecv:  transp.InmemTransportTimeout,
		Dial:      0,
		InProcess: true,
	}
}

func (m *mockTransportAE) Limits() contract.Limits { return delegatedLimits(m.Transport) }
func (m *mockTransportAE) TransportTiming() contract.TransportTiming {
	return delegatedTiming(m.Transport)
}

func (m *mockTransportConflict) Limits() contract.Limits { return delegatedLimits(m.Transport) }
func (m *mockTransportConflict) TransportTiming() contract.TransportTiming {
	return delegatedTiming(m.Transport)
}

func (m *recordingVoteTransport) Limits() contract.Limits { return delegatedLimits(m.Transport) }
func (m *recordingVoteTransport) TransportTiming() contract.TransportTiming {
	return delegatedTiming(m.Transport)
}

func (m *mockPreVoteGrant) Limits() contract.Limits { return delegatedLimits(m.Transport) }
func (m *mockPreVoteGrant) TransportTiming() contract.TransportTiming {
	return delegatedTiming(m.Transport)
}

func (m *blockingPreVoteTransport) Limits() contract.Limits { return delegatedLimits(m.Transport) }
func (m *blockingPreVoteTransport) TransportTiming() contract.TransportTiming {
	return delegatedTiming(m.Transport)
}

func (m *refusingPreVoteTransport) Limits() contract.Limits { return delegatedLimits(m.Transport) }
func (m *refusingPreVoteTransport) TransportTiming() contract.TransportTiming {
	return delegatedTiming(m.Transport)
}

func (m *notImplementedPreVoteTransport) Limits() contract.Limits {
	return delegatedLimits(m.Transport)
}
func (m *notImplementedPreVoteTransport) TransportTiming() contract.TransportTiming {
	return delegatedTiming(m.Transport)
}

func (m *preVoteRegressionTransport) Limits() contract.Limits { return delegatedLimits(m.Transport) }
func (m *preVoteRegressionTransport) TransportTiming() contract.TransportTiming {
	return delegatedTiming(m.Transport)
}

// Заглушка TransportManager для проверок New: сообщает профиль TCP по
// умолчанию — пределы по умолчанию и нормализованные нулевые сроки.
func (stubTransportManager) Limits() contract.Limits { return protocol.DefaultLimits() }
func (stubTransportManager) TransportTiming() contract.TransportTiming {
	_, timing := transp.NormalizeTCPTimeouts(transp.TCPTimeouts{})
	return timing
}
