package raft

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/vskurikhin/raft/pkg/raft/contract"
	"github.com/vskurikhin/raft/pkg/raft/protocol"
)

// _frameStepBytes — ступень масштабирования окна обмена по длине тела кадра
// (256 КиБ); используется только для чисел в тексте диагностики временного
// профиля, решение принимает protocol.CheckTimingStructure.
const _frameStepBytes = 262144

// _frameHeaderBytes — длина заголовка кадра сетевого формата.
const _frameHeaderBytes = 24

// normalizeTimerConfig заменяет неположительные временные параметры их
// умолчаниями Default*. Единственное правило нормализации для Server.Serve
// и ValidateConfig.
func normalizeTimerConfig(tc TimerConfig) TimerConfig {
	if tc.ApplyBatch <= 0 {
		tc.ApplyBatch = DefaultApplyBatchInterval
	}
	if tc.Heartbeat <= 0 {
		tc.Heartbeat = DefaultHeartbeatTimeout
	}
	if tc.Reelection <= 0 {
		tc.Reelection = DefaultReelectionTimeout
	}
	if tc.Ticker <= 0 {
		tc.Ticker = DefaultTickerTimeout
	}
	return tc
}

// timerConfigOf собирает временные параметры узла из Config.
func timerConfigOf(cfg *Config) TimerConfig {
	return TimerConfig{
		ApplyBatch: cfg.ApplyBatchInterval,
		Heartbeat:  cfg.HeartbeatTimeout,
		Reelection: cfg.ReelectionTimeout,
		Ticker:     cfg.TickerTimeout,
	}
}

// ValidateConfig проверяет профиль узла до запуска служб: нормализованный
// профиль пределов cfg.Limits (целиком нулевой — умолчание, частично
// нулевой — ошибка), диапазоны нормализованных временных параметров
// (ValidateTiming) и временную структуру с профилем сроков транспорта
// timing, который будет использован узлом. Для сетевого транспорта
// отвергается профиль heartbeat + ticker + max RPC window >= reelection
// base. Если транспорт cfg.Transport уже создан, он обязан сообщать пределы
// и сроки, а его пределы — совпадать с профилем узла; до создания
// транспорта (cfg.Transport == nil) проверяется только профиль.
func ValidateConfig(cfg *Config, timing contract.TransportTiming) error {
	if cfg == nil {
		return errors.New("raft: ValidateConfig: nil config")
	}
	limits, err := protocol.NormalizeLimits(cfg.Limits)
	if err != nil {
		return fmt.Errorf("raft: invalid limits profile: %w", err)
	}
	if !IsNilInterface(cfg.Transport) {
		if _, _, err = transportLimits(cfg.Limits, cfg.Transport); err != nil {
			return err
		}
	}
	timers := normalizeTimerConfig(timerConfigOf(cfg))
	if err = ValidateTiming(timers); err != nil {
		return fmt.Errorf("raft: invalid timing configuration: %w", err)
	}
	return checkTimingProfile(limits, timing, timers, _defaultCheckQuorumTimeout)
}

// checkTimingProfile — общий валидатор временного профиля всех точек
// проверки: CLI (ValidateConfig) и общего конструктора CM. Решение
// принимает protocol.CheckTimingStructure; при нарушении условия
// heartbeat + ticker + max RPC window < reelection base текст ошибки
// содержит слагаемые и итог.
func checkTimingProfile(
	limits contract.Limits, timing contract.TransportTiming, timers TimerConfig, checkQuorum time.Duration,
) error {
	err := protocol.CheckTimingStructure(limits, timing, protocol.TimerProfile{
		Heartbeat:     timers.Heartbeat,
		Tick:          timers.Ticker,
		ReelectionMin: timers.Reelection,
	}, checkQuorum)
	if err == nil {
		return nil
	}
	window, ok := maxRPCWindow(limits, timing)
	if ok && !timing.InProcess && errors.Is(err, protocol.ErrLimit) {
		left := timers.Heartbeat + timers.Ticker + window
		if left >= timers.Reelection {
			return fmt.Errorf("invalid timing profile: heartbeat %v + ticker %v + max RPC window %v = %v"+
				" >= reelection base %v: %w", timers.Heartbeat, timers.Ticker, window, left, timers.Reelection, err)
		}
	}
	return fmt.Errorf("invalid timing profile: %w", err)
}

// maxRPCWindow — max(BaseSend, BaseRecv) × max(1, ⌈(F−24)/256 КиБ⌉) для текста
// диагностики. ok = false, если окно непредставимо.
func maxRPCWindow(limits contract.Limits, timing contract.TransportTiming) (time.Duration, bool) {
	body := limits.MaxFrameBytes - min(limits.MaxFrameBytes, _frameHeaderBytes)
	factor := body / _frameStepBytes
	if body%_frameStepBytes != 0 {
		factor++
	}
	factor = max(factor, 1)
	base := max(timing.BaseSend, timing.BaseRecv)
	if base <= 0 || factor > uint64(math.MaxInt64/int64(base)) {
		return 0, false
	}
	return base * time.Duration(factor), true
}

// transportProfile проверяет профиль транспорта в общем конструкторе CM до
// запуска горутин: пределы транспорта совпадают с нормализованным профилем
// CM (transportLimits), временной профиль транспорта с фактическими
// таймерами узла проходит общий валидатор. Возвращает нормализованный
// профиль пределов CM.
func transportProfile(
	configured contract.Limits, transport Transport, timers TimerConfig, checkQuorum time.Duration,
) (contract.Limits, error) {
	limits, timing, err := transportLimits(configured, transport)
	if err != nil {
		return contract.Limits{}, err
	}
	if err = checkTimingProfile(limits, timing, timers, checkQuorum); err != nil {
		return contract.Limits{}, err
	}
	return limits, nil
}

// transportLimits нормализует настроенный профиль пределов и сверяет его с
// транспортом: транспорт обязан сообщать пределы (contract.LimitsProvider)
// и сроки (contract.TransportTimingProvider), подстановки умолчаний для
// транспорта без этих интерфейсов нет. Возвращает нормализованный профиль
// и сроки транспорта.
func transportLimits(
	configured contract.Limits, transport Transport,
) (contract.Limits, contract.TransportTiming, error) {
	limits, err := protocol.NormalizeLimits(configured)
	if err != nil {
		return contract.Limits{}, contract.TransportTiming{}, fmt.Errorf("raft: invalid limits profile: %w", err)
	}
	limitsProvider, ok := transport.(contract.LimitsProvider)
	if !ok {
		return contract.Limits{}, contract.TransportTiming{},
			fmt.Errorf("raft: transport %T does not implement contract.LimitsProvider", transport)
	}
	timingProvider, ok := transport.(contract.TransportTimingProvider)
	if !ok {
		return contract.Limits{}, contract.TransportTiming{},
			fmt.Errorf("raft: transport %T does not implement contract.TransportTimingProvider", transport)
	}
	if err = limitsMismatch(limits, limitsProvider.Limits()); err != nil {
		return contract.Limits{}, contract.TransportTiming{}, err
	}
	return limits, timingProvider.TransportTiming(), nil
}

// limitsMismatch сравнивает профиль CM с профилем транспорта и называет
// первый несовпадающий параметр.
func limitsMismatch(node, transport contract.Limits) error {
	fields := [...]struct {
		name            string
		node, transport uint64
	}{
		{"MaxFrameBytes", node.MaxFrameBytes, transport.MaxFrameBytes},
		{"MaxEntries", node.MaxEntries, transport.MaxEntries},
		{"MaxDataBytes", node.MaxDataBytes, transport.MaxDataBytes},
		{"MaxConfigurationBytes", node.MaxConfigurationBytes, transport.MaxConfigurationBytes},
	}
	for _, f := range fields {
		if f.node != f.transport {
			return fmt.Errorf("raft: limits mismatch: parameter %s node %d transport %d",
				f.name, f.node, f.transport)
		}
	}
	return nil
}

// Параметры профиля пределов в диагностике отказов.
const (
	limitParameterF = "F"
	limitParameterN = "N"
	limitParameterD = "D"
	limitParameterC = "C"
)

// Направления отказа по пределу в статистике.
const (
	limitDirectionSend      = "send"
	limitDirectionRecv      = "recv"
	limitDirectionPreflight = "preflight"
)

// limitRejectionPeerLocal — сосед локального preflight до журнала: отказ
// происходит до выбора получателя.
const limitRejectionPeerLocal = "local"

// limitRejectionKey — направление, сосед и параметр профиля отказа по
// пределу. Сосед отправки — ID узла, сосед приёма — адрес отправителя,
// локальный preflight — limitRejectionPeerLocal.
type limitRejectionKey struct {
	direction string
	peer      string
	parameter string
}

// limitRejectionSource — транспорт, учитывающий отказы приёма по пределам:
// ключ "<параметр>|<адрес соседа>".
type limitRejectionSource interface {
	LimitRejections() map[string]uint64
}

// statsLimitRejection — строка счётчика protocol_limit_rejections_total
// в документе статистики: без содержимого Data.
type statsLimitRejection struct {
	Direction string `json:"Direction"`
	Parameter string `json:"Parameter"`
	Peer      string `json:"Peer"`
	Count     uint64 `json:"Count"`
}

// classifyAELimit определяет параметр отказа AppendEntries по пределу без
// содержимого Data: число записей сверх N, первая запись с сетевой длиной
// Data сверх D (её индекс и длина), иначе — полный кадр сверх F. Вызывается
// без cm.mu: измерение Data — кодирование.
func classifyAELimit(entries []LogEntry, limits contract.Limits) (parameter string, index int, actual uint64) {
	if uint64(len(entries)) > limits.MaxEntries {
		return limitParameterN, -1, uint64(len(entries))
	}
	for i := range entries {
		size, err := protocol.MeasureData(entries[i].Data, limits.MaxFrameBytes)
		if err != nil || size > limits.MaxDataBytes {
			return limitParameterD, entries[i].Index, size
		}
	}
	return limitParameterF, -1, 0
}

// recordAELimitRejection учитывает отказ отправки AppendEntries соседу
// peerID по пределу сетевого формата: счётчик по соседу и параметру и
// трассировка уровня ключевых событий с пределом, фактической величиной и
// индексом записи, без содержимого Data. nextIndex не меняется.
// Самостоятельно захватывает и освобождает cm.mu.
func (cm *ConsensusModule) recordAELimitRejection(peerID int, entries []LogEntry, cause error) {
	parameter, index, actual := classifyAELimit(entries, cm.limits)
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.countLimitRejectionLocked(limitDirectionSend, fmt.Sprint(peerID), parameter)
	if traceEnabled(_traceLevelKeyEvents) {
		cm.traceLogfLocked(
			"limit rejection: direction=send peer=%d parameter=%s limit=%s actual=%d index=%d: %v",
			peerID, parameter, limitValue(cm.limits, parameter), actual, index, cause,
		)
	}
}

// limitValue — значение параметра профиля для диагностики.
func limitValue(limits contract.Limits, parameter string) string {
	switch parameter {
	case limitParameterF:
		return fmt.Sprint(limits.MaxFrameBytes)
	case limitParameterN:
		return fmt.Sprint(limits.MaxEntries)
	case limitParameterD:
		return fmt.Sprint(limits.MaxDataBytes)
	case limitParameterC:
		return fmt.Sprint(limits.MaxConfigurationBytes)
	default:
		return "?"
	}
}

// countLimitRejectionLocked увеличивает счётчик отказов по пределу.
// Требует удержания cm.mu.
func (cm *ConsensusModule) countLimitRejectionLocked(direction, peer, parameter string) {
	if cm.counters.limitRejections == nil {
		cm.counters.limitRejections = make(map[limitRejectionKey]int64)
	}
	cm.counters.limitRejections[limitRejectionKey{direction: direction, peer: peer, parameter: parameter}]++
}

// countLimitRejection — countLimitRejectionLocked для вызывающих без cm.mu.
// Самостоятельно захватывает и освобождает cm.mu.
func (cm *ConsensusModule) countLimitRejection(direction, peer, parameter string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.countLimitRejectionLocked(direction, peer, parameter)
}

// limitRejectionsLocked возвращает копию счётчика отказов по пределам.
// Требует удержания cm.mu.
func (cm *ConsensusModule) limitRejectionsLocked() map[limitRejectionKey]int64 {
	out := make(map[limitRejectionKey]int64, len(cm.counters.limitRejections))
	for k, v := range cm.counters.limitRejections {
		out[k] = v
	}
	return out
}

// limitRejectionRows собирает строки счётчика protocol_limit_rejections_total:
// отказы отправки и preflight из CM и отказы приёма транспорта, если он их
// учитывает. Строки упорядочены по направлению, параметру и соседу.
// Вызывается без cm.mu: транспорт читается своим собственным мьютексом.
func limitRejectionRows(cmCounts map[limitRejectionKey]int64, transport Transport) []statsLimitRejection {
	rows := make([]statsLimitRejection, 0, len(cmCounts))
	for k, v := range cmCounts {
		rows = append(rows, statsLimitRejection{
			Direction: k.direction, Parameter: k.parameter, Peer: k.peer, Count: uint64(v),
		})
	}
	if source, ok := transport.(limitRejectionSource); ok && !IsNilInterface(transport) {
		for key, v := range source.LimitRejections() {
			parameter, peer, _ := strings.Cut(key, "|")
			rows = append(rows, statsLimitRejection{
				Direction: limitDirectionRecv, Parameter: parameter, Peer: peer, Count: v,
			})
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		if a.Direction != b.Direction {
			return a.Direction < b.Direction
		}
		if a.Parameter != b.Parameter {
			return a.Parameter < b.Parameter
		}
		return a.Peer < b.Peer
	})
	return rows
}

// checkConfigurationData проверяет данные записи конфигурации до журнала:
// длина EncodeConfiguration не больше MaxConfigurationBytes и сетевая длина
// той же Data не больше MaxDataBytes. Вызывается без cm.mu.
func checkConfigurationData(data []byte, limits contract.Limits) (parameter string, err error) {
	if uint64(len(data)) > limits.MaxConfigurationBytes {
		return limitParameterC, fmt.Errorf("%w: parameter C: configuration %d bytes exceeds MaxConfigurationBytes %d",
			protocol.ErrLimit, len(data), limits.MaxConfigurationBytes)
	}
	if _, err = protocol.MeasureData(data, limits.MaxDataBytes); err != nil {
		return limitParameterD, fmt.Errorf("parameter D: configuration entry data: %w", err)
	}
	return "", nil
}

// measureCommand проверяет сетевую длину Data команды по MaxDataBytes.
// Превышение — ErrCommandTooLarge, ошибка кодирования возвращается как есть.
// Вызывается без cm.mu.
func measureCommand(command any, limits contract.Limits) error {
	if _, err := protocol.MeasureData(command, limits.MaxDataBytes); err != nil {
		if errors.Is(err, protocol.ErrLimit) {
			return fmt.Errorf("%w: %w", ErrCommandTooLarge, err)
		}
		return fmt.Errorf("raft: command data is not encodable: %w", err)
	}
	return nil
}
