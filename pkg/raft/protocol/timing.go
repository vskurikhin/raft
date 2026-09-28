package protocol

import (
	"fmt"
	"math"
	"math/bits"
	"time"

	"github.com/vskurikhin/raft/pkg/raft/contract"
)

// frameStepBytes — ступень масштабирования срока обычного RPC по длине тела.
const frameStepBytes = 262144

// bitsPerByteNanos — множитель перевода байтов при скорости в бит/с
// в наносекунды: 8 бит × 10⁹ нс.
const bitsPerByteNanos = 8 * uint64(time.Second)

// TimerProfile — нормализованные таймеры Raft для проверки временной модели.
type TimerProfile struct {
	Heartbeat     time.Duration
	Tick          time.Duration
	ReelectionMin time.Duration
}

// TimingPassport — паспорт среды: верхние границы задержек и нижняя граница
// полезной скорости передачи на одного соседа в бит/с.
type TimingPassport struct {
	EncodeDispatchMax time.Duration
	PersistDecodeMax  time.Duration
	RTTMax            time.Duration
	DialMax           time.Duration
	MinBitsPerSecond  uint64
}

// timingModel — производные величины проверенного профиля.
type timingModel struct {
	frameFactor     uint64
	heartbeatTick   time.Duration
	maxFrameWindow  time.Duration
	structureInputs string
}

// CheckTimingStructure — проверка временного профиля при старте узла:
// пределы, положительность сроков транспорта и таймеров, представимость
// окон обмена наибольшего кадра и их сумм, а для сетевого транспорта ещё и
// условие S1: Heartbeat + Tick + T_max(F) < ReelectionMin. Функция чистая.
func CheckTimingStructure(limits contract.Limits, timing contract.TransportTiming,
	timer TimerProfile, checkQuorum time.Duration,
) error {
	_, err := checkTimingStructure(limits, timing, timer, checkQuorum)

	return err
}

func checkTimingStructure(limits contract.Limits, timing contract.TransportTiming,
	timer TimerProfile, checkQuorum time.Duration,
) (timingModel, error) {
	if err := checkLimits(limits); err != nil {
		return timingModel{}, err
	}
	if err := checkTransportTiming(timing); err != nil {
		return timingModel{}, err
	}
	if err := checkTimerProfile(timer, checkQuorum); err != nil {
		return timingModel{}, err
	}

	factor := frameFactor(limits.MaxFrameBytes)
	sendWindow, err := scaleDuration("BaseSend", timing.BaseSend, factor)
	if err != nil {
		return timingModel{}, err
	}
	recvWindow, err := scaleDuration("BaseRecv", timing.BaseRecv, factor)
	if err != nil {
		return timingModel{}, err
	}
	heartbeatTick, err := addDurations("Heartbeat+Tick", timer.Heartbeat, timer.Tick)
	if err != nil {
		return timingModel{}, err
	}

	model := timingModel{
		frameFactor:    factor,
		heartbeatTick:  heartbeatTick,
		maxFrameWindow: max(sendWindow, recvWindow),
		structureInputs: fmt.Sprintf("F=%d bytes, factor=%d, BaseSend=%v, BaseRecv=%v",
			limits.MaxFrameBytes, factor, timing.BaseSend, timing.BaseRecv),
	}
	left, err := addDurations("S1 left side", heartbeatTick, model.maxFrameWindow)
	if err != nil {
		return timingModel{}, err
	}
	if !timing.InProcess && left >= timer.ReelectionMin {
		return timingModel{}, fmt.Errorf(
			"%w: S1 Heartbeat+Tick+T_max(F) < ReelectionMin violated: Heartbeat %v + Tick %v + T_max(F) %v = %v"+
				" >= ReelectionMin %v (%s)",
			ErrLimit, timer.Heartbeat, timer.Tick, model.maxFrameWindow, left, timer.ReelectionMin,
			model.structureInputs)
	}

	return model, nil
}

func checkTransportTiming(timing contract.TransportTiming) error {
	if timing.BaseSend <= 0 || timing.BaseRecv <= 0 {
		return fmt.Errorf("%w: transport BaseSend %v and BaseRecv %v must be positive",
			ErrRange, timing.BaseSend, timing.BaseRecv)
	}
	if timing.Dial < 0 {
		return fmt.Errorf("%w: transport Dial %v must not be negative", ErrRange, timing.Dial)
	}
	if timing.Dial == 0 && !timing.InProcess {
		return fmt.Errorf("%w: transport Dial %v must be positive for a network transport", ErrRange, timing.Dial)
	}

	return nil
}

func checkTimerProfile(timer TimerProfile, checkQuorum time.Duration) error {
	if timer.Heartbeat <= 0 || timer.Tick <= 0 || timer.ReelectionMin <= 0 || checkQuorum <= 0 {
		return fmt.Errorf("%w: Heartbeat %v, Tick %v, ReelectionMin %v and CheckQuorum %v must be positive",
			ErrRange, timer.Heartbeat, timer.Tick, timer.ReelectionMin, checkQuorum)
	}

	return nil
}

// CheckTimingPassport — офлайн-проверка временного профиля над паспортом
// среды. После CheckTimingStructure проверяет неравенства:
// I1 t(x) <= T_min(x) на правых концах всех ступеней меньше F и в самом F;
// I2 Heartbeat + Tick + EncodeDispatchMax + T_max(F) < ReelectionMin;
// I3 Heartbeat + Tick + DialMax + t(F) < checkQuorum;
// где t(x) = EncodeDispatchMax + ⌈8·x/MinBitsPerSecond⌉ + RTTMax + PersistDecodeMax.
// Транспорт внутри процесса сетевого паспорта не имеет. Функция чистая.
func CheckTimingPassport(limits contract.Limits, timing contract.TransportTiming,
	timer TimerProfile, checkQuorum time.Duration, passport TimingPassport,
) error {
	model, err := checkTimingStructure(limits, timing, timer, checkQuorum)
	if err != nil {
		return err
	}
	if timing.InProcess {
		return fmt.Errorf("%w: network passport does not apply to an in-process transport", ErrLimit)
	}
	if err = checkPassportValues(passport); err != nil {
		return err
	}
	if passport.DialMax > timing.Dial {
		return fmt.Errorf("%w: passport DialMax %v exceeds transport Dial %v", ErrLimit, passport.DialMax, timing.Dial)
	}

	exchangeF, err := checkFrameSteps(limits.MaxFrameBytes, model.frameFactor, timing, passport)
	if err != nil {
		return err
	}
	if err = checkReelectionWithEncode(model, timer, passport); err != nil {
		return err
	}

	return checkQuorumWithDial(model, timer, checkQuorum, passport, exchangeF)
}

func checkPassportValues(passport TimingPassport) error {
	if passport.EncodeDispatchMax < 0 || passport.PersistDecodeMax < 0 || passport.RTTMax < 0 || passport.DialMax < 0 {
		return fmt.Errorf("%w: passport EncodeDispatchMax %v, PersistDecodeMax %v, RTTMax %v and DialMax %v"+
			" must not be negative", ErrRange, passport.EncodeDispatchMax, passport.PersistDecodeMax,
			passport.RTTMax, passport.DialMax)
	}
	if passport.MinBitsPerSecond == 0 {
		return fmt.Errorf("%w: passport MinBitsPerSecond must be positive", ErrRange)
	}

	return nil
}

// checkFrameSteps проверяет I1 на правых концах ступеней x_k = 24 + k·262144,
// строго меньших F, и в самом F. Возвращает t(F).
func checkFrameSteps(frameBytes, factorF uint64, timing contract.TransportTiming,
	passport TimingPassport,
) (time.Duration, error) {
	minBase := min(timing.BaseSend, timing.BaseRecv)
	for step := uint64(1); step < factorF; step++ {
		if _, err := checkFrameExchange(headerSize+step*frameStepBytes, step, minBase, passport); err != nil {
			return 0, err
		}
	}

	return checkFrameExchange(frameBytes, factorF, minBase, passport)
}

func checkFrameExchange(frameBytes, factor uint64, minBase time.Duration,
	passport TimingPassport,
) (time.Duration, error) {
	exchange, transfer, err := exchangeTime(frameBytes, passport)
	if err != nil {
		return 0, err
	}
	window, err := scaleDuration("T_min", minBase, factor)
	if err != nil {
		return 0, err
	}
	if exchange > window {
		return 0, fmt.Errorf("%w: I1 t(x) <= T_min(x) violated at x=%d bytes (factor %d): EncodeDispatchMax %v +"+
			" transfer %v at %d bit/s + RTTMax %v + PersistDecodeMax %v = %v > min(BaseSend, BaseRecv) %v x %d = %v",
			ErrLimit, frameBytes, factor, passport.EncodeDispatchMax, transfer, passport.MinBitsPerSecond,
			passport.RTTMax, passport.PersistDecodeMax, exchange, minBase, factor, window)
	}

	return exchange, nil
}

// exchangeTime вычисляет t(x) и время передачи x байт с проверкой переполнения.
func exchangeTime(frameBytes uint64, passport TimingPassport) (time.Duration, time.Duration, error) {
	transfer, err := transferDuration(frameBytes, passport.MinBitsPerSecond)
	if err != nil {
		return 0, 0, err
	}

	total := passport.EncodeDispatchMax
	for _, part := range [...]time.Duration{transfer, passport.RTTMax, passport.PersistDecodeMax} {
		if total, err = addDurations("t(x)", total, part); err != nil {
			return 0, 0, err
		}
	}

	return total, transfer, nil
}

func checkReelectionWithEncode(model timingModel, timer TimerProfile, passport TimingPassport) error {
	left, err := addDurations("I2 left side", model.heartbeatTick, passport.EncodeDispatchMax)
	if err == nil {
		left, err = addDurations("I2 left side", left, model.maxFrameWindow)
	}
	if err != nil {
		return err
	}
	if left >= timer.ReelectionMin {
		return fmt.Errorf("%w: I2 Heartbeat+Tick+EncodeDispatchMax+T_max(F) < ReelectionMin violated:"+
			" Heartbeat %v + Tick %v + EncodeDispatchMax %v + T_max(F) %v = %v >= ReelectionMin %v (%s)",
			ErrLimit, timer.Heartbeat, timer.Tick, passport.EncodeDispatchMax, model.maxFrameWindow, left,
			timer.ReelectionMin, model.structureInputs)
	}

	return nil
}

func checkQuorumWithDial(model timingModel, timer TimerProfile, checkQuorum time.Duration,
	passport TimingPassport, exchangeF time.Duration,
) error {
	left, err := addDurations("I3 left side", model.heartbeatTick, passport.DialMax)
	if err == nil {
		left, err = addDurations("I3 left side", left, exchangeF)
	}
	if err != nil {
		return err
	}
	if left >= checkQuorum {
		return fmt.Errorf("%w: I3 Heartbeat+Tick+DialMax+t(F) < CheckQuorum violated:"+
			" Heartbeat %v + Tick %v + DialMax %v + t(F) %v = %v >= CheckQuorum %v (%s, %d bit/s)",
			ErrLimit, timer.Heartbeat, timer.Tick, passport.DialMax, exchangeF, left, checkQuorum,
			model.structureInputs, passport.MinBitsPerSecond)
	}

	return nil
}

// frameFactor — g(x) = max(1, ⌈(x−24)/262144⌉) для кадра x байт.
func frameFactor(frameBytes uint64) uint64 {
	body := frameBytes - min(frameBytes, headerSize)
	factor := body / frameStepBytes
	if body%frameStepBytes != 0 {
		factor++
	}

	return max(factor, 1)
}

// scaleDuration умножает положительный base на factor с проверкой
// переполнения до умножения.
func scaleDuration(name string, base time.Duration, factor uint64) (time.Duration, error) {
	if factor > uint64(math.MaxInt64/base) {
		return 0, fmt.Errorf("%w: %s %v x factor %d overflows time.Duration", ErrRange, name, base, factor)
	}

	return base * time.Duration(factor), nil
}

// addDurations складывает неотрицательные длительности с проверкой
// переполнения.
func addDurations(name string, left, right time.Duration) (time.Duration, error) {
	if right > math.MaxInt64-left {
		return 0, fmt.Errorf("%w: %s %v + %v overflows time.Duration", ErrRange, name, left, right)
	}

	return left + right, nil
}

// transferDuration вычисляет ⌈8·x·10⁹ / bitsPerSecond⌉ нс целочисленно:
// 128-битное произведение не ограничивает представимый результат.
func transferDuration(frameBytes, bitsPerSecond uint64) (time.Duration, error) {
	high, low := bits.Mul64(frameBytes, bitsPerByteNanos)
	if high >= bitsPerSecond {
		return 0, transferOverflow(frameBytes, bitsPerSecond)
	}

	quotient, remainder := bits.Div64(high, low, bitsPerSecond)
	if remainder != 0 {
		if quotient == math.MaxUint64 {
			return 0, transferOverflow(frameBytes, bitsPerSecond)
		}
		quotient++
	}
	if quotient > math.MaxInt64 {
		return 0, transferOverflow(frameBytes, bitsPerSecond)
	}

	return time.Duration(quotient), nil
}

func transferOverflow(frameBytes, bitsPerSecond uint64) error {
	return fmt.Errorf("%w: transfer of %d bytes at %d bit/s overflows time.Duration", ErrRange, frameBytes, bitsPerSecond)
}
