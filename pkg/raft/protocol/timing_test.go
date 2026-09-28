package protocol

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/vskurikhin/raft/pkg/raft/contract"
)

const ms = time.Millisecond

var (
	tcpTiming     = contract.TransportTiming{BaseSend: 200 * ms, BaseRecv: 200 * ms, Dial: 165 * ms}
	inmemTiming   = contract.TransportTiming{BaseSend: 500 * ms, BaseRecv: 500 * ms, InProcess: true}
	defaultTimers = TimerProfile{Heartbeat: 33 * ms, Tick: 20 * ms, ReelectionMin: 430 * ms}
	checkQuorum   = 400 * ms
	// basePassport — условный паспорт расчёта: b_min = 2·10⁸ бит/с,
	// E = 10 мс, RTT = 2 мс, P = 100 мс, DialMax = 165 мс.
	basePassport = TimingPassport{
		EncodeDispatchMax: 10 * ms, PersistDecodeMax: 100 * ms, RTTMax: 2 * ms, DialMax: 165 * ms,
		MinBitsPerSecond: 200_000_000,
	}
)

func limitsWithFrame(frameBytes uint64) contract.Limits {
	return contract.Limits{MaxFrameBytes: frameBytes, MaxEntries: 31, MaxDataBytes: 8192, MaxConfigurationBytes: 2560}
}

func requireMessage(t *testing.T, err error, parts ...string) {
	t.Helper()

	for _, part := range parts {
		if !strings.Contains(err.Error(), part) {
			t.Fatalf("error %q does not contain %q", err, part)
		}
	}
}

// TestTimingStructureDefaults — S1 на действующих умолчаниях: 33+20+200 =
// 253 < 430; равенство отвергается, на 1 нс больше — проходит.
func TestTimingStructureDefaults(t *testing.T) {
	if err := CheckTimingStructure(DefaultLimits(), tcpTiming, defaultTimers, checkQuorum); err != nil {
		t.Fatalf("defaults: %v", err)
	}

	equal := defaultTimers
	equal.ReelectionMin = 253 * ms
	err := CheckTimingStructure(DefaultLimits(), tcpTiming, equal, checkQuorum)
	requireIs(t, err, ErrLimit)
	requireMessage(t, err, "S1", "Heartbeat 33ms", "Tick 20ms", "T_max(F) 200ms", "= 253ms", "ReelectionMin 253ms",
		"F=262144 bytes")

	equal.ReelectionMin += time.Nanosecond
	if err = CheckTimingStructure(DefaultLimits(), tcpTiming, equal, checkQuorum); err != nil {
		t.Fatalf("253ms+1ns: %v", err)
	}
}

// TestTimingStructureStep — обе стороны ступени: F=262168 — factor 1,
// F=262169 — factor 2 (453 >= 430).
func TestTimingStructureStep(t *testing.T) {
	if err := CheckTimingStructure(limitsWithFrame(262168), tcpTiming, defaultTimers, checkQuorum); err != nil {
		t.Fatalf("F=262168: %v", err)
	}
	err := CheckTimingStructure(limitsWithFrame(262169), tcpTiming, defaultTimers, checkQuorum)
	requireIs(t, err, ErrLimit)
	requireMessage(t, err, "= 453ms", "factor=2")

	longer := defaultTimers
	longer.ReelectionMin = 454 * ms
	if err = CheckTimingStructure(limitsWithFrame(262169), tcpTiming, longer, checkQuorum); err != nil {
		t.Fatalf("F=262169 R=454ms: %v", err)
	}
}

// TestTimingStructureAsymmetricBase — T_max берётся по большему из
// BaseSend/BaseRecv в обоих порядках.
func TestTimingStructureAsymmetricBase(t *testing.T) {
	for _, timing := range []contract.TransportTiming{
		{BaseSend: 100 * ms, BaseRecv: 300 * ms, Dial: 165 * ms},
		{BaseSend: 300 * ms, BaseRecv: 100 * ms, Dial: 165 * ms},
	} {
		timers := defaultTimers
		timers.ReelectionMin = 353 * ms
		requireIs(t, CheckTimingStructure(DefaultLimits(), timing, timers, checkQuorum), ErrLimit)
		timers.ReelectionMin = 354 * ms
		if err := CheckTimingStructure(DefaultLimits(), timing, timers, checkQuorum); err != nil {
			t.Fatalf("%+v: %v", timing, err)
		}
	}
}

// TestTimingStructureInProcess — OBS-1: транспорт внутри процесса (500/500,
// Dial=0) принимается без S1; положительность и переполнение проверяются.
func TestTimingStructureInProcess(t *testing.T) {
	if err := CheckTimingStructure(DefaultLimits(), inmemTiming, defaultTimers, checkQuorum); err != nil {
		t.Fatalf("in-process: %v", err)
	}
	networkWithSameWindows := inmemTiming
	networkWithSameWindows.InProcess = false
	networkWithSameWindows.Dial = 165 * ms
	requireIs(t, CheckTimingStructure(DefaultLimits(), networkWithSameWindows, defaultTimers, checkQuorum), ErrLimit)

	overflow := inmemTiming
	overflow.BaseSend = math.MaxInt64/2 + 1
	requireIs(t, CheckTimingStructure(limitsWithFrame(262169), overflow, defaultTimers, checkQuorum), ErrRange)
	requireIs(t, CheckTimingStructure(DefaultLimits(), inmemTiming, TimerProfile{}, checkQuorum), ErrRange)
}

// TestTimingStructureRejects — OBS-1 негативы и нулевые/смешанные
// конфигурации: ошибки, а не нормализация.
func TestTimingStructureRejects(t *testing.T) {
	cases := []struct {
		name    string
		limits  contract.Limits
		timing  contract.TransportTiming
		timers  TimerProfile
		quorum  time.Duration
		want    error
		message string
	}{
		{"zero limits", contract.Limits{}, tcpTiming, defaultTimers, checkQuorum, ErrLimit, ""},
		{"partial limits", contract.Limits{MaxFrameBytes: 262144}, tcpTiming, defaultTimers, checkQuorum, ErrLimit, ""},
		{"zero timing", DefaultLimits(), contract.TransportTiming{}, defaultTimers, checkQuorum, ErrRange, "BaseSend"},
		{"BaseSend zero", DefaultLimits(), contract.TransportTiming{BaseRecv: ms, Dial: ms}, defaultTimers,
			checkQuorum, ErrRange, "BaseSend 0s"},
		{"BaseRecv negative", DefaultLimits(), contract.TransportTiming{BaseSend: ms, BaseRecv: -ms, Dial: ms},
			defaultTimers, checkQuorum, ErrRange, "BaseRecv -1ms"},
		{"Dial negative network", DefaultLimits(), contract.TransportTiming{BaseSend: ms, BaseRecv: ms, Dial: -1},
			defaultTimers, checkQuorum, ErrRange, "Dial -1ns"},
		{"Dial negative in-process", DefaultLimits(), contract.TransportTiming{
			BaseSend: ms, BaseRecv: ms, Dial: -1, InProcess: true,
		}, defaultTimers, checkQuorum, ErrRange, "Dial"},
		{"Dial zero network", DefaultLimits(), contract.TransportTiming{BaseSend: ms, BaseRecv: ms},
			defaultTimers, checkQuorum, ErrRange, "network transport"},
		{"zero timers", DefaultLimits(), tcpTiming, TimerProfile{}, checkQuorum, ErrRange, "Heartbeat 0s"},
		{"Tick zero", DefaultLimits(), tcpTiming, TimerProfile{Heartbeat: ms, ReelectionMin: time.Second},
			checkQuorum, ErrRange, "Tick 0s"},
		{"CheckQuorum zero", DefaultLimits(), tcpTiming, defaultTimers, 0, ErrRange, "CheckQuorum 0s"},
		{"Heartbeat+Tick overflow", DefaultLimits(), tcpTiming, TimerProfile{
			Heartbeat: math.MaxInt64, Tick: 1, ReelectionMin: time.Second,
		}, checkQuorum, ErrRange, "overflows"},
		{"base x factor overflow", limitsWithFrame(262169), contract.TransportTiming{
			BaseSend: math.MaxInt64/2 + 1, BaseRecv: ms, Dial: ms,
		}, defaultTimers, checkQuorum, ErrRange, "BaseSend"},
		{"S1 sum overflow", DefaultLimits(), contract.TransportTiming{
			BaseSend: math.MaxInt64 - 40*ms, BaseRecv: ms, Dial: ms,
		}, defaultTimers, checkQuorum, ErrRange, "S1 left side"},
	}
	for _, tc := range cases {
		err := CheckTimingStructure(tc.limits, tc.timing, tc.timers, tc.quorum)
		requireIs(t, err, tc.want)
		if tc.message != "" {
			requireMessage(t, err, tc.message)
		}
	}
}

// TestTimingPassportBase — паспорт расчёта проходит: I1 t(F)=122.48576 мс
// <= 200 мс, I2 263 < 430, I3 340.48576 < 400.
func TestTimingPassportBase(t *testing.T) {
	if err := CheckTimingPassport(DefaultLimits(), tcpTiming, defaultTimers, checkQuorum, basePassport); err != nil {
		t.Fatalf("base passport: %v", err)
	}
	transfer, err := transferDuration(262144, basePassport.MinBitsPerSecond)
	if err != nil || transfer != 10485760*time.Nanosecond {
		t.Fatalf("8F/b = %v, %v", transfer, err)
	}
}

// TestTimingPassportI1Equality — I1 использует <=: равенство проходит,
// на 1 нс больше — ErrLimit с именем условия и входами.
func TestTimingPassportI1Equality(t *testing.T) {
	passport := TimingPassport{
		EncodeDispatchMax: 10 * ms, RTTMax: 2 * ms, DialMax: 100 * ms, MinBitsPerSecond: 8_000_000_000,
	}
	passport.PersistDecodeMax = 200*ms - 12*ms - 262144*time.Nanosecond
	if err := CheckTimingPassport(DefaultLimits(), tcpTiming, defaultTimers, checkQuorum, passport); err != nil {
		t.Fatalf("I1 equality: %v", err)
	}
	passport.PersistDecodeMax++
	err := CheckTimingPassport(DefaultLimits(), tcpTiming, defaultTimers, checkQuorum, passport)
	requireIs(t, err, ErrLimit)
	requireMessage(t, err, "I1", "x=262144 bytes", "= 200.000001ms", "8000000000 bit/s", "min(BaseSend, BaseRecv) 200ms")
}

// TestTimingPassportStepBoundary — I1 на обеих сторонах ступени 262168/262169
// и нарушение ранней ступени при допустимом F.
func TestTimingPassportStepBoundary(t *testing.T) {
	passport := TimingPassport{DialMax: 100 * ms, MinBitsPerSecond: 8_000_000_000}
	passport.PersistDecodeMax = 200*ms - 262168*time.Nanosecond
	timers := defaultTimers
	timers.ReelectionMin = time.Second
	quorum := 2 * time.Second

	for _, frame := range []uint64{262168, 262169} {
		if err := CheckTimingPassport(limitsWithFrame(frame), tcpTiming, timers, quorum, passport); err != nil {
			t.Fatalf("F=%d at equality: %v", frame, err)
		}
	}

	passport.PersistDecodeMax++
	err := CheckTimingPassport(limitsWithFrame(262168), tcpTiming, timers, quorum, passport)
	requireIs(t, err, ErrLimit)
	requireMessage(t, err, "x=262168 bytes (factor 1)")

	err = CheckTimingPassport(limitsWithFrame(262169), tcpTiming, timers, quorum, passport)
	requireIs(t, err, ErrLimit)
	requireMessage(t, err, "x=262168 bytes (factor 1)")
	if _, err = checkFrameExchange(262169, 2, 200*ms, passport); err != nil {
		t.Fatalf("F=262169 itself must pass: %v", err)
	}
}

// TestTimingPassportCounterexamples — контрпримеры SA: профиль B (factor 2,
// 20 Мбит/с) отвергается на x=262168 при допустимом F; профиль A (factor 3,
// 12 Мбит/с) отвергается по I3.
func TestTimingPassportCounterexamples(t *testing.T) {
	timers := defaultTimers
	timers.ReelectionMin = 2 * time.Second

	profileB := basePassport
	profileB.MinBitsPerSecond = 20_000_000
	err := CheckTimingPassport(limitsWithFrame(24+2*frameStepBytes), tcpTiming, timers, 2*time.Second, profileB)
	requireIs(t, err, ErrLimit)
	requireMessage(t, err, "I1", "x=262168 bytes")
	if _, err = checkFrameExchange(24+2*frameStepBytes, 2, 200*ms, profileB); err != nil {
		t.Fatalf("profile B F alone must pass: %v", err)
	}

	profileA := basePassport
	profileA.MinBitsPerSecond = 12_000_000
	slow := contract.TransportTiming{BaseSend: 300 * ms, BaseRecv: 300 * ms, Dial: 165 * ms}
	err = CheckTimingPassport(limitsWithFrame(24+3*frameStepBytes), slow, timers, checkQuorum, profileA)
	requireIs(t, err, ErrLimit)
	requireMessage(t, err, "I3", "CheckQuorum 400ms")
}

// TestTimingPassportStrictInequalities — I2 и I3 используют строгое <.
func TestTimingPassportStrictInequalities(t *testing.T) {
	timers := defaultTimers
	timers.ReelectionMin = 263 * ms
	err := CheckTimingPassport(DefaultLimits(), tcpTiming, timers, checkQuorum, basePassport)
	requireIs(t, err, ErrLimit)
	requireMessage(t, err, "I2", "= 263ms", "ReelectionMin 263ms")
	timers.ReelectionMin += time.Nanosecond
	if err = CheckTimingPassport(DefaultLimits(), tcpTiming, timers, checkQuorum, basePassport); err != nil {
		t.Fatalf("I2 +1ns: %v", err)
	}

	quorum := 33*ms + 20*ms + 165*ms + 122485760*time.Nanosecond
	err = CheckTimingPassport(DefaultLimits(), tcpTiming, defaultTimers, quorum, basePassport)
	requireIs(t, err, ErrLimit)
	requireMessage(t, err, "I3", "t(F) 122.48576ms", "= 340.48576ms")
	if err = CheckTimingPassport(DefaultLimits(), tcpTiming, defaultTimers, quorum+1, basePassport); err != nil {
		t.Fatalf("I3 +1ns: %v", err)
	}
}

// TestTimingPassportRejects — паспорт к транспорту внутри процесса не
// применяется; отрицательные длительности и нулевая полоса — ErrRange;
// DialMax выше Dial — ErrLimit; ошибки структуры возвращаются как есть.
func TestTimingPassportRejects(t *testing.T) {
	err := CheckTimingPassport(DefaultLimits(), inmemTiming, defaultTimers, checkQuorum, basePassport)
	requireIs(t, err, ErrLimit)
	requireMessage(t, err, "in-process")

	negative := []TimingPassport{
		{EncodeDispatchMax: -1, MinBitsPerSecond: 1},
		{PersistDecodeMax: -1, MinBitsPerSecond: 1},
		{RTTMax: -1, MinBitsPerSecond: 1},
		{DialMax: -1, MinBitsPerSecond: 1},
		{},
	}
	for _, passport := range negative {
		requireIs(t, CheckTimingPassport(DefaultLimits(), tcpTiming, defaultTimers, checkQuorum, passport), ErrRange)
	}

	dial := basePassport
	dial.DialMax = tcpTiming.Dial + 1
	err = CheckTimingPassport(DefaultLimits(), tcpTiming, defaultTimers, checkQuorum, dial)
	requireIs(t, err, ErrLimit)
	requireMessage(t, err, "DialMax")
	dial.DialMax = tcpTiming.Dial
	if err = CheckTimingPassport(DefaultLimits(), tcpTiming, defaultTimers, checkQuorum, dial); err != nil {
		t.Fatalf("DialMax == Dial: %v", err)
	}

	zeroDurations := TimingPassport{MinBitsPerSecond: 1_000_000_000}
	if err = CheckTimingPassport(DefaultLimits(), tcpTiming, defaultTimers, checkQuorum, zeroDurations); err != nil {
		t.Fatalf("zero idealized durations: %v", err)
	}

	requireIs(t, CheckTimingPassport(contract.Limits{}, tcpTiming, defaultTimers, checkQuorum, basePassport), ErrLimit)
	requireIs(t, CheckTimingPassport(DefaultLimits(), contract.TransportTiming{}, defaultTimers, checkQuorum,
		basePassport), ErrRange)
}

// TestTimingPassportOverflow — переполнение времени передачи и сумм — ErrRange.
func TestTimingPassportOverflow(t *testing.T) {
	slowest := basePassport
	slowest.MinBitsPerSecond = 1
	slowest.PersistDecodeMax = math.MaxInt64 - time.Second
	requireIs(t, CheckTimingPassport(DefaultLimits(), tcpTiming, defaultTimers, checkQuorum, slowest), ErrRange)

	_, err := transferDuration(math.MaxInt64, 1)
	requireIs(t, err, ErrRange)
	_, err = transferDuration(math.MaxUint64, 8)
	requireIs(t, err, ErrRange)

	dial := basePassport
	dial.DialMax = math.MaxInt64 - 10*ms
	huge := contract.TransportTiming{BaseSend: 200 * ms, BaseRecv: 200 * ms, Dial: math.MaxInt64}
	err = CheckTimingPassport(DefaultLimits(), huge, defaultTimers, checkQuorum, dial)
	requireIs(t, err, ErrRange)
	requireMessage(t, err, "I3 left side")
}

// TestTransferDuration — ⌈8·x·10⁹/b⌉ целочисленно: точное деление, округление
// вверх и большое промежуточное произведение с представимым результатом.
func TestTransferDuration(t *testing.T) {
	cases := []struct {
		frameBytes uint64
		bits       uint64
		want       time.Duration
	}{
		{1000, 8_000_000_000, 1000},
		{1, 3, 2666666667},
		{262144, 200_000_000, 10485760},
		{262168, 20_000_000, 104867200},
		{1 << 40, 1_000_000_000_000_000, 8796094},
		{math.MaxInt64 / 8, 8 * uint64(time.Second) * 8, math.MaxInt64/8/8 + 1},
	}
	for _, tc := range cases {
		got, err := transferDuration(tc.frameBytes, tc.bits)
		if err != nil || got != tc.want {
			t.Fatalf("transfer(%d, %d) = %d, %v; want %d", tc.frameBytes, tc.bits, got, err, tc.want)
		}
	}
}

// TestTimingPassportLargeFrame — большое промежуточное произведение 8·F·10⁹
// за пределами 64 бит при представимом результате; все ступени до F.
func TestTimingPassportLargeFrame(t *testing.T) {
	if math.MaxInt < 1<<34 {
		t.Skip("F = 2^34 is not representable in int on this platform")
	}
	limits := limitsWithFrame(1 << 34)
	timing := contract.TransportTiming{BaseSend: 10 * ms, BaseRecv: 10 * ms, Dial: 165 * ms}
	timers := TimerProfile{Heartbeat: 33 * ms, Tick: 20 * ms, ReelectionMin: 700 * time.Second}
	passport := TimingPassport{
		EncodeDispatchMax: ms, PersistDecodeMax: ms, RTTMax: ms, DialMax: 165 * ms,
		MinBitsPerSecond: 1_000_000_000_000_000,
	}
	if err := CheckTimingPassport(limits, timing, timers, checkQuorum, passport); err != nil {
		t.Fatalf("large frame: %v", err)
	}
	quorum := 33*ms + 20*ms + 165*ms + 3*ms + 137439*time.Nanosecond
	err := CheckTimingPassport(limits, timing, timers, quorum, passport)
	requireIs(t, err, ErrLimit)
	requireMessage(t, err, "t(F) 3.137439ms")
}

// TestFrameFactor — g(x) = max(1, ⌈(x−24)/262144⌉).
func TestFrameFactor(t *testing.T) {
	cases := map[uint64]uint64{24: 1, 25: 1, 4128: 1, 262144: 1, 262168: 1, 262169: 2, 524312: 2, 524313: 3}
	for frame, want := range cases {
		if got := frameFactor(frame); got != want {
			t.Fatalf("frameFactor(%d) = %d, want %d", frame, got, want)
		}
	}
}
