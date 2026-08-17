package raft

import (
	"fmt"
	"sync/atomic"
	"time"
)

// latencyMetric — один агрегат латентности: сумма замеров в микросекундах и
// их количество. Нулевое значение готово к использованию (важно для CM,
// собираемых литералом в тестах, — INV-M6).
type latencyMetric struct {
	sumUs atomic.Int64 // сумма замеров в микросекундах за текущее окно
	count atomic.Int64 // число замеров за текущее окно
}

// observe регистрирует один замер. Никогда не блокируется, не захватывает
// мьютексов, корректна из любого контекста, включая удержание cm.mu (INV-M1,
// INV-M3). Порядок двух Add — нормативный (ADR-P06-002 rev. 3): на него
// опирается доказательство корректности снятия агрегата в meanMsAndReset.
func (m *latencyMetric) observe(d time.Duration) {
	m.sumUs.Add(d.Microseconds())
	m.count.Add(1)
}

// meanMsAndReset возвращает среднее в миллисекундах за прошедшее окно и
// обнуляет агрегат. Вызывается только из stats (единственный читатель).
//
// НОРМАТИВНО (ADR-P06-002 rev. 3, INV-M4):
//  1. счётчик снимается ПЕРВЫМ, сумма — ВТОРОЙ (обратный порядок к observe);
//  2. если счётчик оказался нулевым, а сумма — нет, сумма возвращается
//     в следующее окно (carry-back), где её встретит «отставший» счётчик.
func (m *latencyMetric) meanMsAndReset() float64 {
	c := m.count.Swap(0) // (1) сначала счётчик
	s := m.sumUs.Swap(0) // (2) затем сумма
	if c == 0 {
		if s != 0 {
			m.sumUs.Add(s) // carry-back: замер не теряется
		}
		return 0
	}
	return float64(s) / float64(c) / 1000.0
}

// cmLatency — набор агрегатов латентности одного ConsensusModule.
// Собственного мьютекса нет: поля — atomic.Int64 (INV-M3).
type cmLatency struct {
	appendEntries, fsmApply, election, handleFsmSnapshot, installSnapshot,
	processLogs, requestPreVote, requestVote, sendBatch, takeSnapshot,
	timeoutNowRequest latencyMetric
}

// latencyReport — снимок агрегатов одного окна в миллисекундах.
// Возвращается по значению, принимается только по указателю (ADR-P06-006:
// 11 × float64 = 88 B > порог gocritic:hugeParam).
type latencyReport struct {
	appendEntries, fsmApply, election, handleFsmSnapshot, installSnapshot,
	processLogs, requestPreVote, requestVote, sendBatch, takeSnapshot,
	timeoutNowRequest float64
}

// snapshotAndReset снимает средние всех 11 агрегатов за прошедшее окно
// и обнуляет их. Вызывается только из stats (раз в секунду).
func (l *cmLatency) snapshotAndReset() latencyReport {
	return latencyReport{
		appendEntries:     l.appendEntries.meanMsAndReset(),
		fsmApply:          l.fsmApply.meanMsAndReset(),
		election:          l.election.meanMsAndReset(),
		handleFsmSnapshot: l.handleFsmSnapshot.meanMsAndReset(),
		installSnapshot:   l.installSnapshot.meanMsAndReset(),
		processLogs:       l.processLogs.meanMsAndReset(),
		requestPreVote:    l.requestPreVote.meanMsAndReset(),
		requestVote:       l.requestVote.meanMsAndReset(),
		sendBatch:         l.sendBatch.meanMsAndReset(),
		takeSnapshot:      l.takeSnapshot.meanMsAndReset(),
		timeoutNowRequest: l.timeoutNowRequest.meanMsAndReset(),
	}
}

// format форматирует снимок агрегатов в одну строку отчёта. Чистая функция:
// не читает глобалей и полей CM, не логирует (ADR-P06-005). Набор, порядок
// и имена колонок, а также формат %5.2fms сохраняются прежними; собственной
// метки времени и префикса [state,N:id,T:term] нет — их добавляют логгер
// трассировки и traceLogf (ADR-P06-003). Приём только по указателю —
// ADR-P06-006 (88 B > порог gocritic:hugeParam).
func (r *latencyReport) format() string {
	return fmt.Sprintf(
		"AE=%5.2fms, BatchingFSM=%5.2fms, Election=%5.2fms, FSMSnapSh=%5.2fms,"+
			" InstSnapShot=%5.2fms, ProcessLog=%5.2fms, RqPVt=%5.2fms, RqVote=%5.2fms,"+
			" SendBatch=%5.2fms, TakeSnapshot=%5.2fms, TmOutNowRq=%5.2fms",
		r.appendEntries, r.fsmApply, r.election, r.handleFsmSnapshot,
		r.installSnapshot, r.processLogs, r.requestPreVote, r.requestVote,
		r.sendBatch, r.takeSnapshot, r.timeoutNowRequest,
	)
}

func (cm *ConsensusModule) stats(shutdownCh chan struct{}) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-shutdownCh:
			return
		case <-ticker.C:
			// Снятие агрегатов — безусловно, чтобы окно агрегации оставалось
			// «за секунду» и при выключенном выводе (INV-M5).
			rep := cm.latency.snapshotAndReset()
			// Формирование строки и вывод — только при включённой трассировке:
			// гейт уровня до мёртвой работы (ADR-P06-003). stdoutTracePrintln сам
			// добавляет префикс [state,N:id,T:term] и захватывает cm.mu;
			// stats мьютекс не удерживает.
			if traceEnabled(0) {
				cm.stdoutTracePrintln(rep.format())
			}
		}
	}
}

func Mean(latencies []float64) (result float64) {
	if n := len(latencies); n > 0 {
		result = SumFloat(latencies) / float64(n)
	}
	return result
}

func SumFloat(slice []float64) (sum float64) {
	for _, v := range slice {
		sum += v
	}
	return sum
}
