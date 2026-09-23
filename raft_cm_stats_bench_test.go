package raft

import (
	"io"
	"os"
	"testing"
	"time"
)

// -- Узкие бенчмарки измерителя: путь трёх периодических строк целиком
// -- (снимок, сортировка, форматирование, запись) на одном и том же
// -- приспособлении. Бенчмарки персиста секундную статистику не вызывают,
// -- поэтому её стоимость измеряется здесь отдельно. Пара Enabled/Disabled
// -- различается только выбором вывода: сбор и сброс латентности выполняются
// -- в обоих режимах, приспособление и приёмник совпадают. --

// statsBenchSink — приёмник бенчмарка: пишет в /dev/null и считает вызовы
// и байты строк. Для enabled и disabled используется один и тот же тип,
// поэтому различие сводится к стоимости форматирования и записи.
type statsBenchSink struct {
	file   *os.File
	writes int
	bytes  int
}

// Write записывает строку в /dev/null и учитывает её размер.
func (s *statsBenchSink) Write(p []byte) (int, error) {
	n, err := s.file.Write(p)
	s.writes++
	s.bytes += n
	return n, err
}

// newStatsBenchSink открывает приёмник бенчмарка вне измеряемого цикла.
func newStatsBenchSink(b *testing.B) *statsBenchSink {
	b.Helper()
	file, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		b.Fatalf("открытие приёмника %s: %v", os.DevNull, err)
	}
	b.Cleanup(func() { _ = file.Close() })
	return &statsBenchSink{file: file}
}

// newStatsBenchCM собирает лидера с двумя соседями, наполненной матрицей
// сохранений и грязными периодами — типовой объём трёх строк на живой узел.
// Приспособление детерминировано; различается только выбор вывода.
func newStatsBenchCM(disabled bool) *ConsensusModule {
	cm := newStatsTestCM()
	cm.disableStatsOutput = disabled
	for _, source := range []persistSource{persistSourceApply, persistSourceAEFinish, persistSourceAECommit} {
		cm.persistence.observe(source, true, time.Millisecond, 100, 4096, 1)
		cm.persistence.observe(source, false, 500*time.Microsecond, 0, 0, 0)
	}
	base := time.Now().Add(-time.Second)
	cm.dirty.markAt(dirtyCauseLeaderAppend, 16, base)
	cm.dirty.completeAt(base.Add(100*time.Millisecond), base.Add(300*time.Millisecond))
	cm.dirty.markAt(dirtyCauseFollowerAppend, 4, base)
	cm.dirty.completeAt(base.Add(50*time.Millisecond), base.Add(150*time.Millisecond))
	cm.dirty.markAt(dirtyCauseInitial, 0, base)
	cm.dirty.completeAt(base, base.Add(time.Second))
	return cm
}

// benchStatsPublish измеряет один выпуск периодического отчёта: снимок под
// cm.mu, сброс латентности, при разрешённом выводе — сортировку, форматирование
// и запись трёх строк; при выключенном — только снимок и сброс.
func benchStatsPublish(b *testing.B, disabled bool) {
	cm := newStatsBenchCM(disabled)
	sink := newStatsBenchSink(b)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cm.publishStats(sink, io.Discard)
	}
	b.StopTimer()
	b.ReportMetric(float64(sink.bytes)/float64(b.N), "bytes/op")
	b.ReportMetric(float64(sink.writes)/float64(b.N), "lines/op")
}

// BenchmarkStatsPublishEnabled измеряет выпуск с разрешённым выводом
// (stats-output=true): полный путь трёх строк, включая запись в приёмник.
func BenchmarkStatsPublishEnabled(b *testing.B) {
	benchStatsPublish(b, false)
}

// BenchmarkStatsPublishDisabled измеряет выпуск с выключенным выводом
// (stats-output=false): снимок и сброс выполняются, форматирование и запись
// пропускаются. Сравнение с Enabled даёт цену форматирования и stdout.
func BenchmarkStatsPublishDisabled(b *testing.B) {
	benchStatsPublish(b, true)
}
