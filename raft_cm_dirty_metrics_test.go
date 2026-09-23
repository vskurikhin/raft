package raft

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"testing"
	"time"

	"github.com/vskurikhin/raft/pkg/raft/store"
)

// -- Детерминированные тесты набора грязных периодов: арифметика времени
// -- через передаваемые значения внутреннего помощника, границы корзин,
// -- переполнение, разделение initial и нулевое значение. Без ожиданий
// -- и без подмены часов: все моменты задаются тестом явно. --

// dirtySnapshotOf снимает копию набора грязных периодов под cm.mu — тем же
// контрактом, что и секундный отчёт.
func dirtySnapshotOf(cm *ConsensusModule) dirtySnapshot {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.dirty.snapshot(time.Now())
}

// dirtyCauseOf возвращает агрегат одной группы происхождения из снимка.
func dirtyCauseOf(s dirtySnapshot, cause dirtyCause) dirtyCauseStats {
	return s.causes[cause]
}

// dirtyBaseTime — опорный момент для детерминированной арифметики времени.
var dirtyBaseTime = time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)

// TestDirtyHistogram_BucketBoundaries проверяет границы корзин: ноль,
// степени двойки, соседние значения и верхнюю границу MaxUint64.
func TestDirtyHistogram_BucketBoundaries(t *testing.T) {
	cases := []struct {
		value  uint64
		bucket int
	}{
		{value: 0, bucket: 0},
		{value: 1, bucket: 1},
		{value: 2, bucket: 2},
		{value: 3, bucket: 2},
		{value: 4, bucket: 3},
		{value: 7, bucket: 3},
		{value: 8, bucket: 4},
		{value: 1 << 10, bucket: 11},
		{value: 1<<10 - 1, bucket: 10},
		{value: uint64(1) << 62, bucket: 63},
		{value: uint64(1)<<63 - 1, bucket: 63},
		{value: uint64(1) << 63, bucket: 64},
		{value: math.MaxUint64, bucket: 64},
	}
	for _, tc := range cases {
		if got := dirtyBucket(tc.value); got != tc.bucket {
			t.Fatalf("dirtyBucket(%d) = %d, want %d", tc.value, got, tc.bucket)
		}
	}

	// Сумма корзин равна числу наблюдений на наборе типовых значений.
	values := []uint64{0, 1, 2, 3, 4, 7, 8, 1 << 10, math.MaxUint64}
	var h dirtyHistogram
	for _, v := range values {
		if h.add(v) {
			t.Fatalf("неожиданное переполнение при добавлении %d", v)
		}
	}
	var sum uint64
	for _, count := range h {
		sum += count
	}
	if sum != uint64(len(values)) {
		t.Fatalf("сумма корзин = %d, want %d", sum, len(values))
	}
}

// TestDirtyHistogram_OverflowSaturates проверяет, что переполнение корзины
// отмечается признаком, а значение не обнуляется по кругу.
func TestDirtyHistogram_OverflowSaturates(t *testing.T) {
	var h dirtyHistogram
	idx := dirtyBucket(7)
	h[idx] = math.MaxUint64
	if !h.add(7) {
		t.Fatal("переполнение корзины не отмечено")
	}
	if h[idx] != math.MaxUint64 {
		t.Fatalf("значение корзины завернулось: %d", h[idx])
	}

	var overflow bool
	if got := addSaturating(math.MaxInt64-1, 2, &overflow); got != math.MaxInt64 || !overflow {
		t.Fatalf("addSaturating = (%d, %v), want (%d, true)", got, overflow, int64(math.MaxInt64))
	}
	overflow = false
	if got := addSaturating(3, 4, &overflow); got != 7 || overflow {
		t.Fatalf("addSaturating = (%d, %v), want (7, false)", got, overflow)
	}
}

// TestDirtyMetrics_RepeatedMarkKeepsFirst проверяет, что повторная отметка
// не сдвигает начало периода и не меняет его первую причину, но увеличивает
// число отметок и копит добавления. Возраст и ожидание считаются от первой
// отметки; завершение учитывается ровно один раз.
func TestDirtyMetrics_RepeatedMarkKeepsFirst(t *testing.T) {
	var m dirtyMetrics
	m.markAt(dirtyCauseLeaderAppend, 2, dirtyBaseTime)
	m.markAt(dirtyCauseFollowerAppend, 3, dirtyBaseTime.Add(time.Millisecond))
	m.completeAt(dirtyBaseTime.Add(10*time.Millisecond), dirtyBaseTime.Add(30*time.Millisecond))

	leader := m.causes[dirtyCauseLeaderAppend]
	if leader.periods != 1 || leader.sumMarks != 2 || repeatedMarks(leader.sumMarks, leader.periods) != 1 {
		t.Fatalf("leader_append: periods=%d, marks=%d, repeated=%d, want (1, 2, 1)",
			leader.periods, leader.sumMarks, repeatedMarks(leader.sumMarks, leader.periods))
	}
	if leader.sumAdditions != 5 || leader.minAdditions != 5 || leader.maxAdditions != 5 {
		t.Fatalf("leader_append добавления = (sum %d, min %d, max %d), want (5, 5, 5)",
			leader.sumAdditions, leader.minAdditions, leader.maxAdditions)
	}
	if leader.sumAgeNS != int64(30*time.Millisecond) || leader.minAgeNS != int64(30*time.Millisecond) {
		t.Fatalf("leader_append возраст = (sum %d, min %d), want 30ms",
			leader.sumAgeNS, leader.minAgeNS)
	}
	if leader.sumWaitNS != int64(10*time.Millisecond) {
		t.Fatalf("leader_append ожидание = %d, want 10ms", leader.sumWaitNS)
	}
	if follower := m.causes[dirtyCauseFollowerAppend]; follower.periods != 0 {
		t.Fatalf("вторая отметка создала отдельный период: periods=%d", follower.periods)
	}
	if m.open {
		t.Fatal("период остался открытым после завершения")
	}

	// Повторное завершение без новой отметки — необъяснённое наблюдение,
	// а не повторный учёт того же периода.
	m.completeAt(dirtyBaseTime, dirtyBaseTime.Add(time.Millisecond))
	if m.unattributed != 1 {
		t.Fatalf("unattributed = %d, want 1", m.unattributed)
	}
	if got := m.causes[dirtyCauseLeaderAppend]; got.periods != 1 || got.sumAdditions != 5 {
		t.Fatalf("повторное завершение переучло период: %+v", got)
	}
}

// TestDirtyMetrics_InitialSeparate проверяет разделение начального периода:
// его итоги попадают в отдельную группу и в отдельные гистограммы, а разность
// корзин даёт обычные периоды без дополнительного учёта.
func TestDirtyMetrics_InitialSeparate(t *testing.T) {
	var m dirtyMetrics
	m.markAt(dirtyCauseInitial, 0, dirtyBaseTime)
	m.completeAt(dirtyBaseTime, dirtyBaseTime.Add(time.Second))
	m.markAt(dirtyCauseLeaderAppend, 4, dirtyBaseTime)
	m.completeAt(dirtyBaseTime, dirtyBaseTime.Add(2*time.Second))

	if got := m.causes[dirtyCauseInitial]; got.periods != 1 || got.sumAdditions != 0 || got.sumMarks != 1 {
		t.Fatalf("initial = %+v, want (1, 0, 1)", got)
	}
	var allCount, initialCount uint64
	for _, count := range m.additionsAll {
		allCount += count
	}
	for _, count := range m.initialAdds {
		initialCount += count
	}
	if allCount != 2 || initialCount != 1 {
		t.Fatalf("гистограммы добавлений: all=%d, initial=%d, want (2, 1)", allCount, initialCount)
	}
	// Разность корзин: обычные периоды дают ровно одно наблюдение с 4 добавлениями.
	ordinary := m.additionsAll
	for i := range ordinary {
		if ordinary[i] < m.initialAdds[i] {
			t.Fatalf("вычитание корзин %d дало отрицательное значение", i)
		}
		ordinary[i] -= m.initialAdds[i]
	}
	if ordinary[dirtyBucket(4)] != 1 || ordinary[dirtyBucket(0)] != 0 {
		t.Fatalf("разность корзин добавлений = %v, want одно наблюдение в корзине 4", ordinary)
	}
}

// TestDirtyMetrics_ActiveNotInHistograms проверяет, что незавершённый период
// публикуется с текущим возрастом и не попадает в гистограммы завершений.
func TestDirtyMetrics_ActiveNotInHistograms(t *testing.T) {
	var m dirtyMetrics
	m.markAt(dirtyCauseCompact, 0, dirtyBaseTime)
	m.markAt(dirtyCauseCompact, 0, dirtyBaseTime.Add(5*time.Millisecond))

	snap := m.snapshot(dirtyBaseTime.Add(20 * time.Millisecond))
	if !snap.active.present || snap.active.cause != dirtyCauseCompact {
		t.Fatalf("активный период = %+v, want compact", snap.active)
	}
	if snap.active.marks != 2 || snap.active.additions != 0 {
		t.Fatalf("активный период = (marks %d, additions %d), want (2, 0)",
			snap.active.marks, snap.active.additions)
	}
	if snap.active.age != 20*time.Millisecond {
		t.Fatalf("возраст активного периода = %s, want 20ms", snap.active.age)
	}
	if snap.causes[dirtyCauseCompact].periods != 0 {
		t.Fatal("незавершённый период попал в завершённые")
	}
	for _, count := range snap.additionsAll {
		if count != 0 {
			t.Fatal("незавершённый период попал в гистограмму добавлений")
		}
	}
}

// TestDirtyMetrics_SnapshotIndependent проверяет, что снимок владеет своими
// массивами: последующие отметки и завершения не меняют снятую копию.
func TestDirtyMetrics_SnapshotIndependent(t *testing.T) {
	var m dirtyMetrics
	m.markAt(dirtyCauseLeaderAppend, 3, dirtyBaseTime)
	m.completeAt(dirtyBaseTime, dirtyBaseTime.Add(7*time.Millisecond))
	before := m.snapshot(dirtyBaseTime.Add(10 * time.Millisecond))

	m.markAt(dirtyCauseFollowerAppend, 9, dirtyBaseTime)
	m.completeAt(dirtyBaseTime, dirtyBaseTime.Add(time.Hour))

	entry := before.causes[dirtyCauseLeaderAppend]
	if entry.periods != 1 || entry.sumAdditions != 3 || entry.sumAgeNS != int64(7*time.Millisecond) {
		t.Fatalf("снимок изменился вместе с набором: %+v", entry)
	}
	if before.agesAll[dirtyBucket(uint64(7*time.Millisecond))] != 1 {
		t.Fatal("снимок потерял корзину возраста")
	}
}

// TestDirtyMetrics_OverflowFlag проверяет, что переполнение суммы или корзины
// взводит признак непригодности ряда и не обнуляет накопленные значения.
func TestDirtyMetrics_OverflowFlag(t *testing.T) {
	var m dirtyMetrics
	m.additionsAll[dirtyBucket(7)] = math.MaxUint64
	m.markAt(dirtyCauseCompact, 7, dirtyBaseTime)
	m.completeAt(dirtyBaseTime, dirtyBaseTime.Add(time.Millisecond))
	if !m.overflow {
		t.Fatal("переполнение корзины не взвело признак")
	}
	snap := m.snapshot(dirtyBaseTime)
	if !snap.overflow {
		t.Fatal("признак переполнения потерян в снимке")
	}
	if got := snap.causes[dirtyCauseCompact]; got.periods != 1 || got.sumAdditions != 7 {
		t.Fatalf("переполнение обнулило агрегат: %+v", got)
	}
}

// TestDirtyMetrics_ZeroValueSafe проверяет, что нулевое значение готово
// к использованию приспособлениями: завершение без отметки, снимок, отмена
// initial и документ не паникуют и не выдают наблюдение за персист.
func TestDirtyMetrics_ZeroValueSafe(t *testing.T) {
	var m dirtyMetrics
	m.cancelInitial()
	m.completeAt(dirtyBaseTime, dirtyBaseTime.Add(time.Millisecond))
	if m.unattributed != 1 {
		t.Fatalf("unattributed = %d, want 1", m.unattributed)
	}
	snap := m.snapshot(dirtyBaseTime)
	if snap.active.present {
		t.Fatal("нулевой набор выдал активный период")
	}
	doc := snap.document()
	if doc.Unattributed != 1 || doc.Overflow {
		t.Fatalf("документ нулевого набора = %+v", doc)
	}
	for i, cause := range doc.Causes {
		if cause.Cause != dirtyCauseNames[i] {
			t.Fatalf("группа %d = %q, want %q", i, cause.Cause, dirtyCauseNames[i])
		}
		if cause.MinAgeNs != nil || cause.MinAdditions != nil || cause.MinMarks != nil {
			t.Fatalf("отсутствие наблюдений выдано значениями: %+v", cause)
		}
	}
}

// TestDirtyMetrics_NoAllocations проверяет, что отметка и закрытие периода
// не выделяют память: это обычные инкременты полей под уже удерживаемым
// мьютексом.
func TestDirtyMetrics_NoAllocations(t *testing.T) {
	var m dirtyMetrics
	allocs := testing.AllocsPerRun(1000, func() {
		m.markAt(dirtyCauseLeaderAppend, 1, dirtyBaseTime)
		m.completeAt(dirtyBaseTime, dirtyBaseTime.Add(time.Microsecond))
	})
	if allocs != 0 {
		t.Fatalf("аллокаций на период = %v, want 0", allocs)
	}
}

// TestDirtyDocument_SchemaAndRoundTrip проверяет финальную схему группы
// грязных периодов: пять групп в фиксированном порядке, четыре массива
// по 65 корзин, null-границы при отсутствии наблюдений, активный период
// и корректный round-trip JSON.
func TestDirtyDocument_SchemaAndRoundTrip(t *testing.T) {
	var m dirtyMetrics
	m.markAt(dirtyCauseFollowerAppend, 5, dirtyBaseTime)
	m.markAt(dirtyCauseCompact, 0, dirtyBaseTime.Add(time.Millisecond))
	m.completeAt(dirtyBaseTime.Add(2*time.Millisecond), dirtyBaseTime.Add(time.Second))
	m.markAt(dirtyCauseLeaderAppend, 2, dirtyBaseTime)

	snap := m.snapshot(dirtyBaseTime.Add(10 * time.Millisecond))
	doc := snap.document()
	if len(doc.Causes) != dirtyCauseCount {
		t.Fatalf("групп %d, want %d", len(doc.Causes), dirtyCauseCount)
	}
	for i, cause := range doc.Causes {
		if cause.Cause != dirtyCauseNames[i] {
			t.Fatalf("группа %d = %q, want %q", i, cause.Cause, dirtyCauseNames[i])
		}
	}
	follower := doc.Causes[dirtyCauseFollowerAppend]
	if follower.Periods != 1 || follower.SumMarks != 2 || follower.RepeatedMarks != 1 {
		t.Fatalf("follower_append = %+v, want (1, 2, 1)", follower)
	}
	if follower.SumAdditions != 5 || follower.MinAdditions == nil || *follower.MinAdditions != 5 {
		t.Fatalf("follower_append добавления = %+v, want 5", follower)
	}
	if follower.SumAgeNs != int64(time.Second) || follower.SumWaitNs != int64(2*time.Millisecond) {
		t.Fatalf("follower_append время = (age %d, wait %d)", follower.SumAgeNs, follower.SumWaitNs)
	}
	if compact := doc.Causes[dirtyCauseCompact]; compact.Periods != 0 || compact.MinAdditions != nil {
		t.Fatalf("compact без завершений = %+v", compact)
	}
	if doc.Active == nil || doc.Active.Cause != _dirtyCauseLeaderAppendName || doc.Active.AgeNs != int64(10*time.Millisecond) {
		t.Fatalf("активный период = %+v, want leader_append 10ms", doc.Active)
	}
	for name, buckets := range map[string][]uint64{
		"AdditionsBuckets":        doc.AdditionsBuckets,
		"AgeBuckets":              doc.AgeBuckets,
		"InitialAdditionsBuckets": doc.InitialAdditionsBuckets,
		"InitialAgeBuckets":       doc.InitialAgeBuckets,
	} {
		if len(buckets) != _dirtyBucketCount {
			t.Fatalf("%s: корзин %d, want %d", name, len(buckets), _dirtyBucketCount)
		}
	}

	data, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	var decoded statsDirtyV1
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if len(decoded.Causes) != dirtyCauseCount || len(decoded.AdditionsBuckets) != _dirtyBucketCount {
		t.Fatalf("round-trip потерял структуру: %+v", decoded)
	}
	if decoded.Causes[dirtyCauseFollowerAppend].RepeatedMarks != 1 ||
		decoded.Active == nil || decoded.Active.Cause != _dirtyCauseLeaderAppendName {
		t.Fatalf("round-trip потерял поля: %+v", decoded)
	}
	if decoded.Unattributed != 0 || decoded.Overflow {
		t.Fatalf("round-trip исказил признаки: %+v", decoded)
	}
}

// TestDirtyDocument_UnattributedVsZeroAge проверяет, что необъяснённое полное
// сохранение отличимо от возраста ноль: отсутствие наблюдения публикуется
// null, незавершённый период с нулевым возрастом — активным периодом
// с причиной, а счётчик unattributed растёт отдельно.
func TestDirtyDocument_UnattributedVsZeroAge(t *testing.T) {
	var m dirtyMetrics
	m.completeAt(dirtyBaseTime, dirtyBaseTime.Add(time.Millisecond))
	snap := m.snapshot(dirtyBaseTime)
	doc := snap.document()
	if doc.Unattributed != 1 {
		t.Fatalf("Unattributed = %d, want 1", doc.Unattributed)
	}
	for i, cause := range doc.Causes {
		if cause.MinAgeNs != nil || cause.MaxAgeNs != nil {
			t.Fatalf("группа %d выдала нулевой возраст вместо отсутствия наблюдения: %+v", i, cause)
		}
	}

	m.markAt(dirtyCauseInitial, 0, dirtyBaseTime)
	activeSnap := m.snapshot(dirtyBaseTime)
	active := activeSnap.document()
	if active.Active == nil {
		t.Fatal("открытый период не опубликован как активный")
	}
	if active.Active.Cause != _dirtyCauseInitialName || active.Active.AgeNs != 0 {
		t.Fatalf("активный период = %+v, want initial с возрастом 0", active.Active)
	}
	if active.Unattributed != 1 {
		t.Fatalf("Unattributed = %d, want 1 (не смешивается с активным периодом)", active.Unattributed)
	}
}

// TestDirtyDocument_InPersistV2 проверяет интеграцию группы грязных периодов
// в документ PersistV2: агрегаты попадают в третью строку, повторная
// публикация не сбрасывает накопительные суммы, а незавершённый период
// публикуется активным.
func TestDirtyDocument_InPersistV2(t *testing.T) {
	cm := newStatsTestCM()
	cm.mu.Lock()
	cm.dirty.markAt(dirtyCauseLeaderAppend, 3, dirtyBaseTime)
	cm.dirty.completeAt(dirtyBaseTime.Add(5*time.Millisecond), dirtyBaseTime.Add(20*time.Millisecond))
	cm.dirty.markAt(dirtyCauseCompact, 0, dirtyBaseTime)
	cm.mu.Unlock()

	var out bytes.Buffer
	cm.publishStats(&out, io.Discard)
	doc := statsDecodePersistV2(t, statsThirdLineBody(t, out.String()))
	if doc.Dirty != _statsGroupAvailable || doc.DirtyPeriods == nil {
		t.Fatalf("Dirty = %q, DirtyPeriods = %v, want ok и документ", doc.Dirty, doc.DirtyPeriods)
	}
	leader := doc.DirtyPeriods.Causes[dirtyCauseLeaderAppend]
	if leader.Periods != 1 || leader.SumAdditions != 3 || leader.SumAgeNs != int64(20*time.Millisecond) ||
		leader.SumWaitNs != int64(5*time.Millisecond) {
		t.Fatalf("leader_append в PersistV2 = %+v", leader)
	}
	if doc.DirtyPeriods.Active == nil || doc.DirtyPeriods.Active.Cause != _dirtyCauseCompactName {
		t.Fatalf("активный период в PersistV2 = %+v, want compact", doc.DirtyPeriods.Active)
	}

	// Публикация не сбрасывает накопительные суммы и не завершает
	// открытый период: смена тика не является наблюдением.
	var out2 bytes.Buffer
	cm.publishStats(&out2, io.Discard)
	second := statsDecodePersistV2(t, statsThirdLineBody(t, out2.String()))
	if second.DirtyPeriods == nil {
		t.Fatal("вторая публикация потеряла группу грязных периодов")
	}
	leader2 := second.DirtyPeriods.Causes[dirtyCauseLeaderAppend]
	if leader2.Periods != 1 || leader2.SumAdditions != 3 {
		t.Fatalf("повторная публикация переучла период: %+v", leader2)
	}
	if second.DirtyPeriods.Active == nil || second.DirtyPeriods.Active.Cause != _dirtyCauseCompactName {
		t.Fatalf("повторная публикация потеряла активный период: %+v", second.DirtyPeriods.Active)
	}
}

// TestDirtyDocument_PersistLineLimitAtExtremes проверяет предел 16 KiB на
// предельных значениях: документ PersistV2 с максимальными матрицей и
// грязными агрегатами остаётся корректным JSON и укладывается в лимит.
func TestDirtyDocument_PersistLineLimitAtExtremes(t *testing.T) {
	var persist persistenceSnapshot
	persist.logLenSum = math.MaxInt64
	persist.logBytesWrittenSum = math.MaxInt64
	persist.logWrites = math.MaxInt64
	persist.conflictSuffixReplacements = math.MaxInt64
	persist.logLenMin = math.MaxInt
	persist.logLenMax = math.MaxInt
	persist.logLenObserved = true
	for i := range persist.cells {
		persist.cells[i] = persistCell{
			logCalls:        math.MaxInt64,
			logElapsedNS:    math.MaxInt64,
			scalarCalls:     math.MaxInt64,
			scalarElapsedNS: math.MaxInt64,
		}
	}

	var dirty dirtySnapshot
	for i := range dirty.causes {
		dirty.causes[i] = dirtyCauseStats{
			periods:      math.MaxInt64,
			sumAgeNS:     math.MaxInt64,
			minAgeNS:     math.MaxInt64,
			maxAgeNS:     math.MaxInt64,
			sumWaitNS:    math.MaxInt64,
			minWaitNS:    math.MaxInt64,
			maxWaitNS:    math.MaxInt64,
			sumMarks:     math.MaxInt64,
			minMarks:     math.MaxInt64,
			maxMarks:     math.MaxInt64,
			sumAdditions: math.MaxInt64,
			minAdditions: math.MaxInt64,
			maxAdditions: math.MaxInt64,
		}
	}
	for i := range dirty.additionsAll {
		dirty.additionsAll[i] = math.MaxUint64
		dirty.agesAll[i] = math.MaxUint64
		dirty.initialAdds[i] = math.MaxUint64
		dirty.initialAges[i] = math.MaxUint64
	}
	dirty.unattributed = math.MaxInt64
	dirty.overflow = true
	dirty.active = dirtyActiveSnapshot{
		present:   true,
		cause:     dirtyCauseInitial,
		marks:     math.MaxInt64,
		additions: math.MaxInt64,
		age:       time.Duration(math.MaxInt64),
	}

	snap := statsSnapshot{
		at:       dirtyBaseTime,
		age:      time.Duration(math.MaxInt64),
		instance: math.MaxUint64,
		role:     Leader,
		id:       math.MaxInt,
		term:     math.MaxInt,
		persist:  persist,
		dirty:    dirty,
	}
	body := snap.persistReport(math.MaxUint64, nil, storageDiagnostics{}, _statsPersistLineLimit)
	if len(body) > _statsPersistLineLimit {
		t.Fatalf("тело PersistV2 %d байт, предел %d", len(body), _statsPersistLineLimit)
	}
	doc := statsDecodePersistV2(t, body)
	if doc.Schema != 2 || doc.Dirty != _statsGroupAvailable || doc.DirtyPeriods == nil {
		t.Fatalf("предельный документ потерял обязательные поля: %+v", doc)
	}
	if !doc.DirtyPeriods.Overflow || doc.DirtyPeriods.Active == nil {
		t.Fatalf("предельный документ потерял признаки: %+v", doc.DirtyPeriods)
	}
}

// TestDirtyMetrics_DisabledOutputStillAccumulates проверяет, что при
// stats-output=false грязные периоды, добавления и гистограммы продолжают
// накапливаться: выключен только вывод, наблюдение не прекращается.
func TestDirtyMetrics_DisabledOutputStillAccumulates(t *testing.T) {
	storage := store.NewMapStorage()
	storage.Set(_storageKeyCurrentTerm, gobEncode(t, 1))
	storage.Set(_storageKeyVotedFor, gobEncode(t, -1))
	storage.RewriteLog([]LogEntry{})
	storage.Set(_storageKeyLastSnapshotIndex, gobEncode(t, -1))
	storage.Set(_storageKeyLastSnapshotTerm, gobEncode(t, -1))

	cm := &ConsensusModule{storage: storage, disableStatsOutput: true}
	cm.cmState.lastSnapshotIndex = -1
	cm.cmState.lastSnapshotTerm = -1

	cm.mu.Lock()
	cm.dirty.markAt(dirtyCauseLeaderAppend, 2, dirtyBaseTime)
	cm.markLogSuffixDirtyLocked(0)
	cm.persistToStorageLocked(persistSourceTest)
	cm.mu.Unlock()

	before := dirtySnapshotOf(cm)
	leader := dirtyCauseOf(before, dirtyCauseLeaderAppend)
	if leader.periods != 1 || leader.sumAdditions != 2 {
		t.Fatalf("при выключенном выводе период = %+v, want (1, 2)", leader)
	}
	if before.additionsAll[dirtyBucket(2)] != 1 {
		t.Fatal("при выключенном выводе гистограмма добавлений не заполнена")
	}

	sink := &countingWriter{}
	cm.publishStats(sink, sink)
	if sink.calls != 0 {
		t.Fatalf("выключенный вывод выполнил %d записей, want 0", sink.calls)
	}
	after := dirtySnapshotOf(cm)
	if after != before {
		t.Fatalf("публикация изменила накопленные наблюдения:\nbefore = %+v\nafter  = %+v", before, after)
	}
}
