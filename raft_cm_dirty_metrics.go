package raft

import (
	"math"
	"math/bits"
	"slices"
	"time"
)

// Коды групп происхождения грязного периода. Закрытый набор: первая отметка
// периода попадает ровно в одну группу, повторные отметки внутри открытого
// периода её не меняют. Порядок кодов — порядок публикации групп.
type dirtyCause int

const (
	// dirtyCauseLeaderAppend — добавление пакета записей лидером.
	dirtyCauseLeaderAppend dirtyCause = iota
	// dirtyCauseFollowerAppend — добавление или замена суффикса ведомым.
	dirtyCauseFollowerAppend
	// dirtyCauseCompact — уплотнение журнала без добавления записей.
	dirtyCauseCompact
	// dirtyCauseInstallSnapshot — установка снимка без добавления записей.
	dirtyCauseInstallSnapshot
	// dirtyCauseInitial — служебный начальный период нового узла;
	// отменяется при восстановлении готового хранилища.
	dirtyCauseInitial
)

// dirtyCauseCount — число групп происхождения.
const dirtyCauseCount = int(dirtyCauseInitial) + 1

// Имена групп: единственное место сопоставления коду строки, попадающей
// в отчёт; те же имена используют проверки документа.
const (
	_dirtyCauseLeaderAppendName    = "leader_append"
	_dirtyCauseFollowerAppendName  = "follower_append"
	_dirtyCauseCompactName         = "compact"
	_dirtyCauseInstallSnapshotName = "install_snapshot"
	_dirtyCauseInitialName         = "initial"
)

// dirtyCauseNames — коды групп в порядке публикации; позиция совпадает
// со значением dirtyCause.
var dirtyCauseNames = [dirtyCauseCount]string{
	_dirtyCauseLeaderAppendName,
	_dirtyCauseFollowerAppendName,
	_dirtyCauseCompactName,
	_dirtyCauseInstallSnapshotName,
	_dirtyCauseInitialName,
}

// _dirtyBucketCount — число корзин гистограммы добавлений и возраста:
// корзина нуля и диапазоны [2^k, 2^(k+1)-1] для k = 0…63. Последняя
// корзина ограничена MaxUint64.
const _dirtyBucketCount = 65

// dirtyBucket возвращает номер корзины значения v: 0 для нуля, иначе
// k+1, где k — номер старшего установленного бита (диапазон [2^k, 2^(k+1)-1]).
// Чистая функция без выделений памяти.
func dirtyBucket(v uint64) int {
	if v == 0 {
		return 0
	}
	return 64 - bits.LeadingZeros64(v)
}

// dirtyHistogram — фиксированная гистограмма фиксированного размера:
// 65 корзин по 8 байт. Сумма корзин равна числу наблюдений; при переполнении
// отдельной корзины признак непригодности возвращается вызывающему,
// тихого обнуления нет.
type dirtyHistogram [_dirtyBucketCount]uint64

// add увеличивает корзину значения v и возвращает признак переполнения
// корзины. Значение корзины насыщается на MaxUint64.
func (h *dirtyHistogram) add(v uint64) (overflow bool) {
	idx := dirtyBucket(v)
	if h[idx] == math.MaxUint64 {
		return true
	}
	h[idx]++
	return false
}

// addSaturating складывает неотрицательные счётчики с насыщением на
// MaxInt64: переполнение не обнуляет ряд, а взводит признак непригодности.
func addSaturating(a, b int64, overflow *bool) int64 {
	if b <= 0 {
		return a
	}
	if a > math.MaxInt64-b {
		*overflow = true
		return math.MaxInt64
	}
	return a + b
}

// dirtyCauseStats — накопительные итоги завершённых периодов одной группы
// происхождения: число, суммы, минимум и максимум возраста, ожидания,
// отметок и добавлений. Минимум и максимум действительны при periods > 0.
type dirtyCauseStats struct {
	periods      int64
	sumAgeNS     int64
	minAgeNS     int64
	maxAgeNS     int64
	sumWaitNS    int64
	minWaitNS    int64
	maxWaitNS    int64
	sumMarks     int64
	minMarks     int64
	maxMarks     int64
	sumAdditions int64
	minAdditions int64
	maxAdditions int64
}

// dirtyMetrics — накопительные наблюдения грязных периодов журнала.
// Принадлежат CM, переживают смену роли и не сохраняются на диск.
// Все писатели сериализованы удерживаемым cm.mu: отдельного мьютекса,
// атомиков и выделений памяти на инкременте нет. Нулевое значение готово
// к использованию, в том числе для CM, собранных напрямую в приспособлениях.
type dirtyMetrics struct {
	// open — признак незавершённого периода; firstMark — первая монотонная
	// отметка, firstCause — её группа, marks — число отметок (первая и
	// повторные), additions — накопленные добавления открытого периода.
	open       bool
	firstMark  time.Time
	firstCause dirtyCause
	marks      int64
	additions  int64

	// causes — итоги завершённых периодов по группам происхождения.
	causes [dirtyCauseCount]dirtyCauseStats

	// Гистограммы завершённых периодов: добавления и возраст всех
	// атрибутированных периодов и отдельно только периода initial.
	// Точное исключение initial выполняется вычитанием корзин.
	additionsAll dirtyHistogram
	agesAll      dirtyHistogram
	initialAdds  dirtyHistogram
	initialAges  dirtyHistogram

	// unattributed — число полных сохранений журнала, у которых не было
	// первой отметки: приспособление выставило флаг напрямую. Возраст
	// такого наблюдения не подменяется нулём, наблюдение не входит
	// ни в группы, ни в гистограммы.
	unattributed int64

	// overflow — признак переполнения суммы или счётчика: ряд непригоден.
	overflow bool
}

// mark отмечает изменение журнала текущим моментом. Повторная отметка
// внутри открытого периода увеличивает число отметок и копит добавления,
// но не перезаписывает момент первой отметки и её группу.
// Требует удержания cm.mu либо однопоточного конструирования до публикации CM.
func (m *dirtyMetrics) mark(cause dirtyCause, additions int64) {
	m.markAt(cause, additions, time.Now())
}

// markAt — внутренний помощник с передаваемым моментом: арифметика времени
// проверяется в тестах детерминированно, без ожиданий и подмены часов.
// Контракт удержания совпадает с mark.
func (m *dirtyMetrics) markAt(cause dirtyCause, additions int64, at time.Time) {
	if !m.open {
		m.open = true
		m.firstMark = at
		m.firstCause = cause
		m.marks = 1
		m.additions = additions
		return
	}
	m.marks = addSaturating(m.marks, 1, &m.overflow)
	m.additions = addSaturating(m.additions, additions, &m.overflow)
}

// cancelInitial отменяет служебный начальный период при восстановлении
// готового хранилища: это не завершение периода и не наблюдение
// «сохранено» — начальное состояние узла уже было долговечным.
// Контракт удержания совпадает с mark.
func (m *dirtyMetrics) cancelInitial() {
	if !m.open || m.firstCause != dirtyCauseInitial {
		return
	}
	m.open = false
	m.firstMark = time.Time{}
	m.firstCause = 0
	m.marks = 0
	m.additions = 0
}

// completeAt закрывает завершённый полный период после успешного Set(log).
// start — момент входа в сохранение (до него считается ожидание), end —
// момент успешного завершения записи журнала (до него считается возраст).
// Число отметок и добавлений периода учитывается ровно один раз. Если
// отметки не было, наблюдение получает dirty_unattributed: прежнее
// поведение хранилища сохраняется, нулевой возраст не подставляется.
// Требует удержания cm.mu — мьютекса владельца набора.
func (m *dirtyMetrics) completeAt(start, end time.Time) {
	if !m.open {
		m.unattributed = addSaturating(m.unattributed, 1, &m.overflow)
		return
	}
	ageNS := end.Sub(m.firstMark).Nanoseconds()
	waitNS := start.Sub(m.firstMark).Nanoseconds()
	if ageNS < 0 {
		ageNS = 0
	}
	if waitNS < 0 {
		waitNS = 0
	}

	stats := &m.causes[m.firstCause]
	stats.periods = addSaturating(stats.periods, 1, &m.overflow)
	stats.sumAgeNS = addSaturating(stats.sumAgeNS, ageNS, &m.overflow)
	stats.sumWaitNS = addSaturating(stats.sumWaitNS, waitNS, &m.overflow)
	stats.sumMarks = addSaturating(stats.sumMarks, m.marks, &m.overflow)
	stats.sumAdditions = addSaturating(stats.sumAdditions, m.additions, &m.overflow)
	if stats.periods == 1 {
		stats.minAgeNS, stats.maxAgeNS = ageNS, ageNS
		stats.minWaitNS, stats.maxWaitNS = waitNS, waitNS
		stats.minMarks, stats.maxMarks = m.marks, m.marks
		stats.minAdditions, stats.maxAdditions = m.additions, m.additions
	} else {
		stats.minAgeNS = min(stats.minAgeNS, ageNS)
		stats.maxAgeNS = max(stats.maxAgeNS, ageNS)
		stats.minWaitNS = min(stats.minWaitNS, waitNS)
		stats.maxWaitNS = max(stats.maxWaitNS, waitNS)
		stats.minMarks = min(stats.minMarks, m.marks)
		stats.maxMarks = max(stats.maxMarks, m.marks)
		stats.minAdditions = min(stats.minAdditions, m.additions)
		stats.maxAdditions = max(stats.maxAdditions, m.additions)
	}
	if m.additionsAll.add(uint64(m.additions)) {
		m.overflow = true
	}
	if m.agesAll.add(uint64(ageNS)) {
		m.overflow = true
	}
	if m.firstCause == dirtyCauseInitial {
		if m.initialAdds.add(uint64(m.additions)) {
			m.overflow = true
		}
		if m.initialAges.add(uint64(ageNS)) {
			m.overflow = true
		}
	}

	m.open = false
	m.firstMark = time.Time{}
	m.firstCause = 0
	m.marks = 0
	m.additions = 0
}

// dirtyActiveSnapshot — открытый (незавершённый) период на момент снимка:
// публикуется отдельно и не входит в гистограммы завершений.
type dirtyActiveSnapshot struct {
	present   bool
	cause     dirtyCause
	marks     int64
	additions int64
	age       time.Duration
}

// dirtySnapshot — согласованная копия набора для одного выпуска отчёта:
// только скаляры и массивы фиксированного размера, ссылок на изменяемые
// данные CM нет. Массивы копируются значением и не разделяют память
// с живым набором.
type dirtySnapshot struct {
	causes       [dirtyCauseCount]dirtyCauseStats
	active       dirtyActiveSnapshot
	additionsAll dirtyHistogram
	agesAll      dirtyHistogram
	initialAdds  dirtyHistogram
	initialAges  dirtyHistogram
	unattributed int64
	overflow     bool
}

// snapshot копирует набор в снимок отчёта; незавершённый период получает
// текущий возраст от первой отметки до момента снимка. Снимок не входит
// в гистограммы завершений и не выдаётся за нулевой.
// Требует удержания cm.mu — мьютекса владельца набора.
func (m *dirtyMetrics) snapshot(now time.Time) dirtySnapshot {
	snap := dirtySnapshot{
		causes:       m.causes,
		additionsAll: m.additionsAll,
		agesAll:      m.agesAll,
		initialAdds:  m.initialAdds,
		initialAges:  m.initialAges,
		unattributed: m.unattributed,
		overflow:     m.overflow,
	}
	if m.open {
		age := now.Sub(m.firstMark)
		if age < 0 {
			age = 0
		}
		snap.active = dirtyActiveSnapshot{
			present:   true,
			cause:     m.firstCause,
			marks:     m.marks,
			additions: m.additions,
			age:       age,
		}
	}
	return snap
}

// statsDirtyCauseV1 — агрегат завершённых периодов одной группы в документе
// PersistV2: число, суммы и границы возраста, ожидания, отметок и добавлений.
// Повторные отметки равны сумме отметок минус число периодов. Границы null,
// пока периодов группы не было: отсутствие наблюдения не подменяется нулём.
type statsDirtyCauseV1 struct {
	Cause         string `json:"Cause"`
	Periods       int64  `json:"Periods"`
	SumMarks      int64  `json:"SumMarks"`
	RepeatedMarks int64  `json:"RepeatedMarks"`
	SumAdditions  int64  `json:"SumAdditions"`
	MinAdditions  *int64 `json:"MinAdditions"`
	MaxAdditions  *int64 `json:"MaxAdditions"`
	MinMarks      *int64 `json:"MinMarks"`
	MaxMarks      *int64 `json:"MaxMarks"`
	SumAgeNs      int64  `json:"SumAgeNs"`
	MinAgeNs      *int64 `json:"MinAgeNs"`
	MaxAgeNs      *int64 `json:"MaxAgeNs"`
	SumWaitNs     int64  `json:"SumWaitNs"`
	MinWaitNs     *int64 `json:"MinWaitNs"`
	MaxWaitNs     *int64 `json:"MaxWaitNs"`
}

// statsDirtyActiveV1 — незавершённый период на момент снимка: первая
// причина, отметки (включая повторные), добавления и текущий возраст.
type statsDirtyActiveV1 struct {
	Cause         string `json:"Cause"`
	Marks         int64  `json:"Marks"`
	RepeatedMarks int64  `json:"RepeatedMarks"`
	Additions     int64  `json:"Additions"`
	AgeNs         int64  `json:"AgeNs"`
}

// statsDirtyV1 — машиночитаемая группа грязных периодов документа PersistV2:
// агрегаты пяти групп происхождения, незавершённый период, четыре
// 65-корзинные гистограммы (добавления и возраст всех атрибутированных
// периодов и отдельно initial), число необъяснённых полных сохранений
// и признак переполнения. Суммы накопительные и не сбрасываются по роли
// или тику; дельты считаются офлайн по соседним публикациям.
type statsDirtyV1 struct {
	Causes                  []statsDirtyCauseV1 `json:"Causes"`
	Active                  *statsDirtyActiveV1 `json:"Active"`
	AdditionsBuckets        []uint64            `json:"AdditionsBuckets"`
	AgeBuckets              []uint64            `json:"AgeBuckets"`
	InitialAdditionsBuckets []uint64            `json:"InitialAdditionsBuckets"`
	InitialAgeBuckets       []uint64            `json:"InitialAgeBuckets"`
	Unattributed            int64               `json:"Unattributed"`
	Overflow                bool                `json:"Overflow"`
}

// repeatedMarks возвращает число повторных отметок завершённых периодов
// (сумма отметок минус число периодов). Каждый период начинается отметкой,
// поэтому результат неотрицателен; защитная граница исключает вычитание
// из неполного ряда.
func repeatedMarks(sumMarks, periods int64) int64 {
	if sumMarks <= periods {
		return 0
	}
	return sumMarks - periods
}

// document собирает JSON-представление группы грязных периодов из снимка.
// Функция чистая: работает с собственной копией и не читает живой CM;
// массивы корзин клонируются и не разделяют память со снимком.
func (s *dirtySnapshot) document() *statsDirtyV1 {
	causes := make([]statsDirtyCauseV1, 0, dirtyCauseCount)
	for i := range s.causes {
		stats := s.causes[i]
		entry := statsDirtyCauseV1{
			Cause:         dirtyCauseNames[i],
			Periods:       stats.periods,
			SumMarks:      stats.sumMarks,
			RepeatedMarks: repeatedMarks(stats.sumMarks, stats.periods),
			SumAdditions:  stats.sumAdditions,
			SumAgeNs:      stats.sumAgeNS,
			SumWaitNs:     stats.sumWaitNS,
		}
		if stats.periods > 0 {
			minAdditions, maxAdditions := stats.minAdditions, stats.maxAdditions
			minMarks, maxMarks := stats.minMarks, stats.maxMarks
			minAge, maxAge := stats.minAgeNS, stats.maxAgeNS
			minWait, maxWait := stats.minWaitNS, stats.maxWaitNS
			entry.MinAdditions, entry.MaxAdditions = &minAdditions, &maxAdditions
			entry.MinMarks, entry.MaxMarks = &minMarks, &maxMarks
			entry.MinAgeNs, entry.MaxAgeNs = &minAge, &maxAge
			entry.MinWaitNs, entry.MaxWaitNs = &minWait, &maxWait
		}
		causes = append(causes, entry)
	}
	doc := &statsDirtyV1{
		Causes:                  causes,
		AdditionsBuckets:        slices.Clone(s.additionsAll[:]),
		AgeBuckets:              slices.Clone(s.agesAll[:]),
		InitialAdditionsBuckets: slices.Clone(s.initialAdds[:]),
		InitialAgeBuckets:       slices.Clone(s.initialAges[:]),
		Unattributed:            s.unattributed,
		Overflow:                s.overflow,
	}
	if s.active.present {
		doc.Active = &statsDirtyActiveV1{
			Cause:         dirtyCauseNames[s.active.cause],
			Marks:         s.active.marks,
			RepeatedMarks: repeatedMarks(s.active.marks, 1),
			Additions:     s.active.additions,
			AgeNs:         s.active.age.Nanoseconds(),
		}
	}
	return doc
}
