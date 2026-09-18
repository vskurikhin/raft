package raft

import (
	"encoding/json"
	"time"
)

const (
	// _statsPersistLineLimit — предельный размер тела строки PersistV1
	// в байтах. Лимит относится только к этой строке: старую строку
	// с парами соседей он не ограничивает.
	_statsPersistLineLimit = 16 * 1024

	// _statsGroupUnavailable — маркер недоступности группы метрик.
	// Недоступность обозначается явно и не подменяется нулевыми значениями:
	// ноль наблюдаемого счётчика и отсутствие возможности — разные факты.
	_statsGroupUnavailable = "unavailable"

	// _statsGroupAvailable — маркер готовности группы метрик: данные
	// сформированы, поля опубликованы.
	_statsGroupAvailable = "ok"

	// _statsPersistOverflow — короткий маркер превышения предела строки;
	// заменяет текст ошибки, если тот не помещается в документ.
	_statsPersistOverflow = "persistV1 line exceeds 16 KiB limit"
)

// persistSource — источник вызова сохранения. Закрытый набор: одиннадцать
// производственных точек перечислены в фиксированном порядке вывода, каждая
// точка имеет собственный код. Отдельный код test предназначен только прямым
// вызовам в приспособлениях и бенчмарках и в публикуемую матрицу не входит.
type persistSource int

const (
	persistSourceApply persistSource = iota
	persistSourceFollowerTerm
	persistSourceCandidate
	persistSourceLeaderAppend
	persistSourceAEFinish
	persistSourceAETransfer
	persistSourceAECommit
	persistSourceVote
	persistSourceInstallSnapshot
	persistSourceTakeSnapshot
	persistSourceStartupRestore
	// persistSourceTest — источник прямых вызовов в приспособлениях:
	// не является производственной точкой и не попадает в публикуемую
	// матрицу; учитывается только собственной ячейкой, сохраняя стоимость
	// наблюдения.
	persistSourceTest
)

const (
	// persistSourceCount — число производственных источников, попадающих
	// в публикуемую матрицу.
	persistSourceCount = int(persistSourceTest)
	// persistSourceSlots — размер массива ячеек, включая ячейку test.
	persistSourceSlots = persistSourceCount + 1
)

// persistSourceNames — коды источников в порядке публикации; позиция
// совпадает со значением persistSource. Это единственное место сопоставления
// коду строки, попадающей в отчёт.
var persistSourceNames = [persistSourceCount]string{
	"apply",
	"follower_term",
	"candidate",
	"leader_append",
	"ae_finish",
	"ae_transfer",
	"ae_commit",
	"vote",
	"install_snapshot",
	"take_snapshot",
	"startup_restore",
}

// persistCell — итоги завершённых вызовов одного источника по двум режимам:
// полному (logNeedsPersist на входе) и скалярному. Значения — целые счётчики
// и суммы наносекунд; ссылок на изменяемые данные нет.
type persistCell struct {
	logCalls        int64
	logElapsedNS    int64
	scalarCalls     int64
	scalarElapsedNS int64
}

// persistenceMetrics — накопительные счётчики завершённых вызовов сохранения
// CM. Принадлежат CM, переживают смену роли и не сохраняются на диск.
// Все писатели сериализованы уже удерживаемым cm.mu: отдельного мьютекса,
// атомиков и выделений памяти на инкременте нет.
type persistenceMetrics struct {
	// cells — ячейки источников; ячейка test обновляется, но в публикуемую
	// матрицу не входит.
	cells [persistSourceSlots]persistCell

	// logLenSum, logLenMin, logLenMax — сумма, минимум и максимум размера
	// журнала в полном режиме. logLenObserved отличает отсутствие
	// наблюдений от минимума, равного нулю.
	logLenSum      int64
	logLenMin      int
	logLenMax      int
	logLenObserved bool

	// logBytesSum — сумма числа байт уже закодированного журнала перед
	// записью. Повторного кодирования ради статистики не выполняется.
	logBytesSum int64
}

// observe регистрирует один успешно завершённый вызов сохранения: источник,
// режим, длительность от входа до последнего необходимого Set и — в полном
// режиме — размер журнала и готовые байты кодирования. Незавершённый вызов
// наблюдения не создаёт. Наблюдение прямого источника приспособлений
// учитывается только собственной ячейкой: публикуемые размеры полного
// сохранения остаются производственными.
// Требует удержания cm.mu: писатели сериализованы вызывающим.
func (m *persistenceMetrics) observe(
	source persistSource, full bool, elapsed time.Duration, logLen, logBytes int,
) {
	cell := &m.cells[source]
	if full {
		cell.logCalls++
		cell.logElapsedNS += elapsed.Nanoseconds()
		if source == persistSourceTest {
			return
		}
		m.logLenSum += int64(logLen)
		m.logBytesSum += int64(logBytes)
		if !m.logLenObserved {
			m.logLenObserved = true
			m.logLenMin = logLen
			m.logLenMax = logLen
			return
		}
		m.logLenMin = min(m.logLenMin, logLen)
		m.logLenMax = max(m.logLenMax, logLen)
		return
	}
	cell.scalarCalls++
	cell.scalarElapsedNS += elapsed.Nanoseconds()
}

// persistenceSnapshot — согласованная копия набора для одного выпуска
// отчёта: только скаляры и массив ячеек, ссылок на изменяемые данные нет.
type persistenceSnapshot struct {
	cells          [persistSourceCount]persistCell
	logLenSum      int64
	logLenMin      int
	logLenMax      int
	logLenObserved bool
	logBytesSum    int64
}

// snapshot копирует набор в снимок отчёта; ячейка test исключается.
// Требует удержания cm.mu — мьютекса владельца набора.
func (m *persistenceMetrics) snapshot() persistenceSnapshot {
	var snap persistenceSnapshot
	copy(snap.cells[:], m.cells[:persistSourceCount])
	snap.logLenSum = m.logLenSum
	snap.logLenMin = m.logLenMin
	snap.logLenMax = m.logLenMax
	snap.logLenObserved = m.logLenObserved
	snap.logBytesSum = m.logBytesSum
	return snap
}

// statsPersistSourceV1 — счётчики одного источника в документе PersistV1:
// число полных и скалярных вызовов и суммы их длительностей.
type statsPersistSourceV1 struct {
	Source          string `json:"Source"`
	LogCalls        int64  `json:"LogCalls"`
	LogElapsedNs    int64  `json:"LogElapsedNs"`
	ScalarCalls     int64  `json:"ScalarCalls"`
	ScalarElapsedNs int64  `json:"ScalarElapsedNs"`
}

// statsPersistenceV1 — матрица сохранений CM: фиксированный порядок
// одиннадцати источников, целые количества и наносекунды. Общие суммы
// вычисляются из ячеек этой же матрицы, поэтому равенства точны в одном
// снимке. LogLenMin/LogLenMax равны null, пока не было ни одного полного
// сохранения: отсутствие наблюдения не подменяется нулём.
type statsPersistenceV1 struct {
	Sources         []statsPersistSourceV1 `json:"Sources"`
	NPersist        int64                  `json:"NPersist"`
	NLogPersist     int64                  `json:"NLogPersist"`
	NScalarOnly     int64                  `json:"NScalarOnly"`
	ElapsedNs       int64                  `json:"ElapsedNs"`
	LogElapsedNs    int64                  `json:"LogElapsedNs"`
	ScalarElapsedNs int64                  `json:"ScalarElapsedNs"`
	LogLenSum       int64                  `json:"LogLenSum"`
	LogLenMin       *int                   `json:"LogLenMin"`
	LogLenMax       *int                   `json:"LogLenMax"`
	LogBytesSum     int64                  `json:"LogBytesSum"`
}

// document собирает JSON-представление матрицы из снимка. Функция чистая:
// работает с собственной копией и не читает живой CM.
func (s *persistenceSnapshot) document() *statsPersistenceV1 {
	sources := make([]statsPersistSourceV1, 0, persistSourceCount)
	var totalLog, totalScalar, elapsedLog, elapsedScalar int64
	for i := range s.cells {
		cell := s.cells[i]
		sources = append(sources, statsPersistSourceV1{
			Source:          persistSourceNames[i],
			LogCalls:        cell.logCalls,
			LogElapsedNs:    cell.logElapsedNS,
			ScalarCalls:     cell.scalarCalls,
			ScalarElapsedNs: cell.scalarElapsedNS,
		})
		totalLog += cell.logCalls
		totalScalar += cell.scalarCalls
		elapsedLog += cell.logElapsedNS
		elapsedScalar += cell.scalarElapsedNS
	}
	doc := &statsPersistenceV1{
		Sources:         sources,
		NPersist:        totalLog + totalScalar,
		NLogPersist:     totalLog,
		NScalarOnly:     totalScalar,
		ElapsedNs:       elapsedLog + elapsedScalar,
		LogElapsedNs:    elapsedLog,
		ScalarElapsedNs: elapsedScalar,
		LogLenSum:       s.logLenSum,
		LogBytesSum:     s.logBytesSum,
	}
	if s.logLenObserved {
		minLen, maxLen := s.logLenMin, s.logLenMax
		doc.LogLenMin = &minLen
		doc.LogLenMax = &maxLen
	}
	return doc
}

// statsPersistV1 — документ третьей строки периодического отчёта: машиночитаемая
// сводка выпуска. Накопительные значения позволяют восстановить дельту через
// пропущенную строку при неизменном Instance и известных границах; смена
// Instance, повторный Seq и уменьшение сумм считаются дефектами ряда.
type statsPersistV1 struct {
	// Schema — версия схемы документа; первая версия — 1.
	Schema int `json:"Schema"`
	// Instance — идентификатор экземпляра CM: различает ряды одного узла
	// после перезапуска и разные CM в процессе.
	Instance uint64 `json:"Instance"`
	// Seq — номер попытки выпуска всего отчёта; при выключенном выводе
	// не растёт.
	Seq uint64 `json:"Seq"`
	// Utc — момент снятия снимка в UTC.
	Utc string `json:"Utc"`
	// AgeNs — монотонный возраст CM от создания в наносекундах.
	AgeNs int64 `json:"AgeNs"`
	// Role — роль узла на момент снятия.
	Role string `json:"Role"`
	// Term — текущий терм узла.
	Term int `json:"Term"`
	// CmSnapshotNs — момент снятия снимка CM в наносекундах эпохи Unix.
	CmSnapshotNs int64 `json:"CmSnapshotNs"`
	// StorageSnapshotNs — момент отдельного диагностического снимка
	// хранилища; null, когда возможность недоступна и снимок не снимался.
	StorageSnapshotNs *int64 `json:"StorageSnapshotNs"`
	// Persistence — готовность группы счётчиков сохранений CM: матрица
	// формируется всегда и публикуется как ok.
	Persistence string `json:"Persistence"`
	// Persist — матрица завершённых сохранений CM.
	Persist *statsPersistenceV1 `json:"Persist"`
	// Storage — готовность диагностики хранилища: full (точные записи и обе
	// суммы Sync), write_count (только записи) или unavailable.
	Storage string `json:"Storage"`
	// StorageWrites — точное число успешных записей ключей хранилища;
	// null, когда возможность недоступна.
	StorageWrites *int64 `json:"StorageWrites"`
	// StorageFileSyncNs, StorageDirSyncNs — суммы длительностей успешных
	// синхронизаций файла и каталога; null при недоступности возможности.
	StorageFileSyncNs *int64 `json:"StorageFileSyncNs"`
	StorageDirSyncNs  *int64 `json:"StorageDirSyncNs"`
	// Dirty — доступность группы грязных периодов журнала.
	Dirty string `json:"Dirty"`
	// OutputError — липкая ошибка общего вывода отчёта; пустая строка
	// означает отсутствие ошибок.
	OutputError string `json:"OutputError"`
}

// persistReport формирует третью строку отчёта — JSON-документ PersistV1:
// версия схемы, экземпляр, номер выпуска, UTC и монотонный возраст CM,
// роль/терм, моменты снимков CM/Storage, доступность групп метрик, матрица
// сохранений, диагностика хранилища и липкая ошибка общего вывода.
//
// maxLen — предел размера тела документа, оставляющий место обязательному
// префиксу строки. При превышении публикуется тот же формат с коротким
// маркером вместо текста ошибки: обрезать JSON посередине запрещено —
// принимающая сторона обязана всегда получать корректный JSON.
func (s *statsSnapshot) persistReport(
	seq uint64, outputErr error, storage storageDiagnostics, maxLen int,
) string {
	doc := s.persistDocument(seq, "", storage)
	if outputErr != nil {
		doc.OutputError = outputErr.Error()
	}
	data, err := json.Marshal(doc)
	if err == nil && len(data) <= maxLen {
		return string(data)
	}
	doc.OutputError = _statsPersistOverflow
	data, err = json.Marshal(doc)
	if err != nil || len(data) > maxLen {
		return `{"Schema":1,"OutputError":"persistV1 line exceeds 16 KiB limit"}`
	}
	return string(data)
}

// persistDocument собирает документ PersistV1 из снимка CM и отдельного
// диагностического снимка хранилища. Все поля — скаляры, строки и массивы
// снимка; ссылок на изменяемые данные CM документ не содержит. Недоступная
// возможность хранилища публикуется признаком и null, а не нулём; моменты
// снимков CM и Storage независимы.
func (s *statsSnapshot) persistDocument(
	seq uint64, outputError string, storage storageDiagnostics,
) statsPersistV1 {
	doc := statsPersistV1{
		Schema:       1,
		Instance:     s.instance,
		Seq:          seq,
		Utc:          s.at.UTC().Format(time.RFC3339Nano),
		AgeNs:        s.age.Nanoseconds(),
		Role:         s.role.String(),
		Term:         s.term,
		CmSnapshotNs: s.at.UnixNano(),
		Persistence:  _statsGroupAvailable,
		Persist:      s.persist.document(),
		Storage:      storage.group(),
		Dirty:        _statsGroupUnavailable,
		OutputError:  outputError,
	}
	if storage.available {
		at := storage.at
		doc.StorageSnapshotNs = &at
	}
	if storage.writesOK {
		writes := storage.writes
		doc.StorageWrites = &writes
	}
	if storage.fileSyncOK {
		fileSyncNS := storage.fileSyncNS
		doc.StorageFileSyncNs = &fileSyncNS
	}
	if storage.dirSyncOK {
		dirSyncNS := storage.dirSyncNS
		doc.StorageDirSyncNs = &dirSyncNS
	}
	return doc
}
