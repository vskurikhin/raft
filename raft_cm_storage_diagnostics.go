package raft

import "time"

// storagePersistenceStats — необязательная диагностическая возможность
// хранилища: существующий счётчик фактических записей ключей и суммы
// длительностей успешных синхронизаций файла и каталога в наносекундах.
// Метод не входит в Storage и не требуется другим реализациям; признаком
// его отсутствия служит неуспешное приведение типа.
type storagePersistenceStats interface {
	PersistenceStats() (writes int, fileSyncNS int64, dirSyncNS int64)
}

// storageWriteCounter — необязательная возможность хранилища: число
// фактически выполненных записей. Используется, когда точные суммы
// синхронизации недоступны.
type storageWriteCounter interface {
	WriteCount() int
}

// Маркеры готовности группы диагностики хранилища в периодическом отчёте.
const (
	// _statsStorageFull — доступны точные записи и обе суммы синхронизации.
	_statsStorageFull = "full"
	// _statsStorageWriteCount — доступно только число фактических записей.
	_statsStorageWriteCount = "write_count"
)

// storageDiagnostics — согласованный диагностический снимок хранилища,
// снятый одним обращением к возможности. Нулевое значение означает
// отсутствие возможности: недоступность отличается от нулевых значений и
// публикуется отдельным признаком. Булевы поля различают частичную
// доступность (только счётчик записей) и полную.
type storageDiagnostics struct {
	// available — снимок снят: возможность у хранилища существует.
	available bool
	// at — момент снятия снимка в наносекундах эпохи Unix.
	at int64
	// writes, writesOK — число фактически успешных записей ключей.
	writes   int64
	writesOK bool
	// fileSyncNS, fileSyncOK — сумма длительностей успешных f.Sync.
	fileSyncNS int64
	fileSyncOK bool
	// dirSyncNS, dirSyncOK — сумма длительностей успешных syncDir.
	dirSyncNS int64
	dirSyncOK bool
}

// group возвращает маркер готовности группы диагностики хранилища:
// полная возможность, только счётчик записей или недоступность.
// Чистая функция над снимком.
func (d storageDiagnostics) group() string {
	switch {
	case d.fileSyncOK && d.dirSyncOK:
		return _statsStorageFull
	case d.writesOK:
		return _statsStorageWriteCount
	default:
		return _statsGroupUnavailable
	}
}

// takeStorageDiagnostics снимает диагностический снимок хранилища.
// Вызывается из stats после освобождения cm.mu и берёт только мьютекс
// хранилища: порядок cm.mu → мьютекс хранилища сохраняется, обратного
// направления не появляется. Точные суммы синхронизации читаются одним
// вызовом возможности; при её отсутствии используется только счётчик
// записей; при отсутствии и его снимок недоступен, а не нулевой.
func takeStorageDiagnostics(storage Storage) storageDiagnostics {
	if IsNilInterface(storage) {
		return storageDiagnostics{}
	}
	if provider, ok := storage.(storagePersistenceStats); ok {
		at := time.Now().UnixNano()
		writes, fileSyncNS, dirSyncNS := provider.PersistenceStats()
		return storageDiagnostics{
			available:  true,
			at:         at,
			writes:     int64(writes),
			writesOK:   true,
			fileSyncNS: fileSyncNS,
			fileSyncOK: true,
			dirSyncNS:  dirSyncNS,
			dirSyncOK:  true,
		}
	}
	if counter, ok := storage.(storageWriteCounter); ok {
		at := time.Now().UnixNano()
		return storageDiagnostics{
			available: true,
			at:        at,
			writes:    int64(counter.WriteCount()),
			writesOK:  true,
		}
	}
	return storageDiagnostics{}
}
