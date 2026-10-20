package store

import (
	"fmt"
	"testing"

	"github.com/vskurikhin/raft/pkg/raft/contract"
)

// logBenchValueSize — размер Data одной записи, соответствующий KV-значению
// value128; тип Data и размер одинаковы во всех точках замера.
const logBenchValueSize = 128

// logBenchCompactionEvery — период уплотнения в установившемся режиме:
// примерно каждые 3000 добавлений полная замена журнала. Уплотнение входит
// в измеряемый путь установившегося режима, а не в подготовку.
const logBenchCompactionEvery = 3000

// logBenchBatch — переиспользуемый набор записей: Data выделяется один раз,
// а индекс и содержимое обновляются на месте. Контракт LogStorage читает
// Data синхронно и не удерживает ссылок, поэтому повторное использование
// буфера не искажает измеряемый путь и убирает разовые аллокации
// приспособления из B/op.
type logBenchBatch struct {
	entries []contract.LogEntry
}

// newLogBenchBatch создаёт набор из count записей с буферами Data.
func newLogBenchBatch(count int) *logBenchBatch {
	entries := make([]contract.LogEntry, count)
	for i := range entries {
		entries[i] = contract.LogEntry{
			Type: contract.LogCommand,
			Data: make([]byte, logBenchValueSize),
		}
	}
	return &logBenchBatch{entries: entries}
}

// fill обновляет индексы и содержимое Data на месте и возвращает записи.
func (pool *logBenchBatch) fill(first int, tag byte) []contract.LogEntry {
	for i := range pool.entries {
		entry := &pool.entries[i]
		entry.Index = first + i
		entry.Term = 1
		data, ok := entry.Data.([]byte)
		if !ok {
			panic("буфер Data потерял тип []byte")
		}
		for j := range data {
			data[j] = byte((first+i+j)%251) ^ tag
		}
	}
	return pool.entries
}

// benchLogDeltas — фактические счётчики записи за измеряемое окно.
type benchLogDeltas struct {
	Batches      uint64
	Bytes        uint64
	ScalarWrites int64
}

// reportLogDeltas выводит измеренные счётчики на операцию. Счётчики пути
// скалярных ключей (scalarWrites/op и длительности синхронизаций) даны
// раздельно: реализация не публикует отдельные счётчики синхронизаций
// журнала, поэтому число пакетов журнала наблюдается через результат
// операции (batches/op), а объём — через journalBytes/op.
func reportLogDeltas(b *testing.B, delta benchLogDeltas) {
	perOp := func(value float64) float64 { return value / float64(b.N) }
	b.ReportMetric(perOp(float64(delta.Batches)), "batches/op")
	b.ReportMetric(perOp(float64(delta.Bytes)), "journalBytes/op")
	b.ReportMetric(perOp(float64(delta.ScalarWrites)), "scalarWrites/op")
}

// BenchmarkLogAppend измеряет обычное добавление N записей к журналу длины L
// на полном пути хранилища. Подготовка (создание базового журнала) вынесена
// до таймера; уплотнение каждые ~3000 добавлений входит в установившийся
// режим. Ожидание механики — один пакет на операцию.
func BenchmarkLogAppend(b *testing.B) {
	for _, length := range []int{128, 1280, 4096} {
		for _, add := range []int{1, 8, 64} {
			b.Run(fmt.Sprintf("L%d_N%d", length, add), func(b *testing.B) {
				benchLogAppendCase(b, length, add)
			})
		}
	}
}

// benchLogAppendCase выполняет один случай добавления.
func benchLogAppendCase(b *testing.B, length, add int) {
	fs := NewFileStorage(b.TempDir())
	fs.RewriteLog(newLogBenchBatch(length).fill(0, 0))
	pool := newLogBenchBatch(add)
	next := length
	before := readStorageStats(fs)
	var batches, written uint64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result := fs.StoreLogEntries(next, pool.fill(next, byte(i)))
		batches += result.Writes
		written += result.BytesWritten
		next += add
		if next-length >= logBenchCompactionEvery {
			compactLogBench(b, fs)
		}
	}
	b.StopTimer()
	reportLogDeltas(b, diffStorageStats(before, readStorageStats(fs), batches, written))
}

// compactLogBench выполняет уплотнение установившегося режима: читает
// сохранённый журнал и публикует его полной заменой.
func compactLogBench(b *testing.B, fs *FileStorage) {
	b.Helper()
	entries, err := fs.LoadLog()
	if err != nil {
		b.Fatalf("LoadLog при уплотнении: %v", err)
	}
	fs.RewriteLog(entries)
}

// BenchmarkLogConflict измеряет конфликтную замену хвоста: суффикс из
// k записей заменяется новыми данными. Ожидание механики — атомарная замена
// файла с синхронизацией файла и каталога.
func BenchmarkLogConflict(b *testing.B) {
	for _, length := range []int{128, 1280, 4096} {
		for _, replace := range []int{1, 8, 64} {
			b.Run(fmt.Sprintf("L%d_K%d", length, replace), func(b *testing.B) {
				benchLogConflictCase(b, length, replace)
			})
		}
	}
}

// benchLogConflictCase выполняет один случай конфликтной замены.
func benchLogConflictCase(b *testing.B, length, replace int) {
	fs := NewFileStorage(b.TempDir())
	fs.RewriteLog(newLogBenchBatch(length).fill(0, 0))
	pool := newLogBenchBatch(replace)
	from := length - replace
	before := readStorageStats(fs)
	var batches, written uint64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result := fs.StoreLogEntries(from, pool.fill(from, byte(i+1)))
		batches += result.Writes
		written += result.BytesWritten
	}
	b.StopTimer()
	reportLogDeltas(b, diffStorageStats(before, readStorageStats(fs), batches, written))
}

// BenchmarkLogRewrite измеряет полную замену журнала длины L.
func BenchmarkLogRewrite(b *testing.B) {
	for _, length := range []int{128, 1280, 4096} {
		b.Run(fmt.Sprintf("L%d", length), func(b *testing.B) {
			fs := NewFileStorage(b.TempDir())
			entries := newLogBenchBatch(length).fill(0, 0)
			before := readStorageStats(fs)
			var batches, written uint64
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result := fs.RewriteLog(entries)
				batches += result.Writes
				written += result.BytesWritten
			}
			b.StopTimer()
			reportLogDeltas(b, diffStorageStats(before, readStorageStats(fs), batches, written))
		})
	}
}

// BenchmarkLogRewriteEmpty измеряет пустое усечение: полную замену пустым
// журналом.
func BenchmarkLogRewriteEmpty(b *testing.B) {
	fs := NewFileStorage(b.TempDir())
	fs.RewriteLog(newLogBenchBatch(128).fill(0, 0))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fs.RewriteLog(nil)
	}
	b.StopTimer()
}

// BenchmarkLogRewriteConfig измеряет полную замену журнала из записей типа
// LogConfiguration — отдельно от командных записей.
func BenchmarkLogRewriteConfig(b *testing.B) {
	fs := NewFileStorage(b.TempDir())
	entries := newLogBenchBatch(128).fill(0, 0)
	for i := range entries {
		entries[i].Type = contract.LogConfiguration
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fs.RewriteLog(entries)
	}
	b.StopTimer()
}

// BenchmarkLogNoop измеряет логический no-op: повторную запись уже
// сохранённого суффикса. Ожидание механики — нулевой ввод-вывод.
func BenchmarkLogNoop(b *testing.B) {
	fs := NewFileStorage(b.TempDir())
	fs.RewriteLog(newLogBenchBatch(128).fill(0, 0))
	pool := newLogBenchBatch(8)
	batch := pool.fill(128, 0)
	if result := fs.StoreLogEntries(128, batch); result.Writes == 0 {
		b.Fatalf("подготовка no-op не выполнила запись")
	}
	before := readStorageStats(fs)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if result := fs.StoreLogEntries(128, batch); result.Writes != 0 || result.BytesWritten != 0 {
			b.Fatalf("логический no-op выполнил ввод-вывод: %+v", result)
		}
	}
	b.StopTimer()
	reportLogDeltas(b, diffStorageStats(before, readStorageStats(fs), 0, 0))
}

// BenchmarkLogLoad измеряет восстановление журнала длины L.
func BenchmarkLogLoad(b *testing.B) {
	for _, length := range []int{128, 1280, 4096} {
		b.Run(fmt.Sprintf("L%d", length), func(b *testing.B) {
			fs := NewFileStorage(b.TempDir())
			fs.RewriteLog(newLogBenchBatch(length).fill(0, 0))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				loaded, err := fs.LoadLog()
				if err != nil {
					b.Fatalf("LoadLog: %v", err)
				}
				if len(loaded) != length {
					b.Fatalf("LoadLog вернул %d записей, ожидалось %d", len(loaded), length)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(length), "entries/op")
		})
	}
}

// storageStats — счётчики записи, читаемые из хранилища.
type storageStats struct {
	Writes     int
	FileSyncNs int64
	DirSyncNs  int64
}

// readStorageStats читает накопленные счётчики хранилища.
func readStorageStats(fs *FileStorage) storageStats {
	writes, fileSync, dirSync := fs.PersistenceStats()
	return storageStats{Writes: writes, FileSyncNs: fileSync, DirSyncNs: dirSync}
}

// diffStorageStats вычитает счётчики до окна из счётчиков после окна и
// переносит число пакетов и байт, возвращённых операциями.
func diffStorageStats(before, after storageStats, batches, written uint64) benchLogDeltas {
	return benchLogDeltas{
		Batches:      batches,
		Bytes:        written,
		ScalarWrites: int64(after.Writes - before.Writes),
	}
}
