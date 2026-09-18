package store

import (
	"bytes"
	"encoding/gob"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/vskurikhin/raft/pkg/raft/contract"
)

// FileStorage — file-backed реализация интерфейса Storage. Каждый ключ
// хранится в отдельном файле <key>.dat в заданной директории.
// Запись атомарна (temp -> fsync -> rename),
// что обеспечивает crash-safety на уровне отдельного ключа.
//
// Предпосылка: ключи, передаваемые в Set, должны быть filesystem-safe.
type FileStorage struct {
	mu      sync.Mutex
	dir     string
	data    map[string][]byte
	hasData bool
	writes  int

	// fileSyncNS и dirSyncNS — суммы длительностей успешно завершённых
	// синхронизаций файла и каталога в наносекундах. Обе величины
	// защищены fs.mu и прибавляются вместе с writes только после полного
	// успешного цикла ключа.
	fileSyncNS int64
	dirSyncNS  int64
}

var _ contract.Storage = (*FileStorage)(nil)

const (
	// _dataFileSuffix — суффикс файла данных: каждый ключ Storage
	// хранится в файле <ключ>.dat; часть формата имён файлов на диске.
	_dataFileSuffix = ".dat"

	// _tmpFileSuffix — суффикс временного файла атомарной записи
	// (запись, fsync, переименование). Значение совпадает с суффиксом
	// временных директорий хранилища снимков (_tmpSuffix), но это разные
	// форматы: константы не связываются, правка одного формата не
	// должна менять другой.
	_tmpFileSuffix = ".tmp"
)

// NewFileStorage создаёт FileStorage в указанной директории, создавая её
// при необходимости и загружая существующие .dat-файлы в in-memory кэш.
func NewFileStorage(dir string) *FileStorage {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		log.Fatalf("FileStorage: cannot create dir %s: %v", dir, err)
	}
	fs := &FileStorage{
		dir:  dir,
		data: make(map[string][]byte),
	}
	fs.loadAll()
	return fs
}

// Set атомарно сохраняет value для key на диск и обновляет in-memory кэш.
// При ошибке ФС процесс завершается через log.Fatalf, поэтому наблюдения
// прибавляются только на полностью успешном пути: неизменный Set и
// прерванная операция не увеличивают ни счётчик записей, ни суммы
// длительностей синхронизации.
//
// Инвариант: после возврата Set на диске долговечно лежит value, а кэш хранит
// собственную копию, побайтово равную value, независимо от последующих мутаций
// среза вызывающим. Ввод-вывод пропускается ровно тогда, когда на диске уже
// лежит это значение: ключ есть в кэше и закэшированная копия побайтово равна
// value. Пропуск долговечности не ослабляет — требуемое значение уже на диске.
//
// Длительности замеряются монотонными часами непосредственно вокруг f.Sync
// и syncDir; сумма syncDir включает открытие и закрытие каталога внутри
// помощника, то есть это длительность вызова, а не чистого системного вызова.
//
//nolint:gocritic // log.Fatalf завершает процесс: отложенное снятие на пути ошибки не наблюдаемо
func (fs *FileStorage) Set(key string, value []byte) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if cached, found := fs.data[key]; found && bytes.Equal(cached, value) {
		return
	}

	path := filepath.Join(fs.dir, key+_dataFileSuffix)
	tmpPath := path + _tmpFileSuffix

	f, err := os.Create(tmpPath)
	if err != nil {
		log.Fatalf("FileStorage.Set: cannot create tmp file %s: %v", tmpPath, err)
	}
	if err := gob.NewEncoder(f).Encode(value); err != nil {
		_ = f.Close()
		log.Fatalf("FileStorage.Set: gob encode %s: %v", key, err)
	}
	fileSyncStart := time.Now()
	if err := f.Sync(); err != nil {
		_ = f.Close()
		log.Fatalf("FileStorage.Set: sync %s: %v", tmpPath, err)
	}
	fileSyncNS := time.Since(fileSyncStart).Nanoseconds()
	if err := f.Close(); err != nil {
		log.Fatalf("FileStorage.Set: close %s: %v", tmpPath, err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		log.Fatalf("FileStorage.Set: rename %s -> %s: %v", tmpPath, path, err)
	}

	// fsync родительской директории, чтобы rename пережил аварийный сбой ОС.
	dirSyncStart := time.Now()
	if err := syncDir(fs.dir); err != nil {
		log.Fatalf("FileStorage.Set: sync dir %s: %v", fs.dir, err)
	}
	dirSyncNS := time.Since(dirSyncStart).Nanoseconds()

	fs.data[key] = slices.Clone(value)
	fs.hasData = true
	fs.writes++
	// Наблюдения прибавляются вместе с writes++: только успешный цикл ключа
	// даёт завершённые f.Sync и syncDir.
	fs.fileSyncNS += fileSyncNS
	fs.dirSyncNS += dirSyncNS
}

// Get возвращает значение key из in-memory кэша.
//
// Возвращается защитная копия: вызывающий может мутировать полученный срез,
// не затрагивая ни кэш, ни содержимое диска.
func (fs *FileStorage) Get(key string) ([]byte, bool) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	v, ok := fs.data[key]
	if !ok {
		return nil, false
	}
	return slices.Clone(v), true
}

// HasData возвращает true, если в хранилище есть хотя бы один .dat-файл
// либо был выполнен хотя бы один Set.
func (fs *FileStorage) HasData() bool {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.hasData
}

// WriteCount возвращает число фактически выполненных записей на диск.
// Служебный счётчик наблюдаемости для тестов и бенчмарков пакета: вызовы Set,
// пропущенные из-за совпадения значения с закэшированным, в него не входят.
func (fs *FileStorage) WriteCount() int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.writes
}

// PersistenceStats возвращает согласованный диагностический снимок под одним
// захватом fs.mu: существующее число фактически завершённых записей ключей и
// суммы длительностей успешно завершённых синхронизаций файла и каталога в
// наносекундах. Это дополнительная диагностическая возможность конкретной
// реализации: интерфейс Storage метода не требует, а WriteCount сохраняет
// прежнюю семантику и служит источником значения writes.
func (fs *FileStorage) PersistenceStats() (writes int, fileSyncNS, dirSyncNS int64) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.writes, fs.fileSyncNS, fs.dirSyncNS
}

// loadAll читает все .dat-файлы из директории в in-memory кэш. Вызывается
// один раз в конструкторе. Не-дат-файлы (включая .tmp) и повреждённые
// файлы пропускаются.
func (fs *FileStorage) loadAll() {
	entries, err := os.ReadDir(fs.dir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, _dataFileSuffix) {
			continue
		}
		key := strings.TrimSuffix(name, _dataFileSuffix)
		f, err := os.Open(filepath.Join(fs.dir, name))
		if err != nil {
			continue
		}
		var val []byte
		if err := gob.NewDecoder(f).Decode(&val); err != nil {
			_ = f.Close()
			continue
		}
		_ = f.Close()
		fs.data[key] = val
		fs.hasData = true
	}
}
