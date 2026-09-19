package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"math"
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
// что обеспечивает устойчивость к сбоям на уровне отдельного ключа.
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

	// write и read — экземплярные швы ввода-вывода. Заполняются
	// конструктором и после этого не мутируются, поэтому не требуют
	// отдельной синхронизации. Операции швов вызываются внутри fs.mu на
	// всём цикле ключа от сравнения до обновления кэша.
	write writeSeam
	read  readSeam

	// hdr — буфер заголовка кадра. Владелец — FileStorage, срок жизни —
	// экземпляр. Заполняется и передаётся в writeHeader только под fs.mu,
	// на каждом изменённом Set перезаписывается целиком. Массив — поле,
	// а не локальная переменная: срез заголовка не должен уходить в кучу.
	hdr [_headerSize]byte
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

	// _headerSize — размер заголовка кадра версии 1: magic 8 Б,
	// version 2 Б, reserved 2 Б, длина payload 8 Б, контрольная сумма 4 Б.
	_headerSize = 24

	// _formatVersion — версия кадра. Version 1 покрывает алгоритм
	// CRC-32C: смена алгоритма контрольной суммы без version=2
	// недопустима.
	_formatVersion uint16 = 1
)

// _magic — сигнатура кадра: байты ASCII "RAFTPST" и завершающий 0x00.
var _magic = [8]byte{'R', 'A', 'F', 'T', 'P', 'S', 'T', 0x00}

// _crc32c — таблица контрольной суммы Кастаньоли. Стандартная библиотека
// предвычисляет её для стандартных полиномов, поэтому обращение к таблице
// не аллоцирует.
var _crc32c = crc32.MakeTable(crc32.Castagnoli)

// entryType — тип записи каталога данных, определяемый загрузчиком до
// открытия файла.
type entryType int

const (
	// entryRegular — обычный файл: единственный тип, который загрузчик
	// открывает и валидирует.
	entryRegular entryType = iota

	// entryDir — каталог: при загрузке пропускается.
	entryDir

	// entrySymlink — симлинк: отказ старта (Storage симлинков не создаёт).
	entrySymlink

	// entryOther — FIFO, сокет, устройство и прочее: отказ старта до
	// открытия (открытие FIFO без писателя заблокировало бы навсегда).
	entryOther
)

// String возвращает человекочитаемое имя типа записи для сообщений об
// отказе старта.
func (t entryType) String() string {
	switch t {
	case entryRegular:
		return "обычный файл"
	case entryDir:
		return "каталог"
	case entrySymlink:
		return "симлинк"
	default:
		return "не-регулярный файл"
	}
}

// entry — запись каталога: имя и тип без следования симлинкам.
type entry struct {
	name string
	typ  entryType
}

// readFile — дескриптор файла, из которого загрузчик читает кадр.
type readFile interface {
	ReadAt(b []byte, off int64) (int, error)
}

// writeAtFile — дескриптор временного файла, в который идёт запись кадра.
type writeAtFile interface {
	WriteAt(b []byte, off int64) (int, error)
}

// writeSeam — закрытый набор операций записи кадра. Экземпляр шва —
// неизменяемое поле FileStorage: заполняется в конструкторе и после этого
// не мутируется, поэтому не требует синхронизации. Все операции вызываются
// под fs.mu. Расширение набора операций требует отдельного решения.
//
// Операции над файлами получают уже собранный полный путь (createTmp —
// временного файла, rename — исходного и конечного): так шов повторяет
// прежний профиль аллокаций производственного пути и не добавляет
// повторных соединений пути на каждом изменённом Set. Каталог dir
// передаётся отдельно — он нужен тестовым реализациям, чтобы отличать
// хранилища; реализация по умолчанию использует переданные пути напрямую.
type writeSeam struct {
	createTmp    func(dir, name string) (writeAtFile, error)
	writeHeader  func(f writeAtFile, h []byte) error
	writePayload func(f writeAtFile, p []byte) error
	syncFile     func(f writeAtFile) error
	closeFile    func(f writeAtFile) error
	rename       func(dir, from, to string) error
	syncDir      func(dir string) error
}

// readSeam — закрытый набор операций чтения каталога и файлов данных.
// Правила владения и вызова те же, что у шва записи.
type readSeam struct {
	readDir     func(dir string) ([]entry, error)
	lstat       func(dir, name string) (entryType, error)
	openRegular func(dir, name string) (f readFile, size int64, err error)
	readAll     func(f readFile, n int) ([]byte, error)
	closeRead   func(f readFile) error
}

// defaultWriteSeam возвращает реализации операций записи поверх os-функций.
func defaultWriteSeam() writeSeam {
	return writeSeam{
		createTmp:    createTmpFile,
		writeHeader:  writeHeaderAt,
		writePayload: writePayloadAt,
		syncFile:     syncFileHandle,
		closeFile:    closeFileHandle,
		rename:       renameFile,
		syncDir:      syncDir,
	}
}

// defaultReadSeam возвращает реализации операций чтения поверх os-функций.
func defaultReadSeam() readSeam {
	return readSeam{
		readDir:     readDirEntries,
		lstat:       lstatEntryType,
		openRegular: openRegularFile,
		readAll:     readAllAt,
		closeRead:   closeReadFile,
	}
}

// createTmpFile создаёт временный файл по полному пути name. Каталог dir
// не используется: путь уже собран вызывающим.
func createTmpFile(_, name string) (writeAtFile, error) {
	return os.Create(name)
}

// writeHeaderAt пишет заголовок кадра по смещению 0. Короткая запись —
// ошибка.
func writeHeaderAt(f writeAtFile, h []byte) error {
	return writeAllAt(f, h, 0)
}

// writePayloadAt пишет payload сразу за заголовком. Короткая запись —
// ошибка.
func writePayloadAt(f writeAtFile, p []byte) error {
	return writeAllAt(f, p, _headerSize)
}

// writeAllAt выполняет запись по смещению и превращает короткую запись в
// ошибку: частичный кадр не должен выглядеть успешным.
func writeAllAt(f writeAtFile, b []byte, off int64) error {
	n, err := f.WriteAt(b, off)
	if err != nil {
		return err
	}
	if n < len(b) {
		return fmt.Errorf("короткая запись: %d из %d байт", n, len(b))
	}
	return nil
}

// syncFileHandle синхронизирует дескриптор, приводя его к *os.File.
func syncFileHandle(f writeAtFile) error {
	file, ok := f.(*os.File)
	if !ok {
		return fmt.Errorf("синхронизация: неожиданный тип дескриптора %T", f)
	}
	return file.Sync()
}

// closeFileHandle закрывает дескриптор, приводя его к *os.File.
func closeFileHandle(f writeAtFile) error {
	file, ok := f.(*os.File)
	if !ok {
		return fmt.Errorf("закрытие: неожиданный тип дескриптора %T", f)
	}
	return file.Close()
}

// renameFile переименовывает файл по полным путям from и to. Каталог dir
// не используется: пути уже собраны вызывающим.
func renameFile(_, from, to string) error {
	return os.Rename(from, to)
}

// readDirEntries возвращает имена и типы записей каталога без следования
// симлинкам.
func readDirEntries(dir string) ([]entry, error) {
	dirEntries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	entries := make([]entry, 0, len(dirEntries))
	for _, de := range dirEntries {
		entries = append(entries, entry{name: de.Name(), typ: entryTypeOf(de.Type())})
	}
	return entries, nil
}

// lstatEntryType возвращает тип записи без следования симлинкам.
func lstatEntryType(dir, name string) (entryType, error) {
	info, err := os.Lstat(filepath.Join(dir, name))
	if err != nil {
		return entryOther, err
	}
	return entryTypeOf(info.Mode()), nil
}

// entryTypeOf переводит биты режима os.FileMode в тип записи загрузчика.
// Нулевой режим (файловые системы без сведений о типе) считается обычным
// файлом, как и у DirEntry.Type.
func entryTypeOf(mode os.FileMode) entryType {
	switch {
	case mode.IsDir():
		return entryDir
	case mode&os.ModeSymlink != 0:
		return entrySymlink
	case mode.IsRegular():
		return entryRegular
	default:
		return entryOther
	}
}

// openRegularFile открывает обычный файл и возвращает его размер из fstat
// уже открытого дескриптора: размер и дескриптор относятся к одному
// состоянию файла, гонка lstat/open исключена.
func openRegularFile(dir, name string) (readFile, int64, error) {
	f, err := os.Open(filepath.Join(dir, name))
	if err != nil {
		return nil, 0, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, 0, err
	}
	return f, info.Size(), nil
}

// readAllAt читает ровно n байт payload сразу за заголовком (off=24).
// Короткое чтение — ошибка: кадр обязан содержать ровно length байт
// payload.
func readAllAt(f readFile, n int) ([]byte, error) {
	buf := make([]byte, n)
	got, err := f.ReadAt(buf, _headerSize)
	if got < n {
		return nil, fmt.Errorf("короткое чтение: %d из %d байт", got, n)
	}
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// closeReadFile закрывает дескриптор чтения, приводя его к *os.File.
func closeReadFile(f readFile) error {
	file, ok := f.(*os.File)
	if !ok {
		return fmt.Errorf("закрытие: неожиданный тип дескриптора %T", f)
	}
	return file.Close()
}

// NewFileStorage создаёт FileStorage в указанной директории, создавая её
// при необходимости и загружая существующие .dat-файлы в in-memory кэш.
//
// Загрузка выполняется по принципу «проверить всё»: любой файл данных,
// который не является корректным кадром версии 1 (старый формат,
// смешанный набор, обрезанный, повреждённый файл, не-регулярная запись
// или симлинк с суффиксом .dat), приводит к отказу старта процесса через
// log.Fatalf, а не к молчаливому пропуску. Повреждённый набор не
// становится пустой базой, автоматической конвертации нет. Каталоги с
// суффиксом .dat пропускаются; временные файлы с суффиксом .tmp не
// считаются зафиксированными данными.
func NewFileStorage(dir string) *FileStorage {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		log.Fatalf("FileStorage: cannot create dir %s: %v", dir, err)
	}
	fs := newFileStorage(dir, defaultWriteSeam(), defaultReadSeam())
	if err := fs.loadAll(); err != nil {
		log.Fatalf("FileStorage: %v", err)
	}
	return fs
}

// newFileStorage собирает хранилище с заданными швами, не создавая каталог
// и не выполняя загрузку. Неэкспортируемый конструктор пакета: тесты
// внедряют через него собственные реализации швов, не подменяя пакетные
// переменные-функции, которые разделялись бы всеми экземплярами.
func newFileStorage(dir string, write writeSeam, read readSeam) *FileStorage {
	return &FileStorage{
		dir:   dir,
		data:  make(map[string][]byte),
		write: write,
		read:  read,
	}
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
// На изменённый ключ пишутся заголовок кадра и payload; файл и каталог
// синхронизируются по разу.
//
// Длительности замеряются монотонными часами непосредственно вокруг
// синхронизации файла и вызова syncDir; сумма syncDir включает открытие и
// закрытие каталога внутри помощника, то есть это длительность вызова, а не
// чистого системного вызова.
//
//nolint:gocritic // log.Fatalf завершает процесс: отложенное снятие на пути ошибки не наблюдаемо
func (fs *FileStorage) Set(key string, value []byte) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Сравнение с кэшем остаётся на быстром пути без вызова помощника:
	// неизменный Set не выполняет ввода-вывода и не несёт лишней работы.
	if cached, found := fs.data[key]; found && bytes.Equal(cached, value) {
		return
	}
	if err := fs.setLocked(key, value); err != nil {
		log.Fatalf("FileStorage.Set: %v", err)
	}
}

// setLocked выполняет полный цикл ключа под fs.mu для значения, заведомо
// отличающегося от закэшированного: запись заголовка и payload во временный
// файл, обе синхронизации, переименование и обновление кэша с метриками.
// Возвращает ошибку для тестов; публичный Set превращает её в отказ
// процесса. Требует удержания fs.mu.
func (fs *FileStorage) setLocked(key string, value []byte) error {
	name := key + _dataFileSuffix
	path := filepath.Join(fs.dir, name)
	tmpPath := path + _tmpFileSuffix

	f, err := fs.write.createTmp(fs.dir, tmpPath)
	if err != nil {
		return fmt.Errorf("cannot create tmp file %s: %w", tmpPath, err)
	}
	fs.buildHeader(value)
	if err := fs.write.writeHeader(f, fs.hdr[:]); err != nil {
		_ = fs.write.closeFile(f)
		return fmt.Errorf("write header %s: %w", tmpPath, err)
	}
	if err := fs.write.writePayload(f, value); err != nil {
		_ = fs.write.closeFile(f)
		return fmt.Errorf("write payload %s: %w", tmpPath, err)
	}
	fileSyncStart := time.Now()
	if err := fs.write.syncFile(f); err != nil {
		_ = fs.write.closeFile(f)
		return fmt.Errorf("sync %s: %w", tmpPath, err)
	}
	fileSyncNS := time.Since(fileSyncStart).Nanoseconds()
	if err := fs.write.closeFile(f); err != nil {
		return fmt.Errorf("close %s: %w", tmpPath, err)
	}
	if err := fs.write.rename(fs.dir, tmpPath, path); err != nil {
		return fmt.Errorf("rename %s -> %s: %w", tmpPath, path, err)
	}

	// Синхронизация родительской директории, чтобы переименование пережило
	// аварийный сбой ОС.
	dirSyncStart := time.Now()
	if err := fs.write.syncDir(fs.dir); err != nil {
		return fmt.Errorf("sync dir %s: %w", fs.dir, err)
	}
	dirSyncNS := time.Since(dirSyncStart).Nanoseconds()

	fs.data[key] = slices.Clone(value)
	fs.hasData = true
	fs.writes++
	// Наблюдения прибавляются вместе с writes++: только успешный цикл ключа
	// даёт завершённые синхронизации файла и каталога.
	fs.fileSyncNS += fileSyncNS
	fs.dirSyncNS += dirSyncNS
	return nil
}

// buildHeader заполняет буфер заголовка fs.hdr для value: magic, version,
// reserved, длину payload и контрольную сумму CRC-32C. Требует удержания
// fs.mu; срез fs.hdr действителен только в пределах одного изменённого Set.
func (fs *FileStorage) buildHeader(value []byte) {
	copy(fs.hdr[:8], _magic[:])
	binary.BigEndian.PutUint16(fs.hdr[8:10], _formatVersion)
	binary.BigEndian.PutUint16(fs.hdr[10:12], 0)
	binary.BigEndian.PutUint64(fs.hdr[12:20], uint64(len(value)))
	binary.BigEndian.PutUint32(fs.hdr[20:24], crc32.Checksum(value, _crc32c))
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

// loadAll проверяет все .dat-файлы каталога и загружает их в in-memory кэш.
// Вызывается один раз в конструкторе. Любая несовместимость или
// повреждение — ошибка: старый, смешанный или повреждённый набор не
// становится пустой базой. Каталоги с суффиксом .dat пропускаются, а
// не-регулярные файлы и симлинки отвергаются до открытия: открытие FIFO
// без писателя заблокировало бы старт навсегда. Не-дат-файлы (включая
// .tmp) не считаются зафиксированными данными и не читаются.
func (fs *FileStorage) loadAll() error {
	entries, err := fs.read.readDir(fs.dir)
	if err != nil {
		return fmt.Errorf("read dir %s: %w", fs.dir, err)
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.name, _dataFileSuffix) {
			continue
		}
		// Каталог с суффиксом .dat — пропуск: Storage не создаёт
		// подкаталогов данных.
		if e.typ == entryDir {
			continue
		}
		// Тип проверяется до открытия. DirEntry.Type на части файловых
		// систем не заполняется, поэтому авторитетной считается проверка
		// Lstat: симлинк, FIFO, сокет и устройство отвергаются, а не
		// открываются.
		typ, err := fs.read.lstat(fs.dir, e.name)
		if err != nil {
			return fmt.Errorf("%s: stat: %w", filepath.Join(fs.dir, e.name), err)
		}
		if typ != entryRegular {
			return fmt.Errorf("%s: %s, отказ старта: Storage не создаёт таких имён",
				filepath.Join(fs.dir, e.name), typ)
		}
		if err := fs.loadFile(e.name); err != nil {
			return err
		}
	}
	return nil
}

// loadFile валидирует кадр одного .dat-файла и помещает payload в кэш.
func (fs *FileStorage) loadFile(name string) error {
	path := filepath.Join(fs.dir, name)
	f, size, err := fs.read.openRegular(fs.dir, name)
	if err != nil {
		return fmt.Errorf("%s: open: %w", path, err)
	}
	payload, err := fs.readFrame(f, size, path)
	if err != nil {
		_ = fs.read.closeRead(f)
		return err
	}
	if err := fs.read.closeRead(f); err != nil {
		return fmt.Errorf("%s: close: %w", path, err)
	}
	fs.data[strings.TrimSuffix(name, _dataFileSuffix)] = payload
	fs.hasData = true
	return nil
}

// readFrame читает и проверяет кадр открытого файла размером size. Порядок
// проверок до выделения памяти под payload: заголовок, magic/version/
// reserved, равенство длины размеру файла и предел платформы, и только
// затем чтение payload и сверка контрольной суммы. Повреждённое поле длины
// даёт диагностический отказ, а не гигантское выделение.
func (fs *FileStorage) readFrame(f readFile, size int64, path string) ([]byte, error) {
	var hdr [_headerSize]byte
	n, err := f.ReadAt(hdr[:], 0)
	// Короткое чтение заголовка — усечение кадра независимо от ошибки.
	if n < len(hdr) {
		if err == nil {
			err = io.ErrUnexpectedEOF
		}
		return nil, fmt.Errorf("%s: усечённый заголовок: %d из %d байт: %w", path, n, len(hdr), err)
	}
	if err != nil {
		return nil, fmt.Errorf("%s: чтение заголовка: %w", path, err)
	}
	if !bytes.Equal(hdr[:8], _magic[:]) {
		return nil, fmt.Errorf("%s: неверная сигнатура кадра", path)
	}
	if v := binary.BigEndian.Uint16(hdr[8:10]); v != _formatVersion {
		return nil, fmt.Errorf("%s: неподдерживаемая версия кадра %d", path, v)
	}
	if r := binary.BigEndian.Uint16(hdr[10:12]); r != 0 {
		return nil, fmt.Errorf("%s: ненулевое зарезервированное поле %d", path, r)
	}
	length := binary.BigEndian.Uint64(hdr[12:20])
	if size < _headerSize || length != uint64(size-_headerSize) {
		return nil, fmt.Errorf("%s: длина кадра %d не совпадает с размером файла %d", path, length, size)
	}
	if length > uint64(math.MaxInt) {
		return nil, fmt.Errorf("%s: длина кадра %d превышает предел платформы", path, length)
	}
	want := binary.BigEndian.Uint32(hdr[20:24])
	payload, err := fs.read.readAll(f, int(length))
	if err != nil {
		return nil, fmt.Errorf("%s: чтение payload: %w", path, err)
	}
	if got := crc32.Checksum(payload, _crc32c); got != want {
		return nil, fmt.Errorf("%s: несовпадение контрольной суммы: получено %d, ожидалось %d", path, got, want)
	}
	return payload, nil
}
