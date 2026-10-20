package store

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// entryType — тип записи каталога данных, определяемый загрузчиком до
// открытия файла.
type entryType int

// entry — запись каталога: имя и тип без следования симлинкам.
type entry struct {
	name string
	typ  entryType
}

const (
	// entryRegular — обычный файл: единственный тип, который загрузчик
	// открывает и валидирует.
	entryRegular entryType = iota

	// entryDir — каталог: при загрузке пропускается.
	entryDir

	// entrySymlink — симлинк: (Storage симлинков не создаёт).
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
		return "regular file"
	case entryDir:
		return "directory"
	case entrySymlink:
		return "symbolic link"
	default:
		return "non-regular file"
	}
}

// writeSeam — неизменяемый набор подменяемых операций для записи кадра.
// Инициализируется один раз в конструкторе FileStorage и остаётся постоянным
// в течение жизненного цикла экземпляра — поэтому не нуждается в синхронизации.
// Все операции выполняются строго под fs.mu, что гарантирует согласованность
// с остальным состоянием хранилища.
// Добавление новых операций требует отдельного архитектурного решения.
//
// Операции над файлами получают уже собранный полный путь (createTmp —
// временного файла, rename — исходного и конечного): так сохраняется
// прежний профиль выделения памяти и не требуется повторное
// соединение частей пути на каждом изменённом Set.
// Каталог dir передаётся отдельно — он нужен тестовым реализациям,
// чтобы отличать хранилища; реализация по умолчанию использует переданные
// пути напрямую.
type writeSeam struct {
	createTmp    func(dir, name string) (writeAtFile, error)
	writeHeader  func(f writeAtFile, h []byte) error
	writePayload func(f writeAtFile, p []byte) error
	syncFile     func(f writeAtFile) error
	closeFile    func(f writeAtFile) error
	rename       func(dir, from, to string) error
	syncDir      func(dir string) error

	// openLog и writeLog — операции обычного добавления пакета в конец уже
	// опубликованного журнала. Открытие отдаёт дескриптор и текущий размер
	// файла; запись идёт по смещению конца принятого журнала. Отдельные
	// операции от записи скалярного ключа позволяют проверить матрицу
	// отказов обычного добавления (открытие, короткая запись, синхронизация,
	// закрытие) без подмены пакетных функций.
	openLog  func(dir, name string) (writeAtFile, int64, error)
	writeLog func(f writeAtFile, b []byte, off int64) error
}

// readSeam — закрытый набор операций чтения каталога и файлов данных.
// Правила владения и вызова те же, что у набора операций записи writeSeam.
type readSeam struct {
	readDir     func(dir string) ([]entry, error)
	lstat       func(dir, name string) (entryType, error)
	openRegular func(dir, name string) (f readFile, size int64, err error)
	readAll     func(f readFile, n int) ([]byte, error)
	readWhole   func(f readFile, size int64) ([]byte, error)
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
		openLog:      openLogFile,
		writeLog:     writeLogAt,
	}
}

// defaultReadSeam возвращает реализации операций чтения поверх os-функций.
func defaultReadSeam() readSeam {
	return readSeam{
		readDir:     readDirEntries,
		lstat:       lstatEntryType,
		openRegular: openRegularFile,
		readAll:     readAllAt,
		readWhole:   readWholeAt,
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
		return fmt.Errorf("short write: %d of %d bytes", n, len(b))
	}
	return nil
}

// syncFileHandle синхронизирует дескриптор, приводя его к *os.File.
func syncFileHandle(f writeAtFile) error {
	file, ok := f.(*os.File)
	if !ok {
		return fmt.Errorf("sync: unexpected handle type %T", f)
	}
	return file.Sync()
}

// closeFileHandle закрывает дескриптор, приводя его к *os.File.
func closeFileHandle(f writeAtFile) error {
	file, ok := f.(*os.File)
	if !ok {
		return fmt.Errorf("close: unexpected handle type %T", f)
	}
	return file.Close()
}

// renameFile переименовывает файл по полным путям from и to.
// Каталог dir не используется: пути уже собраны вызывающим.
func renameFile(_, from, to string) error {
	return os.Rename(from, to)
}

// openLogFile открывает опубликованный журнал для обычного добавления и
// возвращает дескриптор вместе с размером файла из fstat.
// Дескриптор не хранится между вызовами: он закрывается в том же стеке операции.
func openLogFile(dir, name string) (writeAtFile, int64, error) {
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_RDWR, 0o600)
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

// writeLogAt пишет пакет по смещению конца принятого журнала.
// Короткая запись — ошибка.
func writeLogAt(f writeAtFile, b []byte, off int64) error {
	return writeAllAt(f, b, off)
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
		return nil, fmt.Errorf("short read: %d of %d bytes", got, n)
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
		return fmt.Errorf("close: unexpected handle type %T", f)
	}
	return file.Close()
}

// readWholeAt читает файл журнала целиком от нулевого смещения: пакетный
// формат проверяется по всему содержимому, поэтому размер файла и его байты
// обязаны относиться к одному состоянию. Не полное чтение — ошибка.
func readWholeAt(f readFile, size int64) ([]byte, error) {
	if size < 0 {
		return nil, fmt.Errorf("negative log file size %d", size)
	}
	if size == 0 {
		return []byte{}, nil
	}
	buf := make([]byte, size)
	n, err := f.ReadAt(buf, 0)
	if int64(n) < size {
		if err == nil {
			err = io.ErrUnexpectedEOF
		}
		return nil, fmt.Errorf("short log read: %d of %d bytes: %w", n, size, err)
	}
	return buf, nil
}
