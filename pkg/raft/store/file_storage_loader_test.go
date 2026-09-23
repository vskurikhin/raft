package store

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// frameBytes собирает корректный кадр версии 1 для тестов загрузчика.
func frameBytes(payload []byte) []byte {
	raw := make([]byte, _headerSize+len(payload))
	copy(raw[:8], _magic[:])
	binary.BigEndian.PutUint16(raw[8:10], _formatVersion)
	binary.BigEndian.PutUint64(raw[12:20], uint64(len(payload)))
	binary.BigEndian.PutUint32(raw[20:24], crc32.Checksum(payload, _crc32c))
	copy(raw[_headerSize:], payload)
	return raw
}

// writeDatFile кладёт сырые байты в файл данных ключа.
func writeDatFile(t *testing.T, dir, name string, raw []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), raw, 0o600); err != nil {
		t.Fatalf("запись %s: %v", name, err)
	}
}

// loadAllWithDefaultSeams выполняет загрузку каталога штатным швом чтения и
// возвращает ошибку вместо отказа процесса.
func loadAllWithDefaultSeams(dir string) error {
	return newFileStorage(dir, defaultWriteSeam(), defaultReadSeam()).loadAll()
}

// TestLoadAll_RawFrameValidation проверяет отказ старта на каждом классе
// повреждения кадра, включая старый набор байтов: обрезанный заголовок,
// неверные magic/version/reserved, несовпадение длины и размера файла,
// хвост, усечение и несовпадение контрольной суммы. Ни один повреждённый
// файл не должен приводить к пустой базе без ошибки.
func TestLoadAll_RawFrameValidation(t *testing.T) {
	valid := frameBytes([]byte("payload"))

	badMagic := frameBytes([]byte("payload"))
	badMagic[0] = 0x00

	badVersion := frameBytes([]byte("payload"))
	binary.BigEndian.PutUint16(badVersion[8:10], 2)

	badReserved := frameBytes([]byte("payload"))
	binary.BigEndian.PutUint16(badReserved[10:12], 7)

	lengthTooSmall := frameBytes([]byte("payload"))
	binary.BigEndian.PutUint64(lengthTooSmall[12:20], 3)

	lengthTooLarge := frameBytes([]byte("payload"))
	binary.BigEndian.PutUint64(lengthTooLarge[12:20], 4096)

	badChecksum := frameBytes([]byte("payload"))
	badChecksum[20] ^= 0xff

	trailing := append(frameBytes([]byte("payload")), 0x42)
	truncatedPayload := frameBytes([]byte("payload"))
	truncatedPayload = truncatedPayload[:len(truncatedPayload)-2]
	truncatedHeader := []byte("RAFTPST")

	// Реальные старые байты формата предыдущего этапа: внешний gob без
	// сигнатуры кадра.
	oldGob := []byte{0x07, 0x0a, 0x00, 0x04, 0x03, 0x04, 0x00, 0x02}

	tests := []struct {
		name    string
		raw     []byte
		wantErr bool
	}{
		{name: "корректный кадр", raw: valid, wantErr: false},
		{name: "неверная сигнатура", raw: badMagic, wantErr: true},
		{name: "неподдерживаемая версия", raw: badVersion, wantErr: true},
		{name: "ненулевое reserved", raw: badReserved, wantErr: true},
		{name: "длина меньше payload", raw: lengthTooSmall, wantErr: true},
		{name: "длина больше файла", raw: lengthTooLarge, wantErr: true},
		{name: "несовпадение суммы", raw: badChecksum, wantErr: true},
		{name: "хвост после payload", raw: trailing, wantErr: true},
		{name: "усечённый payload", raw: truncatedPayload, wantErr: true},
		{name: "усечённый заголовок", raw: truncatedHeader, wantErr: true},
		{name: "старые gob-байты", raw: oldGob, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			writeDatFile(t, dir, "k.dat", tt.raw)

			fs := newFileStorage(dir, defaultWriteSeam(), defaultReadSeam())
			err := fs.loadAll()
			if tt.wantErr && err == nil {
				t.Fatal("loadAll вернул nil, want отказ")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("loadAll вернул %v, want nil", err)
			}
			if !tt.wantErr {
				if got, ok := fs.Get("k"); !ok || string(got) != "payload" {
					t.Fatalf("загруженное значение = %q, ok=%v, want payload", got, ok)
				}
			}
		})
	}
}

// TestLoadAll_CorruptSetDoesNotBecomeEmptyBase проверяет, что корректный
// ключ рядом с повреждённым не превращает старт в пустую базу: загрузка
// возвращает ошибку, а вызывающий конструктор обязан отказать.
func TestLoadAll_CorruptSetDoesNotBecomeEmptyBase(t *testing.T) {
	dir := t.TempDir()
	writeDatFile(t, dir, "a.dat", frameBytes([]byte("ok")))
	writeDatFile(t, dir, "b.dat", []byte{0x01, 0x02, 0x03})

	err := loadAllWithDefaultSeams(dir)
	if err == nil {
		t.Fatal("loadAll вернул nil на смешанном наборе, want отказ")
	}
	if !strings.Contains(err.Error(), "b.dat") {
		t.Fatalf("ошибка %q не содержит путь повреждённого файла", err)
	}
}

// TestLoadAll_TmpNotCommittedData проверяет, что временный файл с суффиксом
// .tmp не читается и не помечает хранилище как имеющее данные.
func TestLoadAll_TmpNotCommittedData(t *testing.T) {
	dir := t.TempDir()
	writeDatFile(t, dir, "orphan.dat.tmp", frameBytes([]byte("x")))

	fs := newFileStorage(dir, defaultWriteSeam(), defaultReadSeam())
	if err := fs.loadAll(); err != nil {
		t.Fatalf("loadAll вернул %v, want nil", err)
	}
	if fs.HasData() {
		t.Fatal("HasData = true при наличии только .tmp, want false")
	}
}

// probeReadFile — тестовый дескриптор чтения: отдаёт подготовленные байты и
// запоминает смещения вызовов ReadAt.
type probeReadFile struct {
	data    []byte
	offsets []int64
}

// ReadAt реализует readFile, повторяя строгую семантику io.ReaderAt: при
// коротком чтении возвращается ошибка.
func (f *probeReadFile) ReadAt(b []byte, off int64) (int, error) {
	f.offsets = append(f.offsets, off)
	if len(b) == 0 {
		return 0, nil
	}
	if off < 0 || off >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n := copy(b, f.data[off:])
	if n < len(b) {
		return n, io.EOF
	}
	return n, nil
}

// unexpectedSeamCall — маркерная ошибка вызова операции, которой тест не
// ожидает.
var unexpectedSeamCall = errors.New("операция шва не ожидалась")

// probeReadSeam — тестовый шов чтения: незаданные операции падают
// маркерной ошибкой, чтобы тест обнаружил лишний вызов.
type probeReadSeam struct {
	readDir     func(dir string) ([]entry, error)
	lstat       func(dir, name string) (entryType, error)
	openRegular func(dir, name string) (readFile, int64, error)
	readAll     func(f readFile, n int) ([]byte, error)
	closeRead   func(f readFile) error
}

// seam собирает readSeam, подставляя маркерные реализации вместо
// незаданных операций.
func (p *probeReadSeam) seam() readSeam {
	s := readSeam{
		readDir:     func(string) ([]entry, error) { return nil, unexpectedSeamCall },
		lstat:       func(string, string) (entryType, error) { return entryOther, unexpectedSeamCall },
		openRegular: func(string, string) (readFile, int64, error) { return nil, 0, unexpectedSeamCall },
		readAll:     func(readFile, int) ([]byte, error) { return nil, unexpectedSeamCall },
		closeRead:   func(readFile) error { return unexpectedSeamCall },
	}
	if p.readDir != nil {
		s.readDir = p.readDir
	}
	if p.lstat != nil {
		s.lstat = p.lstat
	}
	if p.openRegular != nil {
		s.openRegular = p.openRegular
	}
	if p.readAll != nil {
		s.readAll = p.readAll
	}
	if p.closeRead != nil {
		s.closeRead = p.closeRead
	}
	return s
}

// TestLoadAll_DirectoryDatSkipped проверяет, что запись-каталог с
// суффиксом .dat пропускается: файл не открывается и не читается.
func TestLoadAll_DirectoryDatSkipped(t *testing.T) {
	opened := make([]string, 0, 1)
	probe := &probeReadSeam{
		readDir: func(string) ([]entry, error) {
			return []entry{
				{name: "snapshots.dat", typ: entryDir},
				{name: "value.dat", typ: entryRegular},
			}, nil
		},
		lstat: func(_, name string) (entryType, error) { return entryRegular, nil },
		openRegular: func(_, name string) (readFile, int64, error) {
			opened = append(opened, name)
			raw := frameBytes([]byte("value"))
			return &probeReadFile{data: raw}, int64(len(raw)), nil
		},
		readAll: func(f readFile, n int) ([]byte, error) {
			buf := make([]byte, n)
			if _, err := f.ReadAt(buf, _headerSize); err != nil {
				return nil, err
			}
			return buf, nil
		},
		closeRead: func(readFile) error { return nil },
	}

	fs := newFileStorage(t.TempDir(), defaultWriteSeam(), probe.seam())
	if err := fs.loadAll(); err != nil {
		t.Fatalf("loadAll вернул %v, want nil", err)
	}
	if len(opened) != 1 || opened[0] != "value.dat" {
		t.Fatalf("открытые файлы = %v, want только value.dat", opened)
	}
	if got, ok := fs.Get("value"); !ok || string(got) != "value" {
		t.Fatalf("значение value = %q, ok=%v, want value", got, ok)
	}
}

// TestLoadAll_NonRegularAndSymlinkRefusedBeforeOpen проверяет, что
// не-регулярные записи и симлинки с суффиксом .dat отвергаются до
// открытия: открытие FIFO без писателя заблокировало бы старт навсегда.
func TestLoadAll_NonRegularAndSymlinkRefusedBeforeOpen(t *testing.T) {
	tests := []struct {
		name string
		typ  entryType
	}{
		{name: "FIFO/устройство", typ: entryOther},
		{name: "симлинк", typ: entrySymlink},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opened := false
			probe := &probeReadSeam{
				readDir: func(string) ([]entry, error) {
					return []entry{{name: "x.dat", typ: entryRegular}}, nil
				},
				lstat: func(string, string) (entryType, error) { return tt.typ, nil },
				openRegular: func(string, string) (readFile, int64, error) {
					opened = true
					return nil, 0, nil
				},
			}

			fs := newFileStorage(t.TempDir(), defaultWriteSeam(), probe.seam())
			err := fs.loadAll()
			if err == nil {
				t.Fatal("loadAll вернул nil, want отказ до открытия")
			}
			if opened {
				t.Fatal("openRegular вызван для не-регулярного файла, want отказ до открытия")
			}
		})
	}
}

// TestLoadAll_LstatAuthoritativeOverDirEntryType проверяет, что тип,
// полученный из readDir, не считается достаточным: симлинк, выглядящий как
// обычный файл, всё равно отвергается по Lstat.
func TestLoadAll_LstatAuthoritativeOverDirEntryType(t *testing.T) {
	probe := &probeReadSeam{
		readDir: func(string) ([]entry, error) {
			return []entry{{name: "x.dat", typ: entryRegular}}, nil
		},
		lstat: func(string, string) (entryType, error) { return entrySymlink, nil },
	}
	fs := newFileStorage(t.TempDir(), defaultWriteSeam(), probe.seam())
	if err := fs.loadAll(); err == nil {
		t.Fatal("loadAll вернул nil, want отказ по Lstat")
	}
}

// TestLoadAll_LengthCheckedBeforeAllocation проверяет, что повреждённая
// длина даёт диагностический отказ до чтения payload: readAll не
// вызывается, гигантского выделения не происходит.
func TestLoadAll_LengthCheckedBeforeAllocation(t *testing.T) {
	raw := frameBytes([]byte("payload"))
	binary.BigEndian.PutUint64(raw[12:20], 1<<40)

	readAllCalled := false
	probe := &probeReadSeam{
		readDir: func(string) ([]entry, error) {
			return []entry{{name: "x.dat", typ: entryRegular}}, nil
		},
		lstat: func(string, string) (entryType, error) { return entryRegular, nil },
		openRegular: func(string, string) (readFile, int64, error) {
			return &probeReadFile{data: raw}, int64(len(raw)), nil
		},
		readAll: func(readFile, int) ([]byte, error) {
			readAllCalled = true
			return nil, nil
		},
	}
	fs := newFileStorage(t.TempDir(), defaultWriteSeam(), probe.seam())
	if err := fs.loadAll(); err == nil {
		t.Fatal("loadAll вернул nil, want отказ по длине")
	}
	if readAllCalled {
		t.Fatal("readAll вызван до проверки длины, want отказ до выделения")
	}
}

// TestLoadAll_HeaderReadAtOffsetZeroAndTruncation проверяет, что заголовок
// читается ровно с нулевого смещения, а короткое чтение считается
// усечением и не доходит до чтения payload.
func TestLoadAll_HeaderReadAtOffsetZeroAndTruncation(t *testing.T) {
	short := []byte("RAFTPST") // 7 байт, заголовок усечён
	file := &probeReadFile{data: short}

	readAllCalled := false
	probe := &probeReadSeam{
		readDir: func(string) ([]entry, error) {
			return []entry{{name: "x.dat", typ: entryRegular}}, nil
		},
		lstat: func(string, string) (entryType, error) { return entryRegular, nil },
		openRegular: func(string, string) (readFile, int64, error) {
			return file, int64(len(short)), nil
		},
		readAll: func(readFile, int) ([]byte, error) {
			readAllCalled = true
			return nil, nil
		},
	}
	fs := newFileStorage(t.TempDir(), defaultWriteSeam(), probe.seam())
	err := fs.loadAll()
	if err == nil {
		t.Fatal("loadAll вернул nil, want отказ по усечению")
	}
	if !strings.Contains(err.Error(), "усеч") {
		t.Fatalf("ошибка %q не описывает усечение заголовка", err)
	}
	if readAllCalled {
		t.Fatal("readAll вызван при усечённом заголовке")
	}
	if len(file.offsets) == 0 || file.offsets[0] != 0 {
		t.Fatalf("ReadAt заголовка вызван со смещениями %v, want первое 0", file.offsets)
	}
}

// Защитные строки матрицы отказов, дата пометки — 2026-09-22.
//
// Отказы чтения payload и закрытия обычного файла через штатный шов на
// os-функциях в производстве недостижимы: os.File.ReadAt на обычном файле
// отдаёт запрошенные байты, а ошибки носителя не воспроизводятся
// детерминированно. Строки закрыты инъекцией шва чтения
// (TestLoadAll_OpenReadCloseErrors), а реалистичный заменяющий сценарий —
// несовпадение контрольной суммы (TestLoadAll_ChecksumMismatch).
//
// Проверка «длина больше math.MaxInt» на 64-битной платформе недостижима:
// размер файла int64 не превосходит math.MaxInt, а длина обязана совпадать
// с размером файла минус заголовок, и эта проверка идёт раньше предела
// платформы. Строка защитная; проверка предела нужна при переносе на
// 32-битную платформу.
//
// TestLoadAll_OpenReadCloseErrors проверяет достижимые отказы шва чтения:
// открытие, чтение payload и закрытие переводят старт в отказ.
func TestLoadAll_OpenReadCloseErrors(t *testing.T) {
	valid := frameBytes([]byte("payload"))

	tests := []struct {
		name  string
		probe *probeReadSeam
	}{
		{
			name: "ошибка открытия",
			probe: &probeReadSeam{
				readDir: func(string) ([]entry, error) {
					return []entry{{name: "x.dat", typ: entryRegular}}, nil
				},
				lstat: func(string, string) (entryType, error) { return entryRegular, nil },
				openRegular: func(string, string) (readFile, int64, error) {
					return nil, 0, errors.New("open отказан")
				},
			},
		},
		{
			name: "ошибка чтения payload",
			probe: &probeReadSeam{
				readDir: func(string) ([]entry, error) {
					return []entry{{name: "x.dat", typ: entryRegular}}, nil
				},
				lstat: func(string, string) (entryType, error) { return entryRegular, nil },
				openRegular: func(string, string) (readFile, int64, error) {
					return &probeReadFile{data: valid}, int64(len(valid)), nil
				},
				readAll: func(readFile, int) ([]byte, error) { return nil, errors.New("read отказан") },
			},
		},
		{
			name: "ошибка закрытия",
			probe: &probeReadSeam{
				readDir: func(string) ([]entry, error) {
					return []entry{{name: "x.dat", typ: entryRegular}}, nil
				},
				lstat: func(string, string) (entryType, error) { return entryRegular, nil },
				openRegular: func(string, string) (readFile, int64, error) {
					return &probeReadFile{data: valid}, int64(len(valid)), nil
				},
				readAll: func(f readFile, n int) ([]byte, error) {
					buf := make([]byte, n)
					if _, err := f.ReadAt(buf, _headerSize); err != nil {
						return nil, err
					}
					return buf, nil
				},
				closeRead: func(readFile) error { return errors.New("close отказан") },
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := newFileStorage(t.TempDir(), defaultWriteSeam(), tt.probe.seam())
			if err := fs.loadAll(); err == nil {
				t.Fatal("loadAll вернул nil, want отказ")
			}
			if _, ok := fs.Get("x"); ok {
				t.Fatal("ключ загружен несмотря на отказ, want отсутствие")
			}
		})
	}
}

// TestLoadAll_ReadDirError переводит отказ перечисления каталога в отказ
// старта.
func TestLoadAll_ReadDirError(t *testing.T) {
	probe := &probeReadSeam{
		readDir: func(string) ([]entry, error) { return nil, errors.New("readdir отказан") },
	}
	fs := newFileStorage(t.TempDir(), defaultWriteSeam(), probe.seam())
	if err := fs.loadAll(); err == nil {
		t.Fatal("loadAll вернул nil, want отказ")
	}
}

// TestLoadAll_ChecksumMismatch проверяет перевычисление контрольной суммы
// payload при чтении: несовпадение переводит старт в отказ.
func TestLoadAll_ChecksumMismatch(t *testing.T) {
	raw := frameBytes([]byte("payload"))
	raw[20] ^= 0xff

	probe := &probeReadSeam{
		readDir: func(string) ([]entry, error) {
			return []entry{{name: "x.dat", typ: entryRegular}}, nil
		},
		lstat: func(string, string) (entryType, error) { return entryRegular, nil },
		openRegular: func(string, string) (readFile, int64, error) {
			return &probeReadFile{data: raw}, int64(len(raw)), nil
		},
		readAll: func(f readFile, n int) ([]byte, error) {
			buf := make([]byte, n)
			if _, err := f.ReadAt(buf, _headerSize); err != nil {
				return nil, err
			}
			return buf, nil
		},
		closeRead: func(readFile) error { return nil },
	}
	fs := newFileStorage(t.TempDir(), defaultWriteSeam(), probe.seam())
	err := fs.loadAll()
	if err == nil {
		t.Fatal("loadAll вернул nil, want отказ по сумме")
	}
	if !strings.Contains(err.Error(), "сумм") {
		t.Fatalf("ошибка %q не описывает несовпадение суммы", err)
	}
}

// TestNewFileStorageFatalOnLegacyFixture проверяет, что публичный
// конструктор на реальном старом наборе завершает процесс с сообщением об
// отказе: помощник запускается дочерним процессом, потому что log.Fatalf
// не перехватывается в том же процессе.
func TestNewFileStorageFatalOnLegacyFixture(t *testing.T) {
	if os.Getenv("STORE_LOAD_FATAL_HELPER") == "1" {
		NewFileStorage(os.Getenv("STORE_LOAD_FATAL_DIR"))
		return
	}

	dir := t.TempDir()
	raw, err := os.ReadFile(filepath.Join("testdata/stage3/node-1", "log.dat"))
	if err != nil {
		t.Fatalf("чтение приспособления: %v", err)
	}
	writeDatFile(t, dir, "log.dat", raw)

	cmd := exec.Command(os.Args[0], "-test.run=^TestNewFileStorageFatalOnLegacyFixture$")
	cmd.Env = append(os.Environ(),
		"STORE_LOAD_FATAL_HELPER=1",
		"STORE_LOAD_FATAL_DIR="+dir,
	)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("дочерний процесс завершился успешно, want отказ; вывод:\n%s", out)
	}
	if !strings.Contains(string(out), "FileStorage") || !strings.Contains(string(out), "log.dat") {
		t.Fatalf("сообщение отказа не содержит путь и причину:\n%s", out)
	}
}

// TestNewLoaderRejectsRealStage3SetBeforeRaft проверяет на реальных байтах
// этапа 3 (скалярный и журнальный ключи, SHA зафиксированы в отчёте), что
// новый загрузчик отвергает старый набор до запуска Raft: старый gob без
// сигнатуры кадра не читается молча и не превращается в пустую базу.
func TestNewLoaderRejectsRealStage3SetBeforeRaft(t *testing.T) {
	const fixtureDir = "testdata/stage3/node-1"

	readFixture := func(t *testing.T, name string) []byte {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join(fixtureDir, name))
		if err != nil {
			t.Fatalf("чтение приспособления %s: %v", name, err)
		}
		return raw
	}

	tests := []struct {
		name  string
		files []string
	}{
		{name: "скалярный ключ", files: []string{"currentTerm.dat"}},
		{name: "журнальный ключ", files: []string{"log.dat"}},
		{name: "смешанный набор", files: []string{"currentTerm.dat", "log.dat"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			for _, name := range tt.files {
				writeDatFile(t, dir, name, readFixture(t, name))
			}

			err := loadAllWithDefaultSeams(dir)
			if err == nil {
				t.Fatal("loadAll вернул nil на реальном старом наборе, want отказ старта")
			}
			if !strings.Contains(err.Error(), ".dat") {
				t.Fatalf("ошибка %q не содержит путь файла", err)
			}
		})
	}
}
