package store

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
)

// shortWriteAt — обёртка дескриптора записи, урезающая каждую запись на
// один байт: проверка короткой записи должна вернуть ошибку.
type shortWriteAt struct {
	inner writeAtFile
}

// WriteAt выполняет короткую запись во внутренний дескриптор.
func (s shortWriteAt) WriteAt(b []byte, off int64) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	return s.inner.WriteAt(b[:len(b)-1], off)
}

// TestSetLocked_WriteFaultMatrix проверяет матрицу отказов записи: ошибка
// каждой операции шва и короткая запись не скрываются, не дают успешного
// возврата, не обновляют кэш, записи и наблюдения, а прежний файл данных
// остаётся доступным (кроме отказа синхронизации каталога, где
// переименование уже видно процессу).
func TestSetLocked_WriteFaultMatrix(t *testing.T) {
	injected := errors.New("инъекция отказа")

	tests := []struct {
		name string
		// inject портит одну операцию базового шва.
		inject func(w *writeSeam)
		// fileMayChange отмечает отказ после успешного переименования:
		// новый файл уже виден процессу, его долговечность не обещается.
		fileMayChange bool
	}{
		{
			name: "createTmp",
			inject: func(w *writeSeam) {
				w.createTmp = func(string, string) (writeAtFile, error) { return nil, injected }
			},
		},
		{
			name: "writeHeader",
			inject: func(w *writeSeam) {
				w.writeHeader = func(writeAtFile, []byte) error { return injected }
			},
		},
		{
			name: "короткая запись заголовка",
			inject: func(w *writeSeam) {
				w.writeHeader = func(f writeAtFile, h []byte) error { return writeAllAt(shortWriteAt{f}, h, 0) }
			},
		},
		{
			name: "writePayload",
			inject: func(w *writeSeam) {
				w.writePayload = func(writeAtFile, []byte) error { return injected }
			},
		},
		{
			name: "короткая запись payload",
			inject: func(w *writeSeam) {
				w.writePayload = func(f writeAtFile, p []byte) error {
					return writeAllAt(shortWriteAt{f}, p, _headerSize)
				}
			},
		},
		{
			name: "syncFile",
			inject: func(w *writeSeam) {
				w.syncFile = func(writeAtFile) error { return injected }
			},
		},
		{
			name: "closeFile",
			inject: func(w *writeSeam) {
				w.closeFile = func(writeAtFile) error { return injected }
			},
		},
		{
			name: "rename",
			inject: func(w *writeSeam) {
				w.rename = func(string, string, string) error { return injected }
			},
		},
		{
			name: "syncDir",
			inject: func(w *writeSeam) {
				w.syncDir = func(string) error { return injected }
			},
			fileMayChange: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()

			// Прежнее значение записывается штатным путём на диск.
			seed := newFileStorage(dir, defaultWriteSeam(), defaultReadSeam())
			seed.Set("k", []byte("old"))
			before := readDatFile(t, dir, "k")

			seam := defaultWriteSeam()
			tt.inject(&seam)

			fs := newFileStorage(dir, seam, defaultReadSeam())
			if err := fs.loadAll(); err != nil {
				t.Fatalf("loadAll: %v", err)
			}

			err := fs.setLocked("k", []byte("new"))
			if err == nil {
				t.Fatal("setLocked вернул nil, want отказ")
			}

			// Успешного возврата нет: кэш, счётчик записей и наблюдения не
			// изменились.
			if got, ok := fs.Get("k"); !ok || string(got) != "old" {
				t.Fatalf("кэш = %q, ok=%v, want old", got, ok)
			}
			if got := fs.WriteCount(); got != 0 {
				t.Fatalf("WriteCount = %d, want 0", got)
			}
			writes, fileSyncNS, dirSyncNS := fs.PersistenceStats()
			if writes != 0 || fileSyncNS != 0 || dirSyncNS != 0 {
				t.Fatalf("наблюдения = (%d, %d, %d), want все нули", writes, fileSyncNS, dirSyncNS)
			}

			if !tt.fileMayChange {
				after := readDatFile(t, dir, "k")
				if !bytes.Equal(before, after) {
					t.Fatalf("файл данных изменён при отказе %s: %x -> %x", tt.name, before, after)
				}
			}
		})
	}
}

// TestSetLocked_WriterTrace проверяет порядок операций реального writer:
// заголовок и payload пишутся двумя записями, затем ровно две
// синхронизации — файла и каталога.
func TestSetLocked_WriterTrace(t *testing.T) {
	var trace []string
	base := defaultWriteSeam()
	seam := writeSeam{
		createTmp: func(dir, name string) (writeAtFile, error) {
			trace = append(trace, "createTmp")
			return base.createTmp(dir, name)
		},
		writeHeader: func(f writeAtFile, h []byte) error {
			trace = append(trace, "writeHeader")
			return base.writeHeader(f, h)
		},
		writePayload: func(f writeAtFile, p []byte) error {
			trace = append(trace, "writePayload")
			return base.writePayload(f, p)
		},
		syncFile: func(f writeAtFile) error {
			trace = append(trace, "syncFile")
			return base.syncFile(f)
		},
		closeFile: func(f writeAtFile) error {
			trace = append(trace, "closeFile")
			return base.closeFile(f)
		},
		rename: func(dir, from, to string) error {
			trace = append(trace, "rename")
			return base.rename(dir, from, to)
		},
		syncDir: func(dir string) error {
			trace = append(trace, "syncDir")
			return base.syncDir(dir)
		},
	}

	fs := newFileStorage(t.TempDir(), seam, defaultReadSeam())
	if err := fs.setLocked("k", []byte("value")); err != nil {
		t.Fatalf("setLocked: %v", err)
	}

	want := []string{"createTmp", "writeHeader", "writePayload", "syncFile", "closeFile", "rename", "syncDir"}
	if fmt.Sprint(trace) != fmt.Sprint(want) {
		t.Fatalf("трасса writer = %v, want %v", trace, want)
	}
}

// TestSetLocked_HeaderStaysOnInstance проверяет, что буфер заголовка,
// хранимый в поле FileStorage, не уходит в кучу: на изменённый Set
// добавляется не более трёх строк пути и одной защитной копии значения.
// Если бы заголовок собирался в локальном массиве и передавался через
// поле-функцию шва, добавился бы ещё один объект — 24 байта на изменённый
// Set.
func TestSetLocked_HeaderStaysOnInstance(t *testing.T) {
	stub := writeSeam{
		createTmp:    func(_, _ string) (writeAtFile, error) { return nopWriteFile{}, nil },
		writeHeader:  func(writeAtFile, []byte) error { return nil },
		writePayload: func(writeAtFile, []byte) error { return nil },
		syncFile:     func(writeAtFile) error { return nil },
		closeFile:    func(writeAtFile) error { return nil },
		rename:       func(string, string, string) error { return nil },
		syncDir:      func(string) error { return nil },
	}
	fs := newFileStorage(t.TempDir(), stub, defaultReadSeam())
	value := make([]byte, 64)

	allocs := testing.AllocsPerRun(100, func() {
		value[0]++
		if err := fs.setLocked("k", value); err != nil {
			t.Fatalf("setLocked: %v", err)
		}
	})
	if allocs > 4 {
		t.Fatalf("setLocked аллоцирует %.0f объектов, want не более 4: буфер заголовка ушёл в кучу", allocs)
	}
}

// errPowerLoss — маркер потери питания в модели устойчивого носителя.
var errPowerLoss = errors.New("внезапная потеря питания")

// mediumFile — дескриптор временного файла модели носителя.
type mediumFile struct {
	medium *powerLossMedium
	name   string
}

// WriteAt пишет байты в непостоянную область модели по смещению.
func (f *mediumFile) WriteAt(b []byte, off int64) (int, error) {
	f.medium.writeStaging(f.name, b, off)
	return len(b), nil
}

// powerLossMedium — детерминированная модель устойчивого носителя поверх
// того же файлового шва: синхронизация файла закрепляет данные,
// синхронизация каталога закрепляет переименование, потеря питания
// отбрасывает всё незакреплённое. Модель не доказывает свойства
// контроллера: соблюдение flush накопителем — внешняя предпосылка.
type powerLossMedium struct {
	files map[string][]byte // закреплённые данные по именам
	names map[string]bool   // закреплённые записи каталога

	staging        map[string][]byte // незакреплённые данные временного файла
	pendingRenames []mediumRename    // незакреплённые переименования
	crashAt        int               // номер операции (с 1), на которой происходит потеря
	ops            int
	crashed        bool
}

// mediumRename — отложенное до синхронизации каталога переименование.
type mediumRename struct {
	from, to string
}

// newPowerLossMedium создаёт модель с заданной точкой потери питания.
// crashAt == 0 означает отсутствие сбоя.
func newPowerLossMedium(crashAt int) *powerLossMedium {
	return &powerLossMedium{
		files:   make(map[string][]byte),
		names:   make(map[string]bool),
		staging: make(map[string][]byte),
		crashAt: crashAt,
	}
}

// writeStaging записывает байты в незакреплённую область.
func (m *powerLossMedium) writeStaging(name string, b []byte, off int64) {
	buf := m.staging[name]
	need := int(off) + len(b)
	if need > len(buf) {
		grown := make([]byte, need)
		copy(grown, buf)
		buf = grown
	}
	copy(buf[off:], b)
	m.staging[name] = buf
}

// step учитывает операцию и при совпадении с точкой сбоя отбрасывает
// незакреплённое состояние.
func (m *powerLossMedium) step() error {
	m.ops++
	if m.crashed {
		return errPowerLoss
	}
	if m.crashAt != 0 && m.ops == m.crashAt {
		m.crash()
		return errPowerLoss
	}
	return nil
}

// crash имитирует потерю питания: незакреплённые данные и переименования
// отбрасываются.
func (m *powerLossMedium) crash() {
	m.crashed = true
	m.staging = make(map[string][]byte)
	m.pendingRenames = nil
}

// writeSeam собирает шов записи поверх модели.
func (m *powerLossMedium) writeSeam() writeSeam {
	return writeSeam{
		createTmp: func(_, name string) (writeAtFile, error) {
			if err := m.step(); err != nil {
				return nil, err
			}
			m.staging[name] = nil
			return &mediumFile{medium: m, name: name}, nil
		},
		writeHeader: func(f writeAtFile, h []byte) error {
			if err := m.step(); err != nil {
				return err
			}
			f.(*mediumFile).WriteAt(h, 0)
			return nil
		},
		writePayload: func(f writeAtFile, p []byte) error {
			if err := m.step(); err != nil {
				return err
			}
			f.(*mediumFile).WriteAt(p, _headerSize)
			return nil
		},
		syncFile: func(f writeAtFile) error {
			if err := m.step(); err != nil {
				return err
			}
			name := f.(*mediumFile).name
			m.files[name] = append([]byte(nil), m.staging[name]...)
			return nil
		},
		closeFile: func(f writeAtFile) error {
			if err := m.step(); err != nil {
				return err
			}
			delete(m.staging, f.(*mediumFile).name)
			return nil
		},
		rename: func(_, from, to string) error {
			if err := m.step(); err != nil {
				return err
			}
			m.pendingRenames = append(m.pendingRenames, mediumRename{from: from, to: to})
			return nil
		},
		syncDir: func(string) error {
			if err := m.step(); err != nil {
				return err
			}
			for _, r := range m.pendingRenames {
				m.files[r.to] = m.files[r.from]
				m.names[r.to] = true
				delete(m.files, r.from)
				delete(m.names, r.from)
			}
			m.pendingRenames = nil
			return nil
		},
	}
}

// readSeam собирает шов чтения поверх закреплённого состояния модели: при
// необходимости перед этим вызывается crash, чтобы отбросить незакреплённое.
// Имена в каталоге возвращаются без пути, содержимое берётся по полному
// пути, как это делает штатный шов на os-функциях.
func (m *powerLossMedium) readSeam() readSeam {
	return readSeam{
		readDir: func(string) ([]entry, error) {
			names := make([]string, 0, len(m.names))
			for full := range m.names {
				names = append(names, filepath.Base(full))
			}
			sort.Strings(names)
			entries := make([]entry, 0, len(names))
			for _, name := range names {
				entries = append(entries, entry{name: name, typ: entryRegular})
			}
			return entries, nil
		},
		lstat: func(dir, name string) (entryType, error) {
			if m.names[filepath.Join(dir, name)] {
				return entryRegular, nil
			}
			return entryRegular, os.ErrNotExist
		},
		openRegular: func(dir, name string) (readFile, int64, error) {
			data, ok := m.files[filepath.Join(dir, name)]
			if !ok {
				return nil, 0, os.ErrNotExist
			}
			return &probeReadFile{data: data}, int64(len(data)), nil
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
}

// TestFileStorage_PowerLossModel проверяет модель устойчивого носителя:
// при потере питания на любой операции незакреплённое состояние
// отбрасывается, и загрузчик видит либо прежнее значение, либо, при
// полностью успешном цикле, новое — никогда частичный payload.
func TestFileStorage_PowerLossModel(t *testing.T) {
	// crashAt == 0 — полный успешный цикл; 1..7 — потеря на операции.
	for crashAt := 0; crashAt <= 7; crashAt++ {
		t.Run(fmt.Sprintf("сбой_на_%d", crashAt), func(t *testing.T) {
			m := newPowerLossMedium(crashAt)
			m.files["/data/k.dat"] = frameBytes([]byte("old"))
			m.names["/data/k.dat"] = true

			fs := newFileStorage("/data", m.writeSeam(), m.readSeam())
			if err := fs.loadAll(); err != nil {
				t.Fatalf("loadAll до записи: %v", err)
			}
			err := fs.setLocked("k", []byte("new"))

			if crashAt == 0 {
				if err != nil {
					t.Fatalf("setLocked вернул %v, want успех", err)
				}
			} else if err == nil {
				t.Fatal("setLocked вернул nil при потере питания, want отказ")
			}

			// После сбоя незакреплённое состояние отбрасывается.
			m.crash()

			reopened := newFileStorage("/data", defaultWriteSeam(), m.readSeam())
			if loadErr := reopened.loadAll(); loadErr != nil {
				t.Fatalf("повторная загрузка: %v", loadErr)
			}
			got, ok := reopened.Get("k")
			if !ok {
				t.Fatal("ключ log отсутствует после сбоя")
			}
			want := "old"
			if crashAt == 0 {
				want = "new"
			}
			if string(got) != want {
				t.Fatalf("значение после сбоя = %q, want %q", got, want)
			}
		})
	}
}

// TestFileStorage_PowerLossFreshKey проверяет, что при потере питания
// незакреплённый ключ не появляется и не оставляет частичного payload.
func TestFileStorage_PowerLossFreshKey(t *testing.T) {
	for crashAt := 0; crashAt <= 7; crashAt++ {
		t.Run(fmt.Sprintf("сбой_на_%d", crashAt), func(t *testing.T) {
			m := newPowerLossMedium(crashAt)
			fs := newFileStorage("/data", m.writeSeam(), m.readSeam())
			if err := fs.loadAll(); err != nil {
				t.Fatalf("loadAll: %v", err)
			}
			err := fs.setLocked("fresh", []byte("value"))

			m.crash()
			reopened := newFileStorage("/data", defaultWriteSeam(), m.readSeam())
			if loadErr := reopened.loadAll(); loadErr != nil {
				t.Fatalf("повторная загрузка: %v", loadErr)
			}
			_, ok := reopened.Get("fresh")
			if crashAt == 0 {
				if err != nil {
					t.Fatalf("setLocked вернул %v, want успех", err)
				}
				if !ok {
					t.Fatal("ключ fresh отсутствует после полного успеха")
				}
				return
			}
			if ok {
				t.Fatal("незакреплённый ключ fresh появился после потери питания")
			}
		})
	}
}

// TestFileStorageSeam_NoSharedState проверяет, что шов экземплярный: два
// хранилища с разными внедрёнными швами работают параллельно, и воздействия
// одного не наблюдаются другим. Прогон под -race ловит разделяемое
// изменяемое состояние, если бы шов был общим.
func TestFileStorageSeam_NoSharedState(t *testing.T) {
	t.Parallel()

	type recorder struct {
		mu  sync.Mutex
		got []string
	}
	makeStorage := func(dir string, rec *recorder) *FileStorage {
		seam := writeSeam{
			createTmp: func(_, name string) (writeAtFile, error) {
				rec.mu.Lock()
				rec.got = append(rec.got, name)
				rec.mu.Unlock()
				return nopWriteFile{}, nil
			},
			writeHeader:  func(writeAtFile, []byte) error { return nil },
			writePayload: func(writeAtFile, []byte) error { return nil },
			syncFile:     func(writeAtFile) error { return nil },
			closeFile:    func(writeAtFile) error { return nil },
			rename:       func(string, string, string) error { return nil },
			syncDir:      func(string) error { return nil },
		}
		return newFileStorage(dir, seam, defaultReadSeam())
	}

	recA, recB := &recorder{}, &recorder{}
	fsA := makeStorage("/store-a", recA)
	fsB := makeStorage("/store-b", recB)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			fsA.Set("k", []byte{byte(i)})
		}(i)
		go func(i int) {
			defer wg.Done()
			fsB.Set("k", []byte{byte(i)})
		}(i)
	}
	wg.Wait()

	for _, name := range recA.got {
		if !strings.HasPrefix(name, "/store-a/") {
			t.Fatalf("хранилище A получило чужой путь %q", name)
		}
	}
	for _, name := range recB.got {
		if !strings.HasPrefix(name, "/store-b/") {
			t.Fatalf("хранилище B получило чужой путь %q", name)
		}
	}
	if len(recA.got) == 0 || len(recB.got) == 0 {
		t.Fatal("шов не зафиксировал ни одной записи")
	}
}

// nopWriteFile — дескриптор записи, отбрасывающий данные.
type nopWriteFile struct{}

// WriteAt сообщает полную запись, ничего не сохраняя.
func (nopWriteFile) WriteAt(b []byte, _ int64) (int, error) { return len(b), nil }

// TestSetFatalOnWriteError проверяет, что публичный Set при отказе шва
// завершает процесс: помощник запускается дочерним процессом, отказ
// обнаруживается по ненулевому коду возврата и сообщению.
func TestSetFatalOnWriteError(t *testing.T) {
	if os.Getenv("STORE_FATAL_HELPER") == "1" {
		seam := defaultWriteSeam()
		seam.syncFile = func(writeAtFile) error { return errors.New("отказ синхронизации") }
		fs := newFileStorage(t.TempDir(), seam, defaultReadSeam())
		fs.Set("k", []byte("value"))
		// log.Fatalf завершает процесс: возврат сюда недостижим.
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestSetFatalOnWriteError$")
	cmd.Env = append(os.Environ(), "STORE_FATAL_HELPER=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("дочерний процесс завершился успешно, want отказ; вывод:\n%s", out)
	}
	if !strings.Contains(string(out), "FileStorage.Set") {
		t.Fatalf("вывод дочернего процесса не содержит сообщение Set:\n%s", out)
	}
}

// TestSetKillBeforeAndAfterResponse проверяет реальный SIGKILL процесса,
// пишущего через FileStorage, отдельно от модели потери питания. Помощник
// блокируется внутри шва до ответа либо после ответа; родитель убивает его
// и перечитывает данные новым экземпляром.
func TestSetKillBeforeAndAfterResponse(t *testing.T) {
	if mode := os.Getenv("STORE_KILL_HELPER"); mode != "" {
		runKillHelper(mode)
		return
	}

	tests := []struct {
		name       string
		mode       string
		wantValue  string
		wantOldKey bool
	}{
		// Убийство до ответа: запись не дошла до переименования, прежнее
		// значение остаётся, новый ключ не появляется.
		{name: "kill до ответа", mode: "before", wantOldKey: true},
		// Убийство после ответа: Set вернулся, значение долговечно.
		{name: "kill после ответа", mode: "after", wantValue: "new"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()

			seed := NewFileStorage(dir)
			seed.Set("k", []byte("old"))

			cmd := exec.Command(os.Args[0], "-test.run=^TestSetKillBeforeAndAfterResponse$")
			cmd.Env = append(os.Environ(),
				"STORE_KILL_HELPER="+tt.mode,
				"STORE_KILL_DIR="+dir,
			)
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatalf("StdoutPipe: %v", err)
			}
			if err := cmd.Start(); err != nil {
				t.Fatalf("запуск помощника: %v", err)
			}

			// Синхронизация по каналу: помощник сообщает готовность ровно
			// в той фазе, в которой его нужно убить.
			line, readErr := bufio.NewReader(stdout).ReadString('\n')
			if readErr != nil && line == "" {
				t.Fatalf("чтение готовности: %v", readErr)
			}
			if !strings.Contains(line, "ready") {
				t.Fatalf("помощник не сообщил готовность: %q", line)
			}
			if err := cmd.Process.Kill(); err != nil {
				t.Fatalf("kill: %v", err)
			}
			_ = cmd.Wait()

			reopened := NewFileStorage(dir)
			got, ok := reopened.Get("k")
			if tt.wantOldKey {
				if !ok || string(got) != "old" {
					t.Fatalf("после kill до ответа log = %q, ok=%v, want old", got, ok)
				}
				return
			}
			if !ok || string(got) != tt.wantValue {
				t.Fatalf("после kill после ответа log = %q, ok=%v, want %q", got, ok, tt.wantValue)
			}
		})
	}
}

// runKillHelper выполняет роль дочернего процесса для проверки kill: пишет
// через FileStorage, сообщает готовность и блокируется до убийства.
//
// Режим before останавливается до синхронизации файла: payload уже записан
// во временный файл, но переименование не состоялось, поэтому после
// убийства виден прежний файл данных. Режим after возвращается из Set и
// только затем сообщает готовность — значение долговечно.
func runKillHelper(mode string) {
	dir := os.Getenv("STORE_KILL_DIR")
	if mode == "before" {
		base := defaultWriteSeam()
		seam := base
		seam.syncFile = func(writeAtFile) error {
			_, _ = os.Stdout.WriteString("ready\n")
			select {}
		}
		fs := newFileStorage(dir, seam, defaultReadSeam())
		fs.Set("k", []byte("new"))
		return
	}

	fs := newFileStorage(dir, defaultWriteSeam(), defaultReadSeam())
	fs.Set("k", []byte("new"))
	_, _ = os.Stdout.WriteString("ready\n")
	select {}
}
