package store

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/vskurikhin/raft/pkg/raft/contract"
)

// journalMedium — детерминированная модель устойчивого носителя для
// пакетных операций журнала. Модель хранит закреплённые байты опубликованных
// файлов и незакреплённую (volatile) область записи. Обычное добавление
// дописывает пакет за конец закреплённого журнала; атомарная замена пишет
// временный файл и публикует его переименованием. Сбой на выбранной операции
// отбрасывает незакреплённое состояние, кроме явно заданного числа байт новой
// области: так проверяются все короткие EOF-префиксы пакета, полностью
// сохранившийся пакет до синхронизации и полный по длине, но повреждённый
// пакет.
//
// Модель не доказывает свойства контроллера и файловой системы: соблюдение
// flush накопителем и сохранность прежде закреплённого префикса — внешние
// предпосылки, принятые вместе с целевым решением.
type journalMedium struct {
	files   map[string][]byte // закреплённые данные по полным путям
	names   map[string]bool   // закреплённые записи каталога
	staging map[string][]byte // незакреплённые байты по полным путям

	pendingRenames []mediumRename

	// crashOp — имя операции шва, на которой происходит сбой. Пустая строка
	// означает отсутствие сбоя.
	crashOp string

	// retainUnsynced — сколько байт новой (незакреплённой) области
	// сохраняется при сбое. Ноль отбрасывает всю новую область.
	retainUnsynced int

	// corruptAt — смещение внутри сохранённой новой области, байт которого
	// инвертируется после сбоя. Отрицательное значение отключает порчу.
	corruptAt int

	fired   bool
	crashed bool
	trace   []string
}

// journalFile — дескриптор записи модели журнала: байты попадают в
// незакреплённую область до синхронизации.
type journalFile struct {
	medium *journalMedium
	name   string
}

// WriteAt пишет байты в незакреплённую область модели по смещению.
func (f *journalFile) WriteAt(b []byte, off int64) (int, error) {
	f.medium.writeStaging(f.name, b, off)
	return len(b), nil
}

// writeStaging записывает байты в незакреплённую область.
func (m *journalMedium) writeStaging(name string, b []byte, off int64) {
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

// newJournalMedium создаёт модель без сбоя и без порчи.
func newJournalMedium() *journalMedium {
	return &journalMedium{
		files:     make(map[string][]byte),
		names:     make(map[string]bool),
		staging:   make(map[string][]byte),
		corruptAt: -1,
	}
}

// seedLog закрепляет в модели готовый файл журнала.
func (m *journalMedium) seedLog(dir string, raw []byte) {
	path := filepath.Join(dir, _logFileName)
	m.files[path] = append([]byte(nil), raw...)
	m.names[path] = true
}

// durableLog возвращает закреплённые байты журнала.
func (m *journalMedium) durableLog(dir string) []byte {
	return m.files[filepath.Join(dir, _logFileName)]
}

// step учитывает операцию шва и при совпадении с точкой сбоя отбрасывает
// незакреплённое состояние.
func (m *journalMedium) step(op string) error {
	if m.crashed {
		return errPowerLoss
	}
	m.trace = append(m.trace, op)
	if m.crashOp == op && !m.fired {
		m.fired = true
		m.crash()
		return errPowerLoss
	}
	return nil
}

// crash фиксирует потерю питания: незакреплённые переименования
// отбрасываются; у опубликованных файлов сохраняется закреплённый префикс и
// заданное число байт новой области. Временные файлы не публикуются.
func (m *journalMedium) crash() {
	m.crashed = true
	for name, staged := range m.staging {
		if !m.names[name] {
			continue
		}
		durable := m.files[name]
		if len(staged) <= len(durable) {
			continue
		}
		newRegion := staged[len(durable):]
		keep := m.retainUnsynced
		if keep > len(newRegion) {
			keep = len(newRegion)
		}
		if keep < 0 {
			keep = 0
		}
		merged := make([]byte, 0, len(durable)+keep)
		merged = append(merged, durable...)
		merged = append(merged, newRegion[:keep]...)
		if m.corruptAt >= 0 && keep > 0 {
			at := m.corruptAt
			if at >= keep {
				at = keep - 1
			}
			merged[len(durable)+at] ^= 0xff
		}
		m.files[name] = merged
	}
	m.staging = make(map[string][]byte)
	m.pendingRenames = nil
}

// writeSeam собирает шов записи поверх модели.
func (m *journalMedium) writeSeam() writeSeam {
	return writeSeam{
		createTmp: func(_, name string) (writeAtFile, error) {
			if err := m.step("createTmp"); err != nil {
				return nil, err
			}
			m.staging[name] = nil
			return &journalFile{medium: m, name: name}, nil
		},
		writeHeader: func(f writeAtFile, h []byte) error {
			if err := m.step("writeHeader"); err != nil {
				return err
			}
			f.(*journalFile).WriteAt(h, 0)
			return nil
		},
		writePayload: func(f writeAtFile, p []byte) error {
			if err := m.step("writePayload"); err != nil {
				return err
			}
			f.(*journalFile).WriteAt(p, _headerSize)
			return nil
		},
		syncFile: func(f writeAtFile) error {
			if err := m.step("syncFile"); err != nil {
				return err
			}
			name := f.(*journalFile).name
			m.files[name] = append([]byte(nil), m.staging[name]...)
			return nil
		},
		closeFile: func(f writeAtFile) error {
			if err := m.step("closeFile"); err != nil {
				return err
			}
			delete(m.staging, f.(*journalFile).name)
			return nil
		},
		rename: func(_, from, to string) error {
			if err := m.step("rename"); err != nil {
				return err
			}
			m.pendingRenames = append(m.pendingRenames, mediumRename{from: from, to: to})
			return nil
		},
		syncDir: func(string) error {
			if err := m.step("syncDir"); err != nil {
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
		openLog: func(dir, name string) (writeAtFile, int64, error) {
			if err := m.step("openLog"); err != nil {
				return nil, 0, err
			}
			full := filepath.Join(dir, name)
			if !m.names[full] {
				return nil, 0, os.ErrNotExist
			}
			// Незакреплённая область продолжает закреплённый файл: запись
			// идёт за его конец, а сбой может сохранить лишь часть новых байт.
			m.staging[full] = append([]byte(nil), m.files[full]...)
			return &journalFile{medium: m, name: full}, int64(len(m.files[full])), nil
		},
		writeLog: func(f writeAtFile, b []byte, off int64) error {
			if err := m.step("writeLog"); err != nil {
				return err
			}
			f.(*journalFile).WriteAt(b, off)
			return nil
		},
	}
}

// readSeam собирает шов чтения поверх закреплённого состояния модели.
func (m *journalMedium) readSeam() readSeam {
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
		readWhole: func(f readFile, size int64) ([]byte, error) {
			probe, ok := f.(*probeReadFile)
			if !ok {
				return nil, fmt.Errorf("неожиданный дескриптор чтения %T", f)
			}
			if int64(len(probe.data)) < size {
				return nil, fmt.Errorf("короткое чтение журнала: %d из %d байт", len(probe.data), size)
			}
			return append([]byte(nil), probe.data...), nil
		},
		closeRead: func(readFile) error { return nil },
	}
}

// journalBaseEntries — базовый принятый журнал модели.
func journalBaseEntries() []contract.LogEntry {
	return []contract.LogEntry{
		contractEntry(0, 0, contract.LogNoop, nil),
		contractEntry(1, 1, contract.LogCommand, []byte{0x0a}),
	}
}

// journalAppendEntries — добавляемый пакет из двух записей (count>1).
func journalAppendEntries() []contract.LogEntry {
	return []contract.LogEntry{
		contractEntry(2, 1, contract.LogCommand, []byte{0x0b}),
		contractEntry(3, 1, contract.LogCommand, []byte{0x0c}),
	}
}

// requireJournalEntries сравнивает фактический журнал с ожидаемым.
func requireJournalEntries(t *testing.T, got, want []contract.LogEntry) {
	t.Helper()
	requireSameEntries(t, got, want)
}

// TestStoreLogEntriesUnsyncedPrefixes проверяет каждую длину сохранённого
// короткого префикса нового пакета: незавершённый хвост отбрасывается
// целиком, прежний закреплённый журнал остаётся побайтово неизменным, а
// частичный пакет никогда не выдаётся частичным логическим журналом. При
// сохранении полного пакета до синхронизации допускается весь новый журнал.
func TestStoreLogEntriesUnsyncedPrefixes(t *testing.T) {
	const dir = "/data"

	base := journalBaseEntries()
	baseFile := mustEncodeFile(t, base)
	appended := journalAppendEntries()
	packet := mustEncodeBatch(t, appended)
	fromIndex := base[len(base)-1].Index + 1

	for keep := 0; keep <= len(packet); keep++ {
		t.Run(fmt.Sprintf("сохранено_%d_из_%d_байт", keep, len(packet)), func(t *testing.T) {
			m := newJournalMedium()
			m.seedLog(dir, baseFile)
			m.crashOp = "syncFile"
			m.retainUnsynced = keep

			fs := newFileStorage(dir, m.writeSeam(), m.readSeam())
			if err := fs.loadAll(); err != nil {
				t.Fatalf("loadAll до записи: %v", err)
			}

			result, err := fs.storeLogEntriesLocked(fromIndex, appended)
			if err == nil {
				t.Fatalf("storeLogEntriesLocked = %+v при сбое, want отказ", result)
			}
			if result.Writes != 0 || result.BytesWritten != 0 {
				t.Fatalf("результат при сбое = %+v, want нули", result)
			}
			if !bytes.HasPrefix(m.durableLog(dir), baseFile) {
				t.Fatal("сбой изменил закреплённый префикс журнала")
			}

			reopened := newFileStorage(dir, defaultWriteSeam(), m.readSeam())
			if err := reopened.loadAll(); err != nil {
				t.Fatalf("повторная загрузка после сбоя: %v", err)
			}
			got, err := reopened.LoadLog()
			if err != nil {
				t.Fatalf("LoadLog после сбоя: %v", err)
			}
			want := base
			if keep == len(packet) {
				want = append(append([]contract.LogEntry(nil), base...), appended...)
			}
			requireJournalEntries(t, got, want)
		})
	}
}

// TestStoreLogEntriesAppendCrashPoints проверяет сбой на каждой операции
// обычного добавления. Сбой до записи сохраняет прежний журнал; сбой после
// успешной синхронизации оставляет полный новый журнал, хотя операция не
// вернулась успешно. Частичный логический журнал не наблюдается нигде.
func TestStoreLogEntriesAppendCrashPoints(t *testing.T) {
	const dir = "/data"

	base := journalBaseEntries()
	baseFile := mustEncodeFile(t, base)
	appended := journalAppendEntries()
	fromIndex := base[len(base)-1].Index + 1

	tests := []struct {
		name            string
		crashOp         string
		wantAppendedLog bool
	}{
		{name: "сбой на openLog", crashOp: "openLog"},
		{name: "сбой на writeLog", crashOp: "writeLog"},
		{name: "сбой на syncFile", crashOp: "syncFile", wantAppendedLog: true},
		{name: "сбой на closeFile", crashOp: "closeFile", wantAppendedLog: true},
		{name: "полный успех", crashOp: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newJournalMedium()
			m.seedLog(dir, baseFile)
			// Полный пакет сохранён целиком, но синхронизация не завершилась.
			m.retainUnsynced = len(mustEncodeBatch(t, appended))
			m.crashOp = tt.crashOp

			fs := newFileStorage(dir, m.writeSeam(), m.readSeam())
			if err := fs.loadAll(); err != nil {
				t.Fatalf("loadAll до записи: %v", err)
			}
			_, err := fs.storeLogEntriesLocked(fromIndex, appended)
			if tt.crashOp == "" {
				if err != nil {
					t.Fatalf("полный успех вернул %v", err)
				}
			} else if err == nil {
				t.Fatal("операция вернула nil при сбое, want отказ")
			}

			if !bytes.HasPrefix(m.durableLog(dir), baseFile) {
				t.Fatal("сбой изменил закреплённый префикс журнала")
			}

			reopened := newFileStorage(dir, defaultWriteSeam(), m.readSeam())
			if err := reopened.loadAll(); err != nil {
				t.Fatalf("повторная загрузка после сбоя: %v", err)
			}
			got, err := reopened.LoadLog()
			if err != nil {
				t.Fatalf("LoadLog после сбоя: %v", err)
			}
			want := base
			if tt.wantAppendedLog || tt.crashOp == "" {
				want = append(append([]contract.LogEntry(nil), base...), appended...)
			}
			requireJournalEntries(t, got, want)
		})
	}
}

// TestRewriteJournalCrashPoints проверяет сбой на каждой операции полной
// замены: до переименования и синхронизации каталога опубликованный журнал
// не меняется, временный файл не публикуется, а после полного успеха виден
// только новый журнал.
func TestRewriteJournalCrashPoints(t *testing.T) {
	const dir = "/data"

	base := journalBaseEntries()
	baseFile := mustEncodeFile(t, base)
	next := []contract.LogEntry{
		contractEntry(0, 0, contract.LogNoop, nil),
		contractEntry(1, 1, contract.LogCommand, []byte{0x0b}),
		contractEntry(2, 1, contract.LogCommand, []byte{0x0c}),
	}

	for _, crashOp := range []string{
		"createTmp", "writeHeader", "writePayload", "syncFile", "closeFile", "rename", "syncDir", "",
	} {
		name := "полный успех"
		if crashOp != "" {
			name = "сбой на " + crashOp
		}
		t.Run(name, func(t *testing.T) {
			m := newJournalMedium()
			m.seedLog(dir, baseFile)
			m.crashOp = crashOp

			encoded, err := encodeLogEntries(next)
			if err != nil {
				t.Fatalf("encodeLogEntries: %v", err)
			}
			fs := newFileStorage(dir, m.writeSeam(), m.readSeam())
			if err := fs.loadAll(); err != nil {
				t.Fatalf("loadAll до записи: %v", err)
			}
			if _, err := fs.rewriteJournalLocked(encoded); err == nil {
				if crashOp != "" {
					t.Fatal("замена вернула nil при сбое, want отказ")
				}
			} else if crashOp == "" {
				t.Fatalf("полный успех вернул %v", err)
			}

			reopened := newFileStorage(dir, defaultWriteSeam(), m.readSeam())
			if err := reopened.loadAll(); err != nil {
				t.Fatalf("повторная загрузка: %v", err)
			}
			got, err := reopened.LoadLog()
			if err != nil {
				t.Fatalf("LoadLog: %v", err)
			}
			// До синхронизации каталога переименование не закреплено: модель
			// детерминированно оставляет прежний файл. Обе стороны окна
			// («старый либо новый файл целиком») допустимы по правилам L2;
			// проверяется именно целостность файла и отсутствие частичного
			// журнала.
			want := base
			if crashOp == "" {
				want = next
			}
			requireJournalEntries(t, got, want)
			if !bytes.HasPrefix(m.durableLog(dir), mustEncodeFile(t, base)) &&
				crashOp != "" && crashOp != "syncDir" {
				t.Fatal("сбой до переименования изменил опубликованный журнал")
			}
		})
	}
}

// TestStoreLogEntriesCorruptedFullPacket проверяет, что полный по длине, но
// повреждённый незакреплённый пакет классифицируется как повреждение:
// молчаливое восстановление запрещено, узел обязан отказать в старте.
func TestStoreLogEntriesCorruptedFullPacket(t *testing.T) {
	const dir = "/data"

	base := journalBaseEntries()
	baseFile := mustEncodeFile(t, base)
	appended := journalAppendEntries()
	packet := mustEncodeBatch(t, appended)
	fromIndex := base[len(base)-1].Index + 1

	m := newJournalMedium()
	m.seedLog(dir, baseFile)
	m.crashOp = "syncFile"
	// Сохраняется весь пакет, но один байт контрольной суммы повреждён.
	m.retainUnsynced = len(packet)
	m.corruptAt = len(packet) - 13

	fs := newFileStorage(dir, m.writeSeam(), m.readSeam())
	if err := fs.loadAll(); err != nil {
		t.Fatalf("loadAll до записи: %v", err)
	}
	if _, err := fs.storeLogEntriesLocked(fromIndex, appended); err == nil {
		t.Fatal("операция вернула nil при сбое, want отказ")
	}

	reopened := newFileStorage(dir, defaultWriteSeam(), m.readSeam())
	err := reopened.loadAll()
	if err == nil {
		t.Fatal("loadAll принял полный повреждённый пакет, want отказ старта")
	}
	if !errors.Is(err, errLogCorrupt) {
		t.Fatalf("ошибка %v не помечена как повреждение журнала", err)
	}
}

// TestStoreLogEntriesClosesDescriptorEveryCall проверяет ресурсный контракт
// обычного добавления и полной замены: открытие и закрытие выполняются в
// одном стеке операции, поэтому число закрытий не меньше числа открытий и
// дескриптор не удерживается между вызовами. Завершение операции не зависит
// от сборщика мусора: closeFile вызывается явно, а не финализатором.
func TestStoreLogEntriesClosesDescriptorEveryCall(t *testing.T) {
	dir := t.TempDir()
	base := journalBaseEntries()
	seedLogFile(t, dir, base)

	trace := &seamTrace{}
	fs := newFileStorage(dir, trace.writeSeam(), defaultReadSeam())
	if err := fs.loadAll(); err != nil {
		t.Fatalf("loadAll: %v", err)
	}
	trace.ops = nil

	const rounds = 20
	for i := 0; i < rounds; i++ {
		index := base[len(base)-1].Index + 1 + i
		entry := []contract.LogEntry{contractEntry(index, 1, contract.LogCommand, []byte{byte(i)})}
		if _, err := fs.storeLogEntriesLocked(index, entry); err != nil {
			t.Fatalf("добавление %d: %v", i, err)
		}
	}
	// Полная замена использует createTmp/closeFile.
	if _, err := fs.rewriteLogLocked(base); err != nil {
		t.Fatalf("полная замена: %v", err)
	}

	opens := countOps(trace.ops, "openLog") + countOps(trace.ops, "createTmp")
	closes := countOps(trace.ops, "closeFile")
	if opens == 0 {
		t.Fatal("шов не зафиксировал ни одного открытия дескриптора")
	}
	if closes < opens {
		t.Fatalf("открытий %d, закрытий %d: дескриптор удерживается между вызовами", opens, closes)
	}
}
