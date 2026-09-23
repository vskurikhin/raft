package store

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/vskurikhin/raft/pkg/raft/contract"
)

// seamTrace записывает имена операций шва в порядке вызова. Тесты швов
// выполняются последовательно, поэтому отдельная синхронизация не нужна.
type seamTrace struct {
	ops []string
}

// record добавляет имя операции в трассу.
func (tr *seamTrace) record(name string) {
	tr.ops = append(tr.ops, name)
}

// writeSeam собирает шов записи поверх штатных операций с трассировкой.
func (tr *seamTrace) writeSeam() writeSeam {
	base := defaultWriteSeam()
	return writeSeam{
		createTmp: func(dir, name string) (writeAtFile, error) {
			tr.record("createTmp")
			return base.createTmp(dir, name)
		},
		writeHeader: func(f writeAtFile, h []byte) error {
			tr.record("writeHeader")
			return base.writeHeader(f, h)
		},
		writePayload: func(f writeAtFile, p []byte) error {
			tr.record("writePayload")
			return base.writePayload(f, p)
		},
		syncFile: func(f writeAtFile) error {
			tr.record("syncFile")
			return base.syncFile(f)
		},
		closeFile: func(f writeAtFile) error {
			tr.record("closeFile")
			return base.closeFile(f)
		},
		rename: func(dir, from, to string) error {
			tr.record("rename")
			return base.rename(dir, from, to)
		},
		syncDir: func(dir string) error {
			tr.record("syncDir")
			return base.syncDir(dir)
		},
		openLog: func(dir, name string) (writeAtFile, int64, error) {
			tr.record("openLog")
			return base.openLog(dir, name)
		},
		writeLog: func(f writeAtFile, b []byte, off int64) error {
			tr.record("writeLog")
			return base.writeLog(f, b, off)
		},
	}
}

// countOps возвращает число вхождений операции в трассе.
func countOps(ops []string, name string) int {
	count := 0
	for _, op := range ops {
		if op == name {
			count++
		}
	}
	return count
}

// seedLogFile создаёт журнал штатным швом в указанном каталоге.
func seedLogFile(t *testing.T, dir string, entries []contract.LogEntry) {
	t.Helper()
	seed := newFileStorage(dir, defaultWriteSeam(), defaultReadSeam())
	seed.RewriteLog(entries)
}

// TestStoreLogEntriesAppendTrace проверяет горячий путь обычного добавления:
// открытие существующего журнала, одна позиционная запись пакета, одна
// синхронизация файла и закрытие. Переименования и синхронизации каталога
// нет, файл не пересоздаётся.
func TestStoreLogEntriesAppendTrace(t *testing.T) {
	dir := t.TempDir()
	base := []contract.LogEntry{
		contractEntry(0, 0, contract.LogNoop, nil),
		contractEntry(1, 1, contract.LogCommand, []byte{0x0a}),
	}
	seedLogFile(t, dir, base)

	trace := &seamTrace{}
	fs := newFileStorage(dir, trace.writeSeam(), defaultReadSeam())
	if err := fs.loadAll(); err != nil {
		t.Fatalf("loadAll: %v", err)
	}
	trace.ops = nil

	appendEntries := []contract.LogEntry{contractEntry(2, 1, contract.LogCommand, []byte{0x0b})}
	result, err := fs.storeLogEntriesLocked(2, appendEntries)
	if err != nil {
		t.Fatalf("storeLogEntriesLocked: %v", err)
	}

	want := []string{"openLog", "writeLog", "syncFile", "closeFile"}
	if !reflect.DeepEqual(trace.ops, want) {
		t.Fatalf("трасса append = %v, want %v", trace.ops, want)
	}
	if countOps(trace.ops, "syncFile") != 1 || countOps(trace.ops, "syncDir") != 0 || countOps(trace.ops, "rename") != 0 {
		t.Fatalf("счётчики синхронизаций = %v, want 1 файл / 0 каталог / 0 переименование", trace.ops)
	}
	if result.Writes != 1 {
		t.Fatalf("Writes = %d, want 1", result.Writes)
	}
	packet := mustEncodeBatch(t, appendEntries)
	if result.BytesWritten != uint64(len(packet)) {
		t.Fatalf("BytesWritten = %d, want %d", result.BytesWritten, len(packet))
	}
}

// TestStoreLogEntriesNoOpZeroIO проверяет, что логический no-op в позиции
// last+1 без незавершённого хвоста не выполняет ни одного вызова шва.
func TestStoreLogEntriesNoOpZeroIO(t *testing.T) {
	dir := t.TempDir()
	base := []contract.LogEntry{
		contractEntry(0, 0, contract.LogNoop, nil),
		contractEntry(1, 1, contract.LogCommand, []byte{0x0a}),
	}
	seedLogFile(t, dir, base)

	trace := &seamTrace{}
	fs := newFileStorage(dir, trace.writeSeam(), defaultReadSeam())
	if err := fs.loadAll(); err != nil {
		t.Fatalf("loadAll: %v", err)
	}
	trace.ops = nil

	result, err := fs.storeLogEntriesLocked(2, nil)
	if err != nil {
		t.Fatalf("storeLogEntriesLocked: %v", err)
	}
	if len(trace.ops) != 0 {
		t.Fatalf("трасса no-op = %v, want пусто", trace.ops)
	}
	if result.Writes != 0 || result.BytesWritten != 0 {
		t.Fatalf("результат no-op = %+v, want нули", result)
	}
}

// TestStoreLogEntriesIdenticalSuffixNoOp проверяет, что повтор той же замены
// суффикса побайтово не отличается от сохранённого и не выполняет
// ввода-вывода.
func TestStoreLogEntriesIdenticalSuffixNoOp(t *testing.T) {
	dir := t.TempDir()
	base := []contract.LogEntry{
		contractEntry(0, 0, contract.LogNoop, nil),
		contractEntry(1, 1, contract.LogCommand, []byte{0x11}),
		contractEntry(2, 1, contract.LogCommand, []byte{0x12}),
	}
	seedLogFile(t, dir, base)

	trace := &seamTrace{}
	fs := newFileStorage(dir, trace.writeSeam(), defaultReadSeam())
	if err := fs.loadAll(); err != nil {
		t.Fatalf("loadAll: %v", err)
	}
	trace.ops = nil

	result, err := fs.storeLogEntriesLocked(1, []contract.LogEntry{base[1], base[2]})
	if err != nil {
		t.Fatalf("storeLogEntriesLocked: %v", err)
	}
	if len(trace.ops) != 0 {
		t.Fatalf("трасса повтора = %v, want пусто", trace.ops)
	}
	if result.Writes != 0 || result.BytesWritten != 0 {
		t.Fatalf("результат повтора = %+v, want нули", result)
	}
}

// TestStoreLogEntriesConflictTrace проверяет конфликтный путь: замена
// суффикса выполняется атомарной публикацией нового файла, без добавления в
// старый дескриптор.
func TestStoreLogEntriesConflictTrace(t *testing.T) {
	dir := t.TempDir()
	base := []contract.LogEntry{
		contractEntry(0, 0, contract.LogNoop, nil),
		contractEntry(1, 1, contract.LogCommand, []byte{0x11}),
		contractEntry(2, 1, contract.LogCommand, []byte{0x12}),
	}
	seedLogFile(t, dir, base)

	trace := &seamTrace{}
	fs := newFileStorage(dir, trace.writeSeam(), defaultReadSeam())
	if err := fs.loadAll(); err != nil {
		t.Fatalf("loadAll: %v", err)
	}
	trace.ops = nil

	replaced := []contract.LogEntry{contractEntry(1, 2, contract.LogCommand, []byte{0x22})}
	result, err := fs.storeLogEntriesLocked(1, replaced)
	if err != nil {
		t.Fatalf("storeLogEntriesLocked: %v", err)
	}

	want := []string{"createTmp", "writeHeader", "writePayload", "syncFile", "closeFile", "rename", "syncDir"}
	if !reflect.DeepEqual(trace.ops, want) {
		t.Fatalf("трасса конфликта = %v, want %v", trace.ops, want)
	}
	if result.Writes != 1 {
		t.Fatalf("Writes = %d, want 1", result.Writes)
	}
	onDisk := readDatFile(t, dir, _logKey)
	if uint64(len(onDisk)) != result.BytesWritten {
		t.Fatalf("BytesWritten = %d, размер файла = %d", result.BytesWritten, len(onDisk))
	}
}

// TestStoreLogEntriesAppendFailureMatrix проверяет, что каждый отказ шва
// обычного добавления не даёт успешного результата и не публикует новый
// кэш. Прежний журнал в кэше сохраняется, счётчики результата не
// публикуются.
func TestStoreLogEntriesAppendFailureMatrix(t *testing.T) {
	injected := errors.New("инъекция отказа")
	base := []contract.LogEntry{
		contractEntry(0, 0, contract.LogNoop, nil),
		contractEntry(1, 1, contract.LogCommand, []byte{0x0a}),
	}
	appendEntries := []contract.LogEntry{contractEntry(2, 1, contract.LogCommand, []byte{0x0b})}

	tests := []struct {
		name     string
		inject   func(w *writeSeam)
		fileSame bool
	}{
		{
			name: "openLog",
			inject: func(w *writeSeam) {
				w.openLog = func(string, string) (writeAtFile, int64, error) { return nil, 0, injected }
			},
			fileSame: true,
		},
		{
			name: "writeLog",
			inject: func(w *writeSeam) {
				w.writeLog = func(writeAtFile, []byte, int64) error { return injected }
			},
		},
		{
			name: "короткая запись пакета",
			inject: func(w *writeSeam) {
				w.writeLog = func(f writeAtFile, b []byte, off int64) error {
					return writeAllAt(shortWriteAt{f}, b, off)
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			seedLogFile(t, dir, base)
			beforeFile := readDatFile(t, dir, _logKey)

			seam := defaultWriteSeam()
			tt.inject(&seam)
			fs := newFileStorage(dir, seam, defaultReadSeam())
			if err := fs.loadAll(); err != nil {
				t.Fatalf("loadAll: %v", err)
			}
			beforeEntries := append([]encodedLogEntry(nil), fs.journal.entries...)
			beforeEnd := fs.journal.validEnd

			result, err := fs.storeLogEntriesLocked(2, appendEntries)
			if err == nil {
				t.Fatalf("storeLogEntriesLocked = %+v, want отказ", result)
			}
			if result.Writes != 0 || result.BytesWritten != 0 {
				t.Fatalf("результат при отказе = %+v, want нули", result)
			}
			if !reflect.DeepEqual(fs.journal.entries, beforeEntries) || fs.journal.validEnd != beforeEnd {
				t.Fatal("кэш журнала изменён при отказе операции")
			}
			if tt.fileSame {
				if after := readDatFile(t, dir, _logKey); !bytes.Equal(beforeFile, after) {
					t.Fatal("файл журнала изменён до записи пакета")
				}
			}
		})
	}
}

// TestRewriteJournalFailureMatrix проверяет, что отказ любой операции
// атомарной замены не публикует новый кэш и не даёт успешного результата;
// опубликованный файл не меняется до успешного переименования.
func TestRewriteJournalFailureMatrix(t *testing.T) {
	injected := errors.New("инъекция отказа")
	base := []contract.LogEntry{
		contractEntry(0, 0, contract.LogNoop, nil),
		contractEntry(1, 1, contract.LogCommand, []byte{0x0a}),
	}
	next := []contract.LogEntry{contractEntry(0, 1, contract.LogCommand, []byte{0xff})}

	tests := []struct {
		name          string
		inject        func(w *writeSeam)
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
			name: "writePayload",
			inject: func(w *writeSeam) {
				w.writePayload = func(writeAtFile, []byte) error { return injected }
			},
		},
		{
			name: "короткая запись пакета",
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
			seedLogFile(t, dir, base)
			beforeFile := readDatFile(t, dir, _logKey)

			seam := defaultWriteSeam()
			tt.inject(&seam)
			fs := newFileStorage(dir, seam, defaultReadSeam())
			if err := fs.loadAll(); err != nil {
				t.Fatalf("loadAll: %v", err)
			}
			beforeEntries := append([]encodedLogEntry(nil), fs.journal.entries...)
			beforeEnd := fs.journal.validEnd

			encoded, err := encodeLogEntries(next)
			if err != nil {
				t.Fatalf("encodeLogEntries: %v", err)
			}
			written, err := fs.rewriteJournalLocked(encoded)
			if err == nil {
				t.Fatalf("rewriteJournalLocked = %d, want отказ", written)
			}
			if written != 0 {
				t.Fatalf("written = %d при отказе, want 0", written)
			}
			if !reflect.DeepEqual(fs.journal.entries, beforeEntries) || fs.journal.validEnd != beforeEnd {
				t.Fatal("кэш журнала изменён при отказе замены")
			}
			if !tt.fileMayChange {
				if after := readDatFile(t, dir, _logKey); !bytes.Equal(beforeFile, after) {
					t.Fatal("опубликованный журнал изменён до переименования")
				}
			}
		})
	}
}

// TestRewriteJournalPreservesOldFileUntilRename проверяет, что старый файл
// журнала не меняется до успешного переименования: шов переименования
// снимает содержимое целевого файла и убеждается, что оно прежнее.
func TestRewriteJournalPreservesOldFileUntilRename(t *testing.T) {
	dir := t.TempDir()
	base := []contract.LogEntry{contractEntry(0, 0, contract.LogNoop, nil)}
	seedLogFile(t, dir, base)
	before := readDatFile(t, dir, _logKey)

	next := []contract.LogEntry{
		contractEntry(0, 0, contract.LogNoop, nil),
		contractEntry(1, 1, contract.LogCommand, []byte{0x0b}),
	}

	baseSeam := defaultWriteSeam()
	var atRename []byte
	seam := baseSeam
	seam.rename = func(dir, from, to string) error {
		if !strings.HasSuffix(to, _logFileName) {
			return baseSeam.rename(dir, from, to)
		}
		current, err := os.ReadFile(filepath.Join(dir, filepath.Base(to)))
		if err != nil {
			return err
		}
		atRename = current
		return baseSeam.rename(dir, from, to)
	}

	fs := newFileStorage(dir, seam, defaultReadSeam())
	if err := fs.loadAll(); err != nil {
		t.Fatalf("loadAll: %v", err)
	}
	result, err := fs.rewriteLogLocked(next)
	if err != nil {
		t.Fatalf("rewriteLogLocked: %v", err)
	}
	if result.Writes != 1 {
		t.Fatalf("Writes = %d, want 1", result.Writes)
	}
	if !bytes.Equal(atRename, before) {
		t.Fatal("старый файл изменён до переименования")
	}
	if after := readDatFile(t, dir, _logKey); bytes.Equal(after, before) {
		t.Fatal("после успешной замены файл не обновился")
	}
}
