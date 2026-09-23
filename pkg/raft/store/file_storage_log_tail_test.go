package store

import (
	"bytes"
	"errors"
	"testing"

	"github.com/vskurikhin/raft/pkg/raft/contract"
)

// tailBaseEntries — принятый журнал приспособления для проверок
// незавершённого хвоста.
func tailBaseEntries() []contract.LogEntry {
	return []contract.LogEntry{
		contractEntry(0, 0, contract.LogNoop, nil),
		contractEntry(1, 1, contract.LogCommand, []byte{0x0a}),
		contractEntry(2, 1, contract.LogCommand, []byte{0x0b}),
	}
}

// writeTailFile создаёт файл: целая база и обрезанный последний пакет.
func writeTailFile(t *testing.T, dir string, base, incomplete []contract.LogEntry) []byte {
	t.Helper()
	baseFile := mustEncodeFile(t, base)
	packet := mustEncodeBatch(t, incomplete)
	tailFile := append(append([]byte(nil), baseFile...), packet[:len(packet)-3]...)
	writeDatFile(t, dir, _logFileName, tailFile)
	return tailFile
}

// TestStoreLogEntriesNormalizationFailureKeepsAcceptedPrefix проверяет, что
// сбой нормализации незавершённого хвоста не теряет принятый префикс и не
// изменяет файл: следующая успешная операция обязана нормализовать его
// заново. Даже семантический no-op не подтверждает незавершённую
// нормализацию.
func TestStoreLogEntriesNormalizationFailureKeepsAcceptedPrefix(t *testing.T) {
	dir := t.TempDir()
	base := tailBaseEntries()
	incomplete := []contract.LogEntry{contractEntry(3, 1, contract.LogCommand, []byte{0x0c})}
	tailFile := writeTailFile(t, dir, base, incomplete)

	seam := defaultWriteSeam()
	seam.syncFile = func(writeAtFile) error { return errors.New("отказ нормализации") }
	fs := newFileStorage(dir, seam, defaultReadSeam())
	if err := fs.loadAll(); err != nil {
		t.Fatalf("loadAll: %v", err)
	}
	if !fs.journal.tail {
		t.Fatal("незавершённый хвост не отмечен")
	}

	fromIndex := base[len(base)-1].Index + 1
	result, err := fs.storeLogEntriesLocked(fromIndex, incomplete)
	if err == nil {
		t.Fatalf("storeLogEntriesLocked = %+v при отказе нормализации, want отказ", result)
	}
	if got := readDatFile(t, dir, _logKey); !bytes.Equal(got, tailFile) {
		t.Fatal("сбой нормализации изменил файл журнала")
	}

	reopened := newFileStorage(dir, defaultWriteSeam(), defaultReadSeam())
	if err := reopened.loadAll(); err != nil {
		t.Fatalf("повторная загрузка: %v", err)
	}
	if !reopened.journal.tail {
		t.Fatal("повторная загрузка потеряла незавершённый хвост")
	}
	got, err := reopened.LoadLog()
	if err != nil {
		t.Fatalf("LoadLog: %v", err)
	}
	requireSameEntries(t, got, base)
}

// TestLoadLogTwiceOnTailIdempotent проверяет, что повторное чтение журнала с
// незавершённым хвостом даёт тот же результат и не мутирует файл: хвост
// переписывается только первой записью, а чтение остаётся только чтением.
func TestLoadLogTwiceOnTailIdempotent(t *testing.T) {
	dir := t.TempDir()
	base := tailBaseEntries()
	incomplete := []contract.LogEntry{contractEntry(3, 1, contract.LogCommand, []byte{0x0c})}
	writeTailFile(t, dir, base, incomplete)
	before := readDatFile(t, dir, _logKey)

	fs := newFileStorage(dir, defaultWriteSeam(), defaultReadSeam())
	if err := fs.loadAll(); err != nil {
		t.Fatalf("loadAll: %v", err)
	}
	first, err := fs.LoadLog()
	if err != nil {
		t.Fatalf("первый LoadLog: %v", err)
	}
	second, err := fs.LoadLog()
	if err != nil {
		t.Fatalf("второй LoadLog: %v", err)
	}
	requireSameEntries(t, first, base)
	requireSameEntries(t, second, base)
	if after := readDatFile(t, dir, _logKey); !bytes.Equal(before, after) {
		t.Fatal("повторный LoadLog изменил файл журнала")
	}
}

// TestStoreLogEntriesConflictAfterTailRecovery проверяет конфликтную замену
// сразу после восстановления с незавершённым хвостом: сначала хвост
// нормализуется атомарной заменой, затем сохранённый префикс и новые записи
// собираются в один файл. Прежний закреплённый префикс сохраняется, хвост
// исчезает.
func TestStoreLogEntriesConflictAfterTailRecovery(t *testing.T) {
	dir := t.TempDir()
	base := tailBaseEntries()
	incomplete := []contract.LogEntry{contractEntry(3, 1, contract.LogCommand, []byte{0x0c})}
	writeTailFile(t, dir, base, incomplete)

	fs := newFileStorage(dir, defaultWriteSeam(), defaultReadSeam())
	if err := fs.loadAll(); err != nil {
		t.Fatalf("loadAll: %v", err)
	}
	if !fs.journal.tail {
		t.Fatal("незавершённый хвост не отмечен")
	}

	replaced := []contract.LogEntry{
		contractEntry(1, 5, contract.LogCommand, []byte{0x21}),
		contractEntry(2, 5, contract.LogCommand, []byte{0x22}),
	}
	result, err := fs.storeLogEntriesLocked(1, replaced)
	if err != nil {
		t.Fatalf("конфликтная замена: %v", err)
	}
	if result.Writes == 0 {
		t.Fatalf("конфликтная замена не выполнила ввода-вывода: %+v", result)
	}
	if fs.journal.tail {
		t.Fatal("после нормализации хвост всё ещё отмечен")
	}

	want := []contract.LogEntry{base[0], replaced[0], replaced[1]}
	got, err := fs.LoadLog()
	if err != nil {
		t.Fatalf("LoadLog: %v", err)
	}
	requireSameEntries(t, got, want)

	reopened := newFileStorage(dir, defaultWriteSeam(), defaultReadSeam())
	if err := reopened.loadAll(); err != nil {
		t.Fatalf("повторная загрузка: %v", err)
	}
	if reopened.journal.tail {
		t.Fatal("повторное открытие видит хвост после конфликтной замены")
	}
	got, err = reopened.LoadLog()
	if err != nil {
		t.Fatalf("LoadLog после переоткрытия: %v", err)
	}
	requireSameEntries(t, got, want)
}
