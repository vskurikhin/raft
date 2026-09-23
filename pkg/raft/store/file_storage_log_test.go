package store

import (
	"bytes"
	"encoding/gob"
	"errors"
	"sync"
	"testing"

	"github.com/vskurikhin/raft/pkg/raft/contract"
)

// logNestedData — зарегистрированный тип Data с вложенными изменяемыми
// значениями: проверяет, что хранилище не удерживает граф вызывающего.
type logNestedData struct {
	Items []int
	Blob  []byte
}

// registerLogNestedData регистрирует тестовый тип в gob. Повторная
// регистрация того же типа безопасна.
func registerLogNestedData() {
	gob.Register(logNestedData{})
}

// TestFileStorageLogCounters проверяет наблюдаемые байты и число записей:
// обычное добавление учитывает только свой пакет, замена и усечение — размер
// нового файла, логический no-op — нули.
func TestFileStorageLogCounters(t *testing.T) {
	dir := t.TempDir()
	fs := NewFileStorage(dir)
	base := []contract.LogEntry{
		contractEntry(0, 0, contract.LogNoop, nil),
		contractEntry(1, 1, contract.LogCommand, []byte{0x0a}),
	}
	appended := []contract.LogEntry{contractEntry(2, 1, contract.LogCommand, []byte{0x0b})}

	if got := fs.RewriteLog(base); got.Writes != 1 || got.BytesWritten != uint64(len(mustEncodeFile(t, base))) {
		t.Fatalf("RewriteLog = %+v, want 1 запись и размер файла", got)
	}
	if got := fs.StoreLogEntries(2, appended); got.Writes != 1 || got.BytesWritten != uint64(len(mustEncodeBatch(t, appended))) {
		t.Fatalf("StoreLogEntries(append) = %+v, want 1 запись и размер пакета", got)
	}
	if got := fs.StoreLogEntries(3, nil); got.Writes != 0 || got.BytesWritten != 0 {
		t.Fatalf("StoreLogEntries(no-op) = %+v, want нули", got)
	}

	replaced := []contract.LogEntry{contractEntry(1, 2, contract.LogCommand, []byte{0x22})}
	conflict := fs.StoreLogEntries(1, replaced)
	if conflict.Writes != 1 {
		t.Fatalf("StoreLogEntries(conflict).Writes = %d, want 1", conflict.Writes)
	}
	if conflict.BytesWritten != uint64(len(readDatFile(t, dir, _logKey))) {
		t.Fatalf("StoreLogEntries(conflict).BytesWritten = %d, want размер файла", conflict.BytesWritten)
	}

	truncate := fs.StoreLogEntries(1, nil)
	if truncate.Writes != 1 {
		t.Fatalf("StoreLogEntries(truncate).Writes = %d, want 1", truncate.Writes)
	}
	if truncate.BytesWritten != uint64(len(readDatFile(t, dir, _logKey))) {
		t.Fatalf("StoreLogEntries(truncate).BytesWritten = %d, want размер файла", truncate.BytesWritten)
	}
}

// TestFileStorageRewriteEmptyJournal создаёт пустой журнал: файл состоит из
// заголовка и пустого пакета, HasData истинен, LoadLog возвращает пустой
// срез без ошибки.
func TestFileStorageRewriteEmptyJournal(t *testing.T) {
	dir := t.TempDir()
	fs := NewFileStorage(dir)
	fs.RewriteLog(nil)

	if !fs.HasData() {
		t.Fatal("HasData = false после создания пустого журнала, want true")
	}
	entries, err := fs.LoadLog()
	if err != nil {
		t.Fatalf("LoadLog: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("записей пустого журнала = %d, want 0", len(entries))
	}
	if raw := readDatFile(t, dir, _logKey); !bytes.Equal(raw, mustEncodeFile(t, nil)) {
		t.Fatal("файл пустого журнала не совпадает с заголовком и пустым пакетом")
	}
}

// TestFileStorageStoreLogEntriesMissingJournal проверяет, что запись суффикса
// на отсутствующем журнале — ошибка контракта: первый персист выполняет
// RewriteLog.
func TestFileStorageStoreLogEntriesMissingJournal(t *testing.T) {
	fs := NewFileStorage(t.TempDir())
	_, err := fs.storeLogEntriesLocked(0, []contract.LogEntry{contractEntry(0, 0, contract.LogNoop, nil)})
	if err == nil {
		t.Fatal("storeLogEntriesLocked на отсутствующем журнале вернул nil, want ошибку")
	}
	if !errors.Is(err, errLogMissing) {
		t.Fatalf("ошибка %v не является признаком отсутствующего журнала", err)
	}
}

// TestFileStorageLogNestedDataIndependence проверяет независимость вложенного
// изменяемого графа: мутация входа после записи и результата LoadLog не
// затрагивает хранилище.
func TestFileStorageLogNestedDataIndependence(t *testing.T) {
	registerLogNestedData()
	fs := NewFileStorage(t.TempDir())

	items := []int{7, 8}
	blob := []byte{1, 2, 3}
	fs.RewriteLog([]contract.LogEntry{
		contractEntry(0, 0, contract.LogCommand, logNestedData{Items: items, Blob: blob}),
	})

	// Мутация входа после возврата не меняет сохранённое значение.
	items[0] = 99
	blob[0] = 99
	got, err := fs.LoadLog()
	if err != nil {
		t.Fatalf("LoadLog: %v", err)
	}
	first, ok := got[0].Data.(logNestedData)
	if !ok {
		t.Fatalf("Data = %T, want logNestedData", got[0].Data)
	}
	if first.Items[0] != 7 || first.Blob[0] != 1 {
		t.Fatalf("сохранённое значение повреждено входом: %+v", first)
	}

	// Мутация результата LoadLog не меняет хранилище и следующий результат.
	first.Items[0] = 42
	first.Blob[0] = 42
	again, err := fs.LoadLog()
	if err != nil {
		t.Fatalf("повторный LoadLog: %v", err)
	}
	second := again[0].Data.(logNestedData)
	if second.Items[0] != 7 || second.Blob[0] != 1 {
		t.Fatalf("хранилище изменено результатом LoadLog: %+v", second)
	}
}

// TestFileStorageLogConflictAndTruncateLoad проверяет логическое содержимое
// после замены суффикса и чистого усечения.
func TestFileStorageLogConflictAndTruncateLoad(t *testing.T) {
	fs := NewFileStorage(t.TempDir())
	base := []contract.LogEntry{
		contractEntry(0, 0, contract.LogNoop, nil),
		contractEntry(1, 1, contract.LogCommand, []byte{0x11}),
		contractEntry(2, 1, contract.LogCommand, []byte{0x12}),
		contractEntry(3, 2, contract.LogCommand, []byte{0x13}),
	}
	fs.RewriteLog(base)

	replaced := []contract.LogEntry{
		contractEntry(2, 3, contract.LogCommand, []byte{0x22}),
		contractEntry(3, 3, contract.LogCommand, []byte{0x23}),
	}
	fs.StoreLogEntries(2, replaced)
	got, err := fs.LoadLog()
	if err != nil {
		t.Fatalf("LoadLog: %v", err)
	}
	requireSameEntries(t, got, []contract.LogEntry{base[0], base[1], replaced[0], replaced[1]})

	fs.StoreLogEntries(1, nil)
	got, err = fs.LoadLog()
	if err != nil {
		t.Fatalf("LoadLog после усечения: %v", err)
	}
	requireSameEntries(t, got, base[:1])
}

// TestMapStorageLogResultZeroAndHasData проверяет семантику журнала в
// реализации в памяти: те же логические эффекты, нулевой результат операций,
// HasData учитывает пустой журнал.
func TestMapStorageLogResultZeroAndHasData(t *testing.T) {
	ms := NewMapStorage()
	if ms.HasData() {
		t.Fatal("свежее хранилище сообщает о наличии данных")
	}
	if got := ms.RewriteLog(nil); got.Writes != 0 || got.BytesWritten != 0 {
		t.Fatalf("RewriteLog(пустой) = %+v, want нули", got)
	}
	if !ms.HasData() {
		t.Fatal("HasData = false после создания пустого журнала, want true")
	}

	base := []contract.LogEntry{
		contractEntry(0, 0, contract.LogNoop, nil),
		contractEntry(1, 1, contract.LogCommand, []byte{0x31}),
	}
	ms.RewriteLog(base)
	appended := []contract.LogEntry{contractEntry(2, 1, contract.LogCommand, []byte{0x32})}
	if got := ms.StoreLogEntries(2, appended); got.Writes != 0 || got.BytesWritten != 0 {
		t.Fatalf("StoreLogEntries = %+v, want нули", got)
	}
	got, err := ms.LoadLog()
	if err != nil {
		t.Fatalf("LoadLog: %v", err)
	}
	requireSameEntries(t, got, append(append([]contract.LogEntry(nil), base...), appended...))

	ms.Set("scalar", []byte{0x01})
	if !ms.HasData() {
		t.Fatal("HasData = false при скаляре и журнале, want true")
	}
}

// TestFileStorageLogConcurrentAccess проверяет, что операции журнала
// сериализуются мьютексом: параллельные чтения, проверки наличия данных и
// замены не дают гонок и не возвращают ошибок. Прогон под -race ловит
// разделяемое изменяемое состояние.
func TestFileStorageLogConcurrentAccess(t *testing.T) {
	fs := NewFileStorage(t.TempDir())
	base := []contract.LogEntry{contractEntry(0, 0, contract.LogNoop, nil)}
	fs.RewriteLog(base)

	var wg sync.WaitGroup
	for reader := 0; reader < 4; reader++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				if _, err := fs.LoadLog(); err != nil {
					t.Errorf("LoadLog: %v", err)
				}
				_ = fs.HasData()
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			fs.RewriteLog(base)
		}
	}()
	wg.Wait()
}
