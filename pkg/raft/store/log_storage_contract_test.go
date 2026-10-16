package store

import (
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/vskurikhin/raft/pkg/raft/contract"
)

// runLogStorageContract прогоняет общий набор семантических случаев
// контракта LogStorage на одной реализации. Набор предназначен для вызова
// тестами обеих реализаций (файловой и в памяти). Сам набор не является
// реализацией хранилища и не подтверждает корректность store без вызова.
func runLogStorageContract(t *testing.T, newStorage func(t *testing.T) contract.LogStorage) {
	t.Helper()

	t.Run("отсутствующий журнал отличается от пустого", func(t *testing.T) {
		storage := newStorage(t)
		if storage.HasData() {
			t.Fatal("свежее хранилище сообщает о наличии данных")
		}
		if _, err := storage.LoadLog(); !errors.Is(err, contract.ErrLogNotFound) {
			t.Fatalf("LoadLog отсутствующего журнала = %v, want ErrLogNotFound", err)
		}

		storage.RewriteLog(nil)
		if !storage.HasData() {
			t.Fatal("созданный пустой журнал не отражён в HasData")
		}
		entries, err := storage.LoadLog()
		if err != nil {
			t.Fatalf("LoadLog пустого журнала: %v", err)
		}
		if len(entries) != 0 {
			t.Fatalf("записи пустого журнала = %d, want 0", len(entries))
		}
	})

	t.Run("перезапись и чтение", func(t *testing.T) {
		storage := newStorage(t)
		want := []contract.LogEntry{
			contractEntry(0, 0, contract.LogNoop, nil),
			contractEntry(1, 1, contract.LogConfiguration, []byte{0x01, 0x02}),
			contractEntry(2, 1, contract.LogCommand, []byte("команда")),
		}
		storage.RewriteLog(want)
		got, err := storage.LoadLog()
		if err != nil {
			t.Fatalf("LoadLog: %v", err)
		}
		requireSameEntries(t, got, want)
	})

	t.Run("добавление в конец", func(t *testing.T) {
		storage := newStorage(t)
		base := []contract.LogEntry{
			contractEntry(0, 0, contract.LogNoop, nil),
			contractEntry(1, 1, contract.LogCommand, []byte{0x0a}),
		}
		appended := []contract.LogEntry{
			contractEntry(2, 1, contract.LogCommand, []byte{0x0b}),
			contractEntry(3, 2, contract.LogCommand, []byte{0x0c}),
		}
		storage.RewriteLog(base)
		storage.StoreLogEntries(2, appended)
		got, err := storage.LoadLog()
		if err != nil {
			t.Fatalf("LoadLog: %v", err)
		}
		requireSameEntries(t, got, append(slices.Clone(base), appended...))
	})

	t.Run("замена суффикса", func(t *testing.T) {
		storage := newStorage(t)
		base := []contract.LogEntry{
			contractEntry(0, 0, contract.LogNoop, nil),
			contractEntry(1, 1, contract.LogCommand, []byte{0x11}),
			contractEntry(2, 1, contract.LogCommand, []byte{0x12}),
			contractEntry(3, 2, contract.LogCommand, []byte{0x13}),
		}
		replaced := []contract.LogEntry{
			contractEntry(2, 3, contract.LogCommand, []byte{0x22}),
			contractEntry(3, 3, contract.LogCommand, []byte{0x23}),
		}
		want := []contract.LogEntry{base[0], base[1], replaced[0], replaced[1]}
		storage.RewriteLog(base)
		storage.StoreLogEntries(2, replaced)
		got, err := storage.LoadLog()
		if err != nil {
			t.Fatalf("LoadLog: %v", err)
		}
		requireSameEntries(t, got, want)
	})

	t.Run("усечение пустыми записями", func(t *testing.T) {
		storage := newStorage(t)
		base := []contract.LogEntry{
			contractEntry(0, 0, contract.LogNoop, nil),
			contractEntry(1, 1, contract.LogCommand, []byte{0x31}),
			contractEntry(2, 1, contract.LogCommand, []byte{0x32}),
		}
		storage.RewriteLog(base)
		storage.StoreLogEntries(1, nil)
		got, err := storage.LoadLog()
		if err != nil {
			t.Fatalf("LoadLog: %v", err)
		}
		requireSameEntries(t, got, base[:1])
	})

	t.Run("логический no-op на позиции last+1", func(t *testing.T) {
		storage := newStorage(t)
		base := []contract.LogEntry{
			contractEntry(0, 0, contract.LogNoop, nil),
			contractEntry(1, 1, contract.LogCommand, []byte{0x41}),
		}
		storage.RewriteLog(base)
		before, err := storage.LoadLog()
		if err != nil {
			t.Fatalf("LoadLog до no-op: %v", err)
		}
		storage.StoreLogEntries(2, nil)
		after, err := storage.LoadLog()
		if err != nil {
			t.Fatalf("LoadLog после no-op: %v", err)
		}
		requireSameEntries(t, after, before)
	})

	t.Run("вставка в существующий пустой журнал", func(t *testing.T) {
		storage := newStorage(t)
		storage.RewriteLog(nil)
		entries := []contract.LogEntry{contractEntry(3, 1, contract.LogCommand, []byte{0x33})}
		storage.StoreLogEntries(3, entries)
		got, err := storage.LoadLog()
		if err != nil {
			t.Fatalf("LoadLog: %v", err)
		}
		requireSameEntries(t, got, entries)
	})

	t.Run("замена суффикса после снимка", func(t *testing.T) {
		storage := newStorage(t)
		base := []contract.LogEntry{
			contractEntry(5, 2, contract.LogNoop, nil),
			contractEntry(6, 2, contract.LogCommand, []byte{0x51}),
		}
		storage.RewriteLog(base)
		next := []contract.LogEntry{contractEntry(7, 3, contract.LogCommand, []byte{0x71})}
		storage.StoreLogEntries(7, next)
		got, err := storage.LoadLog()
		if err != nil {
			t.Fatalf("LoadLog: %v", err)
		}
		requireSameEntries(t, got, append(append([]contract.LogEntry(nil), base...), next...))

		// Чистое усечение до границы снимка: сохраняется запись с индексом 5.
		storage.StoreLogEntries(6, nil)
		got, err = storage.LoadLog()
		if err != nil {
			t.Fatalf("LoadLog после усечения: %v", err)
		}
		requireSameEntries(t, got, base[:1])
	})

	t.Run("владение входными Data", func(t *testing.T) {
		storage := newStorage(t)
		first := []byte{0x51, 0x52, 0x53}
		second := []byte{0x61, 0x62, 0x63}
		storage.RewriteLog([]contract.LogEntry{contractEntry(0, 0, contract.LogCommand, first)})
		appended := []contract.LogEntry{contractEntry(1, 1, contract.LogCommand, second)}
		storage.StoreLogEntries(1, appended)

		first[0] = 0xee
		second[0] = 0xee
		appended[0].Index = 99

		got, err := storage.LoadLog()
		if err != nil {
			t.Fatalf("LoadLog: %v", err)
		}
		want := []contract.LogEntry{
			contractEntry(0, 0, contract.LogCommand, []byte{0x51, 0x52, 0x53}),
			contractEntry(1, 1, contract.LogCommand, []byte{0x61, 0x62, 0x63}),
		}
		requireSameEntries(t, got, want)
	})

	t.Run("владение результатом LoadLog", func(t *testing.T) {
		storage := newStorage(t)
		value := []byte{0x71, 0x72, 0x73}
		storage.RewriteLog([]contract.LogEntry{contractEntry(0, 0, contract.LogCommand, value)})

		loaded, err := storage.LoadLog()
		if err != nil {
			t.Fatalf("LoadLog: %v", err)
		}
		loaded[0].Data.([]byte)[0] = 0xee
		loaded[0].Index = 99

		again, err := storage.LoadLog()
		if err != nil {
			t.Fatalf("повторный LoadLog: %v", err)
		}
		want := []contract.LogEntry{contractEntry(0, 0, contract.LogCommand, []byte{0x71, 0x72, 0x73})}
		requireSameEntries(t, again, want)
	})

	t.Run("владение вложенным графом Data", func(t *testing.T) {
		registerLogNestedData()
		storage := newStorage(t)
		items := []int{1, 2}
		blob := []byte{3, 4}
		storage.RewriteLog([]contract.LogEntry{
			contractEntry(0, 0, contract.LogCommand, logNestedData{Items: items, Blob: blob}),
		})

		// Мутация входа после возврата не меняет сохранённое значение.
		items[0] = 9
		blob[0] = 9
		got, err := storage.LoadLog()
		if err != nil {
			t.Fatalf("LoadLog: %v", err)
		}
		first, ok := got[0].Data.(logNestedData)
		if !ok {
			t.Fatalf("Data = %T, want logNestedData", got[0].Data)
		}
		if first.Items[0] != 1 || first.Blob[0] != 3 {
			t.Fatalf("сохранённое значение повреждено входом: %+v", first)
		}

		// Мутация результата не меняет хранилище.
		first.Items[0] = 7
		first.Blob[0] = 7
		again, err := storage.LoadLog()
		if err != nil {
			t.Fatalf("повторный LoadLog: %v", err)
		}
		second := again[0].Data.(logNestedData)
		if second.Items[0] != 1 || second.Blob[0] != 3 {
			t.Fatalf("хранилище изменено результатом LoadLog: %+v", second)
		}
	})
}

// contractEntry собирает запись журнала для контрактных случаев.
func contractEntry(index, term int, logType contract.LogType, data any) contract.LogEntry {
	return contract.LogEntry{Index: index, Term: term, Type: logType, Data: data}
}

// contractTestStorage — тестовое приспособление для проверки самого набора
// контрактных случаев. Оно не является реализацией store, не моделирует
// носитель и не заменяет проверку файловой и памяти-реализаций. Копирование
// Data ограничено встроенными срезами, используемыми набором.
type contractTestStorage struct {
	mu      sync.Mutex
	entries []contract.LogEntry
	exists  bool
}

// newContractTestStorage создаёт приспособление для самопроверки набора.
func newContractTestStorage() *contractTestStorage {
	return &contractTestStorage{}
}

func (s *contractTestStorage) Set(string, []byte)        {}
func (s *contractTestStorage) Get(string) ([]byte, bool) { return nil, false }

func (s *contractTestStorage) HasData() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.exists
}

func (s *contractTestStorage) StoreLogEntries(fromIndex int, entries []contract.LogEntry) contract.LogWriteResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.exists = true
	// Индексы абсолютные: сохраняется префикс записей с индексом ниже
	// fromIndex, затем новые записи. Двойник не моделирует файл и границы
	// отвечает только за логический эффект набора случаев.
	kept := make([]contract.LogEntry, 0, len(s.entries)+len(entries))
	for _, entry := range s.entries {
		if entry.Index < fromIndex {
			kept = append(kept, entry)
		}
	}
	if len(entries) > 0 {
		kept = append(kept, cloneContractEntries(entries)...)
	}
	s.entries = kept
	return contract.LogWriteResult{}
}

func (s *contractTestStorage) RewriteLog(entries []contract.LogEntry) contract.LogWriteResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.exists = true
	s.entries = cloneContractEntries(entries)
	return contract.LogWriteResult{}
}

func (s *contractTestStorage) LoadLog() ([]contract.LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.exists {
		return nil, contract.ErrLogNotFound
	}
	return cloneContractEntries(s.entries), nil
}

// cloneContractEntries копирует записи и их вложенные изменяемые значения,
// чтобы приспособление не делило данные с вызывающим.
func cloneContractEntries(entries []contract.LogEntry) []contract.LogEntry {
	out := make([]contract.LogEntry, len(entries))
	for i := range entries {
		out[i] = entries[i]
		switch data := entries[i].Data.(type) {
		case []byte:
			out[i].Data = slices.Clone(data)
		case logNestedData:
			data.Items = slices.Clone(data.Items)
			data.Blob = slices.Clone(data.Blob)
			out[i].Data = data
		}
	}
	return out
}

// TestLogStorageContractHelperSelfCheck проверяет сам набор контрактных
// случаев на тестовом приспособлении. Это не проверка store: реальные
// файловая и памяти-реализации обязаны вызывать набор отдельно.
func TestLogStorageContractHelperSelfCheck(t *testing.T) {
	runLogStorageContract(t, func(*testing.T) contract.LogStorage {
		return newContractTestStorage()
	})
}

// TestFileStorageLogStorageContract прогоняет общий набор контракта на
// файловой реализации: каждый случай выполняется в собственном каталоге
// данных.
func TestFileStorageLogStorageContract(t *testing.T) {
	runLogStorageContract(t, func(t *testing.T) contract.LogStorage {
		return NewFileStorage(t.TempDir())
	})
}

// TestMapStorageLogStorageContract прогоняет общий набор контракта на
// реализации в памяти.
func TestMapStorageLogStorageContract(t *testing.T) {
	runLogStorageContract(t, func(*testing.T) contract.LogStorage {
		return NewMapStorage()
	})
}

var _ contract.LogStorage = (*contractTestStorage)(nil)
