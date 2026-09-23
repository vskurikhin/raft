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
	if len(entries) == 0 {
		if fromIndex < len(s.entries) {
			s.entries = slices.Clone(s.entries[:fromIndex])
		}
		return contract.LogWriteResult{}
	}
	copied := cloneContractEntries(entries)
	if fromIndex >= len(s.entries) {
		s.entries = append(s.entries, copied...)
	} else {
		s.entries = append(s.entries[:fromIndex], copied...)
	}
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

// cloneContractEntries копирует записи и их срезы Data, чтобы приспособление
// не делило изменяемые значения с вызывающим.
func cloneContractEntries(entries []contract.LogEntry) []contract.LogEntry {
	out := make([]contract.LogEntry, len(entries))
	for i := range entries {
		out[i] = entries[i]
		if data, ok := entries[i].Data.([]byte); ok {
			out[i].Data = slices.Clone(data)
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

var _ contract.LogStorage = (*contractTestStorage)(nil)
