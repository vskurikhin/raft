package store

import (
	"log"
	"slices"
	"sync"

	"github.com/vskurikhin/raft/pkg/raft/contract"
)

// MapStorage — простая реализация хранилища в памяти; предназначена для
// тестирования. Скалярные ключи хранятся с копированием на входе и выходе;
// журнал представлен собственными кодированными записями, разделяющими
// семантику LogStorage без модели диска и дисковых отказов.
type MapStorage struct {
	mu sync.Mutex
	m  map[string][]byte

	// logExists отличает отсутствующий журнал от созданного пустого.
	logExists bool

	// logEntries — собственное кодированное представление журнала.
	logEntries []encodedLogEntry
}

var (
	_ contract.Storage    = (*MapStorage)(nil)
	_ contract.LogStorage = (*MapStorage)(nil)
)

// NewMapStorage создаёт пустое хранилище в памяти.
func NewMapStorage() *MapStorage {
	m := make(map[string][]byte)
	return &MapStorage{
		m: m,
	}
}

// Get возвращает защитную копию значения ключа. Зарезервированный ключ
// журнала возвращает отсутствие.
func (ms *MapStorage) Get(key string) ([]byte, bool) {
	if key == _logKey {
		return nil, false
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	v, found := ms.m[key]
	return slices.Clone(v), found
}

// Set сохраняет копию значения ключа. Прямая запись зарезервированного
// ключа журнала завершается немедленно: журнал пишут операции LogStorage.
func (ms *MapStorage) Set(key string, value []byte) {
	if key == _logKey {
		log.Fatalf("MapStorage.Set: ключ %q зарезервирован: журнал пишут операции LogStorage", key)
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.m[key] = slices.Clone(value)
}

// HasData возвращает true при наличии скалярного ключа либо созданного
// журнала, включая пустой.
func (ms *MapStorage) HasData() bool {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return len(ms.m) > 0 || ms.logExists
}

// StoreLogEntries реализует LogStorage: та же логическая замена суффикса,
// без ввода-вывода. Результат всегда нулевой: диска у хранилища нет.
//
//nolint:gocritic // log.Fatalf завершает процесс: отложенное снятие на пути ошибки не наблюдаемо
func (ms *MapStorage) StoreLogEntries(fromIndex int, entries []contract.LogEntry) contract.LogWriteResult {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if err := ms.storeLogEntriesLocked(fromIndex, entries); err != nil {
		log.Fatalf("MapStorage.StoreLogEntries: %v", err)
	}
	return contract.LogWriteResult{}
}

// RewriteLog реализует LogStorage: полная замена журнала, включая создание
// пустого.
//
//nolint:gocritic // log.Fatalf завершает процесс: отложенное снятие на пути ошибки не наблюдаемо
func (ms *MapStorage) RewriteLog(entries []contract.LogEntry) contract.LogWriteResult {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if err := ms.rewriteLogLocked(entries); err != nil {
		log.Fatalf("MapStorage.RewriteLog: %v", err)
	}
	return contract.LogWriteResult{}
}

// LoadLog реализует LogStorage: возвращает независимый граф записей.
func (ms *MapStorage) LoadLog() ([]contract.LogEntry, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.loadLogLocked()
}

// storeLogEntriesLocked выполняет замену суффикса под ms.mu. Требует
// удержания ms.mu.
func (ms *MapStorage) storeLogEntriesLocked(fromIndex int, entries []contract.LogEntry) error {
	if !ms.logExists {
		return errLogMissing
	}
	encoded, err := encodeLogEntries(entries)
	if err != nil {
		return err
	}
	cache := logCache{exists: true, entries: ms.logEntries}
	if err := checkStoreRange(&cache, fromIndex, encoded); err != nil {
		return err
	}
	if len(encoded) == 0 && (len(ms.logEntries) == 0 || fromIndex == cache.lastIndex()+1) {
		return nil
	}
	if len(ms.logEntries) == 0 || fromIndex == cache.lastIndex()+1 {
		ms.logEntries = append(ms.logEntries, encoded...)
		return nil
	}
	keep := fromIndex - cache.firstIndex()
	combined := make([]encodedLogEntry, 0, keep+len(encoded))
	combined = append(combined, ms.logEntries[:keep]...)
	combined = append(combined, encoded...)
	ms.logEntries = combined
	return nil
}

// rewriteLogLocked выполняет полную замену журнала под ms.mu. Требует
// удержания ms.mu.
func (ms *MapStorage) rewriteLogLocked(entries []contract.LogEntry) error {
	encoded, err := encodeLogEntries(entries)
	if err != nil {
		return err
	}
	ms.logExists = true
	ms.logEntries = encoded
	return nil
}

// loadLogLocked возвращает независимый граф записей. Требует удержания
// ms.mu.
func (ms *MapStorage) loadLogLocked() ([]contract.LogEntry, error) {
	if !ms.logExists {
		return nil, contract.ErrLogNotFound
	}
	return decodeCachedEntries(ms.logEntries)
}
