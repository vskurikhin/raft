package store

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/vskurikhin/raft/pkg/raft/contract"
)

// errLogMissing — внутренняя ошибка записи журнала до его создания: первый
// персист свежего узла выполняет RewriteLog.
var errLogMissing = errors.New("журнал отсутствует: первый персист выполняет RewriteLog")

// logCache — кодированное представление сохранённого журнала: записи, их
// индексы и размер принятого файла. Наполняется при загрузке каталога и
// обновляется только после успешной операции; незавершённый хвост в кэш не
// входит. Поле защищено мьютексом реализации.
type logCache struct {
	// exists отличает отсутствующий журнал от созданного пустого.
	exists bool

	// entries — кодированные записи принятого журнала в порядке индексов.
	entries []encodedLogEntry

	// validEnd — размер файла, соответствующий принятому журналу: конец
	// последнего целого пакета. При незавершённом хвосте меньше размера
	// файла.
	validEnd int

	// tail отмечает незавершённый последний пакет, отброшенный при загрузке.
	// До следующей записи принятый журнал нормализуется атомарной заменой.
	tail bool
}

// firstIndex возвращает индекс первой записи журнала. Для пустого журнала
// значение не определено без проверки empty.
func (c *logCache) firstIndex() int {
	if len(c.entries) == 0 {
		return 0
	}
	return c.entries[0].index
}

// lastIndex возвращает индекс последней записи; для пустого журнала — −1.
func (c *logCache) lastIndex() int {
	if len(c.entries) == 0 {
		return -1
	}
	return c.entries[len(c.entries)-1].index
}

// empty сообщает, что журнал существует, но не содержит записей.
func (c *logCache) empty() bool {
	return c.exists && len(c.entries) == 0
}

// encodedSuffixEqual сообщает, что предложенные кодированные записи
// побайтово совпадают с сохранённым суффиксом от позиции keep: индексы,
// термы, типы и Data равны. Отсутствие логического изменения с теми же
// байтами даёт no-op без ввода-вывода.
func encodedSuffixEqual(existing []encodedLogEntry, keep int, entries []encodedLogEntry) bool {
	if len(existing)-keep != len(entries) {
		return false
	}
	for i := range entries {
		saved := &existing[keep+i]
		proposed := &entries[i]
		if saved.index != proposed.index || saved.term != proposed.term ||
			saved.typ != proposed.typ || !bytes.Equal(saved.data, proposed.data) {
			return false
		}
	}
	return true
}

// checkStoreRange проверяет допустимость замены суффикса от fromIndex по
// контракту: абсолютные границы, выравнивание на fromIndex и отсутствие
// выхода за last+1. Записи уже закодированы и проверены на непрерывность.
func checkStoreRange(cache *logCache, fromIndex int, entries []encodedLogEntry) error {
	if fromIndex < 0 {
		return fmt.Errorf("недопустимый fromIndex %d: индекс журнала неотрицателен", fromIndex)
	}
	if cache.empty() {
		if len(entries) > 0 && entries[0].index != fromIndex {
			return fmt.Errorf("первая запись имеет индекс %d, want %d", entries[0].index, fromIndex)
		}
		return nil
	}
	first, last := cache.firstIndex(), cache.lastIndex()
	if fromIndex < first {
		return fmt.Errorf("fromIndex %d ниже первого индекса журнала %d", fromIndex, first)
	}
	if fromIndex > last+1 {
		return fmt.Errorf("fromIndex %d выше допустимого %d", fromIndex, last+1)
	}
	if len(entries) > 0 && entries[0].index != fromIndex {
		return fmt.Errorf("первая запись имеет индекс %d, want %d", entries[0].index, fromIndex)
	}
	return nil
}

// decodeCachedEntries декодирует независимый граф значений для LoadLog:
// каждая Data читается заново, поэтому результат не делит изменяемые
// значения с кэшем и с предыдущими вызовами.
func decodeCachedEntries(entries []encodedLogEntry) ([]contract.LogEntry, error) {
	out := make([]contract.LogEntry, len(entries))
	for i := range entries {
		entry := &entries[i]
		value, err := decodeLogData(entry.data)
		if err != nil {
			return nil, fmt.Errorf("запись с индексом %d: %w", entry.index, err)
		}
		out[i] = contract.LogEntry{
			Index: entry.index,
			Term:  entry.term,
			Type:  entry.typ,
			Data:  value,
		}
	}
	return out, nil
}
