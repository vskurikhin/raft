package raft

import (
	"bytes"
	"encoding/gob"
	"log"
)

// scalarCacheEntry — элемент кэша кодирования одного постоянного скаляра:
// последнее закодированное значение и готовые байты его gob-представления.
// Нулевое значение означает невалидный кэш (valid == false), поэтому первое
// обращение всегда выполняет кодирование. Байты после заполнения не
// мутируются и не выдаются наружу иначе как в Set соответствующего ключа.
type scalarCacheEntry struct {
	valid   bool
	value   int
	encoded []byte
}

// scalarCache — кэш последних кодирований четырёх постоянных скаляров CM:
// currentTerm, votedFor, lastSnapshotIndex, lastSnapshotTerm. Владелец — CM,
// защита — cm.mu, срок жизни — экземпляр CM; при восстановлении узла кэш не
// прогревается от диска, новый CM снова начинает с пустого кэша.
//
// Это кэш представления, а не долговечности: Set каждого ключа выполняется
// при каждом сохранении независимо от попадания в кэш.
type scalarCache struct {
	currentTerm       scalarCacheEntry
	votedFor          scalarCacheEntry
	lastSnapshotIndex scalarCacheEntry
	lastSnapshotTerm  scalarCacheEntry
}

// encodeScalarLocked возвращает неизменяемые байты gob-кодирования
// скалярного значения. При совпадении значения с последним закодированным
// возвращает сохранённое представление без повторного кодирования; при
// отличии или невалидном кэше кодирует значение прежним способом
// (gob(int) в собственный bytes.Buffer) и сохраняет новое представление
// в элементе. Ошибка кодирования завершает процесс, как и прежний путь
// сохранения.
//
// Возвращённые байты ничего не утверждают о долговечности: вызывающий
// обязан выполнить Set соответствующего ключа в любом случае.
//
// Требует удержания cm.mu — мьютекса владельца кэша.
func encodeScalarLocked(entry *scalarCacheEntry, value int) []byte {
	if entry.valid && entry.value == value {
		return entry.encoded
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(value); err != nil {
		log.Fatal(err)
	}
	entry.valid = true
	entry.value = value
	entry.encoded = buf.Bytes()
	return entry.encoded
}
