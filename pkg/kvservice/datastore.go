package kvservice

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"
)

// DataStore — простое потокобезопасное хранилище «ключ-значение»,
// используемое в качестве внутреннего хранилища данных для kvservice.
type DataStore struct {
	mu   sync.Mutex
	data map[string]string
}

func NewDataStore() *DataStore {
	return &DataStore{
		data: make(map[string]string),
	}
}

// Get получает значение по ключу из хранилища.
// Возвращает (v, true), если ключ найден, либо ("", false) в противном случае.
func (ds *DataStore) Get(key string) (string, bool) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	value, ok := ds.data[key]
	return value, ok
}

// Put присваивает datastore[key] = value.
// Возвращает (v, true), если ключ уже существовал в хранилище и его прежнее
// значение было равно v, либо ("", false) в противном случае.
func (ds *DataStore) Put(key, value string) (string, bool) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	v, ok := ds.data[key]
	ds.data[key] = value
	return v, ok
}

// CAS выполняет атомарную операцию compare-and-swap:
//
// Если ключ существует и его текущее значение совпадает с compare,
// записывается новое значение value, иначе никаких изменений не производится.
//
// Возвращаются предыдущее значение ключа и признак того,
// существовал ли ключ в хранилище.
func (ds *DataStore) CAS(key, compare, value string) (string, bool) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	prevValue, ok := ds.data[key]
	if ok && prevValue == compare {
		ds.data[key] = value
	}
	return prevValue, ok
}

// Snapshot сериализует полное содержимое data в []byte.
// Результат может быть десериализован RestoreFromSnapshot.
func (ds *DataStore) Snapshot() []byte {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(ds.data); err != nil {
		panic(fmt.Sprintf("gob.Encode(data) failed: %v", err))
	}
	return buf.Bytes()
}

// RestoreFromSnapshot заменяет всё содержимое data данными из снепшота.
// data должен быть результатом вызова Snapshot().
func (ds *DataStore) RestoreFromSnapshot(data []byte) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	var newData map[string]string
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&newData); err != nil {
		panic(fmt.Sprintf("gob.Decode(data) failed: %v", err))
	}
	ds.data = newData
}
