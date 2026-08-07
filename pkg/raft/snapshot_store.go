package raft

import (
	"bytes"
	"fmt"
	"io"
	"sync"
)

// SnapshotStore — хранилище снэпшотов Raft.
// Отвечает за создание, хранение и предоставление доступа к снэпшотам.
//
// Контракт:
//   - новый снэпшот становится видимым в List/Open только после успешного
//     Close sink'а;
//   - Cancel обязан сохранять предыдущие снэпшоты нетронутыми и удалять все
//     артефакты незавершённого снэпшота;
//   - List возвращает снэпшоты, упорядоченные от новых к старым;
//   - Create может вызываться конкурентно; одновременные sink'и независимы.
//     FileSnapshotStore поддерживает независимые sink'и полностью;
//     InmemSnapshotStore — один одновременный sink (test-only).
type SnapshotStore interface {
	// Create создаёт новый снэпшот с указанными индексом, термом,
	// конфигурацией и индексом конфигурации. Возвращает SnapshotSink
	// для записи данных снэпшота.
	Create(index, term int, configuration Configuration, configIndex int) (SnapshotSink, error)

	// List возвращает список доступных снэпшотов.
	List() ([]*SnapshotMeta, error)

	// Open открывает снэпшот по ID. Возвращает метаданные и io.ReadCloser
	// для чтения данных снэпшота. Вызывающий обязан закрыть reader.
	Open(id string) (*SnapshotMeta, io.ReadCloser, error)
}

// InmemSnapshotStore — in-memory реализация SnapshotStore. Test-only:
// не переживает рестарт процесса; для production использовать
// FileSnapshotStore.
//
// Хранит только последний снэпшот: незавершённый sink живёт в pending
// и становится видимым (latest) только после успешного Close; Cancel
// очищает pending, не затрагивая latest. Поддерживает один одновременный
// Create — второй Create затирает pending первого, и первый Close не
// продвигает свои данные в latest (защита от гонки двух Create).
type InmemSnapshotStore struct {
	mu      sync.Mutex
	pending *InmemSnapshotSink
	latest  *InmemSnapshotSink
}

// NewInmemSnapshotStore создаёт новый InmemSnapshotStore.
func NewInmemSnapshotStore() *InmemSnapshotStore {
	return &InmemSnapshotStore{}
}

// Create создаёт новый снэпшот. Снэпшот не виден в List/Open до Close.
func (s *InmemSnapshotStore) Create(index, term int, configuration Configuration, configIndex int) (SnapshotSink, error) {
	id := fmt.Sprintf("snapshot-%d-%d", term, index)
	s.mu.Lock()
	defer s.mu.Unlock()
	sink := &InmemSnapshotSink{
		store: s,
		meta: SnapshotMeta{
			Version:       ProtocolVersion,
			ID:            id,
			Index:         index,
			Term:          term,
			Configuration: configuration,
			ConfigIndex:   configIndex,
		},
	}
	s.pending = sink
	return sink, nil
}

// List возвращает срез с одним *SnapshotMeta или пустой срез, если
// завершённого снэпшота нет.
func (s *InmemSnapshotStore) List() ([]*SnapshotMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.latest == nil {
		return nil, nil
	}
	return []*SnapshotMeta{cloneMeta(&s.latest.meta)}, nil
}

// Open ищет завершённый снэпшот по ID. Возвращает копию данных.
func (s *InmemSnapshotStore) Open(id string) (*SnapshotMeta, io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.latest == nil || s.latest.meta.ID != id {
		return nil, nil, fmt.Errorf("raft: snapshot not found: %s", id)
	}
	meta := cloneMeta(&s.latest.meta)
	data := make([]byte, s.latest.buf.Len())
	copy(data, s.latest.buf.Bytes())
	return meta, io.NopCloser(bytes.NewReader(data)), nil
}

// InmemSnapshotSink — реализация SnapshotSink в памяти.
type InmemSnapshotSink struct {
	store  *InmemSnapshotStore
	meta   SnapshotMeta
	buf    bytes.Buffer
	mu     sync.Mutex
	closed bool
}

// Write пишет данные в буфер снэпшота, обновляя Size.
func (s *InmemSnapshotSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, err := s.buf.Write(p)
	s.meta.Size = int64(s.buf.Len())
	return n, err
}

// Close завершает запись снэпшота и делает его видимым в List/Open.
// Идемпотентен: повторный Close — no-op.
func (s *InmemSnapshotSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	// Продвигаем pending → latest только если этот sink всё ещё pending:
	// защита от «второй Create затёр pending первого».
	if s.store.pending == s {
		s.store.pending = nil
		s.store.latest = s
	}
	return nil
}

// ID возвращает идентификатор снэпшота.
func (s *InmemSnapshotSink) ID() string {
	return s.meta.ID
}

// Cancel отменяет запись снэпшота: очищает pending, не затрагивая
// последний завершённый снэпшот. Идемпотентен.
func (s *InmemSnapshotSink) Cancel() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	if s.store.pending == s {
		s.store.pending = nil
	}
	return nil
}

// cloneMeta создаёт копию SnapshotMeta.
func cloneMeta(m *SnapshotMeta) *SnapshotMeta {
	cfg := m.Configuration
	servers := make([]ConfigServer, len(cfg.ConfigServers))
	copy(servers, cfg.ConfigServers)
	return &SnapshotMeta{
		Version:       m.Version,
		ID:            m.ID,
		Index:         m.Index,
		Term:          m.Term,
		Configuration: Configuration{ConfigServers: servers},
		ConfigIndex:   m.ConfigIndex,
		Size:          m.Size,
	}
}
