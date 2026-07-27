package raft

import (
	"bytes"
	"fmt"
	"io"
	"sync"
)

// SnapshotStore — хранилище снэпшотов Raft.
// Отвечает за создание, хранение и предоставление доступа к снэпшотам.
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

// InmemSnapshotStore — in-memory реализация SnapshotStore.
// Хранит только последний снэпшот. При Create перезаписывает предыдущий.
type InmemSnapshotStore struct {
	mu      sync.Mutex
	latest  *InmemSnapshotSink
	hasSnap bool
}

// NewInmemSnapshotStore создаёт новый InmemSnapshotStore.
func NewInmemSnapshotStore() *InmemSnapshotStore {
	return &InmemSnapshotStore{}
}

// Create создаёт новый снэпшот, заменяя предыдущий.
func (s *InmemSnapshotStore) Create(index, term int, configuration Configuration, configIndex int) (SnapshotSink, error) {
	id := fmt.Sprintf("snapshot-%d-%d", term, index)
	s.mu.Lock()
	defer s.mu.Unlock()
	sink := &InmemSnapshotSink{
		meta: SnapshotMeta{
			Version:       ProtocolVersion,
			ID:            id,
			Index:         index,
			Term:          term,
			Configuration: configuration,
			ConfigIndex:   configIndex,
		},
	}
	s.latest = sink
	s.hasSnap = true
	return sink, nil
}

// List возвращает срез с одним *SnapshotMeta или пустой срез, если снэпшота нет.
func (s *InmemSnapshotStore) List() ([]*SnapshotMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.hasSnap {
		return nil, nil
	}
	return []*SnapshotMeta{cloneMeta(&s.latest.meta)}, nil
}

// Open ищет снэпшот по ID. Возвращает копию данных.
func (s *InmemSnapshotStore) Open(id string) (*SnapshotMeta, io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.hasSnap || s.latest.meta.ID != id {
		return nil, nil, fmt.Errorf("raft: snapshot not found: %s", id)
	}
	meta := cloneMeta(&s.latest.meta)
	data := make([]byte, s.latest.buf.Len())
	copy(data, s.latest.buf.Bytes())
	return meta, io.NopCloser(bytes.NewReader(data)), nil
}

// InmemSnapshotSink — реализация SnapshotSink в памяти.
type InmemSnapshotSink struct {
	meta SnapshotMeta
	buf  bytes.Buffer
	mu   sync.Mutex
}

// Write пишет данные в буфер снэпшота, обновляя Size.
func (s *InmemSnapshotSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, err := s.buf.Write(p)
	s.meta.Size = int64(s.buf.Len())
	return n, err
}

// Close завершает запись снэпшота. Для in-memory реализации — no-op.
func (s *InmemSnapshotSink) Close() error {
	return nil
}

// ID возвращает идентификатор снэпшота.
func (s *InmemSnapshotSink) ID() string {
	return s.meta.ID
}

// Cancel отменяет запись снэпшота. Для in-memory реализации — no-op.
func (s *InmemSnapshotSink) Cancel() error {
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
