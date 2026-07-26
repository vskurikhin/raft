package raft

import (
	"log"
	"net"
	"sync"
	"time"
)

// Server — тонкая обёртка над TCPTransport и ConsensusModule для
// обратной совместимости с cmd/main.go и pkg/kvservice.
// В тестах (Harness) не используется — вместо него применяется
// InmemTransport напрямую.
type Server struct {
	mu sync.Mutex

	serverID int
	peerIds  []int

	storage Storage
	fsm     FSM

	transport *TCPTransport
	cm        *ConsensusModule

	ready <-chan any
	quit  chan any
	wg    sync.WaitGroup
}

// Config — конфигурация для создания нового сервера Raft.
// Содержит идентификатор сервера, список идентификаторов узлов-соседей,
// адрес для RPC и карту адресов узлов-соседей.
type Config struct {
	PeerAddresses map[int]net.Addr
	PeerIds       []int
	RPCAddress    string
	ServerID      int
}

// New создаёт новый сервер Raft с заданной конфигурацией cfg, хранилищем storage,
// каналом уведомления ready и FSM для применения зафиксированных записей журнала.
func New(cfg Config, storage Storage, ready <-chan any, fsm FSM) *Server {
	s := &Server{
		serverID: cfg.ServerID,
		peerIds:  cfg.PeerIds,
		storage:  storage,
		fsm:      fsm,
		ready:    ready,
		quit:     make(chan any),
	}
	return s
}

// NewServer создаёт новый сервер Raft с указанными идентификатором serverID,
// списком идентификаторов узлов-соседей peerIds, хранилищем storage, каналом
// уведомления ready и FSM для применения зафиксированных записей журнала.
func NewServer(serverID int, peerIds []int, storage Storage, ready <-chan any, fsm FSM) *Server {
	return New(Config{
		ServerID: serverID,
		PeerIds:  peerIds,
	}, storage, ready, fsm)
}

// Serve запускает TCP-транспорт на указанном адресе и создаёт ConsensusModule.
func (s *Server) Serve(address string) {
	transport, err := NewTCPTransport(address, HeartbeatTimeoutMs*time.Millisecond/2)
	if err != nil {
		log.Fatalf("raft: failed to create TCPTransport: %v", err)
	}
	s.mu.Lock()
	s.transport = transport
	s.mu.Unlock()

	s.cm = NewConsensusModule(s.serverID, s.peerIds, transport, s.storage, s.fsm, s.ready)
}

// Apply отправляет команду в Raft-кластер и возвращает ApplyFuture.
func (s *Server) Apply(cmd any, timeout time.Duration) ApplyFuture {
	return s.cm.Apply(cmd, timeout)
}

// ConnectToPeerWithTimeout сохраняет адрес peer'а в TCPTransport.
// В новой архитектуре транспорт подключается лениво (при первом RPC).
// Метод сохранён для обратной совместимости с kvservice.
func (s *Server) ConnectToPeerWithTimeout(peerID int, addr net.Addr, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.transport != nil {
		s.transport.Connect(ServerID(peerID), addr.String())
	}
	return nil
}

// ConnectToPeer сохраняет адрес peer'а. Аналогично ConnectToPeerWithTimeout.
func (s *Server) ConnectToPeer(peerID int, addr net.Addr) error {
	return s.ConnectToPeerWithTimeout(peerID, addr, 0)
}

// DisconnectPeer отключает от указанного peer'а.
func (s *Server) DisconnectPeer(peerID int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.transport != nil {
		s.transport.Disconnect(ServerID(peerID))
	}
	return nil
}

// DisconnectAll закрывает все соединения с peer'ами.
func (s *Server) DisconnectAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.transport != nil {
		s.transport.DisconnectAll()
	}
}

// GetListenAddr возвращает адрес, на котором слушает TCPTransport.
func (s *Server) GetListenAddr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.transport == nil {
		return nil
	}
	addr, err := net.ResolveTCPAddr("tcp", string(s.transport.LocalAddr()))
	if err != nil {
		return nil
	}
	return addr
}

// IsLeader проверяет, является ли сервер лидером.
func (s *Server) IsLeader() bool {
	_, _, isLeader := s.cm.Report()
	return isLeader
}

// Shutdown останавливает сервер.
func (s *Server) Shutdown() {
	s.cm.Stop()
	s.mu.Lock()
	if s.transport != nil {
		s.transport.Close()
	}
	s.mu.Unlock()
	close(s.quit)
}
