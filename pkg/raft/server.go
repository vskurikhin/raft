package raft

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"
)

// Server инкапсулирует raft.ConsensusModule вместе с rpc.Server, который предоставляет
// свои методы в качестве RPC-конечных точек. Он также управляет узлами Raft-сервера.
// Основная цель этого типа — упростить код raft.Server для целей представления.
// raft.ConsensusModule имеет *Server для осуществления связи с узлами,
// и ему не нужно беспокоиться о специфике работы RPC-сервера.
type Server struct {
	mu sync.Mutex

	serverID int
	peerIds  []int

	cm       *ConsensusModule
	storage  Storage
	rpcProxy *RPCProxy

	rpcServer *rpc.Server
	listener  net.Listener

	commitChan    chan<- CommitEntry
	peerClients   map[int]*rpc.Client
	peerAddresses map[int]net.Addr // адреса пиров, используются для переподключения

	// peerWantsReconnect отслеживает, был ли клиент пира обнулён из-за
	// разрыва TCP-соединения (shut down), а не из-за явного DisconnectPeer.
	// Используется в leaderSendAEs, чтобы отличить случай перезапуска пира
	// от случая изоляции лидера тестовым harness.
	peerWantsReconnect map[int]bool

	ready <-chan any
	quit  chan any
	wg    sync.WaitGroup
}

func NewServer(serverID int, peerIds []int, storage Storage, ready <-chan any, commitChan chan<- CommitEntry) *Server {
	s := new(Server)
	s.serverID = serverID
	s.peerIds = peerIds
	s.peerClients = make(map[int]*rpc.Client)
	s.peerAddresses = make(map[int]net.Addr)
	s.peerWantsReconnect = make(map[int]bool)
	s.storage = storage
	s.ready = ready
	s.commitChan = commitChan
	s.quit = make(chan any)
	return s
}

func (s *Server) Serve(address string) {
	s.mu.Lock()
	s.cm = NewConsensusModule(s.serverID, s.peerIds, s, s.storage, s.ready, s.commitChan)

	// Создаём новый RPC-сервер и регистрируем RPCProxy,
	// который перенаправляет все методы в n.cm (ConsensusModule)
	s.rpcServer = rpc.NewServer()
	s.rpcProxy = NewProxy(s.cm)
	_ = s.rpcServer.RegisterName("ConsensusModule", s.rpcProxy)

	var err error
	s.listener, err = net.Listen("tcp", address)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("[%v] listening at %s", s.serverID, s.listener.Addr())
	s.mu.Unlock()

	s.wg.Add(1)
	//nolint:modernize
	go func() {
		defer s.wg.Done()

		for {
			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-s.quit:
					return
				default:
					log.Fatal("accept error:", err)
				}
			}
			s.wg.Add(1)
			go func() {
				s.rpcServer.ServeConn(conn)
				s.wg.Done()
			}()
		}
	}()
}

// Submit вызывает метод Submit базового экземпляра CM; описание см. в
// документации к этому методу.
func (s *Server) Submit(cmd any) int {
	return s.cm.Submit(cmd)
}

// DisconnectAll закрывает все клиентские соединения с другими узлами для этого сервера.
func (s *Server) DisconnectAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.peerClients {
		if s.peerClients[id] != nil {
			_ = s.peerClients[id].Close()
			s.peerClients[id] = nil
		}
		s.peerWantsReconnect[id] = false
	}
}

// Shutdown закрывает сервер и ожидает его корректного завершения работы.
func (s *Server) Shutdown() {
	s.cm.Stop()
	close(s.quit)
	_ = s.listener.Close()
	s.wg.Wait()
}

func (s *Server) GetListenAddr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listener.Addr()
}

func (s *Server) ConnectToPeer(peerID int, addr net.Addr) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	log.Printf("[%v] connecting to peer %v", s.serverID, peerID)
	if s.peerClients[peerID] == nil {
		client, err := rpc.Dial(addr.Network(), addr.String())
		if err != nil {
			return err
		}
		s.peerClients[peerID] = client
		s.peerAddresses[peerID] = addr
		s.peerWantsReconnect[peerID] = false
		log.Printf("[%v] connected to peer %v", s.serverID, peerID)
	}
	return nil
}

// ReconnectToPeer принудительно переустанавливает соединение к пиру.
// Закрывает существующее соединение (если есть) и создаёт новое.
// Адрес пира должен быть предварительно сохранён через ConnectToPeer.
// Использует таймаут, чтобы не блокировать s.mu надолго при мёртвом пире.
func (s *Server) ReconnectToPeer(peerID int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	addr, ok := s.peerAddresses[peerID]
	if !ok {
		return fmt.Errorf("no address for peer %d", peerID)
	}

	// Закрыть старое соединение, если оно ещё открыто.
	if s.peerClients[peerID] != nil {
		_ = s.peerClients[peerID].Close()
	}

	conn, err := net.DialTimeout(addr.Network(), addr.String(), 50*Quantum*time.Millisecond)
	if err != nil {
		s.peerClients[peerID] = nil
		return err
	}
	client := rpc.NewClient(conn)
	s.peerClients[peerID] = client
	s.peerWantsReconnect[peerID] = false
	log.Printf("[%v] reconnected to peer %v", s.serverID, peerID)
	return nil
}

// DisconnectPeer отключает этот сервер от удаленного узла, идентифицированного по peerId.
func (s *Server) DisconnectPeer(peerID int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[peerID] != nil {
		err := s.peerClients[peerID].Close()
		s.peerClients[peerID] = nil
		s.peerWantsReconnect[peerID] = false
		return err
	}
	s.peerWantsReconnect[peerID] = false
	return nil
}

func (s *Server) Call(id int, serviceMethod string, args, reply any) error {
	s.mu.Lock()
	peer := s.peerClients[id]
	s.mu.Unlock()

	// Если этот метод вызывается после завершения работы (когда вызывается client.Close),
	// он вернет ошибку.
	if peer == nil {
		return fmt.Errorf("call client %d after it's closed", id)
	}

	err := s.rpcProxy.Call(peer, serviceMethod, args, reply)

	// Если клиент закрыт (соединение разорвано сервером), сбросить его
	// и запомнить, что нужно переподключиться.
	// Следующий Call вернёт "call client after it's closed",
	// что является сигналом для leaderSendAEs выполнить ReconnectToPeer.
	if err != nil && strings.Contains(err.Error(), "shut down") {
		s.mu.Lock()
		s.peerClients[id] = nil
		s.peerWantsReconnect[id] = true
		s.mu.Unlock()
	}

	return err
}

// IsLeader проверяет, считает ли сервер s себя лидером в кластере Raft.
func (s *Server) IsLeader() bool {
	_, _, isLeader := s.cm.Report()
	return isLeader
}

// SetSnapshotter устанавливает функцию snapshotter на базовом CM.
// Эта функция будет вызываться лидером для получения snapshot-данных
// от машины состояний.
func (s *Server) SetSnapshotter(fn func() ([]byte, int, int)) {
	s.cm.SetSnapshotter(fn)
}

// Proxy предоставляет доступ к RPC-прокси, используемому данным сервером.
// Используется только в тестах для моделирования отказов.
func (s *Server) Proxy() *RPCProxy {
	return s.rpcProxy
}

// RPCProxy — прокси-сервер, прозрачно перенаправляющий RPC-вызовы
// ConsensusModule. Он принимает RPC-запросы, адресованные CM, при
// необходимости модифицирует их и затем передаёт самому CM.
//
// Полезен для следующих целей:
//   - моделирования потери RPC-вызовов;
//   - моделирования небольшой задержки передачи RPC;
//   - моделирования ненадёжных соединений путём значительной задержки
//     некоторых сообщений и отбрасывания других, если установлена
//     переменная окружения RAFT_UNRELIABLE_RPC.
type RPCProxy struct {
	mu sync.Mutex
	cm *ConsensusModule

	// numCallsBeforeDrop используется для управления отбрасыванием
	// RPC-вызовов:
	//   -1: не отбрасывать ни одного вызова;
	//    0: отбрасывать все вызовы;
	//   >0: начать отбрасывать вызовы после выполнения указанного
	//       количества вызовов.
	numCallsBeforeDrop int
}

func NewProxy(cm *ConsensusModule) *RPCProxy {
	return &RPCProxy{
		cm:                 cm,
		numCallsBeforeDrop: -1,
	}
}

func (rpp *RPCProxy) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	if os.Getenv("RAFT_UNRELIABLE_RPC") != "" {
		dice := rand.Intn(10)
		switch dice {
		case 9:
			rpp.cm.dLogf("drop RequestVote")
			return fmt.Errorf("RPC failed")
		case 8:
			rpp.cm.dLogf("delay RequestVote")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	return rpp.cm.RequestVote(args, reply)
}

func (rpp *RPCProxy) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	if os.Getenv("RAFT_UNRELIABLE_RPC") != "" {
		dice := rand.Intn(10)
		switch dice {
		case 9:
			rpp.cm.dLogf("drop AppendEntries")
			return fmt.Errorf("RPC failed")
		case 8:
			rpp.cm.dLogf("delay AppendEntries")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	return rpp.cm.AppendEntries(args, reply)
}

func (rpp *RPCProxy) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	if os.Getenv("RAFT_UNRELIABLE_RPC") != "" {
		dice := rand.Intn(10)
		switch dice {
		case 9:
			rpp.cm.dLogf("drop InstallSnapshot")
			return fmt.Errorf("RPC failed")
		case 8:
			rpp.cm.dLogf("delay InstallSnapshot")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	return rpp.cm.InstallSnapshot(args, reply)
}

func (rpp *RPCProxy) Call(peer *rpc.Client, method string, args, reply any) error {
	rpp.mu.Lock()
	if rpp.numCallsBeforeDrop == 0 {
		rpp.mu.Unlock()
		rpp.cm.dLogf("drop Call %s: %v", method, args)
		return fmt.Errorf("RPC failed")
	}
	if rpp.numCallsBeforeDrop > 0 {
		rpp.numCallsBeforeDrop--
	}
	rpp.mu.Unlock()

	return peer.Call(method, args, reply)
}

// DropCallsAfterN настраивает прокси так, чтобы он начал отбрасывать
// RPC-вызовы после выполнения следующих n вызовов.
func (rpp *RPCProxy) DropCallsAfterN(n int) {
	rpp.mu.Lock()
	defer rpp.mu.Unlock()

	rpp.numCallsBeforeDrop = n
}

func (rpp *RPCProxy) DontDropCalls() {
	rpp.mu.Lock()
	defer rpp.mu.Unlock()

	rpp.numCallsBeforeDrop = -1
}
