package raft

import (
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

func init() {
	// Регистрируем типы для gob-кодирования, используемые в RPC.
	gob.Register(AppendEntriesArgs{})
	gob.Register(AppendEntriesReply{})
	gob.Register(RequestVoteArgs{})
	gob.Register(RequestVoteReply{})
	gob.Register(TimeoutNowRequest{})
	gob.Register(TimeoutNowResponse{})
	gob.Register(RequestPreVoteArgs{})
	gob.Register(RequestPreVoteReply{})
}

// tcpRPCRequest — структура для gob-кодирования RPC-запроса.
type tcpRPCRequest struct {
	Method string
	Args   any
}

// tcpRPCResponse — структура для gob-кодирования RPC-ответа.
type tcpRPCResponse struct {
	Reply any
	Error string
}

// TCPTransport — реализация Transport, использующая TCP и encoding/gob.
// Каждое RPC — отдельное TCP-соединение (fresh connection per RPC).
// После отправки запроса и получения ответа соединение закрывается.
//
// Потокобезопасность: Connect/Disconnect защищены sync.Mutex.
// Consumer() и LocalAddr() не требуют блокировки.
// После Close() все методы возвращают транспортную ошибку.
type TCPTransport struct {
	consumerCh chan RPC
	localAddr  ServerAddress
	listener   net.Listener
	timeout    time.Duration
	mu         sync.Mutex
	peers      map[ServerID]string
	shutdownCh chan struct{}
	wg         sync.WaitGroup
}

// NewTCPTransport создаёт новый TCPTransport, слушающий на указанном адресе.
// Принимает адрес для прослушивания (например "127.0.0.1:0" для случайного порта)
// и таймаут для gob-кодирования/декодирования и ожидания ответа.
// Запускает acceptLoop в отдельной горутине.
func NewTCPTransport(addr string, timeout time.Duration) (*TCPTransport, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("raft: failed to listen on %s: %w", addr, err)
	}
	t := &TCPTransport{
		consumerCh: make(chan RPC),
		localAddr:  ServerAddress(listener.Addr().String()),
		listener:   listener,
		timeout:    timeout,
		peers:      make(map[ServerID]string),
		shutdownCh: make(chan struct{}),
	}
	t.wg.Add(1)
	go t.acceptLoop()
	return t, nil
}

// Consumer возвращает канал входящих RPC.
func (t *TCPTransport) Consumer() <-chan RPC {
	return t.consumerCh
}

// LocalAddr возвращает адрес, на котором слушает транспорт.
func (t *TCPTransport) LocalAddr() ServerAddress {
	return t.localAddr
}

// AppendEntries отправляет AppendEntries указанному узлу по TCP.
// Каждый вызов устанавливает новое TCP-соединение, сериализует запрос
// через gob, декодирует ответ и закрывает соединение.
// Таймаут определяется t.timeout.
func (t *TCPTransport) AppendEntries(peerID ServerID, args AppendEntriesArgs) (AppendEntriesReply, error) {
	var zero AppendEntriesReply
	reply, err := t.sendRPC(peerID, "AppendEntries", &args)
	if err != nil {
		return zero, err
	}
	r, ok := reply.(*AppendEntriesReply)
	if !ok {
		return zero, fmt.Errorf("raft: unexpected reply type %T", reply)
	}
	return *r, nil
}

// RequestVote отправляет RequestVote указанному узлу по TCP.
// Семантика соединения и таймаута аналогична AppendEntries.
func (t *TCPTransport) RequestVote(peerID ServerID, args RequestVoteArgs) (RequestVoteReply, error) {
	var zero RequestVoteReply
	reply, err := t.sendRPC(peerID, "RequestVote", &args)
	if err != nil {
		return zero, err
	}
	r, ok := reply.(*RequestVoteReply)
	if !ok {
		return zero, fmt.Errorf("raft: unexpected reply type %T", reply)
	}
	return *r, nil
}

// RequestPreVote отправляет PreVote RPC узлу peerID по TCP.
func (t *TCPTransport) RequestPreVote(peerID ServerID, args RequestPreVoteArgs) (RequestPreVoteReply, error) {
	var zero RequestPreVoteReply
	reply, err := t.sendRPC(peerID, "RequestPreVote", &args)
	if err != nil {
		return zero, err
	}
	r, ok := reply.(*RequestPreVoteReply)
	if !ok {
		return zero, fmt.Errorf("raft: unexpected reply type %T", reply)
	}
	return *r, nil
}

// TimeoutNow отправляет TimeoutNowRequest указанному узлу по TCP.
func (t *TCPTransport) TimeoutNow(peerID ServerID, args TimeoutNowRequest) (TimeoutNowResponse, error) {
	var zero TimeoutNowResponse
	reply, err := t.sendRPC(peerID, "TimeoutNow", &args)
	if err != nil {
		return zero, err
	}
	r, ok := reply.(*TimeoutNowResponse)
	if !ok {
		return zero, fmt.Errorf("raft: unexpected reply type %T", reply)
	}
	return *r, nil
}

// InstallSnapshot не реализован.
func (t *TCPTransport) InstallSnapshot(_ ServerID, _ InstallSnapshotRequest, _ io.Reader) (InstallSnapshotResponse, error) {
	return InstallSnapshotResponse{}, ErrNotImplemented
}

// AppendEntriesPipeline не реализован.
func (t *TCPTransport) AppendEntriesPipeline(_ ServerID) (AppendPipeline, error) {
	return nil, ErrNotImplemented
}

// SetHeartbeatHandler устанавливает обработчик heartbeat. В TCP-реализации не используется.
func (t *TCPTransport) SetHeartbeatHandler(_ func(RPC)) {
}

// Connect сохраняет адрес для указанного peer'а. При следующем RPC-вызове
// транспорт подключится к этому адресу.
func (t *TCPTransport) Connect(peerID ServerID, addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peers[peerID] = addr
}

// Disconnect удаляет адрес указанного peer'а.
func (t *TCPTransport) Disconnect(peerID ServerID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.peers, peerID)
}

// DisconnectAll удаляет адреса всех peer'ов.
func (t *TCPTransport) DisconnectAll() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peers = make(map[ServerID]string)
}

// Close завершает работу транспорта: закрывает shutdownCh, listener,
// удаляет все peers и дожидается завершения acceptLoop и serveConn
// через wg.Wait(). Идемпотентен.
func (t *TCPTransport) Close() {
	select {
	case <-t.shutdownCh:
		return
	default:
		close(t.shutdownCh)
	}
	_ = t.listener.Close()
	t.DisconnectAll()
	t.wg.Wait()
}

// acceptLoop принимает входящие TCP-соединения.
func (t *TCPTransport) acceptLoop() {
	defer t.wg.Done()
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			select {
			case <-t.shutdownCh:
				return
			default:
				log.Printf("raft: accept error: %v", err)
				continue
			}
		}
		t.wg.Add(1)
		go t.serveConn(conn)
	}
}

// serveConn обрабатывает одно входящее TCP-соединение: декодирует
// tcpRPCRequest из gob, преобразует value-типы аргументов в pointer-типы
// (требование consumer), отправляет RPC в consumerCh, ожидает ответа
// (с таймаутом t.timeout), кодирует tcpRPCResponse обратно в соединение.
//
// gob-декодирование в any возвращает value-тип (AppendEntriesArgs),
// но consumer ожидает pointer-тип (*AppendEntriesArgs). Преобразование
// выполняется через type switch.
func (t *TCPTransport) serveConn(conn net.Conn) {
	defer t.wg.Done()
	defer func() { _ = conn.Close() }()

	dec := gob.NewDecoder(conn)
	enc := gob.NewEncoder(conn)

	var req tcpRPCRequest
	if err := dec.Decode(&req); err != nil {
		log.Printf("raft: failed to decode request: %v", err)
		return
	}

	respCh := make(chan RPCResponse, 1)

	// gob-декодирование из any возвращает value-тип, но consumer ожидает
	// pointer-типы (*AppendEntriesArgs, *RequestVoteArgs). Приводим.
	cmd := req.Args
	switch v := cmd.(type) {
	case AppendEntriesArgs:
		cmd = &v
	case RequestVoteArgs:
		cmd = &v
	case TimeoutNowRequest:
		cmd = &v
	case RequestPreVoteArgs:
		cmd = &v
	}

	rpc := RPC{
		Command:  cmd,
		RespChan: respCh,
	}

	select {
	case t.consumerCh <- rpc:
	case <-t.shutdownCh:
		return
	}

	var resp tcpRPCResponse
	timer := time.NewTimer(t.timeout)
	select {
	case r := <-respCh:
		if r.Error != nil {
			resp.Error = r.Error.Error()
		}
		resp.Reply = r.Reply
		timer.Stop()
	case <-t.shutdownCh:
		resp.Error = ErrRaftShutdown.Error()
		timer.Stop()
	case <-timer.C:
		resp.Error = ErrEnqueueTimeout.Error()
	}

	if err := enc.Encode(resp); err != nil {
		log.Printf("raft: failed to encode response: %v", err)
	}
}

// sendRPC отправляет RPC указанному peer'у и возвращает ответ.
// Каждый вызов открывает новое TCP-соединение через dial(), создаёт
// gob.Encoder/Decoder, отправляет tcpRPCRequest, декодирует tcpRPCResponse
// и закрывает соединение.
//
// Известные ошибки (ErrRaftShutdown, ErrEnqueueTimeout) распознаются
// и возвращаются как sentinel errors. Неизвестные — как fmt.Errorf.
// gob-декодирование reply возвращает value-тип — преобразуется в pointer
// для совместимости с вызывающим кодом.
func (t *TCPTransport) sendRPC(peerID ServerID, method string, args any) (any, error) {
	conn, err := t.dial(peerID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()

	enc := gob.NewEncoder(conn)
	dec := gob.NewDecoder(conn)

	req := tcpRPCRequest{
		Method: method,
		Args:   args,
	}

	if err := enc.Encode(req); err != nil {
		return nil, fmt.Errorf("raft: failed to send RPC: %w", err)
	}

	var resp tcpRPCResponse
	if err := dec.Decode(&resp); err != nil {
		return nil, fmt.Errorf("raft: failed to decode response: %w", err)
	}

	if resp.Error != "" {
		switch resp.Error {
		case ErrRaftShutdown.Error():
			return nil, ErrRaftShutdown
		case ErrEnqueueTimeout.Error():
			return nil, ErrEnqueueTimeout
		default:
			return nil, fmt.Errorf("raft: RPC error from peer: %s", resp.Error)
		}
	}

	// gob-декодирование из any возвращает value-тип, но вызывающий код
	// ожидает pointer-типы (*AppendEntriesReply, *RequestVoteReply).
	reply := resp.Reply
	switch v := reply.(type) {
	case AppendEntriesReply:
		reply = &v
	case RequestVoteReply:
		reply = &v
	case TimeoutNowResponse:
		reply = &v
	case RequestPreVoteReply:
		reply = &v
	}

	return reply, nil
}

// dial устанавливает новое TCP-соединение к указанному peer'у.
// Использует t.timeout как таймаут соединения. Возвращает ошибку,
// если peer не найден в карте peers или соединение не удалось.
func (t *TCPTransport) dial(peerID ServerID) (net.Conn, error) {
	addr, ok := func() (string, bool) {
		t.mu.Lock()
		defer t.mu.Unlock()
		addr, ok := t.peers[peerID]
		return addr, ok
	}()
	if !ok {
		return nil, fmt.Errorf("raft: unknown peer %d", peerID)
	}

	return net.DialTimeout("tcp", addr, t.timeout)
}
