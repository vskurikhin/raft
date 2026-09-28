package transp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"sync"
	"time"

	"github.com/vskurikhin/raft/pkg/raft/contract"
	"github.com/vskurikhin/raft/pkg/raft/protocol"
)

const (
	// _connReceiveBufferSize — размер буфера для чтения входящих RPC (256KB).
	_connReceiveBufferSize = 256 * 1024

	// _connSendBufferSize — размер буфера для записи исходящих RPC (256KB).
	_connSendBufferSize = 256 * 1024

	// _defaultMaxPool — максимальное количество соединений в пуле на один
	// целевой адрес. Значение 2 позволяет переиспользовать соединения
	// без избыточного удержания ресурсов.
	_defaultMaxPool = 2

	// _timeoutStepBytes — ступень масштабирования сроков по длине данных:
	// 256 КиБ. Для обычного RPC срок умножается на
	// max(1, ⌈BodyLength/256 КиБ⌉), для тела снимка — на
	// max(1, ⌊DataSize/256 КиБ⌋).
	_timeoutStepBytes = 262144

	// _frameHeaderBytes — длина заголовка кадра сетевого формата: длина
	// тела кадра, по которой масштабируется срок, равна длине кадра без
	// заголовка.
	_frameHeaderBytes = 24
)

// Типы RPC сетевого формата. Значения заданы таблицами спецификации формата
// (поле RPCType заголовка кадра) и не зависят от внутренних имён кодека.
const (
	rpcTypeAppendEntries   protocol.RPCType = 0
	rpcTypeRequestVote     protocol.RPCType = 1
	rpcTypeInstallSnapshot protocol.RPCType = 2
	rpcTypeTimeoutNow      protocol.RPCType = 3
	rpcTypeRequestPreVote  protocol.RPCType = 4
)

// rpcTypeName возвращает имя RPC для диагностики.
func rpcTypeName(typ protocol.RPCType) string {
	switch typ {
	case rpcTypeAppendEntries:
		return "AppendEntries"
	case rpcTypeRequestVote:
		return "RequestVote"
	case rpcTypeInstallSnapshot:
		return "InstallSnapshot"
	case rpcTypeTimeoutNow:
		return "TimeoutNow"
	case rpcTypeRequestPreVote:
		return "RequestPreVote"
	default:
		return fmt.Sprintf("RPCType(%d)", uint8(typ))
	}
}

// errSnapshotBodyUnread — ответ обработчика снимка получен, а тело снимка
// прочитано не полностью: успешный ответ без полного тела запрещён.
var errSnapshotBodyUnread = errors.New("raft: snapshot body not fully consumed")

// tcpConn — исходящее соединение пула: net.Conn и буферизированный writer.
// Ответ читается непосредственно из conn, без собственного буфера чтения.
// Соединением владеет ровно один обмен от изъятия из пула до возврата
// в пул либо закрытия.
type tcpConn struct {
	target contract.ServerAddress
	conn   net.Conn
	w      *bufio.Writer
}

// Release закрывает соединение и освобождает ресурсы.
func (t *tcpConn) Release() error {
	return t.conn.Close()
}

// TCPTransport — реализация Transport поверх TCP с бинарным сетевым
// форматом RPC (pkg/raft/protocol). Содержит пул исходящих соединений
// (LIFO, maxPool на адрес) и переиспользует входящие соединения для
// множества RPC (цикл handleConn).
//
// Потокобезопасность: все методы потокобезопасны.
// После Close() все методы возвращают транспортную ошибку.
type TCPTransport struct {
	consumerCh chan contract.RPC
	localAddr  contract.ServerAddress
	listener   net.Listener

	connectionTimeout      time.Duration
	genericRPCTimeout      time.Duration
	installSnapshotTimeout time.Duration
	responseTimeout        time.Duration

	// limits — пределы сетевого формата; одинаковы для отправки и приёма.
	// Неизменяемы после конструктора.
	limits contract.Limits
	// timing — действующие сроки транспорта, полученные той же
	// нормализацией, что и поля сроков выше. Неизменяемы после конструктора.
	timing contract.TransportTiming

	mu    sync.Mutex
	peers map[contract.ServerID]string

	connPool     map[contract.ServerAddress][]*tcpConn
	connPoolLock sync.Mutex
	maxPool      int

	heartbeatFn     func(contract.RPC)
	heartbeatFnLock sync.Mutex

	activeConns     map[net.Conn]struct{}
	activeConnsLock sync.Mutex

	// rejections — отказы приёма кадров по пределам сетевого формата.
	rejections limitRejections

	shutdownCh chan struct{}
	wg         sync.WaitGroup
}

var (
	_ contract.Transport               = (*TCPTransport)(nil)
	_ contract.LimitsProvider          = (*TCPTransport)(nil)
	_ contract.TransportTimingProvider = (*TCPTransport)(nil)
)

// TCPTimeouts задаёт тайм-ауты TCP-транспорта: установки соединения,
// обычного RPC, передачи снимка и ожидания ответа. Неположительное значение
// каждого поля заменяется константой (см. NormalizeTCPTimeouts).
type TCPTimeouts struct {
	// ConnectionTimeout — лимит времени установки TCP-соединения;
	// нулевое значение заменяется константой.
	ConnectionTimeout time.Duration
	// GenericRPCTimeout — лимит времени обычного RPC (AppendEntries,
	// RequestVote и т. п.); нулевое значение заменяется константой.
	GenericRPCTimeout time.Duration
	// InstallSnapshotTimeout — база лимита времени передачи снимка,
	// масштабируется размером данных; нулевое значение заменяется константой.
	InstallSnapshotTimeout time.Duration
	// ResponseTimeout — лимит времени ожидания ответа на RPC;
	// нулевое значение заменяется константой.
	ResponseTimeout time.Duration
}

// NormalizeTCPTimeouts — единственный источник правил нормализации сроков
// TCP-транспорта. Каждое поле <= 0 заменяется своим умолчанием:
// GenericRPCTimeout и ResponseTimeout — contract.TCPRPCTimeout,
// ConnectionTimeout — contract.ConnectionTCPRPCTimeout,
// InstallSnapshotTimeout — contract.InstallSnapshotTimeout; положительные
// значения сохраняются. Возвращает нормализованную копию и соответствующий
// профиль сроков: BaseSend = GenericRPCTimeout, BaseRecv = ResponseTimeout,
// Dial = ConnectionTimeout, InProcess = false. Функция чистая и идемпотентная:
// не открывает соединений и не запускает горутин.
func NormalizeTCPTimeouts(timeouts TCPTimeouts) (TCPTimeouts, contract.TransportTiming) {
	normalized := timeouts
	if normalized.ConnectionTimeout <= 0 {
		normalized.ConnectionTimeout = contract.ConnectionTCPRPCTimeout
	}
	if normalized.GenericRPCTimeout <= 0 {
		normalized.GenericRPCTimeout = contract.TCPRPCTimeout
	}
	if normalized.InstallSnapshotTimeout <= 0 {
		normalized.InstallSnapshotTimeout = contract.InstallSnapshotTimeout
	}
	if normalized.ResponseTimeout <= 0 {
		normalized.ResponseTimeout = contract.TCPRPCTimeout
	}

	return normalized, contract.TransportTiming{
		BaseSend:  normalized.GenericRPCTimeout,
		BaseRecv:  normalized.ResponseTimeout,
		Dial:      normalized.ConnectionTimeout,
		InProcess: false,
	}
}

// NewTCPTransport создаёт новый TCPTransport, слушающий на указанном адресе,
// с профилем пределов по умолчанию (protocol.DefaultLimits). Принимает адрес
// для прослушивания, тайм-ауты TCPTimeouts и максимальный размер пула
// соединений (0 = _defaultMaxPool). Неположительные тайм-ауты нормализуются
// NormalizeTCPTimeouts: 165/200/310/200 мс (установка соединения, обычный
// RPC, снимок, ответ). Запускает acceptLoop в отдельной горутине.
func NewTCPTransport(addr string, timeouts TCPTimeouts, maxPool int) (*TCPTransport, error) {
	return NewTCPTransportWithLimits(addr, timeouts, maxPool, protocol.DefaultLimits())
}

// NewTCPTransportWithLimits создаёт TCPTransport с заданным профилем
// пределов сетевого формата. Целиком нулевой профиль заменяется профилем
// по умолчанию, недопустимый или частично нулевой отвергается до открытия
// слушателя. Остальное — как у NewTCPTransport.
func NewTCPTransportWithLimits(
	addr string, timeouts TCPTimeouts, maxPool int, limits contract.Limits,
) (*TCPTransport, error) {
	normalizedLimits, err := protocol.NormalizeLimits(limits)
	if err != nil {
		return nil, fmt.Errorf("raft: invalid TCP transport limits: %w", err)
	}
	normalized, timing := NormalizeTCPTimeouts(timeouts)

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("raft: failed to listen on %s: %w", addr, err)
	}
	if maxPool <= 0 {
		maxPool = _defaultMaxPool
	}
	t := &TCPTransport{
		consumerCh:             make(chan contract.RPC),
		localAddr:              contract.ServerAddress(listener.Addr().String()),
		listener:               listener,
		connectionTimeout:      normalized.ConnectionTimeout,
		genericRPCTimeout:      normalized.GenericRPCTimeout,
		installSnapshotTimeout: normalized.InstallSnapshotTimeout,
		responseTimeout:        normalized.ResponseTimeout,
		limits:                 normalizedLimits,
		timing:                 timing,
		peers:                  make(map[contract.ServerID]string),
		connPool:               make(map[contract.ServerAddress][]*tcpConn),
		maxPool:                maxPool,
		activeConns:            make(map[net.Conn]struct{}),
		shutdownCh:             make(chan struct{}),
	}
	t.wg.Add(1)
	go t.acceptLoop()
	return t, nil
}

// Limits возвращает действующий профиль пределов сетевого формата.
func (t *TCPTransport) Limits() contract.Limits {
	return t.limits
}

// TransportTiming возвращает действующие сроки транспорта — результат
// NormalizeTCPTimeouts, вычисленный конструктором. TCP всегда сообщает
// InProcess = false.
func (t *TCPTransport) TransportTiming() contract.TransportTiming {
	return t.timing
}

// LimitRejections возвращает копию счётчика отказов приёма кадров по
// пределам сетевого формата: ключ "<параметр F/N/D/C>|<адрес соседа>",
// значение — число отказов.
func (t *TCPTransport) LimitRejections() map[string]uint64 {
	return t.rejections.snapshot()
}

// Consumer возвращает небуферизированный канал входящих RPC.
func (t *TCPTransport) Consumer() <-chan contract.RPC {
	return t.consumerCh
}

// LocalAddr возвращает адрес, на котором слушает транспорт.
func (t *TCPTransport) LocalAddr() contract.ServerAddress {
	return t.localAddr
}

// SetHeartbeatHandler устанавливает обработчик heartbeat fast-path.
func (t *TCPTransport) SetHeartbeatHandler(fn func(contract.RPC)) {
	t.heartbeatFnLock.Lock()
	defer t.heartbeatFnLock.Unlock()
	t.heartbeatFn = fn
}

// AppendEntries отправляет AppendEntries указанному узлу через пул соединений.
//
//nolint:gocritic
func (t *TCPTransport) AppendEntries(
	peerID contract.ServerID, args contract.AppendEntriesArgs,
) (contract.AppendEntriesReply, error) {
	return typedReply[contract.AppendEntriesReply](t.genericRPC(peerID, &args, rpcTypeAppendEntries))
}

// RequestVote отправляет RequestVote указанному узлу через пул соединений.
func (t *TCPTransport) RequestVote(
	peerID contract.ServerID, args contract.RequestVoteArgs,
) (contract.RequestVoteReply, error) {
	return typedReply[contract.RequestVoteReply](t.genericRPC(peerID, &args, rpcTypeRequestVote))
}

// RequestPreVote отправляет PreVote RPC узлу peerID через пул соединений.
func (t *TCPTransport) RequestPreVote(
	peerID contract.ServerID, args contract.RequestPreVoteArgs,
) (contract.RequestPreVoteReply, error) {
	return typedReply[contract.RequestPreVoteReply](t.genericRPC(peerID, &args, rpcTypeRequestPreVote))
}

// TimeoutNow отправляет TimeoutNowRequest указанному узлу через пул соединений.
func (t *TCPTransport) TimeoutNow(
	peerID contract.ServerID, args contract.TimeoutNowRequest,
) (contract.TimeoutNowResponse, error) {
	return typedReply[contract.TimeoutNowResponse](t.genericRPC(peerID, &args, rpcTypeTimeoutNow))
}

// typedReply разыменовывает ответ ожидаемого типа. Кодек гарантирует тип
// ответа по ожидаемому RPCType; несовпадение — ошибка формата, не паника.
func typedReply[T any](reply any, err error) (T, error) {
	var zero T
	if err != nil {
		return zero, err
	}
	typed, ok := reply.(*T)
	if !ok || typed == nil {
		return zero, fmt.Errorf("raft: %w: unexpected reply %T", protocol.ErrFormat, reply)
	}
	return *typed, nil
}

// InstallSnapshot отправляет снимок узлу peerID: управляющий кадр, затем
// ровно DataSize байт из data, затем читает ответ. Источник короче DataSize —
// ошибка; байты источника сверх DataSize не читаются и остаются у владельца
// data. Соединение снимка закрывается при любом исходе и в пул не
// возвращается; переданный data транспорт не закрывает.
//
//nolint:gocritic
func (t *TCPTransport) InstallSnapshot(
	peerID contract.ServerID, args contract.InstallSnapshotRequest, data io.Reader,
) (contract.InstallSnapshotResponse, error) {
	var zero contract.InstallSnapshotResponse
	if data == nil {
		return zero, errors.New("raft: InstallSnapshot: nil snapshot data reader")
	}
	// Управляющий кадр кодируется и проверяется до изъятия соединения:
	// ошибка кодирования (включая DataSize вне диапазона) не открывает поток.
	frame, err := protocol.AppendRequest(nil, &args, t.limits)
	if err != nil {
		return zero, fmt.Errorf("raft: encode InstallSnapshot request: %w", err)
	}
	timeout, err := scaleTimeout("InstallSnapshot send", t.installSnapshotTimeout, snapshotFactor(args.DataSize))
	if err != nil {
		return zero, err
	}
	target, err := t.lookupPeer(peerID)
	if err != nil {
		return zero, err
	}
	conn, err := t.getConn(target)
	if err != nil {
		return zero, err
	}
	defer t.discardConn(conn)

	if err = conn.conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return zero, fmt.Errorf("raft: InstallSnapshot set deadline: %w", err)
	}
	if err = protocol.WriteFrame(conn.w, frame); err != nil {
		return zero, fmt.Errorf("raft: send InstallSnapshot request: %w", err)
	}
	n, err := io.CopyN(conn.w, data, args.DataSize)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return zero, fmt.Errorf("raft: snapshot source ended after %d of %d bytes: %w",
				n, args.DataSize, io.ErrUnexpectedEOF)
		}
		return zero, fmt.Errorf("raft: send snapshot data (%d of %d bytes): %w", n, args.DataSize, err)
	}
	if err = conn.w.Flush(); err != nil {
		return zero, fmt.Errorf("raft: flush InstallSnapshot: %w", err)
	}
	resp, err := protocol.ReadResponseWithHeader(conn.conn, rpcTypeInstallSnapshot, t.limits, nil)
	if err != nil {
		return zero, fmt.Errorf("raft: read InstallSnapshot response: %w", err)
	}
	if resp.Error != nil {
		return zero, remoteError(resp.Error)
	}
	return typedReply[contract.InstallSnapshotResponse](resp.Reply, nil)
}

// AppendEntriesPipeline не реализован.
func (t *TCPTransport) AppendEntriesPipeline(_ contract.ServerID) (contract.AppendPipeline, error) {
	return nil, contract.ErrNotImplemented
}

// Connect сохраняет адрес для указанного соседа. Если адрес изменился —
// сбрасывает пул соединений для старого адреса.
func (t *TCPTransport) Connect(peerID contract.ServerID, addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if oldAddr, ok := t.peers[peerID]; ok && oldAddr != addr {
		t.removeConnPoolForTarget(contract.ServerAddress(oldAddr))
	}

	t.peers[peerID] = addr
}

// Disconnect удаляет адрес указанного соседа.
func (t *TCPTransport) Disconnect(peerID contract.ServerID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.peers, peerID)
}

// DisconnectAll удаляет адреса всех соседей.
func (t *TCPTransport) DisconnectAll() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peers = make(map[contract.ServerID]string)
}

// Close завершает работу транспорта: закрывает shutdownCh, слушатель,
// удаляет всех соседей, закрывает все соединения в пуле и активные входящие
// соединения, дожидается завершения горутин через wg.Wait(). Идемпотентен.
// Исходящий обмен, уже изъявший соединение из пула, завершается по своему
// сроку.
func (t *TCPTransport) Close() {
	select {
	case <-t.shutdownCh:
		return
	default:
		close(t.shutdownCh)
	}
	_ = t.listener.Close()
	t.DisconnectAll()
	t.CloseStreams()
	t.closeActiveConns()
	t.wg.Wait()
}

// CloseStreams закрывает все соединения в пуле и удаляет их.
// Используется при реконфигурации кластера, когда адреса соседей
// могли измениться.
func (t *TCPTransport) CloseStreams() {
	t.connPoolLock.Lock()
	defer t.connPoolLock.Unlock()
	for key, conns := range t.connPool {
		for _, conn := range conns {
			_ = conn.Release()
		}
		delete(t.connPool, key)
	}
}

// IsShutdown проверяет, закрыт ли транспорт.
func (t *TCPTransport) IsShutdown() bool {
	select {
	case <-t.shutdownCh:
		return true
	default:
		return false
	}
}

// closeActiveConns закрывает все активные входящие соединения.
func (t *TCPTransport) closeActiveConns() {
	t.activeConnsLock.Lock()
	defer t.activeConnsLock.Unlock()
	for conn := range t.activeConns {
		_ = conn.Close()
	}
}

// genericRPC выполняет один обычный обмен запрос–ответ. Запрос полностью
// кодируется и проверяется до изъятия соединения: ошибка кодирования не
// открывает поток и не оставляет в нём частичного кадра. Срок обмена —
// один абсолютный предел на отправку и ответ: genericRPCTimeout, умноженный
// на max(1, ⌈BodyLength/256 КиБ⌉) готового кадра. Соединение возвращается
// в пул только после полностью прочитанного ответа кода 0 или 4; ошибки
// ввода-вывода, формата и срока, а также коды 1–3 закрывают соединение.
// Владелец соединения — эта функция: изъятие и возврат либо закрытие
// выполняются здесь однократно.
func (t *TCPTransport) genericRPC(peerID contract.ServerID, command any, typ protocol.RPCType) (any, error) {
	frame, err := protocol.AppendRequest(nil, command, t.limits)
	if err != nil {
		return nil, fmt.Errorf("raft: encode %s request: %w", rpcTypeName(typ), err)
	}
	window, err := scaleTimeout(rpcTypeName(typ)+" send", t.genericRPCTimeout,
		bodyFactor(uint64(len(frame)-_frameHeaderBytes)))
	if err != nil {
		return nil, err
	}
	target, err := t.lookupPeer(peerID)
	if err != nil {
		return nil, err
	}
	conn, err := t.getConn(target)
	if err != nil {
		return nil, err
	}

	if err = conn.conn.SetDeadline(time.Now().Add(window)); err != nil {
		t.discardConn(conn)
		return nil, fmt.Errorf("raft: %s set deadline: %w", rpcTypeName(typ), err)
	}
	if err = protocol.WriteFrame(conn.w, frame); err != nil {
		t.discardConn(conn)
		return nil, fmt.Errorf("raft: send %s request: %w", rpcTypeName(typ), err)
	}
	if err = conn.w.Flush(); err != nil {
		t.discardConn(conn)
		return nil, fmt.Errorf("raft: flush %s request: %w", rpcTypeName(typ), err)
	}
	resp, err := protocol.ReadResponseWithHeader(conn.conn, typ, t.limits, nil)
	if err != nil {
		t.discardConn(conn)
		return nil, fmt.Errorf("raft: read %s response: %w", rpcTypeName(typ), err)
	}
	if resp.Error != nil {
		if closesConnection(resp.Error) {
			t.discardConn(conn)
		} else {
			t.returnConn(conn)
		}
		return nil, remoteError(resp.Error)
	}
	t.returnConn(conn)
	return resp.Reply, nil
}

// closesConnection сообщает, требует ли удалённая ошибка закрытия
// соединения: коды 1–3 (остановка, истечение окна получателя, несовместимая
// версия) — закрытие, код 4 — соединение живо.
func closesConnection(remote error) bool {
	return errors.Is(remote, contract.ErrRaftShutdown) ||
		errors.Is(remote, contract.ErrEnqueueTimeout) ||
		errors.Is(remote, contract.ErrUnsupportedProtocol)
}

// remoteError возвращает удалённую ошибку вызывающему: маркеры кодов 1–3 —
// сами значения contract, код 4 — ошибка с текстом удалённой стороны.
func remoteError(remote error) error {
	if closesConnection(remote) {
		return remote
	}
	return fmt.Errorf("raft: RPC error from peer: %w", remote)
}

// lookupPeer возвращает адрес соседа по его ID.
func (t *TCPTransport) lookupPeer(peerID contract.ServerID) (contract.ServerAddress, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	addr, ok := t.peers[peerID]
	if !ok {
		return "", fmt.Errorf("raft: unknown peer %d", peerID)
	}
	return contract.ServerAddress(addr), nil
}

// getConn возвращает соединение к указанному адресу из пула или создаёт новое.
func (t *TCPTransport) getConn(target contract.ServerAddress) (*tcpConn, error) {
	if conn := t.getPooledConn(target); conn != nil {
		return conn, nil
	}
	c, err := net.DialTimeout("tcp", string(target), t.connectionTimeout)
	if err != nil {
		return nil, err
	}
	return &tcpConn{
		target: target,
		conn:   c,
		w:      bufio.NewWriterSize(c, _connSendBufferSize),
	}, nil
}

// getPooledConn извлекает соединение из пула (LIFO).
func (t *TCPTransport) getPooledConn(target contract.ServerAddress) *tcpConn {
	t.connPoolLock.Lock()
	defer t.connPoolLock.Unlock()

	conns := t.connPool[target]
	if len(conns) == 0 {
		return nil
	}
	var conn *tcpConn
	lastIdx := len(conns) - 1
	conn, conns[lastIdx] = conns[lastIdx], nil
	t.connPool[target] = conns[:lastIdx]
	return conn
}

// returnConn возвращает соединение в пул, если транспорт не закрыт
// и количество соединений в пуле для этого адреса < maxPool.
// Иначе закрывает соединение.
func (t *TCPTransport) returnConn(conn *tcpConn) {
	t.connPoolLock.Lock()
	defer t.connPoolLock.Unlock()

	key := conn.target
	conns := t.connPool[key]

	if !t.IsShutdown() && len(conns) < t.maxPool {
		t.connPool[key] = append(conns, conn)
	} else {
		_ = conn.Release()
	}
}

// discardConn закрывает исходящее соединение без возврата в пул. Ошибка
// закрытия не заменяет первичную ошибку обмена и записывается в журнал.
func (t *TCPTransport) discardConn(conn *tcpConn) {
	if err := conn.Release(); err != nil && !errors.Is(err, net.ErrClosed) {
		log.Printf("raft: close connection to %s: %v", conn.target, err)
	}
}

// removeConnPoolForTarget удаляет и закрывает все соединения к указанному адресу.
func (t *TCPTransport) removeConnPoolForTarget(target contract.ServerAddress) {
	t.connPoolLock.Lock()
	defer t.connPoolLock.Unlock()
	if conns, ok := t.connPool[target]; ok {
		for _, conn := range conns {
			_ = conn.Release()
		}
		delete(t.connPool, target)
	}
}

// acceptLoop принимает входящие TCP-соединения и запускает handleConn для каждого.
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
		t.trackConn(conn)
		go t.handleConn(conn)
	}
}

// trackConn добавляет соединение в список активных для закрытия при Close.
func (t *TCPTransport) trackConn(conn net.Conn) {
	t.activeConnsLock.Lock()
	defer t.activeConnsLock.Unlock()
	t.activeConns[conn] = struct{}{}
}

// untrackConn удаляет соединение из списка активных.
func (t *TCPTransport) untrackConn(conn net.Conn) {
	t.activeConnsLock.Lock()
	defer t.activeConnsLock.Unlock()
	delete(t.activeConns, conn)
}

// handleConn обслуживает входящее соединение: последовательность обменов
// запрос–ответ, пока обмен разрешает продолжение. Единственный
// bufio.Reader соединения создаётся здесь и передаётся кодеку, а для
// снимка — обработчику через ограничивающий Reader. Любая ошибка кадра,
// ввода-вывода или срока, коды ответа 1–3 и снимок завершают соединение
// без попытки синхронизации потока.
func (t *TCPTransport) handleConn(conn net.Conn) {
	defer t.wg.Done()
	defer t.untrackConn(conn)
	defer func() {
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			log.Printf("raft: close incoming connection from %s: %v", conn.RemoteAddr(), err)
		}
	}()

	r := bufio.NewReaderSize(conn, _connReceiveBufferSize)
	w := bufio.NewWriter(conn)

	for {
		select {
		case <-t.shutdownCh:
			return
		default:
		}

		keepAlive, err := t.serveRequest(conn, r, w)
		if err != nil {
			if parameter := t.rejections.record(remoteHost(conn.RemoteAddr()), err); parameter != "" {
				// Диагностика отказа по пределу без содержимого Data: адрес
				// соседа достоверен до разбора кадра, идентификатор узла и
				// индекс записи до декодирования недоступны.
				log.Printf("raft: limit rejection: direction=recv peer=%s parameter=%s: %v",
					conn.RemoteAddr(), parameter, err)
			}
			if !quietConnError(err) {
				log.Printf("raft: closing connection from %s: %v", conn.RemoteAddr(), err)
			}
			return
		}
		if !keepAlive {
			return
		}
	}
}

// quietConnError сообщает, что ошибка соответствует штатному завершению
// соединения и не требует диагностики: конец потока между кадрами,
// закрытое соединение или остановка транспорта.
func quietConnError(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || errors.Is(err, contract.ErrRaftShutdown)
}

// serveRequest обслуживает один входящий обмен. Ожидание первого байта
// кадра идёт без срока (прежний idle между кадрами). После появления
// первого байта заголовок читается до t_first+responseTimeout; проверенный
// заголовок задаёт одно абсолютное окно D_body = t_header +
// responseTimeout × max(1, ⌈BodyLength/256 КиБ⌉) на тело, очередь
// потребителя, ответ обработчика и Flush. Ни поступление байтов, ни переход
// к следующему этапу окно не продлевают. Ошибка заголовка или хука
// прекращает обмен без чтения тела и без dispatch. Возвращает признак
// продолжения соединения.
func (t *TCPTransport) serveRequest(conn net.Conn, r *bufio.Reader, w *bufio.Writer) (bool, error) {
	if _, err := r.Peek(1); err != nil {
		return false, err
	}
	if err := conn.SetReadDeadline(time.Now().Add(t.responseTimeout)); err != nil {
		return false, fmt.Errorf("raft: set header deadline: %w", err)
	}

	var bodyDeadline time.Time
	beforeBody := func(header protocol.FrameHeader) error {
		window, err := scaleTimeout(rpcTypeName(header.Type)+" receive", t.responseTimeout,
			bodyFactor(header.BodyLength))
		if err != nil {
			return err
		}
		bodyDeadline = time.Now().Add(window)
		if err = conn.SetReadDeadline(bodyDeadline); err != nil {
			return err
		}
		return conn.SetWriteDeadline(bodyDeadline)
	}

	typ, command, err := protocol.ReadRequestWithHeader(r, t.limits, beforeBody)
	if err != nil {
		if command == nil && errors.Is(err, contract.ErrUnsupportedProtocol) {
			// Заголовок и длина тела проверены, несовместима только версия
			// протокола: код 3 до dispatch, затем закрытие соединения.
			t.writeResponseBestEffort(w, typ, contract.RPCResponse{Error: contract.ErrUnsupportedProtocol})
		}
		return false, fmt.Errorf("raft: read request: %w", err)
	}

	if typ == rpcTypeInstallSnapshot {
		return false, t.serveSnapshot(conn, r, w, command)
	}
	return t.serveRegular(conn, w, typ, command, bodyDeadline)
}

// serveRegular передаёт обычный запрос потребителю и отвечает. Очередь
// потребителя, ожидание ответа и запись ответа ограничены тем же D_body.
func (t *TCPTransport) serveRegular(
	conn net.Conn, w *bufio.Writer, typ protocol.RPCType, command any, bodyDeadline time.Time,
) (bool, error) {
	respCh := make(chan contract.RPCResponse, 1)
	rpc := contract.RPC{Command: command, RespChan: respCh}

	if !t.dispatchHeartbeat(typ, rpc) {
		if err := t.dispatch(rpc, bodyDeadline); err != nil {
			t.expireBestEffort(w, typ, err)
			return false, fmt.Errorf("raft: dispatch %s: %w", rpcTypeName(typ), err)
		}
	}
	resp, err := t.awaitResponse(respCh, bodyDeadline)
	if err != nil {
		t.expireBestEffort(w, typ, err)
		return false, fmt.Errorf("raft: await %s response: %w", rpcTypeName(typ), err)
	}
	if err = t.writeResponse(w, typ, resp); err != nil {
		return false, err
	}
	if resp.Error != nil && closesConnection(resp.Error) {
		// Коды 1–3: получатель закрывает соединение после записи ответа.
		return false, nil
	}
	// Срок очищается только для ожидания следующего кадра.
	if err = conn.SetDeadline(time.Time{}); err != nil {
		return false, fmt.Errorf("raft: clear deadline: %w", err)
	}
	return true, nil
}

// dispatchHeartbeat вызывает установленный внешним кодом обработчик пульса
// для пустого AppendEntries после полного разбора кадра. Возвращает true,
// если запрос передан обработчику пульса.
func (t *TCPTransport) dispatchHeartbeat(typ protocol.RPCType, rpc contract.RPC) bool {
	if typ != rpcTypeAppendEntries {
		return false
	}
	args, ok := rpc.Command.(*contract.AppendEntriesArgs)
	if !ok || args.Term == 0 || args.LeaderCommit != 0 || len(args.Entries) != 0 {
		return false
	}
	t.heartbeatFnLock.Lock()
	fn := t.heartbeatFn
	t.heartbeatFnLock.Unlock()
	if fn == nil {
		return false
	}
	fn(rpc)
	return true
}

// serveSnapshot обслуживает InstallSnapshot после полного валидного
// управляющего кадра. Отдельное окно потока снимка начинается здесь:
// max(responseTimeout, installSnapshotTimeout) × max(1, ⌊DataSize/256 КиБ⌋)
// и не продлевается очередью, чтением тела, ожиданием ответа и записью.
// Тело читается обработчиком из того же bufio.Reader через ограничивающий
// Reader ровно DataSize байт; до получения ответа транспорт этот Reader не
// читает. Соединение снимка закрывается при любом исходе.
func (t *TCPTransport) serveSnapshot(conn net.Conn, r *bufio.Reader, w *bufio.Writer, command any) error {
	req, ok := command.(*contract.InstallSnapshotRequest)
	if !ok || req == nil {
		return fmt.Errorf("raft: %w: InstallSnapshot command %T", protocol.ErrFormat, command)
	}
	base := max(t.responseTimeout, t.installSnapshotTimeout)
	window, err := scaleTimeout("InstallSnapshot receive", base, snapshotFactor(req.DataSize))
	if err != nil {
		return err
	}
	streamDeadline := time.Now().Add(window)
	if err = conn.SetDeadline(streamDeadline); err != nil {
		return fmt.Errorf("raft: set snapshot stream deadline: %w", err)
	}

	body := &io.LimitedReader{R: r, N: req.DataSize}
	respCh := make(chan contract.RPCResponse, 1)
	rpc := contract.RPC{Command: req, Reader: body, RespChan: respCh}
	if err = t.dispatch(rpc, streamDeadline); err != nil {
		t.expireBestEffort(w, rpcTypeInstallSnapshot, err)
		return fmt.Errorf("raft: dispatch InstallSnapshot: %w", err)
	}
	resp, err := t.awaitResponse(respCh, streamDeadline)
	if err != nil {
		// Обработчик мог ещё читать тело: транспорт не читает общий
		// Reader и не проверяет его счётчики, а только закрывает
		// соединение, что разблокирует сетевое чтение обработчика.
		t.expireBestEffort(w, rpcTypeInstallSnapshot, err)
		return fmt.Errorf("raft: await InstallSnapshot response: %w", err)
	}
	// Ответ получен: владение ограничивающим Reader вернулось транспорту.
	if body.N > 0 {
		unread := fmt.Errorf("%w: %d of %d bytes unread", errSnapshotBodyUnread, body.N, req.DataSize)
		if resp.Error == nil {
			resp = contract.RPCResponse{Error: unread}
		}
		log.Printf("raft: InstallSnapshot from %s: %v", conn.RemoteAddr(), unread)
	}
	return t.writeResponse(w, rpcTypeInstallSnapshot, resp)
}

// dispatch передаёт RPC потребителю не позже deadline. Истечение срока —
// contract.ErrEnqueueTimeout, остановка транспорта — contract.ErrRaftShutdown.
// Уже истёкшая граница отвергается до постановки: готовый потребитель не
// получает запрос, разобранный после окончания окна.
func (t *TCPTransport) dispatch(rpc contract.RPC, deadline time.Time) error {
	if !time.Now().Before(deadline) {
		return contract.ErrEnqueueTimeout
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case t.consumerCh <- rpc:
		return nil
	case <-t.shutdownCh:
		return contract.ErrRaftShutdown
	case <-timer.C:
		return contract.ErrEnqueueTimeout
	}
}

// awaitResponse ждёт единственный ответ обработчика не позже deadline.
// Истёкшая к началу ожидания или к моменту приёма ответа граница —
// contract.ErrEnqueueTimeout. RespChan имеет ёмкость 1, поэтому поздний
// ответ обработчика не блокирует его и не достаётся другому обмену.
func (t *TCPTransport) awaitResponse(respCh <-chan contract.RPCResponse, deadline time.Time) (contract.RPCResponse, error) {
	if !time.Now().Before(deadline) {
		return contract.RPCResponse{}, contract.ErrEnqueueTimeout
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case resp := <-respCh:
		// Ответ, принятый после границы, — истечение окна: новое окно ради
		// записи ответа не открывается.
		if !time.Now().Before(deadline) {
			return contract.RPCResponse{}, contract.ErrEnqueueTimeout
		}
		return resp, nil
	case <-t.shutdownCh:
		return contract.RPCResponse{}, contract.ErrRaftShutdown
	case <-timer.C:
		return contract.RPCResponse{}, contract.ErrEnqueueTimeout
	}
}

// writeResponse кодирует ответ полностью до записи и отправляет его с Flush.
// Локальная ошибка кодирования (например, ответ без Reply и Error или с
// несовместимой версией) не пишет кадр: соединение закрывается.
func (t *TCPTransport) writeResponse(w *bufio.Writer, typ protocol.RPCType, resp contract.RPCResponse) error {
	frame, err := protocol.AppendResponse(nil, typ, resp, t.limits)
	if err != nil {
		return fmt.Errorf("raft: encode %s response: %w", rpcTypeName(typ), err)
	}
	if err = protocol.WriteFrame(w, frame); err != nil {
		return fmt.Errorf("raft: write %s response: %w", rpcTypeName(typ), err)
	}
	if err = w.Flush(); err != nil {
		return fmt.Errorf("raft: flush %s response: %w", rpcTypeName(typ), err)
	}
	return nil
}

// writeResponseBestEffort пишет ответ перед закрытием соединения; ошибка
// записи не меняет исхода — соединение закрывается в любом случае.
func (t *TCPTransport) writeResponseBestEffort(w *bufio.Writer, typ protocol.RPCType, resp contract.RPCResponse) {
	_ = t.writeResponse(w, typ, resp)
}

// expireBestEffort пишет код 2 при истечении окна получателя. Окно для
// записи не продлевается: при истёкшем сроке сокета запись завершится
// ошибкой, и отправитель получит обрыв соединения.
func (t *TCPTransport) expireBestEffort(w *bufio.Writer, typ protocol.RPCType, cause error) {
	if errors.Is(cause, contract.ErrEnqueueTimeout) {
		t.writeResponseBestEffort(w, typ, contract.RPCResponse{Error: contract.ErrEnqueueTimeout})
	}
}

// bodyFactor — max(1, ⌈bodyLength/256 КиБ⌉) для срока обычного RPC.
func bodyFactor(bodyLength uint64) uint64 {
	factor := bodyLength / _timeoutStepBytes
	if bodyLength%_timeoutStepBytes != 0 {
		factor++
	}
	return max(factor, 1)
}

// snapshotFactor — max(1, ⌊DataSize/256 КиБ⌋) для срока тела снимка.
func snapshotFactor(dataSize int64) uint64 {
	if dataSize <= 0 {
		return 1
	}
	return max(uint64(dataSize)/_timeoutStepBytes, 1)
}

// scaleTimeout умножает положительный base на factor с проверкой
// переполнения до умножения: непредставимый срок — ошибка, а не
// отрицательная или насыщенная длительность.
func scaleTimeout(name string, base time.Duration, factor uint64) (time.Duration, error) {
	if base <= 0 {
		return 0, fmt.Errorf("raft: %s timeout base %v must be positive", name, base)
	}
	if factor > uint64(math.MaxInt64/base) {
		return 0, fmt.Errorf("raft: %s timeout %v x factor %d overflows time.Duration", name, base, factor)
	}
	return base * time.Duration(factor), nil
}
