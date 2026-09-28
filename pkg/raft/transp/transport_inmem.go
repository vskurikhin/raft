package transp

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/vskurikhin/raft/pkg/raft/contract"
	"github.com/vskurikhin/raft/pkg/raft/protocol"
)

// InmemTransport — реализация Transport, работающая в оперативной памяти
// (без TCP, без net/rpc, без gob-сериализации). Предназначена для
// использования в тестах (Harness). Каждый вызов AppendEntries,
// RequestVote и т.д. отправляет RPC через канал целевому узлу.
//
// Потокобезопасность: Connect/Disconnect/IsDisconnected защищены sync.Mutex.
// Consumer() и LocalAddr() не требуют блокировки.
// После Close() все методы возвращают contract.ErrRaftShutdown.
type InmemTransport struct {
	consumerCh chan contract.RPC
	localAddr  contract.ServerAddress
	mu         sync.Mutex
	peers      map[contract.ServerID]*InmemTransport
	heartbeat  func(contract.RPC)
	shutdownCh chan struct{}
	timeout    time.Duration
	// limits — профиль пределов, защитно проверяемый при отправке и при
	// приёме. Неизменяем после конструктора.
	limits contract.Limits
	// rejections — отказы приёма по пределам этого транспорта.
	rejections limitRejections
}

var (
	_ contract.Transport               = (*InmemTransport)(nil)
	_ contract.LimitsProvider          = (*InmemTransport)(nil)
	_ contract.TransportTimingProvider = (*InmemTransport)(nil)
)

// InmemTransportTimeout — тайм-аут одного RPC внутрипроцессного
// транспорта (500 мс). Корневой тестовый харнесс выводит свои
// бюджеты ожидания из этой величины (_inmemRPCTimeout), поэтому
// изменение значения меняет и бюджеты тестов. Экспортируется,
// чтобы бюджеты тестов корневого пакета ссылались на единую
// точку значения и рассинхрон был исключён компилятором.
const InmemTransportTimeout = 500 * time.Millisecond

// NewInmemTransport создаёт новый InmemTransport с заданным локальным адресом
// и профилем пределов по умолчанию (protocol.DefaultLimits).
// Тайм-аут по умолчанию — 500ms.
func NewInmemTransport(addr contract.ServerAddress) *InmemTransport {
	return newInmemTransport(addr, protocol.DefaultLimits())
}

// NewInmemTransportWithLimits создаёт InmemTransport с заданным профилем
// пределов. Целиком нулевой профиль заменяется профилем по умолчанию,
// недопустимый или частично нулевой отвергается.
func NewInmemTransportWithLimits(addr contract.ServerAddress, limits contract.Limits) (*InmemTransport, error) {
	normalized, err := protocol.NormalizeLimits(limits)
	if err != nil {
		return nil, fmt.Errorf("raft: invalid in-memory transport limits: %w", err)
	}
	return newInmemTransport(addr, normalized), nil
}

func newInmemTransport(addr contract.ServerAddress, limits contract.Limits) *InmemTransport {
	return &InmemTransport{
		consumerCh: make(chan contract.RPC),
		localAddr:  addr,
		peers:      make(map[contract.ServerID]*InmemTransport),
		shutdownCh: make(chan struct{}),
		timeout:    InmemTransportTimeout,
		limits:     limits,
	}
}

// Limits возвращает действующий профиль пределов транспорта.
func (t *InmemTransport) Limits() contract.Limits {
	return t.limits
}

// TransportTiming возвращает сроки внутрипроцессного транспорта: окно
// обмена InmemTransportTimeout на обеих сторонах, без установки соединения.
// InProcess = true: сетевого обмена нет, сетевое условие временной модели
// к транспорту не применяется.
func (t *InmemTransport) TransportTiming() contract.TransportTiming {
	return contract.TransportTiming{
		BaseSend:  t.timeout,
		BaseRecv:  t.timeout,
		Dial:      0,
		InProcess: true,
	}
}

// checkAppendEntries защитно проверяет запрос по тому же профилю пределов,
// что и сетевой формат: число записей не больше MaxEntries, сетевая длина
// Data каждой записи — protocol.MeasureData — не больше MaxDataBytes.
// Сетевой кадр не строится; Data обязана быть gob-кодируемой.
func (t *InmemTransport) checkAppendEntries(args *contract.AppendEntriesArgs) error {
	if uint64(len(args.Entries)) > t.limits.MaxEntries {
		return fmt.Errorf("raft: %w: AppendEntries EntryCount %d exceeds MaxEntries %d",
			protocol.ErrLimit, len(args.Entries), t.limits.MaxEntries)
	}
	for i := range args.Entries {
		if _, err := protocol.MeasureData(args.Entries[i].Data, t.limits.MaxDataBytes); err != nil {
			if errors.Is(err, protocol.ErrLimit) {
				return fmt.Errorf("raft: AppendEntries entry index %d exceeds MaxDataBytes %d: %w",
					args.Entries[i].Index, t.limits.MaxDataBytes, err)
			}
			return fmt.Errorf("raft: AppendEntries entry index %d: %w", args.Entries[i].Index, err)
		}
	}
	return nil
}

// checkSnapshotConfiguration проверяет длину конфигурации снимка по C.
func checkSnapshotConfiguration(req *contract.InstallSnapshotRequest, limits contract.Limits) error {
	if uint64(len(req.Configuration)) > limits.MaxConfigurationBytes {
		return fmt.Errorf("raft: %w: InstallSnapshot configuration %d bytes exceeds MaxConfigurationBytes %d",
			protocol.ErrLimit, len(req.Configuration), limits.MaxConfigurationBytes)
	}
	return nil
}

// LimitRejections возвращает копию счётчика отказов приёма по пределам:
// ключ "<параметр F/N/D/C>|<адрес отправителя>", значение — число отказов.
func (t *InmemTransport) LimitRejections() map[string]uint64 {
	return t.rejections.snapshot()
}

// Consumer возвращает небуферизированный канал входящих RPC.
func (t *InmemTransport) Consumer() <-chan contract.RPC {
	return t.consumerCh
}

// LocalAddr возвращает локальный адрес транспорта.
func (t *InmemTransport) LocalAddr() contract.ServerAddress {
	return t.localAddr
}

// AppendEntries отправляет AppendEntries RPC узлу peerID.
// Блокируется до получения ответа или тайм-аута. Тайм-аут — t.timeout (500ms).
// Возвращает contract.ErrNotReachable, contract.ErrRaftShutdown или contract.ErrEnqueueTimeout.
//
//nolint:gocritic
func (t *InmemTransport) AppendEntries(
	peerID contract.ServerID, args contract.AppendEntriesArgs,
) (contract.AppendEntriesReply, error) {
	var zero contract.AppendEntriesReply
	select {
	case <-t.shutdownCh:
		return zero, contract.ErrRaftShutdown
	default:
	}
	if err := t.checkAppendEntries(&args); err != nil {
		return zero, err
	}
	peer, err := t.getPeer(peerID)
	if err != nil {
		return zero, err
	}
	// Защитная проверка профиля получателя до доставки; при равных
	// профилях повторное измерение не требуется.
	if peer.limits != t.limits {
		if err = peer.checkAppendEntries(&args); err != nil {
			peer.rejections.record(string(t.localAddr), err)
			return zero, fmt.Errorf("raft: receiver %s: %w", peer.localAddr, err)
		}
	}
	respCh := make(chan contract.RPCResponse, 1)
	select {
	case peer.consumerCh <- contract.RPC{Command: &args, RespChan: respCh}:
	case <-t.shutdownCh:
		return zero, contract.ErrRaftShutdown
	case <-peer.shutdownCh:
		return zero, contract.ErrRaftShutdown
	case <-time.After(t.timeout):
		// Таймаут защищает от вечной блокировки при остановленном получателе.
		return zero, contract.ErrEnqueueTimeout
	}
	select {
	case resp := <-respCh:
		if resp.Error != nil {
			return zero, resp.Error
		}
		reply, ok := resp.Reply.(*contract.AppendEntriesReply)
		if !ok {
			return zero, fmt.Errorf("raft: unexpected reply type %T", resp.Reply)
		}
		return *reply, nil
	case <-peer.shutdownCh:
		return zero, contract.ErrRaftShutdown
	case <-t.shutdownCh:
		return zero, contract.ErrRaftShutdown
	case <-time.After(t.timeout):
		return zero, contract.ErrEnqueueTimeout
	}
}

// RequestVote отправляет RequestVote RPC узлу peerID.
// Семантика ошибок и таймаут аналогичны AppendEntries.
func (t *InmemTransport) RequestVote(
	peerID contract.ServerID, args contract.RequestVoteArgs,
) (contract.RequestVoteReply, error) {
	var zero contract.RequestVoteReply
	select {
	case <-t.shutdownCh:
		return zero, contract.ErrRaftShutdown
	default:
	}
	peer, err := t.getPeer(peerID)
	if err != nil {
		return zero, err
	}
	respCh := make(chan contract.RPCResponse, 1)
	select {
	case peer.consumerCh <- contract.RPC{Command: &args, RespChan: respCh}:
	case <-t.shutdownCh:
		return zero, contract.ErrRaftShutdown
	case <-peer.shutdownCh:
		return zero, contract.ErrRaftShutdown
	case <-time.After(t.timeout):
		// Таймаут защищает от вечной блокировки при остановленном получателе.
		return zero, contract.ErrEnqueueTimeout
	}
	select {
	case resp := <-respCh:
		if resp.Error != nil {
			return zero, resp.Error
		}
		reply, ok := resp.Reply.(*contract.RequestVoteReply)
		if !ok {
			return zero, fmt.Errorf("raft: unexpected reply type %T", resp.Reply)
		}
		return *reply, nil
	case <-peer.shutdownCh:
		return zero, contract.ErrRaftShutdown
	case <-t.shutdownCh:
		return zero, contract.ErrRaftShutdown
	case <-time.After(t.timeout):
		return zero, contract.ErrEnqueueTimeout
	}
}

// RequestPreVote отправляет PreVote RPC узлу peerID.
// Семантика ошибок и тайм-аут аналогичны RequestVote.
func (t *InmemTransport) RequestPreVote(
	peerID contract.ServerID, args contract.RequestPreVoteArgs,
) (contract.RequestPreVoteReply, error) {
	var zero contract.RequestPreVoteReply
	select {
	case <-t.shutdownCh:
		return zero, contract.ErrRaftShutdown
	default:
	}
	peer, err := t.getPeer(peerID)
	if err != nil {
		return zero, err
	}
	respCh := make(chan contract.RPCResponse, 1)
	select {
	case peer.consumerCh <- contract.RPC{Command: &args, RespChan: respCh}:
	case <-t.shutdownCh:
		return zero, contract.ErrRaftShutdown
	case <-peer.shutdownCh:
		return zero, contract.ErrRaftShutdown
	case <-time.After(t.timeout):
		// Ограничение блокировки отправителя:
		// при остановленном получателе с ещё открытым транспортом
		// enqueue не должен блокироваться навсегда — join в Stop()
		// обязан оставаться конечным.
		return zero, contract.ErrEnqueueTimeout
	}
	select {
	case resp := <-respCh:
		if resp.Error != nil {
			return zero, resp.Error
		}
		reply, ok := resp.Reply.(*contract.RequestPreVoteReply)
		if !ok {
			return zero, fmt.Errorf("raft: unexpected reply type %T", resp.Reply)
		}
		return *reply, nil
	case <-peer.shutdownCh:
		return zero, contract.ErrRaftShutdown
	case <-t.shutdownCh:
		return zero, contract.ErrRaftShutdown
	case <-time.After(t.timeout):
		return zero, contract.ErrEnqueueTimeout
	}
}

// TimeoutNow отправляет TimeoutNowRequest узлу peerID.
// Реализация аналогична RequestVote: создаёт RPC, отправляет в consumerCh,
// ожидает ответ с таймаутом t.timeout.
func (t *InmemTransport) TimeoutNow(
	peerID contract.ServerID, args contract.TimeoutNowRequest,
) (contract.TimeoutNowResponse, error) {
	var zero contract.TimeoutNowResponse
	select {
	case <-t.shutdownCh:
		return zero, contract.ErrRaftShutdown
	default:
	}
	peer, err := t.getPeer(peerID)
	if err != nil {
		return zero, err
	}
	respCh := make(chan contract.RPCResponse, 1)
	select {
	case peer.consumerCh <- contract.RPC{Command: &args, RespChan: respCh}:
	case <-t.shutdownCh:
		return zero, contract.ErrRaftShutdown
	case <-peer.shutdownCh:
		return zero, contract.ErrRaftShutdown
	case <-time.After(t.timeout):
		// Ограничение блокировки отправителя:
		// при остановленном получателе с ещё открытым транспортом
		// enqueue не должен блокироваться навсегда — join в Stop()
		// обязан оставаться конечным.
		return zero, contract.ErrEnqueueTimeout
	}
	select {
	case resp := <-respCh:
		if resp.Error != nil {
			return zero, resp.Error
		}
		reply, ok := resp.Reply.(*contract.TimeoutNowResponse)
		if !ok {
			return zero, fmt.Errorf("raft: unexpected reply type %T", resp.Reply)
		}
		return *reply, nil
	case <-peer.shutdownCh:
		return zero, contract.ErrRaftShutdown
	case <-t.shutdownCh:
		return zero, contract.ErrRaftShutdown
	case <-time.After(t.timeout):
		return zero, contract.ErrEnqueueTimeout
	}
}

// InstallSnapshot отправляет InstallSnapshot RPC узлу peerID.
// В отличие от AppendEntries/RequestVote, данные снимка передаются
// через поле RPC.Reader, а RespChan — через поле RPC.RespChan.
//
//nolint:gocritic
func (t *InmemTransport) InstallSnapshot(
	peerID contract.ServerID, req contract.InstallSnapshotRequest, data io.Reader,
) (contract.InstallSnapshotResponse, error) {
	var zero contract.InstallSnapshotResponse
	select {
	case <-t.shutdownCh:
		return zero, contract.ErrRaftShutdown
	default:
	}
	if err := checkSnapshotConfiguration(&req, t.limits); err != nil {
		return zero, err
	}
	peer, err := t.getPeer(peerID)
	if err != nil {
		return zero, err
	}
	if err = checkSnapshotConfiguration(&req, peer.limits); err != nil {
		peer.rejections.record(string(t.localAddr), err)
		return zero, fmt.Errorf("raft: receiver %s: %w", peer.localAddr, err)
	}
	respCh := make(chan contract.RPCResponse, 1)
	select {
	case peer.consumerCh <- contract.RPC{Command: &req, Reader: data, RespChan: respCh}:
	case <-t.shutdownCh:
		return zero, contract.ErrRaftShutdown
	case <-peer.shutdownCh:
		return zero, contract.ErrRaftShutdown
	case <-time.After(t.timeout):
		return zero, contract.ErrEnqueueTimeout
	}
	select {
	case resp := <-respCh:
		if resp.Error != nil {
			return zero, resp.Error
		}
		reply, ok := resp.Reply.(*contract.InstallSnapshotResponse)
		if !ok {
			return zero, fmt.Errorf("raft: unexpected reply type %T", resp.Reply)
		}
		return *reply, nil
	case <-peer.shutdownCh:
		return zero, contract.ErrRaftShutdown
	case <-t.shutdownCh:
		return zero, contract.ErrRaftShutdown
	case <-time.After(t.timeout):
		return zero, contract.ErrEnqueueTimeout
	}
}

// AppendEntriesPipeline возвращает конвейер. Не реализован.
func (t *InmemTransport) AppendEntriesPipeline(_ contract.ServerID) (contract.AppendPipeline, error) {
	return nil, contract.ErrNotImplemented
}

// SetHeartbeatHandler сохраняет обработчик heartbeat-сообщений.
func (t *InmemTransport) SetHeartbeatHandler(h func(contract.RPC)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.heartbeat = h
}

// DisconnectAll отключает все соединения указанного транспорта.
func (t *InmemTransport) DisconnectAll() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peers = make(map[contract.ServerID]*InmemTransport)
}

// Connect добавляет peer в карту peers. Используется Harness для
// установки логического соединения между двумя узлами.
func (t *InmemTransport) Connect(peerID contract.ServerID, transport *InmemTransport) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peers[peerID] = transport
}

// Disconnect удаляет peer из карты peers, разрывая логическое соединение.
func (t *InmemTransport) Disconnect(peerID contract.ServerID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.peers, peerID)
}

// IsDisconnected проверяет, отключён ли указанный peer.
func (t *InmemTransport) IsDisconnected(peerID contract.ServerID) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.peers[peerID]; !ok {
		return true
	}
	return false
}

// Close завершает работу транспорта: закрывает shutdownCh, дренирует consumerCh
// и очищает карту peers.
//
// После Close все вызовы AppendEntries/RequestVote вернут contract.ErrRaftShutdown.
// consumerCh не закрывается — runRPCReader выходит по shutdownCh.
// Close идемпотентен: повторный вызов — no-op.
//
// Дренирование consumerCh гарантирует, что отправители не заблокируются
// навсегда: после закрытия shutdownCh они не слают новые запросы,
// а уже отправленные будут прочитаны и отброшены.
func (t *InmemTransport) Close() {
	select {
	case <-t.shutdownCh:
	default:
		close(t.shutdownCh)
	}
	// Дренируем consumerCh: после закрытия shutdownCh отправители
	// не будут слать новые запросы, остаётся только обработать уже
	// отправленные, чтобы они не заблокировали отправителя навсегда.
	for {
		select {
		case _, ok := <-t.consumerCh:
			if !ok {
				return
			}
		default:
			goto done
		}
	}
done:
	t.mu.Lock()
	t.peers = make(map[contract.ServerID]*InmemTransport)
	t.mu.Unlock()
}

// getPeer возвращает транспорт соседа по его ID. Потокобезопасна.
func (t *InmemTransport) getPeer(peerID contract.ServerID) (*InmemTransport, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	peer, ok := t.peers[peerID]
	if !ok {
		return nil, contract.ErrNotReachable
	}
	return peer, nil
}
