// Package kvservice KV-сервис на основе Raft — основной файл реализации.
package kvservice

import (
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/vskurikhin/raft/pkg/api"
	"github.com/vskurikhin/raft/pkg/raft"
)

// TraceKV — порог детализации отладочных сообщений KV-сервиса (traceLogf).
// Сообщение выводится, если TraceKV > 0. Значение задаётся флагом
// --trace-log-level командной строки, по умолчанию 1.
var TraceKV = 1

// _traceLogger — логгер отладочных сообщений KV-сервиса (traceLogf).
// По умолчанию выводит в стандартный логгер (stderr). Перенаправляется
// в файл функцией SetTraceLogFile.
var _traceLogger = log.Default()

// _traceLogFile — открытый файл трассировки; nil, если вывод в stderr.
var _traceLogFile *os.File

// traceConfigured — сторожевой флаг строгого set-once: единственная
// успешная конфигурация трассировки на процесс уже выполнена.
// Устанавливается только успешным вызовом SetTraceLogFile
// (вызов, завершившийся ошибкой I/O, окно не расходует).
var traceConfigured atomic.Bool

// traceCMCreated — сторожевой флаг: в процессе уже создавался
// KVService. Устанавливается конструктором New/NewKVService.
var traceCMCreated atomic.Bool

// SetTraceLogFile конфигурирует трассировку (traceLogf): путь к файлу
// журнала; пустая строка — вывод в стандартный логгер. Файл открывается
// в режиме добавления и создаётся при необходимости.
//
// Контракт (строгий set-once-before-first-service, симметрично pkg/raft):
// функция успешна ровно один раз на процесс, до создания первого
// KVService. Первый успешный вызов выигрывает и замораживает
// конфигурацию; состояние трассировки после этого read-only. Любой
// повторный вызов — детерминированная ошибка контракта, в том числе
// до создания сервиса; вызов после создания — та же ошибка. Вызов,
// завершившийся ошибкой I/O, окно конфигурации не расходует.
func SetTraceLogFile(path string) error {
	if traceConfigured.Load() || traceCMCreated.Load() {
		return fmt.Errorf(
			"kvservice: trace configuration must be set exactly once, " +
				"before the first KVService is created; repeated calls are forbidden",
		)
	}
	if path == "" {
		_traceLogger = log.Default()
		traceConfigured.Store(true)
		return nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	_traceLogFile = f
	_traceLogger = log.New(f, "", log.LstdFlags|log.Lmicroseconds)
	traceConfigured.Store(true)
	return nil
}

// requestTimeout — таймаут для Apply-операций (PUT, CAS).
// Если за это время не удалось отправить команду в applyCh лидера,
// возвращается ErrEnqueueTimeout.
const requestTimeout = 10 * time.Second

type KVService struct {
	// id — идентификатор сервиса в кластере Raft.
	id int

	// rs — сервер Raft, содержащий экземпляр ConsensusModule (CM).
	rs *raft.Server

	// ds — базовое хранилище данных, реализующее KV-базу данных.
	ds *DataStore

	// srv — HTTP-сервер, через который сервис предоставляет внешний API.
	srv *http.Server

	// httpResponsesEnabled controls whether this service returns HTTP responses
	// to the client. It's only used for testing and debugging.
	httpResponsesEnabled bool
}

// Config — конфигурация для создания нового KVService.
// Встраивает raft.Config и добавляет HTTPAddress — адрес,
// на котором сервис будет принимать HTTP-запросы клиентов.
type Config struct {
	raft.Config

	HTTPAddress string
}

// New создаёт новый экземпляр KVService с заданной конфигурацией cfg,
// хранилищем storage и каналом уведомления readyChan.
// cfg содержит параметры Raft-сервера (идентификатор, список узлов,
// RPC-адрес) и HTTP-адрес для REST API сервиса.
//
// KVService реализует raft.FSM: закоммиченные записи журнала применяются
// непосредственно к DataStore через метод Apply.
func New(cfg *Config, readyChan <-chan any) *KVService {
	gob.Register(Command{})

	// Сторожевой флаг контракта трассировки: конфигурация SetTraceLogFile
	// разрешена только до создания первого сервиса (строгий set-once).
	traceCMCreated.Store(true)

	kvs := &KVService{
		id:                   cfg.ServerID,
		ds:                   NewDataStore(),
		httpResponsesEnabled: true,
	}
	cfg.Fsm = kvs

	// raft.Server обрабатывает RPC-вызовы протокола Raft в кластере.
	// KVService передаётся как FSM, поэтому Apply будет вызываться Raft'ом
	// для каждой закоммиченной записи.
	rs := raft.New(&cfg.Config, readyChan)
	rs.Serve(cfg.RPCAddress)
	kvs.rs = rs

	return kvs
}

// Apply реализует raft.FSM. Вызывается Raft'ом для каждой закоммиченной
// записи журнала. Команда применяется к DataStore, результат сохраняется
// в полях ResultValue/ResultFound команды и возвращается в future клиента.
func (kvs *KVService) Apply(log *raft.LogEntry) any {
	cmd, ok := log.Data.(Command)
	if !ok {
		kvs.traceLogf("unknown command type %T", log.Data)
		return nil
	}

	switch cmd.Kind {
	case CommandGet:
		cmd.ResultValue, cmd.ResultFound = kvs.ds.Get(cmd.Key)
	case CommandPut:
		cmd.ResultValue, cmd.ResultFound = kvs.ds.Put(cmd.Key, cmd.Value)
	case CommandCAS:
		cmd.ResultValue, cmd.ResultFound = kvs.ds.CAS(cmd.Key, cmd.CompareValue, cmd.Value)
	default:
		kvs.traceLogf("unknown command kind %v", cmd.Kind)
		return nil
	}

	return cmd
}

// Snapshot реализует raft.FSM.Snapshot.
// Возвращает снимок текущего состояния DataStore.
func (kvs *KVService) Snapshot() (raft.FSMSnapshot, error) {
	data, err := kvs.ds.Snapshot()
	if err != nil {
		return nil, err
	}
	return &kvSnapshot{data: data}, nil
}

// Restore реализует raft.FSM.Restore.
// Восстанавливает состояние DataStore из снэпшота.
func (kvs *KVService) Restore(reader io.ReadCloser) error {
	defer func() { _ = reader.Close() }()
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	return kvs.ds.Restore(data)
}

// kvSnapshot — снимок состояния KVService для raft.FSMSnapshot.
type kvSnapshot struct {
	data []byte
}

func (s *kvSnapshot) Persist(sink raft.SnapshotSink) error {
	_, err := sink.Write(s.data)
	return err
}

func (s *kvSnapshot) Release() {}

// ApplyBatch реализует интерфейс raft.BatchingFSM. Вызывается Raft'ом
// для группового применения закоммиченных записей журнала. Каждая запись
// применяется к DataStore через Apply, результат (Command с заполненными
// полями ResultValue/ResultFound) возвращается в срезе ответов для
// сопоставления с соответствующими future клиентов.
func (kvs *KVService) ApplyBatch(logs []*raft.LogEntry) []any {
	results := make([]any, 0, len(logs))
	for _, l := range logs {
		cmd := kvs.Apply(l)
		results = append(results, cmd)
	}
	return results
}

// NewKVService создаёт новый экземпляр KVService.
//
//   - address - адрес, который будет слушать Raft сервер.
//   - id — идентификатор данного сервиса в кластере Raft.
//   - peerIds — идентификаторы остальных узлов Raft в кластере.
//   - storage — реализация интерфейса raft.Storage, используемая сервисом
//     для долговременного хранения и сохранения своего состояния.
//   - readyChan — канал уведомления, который должен быть закрыт после того,
//     как кластер Raft будет готов к работе (все узлы запущены и соединены
//     друг с другом).
func NewKVService(address string, id int, peerIds []int, storage raft.Storage, readyChan <-chan any) *KVService {
	return New(&Config{
		Config: raft.Config{
			RPCAddress: address,
			ServerID:   id,
			PeerIds:    peerIds,
			Storage:    storage,
		},
	}, readyChan,
	)
}

// IsLeader проверяет, считает ли kvs себя лидером кластера Raft.
// Используется только для тестирования и отладки.
func (kvs *KVService) IsLeader() bool {
	return kvs.rs.IsLeader()
}

// ServeHTTP запускает HTTP-сервер, предоставляющий REST API KV-сервиса
// на указанном TCP-порту. Метод не блокирует выполнение: он запускает
// HTTP-сервер в отдельной горутине и сразу возвращает управление.
// Для корректной остановки сервера вызовите метод Shutdown.
func (kvs *KVService) ServeHTTP(address string) {
	if kvs.srv != nil {
		panic("ServeHTTP called with existing server")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /get/", kvs.handleGet)
	mux.HandleFunc("POST /put/", kvs.handlePut)
	mux.HandleFunc("POST /cas/", kvs.handleCAS)
	mux.HandleFunc("POST /verifyleader/", kvs.handleVerifyLeader)

	kvs.srv = &http.Server{
		Addr:    address,
		Handler: mux,
	}

	go func() {
		kvs.traceLogf("serving HTTP on %s", kvs.srv.Addr)
		if err := kvs.srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatal(err)
		}
		kvs.srv = nil
	}()
}

// Shutdown корректно завершает работу сервиса: останавливает RPC-сервер
// Raft и основной HTTP-сервер. Метод возвращает управление только после
// полного завершения процедуры остановки.
//
// Примечание: перед вызовом Shutdown необходимо вызвать
// DisconnectFromRaftPeers для всех узлов кластера.
func (kvs *KVService) Shutdown() error {
	kvs.traceLogf("shutting down Raft server")
	kvs.rs.Shutdown()

	if kvs.srv != nil {
		kvs.traceLogf("shutting down HTTP server")
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		_ = kvs.srv.Shutdown(ctx)
		kvs.traceLogf("HTTP shutdown complete")
		return nil
	}

	return nil
}

// ToggleHTTPResponsesEnabled управляет тем, будет ли этот сервис отправлять
// HTTP-ответы клиентам. В обычном режиме работы эта возможность всегда
// включена. Для целей тестирования и отладки этот метод можно вызвать
// со значением false; в этом случае сервис не будет отвечать клиентам
// по HTTP.
func (kvs *KVService) ToggleHTTPResponsesEnabled(enable bool) {
	kvs.httpResponsesEnabled = enable
}

func (kvs *KVService) sendHTTPResponse(w http.ResponseWriter, v any) {
	if kvs.httpResponsesEnabled {
		renderJSON(w, v)
	}
}

func (kvs *KVService) handleVerifyLeader(w http.ResponseWriter, _ *http.Request) {
	// ReadIndex-проверка лидерства (Raft §8) без записи в журнал.
	if err := kvs.rs.VerifyLeader().Error(); err != nil {
		kvs.sendHTTPResponse(w, api.StatusResponse{RespStatus: api.StatusNotLeader})
		return
	}
	kvs.sendHTTPResponse(w, api.StatusResponse{RespStatus: api.StatusOK})
}

func (kvs *KVService) handlePut(w http.ResponseWriter, req *http.Request) {
	start := time.Now()
	pr := &api.PutRequest{}
	defer func() {
		elapsed := time.Since(start)
		kvs.traceLogf("HTTP PUT %v took %v", pr, elapsed)
	}()

	if err := readRequestJSON(req, pr); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// ReadIndex-проверка лидерства (Raft §8) без записи в журнал.
	if err := kvs.rs.VerifyLeader().Error(); err != nil {
		kvs.sendHTTPResponse(w, api.PutResponse{
			RespStatus: api.StatusNotLeader,
		})
		return
	}

	cmd := Command{
		Kind:  CommandPut,
		Key:   pr.Key,
		Value: pr.Value,
		ID:    kvs.id,
	}

	future := kvs.rs.Apply(cmd, requestTimeout)

	select {
	case err := <-future.ErrorCh():
		if err != nil {
			kvs.sendHTTPResponse(w, api.PutResponse{
				RespStatus: api.StatusNotLeader,
			})
			return
		}
		cmdResp := future.Response().(Command)
		kvs.sendHTTPResponse(w, api.PutResponse{
			RespStatus: api.StatusOK,
			KeyFound:   cmdResp.ResultFound,
			PrevValue:  cmdResp.ResultValue,
		})

	case <-req.Context().Done():
		return
	}
}

func (kvs *KVService) handleGet(w http.ResponseWriter, req *http.Request) {
	start := time.Now()
	gr := &api.GetRequest{}
	defer func() {
		elapsed := time.Since(start)
		kvs.traceLogf("HTTP GET %v took %v", gr, elapsed)
	}()

	if err := readRequestJSON(req, gr); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// ReadIndex: подтверждение лидерства без записи в raft-журнал (Raft §8).
	future := kvs.rs.VerifyLeader()
	select {
	case err := <-future.ErrorCh():
		if err != nil {
			kvs.sendHTTPResponse(w, api.GetResponse{
				RespStatus: api.StatusNotLeader,
			})
			return
		}
	case <-req.Context().Done():
		return
	}

	// Локальное чтение из DataStore (без raft-журнала, только после ReadIndex).
	value, found := kvs.ds.Get(gr.Key)
	kvs.sendHTTPResponse(w, api.GetResponse{
		RespStatus: api.StatusOK,
		KeyFound:   found,
		Value:      value,
	})
}

func (kvs *KVService) handleCAS(w http.ResponseWriter, req *http.Request) {
	start := time.Now()
	cr := &api.CASRequest{}
	defer func() {
		elapsed := time.Since(start)
		kvs.traceLogf("HTTP CAS %v took %v", cr, elapsed)
	}()

	if err := readRequestJSON(req, cr); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// ReadIndex-проверка лидерства (Raft §8) без записи в журнал.
	if err := kvs.rs.VerifyLeader().Error(); err != nil {
		kvs.sendHTTPResponse(w, api.CASResponse{
			RespStatus: api.StatusNotLeader,
		})
		return
	}

	cmd := Command{
		Kind:         CommandCAS,
		Key:          cr.Key,
		CompareValue: cr.CompareValue,
		Value:        cr.Value,
		ID:           kvs.id,
	}

	future := kvs.rs.Apply(cmd, requestTimeout)

	select {
	case err := <-future.ErrorCh():
		if err != nil {
			kvs.sendHTTPResponse(w, api.CASResponse{
				RespStatus: api.StatusNotLeader,
			})
			return
		}
		cmdResp := future.Response().(Command)
		kvs.sendHTTPResponse(w, api.CASResponse{
			RespStatus: api.StatusOK,
			KeyFound:   cmdResp.ResultFound,
			PrevValue:  cmdResp.ResultValue,
		})

	case <-req.Context().Done():
		return
	}
}

// traceLogf выводит отладочное сообщение, если TraceKV > 0.
func (kvs *KVService) traceLogf(format string, args ...any) {
	if TraceKV > 0 {
		format = fmt.Sprintf("[kv %d] ", kvs.id) + format
		_traceLogger.Printf(format, args...)
	}
}

// Следующие функции существуют исключительно для целей тестирования
// и используются для моделирования различных сбоев.

func (kvs *KVService) ConnectToRaftPeer(peerID int, addr net.Addr) error {
	return kvs.rs.ConnectToPeerWithTimeout(peerID, addr, 2*raft.Quantum*time.Second)
}

func (kvs *KVService) DisconnectFromAllRaftPeers() {
	kvs.rs.DisconnectAll()
}

func (kvs *KVService) DisconnectFromRaftPeer(peerID int) error {
	return kvs.rs.DisconnectPeer(peerID)
}

func (kvs *KVService) GetRaftListenAddr() net.Addr {
	return kvs.rs.GetListenAddr()
}
