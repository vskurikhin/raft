package raft

import (
	"errors"
	"io"
)

// ErrNotImplemented возвращается для RPC-методов, которые ещё не реализованы
// (TimeoutNow, InstallSnapshot, AppendEntriesPipeline).
var ErrNotImplemented = errors.New("raft: not implemented")

// ErrNotReachable возвращается при попытке отправки RPC узлу, который не подключён
// или отключён от данного транспорта.
var ErrNotReachable = errors.New("raft: peer not reachable")

// Заглушки для RPC-типов, которые будут реализованы в будущих фичах.
// Определены здесь для компиляции Transport interface.

// TimeoutNowArgs — заглушка.
type TimeoutNowArgs struct{}

// TimeoutNowReply — заглушка.
type TimeoutNowReply struct{}

// InstallSnapshotRequest — заглушка.
type InstallSnapshotRequest struct{}

// InstallSnapshotResponse — заглушка.
type InstallSnapshotResponse struct{}

// ServerID — тип для идентификатора (ID) сервера в кластере Raft.
type ServerID int

// ServerAddress — тип для адреса сервера.
// Для in-memory транспорта это произвольное имя (например "raft-0").
// Для TCP транспорта это строка вида "host:port".
type ServerAddress string

// RPCResponse содержит ответ на RPC-вызов: либо Reply, либо Error.
//
// Reply — указатель на структуру ответа (*AppendEntriesReply, *RequestVoteReply).
// Error — транспортная ошибка (ErrRaftShutdown, ErrEnqueueTimeout).
// Логические ошибки Raft (term above, conflict) приходят в Reply, не в Error.
type RPCResponse struct {
	Reply any
	Error error
}

// RPC — входящий RPC-запрос, полученный через Consumer().
//
// Command — указатель на структуру аргументов (*AppendEntriesArgs,
// *RequestVoteArgs и т.д.). Обработчик диспатчит по типу Command.
//
// Reader — io.Reader для streaming-данных (InstallSnapshot).
// В текущей реализации не используется.
//
// RespChan — одноразовый канал ёмкостью 1. Обработчик ОБЯЗАН отправить
// ровно одно RPCResponse в RespChan и закрыть/не закрывать канал —
// отправитель не читает после первого ответа. Неотправленный ответ
// приведёт к таймауту в serveConn или блокировке отправителя.
type RPC struct {
	Command  any
	Reader   io.Reader
	RespChan chan<- RPCResponse
}

// AppendPipeline — интерфейс для конвейерной отправки AppendEntries.
// Позволяет отправлять несколько AppendEntries без ожидания ответа,
// а затем асинхронно читать ответы через Consumer().
// В текущей реализации возвращает ErrNotImplemented.
type AppendPipeline interface {
	AppendEntries(AppendEntriesArgs) (AppendEntriesReply, error)
	Consumer() <-chan RPCResponse
	Close() error
}

// Transport — абстракция сетевого уровня Raft.
//
// ConsensusModule использует Transport для отправки RPC запросов узлам
// и получения входящих RPC через Consumer().
//
// Контракты:
//   - Все методы потокобезопасны.
//   - После Close() все методы возвращают ErrRaftShutdown или транспортную
//     ошибку (зависит от реализации).
//   - AppendEntries и RequestVote — блокирующие; таймаут определяется
//     реализацией (t.timeout).
//   - Transport не управляет жизненным циклом соединений — Consumer() не
//     закрывается при Close(). Читатель выходит по shutdownCh.
type Transport interface {
	// Consumer возвращает небуферизированный канал входящих RPC (cap == 0).
	// ConsensusModule читает из него в горутине runRPCReader.
	//
	// Канал не закрывается при Close() — горутина выходит по shutdownCh.
	// Каждое сообщение в канале — один RPC-запрос. RespChan одноразовый.
	Consumer() <-chan RPC

	// AppendEntries отправляет AppendEntries RPC узлу peerID.
	//
	// Возвращает:
	//   - транспортную ошибку при недоступности peer (ErrNotReachable,
	//     ErrRaftShutdown, ErrEnqueueTimeout);
	//   - логическую ошибку Raft (term выше, conflict) — в reply, err == nil.
	//
	// После Close() возвращает ErrRaftShutdown.
	AppendEntries(ServerID, AppendEntriesArgs) (AppendEntriesReply, error)

	// RequestVote отправляет RequestVote RPC узлу peerID.
	//
	// Семантика ошибок аналогична AppendEntries.
	RequestVote(ServerID, RequestVoteArgs) (RequestVoteReply, error)

	// RequestPreVote отправляет PreVote RPC узлу peerID.
	// В текущей реализации возвращает ErrNotImplemented.
	RequestPreVote(ServerID, RequestPreVoteArgs) (RequestPreVoteReply, error)

	// TimeoutNow отправляет TimeoutNow RPC узлу peerID.
	// В текущей реализации возвращает ErrNotImplemented.
	TimeoutNow(ServerID, TimeoutNowArgs) (TimeoutNowReply, error)

	// InstallSnapshot отправляет snapshot узлу peerID.
	// В текущей реализации возвращает ErrNotImplemented.
	InstallSnapshot(ServerID, InstallSnapshotRequest, io.Reader) (InstallSnapshotResponse, error)

	// AppendEntriesPipeline возвращает конвейер для AppendEntries.
	// В текущей реализации возвращает ErrNotImplemented.
	AppendEntriesPipeline(ServerID) (AppendPipeline, error)

	// SetHeartbeatHandler устанавливает обработчик heartbeat-сообщений.
	// Вызывается, когда лидер отправляет пустые AppendEntries для
	// поддержания лидерства. Позволяет оптимизировать heartbeat,
	// минуя полный цикл через runRPCReader.
	SetHeartbeatHandler(func(RPC))

	// LocalAddr возвращает локальный адрес транспорта.
	// Для InmemTransport — произвольное имя (например "raft-0").
	// Для TCPTransport — строка вида "host:port".
	LocalAddr() ServerAddress
}
