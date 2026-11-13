package transp

import (
	"fmt"

	"github.com/vskurikhin/raft/pkg/raft/contract"
)

// appendEntriesWireVersion — действующая версия сетевого представления
// AppendEntries. Ненулевая и известная версия обязательна: 0 и будущие
// значения отвергаются приёмом.
const appendEntriesWireVersion uint8 = 1

// appendEntriesWireEntry — запись журнала в сетевом представлении
// AppendEntries. DataPresent различает nil и непустой пустой срез Data:
// gob опускает нулевые по длине срезы, поэтому без явного признака
// присутствия пустое значение восстанавливалось бы как nil.
type appendEntriesWireEntry struct {
	Index       int
	Term        int
	Type        contract.LogType
	DataPresent bool
	Data        []byte
}

// appendEntriesBytesV1 — частное сетевое представление AppendEntries
// поверх общего gob-конверта. Содержит все поля запроса и записи; поля
// команды не разбираются и не кодируются повторно — передаются готовые
// байты Data. Не является частью публичного API журнала или контракта.
type appendEntriesBytesV1 struct {
	EncodingVersion uint8

	RPCHeader    contract.RPCHeader
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int

	Entries []appendEntriesWireEntry
}

// appendEntriesToWire собирает сетевое представление из публичного
// запроса. Читает поля и байты записей, не изменяя их: отправитель
// сохраняет владение исходными структурами, gob копирует байты в свой
// буфер при кодировании.
func appendEntriesToWire(args *contract.AppendEntriesArgs) appendEntriesBytesV1 {
	entries := make([]appendEntriesWireEntry, len(args.Entries))
	for i := range args.Entries {
		entry := &args.Entries[i]
		entries[i] = appendEntriesWireEntry{
			Index:       entry.Index,
			Term:        entry.Term,
			Type:        entry.Type,
			DataPresent: entry.Data != nil,
			Data:        entry.Data,
		}
	}
	return appendEntriesBytesV1{
		EncodingVersion: appendEntriesWireVersion,
		RPCHeader:       args.RPCHeader,
		Term:            args.Term,
		LeaderID:        args.LeaderID,
		PrevLogIndex:    args.PrevLogIndex,
		PrevLogTerm:     args.PrevLogTerm,
		LeaderCommit:    args.LeaderCommit,
		Entries:         entries,
	}
}

// appendEntriesFromWire восстанавливает публичный запрос из сетевого
// представления. Проверяет версию и согласованность признака
// присутствия с длиной данных; полученные байты принадлежат получателю
// и не ссылаются на буферы транспорта. Ошибка — ошибка сетевого
// представления: вызывающий закрывает соединение, обычный ответ с
// Success=false не отправляется.
func appendEntriesFromWire(wire *appendEntriesBytesV1) (*contract.AppendEntriesArgs, error) {
	if wire.EncodingVersion != appendEntriesWireVersion {
		return nil, fmt.Errorf("raft: unsupported AppendEntries wire version %d", wire.EncodingVersion)
	}
	entries := make([]contract.LogEntry, len(wire.Entries))
	for i := range wire.Entries {
		src := &wire.Entries[i]
		var data []byte
		switch {
		case !src.DataPresent && len(src.Data) == 0:
			data = nil
		case src.DataPresent && len(src.Data) == 0:
			data = make([]byte, 0)
		case src.DataPresent && len(src.Data) > 0:
			data = make([]byte, len(src.Data))
			copy(data, src.Data)
		default:
			return nil, fmt.Errorf("raft: inconsistent AppendEntries Data presence in entry %d", src.Index)
		}
		entries[i] = contract.LogEntry{
			Index: src.Index,
			Term:  src.Term,
			Type:  src.Type,
			Data:  data,
		}
	}
	return &contract.AppendEntriesArgs{
		RPCHeader:    wire.RPCHeader,
		Term:         wire.Term,
		LeaderID:     wire.LeaderID,
		PrevLogIndex: wire.PrevLogIndex,
		PrevLogTerm:  wire.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: wire.LeaderCommit,
	}, nil
}
