package kvservice

import (
	"errors"
	"fmt"

	"github.com/vskurikhin/raft/pkg/raft/codec"
)

// errCorruptCommand — байты команды из журнала не декодируются. FSM
// возвращает её результатом записи вместо Command и продолжает работу.
var errCorruptCommand = errors.New("kvservice: corrupt command in log")

// EncodeCommand кодирует команду в формат пакета codec. Вид команды
// проверяется до сужения числа: неизвестный вид не превращается в
// поддерживаемый. Поля результата не кодируются — их вычисляет машина
// состояний.
//
//nolint:gocritic // сигнатура по значению: адаптер принимает неизменяемую копию команды.
func EncodeCommand(cmd Command) ([]byte, error) {
	kind, err := commandCodecKind(cmd.Kind)
	if err != nil {
		return nil, err
	}
	return codec.EncodeCommand(codec.Command{
		Kind:         kind,
		ID:           cmd.ID,
		Key:          cmd.Key,
		Value:        cmd.Value,
		CompareValue: cmd.CompareValue,
	})
}

// DecodeCommand декодирует байты журнала в команду. Поля результата
// остаются нулевыми: их вычисляет машина состояний.
func DecodeCommand(data []byte) (Command, error) {
	decoded, err := codec.DecodeCommand(data)
	if err != nil {
		return Command{}, err
	}
	kind, err := commandKindFromCodec(decoded.Kind)
	if err != nil {
		return Command{}, err
	}
	return Command{
		Kind:         kind,
		ID:           decoded.ID,
		Key:          decoded.Key,
		Value:        decoded.Value,
		CompareValue: decoded.CompareValue,
	}, nil
}

// commandCodecKind переводит вид команды сервиса в вид кодека. Проверка
// выполняется до сужения числа: значение вне CommandGet…CommandDelete
// отвергается, а не усекается.
func commandCodecKind(kind CommandKind) (uint8, error) {
	switch kind {
	case CommandGet, CommandPut, CommandCAS, CommandDelete:
		return uint8(kind), nil
	default:
		return 0, fmt.Errorf("kvservice: unsupported command kind %d", kind)
	}
}

// commandKindFromCodec — обратное преобразование вида кодека в вид
// команды сервиса.
func commandKindFromCodec(kind uint8) (CommandKind, error) {
	switch kind {
	case codec.KindGet:
		return CommandGet, nil
	case codec.KindPut:
		return CommandPut, nil
	case codec.KindCAS:
		return CommandCAS, nil
	case codec.KindDelete:
		return CommandDelete, nil
	default:
		return CommandInvalid, fmt.Errorf("kvservice: unsupported command kind %d", kind)
	}
}
