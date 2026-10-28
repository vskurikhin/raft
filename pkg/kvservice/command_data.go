package kvservice

import (
	"encoding/gob"
	"errors"
)

// _commandDataName — стабильное имя gob-регистрации носителя команды.
const _commandDataName = "raft.kv.command-data.v1"

// _encodeCommandFunc — кодек, который вызывает конструктор носителя. Тесты
// пакета заменяют его один раз до запуска, чтобы считать вызовы; в работе
// значение не меняется.
var _encodeCommandFunc = EncodeCommand

// errEmptyCommandData — носитель создан в обход конструктора и не имеет
// готового представления.
var errEmptyCommandData = errors.New("kvservice: command data has no encoded representation")

// commandData — закрытый неизменяемый носитель KV-команды в журнале Raft.
// Хранит команду и её готовое представление: кодек вызывается один раз в
// конструкторе, сериализация только отдаёт готовые байты. Оба поля
// заполняются до публикации и потом только читаются; память принадлежит
// носителю и освобождается сборщиком мусора вместе с записью журнала.
type commandData struct {
	cmd     Command
	encoded []byte
}

var (
	_ gob.GobEncoder = (*commandData)(nil)
	_ gob.GobDecoder = (*commandData)(nil)
)

// registerGobTypes регистрирует типы команд, записываемые в журнал:
// прежний Command и носитель *commandData под стабильным именем.
func registerGobTypes() {
	gob.Register(Command{})
	gob.RegisterName(_commandDataName, &commandData{})
}

// newCommandData проверяет команду, кодирует её один раз и создаёт
// носитель. Поля результата не передаются и сбрасываются. Носитель хранит
// копию cmd и новый буфер кодека, на который больше никто не ссылается.
// Ошибка возвращается до Apply.
func newCommandData(cmd *Command) (*commandData, error) {
	if _, err := validateCommand(cmd); err != nil {
		return nil, err
	}
	data := &commandData{cmd: *cmd}
	data.cmd.ResultValue, data.cmd.ResultFound = "", false
	encoded, err := _encodeCommandFunc(data.cmd)
	if err != nil {
		return nil, err
	}
	data.encoded = encoded
	return data, nil
}

// GobEncode реализует gob.GobEncoder. Вызывается под блокировками
// консенсус-модуля и хранилища, поэтому только читает носитель: без
// кодека, блокировок, каналов и ввода-вывода. Возвращает готовое
// представление; срез ограничен по ёмкости, append вызывающего не
// попадёт в память носителя. gob копирует байты в свой буфер и не
// изменяет их.
func (c *commandData) GobEncode() ([]byte, error) {
	if len(c.encoded) == 0 {
		return nil, errEmptyCommandData
	}
	return c.encoded[:len(c.encoded):len(c.encoded)], nil
}

// GobDecode реализует gob.GobDecoder. gob передаёт срез своего внутреннего
// буфера, поэтому вход копируется в память носителя и строго декодируется;
// поля заполняются только после успешной проверки.
func (c *commandData) GobDecode(data []byte) error {
	encoded := make([]byte, len(data))
	copy(encoded, data)
	cmd, err := DecodeCommand(encoded)
	if err != nil {
		return err
	}
	c.cmd, c.encoded = cmd, encoded
	return nil
}
