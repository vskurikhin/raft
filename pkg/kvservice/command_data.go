package kvservice

import "encoding/gob"

// _commandDataName — стабильное имя gob-регистрации носителя команды.
const _commandDataName = "raft.kv.command-data.v1"

// _encodeCommandFunc — кодек, который вызывает GobEncode. Тесты пакета
// заменяют его один раз до запуска, чтобы считать вызовы; в работе
// значение не меняется.
var _encodeCommandFunc = EncodeCommand

// commandData — закрытый неизменяемый носитель KV-команды в журнале Raft.
// Каждая сериализация заново кодирует команду явным кодеком, кэша нет.
// После публикации поле cmd только читается.
type commandData struct {
	cmd Command
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

// newCommandData проверяет команду и создаёт носитель. Поля результата
// не передаются и сбрасываются. Носитель хранит копию cmd. Для созданного
// носителя кодирование не возвращает ошибки.
func newCommandData(cmd *Command) (*commandData, error) {
	if _, err := validateCommand(cmd); err != nil {
		return nil, err
	}
	data := &commandData{cmd: *cmd}
	data.cmd.ResultValue, data.cmd.ResultFound = "", false
	return data, nil
}

// GobEncode реализует gob.GobEncoder. Вызывается под блокировками
// консенсус-модуля и хранилища, поэтому только читает носитель и кодирует
// его: без блокировок, каналов и ввода-вывода.
func (c *commandData) GobEncode() ([]byte, error) {
	return _encodeCommandFunc(c.cmd)
}

// GobDecode реализует gob.GobDecoder: строгое декодирование до публикации
// носителя.
func (c *commandData) GobDecode(data []byte) error {
	cmd, err := DecodeCommand(data)
	if err != nil {
		return err
	}
	c.cmd = cmd
	return nil
}
