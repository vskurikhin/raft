package kvservice

import (
	"bytes"
	"errors"
	"log"
	"strings"
	"testing"

	"github.com/vskurikhin/raft"
	"github.com/vskurikhin/raft/pkg/raft/codec"
)

// TestKVCommandCodecRoundTripGolden: команды всех видов кодируются
// адаптером в формат пакета codec и декодируются обратно без полей
// результата; байты идентичны прямому кодированию codec.Command.
func TestKVCommandCodecRoundTripGolden(t *testing.T) {
	commands := []Command{
		{Kind: CommandGet, ID: 1, Key: "k"},
		{Kind: CommandPut, ID: 2, Key: "k", Value: "v"},
		{Kind: CommandCAS, ID: 3, Key: "k", Value: "n", CompareValue: "o"},
		{Kind: CommandDelete, ID: 4, Key: "k"},
	}
	for _, cmd := range commands {
		encoded, err := EncodeCommand(cmd)
		if err != nil {
			t.Fatalf("EncodeCommand(%+v): %v", cmd, err)
		}
		direct, err := codec.EncodeCommand(codec.Command{
			Kind: uint8(cmd.Kind), ID: cmd.ID,
			Key: cmd.Key, Value: cmd.Value, CompareValue: cmd.CompareValue,
		})
		if err != nil {
			t.Fatalf("codec.EncodeCommand: %v", err)
		}
		if !bytes.Equal(encoded, direct) {
			t.Fatalf("адаптер и codec дают разные байты: %x vs %x", encoded, direct)
		}
		decoded, err := DecodeCommand(encoded)
		if err != nil {
			t.Fatalf("DecodeCommand: %v", err)
		}
		if decoded.Kind != cmd.Kind || decoded.ID != cmd.ID || decoded.Key != cmd.Key ||
			decoded.Value != cmd.Value || decoded.CompareValue != cmd.CompareValue {
			t.Fatalf("команда = %+v, want %+v", decoded, cmd)
		}
		if decoded.ResultValue != "" || decoded.ResultFound {
			t.Fatalf("поля результата не нулевые: %+v", decoded)
		}
	}
}

// TestKVCommandCodecRejectsKind: неизвестный вид сервиса не превращается в
// поддерживаемый вид codec — проверка выполняется до сужения числа.
func TestKVCommandCodecRejectsKind(t *testing.T) {
	if _, err := EncodeCommand(Command{Kind: CommandInvalid, ID: 1}); err == nil {
		t.Fatal("EncodeCommand принял CommandInvalid")
	}
	if _, err := EncodeCommand(Command{Kind: CommandKind(257), ID: 1}); err == nil {
		t.Fatal("EncodeCommand принял вид 257")
	}
	// Вид 257 в самом формате codec невозможен (байт), но неизвестный байт
	// вида декодер codec отвергает сам — проверим через прямые байты.
	encoded, err := codec.EncodeCommand(codec.Command{Kind: codec.KindPut, ID: 1, Key: "k"})
	if err != nil {
		t.Fatalf("codec.EncodeCommand: %v", err)
	}
	encoded[5] = 5
	if _, err := DecodeCommand(encoded); err == nil {
		t.Fatal("DecodeCommand принял неизвестный вид")
	}
}

// TestFSMRejectsCorruptCommandBytes: повреждённый непустой KVC1 в
// зафиксированной записи LogCommand возвращает errCorruptCommand
// результатом записи (Error от Raft не возникает), DataStore не меняется
// этой записью, следующая запись применяется, и каждый узел безусловно
// записывает наблюдаемую строку в стандартный журнал независимо от уровня
// трассировки.
func TestFSMRejectsCorruptCommandBytes(t *testing.T) {
	kvs := &KVService{ds: NewDataStore()}
	kvs.httpResponsesEnabled.Store(true)

	corrupt := []byte("KVC1\x01\x02\x00\x00broken-payload")
	entry := &raft.LogEntry{Index: 7, Term: 3, Type: raft.LogCommand, Data: corrupt}

	var logBuf bytes.Buffer
	previous := log.Default().Writer()
	log.Default().SetOutput(&logBuf)
	defer func() { log.Default().SetOutput(previous) }()

	result := kvs.Apply(entry)
	if result == nil {
		t.Fatal("Apply вернул nil для повреждённой команды")
	}
	err, ok := result.(error)
	if !ok {
		t.Fatalf("результат повреждённой записи = %T, want error", result)
	}
	if !errors.Is(err, errCorruptCommand) {
		t.Fatalf("ошибка %v не помечена errCorruptCommand", err)
	}
	if !strings.Contains(err.Error(), "index 7") {
		t.Fatalf("ошибка %v не содержит индекс записи", err)
	}

	// Наблюдаемая строка безусловно записана в стандартный журнал: индекс,
	// терм и причина; без payload.
	logged := logBuf.String()
	if !strings.Contains(logged, "kvservice: corrupt command index=7 term=3 reason=") {
		t.Fatalf("строка журнала = %q, want строка kvservice: corrupt command index=7 term=3 reason=", logged)
	}
	if strings.Contains(logged, "broken-payload") {
		t.Fatalf("строка журнала содержит payload: %q", logged)
	}

	// Плохая запись не применилась: ключ здоровой команды до её
	// применения отсутствует.
	if _, found := kvs.ds.Get("healthy"); found {
		t.Fatal("ключ здоровой команды уже присутствует до её применения")
	}

	// Следующая валидная запись применяется: FSM продолжает работу.
	valid, err := EncodeCommand(Command{Kind: CommandPut, ID: 1, Key: "healthy", Value: "yes"})
	if err != nil {
		t.Fatalf("EncodeCommand: %v", err)
	}
	next := kvs.Apply(&raft.LogEntry{Index: 8, Term: 3, Type: raft.LogCommand, Data: valid})
	cmd, ok := next.(Command)
	if !ok {
		t.Fatalf("результат валидной записи = %T, want Command", next)
	}
	if cmd.Kind != CommandPut || cmd.ResultFound {
		t.Fatalf("результат = %+v, want Put без раннего ResultFound", cmd)
	}
	if value, found := kvs.ds.Get("healthy"); !found || value != "yes" {
		t.Fatalf("здоровая запись не применена: %q %v", value, found)
	}
}

// TestFSMIgnoresNonCommandEntries: не-командные записи не декодируются
// KV-декодером: noop и конфигурация возвращают nil без ошибки.
func TestFSMIgnoresNonCommandEntries(t *testing.T) {
	kvs := &KVService{ds: NewDataStore()}
	if got := kvs.Apply(&raft.LogEntry{Index: 1, Term: 1, Type: raft.LogNoop, Data: nil}); got != nil {
		t.Fatalf("noop = %v, want nil", got)
	}
	if got := kvs.Apply(&raft.LogEntry{Index: 2, Term: 1, Type: raft.LogConfiguration, Data: []byte{1, 2}}); got != nil {
		t.Fatalf("конфигурация = %v, want nil", got)
	}
}
