package protocol_test

import (
	"bytes"
	"encoding/gob"
	"encoding/hex"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/vskurikhin/raft/pkg/kvservice"
	"github.com/vskurikhin/raft/pkg/raft/contract"
	"github.com/vskurikhin/raft/pkg/raft/protocol"
)

func loadGoldenFrame(t *testing.T, name string) []byte {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join("testdata", "golden", name+".hex"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var digits strings.Builder
	for line := range strings.SplitSeq(string(raw), "\n") {
		line, _, _ = strings.Cut(line, "#")
		digits.WriteString(strings.Join(strings.Fields(line), ""))
	}
	frame, err := hex.DecodeString(digits.String())
	if err != nil {
		t.Fatalf("decode golden: %v", err)
	}

	return frame
}

// TestKVCommandGoldenDecode — V01/V10: замороженный независимый gob-поток
// kvservice.Command декодируется в конкретный тип Command со всеми полями.
func TestKVCommandGoldenDecode(t *testing.T) {
	gob.Register(kvservice.Command{})

	_, command, err := protocol.ReadRequest(bytes.NewReader(loadGoldenFrame(t, "ae_request_gob_command")),
		protocol.DefaultLimits())
	if err != nil {
		t.Fatalf("ReadRequest: %v", err)
	}
	args, ok := command.(*contract.AppendEntriesArgs)
	if !ok || len(args.Entries) != 1 {
		t.Fatalf("command = %#v", command)
	}
	want := kvservice.Command{
		Kind: kvservice.CommandCAS, Key: "k", Value: "v", CompareValue: "c", ResultValue: "r", ResultFound: true, ID: -2,
	}
	got, ok := args.Entries[0].Data.(kvservice.Command)
	if !ok || got != want {
		t.Fatalf("Data = %#v, want %#v", args.Entries[0].Data, want)
	}
}

// TestKVCommandRoundTrip — V10/V27: реальная Command с каждым полем
// сохраняет тип FSM; MeasureData равна длине EncodeData.
func TestKVCommandRoundTrip(t *testing.T) {
	gob.Register(kvservice.Command{})

	commands := []kvservice.Command{
		{},
		{Kind: kvservice.CommandGet, Key: "key"},
		{Kind: kvservice.CommandPut, Key: "key", Value: strings.Repeat("v", 2048), ID: math.MaxInt},
		{Kind: kvservice.CommandCAS, Key: "k", Value: "new", CompareValue: "old", ID: -1},
		{Kind: kvservice.CommandDelete, Key: "k", ResultValue: "was", ResultFound: true},
	}
	limits := protocol.DefaultLimits()
	for _, command := range commands {
		encoded, err := protocol.EncodeData(command, limits.MaxDataBytes)
		if err != nil {
			t.Fatalf("EncodeData %+v: %v", command, err)
		}
		measured, err := protocol.MeasureData(command, limits.MaxDataBytes)
		if err != nil || measured != uint64(len(encoded)) {
			t.Fatalf("MeasureData = %d, %v; want %d", measured, err, len(encoded))
		}

		args := &contract.AppendEntriesArgs{
			RPCHeader: contract.RPCHeader{ProtocolVersion: contract.ProtocolVersion, ServerID: 1},
			Entries:   []contract.LogEntry{{Index: 1, Term: 1, Type: contract.LogCommand, Data: command}},
		}
		frame, err := protocol.AppendRequest(nil, args, limits)
		if err != nil {
			t.Fatalf("AppendRequest: %v", err)
		}
		_, decoded, err := protocol.ReadRequest(bytes.NewReader(frame), limits)
		if err != nil || !reflect.DeepEqual(decoded, args) {
			t.Fatalf("round trip %+v: %#v, %v", command, decoded, err)
		}
	}
}
