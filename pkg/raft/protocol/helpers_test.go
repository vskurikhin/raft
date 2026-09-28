package protocol

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vskurikhin/raft/pkg/raft/contract"
)

// smallLimits — наименьший допустимый F с малыми N/D/C для проверок механики.
var smallLimits = Limits{MaxFrameBytes: 4128, MaxEntries: 3, MaxDataBytes: 64, MaxConfigurationBytes: 16}

// loadGolden читает независимый эталон testdata/golden/<name>.hex:
// шестнадцатеричные байты, пробелы и комментарии после '#'.
func loadGolden(t testing.TB, name string) []byte {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join("testdata", "golden", name+".hex"))
	if err != nil {
		t.Fatalf("read golden %s: %v", name, err)
	}

	var digits strings.Builder
	for line := range strings.SplitSeq(string(raw), "\n") {
		if i := strings.IndexByte(line, '#'); i >= 0 {
			line = line[:i]
		}
		digits.WriteString(strings.Join(strings.Fields(line), ""))
	}

	frame, err := hex.DecodeString(digits.String())
	if err != nil {
		t.Fatalf("decode golden %s: %v", name, err)
	}

	return frame
}

func mustHex(t testing.TB, s string) []byte {
	t.Helper()

	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("decode hex: %v", err)
	}

	return b
}

// patchUint64 возвращает копию кадра с u64/i64 по смещению.
func patchUint64(frame []byte, offset int, value uint64) []byte {
	out := bytes.Clone(frame)
	binary.BigEndian.PutUint64(out[offset:], value)

	return out
}

// patchByte возвращает копию кадра с байтом по смещению.
func patchByte(frame []byte, offset int, value byte) []byte {
	out := bytes.Clone(frame)
	out[offset] = value

	return out
}

// withBodyLength возвращает копию кадра с новым BodyLength в заголовке.
func withBodyLength(frame []byte, bodyLength uint64) []byte {
	return patchUint64(frame, offsetBodyLength, bodyLength)
}

// countingReader считает байты, отданные нижележащим Reader вызывающему.
type countingReader struct {
	r        io.Reader
	consumed int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.consumed += n

	return n, err
}

// hookRecorder — BeforeBody, запоминающий вызовы и прочитанное к моменту
// вызова число байтов.
type hookRecorder struct {
	calls      int
	header     FrameHeader
	consumedAt int
	spy        *countingReader
	err        error
}

func (h *hookRecorder) hook(header FrameHeader) error {
	h.calls++
	h.header = header
	if h.spy != nil {
		h.consumedAt = h.spy.consumed
	}

	return h.err
}

func readRequestBytes(frame []byte, limits Limits) (RPCType, any, error) {
	return ReadRequest(bytes.NewReader(frame), limits)
}

func readResponseBytes(frame []byte, expected RPCType, limits Limits) (contract.RPCResponse, error) {
	return ReadResponse(bytes.NewReader(frame), expected, limits)
}

func requireIs(t *testing.T, err, target error) {
	t.Helper()

	if !errors.Is(err, target) {
		t.Fatalf("error = %v, want errors.Is %v", err, target)
	}
}

func header3(serverID int) contract.RPCHeader {
	return contract.RPCHeader{ProtocolVersion: contract.ProtocolVersion, ServerID: serverID}
}
