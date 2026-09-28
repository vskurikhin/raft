package config

import (
	"flag"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/vskurikhin/raft/pkg/kvservice"
	"github.com/vskurikhin/raft/pkg/raft/contract"
	"github.com/vskurikhin/raft/pkg/raft/protocol"
)

// TestParseFlagsLimitDefaults — без флагов пределы узла: ключ 512, значение
// 2048, D 8192 (профиль по умолчанию).
func TestParseFlagsLimitDefaults(t *testing.T) {
	origArgs := os.Args
	t.Cleanup(func() { os.Args = origArgs })
	os.Args = []string{"raft", "-number", "1"}

	v := ParseFlags()
	if v.MaxKeyBytes != 512 || v.MaxValueBytes != 2048 || v.MaxDataBytes != 8192 {
		t.Fatalf("limits %d/%d/%d, want 512/2048/8192", v.MaxKeyBytes, v.MaxValueBytes, v.MaxDataBytes)
	}
}

// TestParseFlagsLimitsUpperProfile — верхний профиль владельца через флаги.
func TestParseFlagsLimitsUpperProfile(t *testing.T) {
	origArgs := os.Args
	t.Cleanup(func() { os.Args = origArgs })
	os.Args = []string{"raft", "-number", "1", "-max-key-bytes", "8192", "-max-value-bytes", "16384",
		"-max-data-bytes", "41984"}

	v := ParseFlags()
	if v.MaxKeyBytes != 8192 || v.MaxValueBytes != 16384 || v.MaxDataBytes != 41984 {
		t.Fatalf("limits %d/%d/%d", v.MaxKeyBytes, v.MaxValueBytes, v.MaxDataBytes)
	}
}

// TestValidateLimitFlags — V29: limit−1/limit/limit+1 обоих полей, явный
// ноль и отрицательные значения отвергаются (явный ноль не означает
// отсутствия предела), D в пределах 1..F−120.
func TestValidateLimitFlags(t *testing.T) {
	accept := [][3]int{
		{1, 1, 2560}, {8191, 16383, 8192}, {8192, 16384, 41984}, {512, 2048, 262024},
	}
	for _, c := range accept {
		if err := ValidateLimitFlags(c[0], c[1], c[2]); err != nil {
			t.Fatalf("%v rejected: %v", c, err)
		}
	}
	reject := map[string][3]int{
		"key 0":            {0, 2048, 8192},
		"key -1":           {-1, 2048, 8192},
		"key 8193":         {8193, 2048, 8192},
		"value 0":          {512, 0, 8192},
		"value 16385":      {512, 16385, 8192},
		"data 0":           {512, 2048, 0},
		"data -5":          {512, 2048, -5},
		"data F-119":       {512, 2048, 262025},
		"data below C":     {512, 2048, 2559},
		"data overflowish": {512, 2048, 1 << 40},
	}
	for name, c := range reject {
		if err := ValidateLimitFlags(c[0], c[1], c[2]); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
}

// TestParseFlagsLimitsRejectNonInteger — нецелое и переполняющее значение
// флага отвергаются разбором флагов.
func TestParseFlagsLimitsRejectNonInteger(t *testing.T) {
	for _, arg := range []string{"1.5", "abc", "99999999999999999999"} {
		fs := flag.NewFlagSet("raft", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		addLimitFlags(fs)
		if err := fs.Parse([]string{"-max-key-bytes", arg}); err == nil {
			t.Fatalf("-max-key-bytes %s accepted", arg)
		}
	}
}

// TestDeriveLimits — V29/V30: N_eff = floor((F−88)/(32+D)); 8192 → 31 (профиль
// по умолчанию), 41984 → 6; D > F−120 отвергается; результат проходит
// Limits.Validate с равенством композиции у D = F−120.
func TestDeriveLimits(t *testing.T) {
	cases := map[int]uint64{8192: 31, 41984: 6, 4096: 63, 5424: 48, 262024: 1}
	for d, n := range cases {
		limits, err := DeriveLimits(d)
		if err != nil {
			t.Fatalf("D=%d: %v", d, err)
		}
		if limits.MaxEntries != n || limits.MaxDataBytes != uint64(d) ||
			limits.MaxFrameBytes != 262144 || limits.MaxConfigurationBytes != 2560 {
			t.Fatalf("D=%d: %+v, want N=%d", d, limits, n)
		}
	}
	if limits, _ := DeriveLimits(8192); limits != protocol.DefaultLimits() {
		t.Fatalf("D=8192 profile %+v differs from default", limits)
	}
	if _, err := DeriveLimits(262025); err == nil || !strings.Contains(err.Error(), "262024") {
		t.Fatalf("D=F-119: %v", err)
	}
}

// TestUpperProfileReachable — V30: верхний профиль владельца достижим:
// EncodedMaxKV(8192,16384) = 41776 <= D = 41984 при N_eff = 6; при D = 41775
// профиль отвергается с диагностикой; значения по умолчанию CLI совпадают
// с библиотечными (EncodedMaxKV(512,2048) = 5424 <= 8192).
func TestUpperProfileReachable(t *testing.T) {
	check := func(k, v, d int) error {
		limits, err := DeriveLimits(d)
		if err != nil {
			return err
		}
		cfg := &kvservice.Config{MaxKeyBytes: k, MaxValueBytes: v}
		cfg.Limits = limits
		timing := contract.TransportTiming{BaseSend: contract.TCPRPCTimeout, BaseRecv: contract.TCPRPCTimeout,
			Dial: contract.ConnectionTCPRPCTimeout}
		return kvservice.ValidateConfig(cfg, timing)
	}
	if err := check(MaxKeyBytesLimit, MaxValueBytesLimit, 41984); err != nil {
		t.Fatalf("upper profile: %v", err)
	}
	if err := check(MaxKeyBytesLimit, MaxValueBytesLimit, 41775); err == nil ||
		!strings.Contains(err.Error(), "EncodedMaxKV(8192,16384)=41776 exceeds max-data-bytes=41775") {
		t.Fatalf("upper profile at D=41775: %v", err)
	}
	if err := check(DefaultMaxKeyBytes, DefaultMaxValueBytes, 8192); err != nil {
		t.Fatalf("defaults: %v", err)
	}
	if err := check(0, 0, 5424); err != nil {
		t.Fatalf("library zero at bound: %v", err)
	}
	if err := check(0, 0, 5423); err == nil {
		t.Fatal("library zero below bound accepted")
	}
}
