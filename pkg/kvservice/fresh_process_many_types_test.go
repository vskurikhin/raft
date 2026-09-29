package kvservice

import (
	"encoding/gob"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"

	"github.com/vskurikhin/raft/pkg/raft/contract"
	"github.com/vskurikhin/raft/pkg/raft/protocol"
	"github.com/vskurikhin/raft/pkg/raft/transp"
)

// _freshModeEnv переводит тестовый бинарник в режим вспомогательного
// процесса; значение задаёт регистрационную карту: clean или many.
const _freshModeEnv = "KVSERVICE_FRESH_MODE"

// freshCustom — пользовательский тип Data для проверки измеримости.
type freshCustom struct {
	A int
	B string
}

// registerManyUserTypes регистрирует count различных пользовательских типов
// под собственными именами. Глобальный реестр gob заполняется заранее, как в
// долгоживущем процессе-потребителе с историей типов.
func registerManyUserTypes(count int) {
	for i := range count {
		typ := reflect.StructOf([]reflect.StructField{{Name: fmt.Sprintf("F%d", i), Type: reflect.TypeFor[int]()}})
		gob.RegisterName(fmt.Sprintf("fresh.T%d", i), reflect.New(typ).Elem().Interface())
	}
}

// TestFreshProcessRegistrationMatrix — V27/V30: два отдельных процесса — с
// чистой регистрационной картой и с более чем 64 заранее зарегистрированными
// типами. Каждый процесс печатает измеримость Data (MeasureData ==
// len(EncodeData)) для Command/string/[]byte/custom/nil и исход проверки
// профиля на границе bound−1/bound/bound+1 и верхнего профиля; код возврата
// отличает успех от расхождения ожиданию. Родитель проверяет оба исхода.
// Сброс глобальной карты в одном процессе не используется.
func TestFreshProcessRegistrationMatrix(t *testing.T) {
	if mode := os.Getenv(_freshModeEnv); mode != "" {
		runFreshMatrix(t, mode)
		return
	}
	for _, mode := range []string{"clean", "many"} {
		cmd := exec.Command(os.Args[0], "-test.run=^TestFreshProcessRegistrationMatrix$", "-test.count=1")
		cmd.Env = append(os.Environ(), _freshModeEnv+"="+mode)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("режим %s: %v\n%s", mode, err, out)
		}
		text := string(out)
		t.Logf("режим %s, вывод дочернего процесса:\n%s", mode, text)
		for _, want := range []string{
			"FRESH-" + mode + "-OK",
			"measurable command=", "measurable string=", "measurable bytes[]=", "measurable custom=", "measurable nil=",
			"D=bound-1 REFUSED", "D=bound ACCEPTED", "D=bound+1 ACCEPTED", "upper-profile ACCEPTED",
		} {
			if !strings.Contains(text, want) {
				t.Fatalf("режим %s: нет %q\n%s", mode, want, text)
			}
		}
	}
}

// runFreshMatrix выполняет проверки в процессе с выбранной регистрационной
// картой. Все имена типов существуют только в этом процессе.
func runFreshMatrix(t *testing.T, mode string) {
	gob.Register(Command{})
	gob.Register(freshCustom{})
	if mode == "many" {
		registerManyUserTypes(80)
	}

	bound, err := EncodedMaxKV(512, 2048)
	if err != nil {
		t.Fatalf("EncodedMaxKV: %v", err)
	}
	values := []struct {
		name  string
		value any
	}{
		{"command", Command{Kind: CommandCAS, Key: strings.Repeat("k", 512),
			Value: strings.Repeat("v", 2048), CompareValue: strings.Repeat("c", 2048), ID: -1 << 62}},
		{"string", strings.Repeat("s", 1024)},
		{"bytes[]", []byte(strings.Repeat("b", 1024))},
		{"custom", freshCustom{A: 7, B: strings.Repeat("x", 128)}},
		{"nil", nil},
	}
	for _, v := range values {
		size, err := protocol.MeasureData(v.value, bound)
		if err != nil {
			t.Fatalf("MeasureData(%s): %v", v.name, err)
		}
		encoded, err := protocol.EncodeData(v.value, bound)
		if err != nil {
			t.Fatalf("EncodeData(%s): %v", v.name, err)
		}
		if size != uint64(len(encoded)) {
			t.Fatalf("MeasureData(%s)=%d != len(EncodeData)=%d", v.name, size, len(encoded))
		}
		fmt.Printf("measurable %s=%d\n", v.name, size)
	}

	_, timing := transp.NormalizeTCPTimeouts(transp.TCPTimeouts{})
	for _, c := range []struct {
		label  string
		d      uint64
		wantOK bool
	}{
		{"bound-1", bound - 1, false},
		{"bound", bound, true},
		{"bound+1", bound + 1, true},
	} {
		limits := contract.Limits{MaxFrameBytes: 262144, MaxEntries: 262056 / (32 + c.d),
			MaxDataBytes: c.d, MaxConfigurationBytes: 2560}
		cfg := &Config{}
		cfg.Limits = limits
		err := ValidateConfig(cfg, timing)
		if c.wantOK && err != nil {
			t.Fatalf("D=%s (%d): %v", c.label, c.d, err)
		}
		if !c.wantOK {
			if err == nil || !strings.Contains(err.Error(), "exceeds max-data-bytes") {
				t.Fatalf("D=%s (%d): err=%v", c.label, c.d, err)
			}
			fmt.Printf("D=%s REFUSED\n", c.label)
			continue
		}
		fmt.Printf("D=%s ACCEPTED\n", c.label)
	}

	upper := contract.Limits{MaxFrameBytes: 262144, MaxEntries: 6, MaxDataBytes: 41984, MaxConfigurationBytes: 2560}
	upperCfg := &Config{MaxKeyBytes: 8192, MaxValueBytes: 16384}
	upperCfg.Limits = upper
	if err = ValidateConfig(upperCfg, timing); err != nil {
		t.Fatalf("верхний профиль владельца: %v", err)
	}
	fmt.Println("upper-profile ACCEPTED")
	fmt.Printf("FRESH-%s-OK\n", mode)
}
