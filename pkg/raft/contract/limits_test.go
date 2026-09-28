package contract

import (
	"math"
	"strings"
	"testing"
)

// TestLimitsValidateAccepts — допустимые профили: условный профиль по
// умолчанию, верхний K/V-профиль, наименьший допустимый F и равенства
// каждого отношения.
func TestLimitsValidateAccepts(t *testing.T) {
	valid := map[string]Limits{
		"default":                  {MaxFrameBytes: 262144, MaxEntries: 31, MaxDataBytes: 8192, MaxConfigurationBytes: 2560},
		"upper K/V profile":        {MaxFrameBytes: 262144, MaxEntries: 6, MaxDataBytes: 41984, MaxConfigurationBytes: 2560},
		"B64 microbench":           {MaxFrameBytes: 262144, MaxEntries: 64, MaxDataBytes: 2048, MaxConfigurationBytes: 1024},
		"minimal":                  {MaxFrameBytes: 4128, MaxEntries: 1, MaxDataBytes: 1, MaxConfigurationBytes: 1},
		"D = F-120":                {MaxFrameBytes: 262144, MaxEntries: 1, MaxDataBytes: 262024, MaxConfigurationBytes: 2560},
		"C = D":                    {MaxFrameBytes: 262144, MaxEntries: 1, MaxDataBytes: 2560, MaxConfigurationBytes: 2560},
		"C = F-96 via D":           {MaxFrameBytes: 262144, MaxEntries: 1, MaxDataBytes: 262024, MaxConfigurationBytes: 262024},
		"N = (F-88)/(32+D)":        {MaxFrameBytes: 4128, MaxEntries: 122, MaxDataBytes: 1, MaxConfigurationBytes: 1},
		"N x (32+D) = F-88":        {MaxFrameBytes: 262144, MaxEntries: 1, MaxDataBytes: 262024, MaxConfigurationBytes: 1},
		"N_eff 31 at D=8192":       {MaxFrameBytes: 262144, MaxEntries: 31, MaxDataBytes: 8192, MaxConfigurationBytes: 1},
		"F = MaxInt":               {MaxFrameBytes: math.MaxInt, MaxEntries: 1, MaxDataBytes: 1, MaxConfigurationBytes: 1},
		"F = 262168 (step bound)":  {MaxFrameBytes: 262168, MaxEntries: 31, MaxDataBytes: 8192, MaxConfigurationBytes: 2560},
		"F = 262169 (next factor)": {MaxFrameBytes: 262169, MaxEntries: 31, MaxDataBytes: 8192, MaxConfigurationBytes: 2560},
	}
	for name, limits := range valid {
		if err := limits.Validate(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

// TestLimitsValidateRejects — нулевые, непредставимые и несогласованные
// профили отвергаются с контекстом; Validate не нормализует нули.
func TestLimitsValidateRejects(t *testing.T) {
	invalid := []struct {
		name    string
		limits  Limits
		message string
	}{
		{"zero", Limits{}, "MaxFrameBytes must be positive"},
		{"zero N", Limits{MaxFrameBytes: 262144, MaxDataBytes: 8192, MaxConfigurationBytes: 2560}, "MaxEntries must be positive"},
		{"zero D", Limits{MaxFrameBytes: 262144, MaxEntries: 31, MaxConfigurationBytes: 2560}, "MaxDataBytes must be positive"},
		{"zero C", Limits{MaxFrameBytes: 262144, MaxEntries: 31, MaxDataBytes: 8192}, "MaxConfigurationBytes must be positive"},
		{"F = MaxInt+1", Limits{MaxFrameBytes: math.MaxInt + 1, MaxEntries: 1, MaxDataBytes: 1, MaxConfigurationBytes: 1},
			"exceeds platform MaxInt"},
		{"F = MaxUint64", Limits{MaxFrameBytes: math.MaxUint64, MaxEntries: 1, MaxDataBytes: 1, MaxConfigurationBytes: 1},
			"exceeds platform MaxInt"},
		{"N = MaxUint64", Limits{MaxFrameBytes: 262144, MaxEntries: math.MaxUint64, MaxDataBytes: 1, MaxConfigurationBytes: 1},
			"exceeds platform MaxInt"},
		{"F below error frame", Limits{MaxFrameBytes: 4127, MaxEntries: 1, MaxDataBytes: 1, MaxConfigurationBytes: 1},
			"largest error frame"},
		{"D = F-119", Limits{MaxFrameBytes: 262144, MaxEntries: 1, MaxDataBytes: 262025, MaxConfigurationBytes: 1},
			"MaxDataBytes 262025 exceeds MaxFrameBytes-120"},
		{"C > D", Limits{MaxFrameBytes: 262144, MaxEntries: 31, MaxDataBytes: 2048, MaxConfigurationBytes: 2049},
			"MaxConfigurationBytes 2049 exceeds MaxDataBytes 2048"},
		{"C = F-95", Limits{MaxFrameBytes: 262144, MaxEntries: 1, MaxDataBytes: 262024, MaxConfigurationBytes: 262049},
			"exceeds MaxFrameBytes-96"},
		{"N = (F-88)/32+1", Limits{MaxFrameBytes: 4128, MaxEntries: 127, MaxDataBytes: 1, MaxConfigurationBytes: 1},
			"MaxEntries 127 exceeds (MaxFrameBytes-88)/32"},
		{"N x (32+D) > F-88", Limits{MaxFrameBytes: 262144, MaxEntries: 32, MaxDataBytes: 8192, MaxConfigurationBytes: 2560},
			"MaxEntries 32 x (32+MaxDataBytes 8192) exceeds MaxFrameBytes-88 = 262056"},
		{"N_eff 7 at upper D", Limits{MaxFrameBytes: 262144, MaxEntries: 7, MaxDataBytes: 41984, MaxConfigurationBytes: 2560},
			"MaxEntries 7"},
	}
	for _, tc := range invalid {
		err := tc.limits.Validate()
		if err == nil {
			t.Fatalf("%s: accepted", tc.name)
		}
		if !strings.Contains(err.Error(), tc.message) || !strings.Contains(err.Error(), "invalid limits") {
			t.Fatalf("%s: error %q does not contain %q", tc.name, err, tc.message)
		}
	}
}
