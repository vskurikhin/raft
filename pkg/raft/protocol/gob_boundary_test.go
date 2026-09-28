package protocol

import (
	"bytes"
	"encoding/gob"
	"reflect"
	"runtime"
	"testing"

	"github.com/vskurikhin/raft/pkg/raft/contract"
)

// gobBoundaryGoVersion — версия Go, к которой привязаны ожидания принятия
// внутренне неканоничных gob-потоков Data. На другой версии корпус требует
// переаттестации, а не молчаливого пропуска или смены ожиданий.
const gobBoundaryGoVersion = "go1.26.4"

// Замороженные независимые gob-потоки struct{ Data any }. Байты не построены
// кодеком: корректные потоки получены напрямую стандартным encoding/gob, а
// повреждённые — преобразованиями независимого диагностического примера на
// go1.26.4. Длины и SHA-256 (шестнадцатеричные):
//
//	gobFrozenString        39 Б  1854f95afeea7e80c44303d530b681aea5b9e67f0e8d7709f56a6ade86e3dad0
//	gobFrozenInnerTail     40 Б  71bc10138139c6792242f953efc12af77e67b41ff0e73610e126320a00b832a1
//	gobFrozenInterface     39 Б  dff31162e4b3295fd108ff552771e89783447608b34ff61c0b5d9ff5e5866556
//	gobFrozenWrapperNoEnd  38 Б  e055345a43912863ddd2210b97e8872df17a34589504810a0d2873d51429699e
//	gobFrozenPoint         77 Б  5b7caadef1f3d150ee08ea4fef1c64d53fabd60bbece8c7e54f5937f25123ddc
//	gobFrozenNestedNoEnd   75 Б  5f6bf785b751fe6c9f53007ad3be53003c7fe9f367cefc0a921196450b1089a9
//
// Первые три повреждённых потока имеют корректную пару gobFrozenString,
// четвёртый — gobFrozenPoint.
const (
	// gobFrozenString — корректная пара A/B/C, Data = "hi".
	gobFrozenString = "147f030102ff80000101010444617461011000000011ff800106737472696e670c040002686900"
	// gobFrozenInnerTail — A: лишний байт 0x42 внутри последнего сообщения,
	// счётчик сообщения 17→18 при согласованных длинах записи и кадра.
	gobFrozenInnerTail = "147f030102ff80000101010444617461011000000012ff800106737472696e670c04000268690042"
	// gobFrozenInterface — B: счётчик delimited-значения interface 4→5.
	gobFrozenInterface = "147f030102ff80000101010444617461011000000011ff800106737472696e670c050002686900"
	// gobFrozenWrapperNoEnd — C: удалён терминатор обёртки, счётчик 17→16.
	gobFrozenWrapperNoEnd = "147f030102ff80000101010444617461011000000010ff800106737472696e670c0400026869"
	// gobFrozenPoint — корректная пара D, Data = point{1,2}.
	gobFrozenPoint = "147f030102ff8000010101044461746101100000002dff80010a6d61696e2e706f696e74ff8103010105706f696e7401ff82000102010158010400010159010400000009ff8205010201040000"
	// gobFrozenNestedNoEnd — D: удалены два терминатора вложенной структуры,
	// счётчик 9→7.
	gobFrozenNestedNoEnd = "147f030102ff8000010101044461746101100000002dff80010a6d61696e2e706f696e74ff8103010105706f696e7401ff82000102010158010400010159010400000007ff820501020104"
)

// gobFrozenPointName — имя конкретного типа во замороженных потоках
// gobFrozenPoint/gobFrozenNestedNoEnd; под ним регистрируется boundaryPoint,
// чтобы независимый поток декодировался в конкретный тип.
const gobFrozenPointName = "main.point"

// boundaryPoint — конкретный зарегистрированный тип Data для потоков
// gobFrozenPoint/gobFrozenNestedNoEnd. Регистрация выполняется под именем из
// замороженного потока, поэтому поля совпадают с ним по имени и типу.
type boundaryPoint struct{ X, Y int }

// registerGobBoundaryType регистрирует конкретный тип вектора D. Повторный
// вызов безопасен: gob допускает повторную регистрацию того же имени и типа.
func registerGobBoundaryType() { gob.RegisterName(gobFrozenPointName, boundaryPoint{}) }

// gobBoundaryReader собирает кадр AppendEntries с одной gob-записью
// замороженного потока Data и добавляет следом корректный кадр TimeoutNow в
// тот же reader. Замороженный поток не кодируется. Возвращает reader и байты
// следующего кадра.
func gobBoundaryReader(t *testing.T, payload []byte) (*bytes.Reader, []byte) {
	t.Helper()

	frame := singleEntryFrame(payload, contract.LogCommand)
	next := loadGolden(t, "tn_request")
	reader := bytes.NewReader(append(append([]byte(nil), frame...), next...))

	return reader, next
}

// readFirstBoundaryData читает первый кадр и возвращает его Data, проверяя
// тип RPC, форму команды и сохранность следующего кадра в reader.
func readFirstBoundaryData(t *testing.T, reader *bytes.Reader, next []byte) any {
	t.Helper()

	typ, command, err := ReadRequest(reader, DefaultLimits())
	if err != nil {
		t.Fatalf("ReadRequest: %v", err)
	}
	if typ != rpcAppendEntries {
		t.Fatalf("RPCType = %d, want %d", typ, rpcAppendEntries)
	}
	args, ok := command.(*contract.AppendEntriesArgs)
	if !ok || len(args.Entries) != 1 {
		t.Fatalf("command = %#v", command)
	}
	if reader.Len() != len(next) {
		t.Fatalf("remaining = %d, want %d: следующий кадр должен остаться целиком", reader.Len(), len(next))
	}

	return args.Entries[0].Data
}

// readNextTimeoutNow читает ровно следующий кадр и проверяет его тип и
// содержимое, а также отсутствие остатка.
func readNextTimeoutNow(t *testing.T, reader *bytes.Reader) {
	t.Helper()

	typ, command, err := ReadRequest(reader, DefaultLimits())
	if err != nil {
		t.Fatalf("second ReadRequest: %v", err)
	}
	if typ != rpcTimeoutNow {
		t.Fatalf("second RPCType = %d, want %d", typ, rpcTimeoutNow)
	}
	want := &contract.TimeoutNowRequest{RPCHeader: header3(1)}
	if !reflect.DeepEqual(command, want) {
		t.Fatalf("second command = %#v, want %#v", command, want)
	}
	if reader.Len() != 0 {
		t.Fatalf("reader has %d bytes after next frame", reader.Len())
	}
}

// TestGobAcceptedBoundaryGo1264 — приёмка A/B/C/D и их корректных пар:
// независимые замороженные потоки принимаются стандартным gob, дают ожидаемое
// значение и конкретный тип, а следующий кадр остаётся не прочитанным до
// второго ReadRequest.
//
// Ожидания привязаны к go1.26.4. «Ненулевой interface» в критерии означает
// interface != nil, а не запрет typed nil: поддерживаемый gob typed nil
// остаётся видом gob, неподдерживаемый указатель возвращает прежнюю ошибку
// (TestDataTypedNil).
func TestGobAcceptedBoundaryGo1264(t *testing.T) {
	if version := runtime.Version(); version != gobBoundaryGoVersion {
		t.Fatalf("версия Go %s, ожидалась %s: требуется переаттестация gob-корпуса ARCHITECT",
			version, gobBoundaryGoVersion)
	}
	registerGobBoundaryType()

	cases := []struct {
		name string
		hex  string
		want any
	}{
		{"correct string", gobFrozenString, "hi"},
		{"inner message tail", gobFrozenInnerTail, "hi"},
		{"interface count", gobFrozenInterface, "hi"},
		{"wrapper without terminator", gobFrozenWrapperNoEnd, "hi"},
		{"correct point", gobFrozenPoint, boundaryPoint{X: 1, Y: 2}},
		{"nested without terminators", gobFrozenNestedNoEnd, boundaryPoint{X: 1, Y: 2}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reader, next := gobBoundaryReader(t, mustHex(t, tc.hex))
			got := readFirstBoundaryData(t, reader, next)
			if reflect.TypeOf(got) != reflect.TypeOf(tc.want) || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("Data = %#v (%T), want %#v (%T)", got, got, tc.want, tc.want)
			}
			readNextTimeoutNow(t, reader)
		})
	}
}

// TestGobBoundaryRejections — независимые отрицательные контроли двух разных
// внешних границ; версия Go их не ограничивает.
//
//   - внешнее тело: лишний байт после всех объявленных полей внутри
//     увеличенного BodyLength при прежнем DataLength отвергается проверкой
//     точного исчерпания тела (ErrFormat, command = nil);
//   - внешний остаток Data: байт после последнего gob-сообщения либо
//     отдельное второе gob-значение внутри DataLength при согласованных
//     внешних длинах отвергается проверкой остатка bytes.Reader (ErrData,
//     command = nil).
//
// В обоих случаях следующий корректный кадр в том же reader остаётся целым и
// читается вторым ReadRequest.
func TestGobBoundaryRejections(t *testing.T) {
	t.Run("trailing byte in body", func(t *testing.T) {
		frame := singleEntryFrame(mustHex(t, gobFrozenString), contract.LogCommand)
		corrupted := withBodyLength(append(bytes.Clone(frame), 0x42), uint64(len(frame)-headerSize+1))
		next := loadGolden(t, "tn_request")
		reader := bytes.NewReader(append(corrupted, next...))

		_, command, err := ReadRequest(reader, DefaultLimits())
		requireIs(t, err, ErrFormat)
		if command != nil {
			t.Fatalf("command = %v, want nil", command)
		}
		if reader.Len() != len(next) {
			t.Fatalf("remaining = %d, want %d: следующий кадр должен остаться целиком", reader.Len(), len(next))
		}
		readNextTimeoutNow(t, reader)
	})

	cases := []struct {
		name    string
		payload []byte
	}{
		{"trailing byte in Data", append(append([]byte(nil), mustHex(t, gobFrozenString)...), 0x42)},
		{"second gob value in Data", append(mustHex(t, gobFrozenString), mustHex(t, gobFrozenString)...)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reader, next := gobBoundaryReader(t, tc.payload)

			_, command, err := ReadRequest(reader, DefaultLimits())
			requireIs(t, err, ErrData)
			if command != nil {
				t.Fatalf("command = %v, want nil", command)
			}
			if reader.Len() != len(next) {
				t.Fatalf("remaining = %d, want %d: следующий кадр должен остаться целиком", reader.Len(), len(next))
			}
			readNextTimeoutNow(t, reader)
		})
	}
}
