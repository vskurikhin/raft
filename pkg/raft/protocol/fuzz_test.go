package protocol

import (
	"bytes"
	"testing"
)

// fuzzMaxInputBytes — технический предел входного вектора фаззинга,
// совпадающий с бюджетом тестовой кампании F ≤ 64 КиБ. Большие векторы не
// разбираются: это защита от неограниченной работы на неполном вводе, а не
// производственный предел.
const fuzzMaxInputBytes = 64 << 10

// fuzzMaxFrames — предел числа кадров, разбираемых из одного вектора
// последовательности. Ограничивает цикл, даже если разбор не двигает Reader.
const fuzzMaxFrames = 64

// FuzzReadRequest — разбор произвольного потока как одного запроса:
// отсутствие паники, конечность и обязательный ненулевой результат при
// успехе. Успешный разбор не может вернуть nil-команду.
func FuzzReadRequest(f *testing.F) {
	for _, tc := range goldenRequests() {
		f.Add(loadGolden(f, tc.name))
	}
	f.Add([]byte{})
	f.Add([]byte("RRPC"))
	f.Add([]byte("RRPC\x00"))
	f.Add([]byte{0xff, 0xff, 0xff, 0xff})
	f.Add(append(loadGolden(f, "ae_request_gob_string")[:10:10], 0x00))

	f.Fuzz(func(t *testing.T, frame []byte) {
		if len(frame) > fuzzMaxInputBytes {
			t.Skip()
		}
		typ, command, err := ReadRequest(bytes.NewReader(frame), smallLimits)
		if err != nil {
			return
		}
		if command == nil {
			t.Fatalf("успешный разбор без команды: тип %d", typ)
		}
	})
}

// FuzzReadResponse — разбор произвольного потока как одного ответа:
// ожидаемый тип нормализуется в допустимый набор 0…4. При успехе ответ
// обязан содержать reply или ошибку.
func FuzzReadResponse(f *testing.F) {
	for _, tc := range goldenResponses() {
		f.Add(uint8(tc.typ), loadGolden(f, tc.name))
	}
	f.Add(uint8(rpcAppendEntries), []byte{})
	f.Add(uint8(rpcRequestVote), []byte("RRPC"))
	f.Add(uint8(0xff), []byte{0xff, 0xff, 0xff, 0xff})

	f.Fuzz(func(t *testing.T, expected uint8, frame []byte) {
		if len(frame) > fuzzMaxInputBytes {
			t.Skip()
		}
		// Допустимый ожидаемый тип — один из пяти; остальные значения не
		// проверяют разбор ответа, а лишь нормализуются.
		typ := RPCType(expected % 5)
		resp, err := ReadResponse(bytes.NewReader(frame), typ, smallLimits)
		if err != nil {
			return
		}
		if resp.Reply == nil && resp.Error == nil {
			t.Fatalf("успешный ответ без reply и ошибки: тип %d", typ)
		}
	})
}

// FuzzFrameSequence — разбор произвольного потока как последовательности
// кадров: цикл конечен, каждый успешный кадр продвигает Reader, ошибка
// завершает последовательность.
func FuzzFrameSequence(f *testing.F) {
	var requests []byte
	for _, tc := range goldenRequests() {
		requests = append(requests, loadGolden(f, tc.name)...)
	}
	var responses []byte
	for _, tc := range goldenResponses() {
		responses = append(responses, loadGolden(f, tc.name)...)
	}
	f.Add(requests)
	f.Add(responses)
	f.Add(append(append([]byte{}, requests[:len(requests)/2]...), responses...))
	f.Add([]byte{})
	f.Add([]byte("RRPC"))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > fuzzMaxInputBytes {
			t.Skip()
		}
		reader := bytes.NewReader(data)
		for range fuzzMaxFrames {
			before := reader.Len()
			if before == 0 {
				return
			}
			if _, _, err := ReadRequest(reader, smallLimits); err != nil {
				return
			}
			if reader.Len() == before {
				t.Fatalf("успешный кадр не продвинул Reader: осталось %d", before)
			}
		}
	})
}
