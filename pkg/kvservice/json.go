package kvservice

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
)

// errTrailingJSON — после единственного JSON-объекта тела запроса есть
// ещё данные.
var errTrailingJSON = errors.New("unexpected data after JSON object")

// readRequestJSON ожидает, что req содержит тело с типом содержимого
// application/json и ровно одно JSON-представление значения, соответствующего
// типу, на который указывает target. Тело читается через http.MaxBytesReader
// с пределом maxBodyBytes до декодирования; после объекта допустимы только
// пробельные символы до конца тела. Заполняет target или возвращает ошибку;
// превышение предела тела распознаётся isBodyTooLarge.
func readRequestJSON(w http.ResponseWriter, req *http.Request, target any, maxBodyBytes int64) error {
	contentType := req.Header.Get("Content-Type")
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return err
	}
	if mediaType != "application/json" {
		return fmt.Errorf("expect application/json Content-Type, got %s", mediaType)
	}

	dec := json.NewDecoder(http.MaxBytesReader(w, req.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err = dec.Decode(target); err != nil {
		return err
	}
	if _, err = dec.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errTrailingJSON
		}
		return err
	}
	return nil
}

// isBodyTooLarge сообщает, что тело запроса превысило предел MaxBytesReader.
func isBodyTooLarge(err error) bool {
	var maxBytesErr *http.MaxBytesError
	return errors.As(err, &maxBytesErr)
}

// renderJSON сериализует значение v в формат JSON и записывает его
// в HTTP-ответ w.
func renderJSON(w http.ResponseWriter, v any) {
	renderJSONStatus(w, http.StatusOK, v)
}

// renderJSONStatus сериализует значение v в формат JSON и записывает его
// в HTTP-ответ w с кодом состояния status.
func renderJSONStatus(w http.ResponseWriter, status int, v any) {
	js, err := json.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if status != http.StatusOK {
		w.WriteHeader(status)
	}
	_, _ = w.Write(js)
}
