package kvclient

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vskurikhin/raft/pkg/api"
)

// countingServer считает запросы; второй адрес кластера — счётчик ротации.
func countingServer(handler func(w http.ResponseWriter)) (*httptest.Server, *atomic.Int64) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		handler(w)
	}))
	return srv, &calls
}

// TestTooLargeIsTerminal — отказ по пределам терминален для всех путей
// клиента (PUT, CAS, ConsensusGet, Delete, WeakGet через sendGet →
// sendJSONGetRequest): HTTP 413 с JSON, HTTP 413 с текстом, HTTP 200 со
// статусом TooLarge — ровно одна попытка, без перехода к следующему адресу,
// без повтора и паники; неизвестный статус — терминальная ошибка протокола.
func TestTooLargeIsTerminal(t *testing.T) {
	responses := map[string]func(w http.ResponseWriter){
		"413 json": func(w http.ResponseWriter) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			_ = json.NewEncoder(w).Encode(api.StatusResponse{RespStatus: api.StatusTooLarge})
		},
		"413 text": func(w http.ResponseWriter) {
			http.Error(w, "http: request body too large", http.StatusRequestEntityTooLarge)
		},
		"200 TooLarge": func(w http.ResponseWriter) {
			_ = json.NewEncoder(w).Encode(api.StatusResponse{RespStatus: api.StatusTooLarge})
		},
		"200 unknown status": func(w http.ResponseWriter) {
			_ = json.NewEncoder(w).Encode(api.StatusResponse{RespStatus: api.ResponseStatus(99)})
		},
	}
	for name, respond := range responses {
		t.Run(name, func(t *testing.T) {
			srv, calls := countingServer(respond)
			defer srv.Close()
			other, otherCalls := countingServer(func(w http.ResponseWriter) {
				_ = json.NewEncoder(w).Encode(api.StatusResponse{RespStatus: api.StatusOK})
			})
			defer other.Close()
			c := New([]string{serverAddr(t, srv), serverAddr(t, other)})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			ops := map[string]func() error{
				"Put": func() error { _, _, err := c.Put(ctx, "k", "v"); return err },
				"CAS": func() error { _, _, err := c.CAS(ctx, "k", "c", "v"); return err },
				"ConsensusGet": func() error {
					_, _, err := c.ConsensusGet(ctx, "k")
					return err
				},
				"Delete":  func() error { _, _, err := c.Delete(ctx, "k"); return err },
				"WeakGet": func() error { _, _, err := c.WeakGet(ctx, "k"); return err },
			}
			for op, call := range ops {
				before := calls.Load()
				err := call()
				if name == "200 unknown status" {
					if err == nil || errors.Is(err, ErrRequestTooLarge) || !strings.Contains(err.Error(), "unknown response status") {
						t.Fatalf("%s: err = %v, want protocol error", op, err)
					}
				} else if !errors.Is(err, ErrRequestTooLarge) {
					t.Fatalf("%s: err = %v, want ErrRequestTooLarge", op, err)
				}
				if calls.Load()-before != 1 {
					t.Fatalf("%s: %d attempts, want 1", op, calls.Load()-before)
				}
				if c.leader() != 0 || otherCalls.Load() != 0 {
					t.Fatalf("%s: rotated to next address", op)
				}
			}
		})
	}
}
