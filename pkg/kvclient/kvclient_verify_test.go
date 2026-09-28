package kvclient

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/vskurikhin/raft/pkg/api"
)

// TestVerifyLeaderTerminalStatus — VerifyLeader: HTTP413 (JSON и текст),
// HTTP200+TooLarge — ErrRequestTooLarge; неизвестный статус — ошибка
// протокола; ровно одна попытка, предполагаемый лидер не меняется.
func TestVerifyLeaderTerminalStatus(t *testing.T) {
	responses := map[string]func(w http.ResponseWriter){
		"413 json": func(w http.ResponseWriter) {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			_ = json.NewEncoder(w).Encode(api.StatusResponse{RespStatus: api.StatusTooLarge})
		},
		"413 text": func(w http.ResponseWriter) {
			http.Error(w, "too large", http.StatusRequestEntityTooLarge)
		},
		"200 TooLarge": func(w http.ResponseWriter) {
			_ = json.NewEncoder(w).Encode(api.StatusResponse{RespStatus: api.StatusTooLarge})
		},
		"200 unknown": func(w http.ResponseWriter) {
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
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := c.VerifyLeader(ctx)
			if name == "200 unknown" {
				if err == nil || errors.Is(err, ErrRequestTooLarge) || !strings.Contains(err.Error(), "unknown response status") {
					t.Fatalf("err = %v, want protocol error", err)
				}
			} else if !errors.Is(err, ErrRequestTooLarge) {
				t.Fatalf("err = %v, want ErrRequestTooLarge", err)
			}
			if calls.Load() != 1 || otherCalls.Load() != 0 || c.leader() != 0 {
				t.Fatalf("attempts %d, other %d, leader %d", calls.Load(), otherCalls.Load(), c.leader())
			}
		})
	}
	// NotLeader по-прежнему переводит на следующий адрес.
	nl, _ := countingServer(func(w http.ResponseWriter) {
		_ = json.NewEncoder(w).Encode(api.StatusResponse{RespStatus: api.StatusNotLeader})
	})
	defer nl.Close()
	ok, _ := countingServer(func(w http.ResponseWriter) {
		_ = json.NewEncoder(w).Encode(api.StatusResponse{RespStatus: api.StatusOK})
	})
	defer ok.Close()
	c := New([]string{serverAddr(t, nl), serverAddr(t, ok)})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if leader, err := c.VerifyLeader(ctx); err != nil || leader != 1 {
		t.Fatalf("NotLeader rotation: %d %v", leader, err)
	}
}
