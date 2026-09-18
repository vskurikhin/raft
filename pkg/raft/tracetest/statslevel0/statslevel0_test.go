// Package statslevel0_test — матрица периодической статистики на уровне
// трассировки 0: один уровень задаётся один раз на процесс до создания
// первого CM. Обе настройки вывода (stats-output true/false) проверяются
// последовательно в одном процессе без изменения уровня.
package statslevel0_test

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/vskurikhin/raft"
	"github.com/vskurikhin/raft/pkg/raft/store"
	"github.com/vskurikhin/raft/pkg/raft/transp"
)

// traceLevel — порог событийной трассировки процесса. Один уровень
// на пакет-процесс: SetTrace строго set-once и вызывается до создания CM.
const traceLevel = 0

// statsWaitBudget — предельное ожидание трёх строк отчёта: секундный тик
// плюс запас на планировщик. Ожидание условия, а не доказательство гонки.
const statsWaitBudget = 5 * time.Second

// statsDisabledWindow — окно наблюдения выключенного вывода: не менее двух
// секундных тактов, чтобы тикер гарантированно сработал. Для отрицательной
// проверки окно необходимо: выключенный вывод не оставляет наблюдаемых
// следов, при этом тик сбора обязан состояться.
const statsDisabledWindow = 2 * time.Second

// TestMain задаёт единственный порог трассировки процесса до создания CM.
func TestMain(m *testing.M) {
	if err := raft.SetTrace(raft.TraceConfig{Level: traceLevel}); err != nil {
		_, _ = os.Stderr.WriteString("raft: trace configuration failed: " + err.Error() + "\n")
		os.Exit(1)
	}
	os.Exit(m.Run())
}

// captureStdout подменяет стандартный поток вывода временным файлом на время
// теста: публикация читает os.Stdout при каждом выпуске, поэтому подмена
// видна горутине stats. Восстановление потока выполняется до проверки утечек.
func captureStdout(t *testing.T) string {
	t.Helper()
	f, err := os.CreateTemp("", "raft-statslevel0-*.log")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	original := os.Stdout
	os.Stdout = f
	t.Cleanup(func() {
		os.Stdout = original
		_ = f.Close()
		_ = os.Remove(path)
	})
	return path
}

// newStatsServer поднимает одиночный узел через публичный Server;
// disabled передаётся в raft.Config.DisableStatsOutput.
func newStatsServer(t *testing.T, disabled bool) *raft.Server {
	t.Helper()
	commitCh := make(chan raft.CommitEntry)
	readerDone := make(chan any)
	go func() {
		defer close(readerDone)
		for range commitCh {
		}
	}()

	transport, err := transp.NewTCPTransport("127.0.0.1:0", transp.TCPTimeouts{}, 0)
	if err != nil {
		t.Fatalf("NewTCPTransport: %v", err)
	}
	ready := make(chan any)
	srv := raft.New(&raft.Config{
		DisableStatsOutput: disabled,
		Fsm:                raft.NewCommitChannelFSM(commitCh),
		PeerIds:            []int{},
		ServerID:           1,
		Storage:            store.NewMapStorage(),
		Transport:          transport,
	}, ready)
	srv.Serve()
	t.Cleanup(func() {
		srv.Shutdown()
		close(commitCh)
		<-readerDone
	})
	close(ready)
	return srv
}

// TestStatsOutputEnabled — при stats-output=true выводятся строки всех трёх
// типов: латентность, счётчики Raft и PersistV1; уровень трассировки
// на периодическую статистику не влияет.
func TestStatsOutputEnabled(t *testing.T) {
	t.Cleanup(leaktest.CheckTimeout(t, raft.LeaktestBudget))
	path := captureStdout(t)
	newStatsServer(t, false)

	deadline := time.Now().Add(statsWaitBudget)
	for {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		text := string(data)
		if strings.Contains(text, "AE=") &&
			strings.Contains(text, "ISsent=") &&
			strings.Contains(text, `"Schema":1`) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("за %v не появились три строки отчёта: %q", statsWaitBudget, text)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestStatsOutputDisabled — при stats-output=false за окно не менее двух
// секундных тактов не выводится ни одна периодическая строка.
func TestStatsOutputDisabled(t *testing.T) {
	t.Cleanup(leaktest.CheckTimeout(t, raft.LeaktestBudget))
	path := captureStdout(t)
	newStatsServer(t, true)

	time.Sleep(statsDisabledWindow)

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 0 {
		t.Fatalf("выключенный вывод написал строки: %q", data)
	}
}
