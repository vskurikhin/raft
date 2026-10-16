package store

import (
	"bufio"
	"bytes"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/vskurikhin/raft/pkg/raft/contract"
)

// TestStoreLogKillSubprocess проверяет реальный SIGKILL процесса, пишущего
// журнал через FileStorage, отдельно от детерминированной модели носителя.
// Фазы синхронизируются строкой готовности в стандартном выводе дочернего
// процесса: фиксированной паузы нет. После убийства новый экземпляр
// хранилища читает каталог заново.
//
// Это не физическое отключение питания: страничный кэш операционной системы
// переживает завершение процесса, поэтому записанные, но не
// синхронизированные байты допустимо видны после старта. Проверяется
// отсутствие частичного логического журнала и сохранность закреплённого
// префикса.
func TestStoreLogKillSubprocess(t *testing.T) {
	if mode := os.Getenv("STORE_LOG_KILL_MODE"); mode != "" {
		runStoreLogKillHelper(mode)
		return
	}

	base := journalBaseEntries()
	appended := journalAppendEntries()

	tests := []struct {
		name string
		mode string
		// wantExact задаёт точный ожидаемый журнал; при nil допустим как
		// прежний, так и полный новый (фаза до ответа).
		wantExact []contract.LogEntry
	}{
		{
			name:      "kill после ответа StoreLogEntries",
			mode:      "after",
			wantExact: append(append([]contract.LogEntry(nil), base...), appended...),
		},
		{
			name: "kill до синхронизации файла",
			mode: "before-sync",
		},
		{
			name:      "kill до переименования при полной замене",
			mode:      "rewrite-before-rename",
			wantExact: base,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			seedLogFile(t, dir, base)
			before := readDatFile(t, dir, _logKey)

			cmd := exec.Command(os.Args[0], "-test.run=^TestStoreLogKillSubprocess$")
			cmd.Env = append(os.Environ(),
				"STORE_LOG_KILL_MODE="+tt.mode,
				"STORE_LOG_KILL_DIR="+dir,
			)
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatalf("StdoutPipe: %v", err)
			}
			if err := cmd.Start(); err != nil {
				t.Fatalf("запуск помощника: %v", err)
			}

			line, readErr := bufio.NewReader(stdout).ReadString('\n')
			if readErr != nil && line == "" {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
				t.Fatalf("чтение готовности: %v", readErr)
			}
			if !strings.Contains(line, "ready") {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
				t.Fatalf("помощник не сообщил готовность: %q", line)
			}
			if err := cmd.Process.Kill(); err != nil {
				t.Fatalf("kill: %v", err)
			}
			_ = cmd.Wait()

			// Повторная загрузка без отказа процесса: для класса
			// незавершённой записи отказ старта не ожидается.
			fs := newFileStorage(dir, defaultWriteSeam(), defaultReadSeam())
			if loadErr := fs.loadAll(); loadErr != nil {
				t.Fatalf("повторная загрузка: %v", loadErr)
			}
			got, err := fs.LoadLog()
			if err != nil {
				t.Fatalf("LoadLog: %v", err)
			}

			if tt.wantExact != nil {
				requireSameEntries(t, got, tt.wantExact)
				return
			}

			// Фаза до синхронизации: допустим прежний журнал либо полный
			// новый, но не частичный набор записей.
			switch len(got) {
			case len(base):
				requireSameEntries(t, got, base)
			case len(base) + len(appended):
				requireSameEntries(t, got, append(append([]contract.LogEntry(nil), base...), appended...))
			default:
				t.Fatalf("журнал после kill содержит %d записей, want %d либо %d",
					len(got), len(base), len(base)+len(appended))
			}
			// Закреплённый префикс не мог быть разрушен записью в конец:
			// первые байты нового файла совпадают с прежним.
			after := readDatFile(t, dir, _logKey)
			if !bytes.HasPrefix(after, before) {
				t.Fatal("kill разрушил закреплённый префикс журнала")
			}
		})
	}
}

// runStoreLogKillHelper выполняет роль дочернего процесса: пишет журнал,
// сообщает готовность в выбранной фазе и блокируется до убийства.
func runStoreLogKillHelper(mode string) {
	dir := os.Getenv("STORE_LOG_KILL_DIR")
	fromIndex := 2
	appended := journalAppendEntries()

	switch mode {
	case "before-sync":
		seam := defaultWriteSeam()
		seam.syncFile = func(writeAtFile) error {
			_, _ = os.Stdout.WriteString("ready\n")
			select {}
		}
		fs := newFileStorage(dir, seam, defaultReadSeam())
		if err := fs.loadAll(); err != nil {
			os.Exit(2)
		}
		fs.StoreLogEntries(fromIndex, appended)
	case "after":
		fs := newFileStorage(dir, defaultWriteSeam(), defaultReadSeam())
		if err := fs.loadAll(); err != nil {
			os.Exit(2)
		}
		fs.StoreLogEntries(fromIndex, appended)
		_, _ = os.Stdout.WriteString("ready\n")
		select {}
	case "rewrite-before-rename":
		seam := defaultWriteSeam()
		seam.rename = func(string, string, string) error {
			_, _ = os.Stdout.WriteString("ready\n")
			select {}
		}
		fs := newFileStorage(dir, seam, defaultReadSeam())
		if err := fs.loadAll(); err != nil {
			os.Exit(2)
		}
		fs.RewriteLog([]contract.LogEntry{contractEntry(0, 0, contract.LogNoop, nil)})
	}
}
