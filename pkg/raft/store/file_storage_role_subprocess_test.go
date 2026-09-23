package store

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vskurikhin/raft/pkg/raft/contract"
)

// _stage3LogSHA — SHA-256 реальных байтов журнала предыдущего этапа
// (testdata/stage3/node-1/log.dat) и скалярного файла того же набора. Хеши
// фиксируют приспособление для сверки в отчётах о совместимости ролей.
const (
	_stage3LogSHA     = "cfc868b849a9f2c9d1d20eeed696f1fa6fb258372dcc806fb52dfceef3188bc1"
	_stage3CurrentSHA = "f4779b4450a12749c45b1e6f6b78b3de515966bc0165eb0057e16c6f50b30da2"
)

// TestStage3FixtureSHA проверяет, что реальные байты предыдущего этапа не
// изменились: хеши зафиксированы, синтетическое пересоставление приспособления
// не заменяет подлинные байты.
func TestStage3FixtureSHA(t *testing.T) {
	files := map[string]string{
		"log.dat":         _stage3LogSHA,
		"currentTerm.dat": _stage3CurrentSHA,
	}
	for name, want := range files {
		raw, err := os.ReadFile(filepath.Join("testdata/stage3/node-1", name))
		if err != nil {
			t.Fatalf("чтение приспособления %s: %v", name, err)
		}
		sum := sha256.Sum256(raw)
		if got := hex.EncodeToString(sum[:]); got != want {
			t.Fatalf("SHA-256 %s = %s, want %s", name, got, want)
		}
	}
}

// TestRoleVersionMismatchSubprocess подтверждает фактическим выводом
// дочернего процесса разделение версий по ролям файлов: пакетный журнал
// версии 2, помещённый в скалярную роль, отвергается скалярным читателем
// («неподдерживаемая версия кадра 2»), а реальные байты журнала предыдущего
// этапа отвергаются загрузчиком журнала («неподдерживаемая версия журнала 1»).
// Каждый случай выполняется в отдельном временном каталоге: рабочие данные и
// прошлые результаты не затрагиваются.
func TestRoleVersionMismatchSubprocess(t *testing.T) {
	if os.Getenv("STORE_ROLE_HELPER") == "1" {
		NewFileStorage(os.Getenv("STORE_ROLE_DIR"))
		return
	}

	t.Run("версия 2 в скалярной роли", func(t *testing.T) {
		dir := t.TempDir()
		fs := NewFileStorage(dir)
		fs.RewriteLog([]contract.LogEntry{contractEntry(0, 0, contract.LogNoop, nil)})
		rawLog := readDatFile(t, dir, _logKey)
		writeDatFile(t, dir, "currentTerm.dat", rawLog)
		if err := os.Remove(filepath.Join(dir, _logFileName)); err != nil {
			t.Fatalf("удаление журнала: %v", err)
		}

		out := runStoreRoleHelper(t, dir)
		if !strings.Contains(out, "неподдерживаемая версия кадра 2") {
			t.Fatalf("сообщение не подтверждает отказ версии 2 в скалярной роли:\n%s", out)
		}
	})

	t.Run("версия 1 в журнальной роли", func(t *testing.T) {
		dir := t.TempDir()
		// Реальные байты кадра версии 1 (L2) с зафиксированным SHA:
		// синтетическое описание не заменяет подлинные байты.
		writeDatFile(t, dir, _logFileName, v1LogFrame(t))

		out := runStoreRoleHelper(t, dir)
		if !strings.Contains(out, "неподдерживаемая версия журнала 1") {
			t.Fatalf("сообщение не подтверждает отказ версии 1 в журнальной роли:\n%s", out)
		}
	})
}

// runStoreRoleHelper запускает публичный конструктор хранилища в дочернем
// процессе и возвращает объединённый вывод: log.Fatalf не перехватывается в
// том же процессе, а сообщение обязано быть фактическим.
func runStoreRoleHelper(t *testing.T, dir string) string {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestRoleVersionMismatchSubprocess$")
	cmd.Env = append(os.Environ(),
		"STORE_ROLE_HELPER=1",
		"STORE_ROLE_DIR="+dir,
	)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("дочерний процесс завершился успешно, want отказ старта; вывод:\n%s", out)
	}
	return string(out)
}
