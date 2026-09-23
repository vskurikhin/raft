package raft

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vskurikhin/raft/pkg/raft/store"
	"github.com/vskurikhin/raft/pkg/raft/transp"
)

// Отступы формата скалярного кадра версии 1 и пакетного журнала версии 2,
// используемые для синтетической порчи внутреннего gob при сохранении
// корректной оболочки. Значения — часть форматов на диске, а не
// производственная константа пакета.
const (
	_v1FrameHeaderSize = 24
	_v2FileHeaderSize  = 24
	_v2BatchHeaderSize = 32
	_v2EntryHeadSize   = 32
)

// v1ScalarFrame собирает корректный скалярный кадр версии 1 с произвольным
// payload. Нужен для проверки CM-старта на payload, который не декодируется
// как gob.
func v1ScalarFrame(payload []byte) []byte {
	raw := make([]byte, _v1FrameHeaderSize+len(payload))
	copy(raw[:8], []byte{'R', 'A', 'F', 'T', 'P', 'S', 'T', 0x00})
	binary.BigEndian.PutUint16(raw[8:10], 1)
	binary.BigEndian.PutUint64(raw[12:20], uint64(len(payload)))
	binary.BigEndian.PutUint32(raw[20:24], crc32.Checksum(payload, crc32.MakeTable(crc32.Castagnoli)))
	copy(raw[_v1FrameHeaderSize:], payload)
	return raw
}

// corruptLogDataKeepingCRC портит Data первой записи пакетного журнала,
// пересчитывая CRC пакета: оболочка остаётся корректной, а внутренний gob
// перестаёт декодироваться. Так проверяется отложенное декодирование Data на
// пути восстановления CM.
func corruptLogDataKeepingCRC(t *testing.T, path string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("чтение журнала: %v", err)
	}
	dataLength := int(binary.BigEndian.Uint64(
		raw[_v2FileHeaderSize+_v2BatchHeaderSize+24 : _v2FileHeaderSize+_v2BatchHeaderSize+32]))
	dataStart := _v2FileHeaderSize + _v2BatchHeaderSize + _v2EntryHeadSize
	if dataStart+dataLength > len(raw) {
		t.Fatalf("длина Data %d выходит за файл %d", dataLength, len(raw))
	}
	for i := dataStart; i < dataStart+dataLength; i++ {
		raw[i] = 0xff
	}
	bodyLength := int(binary.BigEndian.Uint64(
		raw[_v2FileHeaderSize+8 : _v2FileHeaderSize+16]))
	table := crc32.MakeTable(crc32.Castagnoli)
	sum := crc32.Checksum(raw[_v2FileHeaderSize:_v2FileHeaderSize+_v2BatchHeaderSize], table)
	sum = crc32.Update(sum, table, raw[_v2FileHeaderSize+_v2BatchHeaderSize:_v2FileHeaderSize+_v2BatchHeaderSize+bodyLength])
	footerStart := _v2FileHeaderSize + _v2BatchHeaderSize + bodyLength
	binary.BigEndian.PutUint32(raw[footerStart:footerStart+4], sum)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("запись журнала: %v", err)
	}
}

// TestCMStartFatalSubprocess покрывает наблюдаемое поведение старта CM на
// невосстановимом постоянном состоянии: отсутствие ключей при HasData=true,
// недопустимый gob скаляров и ошибку LoadLog при корректной оболочке журнала.
// Отказ выполняется в дочернем процессе, потому что log.Fatal в том же
// процессе перехватить нельзя. Отсутствие журнала не считается пустым: для
// существующего пустого журнала отдельный случай стартует успешно.
func TestCMStartFatalSubprocess(t *testing.T) {
	if os.Getenv("RAFT_CM_START_HELPER") == "1" {
		runCMStartHelper()
		return
	}

	prepareValid := func(t *testing.T, dir string) {
		t.Helper()
		fs := store.NewFileStorage(dir)
		fs.Set("currentTerm", gobEncode(t, 1))
		fs.Set("votedFor", gobEncode(t, -1))
		fs.RewriteLog(nil)
	}
	prepareNonEmptyLog := func(t *testing.T, dir string) {
		t.Helper()
		fs := store.NewFileStorage(dir)
		fs.Set("currentTerm", gobEncode(t, 1))
		fs.Set("votedFor", gobEncode(t, -1))
		fs.RewriteLog([]LogEntry{{Index: 0, Term: 1, Type: LogCommand, Data: "k0=v0"}})
	}

	tests := []struct {
		name      string
		prepare   func(t *testing.T, dir string)
		wantReady bool
		wantMsg   string
	}{
		{
			name:      "существующий пустой журнал стартует",
			prepare:   prepareValid,
			wantReady: true,
		},
		{
			name: "нет currentTerm",
			prepare: func(t *testing.T, dir string) {
				prepareValid(t, dir)
				if err := os.Remove(filepath.Join(dir, "currentTerm.dat")); err != nil {
					t.Fatalf("удаление currentTerm.dat: %v", err)
				}
			},
			wantMsg: "currentTerm not found in storage",
		},
		{
			name: "нет votedFor",
			prepare: func(t *testing.T, dir string) {
				prepareValid(t, dir)
				if err := os.Remove(filepath.Join(dir, "votedFor.dat")); err != nil {
					t.Fatalf("удаление votedFor.dat: %v", err)
				}
			},
			wantMsg: "votedFor not found in storage",
		},
		{
			name: "нет журнала при наличии данных",
			prepare: func(t *testing.T, dir string) {
				prepareValid(t, dir)
				if err := os.Remove(filepath.Join(dir, "log.dat")); err != nil {
					t.Fatalf("удаление log.dat: %v", err)
				}
			},
			wantMsg: "log not found in storage",
		},
		{
			name: "битый gob скаляра при корректной оболочке",
			prepare: func(t *testing.T, dir string) {
				prepareValid(t, dir)
				if err := os.WriteFile(filepath.Join(dir, "currentTerm.dat"),
					v1ScalarFrame([]byte("не gob значение")), 0o600); err != nil {
					t.Fatalf("запись currentTerm.dat: %v", err)
				}
			},
			wantMsg: "gob",
		},
		{
			name: "битый gob Data журнала при корректных длинах и CRC",
			prepare: func(t *testing.T, dir string) {
				prepareNonEmptyLog(t, dir)
				corruptLogDataKeepingCRC(t, filepath.Join(dir, "log.dat"))
			},
			wantMsg: "декодирование Data журнала",
		},
		{
			name: "журнал уплотнён без ключей снимка",
			prepare: func(t *testing.T, dir string) {
				fs := store.NewFileStorage(dir)
				fs.Set("currentTerm", gobEncode(t, 1))
				fs.Set("votedFor", gobEncode(t, -1))
				// Журнал начинается с индекса 5: префикс удалён без
				// долговечных метаданных снимка. Порядок «скаляры снимка до
				// журнала» исключает такое состояние при штатной работе; тест
				// подтверждает отказ старта, а не молчаливое продолжение.
				fs.RewriteLog([]LogEntry{{Index: 5, Term: 1, Type: LogCommand, Data: "k5=v5"}})
			},
			wantMsg: "lastSnapshotIndex missing in storage",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			tt.prepare(t, dir)

			cmd := exec.Command(os.Args[0], "-test.run=^TestCMStartFatalSubprocess$")
			cmd.Env = append(os.Environ(),
				"RAFT_CM_START_HELPER=1",
				"RAFT_CM_START_DIR="+dir,
			)
			if tt.wantReady {
				cmd.Env = append(cmd.Env, "RAFT_CM_START_READY=1")
			}
			out, err := cmd.CombinedOutput()

			if tt.wantReady {
				if err != nil {
					t.Fatalf("дочерний процесс не стартовал на корректном состоянии: %v\n%s", err, out)
				}
				return
			}
			if err == nil {
				t.Fatalf("дочерний процесс завершился успешно, want отказ старта; вывод:\n%s", out)
			}
			if !strings.Contains(string(out), tt.wantMsg) {
				t.Fatalf("сообщение отказа не содержит %q:\n%s", tt.wantMsg, out)
			}
		})
	}
}

// runCMStartHelper выполняет роль дочернего процесса: поднимает узел на
// подготовленном каталоге. Невосстановимое состояние завершает процесс через
// log.Fatal; корректное пустое состояние стартует и останавливается штатно.
func runCMStartHelper() {
	dir := os.Getenv("RAFT_CM_START_DIR")
	ready := make(chan any)
	transport := transp.NewInmemTransport("cm-start-helper")
	if os.Getenv("RAFT_CM_START_READY") == "1" {
		close(ready)
	}
	cm := NewConsensusModule(0, []int{}, transport, store.NewFileStorage(dir), newSnapshotTestFSM(), ready)
	if os.Getenv("RAFT_CM_START_READY") == "1" {
		cm.Stop()
	}
	transport.Close()
}
