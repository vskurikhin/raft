package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/vskurikhin/raft/pkg/raft/contract"
)

// v1LogFixtureHex — зафиксированные байты журнала версии 1: кадр L2 с
// payload — gob-представлением среза записей (Data "hello" и nil). Байты
// записаны напрямую, а не пересобираются в тесте: представление фикстуры
// должно быть неизменным независимо от порядка регистраций gob.
const v1LogFixtureHex = "524146545053540000010000000000000000006e95328e220dff8302" +
	"0102ff840001ff8200003dff810301010a6c6f67456e747279563101ff820001" +
	"040105496e64657801040001045465726d010400010454797065010400010444" +
	"617461011000000021ff840002010201020206737472696e670c07000568656c" +
	"6c6f0001040102010200"

// v1LogFrame возвращает реальные байты журнала версии 1 из фикстуры. Такие
// байты обязана отвергать сборка версии 2.
func v1LogFrame(t *testing.T) []byte {
	t.Helper()
	raw, err := hex.DecodeString(v1LogFixtureHex)
	if err != nil {
		t.Fatalf("декодирование фикстуры v1: %v", err)
	}
	return raw
}

// rawLogPacket собирает структурно корректный пакет с произвольными байтами
// Data: контрольные суммы и длины согласованы, содержимое Data может быть
// недекодируемым gob. Нужен для проверки отложенного декодирования.
func rawLogPacket(t *testing.T, data ...[]byte) []byte {
	t.Helper()
	var body []byte
	for i, d := range data {
		body = append(body, buildEntryRecord(i, 1, contract.LogCommand, uint64(len(d)))...)
		body = append(body, d...)
	}
	return buildLogPacket(t, body, uint64(len(data)))
}

// TestLoadAllLogV2WithScalarV1 проверяет штатное сочетание: журнал версии 2
// и скалярные ключи версии 1 загружаются вместе, журнал читается через
// LoadLog, зарезервированный ключ через Get недоступен.
func TestLoadAllLogV2WithScalarV1(t *testing.T) {
	dir := t.TempDir()
	entries := []contract.LogEntry{
		contractEntry(0, 0, contract.LogNoop, nil),
		contractEntry(1, 1, contract.LogConfiguration, []byte{0x01, 0x02}),
	}
	writeDatFile(t, dir, _logFileName, mustEncodeFile(t, entries))
	writeDatFile(t, dir, "currentTerm.dat", frameBytes([]byte("term")))

	fs := newFileStorage(dir, defaultWriteSeam(), defaultReadSeam())
	if err := fs.loadAll(); err != nil {
		t.Fatalf("loadAll: %v", err)
	}
	if !fs.HasData() {
		t.Fatal("HasData = false при журнале и скалярах, want true")
	}
	got, err := fs.LoadLog()
	if err != nil {
		t.Fatalf("LoadLog: %v", err)
	}
	requireSameEntries(t, got, entries)
	if v, ok := fs.Get("currentTerm"); !ok || string(v) != "term" {
		t.Fatalf("currentTerm = %q, ok=%v, want term", v, ok)
	}
	if _, ok := fs.Get(_logKey); ok {
		t.Fatal("Get зарезервированного ключа вернул значение")
	}
}

// TestLoadAllRejectsLogV1 проверяет, что журнал версии 1 отвергается
// загрузчиком версии 2 до запуска протокола: старой сборке и новой
// соответствуют несовместимые форматы одной роли файла.
func TestLoadAllRejectsLogV1(t *testing.T) {
	dir := t.TempDir()
	writeDatFile(t, dir, _logFileName, v1LogFrame(t))

	err := loadAllWithDefaultSeams(dir)
	if err == nil {
		t.Fatal("loadAll вернул nil на журнале версии 1, want отказ")
	}
	if !strings.Contains(err.Error(), _logFileName) {
		t.Fatalf("ошибка %q не содержит путь %s", err, _logFileName)
	}
}

// TestV1LogFixtureSHA записи версии 1: байты фикстуры побайтово
// зафиксированы хешем SHA-256 и отвергаются загрузчиком версии 2. Хеш
// предназначен для сверки в отчётах о долговечности.
func TestV1LogFixtureSHA(t *testing.T) {
	raw := v1LogFrame(t)
	sum := sha256.Sum256(raw)
	const wantSHA = "d8400b17e5bfdb3f1d238523f331cc3754adc84ab72d30a6712899071242de8b"
	got := hex.EncodeToString(sum[:])
	if got != wantSHA {
		t.Fatalf("SHA-256 байтов v1 = %s, want %s; hex=%s", got, wantSHA, hex.EncodeToString(raw))
	}

	dir := t.TempDir()
	writeDatFile(t, dir, _logFileName, raw)
	if err := loadAllWithDefaultSeams(dir); err == nil {
		t.Fatal("loadAll вернул nil на реальных байтах v1, want отказ")
	}
}

// TestLoadAllRejectsScalarV2 проверяет, что скалярный ключ с версией кадра 2
// отвергается: версии распределены по ролям файлов, скаляры остаются v1.
func TestLoadAllRejectsScalarV2(t *testing.T) {
	dir := t.TempDir()
	writeDatFile(t, dir, "scalar.dat", mustEncodeFile(t, nil))

	err := loadAllWithDefaultSeams(dir)
	if err == nil {
		t.Fatal("loadAll вернул nil на скаляре версии 2, want отказ")
	}
	if !strings.Contains(err.Error(), "scalar.dat") {
		t.Fatalf("ошибка %q не содержит путь scalar.dat", err)
	}
}

// TestLoadLogDefersDataDecode проверяет, что загрузка каталога проверяет
// только структуру: недекодируемая Data не мешает открытию и возвращается
// ошибкой из LoadLog после того, как потребитель зарегистрировал типы.
func TestLoadLogDefersDataDecode(t *testing.T) {
	dir := t.TempDir()
	header := encodeLogFileHeader()
	// Структурно корректный пакет с байтами, которые не являются gob.
	file := append(header[:], rawLogPacket(t, []byte{0xff, 0xff, 0xff, 0xff})...)
	writeDatFile(t, dir, _logFileName, file)

	fs := newFileStorage(dir, defaultWriteSeam(), defaultReadSeam())
	if err := fs.loadAll(); err != nil {
		t.Fatalf("loadAll не должен декодировать Data: %v", err)
	}
	if _, err := fs.LoadLog(); err == nil {
		t.Fatal("LoadLog вернул nil на недекодируемой Data, want ошибку")
	}
}

// TestLoadLogIncompleteTailAccepted проверяет правила хвоста: после целой
// базы неполный последний пакет отбрасывается, LoadLog отдаёт только целые
// записи и сам файл не изменяет.
func TestLoadLogIncompleteTailAccepted(t *testing.T) {
	dir := t.TempDir()
	base := []contract.LogEntry{
		contractEntry(0, 0, contract.LogNoop, nil),
		contractEntry(1, 1, contract.LogCommand, []byte{0x0a}),
	}
	baseFile := mustEncodeFile(t, base)
	packet := mustEncodeBatch(t, []contract.LogEntry{contractEntry(2, 1, contract.LogCommand, []byte{0x0b})})
	file := append(append([]byte(nil), baseFile...), packet[:len(packet)-3]...)
	writeDatFile(t, dir, _logFileName, file)

	fs := newFileStorage(dir, defaultWriteSeam(), defaultReadSeam())
	if err := fs.loadAll(); err != nil {
		t.Fatalf("loadAll: %v", err)
	}
	if !fs.journal.tail {
		t.Fatal("незавершённый хвост не отмечен")
	}
	if fs.journal.validEnd != len(baseFile) {
		t.Fatalf("validEnd = %d, want %d", fs.journal.validEnd, len(baseFile))
	}
	got, err := fs.LoadLog()
	if err != nil {
		t.Fatalf("LoadLog: %v", err)
	}
	requireSameEntries(t, got, base)
	if onDisk := readDatFile(t, dir, _logKey); !bytes.Equal(onDisk, file) {
		t.Fatal("LoadLog изменил файл журнала, want только чтение")
	}
}

// TestLoadAllRejectsCorruptFullPacket проверяет, что полный пакет с неверной
// контрольной суммой считается повреждением, а не хвостом.
func TestLoadAllRejectsCorruptFullPacket(t *testing.T) {
	dir := t.TempDir()
	baseFile := mustEncodeFile(t, []contract.LogEntry{contractEntry(0, 0, contract.LogNoop, nil)})
	packet := mustEncodeBatch(t, []contract.LogEntry{contractEntry(1, 1, contract.LogCommand, []byte{0x0c})})
	packet[len(packet)-1] ^= 0xff
	writeDatFile(t, dir, _logFileName, append(append([]byte(nil), baseFile...), packet...))

	err := loadAllWithDefaultSeams(dir)
	if err == nil {
		t.Fatal("loadAll вернул nil на полном повреждённом пакете, want отказ")
	}
	if !errors.Is(err, errLogCorrupt) {
		t.Fatalf("ошибка %v не помечена как повреждение", err)
	}
}

// TestLoadAllRejectsIndexDiscontinuity проверяет, что разрыв индексов между
// пакетами считается повреждением, а не хвостом.
func TestLoadAllRejectsIndexDiscontinuity(t *testing.T) {
	dir := t.TempDir()
	baseFile := mustEncodeFile(t, []contract.LogEntry{
		contractEntry(0, 0, contract.LogNoop, nil),
		contractEntry(1, 1, contract.LogCommand, []byte{0x0a}),
	})
	packet := mustEncodeBatch(t, []contract.LogEntry{contractEntry(5, 1, contract.LogCommand, []byte{0x0d})})
	writeDatFile(t, dir, _logFileName, append(append([]byte(nil), baseFile...), packet...))

	err := loadAllWithDefaultSeams(dir)
	if err == nil {
		t.Fatal("loadAll вернул nil на разрыве индексов, want отказ")
	}
	if !errors.Is(err, errLogCorrupt) {
		t.Fatalf("ошибка %v не помечена как повреждение", err)
	}
}

// TestStoreLogEntriesNormalizesTail проверяет однократную нормализацию
// незавершённого хвоста до следующей записи: логический no-op всё равно
// нормализует файл и учитывает расход, повторный no-op уже не пишет.
func TestStoreLogEntriesNormalizesTail(t *testing.T) {
	dir := t.TempDir()
	base := []contract.LogEntry{
		contractEntry(0, 0, contract.LogNoop, nil),
		contractEntry(1, 1, contract.LogCommand, []byte{0x0a}),
	}
	baseFile := mustEncodeFile(t, base)
	packet := mustEncodeBatch(t, []contract.LogEntry{contractEntry(2, 1, contract.LogCommand, []byte{0x0b})})
	file := append(append([]byte(nil), baseFile...), packet[:len(packet)-3]...)
	writeDatFile(t, dir, _logFileName, file)

	fs := newFileStorage(dir, defaultWriteSeam(), defaultReadSeam())
	if err := fs.loadAll(); err != nil {
		t.Fatalf("loadAll: %v", err)
	}

	fromIndex := base[len(base)-1].Index + 1
	result, err := fs.storeLogEntriesLocked(fromIndex, nil)
	if err != nil {
		t.Fatalf("storeLogEntriesLocked: %v", err)
	}
	if result.Writes != 1 || result.BytesWritten == 0 {
		t.Fatalf("нормализация: результат = %+v, want одну запись с ненулевыми байтами", result)
	}
	if fs.journal.tail {
		t.Fatal("после нормализации хвост всё ещё отмечен")
	}
	if onDisk := readDatFile(t, dir, _logKey); !bytes.Equal(onDisk, mustEncodeFile(t, base)) {
		t.Fatal("нормализация не привела файл к принятому журналу")
	}

	// Повторный no-op уже не пишет.
	second, err := fs.storeLogEntriesLocked(fromIndex, nil)
	if err != nil {
		t.Fatalf("повторный storeLogEntriesLocked: %v", err)
	}
	if second.Writes != 0 || second.BytesWritten != 0 {
		t.Fatalf("повторный no-op = %+v, want нули", second)
	}

	// Повторное открытие нормализованного файла не видит хвоста.
	reopened := newFileStorage(dir, defaultWriteSeam(), defaultReadSeam())
	if err := reopened.loadAll(); err != nil {
		t.Fatalf("повторный loadAll: %v", err)
	}
	if reopened.journal.tail {
		t.Fatal("повторное открытие снова видит хвост")
	}
}

// TestLoadAllRepeatedOpenOnTailIdempotent проверяет, что многократное
// открытие каталога с незавершённым хвостом не мутирует файл: отброшенный
// хвост переписывается только первой записью.
func TestLoadAllRepeatedOpenOnTailIdempotent(t *testing.T) {
	dir := t.TempDir()
	baseFile := mustEncodeFile(t, []contract.LogEntry{contractEntry(0, 0, contract.LogNoop, nil)})
	packet := mustEncodeBatch(t, []contract.LogEntry{contractEntry(1, 1, contract.LogCommand, []byte{0x0e})})
	file := append(append([]byte(nil), baseFile...), packet[:len(packet)-2]...)
	writeDatFile(t, dir, _logFileName, file)
	before := readDatFile(t, dir, _logKey)

	for i := 0; i < 3; i++ {
		fs := newFileStorage(dir, defaultWriteSeam(), defaultReadSeam())
		if err := fs.loadAll(); err != nil {
			t.Fatalf("открытие %d: %v", i, err)
		}
		if _, err := fs.LoadLog(); err != nil {
			t.Fatalf("LoadLog %d: %v", i, err)
		}
	}
	if after := readDatFile(t, dir, _logKey); !bytes.Equal(before, after) {
		t.Fatal("повторные открытия изменили файл журнала")
	}
}

// TestFileStorageReservedLogKey проверяет зарезервированный ключ журнала:
// Get возвращает отсутствие, LoadLog на свежем хранилище — ErrLogNotFound.
func TestFileStorageReservedLogKey(t *testing.T) {
	fs := NewFileStorage(t.TempDir())
	if _, ok := fs.Get(_logKey); ok {
		t.Fatal("Get(log) вернул значение, want отсутствие")
	}
	if _, err := fs.LoadLog(); !errors.Is(err, contract.ErrLogNotFound) {
		t.Fatalf("LoadLog свежего хранилища = %v, want ErrLogNotFound", err)
	}
}

// TestFileStorageSetLogFatal проверяет, что прямой Set зарезервированного
// ключа завершает процесс: помощник запускается дочерним процессом.
func TestFileStorageSetLogFatal(t *testing.T) {
	if os.Getenv("STORE_LOG_RESERVED_HELPER") == "1" {
		NewFileStorage(t.TempDir()).Set(_logKey, []byte("value"))
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestFileStorageSetLogFatal$")
	cmd.Env = append(os.Environ(), "STORE_LOG_RESERVED_HELPER=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("дочерний процесс завершился успешно, want отказ; вывод:\n%s", out)
	}
	if !strings.Contains(string(out), "FileStorage.Set") {
		t.Fatalf("вывод дочернего процесса не содержит сообщение Set:\n%s", out)
	}
}

// TestMapStorageReservedLogKey проверяет зарезервированный ключ в
// реализации в памяти: Get возвращает отсутствие, Set завершает процесс.
func TestMapStorageReservedLogKey(t *testing.T) {
	ms := NewMapStorage()
	if _, ok := ms.Get(_logKey); ok {
		t.Fatal("Get(log) вернул значение, want отсутствие")
	}
	if _, err := ms.LoadLog(); !errors.Is(err, contract.ErrLogNotFound) {
		t.Fatalf("LoadLog свежего хранилища = %v, want ErrLogNotFound", err)
	}
}

// TestMapStorageSetLogFatal проверяет немедленное завершение прямого Set
// зарезервированного ключа в реализации в памяти.
func TestMapStorageSetLogFatal(t *testing.T) {
	if os.Getenv("MAP_LOG_RESERVED_HELPER") == "1" {
		NewMapStorage().Set(_logKey, []byte("value"))
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestMapStorageSetLogFatal$")
	cmd.Env = append(os.Environ(), "MAP_LOG_RESERVED_HELPER=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("дочерний процесс завершился успешно, want отказ; вывод:\n%s", out)
	}
	if !strings.Contains(string(out), "MapStorage.Set") {
		t.Fatalf("вывод дочернего процесса не содержит сообщение Set:\n%s", out)
	}
}
