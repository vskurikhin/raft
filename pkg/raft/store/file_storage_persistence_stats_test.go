package store

import (
	"testing"

	"github.com/fortytw2/leaktest"

	"github.com/vskurikhin/raft"
)

// -- Тесты диагностической возможности PersistenceStats: точные записи и
// -- суммы длительностей Sync. Проверки не зависят от абсолютной скорости
// -- диска: утверждения касаются только равенства счётчика записей,
// -- неизменности наблюдений при пропущенном Set и неубывания сумм. --

// TestFileStoragePersistenceStats_RestingMatchesWriteCount проверяет, что на
// покоящемся экземпляре PersistenceStats возвращает ровно то же число
// записей, что и существующий WriteCount, а до первой записи суммы нулевые:
// нулевое наблюдение не подменяется отсутствием возможности.
func TestFileStoragePersistenceStats_RestingMatchesWriteCount(t *testing.T) {
	defer leaktest.CheckTimeout(t, raft.LeaktestBudget)()

	fs := NewFileStorage(t.TempDir())

	writes, fileSyncNS, dirSyncNS := fs.PersistenceStats()
	if writes != 0 || fileSyncNS != 0 || dirSyncNS != 0 {
		t.Fatalf("свежее хранилище: stats = (%d, %d, %d), want все нули", writes, fileSyncNS, dirSyncNS)
	}
	if got := fs.WriteCount(); got != writes {
		t.Fatalf("WriteCount = %d, PersistenceStats.writes = %d, want равенство", got, writes)
	}

	fs.Set("currentTerm", gobEncode(t, 1))
	writes, fileSyncNS, dirSyncNS = fs.PersistenceStats()
	if got := fs.WriteCount(); got != writes {
		t.Fatalf("после записи WriteCount = %d, PersistenceStats.writes = %d, want равенство", got, writes)
	}
	if writes != 1 {
		t.Fatalf("writes = %d после одной записи, want 1", writes)
	}
	// Ненулевые суммы доказывают, что замеры вокруг f.Sync и syncDir
	// действительно выполняются: системный вызов не может завершиться
	// за ноль наносекунд. Порог в миллисекундах не проверяется.
	if fileSyncNS <= 0 || dirSyncNS <= 0 {
		t.Fatalf("суммы Sync = (%d, %d), want обе > 0", fileSyncNS, dirSyncNS)
	}
}

// TestFileStoragePersistenceStats_SkippedSetUnchanged проверяет, что Set с
// тем же значением не выполняет ввода-вывода и не меняет ни записи, ни
// суммы: наблюдения прибавляются только вместе с writes++.
func TestFileStoragePersistenceStats_SkippedSetUnchanged(t *testing.T) {
	defer leaktest.CheckTimeout(t, raft.LeaktestBudget)()

	fs := NewFileStorage(t.TempDir())
	value := gobEncode(t, 42)
	fs.Set("currentTerm", value)

	beforeWrites, beforeFile, beforeDir := fs.PersistenceStats()
	for i := 0; i < 10; i++ {
		fs.Set("currentTerm", value)
	}
	afterWrites, afterFile, afterDir := fs.PersistenceStats()

	if afterWrites != beforeWrites {
		t.Fatalf("writes %d -> %d после повторного равного Set, want без изменений", beforeWrites, afterWrites)
	}
	if afterFile != beforeFile || afterDir != beforeDir {
		t.Fatalf("суммы Sync изменились на равном Set: file %d -> %d, dir %d -> %d",
			beforeFile, afterFile, beforeDir, afterDir)
	}
}

// TestFileStoragePersistenceStats_ChangedSetAddsOneWrite проверяет, что
// каждая фактическая запись увеличивает writes ровно на один, а суммы
// длительностей Sync не убывают: замеры относятся к завершённому циклу
// ключа, а не к попытке.
func TestFileStoragePersistenceStats_ChangedSetAddsOneWrite(t *testing.T) {
	defer leaktest.CheckTimeout(t, raft.LeaktestBudget)()

	fs := NewFileStorage(t.TempDir())
	writes, fileSyncNS, dirSyncNS := fs.PersistenceStats()

	for i := 0; i < 5; i++ {
		fs.Set("currentTerm", gobEncode(t, i))
		gotWrites, gotFile, gotDir := fs.PersistenceStats()
		if gotWrites != writes+1 {
			t.Fatalf("итерация %d: writes = %d, want %d", i, gotWrites, writes+1)
		}
		if gotFile < fileSyncNS || gotDir < dirSyncNS {
			t.Fatalf("итерация %d: суммы Sync убыли: file %d -> %d, dir %d -> %d",
				i, fileSyncNS, gotFile, dirSyncNS, gotDir)
		}
		writes, fileSyncNS, dirSyncNS = gotWrites, gotFile, gotDir
	}
	if writes != 5 {
		t.Fatalf("writes = %d после пяти изменённых Set, want 5", writes)
	}
	if fileSyncNS <= 0 || dirSyncNS <= 0 {
		t.Fatalf("суммы Sync = (%d, %d) после пяти записей, want обе > 0", fileSyncNS, dirSyncNS)
	}
}
