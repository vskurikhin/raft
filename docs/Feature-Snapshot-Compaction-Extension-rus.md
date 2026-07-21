# Snapshot/Compaction Extension

## Проблема

`cm.log` растёт неограниченно — каждая зафиксированная команда добавляет `LogEntry`, а `persistToStorage()`
сериализует весь журнал целиком. 
Механизма удаления старых записей нет. 
Отставший или перезапущенный узел вынужден воспроизводить полный журнал.

## Объём работ

Добавить снепшотирование/сжатие Raft: обрезать журнал, сохранять метаданные снепшота и реализовать RPC `InstallSnapshot`.

## Изменения

### 1. Новые типы в `pkg/raft/raft.go`

```go
// SnapshotHeader — метаданные, хранящиеся вместе с данными снепшота.
type SnapshotHeader struct {
    LastIncludedIndex int
    LastIncludedTerm  int
}

// InstallSnapshotArgs / InstallSnapshotReply для RPC.
type InstallSnapshotArgs struct {
    Term              int
    LeaderID          int
    LastIncludedIndex int
    LastIncludedTerm  int
    Data              []byte
}

type InstallSnapshotReply struct {
    Term int
}
```

### 2. Новые поля в `ConsensusModule` (`pkg/raft/raft.go`)

Добавить в структуру:

```go
    // Состояние снепшота
    lastIncludedIndex int           // последний индекс, включённый в снепшот
    lastIncludedTerm  int           // терм этого индекса
    snapshotData      []byte        // кэшированные данные снепшота (nil, если нет)
```

### 3. Создание снепшота (`pkg/raft/raft.go`)

Новый метод `TakeSnapshot(stateMachineData []byte)`:

1. Вызывающий передаёт сериализованные данные машины состояний (содержимое KV-хранилища).
2. Установить `lastIncludedIndex = cm.lastApplied`, `lastIncludedTerm = cm.log[lastApplied].Term`.
3. Обрезать `cm.log` до записей после `lastIncludedIndex` (или оставить хотя бы одну запись, чтобы работали проверки PrevLogIndex).
4. Сохранить метаданные снепшота + данные через `persistToStorage()`.
5. Закэшировать `snapshotData` в памяти.

### 4. Изменения персистентности (`pkg/raft/storage.go`)

Расширить ключи `Storage`:

| Ключ                    | Содержимое                            |
|-------------------------|---------------------------------------|
| `"lastIncludedIndex"`   | gob от `int`                          |
| `"lastIncludedTerm"`    | gob от `int`                          |
| `"snapshot"`            | сырые байты (данные машины состояний) |

`persistToStorage()` сохраняет их вместе с `"currentTerm"`, `"votedFor"` и **обрезанным** журналом.

`restoreFromStorage()` загружает их и восстанавливает `cm.lastIncludedIndex`, `cm.lastIncludedTerm`, `cm.snapshotData`.

### 5. RPC InstallSnapshot (`pkg/raft/raft.go`)

Новый метод `InstallSnapshot(args, reply)`:

- **Получатель (follower)**:
  1. Если `args.Term < cm.currentTerm` → отклонить, установить `reply.Term = cm.currentTerm`, вернуться.
  2. Если `args.Term > cm.currentTerm` → стать follower.
  3. Принять снепшот: установить `lastIncludedIndex`, `lastIncludedTerm`,
     установить `commitIndex = max(commitIndex, args.LastIncludedIndex)`,
     установить `lastApplied = max(lastApplied, args.LastIncludedIndex)`.
  4. Заменить `cm.log` на пустой срез (или оставить одну запись-заглушку).
  5. Сохранить снепшот через `persistToStorage()`.
  6. Передать данные снепшота машине состояний (через `commitChan` как snapshot-запись или через отдельный `snapshotChan`).

- **Отправитель (leader)**:
  - Вызывается из `leaderSendAEs`, когда `nextIndex[peer] ≤ lastIncludedIndex` (follower слишком далеко позади).
  - Отправить снепшот целиком или по частям.

### 6. Хелперы доступа к журналу (`pkg/raft/raft.go`)

Заменить прямой доступ `cm.log[i]` на хелперы, учитывающие `lastIncludedIndex`:

```go
func (cm *ConsensusModule) getLogEntry(i int) (LogEntry, bool) {
    offset := cm.lastIncludedIndex + 1
    if i < offset || i >= offset+len(cm.log) {
        return LogEntry{}, false
    }
    return cm.log[i-offset], true
}

func (cm *ConsensusModule) getLogLength() int {
    return cm.lastIncludedIndex + len(cm.log)
}

func (cm *ConsensusModule) getLastLogIndex() int {
    return cm.lastIncludedIndex + len(cm.log) - 1
}

func (cm *ConsensusModule) getLastLogTerm() int {
    if len(cm.log) > 0 {
        return cm.log[len(cm.log)-1].Term
    }
    return cm.lastIncludedTerm
}
```

### 7. Запуск InstallSnapshot на лидере (`pkg/raft/raft.go`)

В `leaderSendAEs` (или в новой горутине `sendInstallSnapshot`):

- Если `nextIndex[peer] <= cm.lastIncludedIndex`:
  1. Отправить `InstallSnapshotArgs` с `cm.snapshotData`, `cm.lastIncludedIndex`, `cm.lastIncludedTerm`.
  2. При успехе: установить `nextIndex[peer] = cm.lastIncludedIndex + 1`, `matchIndex[peer] = cm.lastIncludedIndex`.

### 8. Адаптация commitChanSender (`pkg/raft/raft.go`)

Если данные снепшота приходят через `InstallSnapshot`, передать их машине состояний. Два варианта:

- **Вариант A** (проще): отправить `CommitEntry` с `Command = snapshotData` и специальным флагом. Машина состояний детектирует его и заменяет своё состояние.
- **Вариант B**: добавить отдельный канал `snapshotChan chan<- []byte` в `ConsensusModule`.

Рекомендуется Вариант B для ясности — машина состояний подписывается на два канала: `commitChan` для инкрементальных команд и `snapshotChan` для полной замены состояния.

### 9. Интеграция с машиной состояний (`pkg/kvservice/kvservice.go`)

- Добавить метод `Snapshot()` в `DataStore`, сериализующий полную `data` map.
- Добавить метод `RestoreFromSnapshot(data []byte)`, десериализующий и заменяющий `data`.
- `runUpdater` слушает оба канала `commitChan` и `snapshotChan`:
  - По `snapshotChan`: вызвать `RestoreFromSnapshot`, затем продвинуть `lastApplied` до `lastIncludedIndex`.
  - По `commitChan`: применить команду как раньше.

### 10. Регистрация RPC (`pkg/raft/server.go`)

Зарегистрировать `"ConsensusModule.InstallSnapshot"` в `Server.Serve()`.

### 11. Политика интервала снепшотов

Добавить конфигурируемые `SnapshotInterval int` (по умолчанию: например, 128 записей) и `SnapshotThreshold int` (например, 1024 записи).  
После коммита, если `len(cm.log) > SnapshotThreshold`, вызвать `TakeSnapshot()`.  
Или запускать периодически на основе `commitIndex - lastIncludedIndex`.

## План тестирования

1. **Модульный тест**: `TestSnapshotBasic` — отправить N команд, сделать снепшот, проверить обрезание журнала, проверить, что `getLogEntry` возвращает правильные записи.
2. **Модульный тест**: `TestSnapshotInstall` — запустить кластер, дать лидеру накопить журнал, сделать снепшот, добавить новый узел (или перезапустить узел), которому нужен снепшот, проверить, что он получает `InstallSnapshot` и догоняет.
3. **Модульный тест**: `TestSnapshotPersistence` — сделать снепшот, перезапустить узел, проверить, что восстановленные журнал/метаданные снепшота корректны.
4. **Интеграционный тест**: системный тест с KV-сервисом — проверить, что KV-хранилище переживает перезапуск со снепшотом.

## Обратная совместимость

- Существующие кластеры без данных снепшота: `restoreFromStorage()` не видит ключей `"lastIncludedIndex"`/`"lastIncludedTerm"`/`"snapshot"` → ведёт себя как раньше (`lastIncludedIndex = 0`, `lastIncludedTerm = 0`, `snapshotData = nil`).
- RPC: узлы, не понимающие `InstallSnapshot`, вернут ошибку; лидер считает это неудачным вызовом и возвращается к отправке полного журнала (существующее поведение).

## Риски

- **Gob-кодирование данных снепшота**: данные снепшота — непрозрачные байты, не gob-кодированные. Машина состояний занимается своей сериализацией.
- **Обрезание журнала + логика смещения**: все прямые обращения `cm.log[i]` должны проходить через хелперы. Пропущенные обращения вызывают панику.
- **InstallSnapshot + конкурентный коммит**: снепшот может прибыть на follower, пока работает `commitChanSender`. Защитить через `cm.mu`.
