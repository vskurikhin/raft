# Байтовый контракт Data: команда KVC1, журнал v3 и AppendEntries

## Назначение

Документ описывает, как данные записи журнала (`LogEntry.Data`) проходят
путь от HTTP-обработчика KV-сервиса до машины состояний: формат команды,
представление в журнале на диске, сетевое представление AppendEntries и
поведение при ошибках. Первоисточник — код; ссылки на исходные файлы приведены в соответствующих разделах. Порядок перехода существующего кластера — в
[Data-v3-migration.md](Data-v3-migration.md).

Английская версия — [API-Data-v3-en.md](API-Data-v3-en.md).

## 1. Путь данных

```text
HTTP → kvservice: Command → EncodeCommand (один раз, формат KVC1)
     → Server.Apply([]byte) → ConsensusModule.Apply: собственная копия
     → LogEntry.Data []byte (неизменяемые готовые байты)
       ├─ журнал: Data версии 1 внутри файла журнала версии 3
       └─ TCP: AppendEntriesBytesV1 с теми же байтами
     → фиксация → FSM KV-сервиса → DecodeCommand → Command → DataStore
```

- Все четыре HTTP-обработчика команд (`POST /put/`, `/get/`, `/cas/`, `/delete/`)
  вызывают одну функцию `applyCommand`, которая кодирует команду ровно
  один раз и передаёт байты в `Apply` (`pkg/kvservice/kvservice.go`).
- Корневой пакет `raft`, `pkg/raft/store` и `pkg/raft/transp` не
  импортируют `pkg/kvservice` и `pkg/raft/codec` и не разбирают KVC1:
  журнал и транспорт копируют и упаковывают готовые байты, а не кодируют
  поля команды заново.
- Произвольных Go-типов в `Data` нет: общий контракт —
  `LogEntry.Data []byte` (`pkg/raft/contract/rpc.go`),
  `CommitEntry.Data []byte` (`raft.go`), `Apply([]byte, time.Duration)`
  (`server.go`, `raft_cm_leader.go`). Прежний `Apply(any)` и носитель
  команды не поддерживаются.

## 2. Публичный API

| Символ | Сигнатура | Файл |
|---|---|---|
| `Server.Apply` | `func (s *Server) Apply(cmd []byte, timeout time.Duration) ApplyFuture` | `server.go` |
| `ConsensusModule.Apply` | `func (cm *ConsensusModule) Apply(command []byte, timeout time.Duration) ApplyFuture` | `raft_cm_leader.go` |
| `LogEntry.Data` | `Data []byte` | `pkg/raft/contract/rpc.go` |
| `CommitEntry.Data` | `Data []byte` | `raft.go` |
| `FSM.Apply` | `Apply(*LogEntry) any` — без изменений | `raft_fsm.go` |
| `BatchingFSM.ApplyBatch` | `ApplyBatch([]*LogEntry) []any` — без изменений | `raft_fsm.go` |
| `codec.EncodeCommand` | `func EncodeCommand(cmd Command) ([]byte, error)` | `pkg/raft/codec/command.go` |
| `codec.DecodeCommand` | `func DecodeCommand(data []byte) (Command, error)` | `pkg/raft/codec/command.go` |
| `kvservice.EncodeCommand` | `func EncodeCommand(cmd Command) ([]byte, error)` | `pkg/kvservice/command_codec.go` |
| `kvservice.DecodeCommand` | `func DecodeCommand(data []byte) (Command, error)` | `pkg/kvservice/command_codec.go` |

Владение байтами:

- `Apply` копирует вход до публикации записи (`cloneLogData`,
  `raft_cm_leader.go`), сохраняя различие между `nil` и пустым срезом,
  отличным от `nil`. До возврата `Apply` вызывающий код не должен изменять буфер; после
  возврата его можно изменять и использовать повторно, даже если future ещё не завершён.
- Опубликованные байты неизменяемы: журнал, транспорт, FSM и потребитель
  `CommitChannelFSM` их только читают; для изменения нужна собственная
  копия (`raft_fsm.go`, `raft.go`).
- Журнал копирует вход в кадр и возвращает при чтении отдельные байты
  (`pkg/raft/store/log_format.go`, `cloneBytes`). TCP-приём создаёт
  собственные байты получателя (`appendEntriesFromWire`,
  `pkg/raft/transp/append_entries_wire.go`). Транспорт в памяти глубоко
  копирует записи и `Data` перед доставкой (`copyAppendEntriesArgs`,
  `pkg/raft/transp/transport_inmem.go`).

Смысл `nil` и пустого среза. Для Raft это разные допустимые данные
записи `LogCommand`; смысл задаёт тип записи, а не длина. Вступительная
запись лидера (`LogNoop`) создаётся с `nil` и в FSM не попадает.
Записи конфигурации несут готовые байты `EncodeConfiguration`
(`configuration.go`) и в декодер команд KV не передаются. FSM KV-сервиса
считает и `nil`, и пустой срез в записи `LogCommand` повреждённой
командой (раздел 5).

## 3. Формат команды KVC1 версии 1

Пакет `pkg/raft/codec` зависит только от стандартной библиотеки и не знает
типов сервиса, журнала или транспорта (`pkg/raft/codec/command.go`).
Тип `codec.Command` — поля команды без результата:
`Kind uint8`, `ID int`, `Key`, `Value`, `CompareValue string`.

Заголовок — 40 байт, целые числа в порядке big-endian:

| Смещение | Размер | Поле | Значение |
|---|---|---|---|
| 0 | 4 | magic | `"KVC1"` |
| 4 | 1 | version | `1` |
| 5 | 1 | kind | `1` Get, `2` Put, `3` CAS, `4` Delete |
| 6 | 2 | reserved | `0` |
| 8 | 8 | ID | `int64` |
| 16 | 8 | длина Key | `uint64` |
| 24 | 8 | длина Value | `uint64` |
| 32 | 8 | длина CompareValue | `uint64` |
| 40 | — | строки | байты Key, Value, CompareValue подряд |

- Все три строки записываются всегда, даже если операция использует не
  все поля. Кодировка UTF-8 не проверяется.
- Поля результата (`ResultValue`, `ResultFound` у `kvservice.Command`) не
  входят в формат: их вычисляет FSM.
- Декодер проверяет всё до выделения памяти под строки: длину заголовка,
  magic, версию, вид, резерв, представимость ID в `int`, каждую длину
  относительно оставшихся байтов и отсутствие лишних байтов в конце.
  Строки копируются, поэтому результат не ссылается на вход.
- Отдельного предела размера команды нет; размер считается без
  переполнения (`encodedCommandSize`). Пределы внешних кадров не
  менялись.

Ошибки кодека (английские строки из кода):

| Ситуация | Текст |
|---|---|
| Кодирование неизвестного вида | `codec: unsupported command kind <n>` |
| Размер не помещается в `int` | `codec: command encoding size overflows int` |
| Любая ошибка декодирования | начинается с `codec: invalid command encoding:` и уточнения: `<n> bytes is shorter than the 40-byte header`, `bad magic`, `unsupported version <n>`, `unsupported command kind <n>`, `reserved field is <n>, want 0`, `ID <n> does not fit in int`, `string <i> length <n> exceeds remaining <m> bytes`, `<n> trailing bytes` |

Адаптер KV-сервиса (`pkg/kvservice/command_codec.go`) переводит
`kvservice.Command` в `codec.Command` и обратно. Вид проверяется до
сужения числа: значение вне `CommandGet…CommandDelete` (например, 257)
отвергается с ошибкой `kvservice: unsupported command kind <n>`, а не
усекается до поддерживаемого вида.

## 4. Журнал на диске

Файл журнала `log.dat` в каталоге данных узла
(`pkg/raft/store/file_storage.go`); формат версии 3 описан в начале
`pkg/raft/store/log_format.go`:

- заголовок файла 24 байта: magic `"RAFTPST\0"`, version `3`, размер
  заголовка, резерв, CRC-32C;
- пакеты: заголовок 32 байта с magic `"RLB3"`, тело с записями
  (index, term, type, dataLength, Data), завершающая часть размером 12 байт с CRC-32C и
  маркером `"RLCMIT3\0"`.

`Data` записи — представление версии 1: байт версии `1`, байт тега, затем
полезная нагрузка (payload).

| Тег | Значение | Правило |
|---|---|---|
| `1` | `nil []byte` | payload пуст |
| `2` | не-`nil` `[]byte` | пустой (не `nil`) срез записи любого типа либо непустые байты записи, отличной от `LogCommand` |
| `3` | байты команды | только запись `LogCommand` с непустым payload |
| `0`, `4` | зарезервированы | кодировщик их не записывает, декодер отвергает |

Кодирование каноническое (`encodeLogData`): `nil` — тег 1, непустые байты
`LogCommand` — тег 3, остальное — тег 2. Поэтому живые и восстановленные
байты сравниваются побайтово, а повтор уже записанного суффикса после
перезапуска не пишет новых байтов (`encodedSuffixEqual`,
`pkg/raft/store/log_storage.go`). Различие `nil` и пустого среза видно на
диске и сохраняется после перезапуска. Байты тега 3 журнал не разбирает:
их проверяет FSM.

Отказы декодера `decodeLogData` (все — повреждение журнала,
`log corruption`): данные короче двух байт, неизвестная версия Data,
теги 0, 4 и любые неизвестные, лишние байты у тега 1, пустой тег 3,
тег 3 у записи не `LogCommand`, непустой тег 2 у `LogCommand`. Резервного декодирования
через gob или произвольные Go-типы нет.

Файл версии, отличной от 3 (в том числе журнал версии 2), отвергается
до изменения файла (`checkLogFileHeader`: `unsupported log version <n>`);
`NewFileStorage` завершает процесс через `log.Fatalf` с префиксом
`FileStorage:` (`pkg/raft/store/file_storage.go`). Автоматической
конвертации нет. `FileStorage` и `MapStorage` используют одни и те же
процедуры кодирования Data.

## 5. Ошибка команды в FSM KV-сервиса

FSM (`KVService.Apply`, `pkg/kvservice/kvservice.go`) пропускает записи
не `LogCommand` и строго декодирует байты команды. Если байты не
декодируются:

1. каждый узел (лидер и ведомые) безусловно, независимо от уровня
   трассировки и `-stats-output`, пишет в стандартный журнал строку
   `kvservice: corrupt command index=<index> term=<term> reason=<reason>`;
   причина — текст ошибки декодера, без payload, ключа и значений;
2. результатом записи становится ошибка, обёртывающая
   `kvservice: corrupt command in log` с индексом и причиной; `DataStore`
   этой записью не меняется;
3. Raft считает запись успешно зафиксированной и применённой:
   `ApplyFuture.Error()` равен `nil`, ошибка находится в
   `ApplyFuture.Response()` (`raft_cm_apply.go`);
4. HTTP-обработчик получает ответ, не являющийся `Command`, и возвращает
   `RespStatus = StatusInvalid` (существующая ветка, см.
   [API-HTTP.md](API-HTTP.md));
5. следующие записи и следующие элементы пакета `ApplyBatch`
   применяются обычным порядком; продвижение применённого индекса не
   останавливается.

Если `EncodeCommand` отвергает команду ещё в HTTP-обработчике, клиент
получает HTTP 400, а `Apply` не вызывается (`applyCommand`). Штатные
обработчики формируют только поддерживаемые виды команд.

Ограничение. Продолжение после ошибки команды безопасно только при
одной сборке на весь кластер: все исправные узлы одинаково не применяют
одни и те же ошибочные байты. Строка журнала обнаруживает событие, но не
предотвращает расхождение состояния при локальной порче, не пойманной
CRC.

## 6. Сеть: AppendEntriesBytesV1

TCP-транспорт по-прежнему использует gob-конверт RPC (`tcpRPCRequest`,
`pkg/raft/transp/transport_tcp.go`) с прежними пулами соединений,
таймерами и ответами. Только запрос AppendEntries передаётся в частном
сетевом представлении (`pkg/raft/transp/append_entries_wire.go`):

- gob-имя `github.com/vskurikhin/raft.AppendEntriesBytesV1`;
- поле `EncodingVersion uint8 = 1`;
- все поля запроса (`RPCHeader`, `Term`, `LeaderID`, `PrevLogIndex`,
  `PrevLogTerm`, `LeaderCommit`) и у каждой записи `Index`, `Term`,
  `Type`, `DataPresent bool`, `Data []byte`.

Признак присутствия нужен потому, что gob не отличает пустой срез от
`nil`. Правила:

| Отправка | Приём |
|---|---|
| `DataPresent = (Data != nil)` | `false` и длина 0 → `nil` |
| | `true` и длина 0 → `make([]byte, 0)` |
| | `true` и длина > 0 → собственная копия байтов |
| | `false` и длина > 0 → ошибка |

Сетевой payload команды равен payload тега 3 на диске. KVC1 транспорт не
разбирает.

Отказы на приёме возникают в `handleCommand` до обработчиков Raft,
быстрого пути пульса и канала потребителя:

| Ситуация | Текст ошибки |
|---|---|
| Пришёл не `AppendEntriesBytesV1`, в том числе прежний `AppendEntriesArgs` (значение или указатель) | `raft: unsupported AppendEntries wire type <T>` |
| `EncodingVersion` 0 или неизвестная | `raft: unsupported AppendEntries wire version <n>` |
| `DataPresent = false` при непустых `Data` | `raft: inconsistent AppendEntries Data presence in entry <index>` |

`handleConn` пишет `raft: handle conn error: <ошибка>` и закрывает
соединение; ответ `AppendEntriesReply` с `Success=false` не
отправляется. Корректное представление переводится в
`*contract.AppendEntriesArgs` до проверки быстрого пути пульса.

Метки gob-регистрации — ровно 11
(`pkg/raft/transp/testdata/wire_labels.txt`). Метка
`github.com/vskurikhin/raft.AppendEntriesArgs` сохранена только для того,
чтобы распознать и явно отвергнуть прежнее представление; регистрация не
означает совместимости. Остальные девять меток не менялись.

`contract.ProtocolVersion` остаётся `3`: несовместимость выражена новым
gob-именем и версией представления, а не общим номером протокола.
Согласования версий при соединении нет, смешанный кластер не
поддерживается.

## 7. Что осталось в gob

Изменение устранило сериализацию полей команды через gob внутри `Data`. Без изменений остались:

| Данные | Формат | Файл |
|---|---|---|
| Конверт RPC по TCP (`tcpRPCRequest`, ответы, остальные запросы) | gob | `pkg/raft/transp/transport_tcp.go` |
| Снимок KV (`map[string]string`) | gob | `pkg/kvservice/datastore.go` |
| Метаданные снимка, `Version = contract.ProtocolVersion` (3) | gob | `pkg/raft/store/file_snapshot_store.go` |
| Постоянные скаляры (`currentTerm`, `votedFor`, `lastSnapshotIndex`, `lastSnapshotTerm`) | gob(int) внутри кадра `FileStorage` | `raft_cm_scalar_cache.go`, `pkg/raft/store/file_storage.go` |
| Конфигурация кластера | gob `EncodeConfiguration`; в журнале — тег 2 готовых байтов без повторной сериализации | `configuration.go` |

Новых номеров версий у этих форматов нет, потому что их байты не
менялись. Перенос не обещает полного устранения gob из узла.

## 8. Совместимость

- Контракт `Apply(any)` и `Data any` не поддерживается: прикладной код
  передаёт готовые байты.
- Узлы прежней и новой сборки несовместимы по AppendEntries и формату
  журнала; смешанный кластер не поддерживается. Переход — только на
  чистом кластере ([Data-v3-migration.md](Data-v3-migration.md)).
- Журнал версии 2 не читается; журнал исследовательской сборки с тегами
  0 или 4 тоже отвергается.

## Синхронизация версий

Русская версия — основная. Английская версия
[API-Data-v3-en.md](API-Data-v3-en.md) обновляется в той же задаче, что и
любое изменение фактов русской версии.
