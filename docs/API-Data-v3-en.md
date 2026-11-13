# Byte Data contract: KVC1 command, log v3, and AppendEntries

## Purpose

This document describes how log entry data (`LogEntry.Data`) travels from
the KV service HTTP handler to the state machine: the command format, the
on-disk log representation, the AppendEntries network representation, and
error behavior. The code is the primary source; source files are identified in the relevant sections. The procedure for moving an existing cluster is in
[Data-v3-migration-en.md](Data-v3-migration-en.md).

Russian original — [API-Data-v3.md](API-Data-v3.md).

## 1. Data path

```text
HTTP → kvservice: Command → EncodeCommand (once, KVC1 format)
     → Server.Apply([]byte) → ConsensusModule.Apply: own copy
     → LogEntry.Data []byte (immutable ready-made bytes)
       ├─ log: Data version 1 inside log file version 3
       └─ TCP: AppendEntriesBytesV1 carrying the same bytes
     → commit → KV service FSM → DecodeCommand → Command → DataStore
```

- All four HTTP command handlers (`POST /put/`, `/get/`, `/cas/`, `/delete/`) call a
  single function `applyCommand`, which encodes the command exactly once
  and passes the bytes to `Apply` (`pkg/kvservice/kvservice.go`).
- The root `raft` package, `pkg/raft/store`, and `pkg/raft/transp` do not
  import `pkg/kvservice` or `pkg/raft/codec` and do not parse KVC1: the
  log and the transport copy and frame the ready-made bytes rather than
  re-encoding command fields.
- There are no arbitrary Go types in `Data`: the common contract is
  `LogEntry.Data []byte` (`pkg/raft/contract/rpc.go`),
  `CommitEntry.Data []byte` (`raft.go`), `Apply([]byte, time.Duration)`
  (`server.go`, `raft_cm_leader.go`). The former `Apply(any)` and the
  command carrier are not supported.

## 2. Public API

| Symbol | Signature | File |
|---|---|---|
| `Server.Apply` | `func (s *Server) Apply(cmd []byte, timeout time.Duration) ApplyFuture` | `server.go` |
| `ConsensusModule.Apply` | `func (cm *ConsensusModule) Apply(command []byte, timeout time.Duration) ApplyFuture` | `raft_cm_leader.go` |
| `LogEntry.Data` | `Data []byte` | `pkg/raft/contract/rpc.go` |
| `CommitEntry.Data` | `Data []byte` | `raft.go` |
| `FSM.Apply` | `Apply(*LogEntry) any` — unchanged | `raft_fsm.go` |
| `BatchingFSM.ApplyBatch` | `ApplyBatch([]*LogEntry) []any` — unchanged | `raft_fsm.go` |
| `codec.EncodeCommand` | `func EncodeCommand(cmd Command) ([]byte, error)` | `pkg/raft/codec/command.go` |
| `codec.DecodeCommand` | `func DecodeCommand(data []byte) (Command, error)` | `pkg/raft/codec/command.go` |
| `kvservice.EncodeCommand` | `func EncodeCommand(cmd Command) ([]byte, error)` | `pkg/kvservice/command_codec.go` |
| `kvservice.DecodeCommand` | `func DecodeCommand(data []byte) (Command, error)` | `pkg/kvservice/command_codec.go` |

Byte ownership:

- `Apply` copies its input before publishing the entry (`cloneLogData`,
  `raft_cm_leader.go`), preserving the distinction between `nil` and a
  non-nil empty slice. Until `Apply` returns, the caller does not modify
  the buffer; after it returns, the buffer may be modified and reused even
  if the future has not completed yet.
- Published bytes are immutable: the log, the transport, the FSM, and the
  `CommitChannelFSM` consumer only read them; a consumer must make its own
  copy before modifying them (`raft_fsm.go`, `raft.go`).
- The log copies its input into a frame and returns separate bytes on
  read (`pkg/raft/store/log_format.go`, `cloneBytes`). TCP receive creates
  bytes owned by the receiver (`appendEntriesFromWire`,
  `pkg/raft/transp/append_entries_wire.go`). The in-memory transport
  deep-copies entries and `Data` before delivery (`copyAppendEntriesArgs`,
  `pkg/raft/transp/transport_inmem.go`).

Meaning of `nil` and an empty slice. For Raft these are different valid
`LogCommand` data; the meaning comes from the entry type, not the length.
The leader's no-op entry (`LogNoop`) is created with `nil` and never
reaches the FSM. Configuration entries carry the ready-made bytes of
`EncodeConfiguration` (`configuration.go`) and are not passed to the KV
command decoder. The KV service FSM treats both `nil` and an empty slice
in a `LogCommand` entry as a corrupt command (section 5).

## 3. KVC1 command format, version 1

Package `pkg/raft/codec` depends only on the standard library and knows
nothing of service, log, or transport types (`pkg/raft/codec/command.go`).
Type `codec.Command` holds the command fields without the result:
`Kind uint8`, `ID int`, `Key`, `Value`, `CompareValue string`.

The header is 40 bytes, integers are big-endian:

| Offset | Size | Field | Value |
|---|---|---|---|
| 0 | 4 | magic | `"KVC1"` |
| 4 | 1 | version | `1` |
| 5 | 1 | kind | `1` Get, `2` Put, `3` CAS, `4` Delete |
| 6 | 2 | reserved | `0` |
| 8 | 8 | ID | `int64` |
| 16 | 8 | Key length | `uint64` |
| 24 | 8 | Value length | `uint64` |
| 32 | 8 | CompareValue length | `uint64` |
| 40 | — | strings | bytes of Key, Value, CompareValue back to back |

- All three strings are always written, even if the operation does not
  use every field. UTF-8 is not validated.
- Result fields (`ResultValue`, `ResultFound` of `kvservice.Command`) are
  not part of the format: the FSM computes them.
- Before allocating memory for strings the decoder checks: header length,
  magic, version, kind, reserved field, representability of ID as `int`,
  each length against the remaining bytes, and the absence of trailing bytes. Strings
  are copied: the result does not reference the input.
- There is no separate command size limit; the size is computed without
  overflow (`encodedCommandSize`). Outer frame limits are unchanged.

Codec errors (English strings from the code):

| Situation | Text |
|---|---|
| Encoding an unknown kind | `codec: unsupported command kind <n>` |
| Size does not fit in `int` | `codec: command encoding size overflows int` |
| Any decoding error | starts with `codec: invalid command encoding:` followed by: `<n> bytes is shorter than the 40-byte header`, `bad magic`, `unsupported version <n>`, `unsupported command kind <n>`, `reserved field is <n>, want 0`, `ID <n> does not fit in int`, `string <i> length <n> exceeds remaining <m> bytes`, `<n> trailing bytes` |

The KV service adapter (`pkg/kvservice/command_codec.go`) converts
`kvservice.Command` to `codec.Command` and back. The kind is checked
before narrowing: a value outside `CommandGet…CommandDelete` (for example,
257) is rejected with `kvservice: unsupported command kind <n>` rather
than truncated to a supported kind.

## 4. On-disk log

The log file `log.dat` lives in the node's data directory
(`pkg/raft/store/file_storage.go`); the version 3 format is described at
the top of `pkg/raft/store/log_format.go`:

- a 24-byte file header: magic `"RAFTPST\0"`, version `3`, header size,
  reserved field, CRC-32C;
- batches: a 32-byte header with magic `"RLB3"`, a body of entries
  (index, term, type, dataLength, Data), and a 12-byte footer with
  CRC-32C and the marker `"RLCMIT3\0"`.

An entry's `Data` uses representation version 1: a version byte `1`, a
tag byte, then the payload.

| Tag | Meaning | Rule |
|---|---|---|
| `1` | `nil []byte` | empty payload |
| `2` | non-`nil` `[]byte` | an empty slice of any entry type, or non-empty bytes of an entry other than `LogCommand` |
| `3` | command bytes | only a `LogCommand` entry with a non-empty payload |
| `0`, `4` | reserved | never written by the encoder, rejected by the decoder |

Encoding is canonical (`encodeLogData`): `nil` is tag 1, non-empty
`LogCommand` bytes are tag 3, everything else is tag 2. Live and restored
bytes are therefore compared byte for byte, and re-appending an already
stored suffix after a restart writes no new bytes (`encodedSuffixEqual`,
`pkg/raft/store/log_storage.go`). The `nil`/empty distinction is visible
on disk and survives a restart. The log does not parse tag 3 bytes: the
FSM validates them.

Rejections by the `decodeLogData` decoder (all reported as
`log corruption`): data shorter than two bytes, an unknown Data version,
tags 0, 4, and any unknown tag, extra bytes after tag 1, an empty tag 3,
tag 3 on a non-`LogCommand` entry, a non-empty tag 2 on `LogCommand`.
There is no fallback to gob or arbitrary Go types.

A file whose version is not 3 (including a version 2 log) is rejected
before the file is modified (`checkLogFileHeader`:
`unsupported log version <n>`); `NewFileStorage` terminates the process
via `log.Fatalf` with the `FileStorage:` prefix
(`pkg/raft/store/file_storage.go`). There is no automatic conversion.
`FileStorage` and `MapStorage` share the same Data encoding procedures.

## 5. Command error in the KV service FSM

The FSM (`KVService.Apply`, `pkg/kvservice/kvservice.go`) skips
non-`LogCommand` entries and strictly decodes the command bytes. If the
bytes do not decode:

1. every node (leader and followers) unconditionally, regardless of the
   trace level and `-stats-output`, writes to the standard log the line
   `kvservice: corrupt command index=<index> term=<term> reason=<reason>`;
   the reason is the decoder error text, without payload, key, or values;
2. the entry's result becomes an error wrapping
   `kvservice: corrupt command in log` with the index and the reason; the
   `DataStore` is not modified by this entry;
3. Raft treats the entry as successfully committed and applied:
   `ApplyFuture.Error()` is `nil`, the error is in
   `ApplyFuture.Response()` (`raft_cm_apply.go`);
4. the HTTP handler receives a response that is not a `Command` and
   returns `RespStatus = StatusInvalid` (the existing branch, see
   [API-HTTP-en.md](API-HTTP-en.md));
5. subsequent entries and subsequent `ApplyBatch` elements are applied as
   usual; the applied index keeps advancing.

If `EncodeCommand` rejects a command already in the HTTP handler, the
client receives HTTP 400 and `Apply` is not called (`applyCommand`). The
standard handlers only build supported command kinds.

Limitation. Continuing after a command error is safe only with a single
build across the whole cluster: all healthy nodes equally skip the same
erroneous bytes. The log line detects the event but does not prevent
state divergence under local corruption not caught by CRC.

## 6. Network: AppendEntriesBytesV1

The TCP transport still uses the gob RPC envelope (`tcpRPCRequest`,
`pkg/raft/transp/transport_tcp.go`) with the same connection pools,
timers, and replies. Only the AppendEntries request is sent in a private
network representation (`pkg/raft/transp/append_entries_wire.go`):

- gob name `github.com/vskurikhin/raft.AppendEntriesBytesV1`;
- field `EncodingVersion uint8 = 1`;
- all request fields (`RPCHeader`, `Term`, `LeaderID`, `PrevLogIndex`,
  `PrevLogTerm`, `LeaderCommit`) and, per entry, `Index`, `Term`, `Type`,
  `DataPresent bool`, `Data []byte`.

The presence flag is needed because gob does not distinguish an empty
slice from `nil`. Rules:

| Send | Receive |
|---|---|
| `DataPresent = (Data != nil)` | `false` and length 0 → `nil` |
| | `true` and length 0 → `make([]byte, 0)` |
| | `true` and length > 0 → own copy of the bytes |
| | `false` and length > 0 → error |

The network command payload equals the tag 3 payload on disk. The
transport does not parse KVC1.

Receive-side rejections happen in `handleCommand` before the Raft
handlers, the heartbeat fast path, and the consumer channel:

| Situation | Error text |
|---|---|
| Not an `AppendEntriesBytesV1`, including the former `AppendEntriesArgs` (value or pointer) | `raft: unsupported AppendEntries wire type <T>` |
| `EncodingVersion` 0 or unknown | `raft: unsupported AppendEntries wire version <n>` |
| `DataPresent = false` with non-empty `Data` | `raft: inconsistent AppendEntries Data presence in entry <index>` |

`handleConn` logs `raft: handle conn error: <error>` and closes the
connection; no `AppendEntriesReply` with `Success=false` is sent. A valid
representation is converted to `*contract.AppendEntriesArgs` before the
heartbeat fast-path check.

There are exactly 11 gob registration labels
(`pkg/raft/transp/testdata/wire_labels.txt`). The label
`github.com/vskurikhin/raft.AppendEntriesArgs` is kept only to recognize
and explicitly reject the former representation; registration does not
imply compatibility. The other nine labels are unchanged.

`contract.ProtocolVersion` stays `3`: the incompatibility is expressed by
the new gob name and the representation version, not by the global
protocol number. There is no version negotiation on connect, and a mixed
cluster is not supported.

## 7. What remains in gob

The change removed gob only from command fields in `Data`. Unchanged:

| Data | Format | File |
|---|---|---|
| TCP RPC envelope (`tcpRPCRequest`, replies, other requests) | gob | `pkg/raft/transp/transport_tcp.go` |
| KV snapshot (`map[string]string`) | gob | `pkg/kvservice/datastore.go` |
| Snapshot metadata, `Version = contract.ProtocolVersion` (3) | gob | `pkg/raft/store/file_snapshot_store.go` |
| Persistent scalars (`currentTerm`, `votedFor`, `lastSnapshotIndex`, `lastSnapshotTerm`) | gob(int) inside a `FileStorage` frame | `raft_cm_scalar_cache.go`, `pkg/raft/store/file_storage.go` |
| Cluster configuration | gob `EncodeConfiguration`; in the log — tag 2 of ready-made bytes without re-serialization | `configuration.go` |

These formats have no new version numbers because their bytes did not
change. The change does not eliminate gob from the node entirely.

## 8. Compatibility

- The `Apply(any)` and `Data any` contract is not supported: application
  code passes ready-made bytes.
- Nodes of the previous and the new build are incompatible in
  AppendEntries and log format; a mixed cluster is not supported. The
  transition is done on a clean cluster only
  ([Data-v3-migration-en.md](Data-v3-migration-en.md)).
- A version 2 log cannot be read; a log of the research build containing
  tags 0 or 4 is rejected as well.

## Version synchronization

The Russian version is the primary one. The English version is updated in
the same task as any change of facts in the Russian version
[API-Data-v3.md](API-Data-v3.md).
