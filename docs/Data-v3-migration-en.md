# Moving to the byte Data contract: clean cluster, backup, and rollback

## Purpose

This document describes moving a `raftkv` cluster to the build with the
byte `Data` contract ([API-Data-v3-en.md](API-Data-v3-en.md)). There is
no data conversion: the transition is performed on a clean cluster only,
and rollback requires restoring a saved
"executable + backup" pair.

Russian original — [Data-v3-migration.md](Data-v3-migration.md).

## 1. Why a clean cluster is required

- Builds before the transition (the release based on `b51e637`) write a
  version 2 log (`logFileVersion = 2` in `pkg/raft/store/log_format.go` of
  that version). The new build does not read a version 2 log:
  `NewFileStorage` terminates the process with the error
  `unsupported log version 2`, the file is not modified, and there is no
  automatic conversion.
- Logs of the research build with format version 3 may contain the
  interface Data tags 0 and 4. The new build rejects them as log
  corruption, so compatibility with the complete log is not guaranteed.
- The new AppendEntries network representation (`AppendEntriesBytesV1`)
  is incompatible with the previous one: a node of the new build closes
  a connection on which the former `AppendEntriesArgs` arrives. The
  `ProtocolVersion` number stays `3`, and there is no version negotiation.
  Therefore rolling one-node-at-a-time upgrades and mixed clusters are not
  supported.
- Snapshots of the previous cluster are not moved into the new
  directories: a snapshot of one moment must not be mixed with a
  different log.

## 2. Transition procedure

The commands below assume Linux nodes: they use `/proc`, `readlink -f`,
and `findmnt`.

1. Stop the load source and make sure it is not running
   (`pgrep -x loadkv` exits with code 1).
2. Before stopping the nodes, record on each node: the service PID, the
   SHA-256 of the executable actually running (`/proc/<PID>/exe`), its
   build-info (`go version -m`) and a copy taken from `/proc/<PID>/exe`
   itself, and the header version of every log file. The version is
   bytes 8–9 of `log.dat`, a big-endian `uint16`; for example,
   `od -An -tx1 -j8 -N2 <dir>/log.dat` prints `00 02` or `00 03`.
3. Stop all nodes. Upgrading only some of the nodes is not allowed.
4. Check the real paths of the data directories (`readlink -f`,
   `findmnt -T`); an unexpected path, symbolic link, or owner — stop.
5. Back up the data of all stopped nodes, the service configuration, and
   the previous executables to independent media. Verify SHA-256, the
   listing, and a trial extraction into an empty directory. The SHA of the
   saved executable must match the SHA from step 2, and the log header
   versions must match those read before the stop. A copy on the same
   media as the data does not count as a backup.
6. Clean only the confirmed data directories of each node, without
   general path patterns and without `make clean`. Record the listing of
   the empty directories with a timestamp — before the first start of the
   new build.
7. Install one and the same executable of the new build on all nodes and
   check its SHA-256 against the build manifest.
8. Start all nodes. Check the SHA of the executable actually running
   (`/proc/<PID>/exe`), a single leader, and the absence of log open and
   decode errors.
9. The node logs for the whole run must contain no
   `kvservice: corrupt command` lines and no version 3 log load errors.

The state accumulated before the transition is absent from the new
cluster: it remains only in the backup.

## 3. Rollback

1. Stop the load source and all nodes of the new build.
2. Separately save and verify the data and logs of the new build.
3. Restore the executables whose SHA was recorded in step 2 of the
   transition for this backup, and the matching service configuration
   files.
4. Restore the consistent backup of all nodes into the confirmed
   directories, and check SHA and permissions.
5. Start the cluster from this "executable + backup" pair.

If the executable SHA or the log versions do not match those recorded for
the backup, the rollback stops until the cluster owner decides; picking a
different executable is not allowed. The new build must not be started on
old data, and the old build must not be started on data of the new build.

**Data loss on rollback.** Restoring the old backup does not preserve the
entries the cluster received after the transition. There is no reverse
conversion of the new build's log; the new build's data are kept
separately for analysis only.

## 4. What is not supported

- Rolling upgrades and clusters of nodes running different builds.
- Reading a version 2 log and logs with Data tags 0 or 4.
- The `Apply(any)` contract: application code that calls `Server.Apply`
  directly passes ready-made bytes (`[]byte`), for example the result of
  `kvservice.EncodeCommand`.
- Moving old snapshots into the directories of the new cluster.

The format of KV snapshots, their metadata, and the persistent scalars did
not change (section 7 of [API-Data-v3-en.md](API-Data-v3-en.md)); this
does not make the old directories compatible, because the log next to the
snapshot is different.

## Version synchronization

The Russian version is the primary one. The English version is updated in
the same task as any change of facts in the Russian version
[Data-v3-migration.md](Data-v3-migration.md).
