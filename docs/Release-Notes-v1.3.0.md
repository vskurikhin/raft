# Release Notes — v1.3.0

**Qualification draft; final release commit: PENDING OWNER.**

The P-PERF-3 milestone includes commits `b51e637` (defaults) and `5fcf815`
(byte Data contract and codec). The owner will create the final documentation
and qualification commit after the remaining gates. These notes do not declare
the milestone DONE or identify an unpublished commit as a released version.

Version 1.3.0 updates load and node defaults and introduces an explicit byte
contract for application commands. **The Go API, log format and AppendEntries
representation are incompatible with the preceding implementation. Read the
upgrade notes before using existing data or replacing any node.**

## Defaults and observability

| Setting | CLI default | Local Makefile stand |
|---|---|---|
| Load concurrency | 2048 | 2048 |
| Request rate | 600/s | 600/s |
| Key count / value size | 15000 / 1024 bytes | same |
| Weak get / strong get / delete | 70% / 5% / 0% | same |
| Verify | 5% | 5% |
| Duration | 0 (until signal) | 5m |
| Apply batch / reelection base / ticker | 50 / 750 / 20 ms | same |
| Heartbeat | 33 ms | 45 ms |

The node's connection pool default is 16. Node `-stats-output=false` controls
periodic publication independently of the trace level; it does not disable
metric collection. Trace defaults remain 1 for the node and 0 for the stand.
Flags and Makefile variables remain overridable. The stand heartbeat is a
separate profile, not the node default.

## Data API and command encoding

`Apply(any)` becomes `Apply([]byte)`, and `LogEntry.Data` and `CommitEntry.Data`
are byte slices. This is a source compatibility break for Go integrations.
`Apply` copies submitted bytes before publication; published command bytes are
read-only. Nil and non-nil empty slices remain distinct.

KV commands use the versioned KVC1 codec in `pkg/raft/codec`. The adapter in
`pkg/kvservice/command_codec.go` encodes once before submission and decodes on
FSM application. Raft, storage and transport carry command bytes without
interpreting their fields. The HTTP contract is preserved. See the
[Data API reference](API-Data-v3-en.md) for ownership, validation, and error
handling.

## Persistence and network formats

The journal uses format version 3 with byte Data tags 1, 2 and 3. Reserved
historical tags 0 and 4 and version 2 journals are rejected. There is no generic
gob fallback for application Data.

AppendEntriesBytesV1 carries the already encoded bytes and an explicit presence
indicator inside the outer RPC envelope. Mixed old/new node builds and rolling
upgrades across this change are unsupported. Disk compatibility and source API
compatibility are separate from network compatibility.

Gob remains in KV snapshots, snapshot metadata, scalar values, configuration
encoding and the outer RPC envelope. This change does not remove gob from the
whole product or change snapshot encoding.

## Warning: incompatible storage and upgrade notes

**Version 2 journals cannot be read by this version. There is no automatic
converter. Do not overwrite the old deployment or reuse its data directories
with the new executable.**

1. Stop the old deployment and preserve a verified, consistent backup of every
   node's data and snapshots together with the matching old executable.
2. Use clean directories and one consistent new build across the entire new
   cluster. A clean cluster does **not** transfer old user data; retaining that
   data needs a separately planned migration procedure.
3. Update Go integrations for `Apply([]byte)` and the byte Data fields. Follow
   the [migration guide](Data-v3-migration-en.md); HTTP clients retain their
   existing contract.
4. Roll back by restoring the old executable and its matching backup as a pair.
   The new data directory is not an input to the old build. **Rollback loses
   writes made after the backup.**

## Measured results and limits

These are historical results from the closed milestone stages, transcribed
without new measurements or recalculation. Detailed run passports and evidence
remain in the internal stage archives.

### Baseline at 1000 requests/s

Stage 2, RUN `p3s2-run5` A@1000: C_cluster = **0.001229265 CPU-s per successful
operation**, N_success = **303685**, mean RPS = **1012.31**. This is the separate
baseline, not the historical A/A control at 3000 requests/s. Historical VERIFY
bad/dropped observations were recorded and ignored under that methodology.

### Historical Data hypothesis

Stage 3, diagnostic D run No. 8 relative to A controls No. 3 / No. 7 reported
f **−26.8% / −28.2%**, U **−27.9% / −29.2%**, C **−4.04% / −2.80%**.
The numerical reductions exceed the registered thresholds of 14% for f/U and
2.5% for C diagnostically. The controls failed their original comparability
limit: **rel(C)=1.28% > 1%; INCOMPARABLE_CONTROL**. The C margin against No. 7
was only 0.298 percentage points. These results are not a machine PASS.

On 2026-10-30 the owner confirmed H-PERF3-DATA-1 by D with limitations. Goal 6
is therefore achieved with caveats, on an owner decision distinct from the
machine verdict. Incomplete symbolization can lead to an earlier strict
INCONCLUSIVE result. Controls are not averaged and drift is not subtracted.

Q-P3-09 is CLOSED by the owner's 2026-11-12 decision. Its **1.5% limit applies
only to future comparisons**, after an explicit methodology/ADR and independent
approval before measurements. It does not apply retroactively to the historical
1.28%; the original 1% limit and INCOMPARABLE_CONTROL remain unchanged.

Goal 7 is **achieved with caveats**: the byte contract, codec and clean migration
were implemented and documented. The original goal named `pkg/kvservice`; the
approved implementation places the codec in `pkg/raft/codec` with the KV adapter
in `pkg/kvservice`. Snapshots remain gob. Production CPU acceleration against
the stage 2 baseline was **not measured**; production CPU and allocations remain
**UNKNOWN**. Historical D is not evidence of a measured CPU reduction in the
production build.

### Goal 4: one production throughput run

Stage 5, RUN `p5-20261112-1`: **PASS**, COMPLETE, VALID. The recorded criterion
was **total=905280 ≥ 894000=3000×298**, using **298 accounted seconds** from
the recorded final RPS line, during a full **5m** run. The displayed mean was
3037.9. No division by nominal 300 seconds replaces the recorded criterion.

This used the fast stand (vm11/12/13, generator vm03), production build after
stage 4 and accumulated v3 data after smoke. One generator used concurrency
2048, 15000 keys, 1024-byte values, weak-get/get/put/delete 70/5/25/0%, verify
5%, and request-rate 3000/s. The r6 node profile was heartbeat/reelection/batch/
ticker 33/750/50/20 ms, max-pool 16, trace 3, stats enabled, mutex 5, block 0,
GOGC=400, GOMEMLIMIT=2GiB and gctrace=1.

The metric counts **all completed operations, including failures**, rather than
successful operations alone. Zero fail/bad counters in this run do not change
that definition. One run does not establish sustainable throughput in all
conditions, service correctness, CPU improvement, or run-to-run variability.

### Remaining uncertainty

- Run-to-run variability, the effect of accumulated data, and a stable margin
  remain UNKNOWN. Historical D used clean v3 data while A/B/C had accumulated
  data; the effects were not separated. The series had a fixed order and one D.
- The cause of control CPU drift remains UNKNOWN. VM noise is an assumption;
  contention, steal time and frequency telemetry were not collected.
- Production CPU, allocations, network bytes and the cost of copies/GC remain
  UNKNOWN. The entire observed difference cannot be attributed to gob.
- Role coverage remains UNKNOWN: the undefined share was large and a separate
  interval role report was not supplied. Whole-run stability cannot reconstruct
  interval roles or rule out hidden transitions.
- Historical VERIFY bad causes remain UNKNOWN; zero counts in smoke/P5 do not
  explain them. Incomplete symbolization and diagnostic f/U are not a strict
  confirmed tool sum.
- Historical DURATION_DEVIATION for No. 7/8 remains. Complete output/exit codes
  of earlier compare invocations were not supplied; completion flags and the
  old generator's synthetic timeline do not prove end acknowledgements or
  post-window mutex collection.
- Independence of pprof samples and sensitivity transfer to 3000 requests/s
  remain assumptions, not established statistical power.
- P5 p80 is UNKNOWN (the summary has p50/p90/p95/p99). Empty signal records do
  not exclude a direct signal to the generator; the owner's self-completion
  testimony was accepted. The initial failed HTTP probe, recovered free-space
  check and tar exit code 1 remain in the historical record.

## Validation

**Final stage 6 verification: PENDING_VERIFICATION.** Earlier stage 5 logs dated
2026-11-12 report rc=0 for formatting, vet, build, tests, race and lint, but do
not close the final stage 6 gate. That vet invocation omitted the root package.
The referenced stage 5 formatting log is missing; its outcome is available only
in the verification report. These are historical records, not fresh runs.

| Final check | Status | Full log / exit code |
|---|---|---|
| gofmt of working Go files | PENDING_VERIFICATION | pending / unknown |
| `go vet . ./cmd/... ./internal/... ./pkg/...` | PENDING_VERIFICATION | pending / unknown |
| `go build ./...` | PENDING_VERIFICATION | pending / unknown |
| `go test -count=1 . ./pkg/...` | PENDING_VERIFICATION | pending / unknown |
| `go test -race -count=1 . ./pkg/...` | PENDING_VERIFICATION | pending / unknown |
| `golangci-lint run --verbose` (v2.12.2) | PENDING_VERIFICATION | pending / unknown |
| `make test-stress COUNT=10` | DONE | rc=0 / milestone archive |

One `make test-stress COUNT=10` invocation was performed on 2026-11-12 under the
owner's explicit single-run authorization. It completed in 45 min 33 s with
rc=0; every package of the `RACE_PKGS` scope (`.`, `./pkg/...`,
`./internal/tracelog`) passed ten `-race` repetitions. The full output, exit
code and metadata are retained in the milestone archive (internal
`.ai/developer/TASK-1015/stress/run-1/`, not linked here). The recipe sets a
60-minute timeout per package process; approximately one hour is an expectation,
not a bound on the whole invocation. A successful run cannot prove the absence
of rare defects; a failure, including timeout, would have required investigation
rather than a rerun.

Updates applied as a controlled documentation delta (including the stress
outcome) require a repeat reviewer check of the factual delta against full
logs, a file/SHA-256 list, and integrator verification of the final manifest.
Only this controlled documentation delta preserves Go-run reuse; code, test,
dependency or build-configuration changes invalidate it. Release approval and
the final commit remain with the owner.

## Licensing

Apache License 2.0 remains in effect; this version introduces no new licensing
change. See [LICENSE](../LICENSE) and [NOTICE](../NOTICE) for terms and upstream
provenance. Previously published code and third-party components retain their
respective applicable terms.
