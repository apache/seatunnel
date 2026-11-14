# Experimental CDC Progress Contract

SeaTunnel CDC sources can expose their latest runtime progress through the experimental types in
`org.apache.seatunnel.api.cdc`. The contract describes what a connector reports; how an engine
collects, stores or exposes those reports is outside this document. It does not change checkpoint
or restore behavior and is not yet a stable connector API.

## Ownership

Readers and enumerators report different facts, and neither infers the other's state:

- A reader reports its lifecycle, active split, consumed position, the time that position last
  changed, and the latest source event time.
- An enumerator reports snapshot discovery and assignment status, split counts, remaining work, and
  bounded active-split details.

Completing split assignment does not prove that a reader has entered catch-up or incremental mode.

## Provider guarantees

A source opts in with the `SupportCdcProgress` marker. Its reader or enumerator then implements
`CdcProgressProvider#getCdcProgress()`, which must satisfy the following:

- **Non-blocking.** It returns a snapshot the connector already maintains. It never performs
  source, network, checkpoint or other blocking I/O, and never takes a lock that record processing
  or split assignment holds.
- **Thread-safe.** Collection can run concurrently with any reader or enumerator callback.
- **Immutable.** Every report and the values inside it are immutable. A report never references a
  connector's live offset object; coordinates are copied when the report is built.
- **Truthful.** A value is `EXACT` only when current runtime state proves it. Missing, stale,
  unsupported or approximate values are marked as such; they are never replaced with zero or a
  guess.
- **Nullable before the first report.** Returning `null` means no report is available yet.

Each `CdcProgressValue` carries its own accuracy:

| Accuracy | Meaning |
| --- | --- |
| `EXACT` | Current connector state proves the value without approximation. |
| `BEST_EFFORT` | A useful value is available, but exact precision is not guaranteed. |
| `UNSUPPORTED` | The connector cannot provide this value. |
| `UNAVAILABLE` | The value is supported but is not available for this observation. |

`EXACT` and `BEST_EFFORT` always carry a non-null payload; `UNSUPPORTED` and `UNAVAILABLE` never do.

## Positions

A `CdcProgressPosition` keeps the connector's native coordinates under an explicit position family
(for example `MYSQL_BINLOG`) and schema version. Consumers must not assume that keys from one
family apply to another, and must tolerate different keys within one family between a
`BEST_EFFORT` and an `EXACT` value: a configured timestamp start position carries only a timestamp
until the first consumed record replaces it with full native coordinates.

Position payloads contain only offset coordinates such as binlog positions, GTIDs, LSNs or
timestamps. They must never contain credentials, connection URLs or other authentication material.

The reader's consumed position is the latest position it has observed. It is not a completed
checkpoint position, and an unchanged position alone does not prove source lag, backpressure or a
stalled source.

## Reader lifecycle

`CdcProgressLifecycle` is derived from the split the reader is processing, so its values mean the
same thing in every startup mode:

| Lifecycle | Meaning |
| --- | --- |
| `UNKNOWN` | No split has been assigned yet. |
| `SNAPSHOT` | The reader is reading a snapshot split. The consumed position is `UNAVAILABLE`; split watermarks are reported by the enumerator. |
| `CATCH_UP` | The reader is consuming the change stream but has not yet passed the snapshot high watermarks it must honour. |
| `INCREMENTAL` | The reader is consuming only the change stream. |
| `COMPLETED` | The reader has finished every split it will receive and signalled that no more elements follow. Every reader of a bounded job reaches this stage; in an unbounded job, a reader that holds no change-stream split reaches it once the enumerator has nothing more to assign. |

### Expected reporting per startup mode

| Startup mode | Reader lifecycle sequence | Enumerator snapshot status | Consumed position before → after the first record |
| --- | --- | --- | --- |
| `initial` | `UNKNOWN` → `SNAPSHOT` → `CATCH_UP` → `INCREMENTAL` for readers that receive a change-stream split; `UNKNOWN` → `SNAPSHOT` → `COMPLETED` for the others when reader parallelism exceeds the incremental parallelism | `DISCOVERING` → `ASSIGNING` → `COMPLETED`; counts are retained during the incremental phase | `UNAVAILABLE` during snapshot → `BEST_EFFORT` (minimum snapshot watermark) → `EXACT` |
| `snapshot-only` | `UNKNOWN` → `SNAPSHOT` → `COMPLETED` | `DISCOVERING` → `ASSIGNING` → `COMPLETED` | `UNAVAILABLE` throughout |
| `earliest`, `latest`, `specific`, `timestamp`, `committed-offset` | `UNKNOWN` → `INCREMENTAL` | `NOT_APPLICABLE`; counts `UNSUPPORTED` | `BEST_EFFORT` (configured or resolved start offset) → `EXACT` |

`CATCH_UP` only appears after a snapshot phase. When the change-stream start offset already equals
the maximum snapshot high watermark the reader skips it and reports `INCREMENTAL` directly.

After a restore the lifecycle is derived from the restored split in the same way: an unfinished
snapshot split reports `SNAPSHOT`, a change-stream split reports `CATCH_UP` while it still carries
snapshot watermarks to pass and `INCREMENTAL` otherwise. A restored start position is `BEST_EFFORT`
until the first consumed record. The contract has no separate restoring state and does not report
the restore origin.

## Enumerator snapshot assignment

`CdcSnapshotAssignmentStatus` is owned by the enumerator:

| Status | Meaning |
| --- | --- |
| `NOT_APPLICABLE` | This startup mode has no snapshot phase. |
| `DISCOVERING` | Tables are still being discovered or chunked into splits. |
| `ASSIGNING` | Created splits are waiting for a reader. |
| `COMPLETED` | Every snapshot split has been assigned. The running count says how many are still being read. |

Assigned, completed, running, prepared-remaining and remaining-unchunked-table counts are reported
separately so that lazy chunk creation is not hidden behind one ambiguous remaining count. Present
counts are non-negative; `assigned == completed + running` is enforced only when all three are
`EXACT`. Active-split details are capped at `CdcEnumeratorProgressReport.MAX_ACTIVE_SPLITS`;
`activeSplitsTruncated` says whether details were dropped, while the counts still describe the
complete state.

## Current limitations

- The contract and report types are experimental and may change while connectors validate them.
- Checkpoint and restore positions are not part of the reader report.
- There is no universal numeric lag value; consumers compare native positions per family.
- This document does not cover how an engine collects or exposes reports.
