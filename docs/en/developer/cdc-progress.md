# Experimental CDC Progress Contract

SeaTunnel CDC sources can expose their latest runtime progress through the experimental types in
`org.apache.seatunnel.api.cdc`. This contract is intended for engine observability. It does not
change checkpoint or restore behavior and is not yet a stable connector API.

## Ownership

Reader and enumerator reports contain different facts:

- A reader reports its lifecycle, active split, consumed position, position-change time, source
  event time, completed-checkpoint position, and restored position.
- An enumerator reports snapshot discovery and assignment state, split counts, remaining work, and
  bounded active-split details.

An enumerator must not infer reader lifecycle. In particular, completing split assignment does not
prove that readers are in catch-up or incremental mode.

## Provider contract

`CdcProgressProvider#getCdcProgress()` returns an immutable snapshot that the connector already
maintains. Implementations must be thread-safe and non-blocking. The method must not perform source,
network, checkpoint, or other blocking I/O. It may return `null` before a report is available.

Each `CdcProgressValue` describes one fact independently:

| Accuracy | Meaning |
| --- | --- |
| `EXACT` | Current connector state proves the value without approximation. |
| `BEST_EFFORT` | A useful value is available, but exact precision is not guaranteed. |
| `UNSUPPORTED` | The connector or current lifecycle wiring cannot provide the value. |
| `UNAVAILABLE` | The value is supported but is not available for this observation. |

Supported values carry a non-null payload. Unsupported and unavailable values do not carry a
payload. Connector-native positions keep an explicit position family and schema version; consumers
must not assume that fields from one connector apply to another. Position payloads must contain
only offset coordinates such as binlog positions, GTIDs, LSNs, or timestamps. They must never
contain credentials, connection URLs, or other authentication material.

## Runtime collection

Reader reports are sampled on execution members, batched, and sent to the active coordinator.
Enumerator reports use a separate coordinator-owned collection path. The active coordinator derives
enumerator task group locations from running job plans and coordinator-owned slot assignments. It
requests reports from the assigned members, including itself when applicable, and writes accepted
reports to the coordinator-side latest-only store.

Sources opt in through `SupportCdcProgress`. The coordinator filters for this capability before
reading slot assignments, so non-CDC enumerators are not polled. Collection follows the existing
`print-execution-info-interval` monitor cadence and allows at most one outstanding request per
worker. A failed provider is isolated from other tasks and a failed collection does not stop later
monitor ticks. Providers publish immutable snapshots; polling does not acquire the chunk-generation
monitor or initiate source I/O.

During a mixed-version deployment, a member that does not recognize the CDC progress operation can
reject collection requests. A failed worker request logs a warning; later monitor ticks can retry
after the in-flight request finishes. Task groups are batched by worker, so these warnings are per
failed worker request, not per pipeline. This describes progress-collection failure handling only,
not a general guarantee of rolling-upgrade compatibility.

Enumerator tasks can be placed on a member other than the active coordinator. This transport detail
does not transfer ownership to the worker sampler: the coordinator selects the enumerators to poll,
initiates collection, and owns ordering and storage. After master failover, recovered job masters and
slot assignments rebuild the collection set.

Every accepted report carries task identity, source vertex identity, execution attempt, an
attempt-local sequence, and observation time. Relative to the stored report for the same owner and
task, lower attempts and non-increasing sequences within an attempt are ignored. This ordering is
not a check against an authoritative deployment identity: an empty, reopened scope can receive an
older reader observation before its first newer report. Reader task details remain separate;
reports from parallel readers are not treated as one atomic distributed snapshot.

## Lifecycle and cleanup

Reader lifecycle is one of `SNAPSHOT`, `CATCH_UP`, `INCREMENTAL`, or `UNKNOWN`. Enumerator snapshot
assignment is reported separately as `NOT_APPLICABLE`, `DISCOVERING`, `ASSIGNING`, or `COMPLETED`.

The latest report store keeps no history. Reports are removed when the owning pipeline is cleaned
up, including savepoint completion. Only registered live pipelines accept reports; late reports
cannot reopen a removed scope. Master cleanup invalidates the coordinator generation as well as
clearing its scopes; registrations and enumerator callbacks captured by the old generation cannot
publish into the new one. Failed initialization rolls back only scopes owned by that JobMaster.
Scopes are rebuilt before pipeline deployment. Completed-checkpoint and restored-position facts
remain `UNSUPPORTED` until their corresponding lifecycle callbacks prove them. The current consumed
position is not a completed-checkpoint position, and normal split assignment does not prove restore
origin.

Reader progress samples detached offset coordinates after successful processing, at most once per
second unless the split or reader lifecycle changes or this is its first successful emission.
The sampling budget uses a monotonic clock; report timestamps remain epoch timestamps. Polling never
reads a connector's mutable offset object. A later in-place update or failed emission cannot advance
an already published consumed position. On a partially failed batch, the last verified sample is
retained; it need not include every successful record before the failure. An exact coordinate is
evidence of that sampled successful observation, not a freshness or checkpoint guarantee. If no later
record succeeds, the sample can remain stale indefinitely. `lastPositionChangeAt` records when a
sampled position change was observed. Coordinate copying is limited to selected samples, while a
small sampling check remains on the record path.

Enumerator count mismatches are reported as `BEST_EFFORT` rather than changing assignment or
checkpoint behavior. Ordinary diagnostic-conversion failures publish unavailable counts instead of
presenting an older exact assignment snapshot as fresh; they do not fail assignment or restore.
The remaining-unchunked-table count includes a table currently being chunked, without retaining it
in the assigner's production queue.

## Current limitations

- The contract and report types are experimental.
- MySQL CDC, PostgreSQL CDC, Oracle CDC, SQL Server CDC, DB2 CDC and MongoDB CDC
  currently inherit the progress-provider wiring from `connector-cdc-base`. MySQL uses an explicit
  `MYSQL_BINLOG` position family; the other base connectors use their plugin name until a more specific
  position family is defined. This list describes provider wiring, not equal coordinate precision or
  completed end-to-end validation for every connector. CDC sources without this provider wiring,
  including TiDB CDC and Vitess CDC, return no report.
- Snapshot-only, initial snapshot followed by incremental, and incremental-only modes use the same
  provider contract. A configured or restored starting position is `BEST_EFFORT` until a successful
  emission establishes consumption evidence; it is not a restored-position lifecycle report.
- Enumerator reports retain at most 100 active-split details. `activeSplitsTruncated` indicates that
  additional active splits were omitted; aggregate split counts still describe the complete state.
- Internal diagnostic frames allow at most 100,000 report/task-group entries and 1,024 fields in a
  native position. Oversized or malformed frames are rejected; these bounds do not change source,
  checkpoint, or savepoint state.
- This slice does not expose progress through REST, the CLI, or metrics.
- Completed-checkpoint and restored positions remain unsupported until their engine lifecycle
  callbacks are connected.
- An unchanged position alone does not prove source lag, backpressure, or a stalled source.
