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

Enumerator tasks can be placed on a member other than the active coordinator. This transport detail
does not transfer ownership to the worker sampler: the coordinator selects the enumerators to poll,
initiates collection, and owns ordering and storage. After master failover, recovered job masters and
slot assignments rebuild the collection set.

Every accepted report carries task identity, source vertex identity, execution attempt, an
attempt-local sequence, and observation time. Reports from older attempts or older sequences are
ignored. Reader task details remain separate; reports from parallel readers are not treated as one
atomic distributed snapshot.

## Lifecycle and cleanup

Reader lifecycle is one of `SNAPSHOT`, `CATCH_UP`, `INCREMENTAL`, or `UNKNOWN`. Enumerator snapshot
assignment is reported separately as `NOT_APPLICABLE`, `DISCOVERING`, `ASSIGNING`, or `COMPLETED`.

The latest report store keeps no history. Reports are removed when the owning pipeline is cleaned
up. A position must remain `UNSUPPORTED` until the corresponding lifecycle proves it. For example,
the current consumed position is not a completed-checkpoint position, and normal split assignment
does not prove restore origin.

## Current limitations

- The contract and report types are experimental.
- CDC sources based on `connector-cdc-base` currently provide reports. MySQL uses an explicit
  `MYSQL_BINLOG` position family; other base connectors use their plugin name until a more specific
  position family is defined. CDC sources without this provider wiring return no report.
- Enumerator reports retain at most 100 active-split details. `activeSplitsTruncated` indicates that
  additional active splits were omitted; aggregate split counts still describe the complete state.
- This slice does not expose progress through REST, the CLI, or metrics.
- Completed-checkpoint and restored positions remain unsupported until their engine lifecycle
  callbacks are connected.
- An unchanged position alone does not prove source lag, backpressure, or a stalled source.
