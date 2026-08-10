# Worker Node Resource View

## Status

This page is the proposed design contract for [issue #11665](https://github.com/apache/seatunnel/issues/11665). The first delivery should turn already-collected per-node data into a usable cluster resource view on the existing Workers/Master pages, without introducing new scheduling state.

The design is intentionally bounded:

- reuse the existing `/system-monitoring-information` payload as the source of per-node JVM/host metrics
- reuse the resource manager's existing live `WorkerProfile` state as the source of per-worker slot accounting
- add one new lightweight, read-only REST projection; add no new mutable state and change no scheduling decision logic
- defer per-worker task-assignment drill-down (correlating with `/trace/task-mapping`) to a follow-up, since that endpoint is scoped per-job and the Workers page has no job context

## Problem

The Workers/Master page today (`seatunnel-engine-ui/src/views/managers/index.tsx`) renders only 4 columns - Host, Port, Physical MEM Total, and Heap MEM Used - out of roughly 35 fields already returned by `/system-monitoring-information` (modeled in `seatunnel-engine-ui/src/service/manager/types.ts`). An Action column exists in the source but is commented out.

Separately, there is no per-worker slot view anywhere. `OverviewInfo` (`GET /overview`) only exposes cluster-wide `totalSlot`/`unassignedSlot` integers. An operator cannot tell, from the UI, which worker has free slots, which is fully allocated, or whether cluster capacity is unevenly distributed.

## Existing Foundation

| Area | Existing contract |
|---|---|
| Per-node JVM/host metrics | `GET /system-monitoring-information` returns one `Monitor` row per node (CPU load, heap/physical memory, GC counts and time, thread count, executor queue sizes, and more). |
| Cluster-wide slot totals | `GET /overview` returns `OverviewInfo.totalSlot` / `unassignedSlot` as cluster-wide sums only. |
| Per-worker live resource state | `ResourceManager.getRegisterWorker()` returns a live `ConcurrentMap<Address, WorkerProfile>`. Each `WorkerProfile` already carries `assignedSlots` / `unassignedSlots` (`SlotProfile[]`), `dynamicSlot`, `attributes` (worker tags), and a `systemLoadInfo` (`cpuPercentage`, `memPercentage`) used internally by the scheduler. This is the canonical source the resource manager itself uses to accept or reject allocation requests - not a derived approximation. |
| Per-job task-to-worker mapping | `GET /trace/task-mapping/:jobId` already computes task/host assignment for one job, but has no UI consumer and no cluster-wide (cross-job) shape. |
| Cross-node read pattern | `GetOverviewOperation` already shows the established pattern for a REST call to reach live master-held state: `OverviewService` checks whether the local node is the active master (`getSeaTunnelServer(true)`) and, if not, forwards via `NodeEngineUtil.sendOperationToMasterNode(...)`. |

## Goals

1. Render a curated, operationally useful set of the already-fetched `/system-monitoring-information` fields as table columns, instead of today's 4.
2. Give access to the *full* raw payload without turning the table into an unreadable ~35-column grid.
3. Add a per-worker slot view (total / used) sourced from the resource manager's own live state, with no new bookkeeping.
4. Keep the view useful when the cluster is idle (no jobs running).
5. Keep refresh cost bounded and consistent with the existing polling model.

## Non-Goals

- Per-worker "tasks currently running here" drill-down via `/trace/task-mapping`. That endpoint is scoped per-job; building a cluster-wide, cross-job view needs its own fan-out design and is left for a follow-up once this view's contract is settled.
- Historical or long-term retention of node resource metrics.
- Cluster capacity planning or autoscaling recommendations.
- Per-node log tailing (already covered by the existing worker log viewer).

## Resource Model

### Per-Node Monitoring Fields (reuse, no backend change)

No new backend work is needed here: the fields already exist in `Monitor`. The V1 table should surface a curated subset instead of all ~35 fields:

| Column | Existing `Monitor` field(s) |
|---|---|
| Host / Port | `host`, `port` |
| Role | derived from `isMaster` (already used to split Master vs. Worker pages) |
| CPU Load | `load.systemAverage` |
| Heap Used / Max | `heap.memory.used`, `heap.memory.max` |
| Physical MEM Total | `physical.memory.total` |
| GC (minor/major) | `minor.gc.count`, `major.gc.count` |
| Thread Count | `thread.count` |

The remaining fields stay available, not discarded: a "View Details" action (the column already present but commented out in `managers/index.tsx`) opens a drawer showing the complete raw `Monitor` row as key/value pairs, plus the new per-worker slot/attribute data below. This avoids a wide, hard-to-scan table while keeping every existing field reachable.

### Per-Worker Slot State (new REST projection, no new state)

A new endpoint projects the resource manager's existing live state, one row per registered worker:

| Field | Source |
|---|---|
| `address` | `WorkerProfile.address` |
| `totalSlot` | `assignedSlots.length + unassignedSlots.length` |
| `usedSlot` | `assignedSlots.length` |
| `dynamicSlot` | `WorkerProfile.dynamicSlot` |
| `cpuPercentage` / `memPercentage` | `WorkerProfile.systemLoadInfo` (nullable - only populated where the scheduler already tracks it) |
| `attributes` | `WorkerProfile.attributes` (worker tags) |

This is a pure read/projection: it calls `getRegisterWorker()` and maps each `WorkerProfile`, following the exact cross-node pattern already used by `GetOverviewOperation`. It introduces no new field on `WorkerProfile` itself and changes no allocation/scheduling code path.

The Workers/Master page joins this by `address` (host + port) with the existing `Monitor` rows client-side, the same way the page already separates Master vs. Worker rows by `isMaster`.

## Stable Contract and Best-Effort Signals

Stable identifiers:

- `host`, `port`, `isMaster` / role
- `totalSlot`, `usedSlot` (derived from live slot arrays, always internally consistent with the scheduler's own view because it reads the same state)

Best-effort diagnostic signals:

- CPU load, heap/memory usage, GC counters (already best-effort today via `/system-monitoring-information`)
- `cpuPercentage` / `memPercentage` from `systemLoadInfo` - populated on the scheduler's own cadence, not a guaranteed real-time value

## Refresh, Retention, and Cost

- Reuse the existing Workers/Master page's request-on-navigation model; no continuous polling is introduced beyond what the page already does for `/system-monitoring-information`.
- The new endpoint is O(number of registered workers) per call, matching the cost profile of `GET /overview`, which already iterates the same resource manager state.
- No new data is written to disk or retained beyond the live in-memory `WorkerProfile` map the resource manager already maintains.

## Large Cluster Degradation

For clusters with many workers, the table remains a flat, sortable/filterable list rather than a graph; there is no rendering-cost concern comparable to a DAG, since row count scales linearly with worker count and the existing `NDataTable` component already paginates.

## V1 Delivery Plan

1. Add a `WorkerOverviewInfo` DTO and a `GetWorkerOverviewOperation` mirroring `GetOverviewOperation`'s cross-node pattern, backed by `ResourceManager.getRegisterWorker()`.
2. Add a dedicated REST endpoint and servlet for the new projection, following the existing `OverviewService` / `OverviewServlet` structure.
3. Extend `seatunnel-engine-ui`'s manager service/types to fetch and join the new endpoint with the existing `/system-monitoring-information` rows by address.
4. Extend the Workers/Master table with the curated column set above, and re-enable the existing (currently commented-out) Action column as a "View Details" drawer showing the full raw payload.
5. Update English and Chinese Web UI and REST API documentation.

## Validation Requirements

- Backend unit tests cover the new operation's mapping from `WorkerProfile` to `WorkerOverviewInfo`, including the case of zero registered workers and workers with zero slots.
- Frontend tests cover the client-side join by address and the curated column rendering.
- Docs state clearly that slot and load fields reflect the resource manager's live in-memory state, not a persisted history.
- No new persistent table, file, or write path is introduced for V1.

## Related Documentation

- [Web UI](./web-ui.md)
- [RESTful API V2](./rest-api-v2.md)
- [Realtime Observability](./realtime-observability.md)
