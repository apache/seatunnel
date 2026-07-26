---
sidebar_position: 16
---

<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Engine-Managed Source Runtime

The engine-managed Source runtime is an experimental Zeta execution lane introduced by
[STIP-31](https://github.com/apache/seatunnel/issues/11558). It serializes checkpoint-visible
Reader and Split Enumerator state in engine-owned event loops, so connector implementations do
not need to coordinate engine callbacks with checkpoint locks.

The feature is disabled by default. Existing connectors and jobs continue to use the legacy lane
unless all of the following gates pass:

1. `managed-source-runtime.enabled` is `true`.
2. The connector name is in `connector-allowlist`.
3. The connector declares a compatible `ManagedSourceCapability`.
4. The runtime protocol and restored checkpoint metadata match.

If any configured gate is invalid, deployment fails closed instead of silently changing execution
semantics.

## Runtime Model

The managed lane provides these guarantees:

- Reader commands, split assignments, barriers, checkpoint callbacks, schema-change transitions,
  graceful close, and cancellation share one ordered domain.
- For each managed component, dedicated managed-command Hazelcast operation threads only perform
  bounded admission. They never execute connector code or wait for command execution.
- Accepted split assignments remain in an engine-owned assignment ledger until Reader checkpoint
  inclusion is proven.
- Transport is at-least-once with command deduplication, attempt fencing, checksums, and explicit
  protocol versions.
- Reader polling is cooperative and bounded by records, estimated bytes, and time. Records are not
  copied into a per-record mailbox.
- One reusable watchdog is allocated per managed Reader. Watchdog allocation does not grow with
  records per second or poll frequency.
- Blocking coordinator discovery runs in bounded engine-owned I/O or CPU worker pools. Results are
  applied on the coordinator event loop and stale-epoch results are discarded.
- Mailboxes, outbound commands, scheduler callbacks, async queues, and assignment history all have
  hard count or byte limits. Reserved capacity is kept for barriers and terminal control.

Existing connector locks are not removed automatically. Connectors not certified for the managed
contract remain on the legacy lane.
Capability is component-scoped: an Iceberg coordinator-only canary moves discovery and coordinator
state to the managed owner but deliberately leaves its Reader transport and Reader locking on the
legacy lane. The no-operation-thread-connector-code guarantee applies end to end only when both
Reader and coordinator capabilities are enabled.

## Enable A Canary

Configure `config/seatunnel.yaml` on every Zeta node:

```yaml
seatunnel:
  engine:
    managed-source-runtime:
      enabled: true
      connector-allowlist:
        - FakeSource
      runtime-protocol-version: 1
```

Lane selection is frozen in the physical plan and persisted in checkpoint metadata. Configuration
reload does not switch a running job between lanes.

The initial certification pilots are:

| Connector | Contract exercised |
|---|---|
| FakeSource | Managed Reader polling, availability, split state, and checkpoint ordering |
| Kafka | Background fetcher confinement, async commit completion, split discovery, and assignment |
| Iceberg | Blocking streaming discovery through the coordinator scheduler |

An allowlist entry is not sufficient by itself. A connector without a compatible capability causes
deployment to fail.

## Safety Configuration

Byte values are raw bytes and duration values are milliseconds.

| Option | Default | Purpose |
|---|---:|---|
| `enabled` | `false` | Enables selection for new physical plans |
| `connector-allowlist` | `[]` | Explicit connector plugin names allowed to enter the managed lane; wildcards are rejected |
| `runtime-protocol-version` | `1` | Required engine/connector wire protocol |
| `reader-mailbox-max-commands` | `1024` | Total Reader command capacity |
| `reader-mailbox-max-bytes` | `4194304` | Per-Reader command bytes |
| `reader-reserved-control-commands` | `64` | Commands reserved for checkpoint and terminal control |
| `reader-reserved-control-bytes` | `262144` | Bytes reserved for checkpoint and terminal control |
| `worker-mailbox-max-bytes` | `268435456` | Worker-wide managed Source memory budget |
| `max-command-payload-bytes` | `524288` | Maximum payload before split/report chunking |
| `poll-max-records` | `64` | Maximum records per poll turn |
| `poll-max-bytes` | `1048576` | Maximum estimated output bytes per poll turn |
| `poll-soft-duration-ms` | `5` | Cooperative poll deadline and warning threshold |
| `poll-hard-duration-ms` | `1000` | Threshold before `wakeUp()` is requested |
| `poll-cancellation-timeout-ms` | `30000` | Additional time before the task is failed and interrupted |
| `idle-wait-ms` | `10` | Event-loop idle wait |
| `admission-budget-ms` | `5` | Operation-thread admission budget |
| `retry-initial-backoff-ms` | `10` | Initial transport retry delay |
| `retry-max-backoff-ms` | `1000` | Maximum retry delay |
| `command-retry-deadline-ms` | `30000` | Durable command delivery deadline |
| `coordinator-async-max-concurrency` | `4` | Per-coordinator active async work |
| `coordinator-async-io-threads` | `32` | Worker-wide blocking discovery threads |
| `coordinator-async-cpu-threads` | `4` | Worker-wide CPU discovery threads |
| `coordinator-async-queue-capacity` | `4096` | Capacity of each worker-wide async queue |
| `assignment-tracker-max-entries` | `100000` | Per-coordinator assignment ledger entries |
| `assignment-tracker-max-bytes` | `67108864` | Per-coordinator assignment ledger bytes |

Invalid combinations, such as reserving the whole mailbox or allowing one payload to consume
reserved control bytes, are rejected during configuration parsing.
Reserved control command capacity must also be at least the configured coordinator async
concurrency so every in-flight worker can always publish one terminal completion.

## Checkpoint And Recovery

`ACCEPTED` means the receiving runtime owns delivery of the command in its current attempt. It does
not mean the command is durable in a completed checkpoint.

For split assignment:

1. The coordinator records `DISPATCHED` before transport.
2. Reader admission moves the ledger to `ADMITTED`.
3. The Reader applies the command in sender-sequence order and returns an application proof.
4. The Reader snapshot contains connector split state, applied command watermark, stable split
   identifiers, no-more-splits generation, and lifecycle metadata.
5. The Reader checkpoint report moves matching assignments to `CHECKPOINT_INCLUDED`.
6. Only checkpoint completion makes those ledger entries eligible for compaction.

Failover restores the last completed connector state and engine metadata. Old attempt IDs and
in-flight async work are fenced. During rescale, completed-checkpoint split identity is reconciled
before unresolved assignments are returned to the enumerator. Checkpoint-disabled jobs keep the
ledger only until application proof and cannot claim checkpoint durability.
Per-Reader no-more-splits state is retained only when parallelism is unchanged. After rescale, only
global end-of-input is propagated; partial old-subtask finality is discarded so new Readers
renegotiate with the restored Enumerator instead of ending prematurely.
Chunked checkpoint reports and restore ownership proofs are applied only after every chunk in the
fenced group arrives. Their retained identifiers share the worker memory budget and are released on
completion, checkpoint termination, attempt replacement, or runtime close.

Schema-change checkpoints use an explicit state machine. A graceful close received during an
active schema change is latched, the schema checkpoint finishes first, and the Reader then enters
`DRAINING`. Abort, timeout, and protocol mismatch fail the source.

## Monitoring

Managed metrics use a stable Source action suffix. Command IDs, split IDs, table names, and
exception messages are never metric dimensions.

Monitor at least:

- `SourceManagedMailboxCommands`, `SourceManagedMailboxBytes`, and
  `SourceManagedMailboxOldestAgeMs`.
- `SourceManagedAdmissionTotal`, `SourceManagedAdmissionNs`, and
  `SourceManagedAdmissionBudgetExceededTotal`.
- `SourceManagedCommandQueueNs`, `SourceManagedCommandNs`, and
  `SourceManagedTransportRetryTotal`.
- `SourceManagedAssignmentEntries`, `SourceManagedAssignmentBytes`,
  `SourceManagedAssignmentOldestAgeMs`, and per-state entry counts.
- `SourceManagedAsyncRunning`, `SourceManagedAsyncWaiting`,
  `SourceManagedAsyncTimeoutTotal`, and async queue/execution time.
- `SourceManagedWakeupTotal` and `SourceManagedWakeupTimeoutTotal`.

Stop the rollout when mailbox age grows continuously, reserved capacity is consumed in steady
state, assignment entries grow across successful checkpoints, admission exceeds its budget, async
timeouts occur, or wakeup timeouts are non-zero.

## Production Gate

Compare the managed lane against the same-hardware, same-JVM legacy baseline:

- Parallelism: `1`, `16`, `128`, and `512`.
- Record size: `128 B`, `1 KiB`, and `16 KiB`.
- Split count: `1x`, `10x`, and `100x` parallelism.
- Checkpoint interval: `10 s` and `60 s`.
- Command load: steady state, `10x` burst, and full-mailbox recovery.
- Duration: at least 30 minutes steady state, 2 hours with failover, and 24 hours soak.

Release criteria are no more than 3% throughput loss, 5% CPU and steady-state heap increase,
checkpoint p99 increase no more than 5% or 100 ms (whichever is larger), admission p99 no more than
5 ms, no operation-thread connector callback within each certified managed component, no configured
capacity violation, and no unbounded assignment growth during the 24-hour soak. A full
Reader-and-coordinator certification must satisfy the operation-thread criterion end to end.

## Rollout And Rollback

1. Keep `enabled: false` while collecting the legacy baseline.
2. Enable only `FakeSource` in a non-production cluster and run recovery, rescale, and mailbox
   saturation tests.
3. Canary one Kafka or Iceberg workload after its conformance and performance gates pass.
4. Expand one connector and one workload class at a time.
5. Disable selection for new deployments immediately when a stop condition is reached.

Do not restore a managed checkpoint into the legacy lane. For a recoverable rollback, keep the same
managed selection until the canary is drained, or restart from an earlier compatible legacy
checkpoint or fresh source position. Running jobs and failover from their persisted physical plan
do not switch lanes merely because the cluster allowlist changes.

Protocol version 1 intentionally rejects arbitrary custom `SourceEvent` payloads. Such connectors
must remain on the legacy lane until a versioned event codec is introduced.
