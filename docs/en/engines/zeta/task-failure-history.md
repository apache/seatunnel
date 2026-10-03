# Task Failure History Design

This document proposes the first backend contract for [GH-11667](https://github.com/apache/seatunnel/issues/11667). The canonical STIP discussion is [STIP-33](https://github.com/apache/seatunnel/issues/11735). This document does not describe an implemented API yet.

Changes since `e944f1e3`:

- The attempt key's source, its single writer, and how each kind of report resolves are now tables (Normative Rules R1 and R2).
- What each dropped operation leaves behind is now a table (R3), and the ordering statements now cover only operations accepted into the queue.
- Each history map now has one row naming its writers, fence, expiry, cleanup owner and persistence (R4).
- The REST resolution order is now a decision table that handles an absent job state (R5).
- Entry creation and adoption are ordered before any capture for the same job, including master-recovery captures during `JobMaster.init` (Ownership section and R4).
- The wire-compatibility fixtures are delivered as a first implementation slice, before any field is added (Compatibility).

## Problem

The Job Detail page currently exposes one exception string. This is not enough when a pipeline is restored several times, or when different task groups fail during the same job. Operators need to see which pipeline attempt and task group failed, and when, without searching every worker log. The first version attributes failures to pipelines and task groups, not to worker addresses.

The current engine state has three relevant limitations:

- `PhysicalPlan` retains only the first error reported by a sub-plan.
- `TaskExecutionState` carries a formatted throwable message, but no durable failure identity.
- Finished-job history keeps the final error text, not the sequence of failures that led to the terminal state.

## Scope

The first implementation provides a bounded, job-scoped failure history. Running and finished jobs are available through the same REST contract.

It:

- groups failures by pipeline execution attempt;
- identifies the pipeline, and the task group when one is involved, that reported the failure;
- records pipeline-level failures that are not task-group failures, such as checkpoint and resource-allocation failures;
- retains task metadata when it is available, without storing worker addresses as failure-history attribution fields;
- preserves the timestamp, message, stack trace and exception type without parsing display text;
- survives active-master failover and pipeline restore;
- expires with the existing finished-job history policy; and
- keeps the existing single `errorMsg` field for compatibility.

The first implementation does not add log aggregation, distributed tracing, or an unbounded exception archive. The Web UI follows in a separate change after the backend contract is agreed.

Compared with GH-11667, the first version makes these deliberate choices:

- It does not attribute failures to a host or worker address, following the security review of this design.
- Links from a record to a worker log depend on [#11662](https://github.com/apache/seatunnel/issues/11662) and a separately agreed logical worker identifier.
- The time an attempt had been running is derived from `attemptStartedAt`. The time a failure occurred is `timestamp`.

## Attempt Model

An attempt belongs to a pipeline, not to an individual task.

`SubPlan.pipelineRestoreNum` participates in the `job.retry.times` decision. It is held in memory and restarts at `0` when a new active master rebuilds the `SubPlan`. Reusing it for history would either lose attempts across failover or, if made durable, change the retry budget. Failure history therefore keeps a separate diagnostic identity. `canRestorePipeline()` and `job.retry.times` never read it, and no restore step waits for a history operation. Restore eligibility, timing and availability are unchanged.

### Attempt key

The durable identity of one pipeline execution is its attempt key. The attempt key is the pipeline's `CREATED` timestamp in `runningJobStateTimestampsIMap`. Current `dev` writes that slot in two places only:

- the `SubPlan` constructor, when it first creates the pipeline state; and
- `SubPlan.resetPipelineState()` before a restore, which writes the timestamp before setting the pipeline state to `CREATED`.

The constructor keeps an existing value, so a `SubPlan` rebuilt by a new active master keeps the key of the execution that is still running.

The key is always copied at the moment it is written or deployed, and stored in failure history. It is never resolved later from `runningJobStateTimestampsIMap`, because each restore overwrites that slot.

- `SubPlan` keeps the current key in memory. It is set in the constructor from the persisted value, and from the value of the last successful write in `resetPipelineState()`.
- If no timestamp array exists when the key would be written, the key is unknown. Records captured under an unknown key have null `attempt` and `attemptStartedAt`.

To make keys unique and ordered, `resetPipelineState()` writes `max(now, previousCreated + 1)` instead of `now`. `previousCreated` is read from the same persisted timestamp array. Keys are then strictly increasing per pipeline, including across active masters whose clocks differ.

This value is public. Since #11982, `/job-info` returns it as `diagnostics.pipelines[].stateTimestamps.CREATED`. The value differs from wall-clock time only when a reset falls in the same millisecond as the previous one, or after the clock steps backward. In that case it can be later than the pipeline's `SCHEDULED` timestamp by at most the size of the step. The `rest-api-v2` documentation of that field will state that `CREATED` is strictly increasing across restores. This is the only change to an existing value in this design.

### Deployment identity

When `PhysicalVertex` deploys a task group, it records the pair `(executionId, attemptKey)` for that deployment. `executionId` is the per-deployment ID that `getTaskGroupImmutableInformation()` already generates with the flake ID generator. The vertex keeps its current deployment and the one before it, and `reset()` moves the current deployment to the previous slot.

These pairs live in master memory. A new active master therefore rebuilds them. When `JobMaster.init` rebuilds the plan on a new active master (`restart = true`), `PhysicalVertex.initStateFuture` gives every vertex whose state is `DEPLOYING`, `RUNNING`, `FAILING` or `CANCELING` a placeholder deployment with an unknown `executionId` under the `SubPlan`'s current key. It does this before it checks whether the task group is still executing. `initStateFuture` also runs when a pipeline is restored on the same master. There it installs no placeholder, and a transition it causes carries no failure context, so it records nothing. A worker keeps executing a task group that survived the failover and skips the redeploy (`TaskExecutionService`), so it keeps reporting the `executionId` that the old master assigned. That ID is unknown to the new master and resolves to the placeholder.

### Attempt number

For each pipeline, the history entry keeps an attempt table and a next-number counter. The table maps each attempt key to its attempt number and suppressed-failure count. It is bounded to 256 keys per pipeline. When full, it evicts its smallest key, and eviction never changes the counter. The table is kept independently of record eviction.

Entry creation and adoption after failover register each pipeline's current key. Each record resolves its key in the same atomic update as its append:

- A key that is in the table uses its number.
- A key larger than every key in the table takes the counter's value, and the counter advances.
- A key that is not in the table and is smaller than its largest key belongs to a late or evicted older execution. It is stored with `attempt = null` and keeps its `attemptStartedAt`. Existing numbers never change.

After `resetPipelineState()`, the master also submits a best-effort `registerAttempt` for the new key, so an execution that records no failure still takes its number. If that operation is dropped, the next recorded execution takes the next number instead. `attempt` is therefore the ordinal of executions observed by failure history, and `attemptStartedAt` is the exact identity.

This is the reconciliation form of the attempt advance. Correct grouping never depends on an advance having completed before the restored execution fails, because every record carries its own key. A retry, or a record arriving before the registration, resolves to the same number.

### Attribution of a report

- A worker report carries the `executionId` of the deployment that produced it, as a new nullable field on `TaskExecutionState`.
- If the ID matches the vertex's current or previous deployment, the report is attributed to that deployment's key.
- An ID the vertex does not know resolves to its placeholder deployment, if it has one. Otherwise the report is from an older deployment; it is dropped from history with a WARN log.
- A report without an `executionId` uses the vertex's current deployment. This covers node loss, master recovery, and reports from a worker that predates this field.
- A deployment that fails before a deployment identity exists uses the `SubPlan`'s current key.
- A vertex with no current deployment, such as a vertex reset to `CREATED` that has not been redeployed, produces no task-group record.

On current `dev`, the engine itself accepts a late `FAILED` report into a vertex that was reset to `CREATED`, because `updateTaskState` rejects only transitions out of a terminal state. The `executionId` attribution keeps such a report under the execution that produced it. Changing the engine to ignore stale reports is a separate fix and outside this design.

### Crash windows

The table follows current `dev`. A new active master runs `restorePipelineState()` only after the recovered job leaves the pending queue. It cancels and reschedules a pipeline whose state is below `RUNNING`. A pipeline ending in `FAILED` or `CANCELED` is restored only if `canRestorePipeline()` holds on the new master, where the in-memory retry counter restarts at `0`.

| Active master fails | Durable state seen by the new master | Engine action on `dev` | Diagnostic result |
|---|---|---|---|
| Before `resetPipelineState()` writes the state | `FAILED` or `CANCELED`; the `CREATED` slot may already hold an unused key | Restores again if `canRestorePipeline()`, writing a new key | The unused key never deploys; adoption may register it, otherwise it receives no number |
| After reset, before any deployment | `CREATED` with key `K1` | Cancels, then restores with `K2` | `K1` is registered at adoption; `K2` takes the next number |
| During scheduling or deployment | `SCHEDULED` or `DEPLOYING` with `K1` | Cancels, then restores with `K2` | Deployment failures stay under `K1`; `K2` takes the next number |
| While `RUNNING`, all task groups alive | `RUNNING` with `K` | Continues; the rebuilt `SubPlan` keeps `K` | No new key; failures resolve to `K` through the placeholder deployments |
| While `RUNNING`, some task groups reported lost | `RUNNING` with `K` | `initStateFuture` marks those task groups `FAILING`; the pipeline fails and may restore | Each gets a `MASTER_RECOVERY` record under `K`, with no exception text. The check also reports a task group lost after any `ExecutionException`, so a record can be a false positive of the existing check |
| While `FAILING` or `CANCELING` | `FAILING` or `CANCELING` with `K` | Cancels the remaining tasks, ends the pipeline, and may restore | Worker reports resolve to `K` through the placeholder deployments |
| Recovered job waiting in the pending queue | Any | Workers that report during this window receive `JobNotFoundException` and stop retrying | Those reports are lost. This is existing engine behavior; history cannot record what the master never receives |

### Fencing

Current `dev` has no durable master epoch. `resetPipelineState()` and `updatePipelineState()` write `runningJobStateIMap` without an owner check. Failure history relies on the same single-active-master assumption as the pipeline state it describes. It does not claim stronger stale-master fencing. Within that assumption:

- Keys are strictly increasing, and existing numbers never change. A late operation from an old master cannot reorder attempts; at worst it stores a record with `attempt = null`.
- The history consumer checks that its node is still the active master before each operation. `clearCoordinatorService()` discards its queue and its in-memory coalescing state.
- Writes from a previous job incarnation, or after the terminal transition, are rejected by the owner and terminal checks in Storage and Retention.

A split-brain second active master is outside the first version's guarantee, as it is for existing job state.

## Failure Record

The proposed REST response is:

```json
{
  "jobId": "123456789",
  "attempts": [
    {
      "pipelineId": 1,
      "attempt": 2,
      "attemptStartedAt": 1753574380000,
      "suppressedCount": 0
    }
  ],
  "failures": [
    {
      "sequence": 7,
      "timestamp": 1753574400000,
      "jobId": "123456789",
      "pipelineId": 1,
      "attempt": 2,
      "attemptStartedAt": 1753574380000,
      "scope": "TASK_GROUP",
      "source": "WORKER",
      "taskGroupId": 4,
      "taskId": null,
      "taskName": "mysql-source -> transform",
      "exceptionType": "java.sql.SQLException",
      "exceptionFingerprint": "5f3a9c1e20b47d86",
      "message": "Connection reset",
      "messageOriginalBytes": 16,
      "messageTruncated": false,
      "stackTrace": "java.sql.SQLException: Connection reset\n...",
      "stackTraceOriginalBytes": 43,
      "stackTraceTruncated": false
    }
  ]
}
```

`attempts` lists the attempt table of every pipeline as stored, including attempts with no retained failure. `suppressedCount` is the number of failures in that attempt that were not recorded individually (see Capture and Deduplication).

Field rules for a failure:

- `scope` is `TASK_GROUP` or `PIPELINE`.
- `source` identifies the capture path:
  - for `TASK_GROUP`: `WORKER`, `NODE_LOSS`, `DEPLOY` or `MASTER_RECOVERY`;
  - for `PIPELINE`: `CHECKPOINT`, `RESOURCE` or `ENGINE`.
- `taskGroupId` is present for `TASK_GROUP` records and null for `PIPELINE` records.
- `attempt` is the attempt number from the Attempt Model. `attemptStartedAt` is the attempt key: when the engine created or reset the pipeline for that execution. Both are null only as described in the Attempt Model.
- `timestamp` is the active master's time when it accepted the failure.
  - The time an attempt had been running is `timestamp - attemptStartedAt`.
  - Clocks can differ across a master change, so the Web UI shows a negative duration as `0`.
- `messageTruncated` and `stackTraceTruncated` are required booleans.
  - `messageOriginalBytes` and `stackTraceOriginalBytes` are the UTF-8 byte lengths after redaction and before truncation, as defined in Text Processing below.
  - They are null when the text is unavailable. Raw pre-redaction lengths are not persisted.
- `exceptionType` and `exceptionFingerprint` come from structured failure transport. Neither is inferred by parsing formatted text.
  - The fingerprint is 16 hex characters (64 bits) of a hash over the root cause's class name plus, for each of its first five frames, the declaring class name and method name.
  - It excludes the message, line numbers and `StackTraceElement.toString()`, whose format differs between JDK 8 and JDK 11. Generated lambda class suffixes are normalized.
  - It is null on paths without a `Throwable`, such as `NODE_LOSS` and `MASTER_RECOVERY`. It groups recurring causes in the Web UI and is not a security identifier.
- The stored UTF-8 representation is limited to 4 KiB for `message` and 64 KiB for `stackTrace`. Truncation preserves valid UTF-8. A truncated message keeps its prefix. A truncated stack trace keeps its beginning and its end, so the exception and the deepest cause remain available.

The fields fall into two groups:

| Group | Fields | Guarantee |
|---|---|---|
| Stable contract | `sequence`, `timestamp`, `jobId`, `pipelineId`, `attempt`, `attemptStartedAt`, `scope`, `source`, `taskGroupId`, `messageTruncated`, `stackTraceTruncated`, and all fields of `attempts` | Always present with the documented meaning. `attempt`, `attemptStartedAt` and `taskGroupId` are nullable only as described above |
| Best-effort telemetry | `taskId`, `taskName`, `exceptionType`, `exceptionFingerprint`, `message`, `messageOriginalBytes`, `stackTrace`, `stackTraceOriginalBytes` | Present when the capture path provides them; may be null for older or synthetic paths |

The first version has no worker-address attribution field in running history, finished history, or REST responses.

## Capture and Deduplication

Capture happens inside the engine's own state transitions, so it is ordered before any pipeline reset that the transition can trigger.

- **Task-group failures (`WORKER`, `NODE_LOSS`, `MASTER_RECOVERY`).** Capture happens inside `PhysicalVertex.updateTaskState`, within the same synchronized transition. It runs after the `FAILED` or `FAILING` state is written and before `stateProcess()` and the task future complete.
  - `updateStateByExecutionService` passes the report's structured fields and `executionId` into that transition.
  - Node loss (`CoordinatorService.makeTasksFailed`) and master recovery (`initStateFuture` during `JobMaster.init` with `restart = true`) use the same transition, without an `executionId`.
  - A pipeline end callback runs asynchronously on the job executor. Capture has already been offered at that point, so an accepted capture is queued ahead of every operation the reset offers. A rejected capture is handled as described in R3.
- **Deployment failures (`DEPLOY`).** The deploy failure paths in `PhysicalVertex` (`deploy` and `deployOnRemote`) pass a `DEPLOY` failure context to `makeTaskGroupFailing`. Capture happens there, after the `FAILING` transition is accepted.
  - `TaskDeployState.failed(Throwable)` carries the original failure's class name, message, stack trace and fingerprint as strings, captured where the `Throwable` is available. On the worker, that is before the response crosses the Hazelcast RPC boundary.
  - Master-side failures unwrap `ExecutionException` and `CompletionException` to the original cause. The live `Throwable` never crosses the boundary, so connector-specific exception classes are not needed on the master.
  - The record's `exceptionType` identifies the original cause, not `TaskGroupDeployException`.
- **Pipeline failures (`CHECKPOINT`, `RESOURCE`, `ENGINE`).**
  - `CHECKPOINT` is captured in `CheckpointCoordinator.handleCoordinatorError` when the coordinator first moves to `FAILED`, where the `Throwable` is available and before the pipeline is canceled.
  - `RESOURCE` is captured when resource allocation fails in the `SCHEDULED` state.
  - `ENGINE` is captured from the other `makePipelineFailing` causes (state-update and restore errors).
- Every capture point passes a failure context that names its `source`. A transition to `FAILING` or `FAILED` without a failure context records nothing. This includes cancellation and the internal error handling of `updateTaskState`.

Capture runs on Hazelcast operation threads, the membership event thread, the job executor, the thread that restores a job after failover, and checkpoint coordinator threads. It does constant work plus one bounded copy:

- It first checks the per-attempt coalescing limit. Beyond that limit it touches no text.
- It copies at most the first 256 KiB of each text field.
- It calls a non-blocking `offer`.

Redaction, truncation and serialization run on the consumer thread described below, never on the capturing thread.

Deduplication keys:

- `TASK_GROUP`: `(pipelineId, attemptKey, taskGroupId)`.
- `PIPELINE`: `(pipelineId, attemptKey, source)`.

The first delivery creates the record and receives a job-wide `sequence` number. Later deliveries with the same key are ignored without consuming a sequence number. The retained records are the bounded key set, so after eviction a delayed duplicate can be recorded again. A failure after restore has a different attempt key and remains a separate record.

The master coalesces failure storms. Per pipeline attempt, at most 20 records are submitted. Further failures in that attempt only increment a counter in master memory. The counter's total is sent as an absolute value with the next operation for that pipeline, or with the job's terminal operation. The history keeps the largest total it has received for the attempt, so a retried operation cannot count twice. A node loss at high parallelism therefore produces at most 20 record operations per attempt. The count is best effort and is lost at failover.

Recording history is diagnostic and best effort:

- Each active master's `CoordinatorService` owns one bounded FIFO queue and one dedicated consumer thread.
- The queue holds at most 1,000 operations and 16 MiB of captured text, and at most 100 queued operations per job. An `offer` that would exceed a bound is dropped.
- The consumer runs each `EntryProcessor` outside Hazelcast operation threads and outside the job scheduling executor. It processes the queue strictly in order. It retries an operation in place, before taking the next one, only for failures that Hazelcast classifies as retryable, up to three times, and then drops it.
- Completion never changes task or pipeline state, never calls back into `JobMaster` or `SubPlan`, and never re-enters failure or restore processing.
- No failure-history operation is required for a task failure, restore decision, or restored execution to proceed.

Every dropped or failed history operation is logged at WARN, rate-limited per job. The log line carries only `jobId`, `pipelineId`, the attempt key, `taskGroupId` or source, the exception class name and a fixed reason code. It never includes message, stack-trace or task-name text. The history value, record and operation classes override `toString()` to omit that text.

### Text processing

Text is processed in this fixed order:

1. The capturing side takes at most the first 256 KiB of each field. This cap removes only a suffix, so it can drop a secret's value but never expose a value without its key. Workers apply the same cap to the new structured fields before sending them.
2. The consumer redacts the whole capped text as one string, including multi-line patterns.
3. The consumer records `*OriginalBytes` from the redacted text.
4. The consumer truncates the redacted text at UTF-8 boundaries to the limits above, and at line boundaries for stack traces. It marks the omitted range explicitly.

Nothing truncates, windows or slices text before step 2. Later re-truncation, such as the finished-snapshot cap, works on already redacted text. The master redacts all structured text received from workers and never relies on a worker having done so. Redaction is idempotent.

## Storage and Retention

Failure history uses two dedicated Hazelcast maps: `engine_runningJobFailureHistory`, keyed by `jobId`, and `engine_finishedJobFailureHistory`, keyed by `jobId`.

- Both maps use the default Hazelcast `MapConfig` that the existing job-state maps use. This design adds no backups, persistence, or external history backend.
- `FileMapStore.init` already excludes `engine_runningJobMetrics` from IMap storage. It excludes both history maps in the same way, even when operators configure `map.engine*` with a `map-store`.
- This keeps large diagnostic values off HDFS and S3 IMap storage and away from write-through on the partition thread. It also keeps TTL expiry effective, because `MapStore` entries are neither deleted on expiry nor reloaded with their TTL.
- As a consequence, failure history does not survive a full-cluster restart. A job recovered from IMap storage after such a restart has no failure-history entry.
- The REST representation is independent of this storage choice.

### Ownership

Each running entry and each finished snapshot records its owner as `(jobId, initializationTimestamp)` from `JobInfo`. Current `dev` uses the same identity for cleanup ownership (`isCleanupOwnedByCurrentJob` and `JobCleanupRecord.ownerInitializationTimestamp`). Neither `JobInfo` nor `JobCleanupRecord` changes.

- `JobMaster` initialization of a new submission, including a savepoint start that reuses a job ID, offers a create operation. No other operation creates an entry.
- `JobMaster.init` offers the create operation (or, after failover, the adopt operation) immediately after `PlanUtils.fromLogicalDAG` returns. This is before `initCheckPointManager` and `initStateFuture`, which can capture `MASTER_RECOVERY` failures on a new active master. On current `dev`, `submitJob` and `restoreJobFromMasterActiveSwitch` both call `init` before the job enters the pending queue, and no other capture point runs before the job leaves that queue. An accepted create or adopt is therefore queued ahead of every capture for the job. If it is rejected, later operations for the job are rejected as `NO_ENTRY` or `OWNER_MISMATCH` (R3).
- The consumer runs the create operation in order:
  1. If the running entry belongs to another owner and is not finalized, finalize it as described in Terminal Handling.
  2. Create the new entry and register each pipeline's current key.

  A create operation never removes a finished snapshot of another owner. The snapshot of the new owner replaces it once that owner finishes (R4).

  If step 1 fails, the new entry is still created and the loss of the previous incarnation's history is logged.
- Active-master recovery adopts an existing entry only when its owner matches the recovered `JobInfo` and the entry is not terminal. Adoption registers each pipeline's current key.
- When no matching entry exists, the job records no failure history, and REST returns the known-job empty list. Examples are a job started before the feature was deployed, and a job whose create operation was dropped.
- Append and attempt operations never create an entry.

The owner identity is only as unique as `initializationTimestamp`. A collision needs a savepoint start whose initialization lands on exactly the previous incarnation's millisecond, which requires a backward clock step. Existing cleanup ownership has the same limit, and this design does not widen it.

### Bounds

Running history keeps, per job:

- at most 100 failure records;
- at most 1 MiB of combined UTF-8 text across `message`, `stackTrace`, `taskName` and `exceptionType`. Each `taskName` and `exceptionType` is capped at 1 KiB, and best-effort redaction applies to them too;
- the attempt tables, at most 256 keys per pipeline, with a few bytes per key.

When a limit is exceeded, records are evicted oldest first until both the record and text limits are satisfied. The first record of each pipeline attempt, usually the root cause, is evicted only after all other records. There is no TTL while the job is active.

The finished snapshot keeps the attempt tables and every retained record. It caps each stored stack trace at 16 KiB (head and tail) and the job's total text at 256 KiB, using the same eviction order. It keeps the truncation flags and original byte lengths.

Worst-case cluster heap is:

`(running jobs × 1 MiB + jobs finished within history-job-expire-minutes × 256 KiB) × (1 + backup count) + 16 MiB of queued text per active master`

With the defaults (24 hours, one backup), 1,000 failing jobs per day retain about 0.5 GiB in finished snapshots. Operators who need a hard global cap can add a Hazelcast eviction policy for `engine_finishedJobFailureHistory`. An evicted snapshot yields the known-job empty list.

Each `EntryProcessor` operation deserializes and re-serializes the job's value, up to about 1 MiB, on the partition thread, and the backup repeats it. Coalescing bounds the rate to 20 record operations per pipeline attempt.

These limits are constants rather than user options. Configurable limits can be added later if operational evidence shows these limits are insufficient.

### Terminal fence

The history `EntryProcessor` enforces the fence. In the same atomic per-key update as its mutation, every append and attempt operation checks that:

- the entry exists;
- its owner equals the operation's `(jobId, initializationTimestamp)`; and
- the entry is not terminal.

Otherwise the operation returns without writing, so an absent entry is never recreated.

Processors are deterministic. The consumer computes the current time and any TTL immediately before it first submits an operation, and reuses the same values for that operation's retries. Primary and backup therefore apply the same result, and a retry never extends a deadline.

### Terminal handling

Terminal handling has three idempotent steps:

1. **`markTerminal(owner, terminalTime, ttl)`.** When the owner matches and the entry is not terminal:
   - set `terminal = true` and store `terminalTime`;
   - set the entry TTL through `ExtendedMapEntry.setValue(value, ttl, unit)`, where `ttl = terminalTime + history-job-expire-minutes - now`. A non-positive TTL removes the entry.

   An already terminal entry is returned unchanged, without calling `setValue`. A plain `setValue` would clear the TTL, and not calling it means no retry or recovery can extend the deadline. The processor returns the frozen records.
2. **Write the finished snapshot** from the frozen records, with its owner and the same remaining TTL. This step is best effort.
3. **`removeIfOwner(owner)`.** Remove the entry only when the owner matches and the entry is terminal. This step runs only after step 2 succeeded, or after a snapshot of the same owner was found. Otherwise the terminal entry stays until the next finalization offer or the sweep retries step 2, or until its TTL expires.

Every finalization goes through the same FIFO queue, behind every earlier operation that the queue accepted. Captures accepted before the finalization are therefore applied before the entry becomes terminal. A capture that was rejected or exhausted its retries is absent and logged (R3). The terminal fence depends only on the owner, the terminal flag and `terminalTime`, never on whether every earlier record was stored. Callers resolve `terminalTime` before they offer the operation. Finalization skips step 2 when a finished snapshot with the same owner already exists.

The finished snapshot stores its owner and the frozen records and attempt tables. It is written only over no snapshot, or over a snapshot whose owner has a smaller `initializationTimestamp`, so an older incarnation can never overwrite a newer one.

A finished savepoint start that reuses a job ID can have no snapshot of its own in two cases: its create operation was dropped, or every attempt to write its snapshot failed until its running entry expired. In either case, once its `JobInfo` is removed, REST returns the previous incarnation's snapshot, if one remains, until that snapshot's TTL expires. Both cases are logged at WARN. They are the only cases where history can belong to an earlier incarnation of the same job ID.

`JobMaster.cleanJob()` offers finalization first, in its own `try`, before its other terminal work. Finalization is also offered best effort from four other places:

- `processPendingJobCleanup`, with `ownerInitializationTimestamp`, before `cleanupPendingJobStateMaps`;
- `cleanupTerminalZombieJob`, with the `JobInfo` initialization timestamp, before its state keys are removed;
- `cleanupPendingJobStateForRestore`, before a savepoint start initializes the new incarnation; and
- a history sweep. The sweep runs every 60 seconds on the existing `pipelineCleanupScheduler`, only on the active master. It finalizes any running entry whose owner has no matching `JobInfo` in `runningJobInfoIMap`, and any running entry whose job state is an end state; an absent job state is not an end state. An entry whose owner has no `JobInfo`, that is not terminal, and whose job never reached an end state is an orphan, such as one left by a failed submission. For an orphan, the sweep runs `markTerminal` and `removeIfOwner` without step 2, both fenced by owner. Every other finalization writes a snapshot, even an empty one. Each run continues while such entries remain, within a 5-second budget.

A failed or dropped finalization is logged and not thrown into those paths. It cannot keep a `JobCleanupRecord` pending, delay existing state cleanup, or fail a savepoint-start submission. The sweep retries it.

`terminalTime` is resolved in this order, reading everything before any state key is removed:

1. the value stored by `markTerminal`;
2. the job's end-state timestamp in `runningJobStateTimestampsIMap`;
3. `JobState.finishTime` in `engine_finishedJobState`;
4. only when none of these exists, the time of finalization. This case is logged.

`JobCleanupRecord` is not extended. A third key set would change a shared `IdentifiedDataSerializable` that has HA-persisted instances. `IMAP_PENDING_JOB_CLEANUP` stores one record per job ID. A diagnostic failure could also keep existing state cleanup pending.

Master failover preserves records already acknowledged by the history map, and it keeps sequence numbers and attempt tables. An operation still queued when the active master fails may be lost. This diagnostic path never delays a task failure or restore decision to wait for a history acknowledgement.

## REST Contract

The proposed endpoint is:

```text
GET /job-info/{jobId}/failures?limit=100
```

Behavior:

- `failures` is returned in descending `sequence` order, with at most `limit` records. `attempts` is not limited by `limit`.
- `limit` defaults to 100. Larger values are capped at 100.
- Running and finished jobs use the same response model.
- The response is written with a streaming JSON writer. Its size is bounded by the stored text limits after JSON escaping.

The endpoint resolves the job with the decision table R5 in Normative Rules. A terminal job never reads running records, including during the `state-cleanup-delay-ms` window when `JobInfo` still exists. While a savepoint start that reuses the job ID is running, R5 returns only records and snapshots whose owner matches its `JobInfo`. After it finishes, see the limit described in Terminal handling.

`JobInfoServlet` currently parses the whole decoded path as one numeric job ID. It also serves the deprecated `/running-job/*` alias. Jetty's decoded path turns `%2F` into `/`, strips `;` parameters and resolves dot segments, so the new route never routes on it. The failures route is matched as follows:

- The handler takes `getRequestURI()` and requires the prefix `getContextPath() + getServletPath()` to match literally.
- The rest of the URI must exactly match `^/([0-9]{1,19})/failures$`: ASCII digits only, and a case-sensitive `failures`.
- A URI containing `%`, `;`, `\`, an empty segment or a `.` or `..` segment, or ending in a slash, receives `404`.
- The job ID must parse as a positive signed 64-bit value; otherwise the response is `400`.
- Only a matched path reads `limit`. It must match `^[0-9]{1,10}$` and be positive. An empty or repeated `limit` receives `400`, and unknown query parameters are ignored.
- The route serves `GET` only. Other methods receive `405` from the handler itself.

The single-ID routes `/job-info/{jobId}` and `/running-job/{jobId}` keep their existing behavior, including their existing `400` response for malformed IDs. Every other path shape under these mappings returns `404`, including `/running-job/{jobId}/failures` and extra segments after `/failures`. Prefix or substring matching is not allowed. This explicitly changes invalid multi-segment paths from today's numeric-parse `400` to `404`.

The failures route writes every non-200 response itself:

- It uses `setStatus` and a fixed JSON body such as `{"status":"fail","message":"Not found"}`.
- It never calls `sendError`. Jetty's default error page echoes the URI and can include stack traces.
- It never builds a body from request input or exception text.
- It catches `RuntimeException` from map reads and returns a fixed `503` body, with the detail only in the server log.
- Responses set `Content-Type: application/json; charset=UTF-8` and `X-Content-Type-Options: nosniff`.

The current `/job-info/{jobId}` behavior and its `errorMsg` field remain unchanged, including the response for an unknown job. The new endpoint defines its own `404`, so callers can tell an unknown job from a known job with no failures. The endpoint is documented in the `rest-api-v2` EN and ZH pages.

## Security and Input Validation

**Authentication.** The endpoint sits under the existing `JobInfoServlet` mappings, so it inherits the `BasicAuthFilter` boundary of the engine REST API. It adds no endpoint-specific authentication.
- REST authentication is disabled by default (`enable-basic-auth: false`) while HTTP is enabled on port 8080. By default, anyone who can reach the port can read failure history.
- The existing boundary is a single shared credential with no roles, and provides no per-job or per-tenant authorization. Deployments that need narrower access must enforce it at their gateway or network boundary.
- The route handles requests directly, without async or forward dispatch, because the filters are registered for `REQUEST` dispatch only.

**Other access paths.** Redaction is defence in depth, not an access boundary. The same failures remain available unredacted through:
- the existing `errorMsg` field of `/job-info/{jobId}`;
- the node-log endpoints;
- any client that can reach the Hazelcast member port. OSS Hazelcast has no member or client authentication, so such a client can read every engine map, including failure history. The member port must stay on a trusted network.

The shipped `hazelcast.yaml` already enables the `DATA` endpoint group, so enabling Hazelcast's built-in REST API exposes map values outside `BasicAuthFilter`. Anyone allowed to read failure history must also be trusted with those paths.

Exception text can also contain record values and remote-server responses, which redaction does not target. Failure history keeps them for up to `history-job-expire-minutes` after the job ends. The engine security documentation will list the new endpoint and these caveats.

**Redaction.** Best-effort redaction runs, in the order defined in Text Processing, before every write to the running map or the finished snapshot. This covers the worker-reported, deployment, node-loss, master-recovery, pipeline and terminal-snapshot paths. The implementation extracts the patterns of `DryRunConnectFailureMessageSanitizer` into a shared utility with separate `redact` and `truncate` functions, and extends them as follows:

- **Sensitive key names.** These tokens are matched anywhere in a key of `[A-Za-z0-9._-]`, in snake, kebab, dot or camel case, and the key may be quoted:
  - `password`, `passwd`, `pwd`, `passphrase`;
  - `secret`, `client secret`;
  - `token`, `session token`, `security token`, `auth token`;
  - `credential`, `signature`;
  - `private key`, `access key`, `account key`, `shared access key`, `api key`;
  - `cookie`, `jaas`.

  Examples include `db_password`, `ssl.keystore.password`, `fs.s3a.secret.key`, `fs.azure.account.key.*`, `aws_secret_access_key`, `accessKeySecret` and `SharedAccessKey`.
- **Values.** A value can follow `=` or `:` and can be:
  - a double-quoted string, including escaped quotes;
  - a single-quoted string;
  - a braced value;
  - otherwise, everything up to whitespace, `&`, `,`, `}` or `)`.
- **Whole values.**
  - the whole value of `sasl.jaas.config`, and `password="..."` inside JAAS text on its own;
  - sensitive `Key=Value;` pairs in connection strings (Azure, ODBC, SQL Server);
  - JDBC URLs, keeping only `jdbc:<subprotocol>:`.
- **URLs and HTTP.**
  - URL user information (`scheme://user:pass@host`);
  - sensitive URL query parameters, including `sig`, `signature`, `key`, `apikey`, `access_token`, `refresh_token`, `id_token`, `code` and `X-Amz-*`;
  - `Authorization`, `Proxy-Authorization`, `Cookie`, `Set-Cookie` and `X-Api-Key` header values;
  - bare `Bearer` and `Basic` credentials.
- **PEM private keys.** These are matched with real or JSON-escaped newlines. An unterminated block is masked to the end of the text.
- **Token shapes.** These are prefix-anchored: JWTs, AWS access key IDs, Google API keys, GitHub tokens and Slack tokens.

Every pattern is anchored on a literal and has no nested unbounded quantifiers. A performance test bounds each pattern on adversarial 1 MiB input.

Stated non-goals: generic high-entropy detection, percent-encoded secrets, secrets inside SQL literals, row data, secrets embedded in URL paths, and usernames or hosts outside URLs. Existing job `errorMsg` storage and REST behavior are unchanged.

**Worker addresses.** The first version neither stores worker `host:port` attribution fields in running or finished history nor exposes them in REST responses. Capture code may consult existing execution metadata but must not copy its addresses into history fields. This does not promise to remove every address mentioned inside sanitized exception text. Existing execution metadata and `/pending-jobs` behavior are unchanged.

## Web UI Follow-up

The Exception tab can consume the REST endpoint in a separate change. The first UI version:

- groups failures by pipeline and attempt, using `attempts` to show attempts without failures and suppressed counts;
- shows the timestamp, pipeline, task group or pipeline source, task name, exception type and message;
- marks records that share an `exceptionFingerprint` as recurring across attempts;
- shows how long each attempt had been running when it failed;
- shows the truncation flags, with stack traces collapsed by default.

It renders `message`, `stackTrace`, `taskName` and `exceptionType` as text only. It does not interpret HTML or turn URLs in exception text into links. It must not infer or expose worker addresses omitted by the API. Missing optional fields are displayed as unavailable. It must not claim task-level precision for task-group failures. Log links wait for #11662 and a logical worker identifier.

## Compatibility

For valid existing job-detail requests the feature is purely additive. Invalid multi-segment paths change from numeric-parse `400` to `404`, as specified above.

- Existing jobs need no configuration changes.
- Existing REST fields and the final `errorMsg` remain available.
- Checkpoint and savepoint payloads, `JobInfo` and `JobCleanupRecord` are unchanged.
- `job.retry.times`, restore eligibility and restore availability are unchanged.
- The only change to an existing value is the one described under Attempt key: the pipeline `CREATED` timestamp, returned by `/job-info` diagnostics, becomes strictly increasing across restores.
- Old failure paths populate only the fields they know.

Two Java-serialized classes cross the worker-master boundary and gain fields. Both declare no `serialVersionUID` today:

- `TaskExecutionState` is written with `writeObject` by `NotifyTaskStatusOperation`. Its generated value is `-108652017022658969L`. The source is unchanged since March 2023.
- `TaskDeployState`, a Lombok `@Data` class, is the `DeployTaskOperation` response. Its generated value is `2646079648150626562L`, computed on the Lombok-built class. The source is unchanged since March 2023.

Both values were computed with `serialver` on JDK 8 and JDK 11. The implementation declares exactly these values before adding fields. It keeps the existing fields' names and types, and adds only nullable fields: `executionId` as a `Long`, and the structured failure fields as `String`s. No new class appears in the serialized form, so an old reader never needs a class it lacks.

Under Java serialization's compatible-change rules, a new reader leaves absent fields `null` and an old reader ignores unknown fields. Old and new workers and masters therefore interoperate; a report from an old worker simply has no `executionId`. The first implementation slice pins both values and adds `TaskStateSerializationTest` in the `org.apache.seatunnel.engine.server.serializable` test package, before any field is added:

- The fixtures are the Java serialization bytes of fixed `TaskExecutionState` and `TaskDeployState` values, written by the unmodified classes on current `dev`. JDK 8 and JDK 11 produced identical bytes.
- The tests assert:
  - the fixtures deserialize with the pinned classes, field by field;
  - the pinned classes write exactly the fixture bytes;
  - each declared UID equals the previously generated value.

The slice that adds the optional fields keeps these fixtures. It adds tests that the new class reads the old bytes with the new fields `null`, and that bytes written by the new class are read by the unmodified class definition in an isolated class loader.

The history value and its `EntryProcessor` classes are new types. On members that do not have them, history operations fail and are dropped as best effort. The design does not claim failure history during a mixed-version rollout.

## Normative Rules

The tables below are the precise form of the rules above. Where prose and a table differ, the table applies.

### R1. Attempt key source and writer

| Moment | Value copied | Written by | Stored as |
|---|---|---|---|
| First creation of the pipeline state | The `CREATED` slot that the `SubPlan` constructor writes with `System.currentTimeMillis()` when the pipeline state is absent. The `INITIALIZING` slot, which holds `initializationTimestamp`, is not the key | The active master's `SubPlan` constructor | `SubPlan.currentKey`, registered by create |
| Constructor on a new active master, state present | The persisted `CREATED` value, unchanged. If that slot is null, the key is unknown | Nobody; it is only read | `SubPlan.currentKey`, registered by adopt |
| Restore | `max(now, previousCreated + 1)`, where `previousCreated` is read from the persisted timestamp array in the same execution of the write. If that slot is null, `now` | Only the active master's `SubPlan`. `resetPipelineState()` runs only from `SubPlan`'s `synchronized reset()` under `restoreLock` | `SubPlan.currentKey` is set from the value written by the execution of the write that returned without an exception. `RetryUtils` re-runs the whole write on a retry, so each retry computes a new value from the persisted one. A best-effort `registerAttempt` follows |
| Deployment of a task group | `SubPlan.currentKey` at the moment `getTaskGroupImmutableInformation()` generates the `executionId` | The active master's `PhysicalVertex` | The vertex's current deployment `(executionId, key)` |
| `PhysicalVertex.initStateFuture` during `JobMaster.init` with `restart = true`, vertex in `DEPLOYING`, `RUNNING`, `FAILING` or `CANCELING` | `SubPlan.currentKey`, before the liveness check | The new active master's `PhysicalVertex` | A placeholder deployment `(unknown, key)` |
| Timestamp array absent | None | Nobody | Key unknown: `attempt` and `attemptStartedAt` are null |

Each new key is derived from the persisted previous key, so no rule compares clocks of different masters. The only ordering comparison is "larger than every key in the attempt table", and keys increase by construction.

### R2. Report resolution

| Report | Resolved key | Result |
|---|---|---|
| `executionId` equals the vertex's current deployment | That deployment's key | Record, subject to deduplication |
| `executionId` equals the vertex's previous deployment | That deployment's key | Record under the previous attempt |
| `executionId` unknown, and the vertex holds a placeholder in either slot | The placeholder's key | Record. A report from a deployment older than the placeholder's execution cannot be told apart and is attributed to the placeholder |
| `executionId` unknown, and no placeholder | None | Not recorded. WARN with reason `STALE_EXECUTION`, through the per-job rate limiter |
| No `executionId` (node loss, master recovery, older worker) and a current deployment or placeholder exists | Its key | Record |
| No `executionId`, and neither exists | None | No task-group record |
| Deployment failure before an `executionId` exists | `SubPlan.currentKey` | `DEPLOY` record |
| Pipeline failure (`CHECKPOINT`, `RESOURCE`, `ENGINE`) | `SubPlan.currentKey` when captured | `PIPELINE` record |
| Same `(pipelineId, key, taskGroupId)` or `(pipelineId, key, source)` as a retained record | Same key | No-op; no sequence number consumed |

### R3. Dropped operations

An operation is dropped when its `offer` is rejected (queue full, the job's share used up, or the node no longer the active master), or when it exhausts three in-place retries. It is also dropped when the fence rejects it. Rejections by the fence are:

- `NO_ENTRY`: no entry exists;
- `OWNER_MISMATCH`: the entry has another owner;
- `TERMINAL`: the entry is terminal. This covers expected late writes.

Every dropped or rejected operation is logged at WARN through the per-job rate limiter, with identifiers and a reason code only. History does not count drops and does not claim completeness. What each drop leaves behind is:

| Dropped operation | What history shows | What REST shows | `/job-info` diagnostics |
|---|---|---|---|
| create | No entry. Later operations for the job are rejected as `NO_ENTRY` or, while another owner's entry remains, `OWNER_MISMATCH` | Known running job: empty `failures`. After it finishes: the empty list while its `JobInfo` exists, then the previous incarnation's snapshot, if one remains (Terminal handling) | Unchanged; `JobRuntimeDiagnostics` never reads failure history |
| adopt (after failover) | The entry keeps its records, but a current key is registered only when one of its records arrives | Existing records; attempt numbers may skip | Unchanged |
| `registerAttempt` | The execution receives a number only if one of its records arrives before a larger key is registered | An attempt without failures may be missing from `attempts` | Unchanged |
| append | That failure is absent. Other failures, including later ones of the same task group, are still recorded | The failure is missing | Unchanged |
| append carrying a suppressed total | `suppressedCount` stays at the last total received | Lower `suppressedCount` | Unchanged |
| finalization | The running entry stays live until another finalization offer or the sweep applies. The terminal time is still resolved from the sources listed in Terminal handling | Terminal job: its snapshot appears once a finalization applies; until then the empty list | Unchanged |

Restore, task-failure handling and existing job cleanup never wait on the queue, and a drop never changes their outcome.

### R4. History maps and in-memory state

| State | Key | Created by | Mutated by | Fence | Expiry | Cleanup owner | IMap storage |
|---|---|---|---|---|---|---|---|
| `engine_runningJobFailureHistory` | `jobId` | The consumer's create operation, offered by `JobMaster.init` right after the plan is built | Adopt, append, `registerAttempt`, `markTerminal`, `removeIfOwner` | Create: an unfinalized entry of another owner is finalized first, then replaced. Adopt: owner matches and not terminal. Append and register: entry exists, owner matches, not terminal. `markTerminal`: owner matches. `removeIfOwner`: owner matches and terminal | None while live. After `markTerminal`: `terminalTime + history-job-expire-minutes` | The consumer's finalization, offered by `cleanJob`, `processPendingJobCleanup`, `cleanupTerminalZombieJob`, `cleanupPendingJobStateForRestore` and the sweep | Excluded by `FileMapStore` |
| `engine_finishedJobFailureHistory` | `jobId` | Finalization step 2 | Nothing after the write | Written only when no snapshot exists, or the existing snapshot's owner has a smaller `initializationTimestamp` | `terminalTime + history-job-expire-minutes`, set at the write. Operators may add an eviction policy | Its TTL | Excluded by `FileMapStore` |
| Operation queue, coalescing counters, per-job WARN rate limiter | Per master; per job | `CoordinatorService` | Capture, consumer | The consumer checks it is still the active master before each operation | A job's counters and limiter state are removed when its finalization is offered. The sweep also removes them for jobs without `JobInfo` | `clearCoordinatorService()` discards everything | In memory only |
| Deployment slots and placeholders | Per `PhysicalVertex` | Deployment, `initStateFuture` during `init` with `restart = true` | Deployment, `reset()` | None needed; master-local | With the `JobMaster` | Discarded with the `JobMaster` | In memory only |

An orphan, as defined in Terminal handling, is removed without writing a snapshot. An example is the entry of a submission whose `init` failed after the create operation. Every other finalization writes a snapshot, even an empty one, and removes the running entry only after that write.

### R5. REST resolution

Checks run top to bottom. The job state is tested for absence before `isEndState()` is called. An absent state is never treated as running. On current `dev` it occurs in two windows:
- `submitJob` stores `JobInfo` before `init` writes the job state;
- `cleanupTerminalZombieJob` and `cleanupPendingJobStateForRestore` remove state keys before `JobInfo`.

| `JobInfo` | Job state in `runningJobStateIMap` | Running entry | Finished snapshot | Response |
|---|---|---|---|---|
| Present | Present, not an end state | Owner matches `JobInfo`, not terminal | Any | Running records |
| Present | Present, not an end state | Absent, another owner, or terminal | Any | Empty list |
| Present | End state, or absent | Any | Owner matches `JobInfo` | That snapshot |
| Present | End state, or absent | Any | Absent or another owner | Empty list |
| Absent | Any | Any | Present | That snapshot |
| Absent | Any | Any | Absent, but a finished job state exists | Empty list |
| Absent | Any | Any | Absent, and no finished job state | `404` |

A leftover running entry never makes an unknown or expired job appear known.

## Acceptance Criteria

1. A first-attempt task-group failure creates one `TASK_GROUP` record with attempt `0`.
2. A failure in a restored execution creates a record with the next attempt and a later `attemptStartedAt`. This holds even when the restored execution fails before its best-effort registration is applied, when the registration is dropped, and when the pipeline restored once without any recorded failure.
3. Duplicate delivery of one terminal state creates one record while the original is retained. A delayed duplicate may be recorded after eviction. Concurrent duplicates allocate one sequence number.
4. Failures from different task groups in the same attempt remain separate.
5. A late `FAILED` report carrying the previous deployment's `executionId` is recorded under the previous attempt key, not under the restored execution.
6. After an active-master change, reports from task groups that survived, including reports carrying an `executionId` assigned by the old master, are recorded under the current key through placeholder deployments.
7. A node-loss failure, a master-recovery lost task group, and a deployment failure each create one bounded record with the matching `source`. This includes a deployment that fails before a deployment identity exists. The deployment record's `exceptionType` identifies the original cause rather than `TaskGroupDeployException`. An internal state-update error in `updateTaskState` is not recorded as `DEPLOY`.
8. A checkpoint coordinator failure, including one where tasks also report `FAILED` during the cancel, a resource-allocation failure and an engine restore error each create one `PIPELINE` record with a null `taskGroupId`.
9. Capture for a failing task group is offered before its pipeline can be reset. This is tested with a single-task-group pipeline, where the end callback races the operation thread.
10. At an active-master change in each row of the crash-window table, attempts are numbered as stated. Retry eligibility and restore timing are unchanged.
11. A record whose key is not in the attempt table and is smaller than its largest key is stored with `attempt = null`. Attempt-table eviction removes the smallest key and never changes existing numbers or the next-number counter.
12. `resetPipelineState()` writes a `CREATED` timestamp strictly greater than the previous one, including when the clock steps backward and when the write is retried. The in-memory key equals the value written by the execution of the write that returned without an exception. The `rest-api-v2` documentation describes the field accordingly.
13. `attemptStartedAt` always equals the attempt key copied at reset or deploy time. It is never read from `runningJobStateTimestampsIMap` after a later restore.
14. More than 100 records, or more than 1 MiB of retained text, evicts records oldest first until both limits hold. The first record of each attempt is evicted last.
15. Redaction precedes any truncation, including the worker-side cap. A secret split across a truncation boundary is not stored. Messages over 4 KiB and stack traces over 64 KiB are truncated at valid UTF-8 boundaries, and they expose the truncation flag and the post-redaction, pre-truncation byte length in running and finished history.
16. The finished snapshot records its owner and caps each stack trace at 16 KiB and the total text at 256 KiB, keeping flags and lengths. It expires no later than `history-job-expire-minutes` after the terminal time.
17. More than 20 failures in one pipeline attempt submit at most 20 record operations and report the remainder in `attempts[].suppressedCount`. A retried operation does not count twice.
18. A full queue, and failed, retried and delayed history operations, do not block the capturing thread, do not re-enter failure or restore processing, and do not change the failure, restore or terminal outcome. Capture does no regex work, and the queue stays within its count, byte and per-job bounds.
19. No WARN or ERROR line produced by failure history contains message, stack-trace or task-name text. A marker secret in a dropped record never appears in any log line.
20. Append and attempt operations never create an entry. After `markTerminal` or removal they are no-ops, and they never remove or extend the terminal TTL. Captures accepted into the queue before a finalization are applied before the entry becomes terminal.
21. No running entry survives finalization. This includes after active-master failover, terminal-zombie recovery, a savepoint start that reuses the job ID, and an orphan found by the sweep. A failed finalization is retried by the sweep. It does not keep a `JobCleanupRecord` pending, delay existing state cleanup, or fail a savepoint-start submission.
22. A savepoint start that reuses a job ID finalizes the previous incarnation's unfinalized entry before creating its own entry. In-flight writes of the previous owner do not change the new entry.
23. The terminal time is anchored as specified, and recovery or cleanup retries never extend a deadline. A backup promoted after `markTerminal` keeps the expiry.
24. With `map.engine*` configured with a `map-store`, neither history map is written to IMap storage.
25. At REST:
    - a running job returns running records;
    - a terminal job returns only its own finished snapshot, including during `state-cleanup-delay-ms` and after its state keys are removed;
    - a running savepoint start that reuses the job ID never returns the previous incarnation's history, and a finished one returns it only in the two cases stated in Terminal handling;
    - a known job with no records, including after a failed or evicted snapshot, returns an empty list;
    - an unknown or expired job returns `404` even when a leftover entry exists.
26. Only a raw URI matching the specified grammar serves failure history. Legacy single-ID routes keep their behavior, including malformed-ID `400` responses. Alias failure-history requests, extra segments, trailing slashes, `%`-encoded characters, `;` parameters, dot segments and empty segments receive the controlled `404`.
27. Every non-200 response of the route has a fixed JSON body, set by the handler, that contains no request input and no stack trace. This is tested with a unique marker in the URI and a map read failure whose message carries a marker. No response comes from `sendError` or Jetty's error page.
28. On the new route, absent, empty, repeated, zero, negative, non-numeric, Unicode-digit, signed and overflowing limits, and overflowing job IDs, return the specified responses.
29. The endpoint uses the same configured REST authentication boundary as existing job-detail endpoints. No history record or response contains a worker-address attribution field.
30. Every redaction item listed in Security has a positive and a near-miss test. The tests include secrets across a truncation boundary, PEM blocks, escaped quotes and JAAS text with and without its prefix. Each pattern finishes within a fixed budget on adversarial 1 MiB input, and redaction is idempotent.
31. The `TaskExecutionState` and `TaskDeployState` UID guards and byte fixtures pass: the first slice asserts identical bytes, and the slice that adds fields adds both old/new directions. Existing `errorMsg` clients are unaffected.
32. Existing `job.retry.times`, restore eligibility and restore scheduling never read or wait on failure-history state, including after active-master failover.
33. English and Chinese documentation describe the same contract.
34. Each row of R2 has a test, including an unknown `executionId` with and without a placeholder, and a duplicate that consumes no sequence number.
35. Each row of R3 has a test that forces the drop or rejection (full queue, per-job share, exhausted retries, `NO_ENTRY`, `OWNER_MISMATCH`, `TERMINAL`). The test asserts the history and REST result in the table, the WARN reason code, that `/job-info` diagnostics are unchanged, and that restore and cleanup timing are unaffected.
36. A finished snapshot never replaces a snapshot whose owner has a larger `initializationTimestamp`. Only an orphan, as defined in Terminal handling, is removed without a snapshot. A savepoint start that finished without failures gets an empty snapshot of its own. A running entry is removed only after its snapshot is written.
37. Each row of R4 and R5 has a test, including an absent job state while `JobInfo` exists, and a savepoint start whose create operation has not yet run.

## Delivery Plan

1. Agree on this contract in STIP-33.
2. Structured failure transport:
   - pin both serial UIDs;
   - add the nullable `executionId` and failure fields;
   - track deployment identity per vertex, including placeholders after failover;
   - make the `CREATED` timestamp strictly increasing and update its `rest-api-v2` description;
   - add the compatibility tests.
3. History store: the two maps, the `FileMapStore` exclusion, the `EntryProcessor`s, ownership, bounds, the terminal fence and finalization, and the sweep, with failover and cleanup tests.
4. Capture: the capture points, the consumer queue, coalescing, text processing and the shared redaction utility, with capture-ordering, crash-window, redaction and logging tests.
5. REST: routing and the endpoint, with the authentication-boundary, URI-grammar, error-body, validation, compatibility and running/finished tests. Also the `rest-api-v2` and engine security documentation in EN and ZH.
6. Web UI history view, in a separate pull request.
