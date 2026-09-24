# Task Failure History Design

This document proposes the first backend contract for [GH-11667](https://github.com/apache/seatunnel/issues/11667). It does not describe an implemented API yet.

## Problem

The Job Detail page currently exposes one exception string. This is not enough when a pipeline is restored several times or when different task groups fail during the same job. Operators need to see which pipeline attempt and task group failed, and when it happened without searching every worker log. The first version attributes failures to pipelines and task groups, not worker addresses.

The current engine state has three relevant limitations:

- `PhysicalPlan` retains only the first error reported by a sub-plan.
- `TaskExecutionState` carries a formatted throwable message, but no durable failure identity.
- finished-job history keeps the final error text, not the sequence of failures that led to the terminal state.

## Scope

The first implementation should provide a bounded, job-scoped failure history that is available for running and finished jobs through the same REST contract.

It should:

- group failures by pipeline execution attempt;
- identify the pipeline and task group that reported the failure;
- retain task metadata when it is available, without storing worker addresses as failure-history attribution fields;
- preserve timestamp, message, stack trace, and exception type without parsing display text;
- survive master failover and pipeline restore;
- expire with the existing finished-job history policy; and
- keep the existing single `errorMsg` field for compatibility.

The first implementation should not add log aggregation, distributed tracing, or an unbounded exception archive. The Web UI is a follow-up after the backend contract is agreed.

## Attempt Model

An attempt belongs to a pipeline, not to an individual task.

- The initial pipeline execution is attempt `0`.
- A restored execution receives the next attempt number when failure history first observes its attempt key.
- Every failure captured during that execution carries the same attempt number.
- The diagnostic attempt identity must be stored in HA state so a new active master does not restart numbering at `0`.

`SubPlan.pipelineRestoreNum` currently participates in the `job.retry.times` decision. Making it durable and reusing it for history would also make the retry budget survive an active-master failover, which is a separate behavior change. The first implementation must therefore keep the existing retry counter and retry-limit behavior unchanged.

Failure history stores a separate durable diagnostic attempt identity in the job-scoped history state. It never participates in restore eligibility or retry-limit checks, and no restore step waits for it.

**Attempt key.** The durable identity of one pipeline execution is that pipeline's `CREATED` timestamp in `runningJobStateTimestampsIMap`. Current `dev` writes this slot in two places only: in the `SubPlan` constructor when it first creates the pipeline state, and in `SubPlan.resetPipelineState()` before a restore. The reset writes the timestamp first and then sets the pipeline state to `CREATED`. The constructor keeps an existing value, so an active-master switch that rebuilds the `SubPlan` preserves the key of the execution that is still running. This existing durable value is the restore-decision identity. `pipelineRestoreNum` is kept in memory and restarts at `0` when the `SubPlan` is rebuilt, so it is not used.

**Attempt number.** For each pipeline, the history entry stores the current attempt key and attempt number, plus the key-to-attempt mapping of retained records. One operation, `resolveAttempt(pipelineId, key)`, runs inside the same job-scoped `EntryProcessor` as record appends:

- the pipeline has no attempt yet: `key` becomes attempt `0`;
- `key` equals the current key, or belongs to a retained record: return that attempt unchanged;
- otherwise: advance to the current attempt plus one and make `key` current.

After `reset()` in `prepareRestorePipeline()`, the master submits a best-effort `resolveAttempt` for the new key. Every failure record also carries the key of the execution that produced it, read from the `SubPlan` when the failure is accepted, and resolves its attempt in the same atomic update as the append. Correct grouping therefore depends on neither the explicit advance nor its acknowledgement. A lost acknowledgement or a retry is idempotent, because the key is the same. If an explicit advance is dropped and that execution records no failure, the execution receives no number and the next recorded execution takes the next number. `attempt` is therefore a monotonic ordinal of executions observed by failure history, and `attemptStartedAt` is the exact identity.

**Crash windows.** The following follows current `dev` behavior in `SubPlan` and `restorePipelineState()`, where a new active master cancels and restores any pipeline whose state is below `RUNNING`:

| Active master fails | Durable state seen by the new master | Engine action on `dev` | Diagnostic result |
|---|---|---|---|
| Before `resetPipelineState()` writes the state | `FAILED` or `CANCELED`; the `CREATED` slot may already hold an unused value | Restores again and writes a new key | One advance, for the execution that actually starts |
| After reset, before `resolveAttempt` is applied | `CREATED` with key `K1` | Cancels, restores, writes `K2` | `K1` never deployed and receives no number; `K2` advances once |
| After `resolveAttempt(K1)`, before or during deployment | `CREATED`, `SCHEDULED` or `DEPLOYING` with `K1` | Cancels, restores, writes `K2` | `K1` keeps its number and any deployment failures; `K2` takes the next number. The engine ran two restores, so this is not double counting |
| While `RUNNING` | `RUNNING` with `K` | Continues; the rebuilt `SubPlan` keeps `K` | No advance; new failures resolve to the existing attempt |

**Fencing.** Current `dev` has no durable master epoch. `resetPipelineState()` and `updatePipelineState()` write `runningJobStateIMap` without an owner check. Diagnostic state relies on the same single-active-master assumption as the pipeline state it describes, and does not claim stronger stale-master fencing. Within that assumption:

- one FIFO submission path per master (see Capture and Deduplication) orders operations. Records of execution `K1` are enqueued before its pipeline reaches `FAILED` or `CANCELED` and is reset, so they are applied before the advance for `K2`;
- after failover, the old master's unapplied operations are lost, not replayed; and
- writes from a previous job incarnation, or writes after the terminal transition, are rejected by the owner and terminal checks described in Storage and Retention.

A split-brain second active master is outside the first version's guarantee, as it is for existing job state.

`canRestorePipeline()` and `job.retry.times` never read diagnostic state, and no restore step waits on a history operation, so restore eligibility and availability are unchanged. On `dev`, the in-memory retry counter restarts when a new active master rebuilds the `SubPlan`; this design does not change that behavior.

## Failure Record

The proposed REST representation is:

```json
{
  "sequence": 7,
  "timestamp": 1753574400000,
  "jobId": "123456789",
  "pipelineId": 1,
  "attempt": 2,
  "attemptStartedAt": 1753574380000,
  "taskGroupId": 4,
  "taskId": null,
  "taskName": "mysql-source -> transform",
  "exceptionType": "java.sql.SQLException",
  "message": "Connection reset",
  "messageOriginalBytes": 16,
  "messageTruncated": false,
  "stackTrace": "java.sql.SQLException: Connection reset\n...",
  "stackTraceOriginalBytes": 43,
  "stackTraceTruncated": false
}
```

Field rules:

- `sequence` is monotonically increasing within one job and provides deterministic ordering when timestamps are equal.
- `timestamp`, `jobId`, `pipelineId`, `attempt`, and `taskGroupId` are required.
- `attemptStartedAt` is the attempt key: the pipeline's durable `CREATED` timestamp for that execution in `runningJobStateTimestampsIMap`, written when the pipeline was created or reset for restore. It is optional for legacy or synthetic paths that cannot resolve this metadata. It has the same value for every record with the same `pipelineId` and `attempt` and is distinct from `timestamp`, which records when the individual failure was captured.
- `taskId`, `taskName`, `exceptionType`, `message`, and `stackTrace` are optional because older or synthetic failure paths may not provide them. The first version has no worker-address attribution field in running history, finished history, or REST responses.
- `messageTruncated` and `stackTraceTruncated` are required booleans. They indicate whether the corresponding value was shortened before storage.
- `messageOriginalBytes` and `stackTraceOriginalBytes` are the UTF-8 byte lengths after redaction but before truncation. They are present for non-null text and null when that text is unavailable. The finished snapshot retains these lengths and the truncation flags with each record; raw pre-redaction lengths are not persisted.
- `exceptionType` must come from structured failure transport. It must not be inferred by parsing the formatted stack trace.
- `stackTrace` remains the diagnostic detail; `message` is the concise display value.
- The stored UTF-8 representation is limited to 4 KiB for `message` and 64 KiB for `stackTrace`. Truncation must preserve valid UTF-8. A truncated message keeps its prefix. A truncated stack trace keeps both its beginning and end so the exception and the deepest cause remain available.

## Capture and Deduplication

`TaskExecutionState` remains the structured worker-to-master failure transport, but it is not the only way a task group can fail. The common capture point for terminal worker-reported failures is the `PhysicalVertex` state transition after `updateStateByExecutionService` accepts a `FAILED` state. This covers both normal worker reports and node-loss state updates that are routed directly to the physical vertex.

Deployment failures do not carry a `TaskExecutionState`; they enter through `makeTaskGroupFailing`. That path must create a failure record from the deployment exception and the known pipeline and task-group metadata, without copying slot or worker addresses into the history record. `TaskDeployState.failed(Throwable)` must extract the original failure's class name, message, and bounded stack trace as strings while the `Throwable` is still available. For `deployOnRemote`, these strings are captured on the worker before the response crosses the Hazelcast RPC boundary; the live `Throwable` must not be included because its connector-specific class may not be available to the master. The resulting record reads those structured fields directly, so its `exceptionType` identifies the original cause rather than `TaskGroupDeployException`. The same deduplication key prevents a later terminal delivery for that attempt from creating another record. Cancellation without a failure cause is not recorded as an exception.

Exception content must be sanitized and bounded at these capture boundaries, before it is written to HA or finished-job history. The implementation should extract the redaction patterns from `DryRunConnectFailureMessageSanitizer` into a shared utility rather than persisting raw connector messages or stack traces. Pattern-based redaction is best effort: unknown credential formats and other sensitive diagnostic text can remain, so access control is still required. Tests must cover supported credential formats and adversarial variants. Failure history keeps its own 4 KiB message and 64 KiB stack-trace limits and truncation flags; it must not inherit the dry-run utility's 2 KiB display limit.

Repeated delivery of the same terminal task-group state must not create duplicate rows while the original record is retained. The first implementation deduplicates on `(pipelineId, attempt, taskGroupId)`, because a task group has one terminal failure for one pipeline attempt. A Hazelcast `EntryProcessor` on the job-scoped HA entry performs the deduplication check, sequence allocation, append, and oldest-record eviction as one atomic operation. It must be submitted asynchronously from the task-status operation path. Completion handling can log a store failure, but must not wait on or re-enter a Hazelcast operation thread. The first terminal delivery creates the record and receives a sequence number, while later deliveries with the same key are ignored without consuming another sequence number. A different task group in the same attempt remains separate, and a failure after restore has a different attempt number and remains visible.

Deduplication uses the retained records as its bounded key set. After a record is evicted by either the 100-record limit or the 1 MiB aggregate-text limit, a delayed duplicate for that key can be recorded again. The first version does not keep a separate unbounded set of every key seen during the job lifetime.

Recording history is diagnostic and best effort. Each master's `CoordinatorService` owns one bounded FIFO queue and one dedicated consumer thread for failure-history operations. Task-status reports reach the master on a Hazelcast operation thread through `NotifyTaskStatusOperation`, `CoordinatorService.updateTaskExecutionState`, `JobMaster.updateTaskExecutionState` and `PhysicalVertex.updateStateByExecutionService`. That path, `makeTaskGroupFailing`, and restore scheduling only call a non-blocking `offer`; when the queue is full, the operation is logged and dropped. The consumer runs each `EntryProcessor` outside Hazelcast operation threads and outside the job scheduling executor. It retries only failures that Hazelcast classifies as retryable, a bounded number of times, and then logs and drops the operation. Completion never changes task or pipeline state, never calls back into `JobMaster` or `SubPlan`, and never re-enters failure or restore processing. No failure-history operation, including the attempt advance, is required for a task failure, restore decision or restored execution to proceed.

## Storage and Retention

Failure history should use a dedicated HA-backed engine state entry keyed by `jobId`. The first implementation can use a dedicated Hazelcast `IMap` with the same default Hazelcast `MapConfig` baseline as the engine's existing job-state maps. It does not introduce additional backups, persistence, or an external history backend. The REST representation remains independent of that storage choice.

**Ownership.** Each entry records its owner as `(jobId, initializationTimestamp)` from `JobInfo`. This is the same identity current `dev` uses for cleanup ownership (`isCleanupOwnedByCurrentJob` and `JobCleanupRecord.ownerInitializationTimestamp`); neither `JobInfo` nor `JobCleanupRecord` changes. Only `JobMaster` initialization of a new submission creates an entry, including a savepoint start that reuses a job ID. It replaces any existing entry for that ID with an empty entry for the new owner. Active-master recovery adopts an existing entry only when its owner matches the recovered `JobInfo` and the entry is not terminal. When no matching entry exists, for example for a job started before the feature was deployed or after a failed best-effort creation, the job records no failure history and REST returns the known-job empty list. Append and attempt operations never create an entry.

The owner identity is only as unique as `initializationTimestamp`. On `dev`, `submitJob` already rejects a non-savepoint resubmission of a used job ID, so reuse happens through a savepoint start. A collision would require the new initialization to fall on exactly the previous incarnation's millisecond, which needs a backward clock step. Existing cleanup ownership has the same limit, and this design does not widen it. A random token stored only in the history entry would not help, because recovery would have to trust the entry to learn the token.

The first version uses these bounds:

- retain at most 100 failure records per job;
- retain at most 1 MiB of combined UTF-8 text content per job across `message`, `stackTrace`, `taskName`, and `exceptionType`; cap each `taskName` and `exceptionType` at 1 KiB before storage and apply best-effort redaction to them too;
- evict the oldest records until both the record-count and aggregate-text limits are satisfied;
- do not apply a TTL while the job is active; and
- after the job reaches a terminal state, give the dedicated finished-history entry its own `history-job-expire-minutes` TTL and apply that retention period to any remaining running-history entry as a cleanup fallback.

The initial limit should be a constant rather than a new user option. A configurable limit can be added later if operational evidence shows that 100 records is insufficient.

At the terminal transition, the finalize sequence below writes the retained records and pipeline attempt values to a dedicated finished-history entry. That entry expires `history-job-expire-minutes` after the job's terminal time, so expiration does not depend on a cleanup-listener callback. A listener may remove it eagerly when the finished-job record is deleted, but the entry's own TTL remains the fallback guarantee. This reuses the existing finished-job lifecycle without introducing a pluggable history-store abstraction.

The terminal snapshot write is best effort. A write or cleanup failure must be logged, but must not change the job terminal state, restore behavior, or existing finished-job record. Running and finished reads use the same response model even though their storage lifecycle differs.

**Terminal fence.** The history `EntryProcessor` enforces the fence. In the same atomic per-key update as its mutation, every append or attempt operation checks that the entry exists, that its owner equals the operation's `(jobId, initializationTimestamp)`, and that it is not terminal. Otherwise it returns without writing, so an absent key is never recreated. Terminal handling has three idempotent steps. `JobMaster.cleanJob()` submits them through the same FIFO queue, after every earlier capture operation, so failures that led to the terminal state are applied first:

1. `markTerminal(owner, terminalTime)`: when the owner matches and the entry is not terminal, set `terminal = true`, store `terminalTime`, and set the entry TTL to `terminalTime + history-job-expire-minutes - now` through `ExtendedMapEntry.setValue(value, ttl, unit)`, which the shaded Hazelcast 5.1 provides. A non-positive remainder removes the entry. An already terminal entry is returned without calling `setValue`, so no retry or recovery can extend its deadline. The processor returns the frozen records.
2. Write the finished-history snapshot from the frozen records with the same remaining TTL. This step is best effort.
3. `removeIfOwner(owner)`: remove the entry only when its owner matches and it is terminal.

**Cleanup and recovery.** The history entry itself is the durable cleanup obligation: it exists until removal or its terminal TTL. It is not added to `JobCleanupRecord`. That record's `IdentifiedDataSerializable` wire form, its two key sets, and its consumers (`JobMaster.createJobCleanupRecord`, `CoordinatorService.cleanupPendingJobStateMaps` and `createTerminalZombieCleanupRecord`) are unchanged. Instead, an owner-conditional finalize runs steps 1 to 3, skipping step 2 when a finished snapshot already exists, and is invoked best effort from:

- `processPendingJobCleanup`, with `ownerInitializationTimestamp`;
- `cleanupTerminalZombieJob`, with the `JobInfo` initialization timestamp;
- `cleanupPendingJobStateForRestore`, before a savepoint start initializes the new incarnation; and
- a sweep of the history map on active-master activation and on the existing periodic pending-cleanup schedule. The sweep finalizes any entry whose owner has no matching `JobInfo` in `runningJobInfoIMap`, including an orphan left by a failed submission.

A failed finalize is logged and not thrown into those paths. It cannot keep a `JobCleanupRecord` pending, delay existing state cleanup, or fail a savepoint-start submission; the next sweep retries it. The terminal time comes from the job end-state timestamp in `runningJobStateTimestampsIMap`, falling back to the cleanup record's `createTimeMillis`, which is written at the terminal transition. An orphan with neither uses the time the sweep finds it. That case, and only that case, is not anchored to the original terminal time, and it is logged.

This replaces the earlier plan to extend `JobCleanupRecord`. A third key set would change a shared `IdentifiedDataSerializable` with HA-persisted instances, and `IMAP_PENDING_JOB_CLEANUP` stores one record per job ID with `put`. It would also let a diagnostic failure keep existing state cleanup pending.

Once the job is terminal, REST reads only the finished-history snapshot, never the running entry. A failed best-effort snapshot may leave a known finished job with no failure records; keeping diagnostics indefinitely is not a fallback.

Master failover preserves records already acknowledged by the HA history store and resumes sequence numbers and attempt keys from that persisted state. Because history submission is asynchronous and best effort, an operation still queued when the active master fails may be lost. This diagnostic path does not delay the task failure or restore decision to wait for a history acknowledgement.

## REST Contract

The proposed endpoint is:

```text
GET /job-info/{jobId}/failures?limit=100
```

Behavior:

- return records in descending `sequence` order;
- default `limit` to 100 and reject non-positive values;
- cap requested limits at the retained maximum;
- return an empty list for a known job with no failures;
- return a controlled `404` response for an unknown or expired job; and
- return the same response model for running and finished jobs, regardless of which dedicated state entry supplies the records.

`JobInfoServlet` currently parses the entire path information as one numeric job ID and also serves the deprecated `/running-job/*` alias. The implementation must distinguish the servlet mapping and exact path segments, or use an equivalent dedicated handler. Only `/job-info/{jobId}/failures` serves failure history. The single-ID routes `/job-info/{jobId}` and `/running-job/{jobId}` keep their existing behavior, including their existing `400` response for malformed IDs. Any other path shape under these mappings, including `/running-job/{jobId}/failures` and extra segments after `/failures`, returns a controlled `404` without reflecting input or exposing a stack trace. Prefix or substring matching is not allowed. This explicitly changes invalid multi-segment paths from today's numeric-parse `400` to `404`; it does not claim that this not-found behavior already exists.

The current `/job-info/{jobId}` behavior and its `errorMsg` field remain unchanged, including its existing response for an unknown job. The new failure-history endpoint defines its own explicit `404` response so callers can distinguish an unknown job from a known job with no failures.
The endpoint determines job existence from the running-job or finished-job record, not from the presence of a failure-history entry. A known job with no entry, including a failed best-effort snapshot, returns an empty list; once the corresponding job record has expired, a leftover history entry must not make the job appear known.

## Security and Input Validation

The endpoint uses the same `BasicAuthFilter` boundary as the existing engine REST API. It must not introduce an endpoint-specific authentication mechanism. This is an operator-facing endpoint: the existing boundary does not provide per-job or per-tenant authorization, and authenticated REST users can access job diagnostics. Deployments requiring narrower access must enforce it at their existing gateway or network boundary. With REST authentication disabled, callers reaching the endpoint can read the diagnostics; operators must restrict access rather than assume the new route supplies authorization. Exception text can contain operationally sensitive information even after credential redaction.

Best-effort redaction and UTF-8-safe truncation are applied at capture, before every new failure-history HA IMap or finished-history write, not only while serializing a REST response. This includes worker-reported, deployment, node-loss, and terminal-snapshot paths. The new history storage and API response use the same bounded representation. Existing job `errorMsg` storage and REST behavior are unchanged; this design does not sanitize that legacy path or guarantee that all secrets have been removed from diagnostics.

The new failure-history route handler owns validation of `jobId` and `limit`; these rules do not change validation on the legacy single-ID routes:

- malformed or overflowing signed-64-bit job identifiers and non-numeric, overflowing signed-32-bit, or non-positive limits return a controlled `400` response;
- an absent limit defaults to 100; valid positive limits above 100 are capped at 100; and
- validation failures must not include a stack trace or echo untrusted input through the shared exception handler.

The first version neither persists worker `host:port` attribution fields in running or finished failure history nor exposes them in REST responses. Capture code may consult existing execution metadata, but must not copy its addresses into history fields. This avoids retaining topology data with no consumer in this version; it does not promise to remove every address mentioned inside sanitized exception text. Existing execution metadata and `/pending-jobs` behavior are unchanged. An operator-only opt-in or logical worker identifier requires a separate agreed contract; this design does not introduce a new role system or address-exposure option.

## Web UI Follow-up

The Exception tab can consume the REST endpoint in a separate change. The first UI version should group records by attempt and show timestamp, pipeline, task group, task name, exception type, and message. It must not infer or expose worker addresses omitted by the API. Stack traces should be collapsed by default.

Missing optional fields should be displayed as unavailable. The UI must not claim task-level precision when the engine only supplied a task-group failure.

## Compatibility

The feature is additive for valid existing job-detail requests. Invalid multi-segment paths change from numeric-parse `400` to controlled `404`, as specified above:

- existing jobs do not need configuration changes;
- existing REST fields and the final error message remain available;
- no checkpoint or savepoint payload is changed;
- `job.retry.times`, restore eligibility and restore availability are unchanged, because no restore step reads or waits on diagnostic state; and
- old failure paths can populate only the fields they know.

`TaskExecutionState` is Java-serialized between workers and the master: `NotifyTaskStatusOperation.writeInternal` uses `writeObject`. It declares no `serialVersionUID`. The value generated for the current class, whose source has not changed since March 2023, is `-108652017022658969L`, computed with `serialver` on both JDK 8 and JDK 11. The implementation declares exactly that value before adding fields, keeps the three existing fields' names and types, and adds only nullable fields. Under Java serialization's compatible-change rules, a new reader leaves absent fields `null` and an old reader ignores unknown fields. Tests cover both directions: a byte fixture written by the unchanged class deserializes with the new class and keeps its existing fields, and a value written by the new class with the optional fields set deserializes with the old class definition. A guard test fails if the declared UID changes.

`JobInfo`, `JobCleanupRecord`, and checkpoint and savepoint payloads are unchanged. The history value and its `EntryProcessor` classes are new types. On members that do not have them, history operations fail and are dropped as best effort. The design does not claim failure history during a mixed-version rollout.

## Acceptance Criteria

1. A first-attempt task-group failure creates one record with attempt `0`.
2. A restored pipeline failure creates another record with the incremented attempt.
3. Duplicate delivery of one terminal state does not create a duplicate record while the original record remains in the bounded history; a delayed duplicate may be recorded after eviction.
4. Failures from different task groups in the same attempt remain separate.
5. A master failover preserves records acknowledged by the HA history store and the next attempt number; an asynchronous submission still in flight at failover may be lost.
6. A successfully written finished-history snapshot exposes the retained records until the configured history expiration; a failed best-effort snapshot may leave a known finished job with no failure records.
7. More than 100 failures, or more than 1 MiB of retained UTF-8 text across all four variable-length fields, evicts the oldest records deterministically until both limits are satisfied.
8. Messages larger than 4 KiB and stack traces larger than 64 KiB are truncated at valid UTF-8 boundaries and expose the corresponding truncation flag and post-redaction, pre-truncation byte length in running and finished history.
9. A terminal job writes one bounded failure-history entry that expires with its corresponding finished-job record.
10. Best-effort failure-record, attempt, finished-snapshot, or cleanup errors do not change the job failure, restore, restore availability, or terminal-state path.
11. Existing job-detail clients continue to receive the current `errorMsg` field.
12. Concurrent duplicate deliveries create one record and allocate one sequence number through the atomic job-entry update.
13. Supported credential patterns in all persisted text fields are redacted before new failure-history HA and finished-history persistence; adversarial variants are tested, and documentation warns that this is best effort rather than a complete secret-removal guarantee.
14. On the new failure-history route, malformed `jobId` or `limit` input returns a controlled `400` response without exposing a stack trace or reflecting the invalid value.
15. The endpoint is covered by the same configured REST authentication boundary as existing job-detail endpoints.
16. Failure-history updates are submitted asynchronously and do not block a Hazelcast operation thread.
17. Failure-history state uses the same default Hazelcast map configuration as the existing job-state maps and does not add backups or persistence.
18. Only the exact `/job-info/{jobId}/failures` route serves failure history. Legacy single-ID `/job-info/{jobId}` and `/running-job/{jobId}` requests retain their behavior, including malformed-ID `400` responses. Other path shapes, including alias failure-history requests and extra segments, return controlled `404` responses without reflected input or stack traces.
19. Existing serialized `TaskExecutionState` values remain readable after the optional structured failure fields are added.
20. A deployment failure that enters through `makeTaskGroupFailing` creates one bounded record even though no `TaskExecutionState` exists, and its `exceptionType` identifies the original cause rather than `TaskGroupDeployException` when a cause is available.
21. Diagnostic attempt state does not enter `job.retry.times`, restore eligibility, or restore scheduling, including after active-master failover; no restore step waits on a history operation.
22. `attemptStartedAt` equals the pipeline's durable `CREATED` timestamp for that execution.
23. Failures resolve their attempt from the durable attempt key in the same atomic update as the append. A lost or dropped advance acknowledgement does not group two executions under one attempt, and a retried advance with the same key does not advance twice.
24. Running-history entries, finished-history snapshots and REST responses contain no worker-address attribution field. Enabling or disabling REST authentication does not add per-job authorization or expose an opt-in address field; existing execution metadata is unchanged.
25. Tests cover absent, zero, negative, non-numeric, overflowing, and above-maximum limits, plus overflowing job IDs and extra path segments.
26. No running-history entry survives finalize, including after active-master failover, terminal-zombie recovery, a savepoint start that reuses the job ID, and an orphan found by the sweep. Finalize removes only an entry whose owner matches. A failed finalize is retried by the sweep and does not keep a `JobCleanupRecord` pending, delay existing state cleanup, or fail a savepoint-start submission.
27. A remaining terminal running-history entry expires using `history-job-expire-minutes` from the original terminal time even when eager removal fails; cleanup retries and master recovery do not extend the deadline.
28. Append and attempt operations never create an entry. After `markTerminal` or removal they are no-ops, and they never remove or extend the terminal TTL. Terminal REST reads never fall back to running entries.
29. Tests cover: (a) a restore with a master change at each crash window in the Attempt Model table, a lost advance acknowledgement with retry, and a dropped advance, with retry eligibility and restore timing unchanged; (b) a full queue and failed, retried, and delayed history writes, with no blocking of the task-status operation, no callback re-entry, and an unchanged failure or restore outcome; (c) the `TaskExecutionState` UID guard and byte fixtures in both directions; (d) late appends and attempt operations before, during, and after `markTerminal` and removal; (e) failed removal followed by the sweep, terminal-time-anchored expiry across recovery, a savepoint start reusing a job ID with an in-flight write from the old owner, orphan cleanup after a failed submission, and a recovered job with no entry; and (f) at REST, a known job with no records, including after a failed snapshot, versus an unknown or expired job that still has a leftover entry.

## Delivery Plan

1. Agree on the record, attempt, storage, retention, and REST contracts.
2. Add the dedicated HA-backed running and finished history entries, atomic `EntryProcessor`, and structured task failure transport with unit tests.
3. Add capture-time sanitization, deduplication, retention, and restore tests.
4. Add the REST routing and endpoint with authentication-boundary, validation, backward-compatibility, and running/finished job API tests.
5. Add the Web UI history view in a separate pull request.
