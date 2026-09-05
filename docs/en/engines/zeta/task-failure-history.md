# Task Failure History Design

This document proposes the first backend contract for [GH-11667](https://github.com/apache/seatunnel/issues/11667). It does not describe an implemented API yet.

## Problem

The Job Detail page currently exposes one exception string. This is not enough when a pipeline is restored several times or when different task groups fail during the same job. Operators need to see which attempt failed, where it ran, and when it happened without searching every worker log.

The current engine state has three relevant limitations:

- `PhysicalPlan` retains only the first error reported by a sub-plan.
- `TaskExecutionState` carries a formatted throwable message, but no durable failure identity.
- finished-job history keeps the final error text, not the sequence of failures that led to the terminal state.

## Scope

The first implementation should provide a bounded, job-scoped failure history that is available for running and finished jobs through the same REST contract.

It should:

- group failures by pipeline execution attempt;
- identify the pipeline and task group that reported the failure;
- retain the worker address and task metadata when they are available;
- preserve timestamp, message, stack trace, and exception type without parsing display text;
- survive master failover and pipeline restore;
- expire with the existing finished-job history policy; and
- keep the existing single `errorMsg` field for compatibility.

The first implementation should not add log aggregation, distributed tracing, or an unbounded exception archive. The Web UI is a follow-up after the backend contract is agreed.

## Attempt Model

An attempt belongs to a pipeline, not to an individual task.

- The initial pipeline execution is attempt `0`.
- The attempt increments before a restore is scheduled.
- Every failure captured during that execution carries the same attempt number.
- The diagnostic attempt identity must be stored in HA state so a new active master does not restart numbering at `0`.

`SubPlan.pipelineRestoreNum` currently participates in the `job.retry.times` decision. Making it durable and reusing it for history would also make the retry budget survive an active-master failover, which is a separate behavior change. The first implementation must therefore keep the existing retry counter and retry-limit behavior unchanged.

Failure history stores a separate durable diagnostic attempt identity in the job-scoped history state. The initial identity is created as attempt `0`. Before a restore starts a new execution, the history entry atomically advances that pipeline's diagnostic attempt and records its start time. A new active master reads this identity before recording another failure. This counter is used only for failure correlation and REST output; it must not participate in restore eligibility or retry-limit checks.

The attempt advance uses the same job-scoped `EntryProcessor` serialization boundary as failure-record updates, so it cannot overwrite a concurrent record from another pipeline in the same job. Unlike best-effort failure-record capture, the advance is part of restore scheduling: it must complete before the restored execution can start. If the write fails, restore scheduling retries or fails without starting the new execution under a stale attempt identity.

This keeps the diagnostic model aligned with the existing pipeline restore boundary without changing retry semantics.

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
  "worker": "10.0.0.12:5801",
  "exceptionType": "java.sql.SQLException",
  "message": "Connection reset",
  "messageTruncated": false,
  "stackTrace": "java.sql.SQLException: Connection reset\n...",
  "stackTraceTruncated": false
}
```

Field rules:

- `sequence` is monotonically increasing within one job and provides deterministic ordering when timestamps are equal.
- `timestamp`, `jobId`, `pipelineId`, `attempt`, and `taskGroupId` are required.
- `attemptStartedAt` is the start time stored with the durable diagnostic attempt metadata. Attempt `0` is initialized when the pipeline execution is created for deployment, and a restored attempt is initialized when its diagnostic attempt identity is advanced before restore scheduling. The field is optional for legacy or synthetic paths that cannot resolve this metadata. It has the same value for every record with the same `pipelineId` and `attempt` and is distinct from `timestamp`, which records when the individual failure was captured.
- `taskId`, `taskName`, `worker`, `exceptionType`, `message`, and `stackTrace` are optional because older or synthetic failure paths may not provide them.
- `messageTruncated` and `stackTraceTruncated` are required booleans. They indicate whether the corresponding value was shortened before storage.
- `exceptionType` must come from structured failure transport. It must not be inferred by parsing the formatted stack trace.
- `stackTrace` remains the diagnostic detail; `message` is the concise display value.
- The stored UTF-8 representation is limited to 4 KiB for `message` and 64 KiB for `stackTrace`. Truncation must preserve valid UTF-8. A truncated message keeps its prefix. A truncated stack trace keeps both its beginning and end so the exception and the deepest cause remain available.

## Capture and Deduplication

`TaskExecutionState` remains the structured worker-to-master failure transport, but it is not the only way a task group can fail. The common capture point for terminal worker-reported failures is the `PhysicalVertex` state transition after `updateStateByExecutionService` accepts a `FAILED` state. This covers both normal worker reports and node-loss state updates that are routed directly to the physical vertex.

Deployment failures do not carry a `TaskExecutionState`; they enter through `makeTaskGroupFailing`. That path must create a failure record from the deployment exception and the known pipeline, task-group, slot, and worker metadata. `TaskDeployState.failed(Throwable)` must extract the original failure's class name, message, and bounded stack trace as strings while the `Throwable` is still available. For `deployOnRemote`, these strings are captured on the worker before the response crosses the Hazelcast RPC boundary; the live `Throwable` must not be included because its connector-specific class may not be available to the master. The resulting record reads those structured fields directly, so its `exceptionType` identifies the original cause rather than `TaskGroupDeployException`. The same deduplication key prevents a later terminal delivery for that attempt from creating another record. Cancellation without a failure cause is not recorded as an exception.

Exception content must be sanitized and bounded at these capture boundaries, before it is written to HA or finished-job history. The implementation should extract the redaction patterns from `DryRunConnectFailureMessageSanitizer` into a shared utility rather than persisting raw connector messages or stack traces. Failure history keeps its own 4 KiB message and 64 KiB stack-trace limits and truncation flags; it must not inherit the dry-run utility's 2 KiB display limit.

Repeated delivery of the same terminal task-group state must not create duplicate rows while the original record is retained. The first implementation deduplicates on `(pipelineId, attempt, taskGroupId)`, because a task group has one terminal failure for one pipeline attempt. A Hazelcast `EntryProcessor` on the job-scoped HA entry performs the deduplication check, sequence allocation, append, and oldest-record eviction as one atomic operation. It must be submitted asynchronously from the task-status operation path. Completion handling can log a store failure, but must not wait on or re-enter a Hazelcast operation thread. The first terminal delivery creates the record and receives a sequence number, while later deliveries with the same key are ignored without consuming another sequence number. A different task group in the same attempt remains separate, and a failure after restore has a different attempt number and remains visible.

Deduplication uses the retained records as its bounded key set. After a record is evicted by either the 100-record limit or the 1 MiB aggregate-text limit, a delayed duplicate for that key can be recorded again. The first version does not keep a separate unbounded set of every key seen during the job lifetime.

Recording history is diagnostic and best effort. A history-store failure must be logged, but it must not block the original task failure or restore decision.

## Storage and Retention

Failure history should use a dedicated HA-backed engine state entry keyed by `jobId`. The first implementation can use a dedicated Hazelcast `IMap` with the same default Hazelcast `MapConfig` baseline as the engine's existing job-state maps. It does not introduce additional backups, persistence, or an external history backend. The REST representation remains independent of that storage choice.

The first version uses these bounds:

- retain at most 100 failure records per job;
- retain at most 1 MiB of combined UTF-8 `message` and `stackTrace` content per job;
- evict the oldest records until both the record-count and aggregate-text limits are satisfied;
- do not apply a TTL while the job is active; and
- after the job reaches a terminal state, give the dedicated finished-history entry its own `history-job-expire-minutes` TTL.

The initial limit should be a constant rather than a new user option. A configurable limit can be added later if operational evidence shows that 100 records is insufficient.

When a job reaches a terminal state, `JobHistoryService` writes the retained records and authoritative pipeline attempt values to a dedicated finished-history entry. The entry receives the same `history-job-expire-minutes` TTL as the corresponding finished-job record, so expiration does not depend on a cleanup-listener callback. A listener may remove it eagerly when the finished-job record is deleted, but the entry's own TTL remains the fallback guarantee. This reuses the existing finished-job lifecycle without introducing a pluggable history-store abstraction.

The terminal snapshot write is best effort. A write or cleanup failure must be logged, but must not change the job terminal state, restore behavior, or existing finished-job record. Running and finished reads use the same response model even though their storage lifecycle differs.

Master failover preserves records already acknowledged by the HA history store and resumes sequence and attempt numbering from that persisted state. Because history submission is asynchronous and best effort, a submission still in flight when the active master fails may be lost. This diagnostic path does not delay the task failure or restore decision to wait for a history acknowledgement.

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

`JobInfoServlet` currently treats all path information after `/job-info/` as one numeric job ID. The REST implementation must extend that routing, or add an equivalent dedicated handler, so `/job-info/{jobId}` keeps its current behavior while `/job-info/{jobId}/failures` is routed to failure history. Routing must match only these exact path shapes. Additional segments, prefixes, or substring matches must fall through to the existing not-found behavior.

The current `/job-info/{jobId}` behavior and its `errorMsg` field remain unchanged, including its existing response for an unknown job. The new failure-history endpoint defines its own explicit `404` response so callers can distinguish an unknown job from a known job with no failures.

## Security and Input Validation

The endpoint uses the same `BasicAuthFilter` boundary as the existing engine REST API. It must not introduce an endpoint-specific authentication mechanism. Deployments that leave REST authentication disabled expose this diagnostic data under the same policy as the other job-detail endpoints, and the documentation must call out that exception text and worker addresses can contain operationally sensitive information.

Redaction is applied before HA persistence, not only while serializing a REST response. This ensures that Hazelcast state, dedicated finished-history entries, and API responses all contain the same bounded representation and that an unsanitized value cannot be recovered through another storage path.

The route handler owns validation of `jobId` and `limit`:

- malformed job identifiers and non-numeric or non-positive limits return a controlled `400` response;
- values above the retained maximum are capped at that maximum; and
- validation failures must not include a stack trace or echo untrusted input through the shared exception handler.

Worker addresses remain optional and follow the same authorization boundary as the rest of the failure record. A later API version may replace them with logical worker identifiers, but the first version is limited to the same worker metadata already exposed by `/pending-jobs`: the `address` value in `host:port` form.

## Web UI Follow-up

The Exception tab can consume the REST endpoint in a separate change. The first UI version should group records by attempt and show timestamp, pipeline, task group, task name, worker, exception type, and message. Stack traces should be collapsed by default.

Missing optional fields should be displayed as unavailable. The UI must not claim task-level precision when the engine only supplied a task-group failure.

## Compatibility

The feature is additive:

- existing jobs do not need configuration changes;
- existing REST fields and the final error message remain available;
- no checkpoint or savepoint payload is changed;
- existing `job.retry.times` and active-master failover behavior remain unchanged; and
- old failure paths can populate only the fields they know.

`TaskExecutionState` is Java-serialized between workers and the master. Before adding the new optional failure fields, the implementation must capture the serial UID generated for the current class and declare that value explicitly. Keeping that UID and treating the new fields as optional preserves deserialization of the existing wire form instead of changing compatibility accidentally.

## Acceptance Criteria

1. A first-attempt task-group failure creates one record with attempt `0`.
2. A restored pipeline failure creates another record with the incremented attempt.
3. Duplicate delivery of one terminal state does not create a duplicate record while the original record remains in the bounded history; a delayed duplicate may be recorded after eviction.
4. Failures from different task groups in the same attempt remain separate.
5. A master failover preserves records acknowledged by the HA history store and the next attempt number; an asynchronous submission still in flight at failover may be lost.
6. A finished job exposes the same records until the configured history expiration.
7. More than 100 failures, or more than 1 MiB of retained UTF-8 message and stack-trace content, evicts the oldest records deterministically until both limits are satisfied.
8. Messages larger than 4 KiB and stack traces larger than 64 KiB are truncated at valid UTF-8 boundaries and expose the corresponding truncation flag.
9. A terminal job writes one bounded failure-history entry that expires with its corresponding finished-job record.
10. Failure-history write or cleanup errors do not change the job failure, restore, or terminal-state path.
11. Existing job-detail clients continue to receive the current `errorMsg` field.
12. Concurrent duplicate deliveries create one record and allocate one sequence number through the atomic job-entry update.
13. Secrets in messages and stack traces are redacted before HA and finished-history persistence.
14. Malformed `jobId` or `limit` input returns a controlled `400` response without exposing a stack trace or reflecting the invalid value.
15. The endpoint is covered by the same configured REST authentication boundary as existing job-detail endpoints.
16. Failure-history updates are submitted asynchronously and do not block a Hazelcast operation thread.
17. Failure-history state uses the same default Hazelcast map configuration as the existing job-state maps and does not add backups or persistence.
18. Only the exact `/job-info/{jobId}` and `/job-info/{jobId}/failures` path shapes are accepted; additional path segments use the existing not-found behavior.
19. Existing serialized `TaskExecutionState` values remain readable after the optional structured failure fields are added.
20. A deployment failure that enters through `makeTaskGroupFailing` creates one bounded record even though no `TaskExecutionState` exists, and its `exceptionType` identifies the original cause rather than `TaskGroupDeployException` when a cause is available.
21. Persisting diagnostic attempt identity does not change `job.retry.times`, restore eligibility, or retry behavior after active-master failover.
22. Every record that exposes `attemptStartedAt` reads it from the durable metadata created for that pipeline attempt.
23. A restored execution cannot report a failure before its attempt advance is atomically committed in the shared job-scoped entry; an advance failure does not start execution with the previous attempt identity.

## Delivery Plan

1. Agree on the record, attempt, storage, retention, and REST contracts.
2. Add the dedicated HA-backed running and finished history entries, atomic `EntryProcessor`, and structured task failure transport with unit tests.
3. Add capture-time sanitization, deduplication, retention, and restore tests.
4. Add the REST routing and endpoint with authentication-boundary, validation, backward-compatibility, and running/finished job API tests.
5. Add the Web UI history view in a separate pull request.
