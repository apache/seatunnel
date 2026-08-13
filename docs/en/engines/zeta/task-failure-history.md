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
- The current attempt must be stored in HA state so a new active master does not restart numbering at `0`.

`SubPlan.pipelineRestoreNum` is the existing in-memory counter for this boundary. The implementation must make that counter HA-durable and use it as the single authoritative attempt value. It must not introduce a second counter. A new active master restores the value before scheduling or recording another attempt, and all restore-limit checks, failure records, and REST responses read the same value.

This matches the existing pipeline restore boundary and avoids inventing task-level retry semantics that the engine does not currently expose.

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
- `attemptStartedAt` is optional because a failure can occur before the pipeline reaches `RUNNING`. When available, it is the persisted start time for that pipeline attempt and has the same value for every record with the same `pipelineId` and `attempt`. It is distinct from `timestamp`, which records when the individual failure was captured.
- `taskId`, `taskName`, `worker`, `exceptionType`, `message`, and `stackTrace` are optional because older or synthetic failure paths may not provide them.
- `messageTruncated` and `stackTraceTruncated` are required booleans. They indicate whether the corresponding value was shortened before storage.
- `exceptionType` must come from structured failure transport. It must not be inferred by parsing the formatted stack trace.
- `stackTrace` remains the diagnostic detail; `message` is the concise display value.
- The stored UTF-8 representation is limited to 4 KiB for `message` and 64 KiB for `stackTrace`. Truncation must preserve valid UTF-8. A truncated message keeps its prefix. A truncated stack trace keeps both its beginning and end so the exception and the deepest cause remain available.

## Capture and Deduplication

`TaskExecutionState` is the natural task-to-master transport boundary. The implementation should extend that transport with structured failure fields and capture the record in the JobMaster before task resources are released. Exception content must be sanitized and bounded at this capture boundary, before it is written to HA or finished-job history. The implementation should generalize the existing `DryRunConnectFailureMessageSanitizer` rules into a shared utility rather than persisting raw connector messages or stack traces.

Repeated delivery of the same terminal task-group state must not create duplicate rows. The first implementation deduplicates on `(pipelineId, attempt, taskGroupId)`, because a task group has one terminal failure for one pipeline attempt. A Hazelcast `EntryProcessor` on the job-scoped HA entry performs the deduplication check, sequence allocation, append, and oldest-record eviction as one atomic operation. The first terminal delivery creates the record and receives a sequence number, while later deliveries with the same key are ignored without consuming another sequence number. A different task group in the same attempt remains separate, and a failure after restore has a different attempt number and remains visible.

Recording history is diagnostic and best effort. A history-store failure must be logged, but it must not block the original task failure or restore decision.

## Storage and Retention

Failure history should use a dedicated HA-backed engine state entry keyed by `jobId`. The first implementation can use a dedicated Hazelcast `IMap`, consistent with the engine's existing running-job state. The REST representation remains independent of that storage choice.

The first version uses these bounds:

- retain at most 100 failure records per job;
- evict the oldest record when the limit is exceeded;
- do not apply a TTL while the job is active; and
- after the job reaches a terminal state, apply the existing `history-job-expire-minutes` policy used by `JobHistoryService`.

The initial limit should be a constant rather than a new user option. A configurable limit can be added later if operational evidence shows that 100 records is insufficient.

When a job reaches a terminal state, `JobHistoryService` writes the retained records and authoritative pipeline attempt values to a dedicated finished-history entry. This reuses the existing finished-job lifecycle and `history-job-expire-minutes` semantics; it does not assume a separate or pluggable history-store abstraction. The failure-history entry is removed when the corresponding finished-job record expires.

The terminal snapshot write is best effort. A write or cleanup failure must be logged, but must not change the job terminal state, restore behavior, or existing finished-job record. Running and finished reads use the same response model even though their storage lifecycle differs.

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
- use the existing job-not-found behavior for an unknown or expired job; and
- return the same response model for running and finished jobs, regardless of which dedicated state entry supplies the records.

`JobInfoServlet` currently treats all path information after `/job-info/` as one numeric job ID. The REST implementation must extend that routing, or add an equivalent dedicated handler, so `/job-info/{jobId}` keeps its current behavior while `/job-info/{jobId}/failures` is routed to failure history.

The current job-detail response and its `errorMsg` field remain unchanged. This keeps existing clients compatible while the UI adopts the history endpoint separately.

## Security and Input Validation

The endpoint uses the same `BasicAuthFilter` boundary as the existing engine REST API. It must not introduce an endpoint-specific authentication mechanism. Deployments that leave REST authentication disabled expose this diagnostic data under the same policy as the other job-detail endpoints, and the documentation must call out that exception text and worker addresses can contain operationally sensitive information.

Redaction is applied before HA persistence, not only while serializing a REST response. This ensures that Hazelcast state, terminal snapshots, external history backends, and API responses all contain the same bounded representation and that an unsanitized value cannot be recovered through another storage path.

The route handler owns validation of `jobId` and `limit`:

- malformed job identifiers and non-numeric or non-positive limits return a controlled `400` response;
- values above the retained maximum are capped at that maximum; and
- validation failures must not include a stack trace or echo untrusted input through the shared exception handler.

Worker addresses remain optional and follow the same authorization boundary as the rest of the failure record. A later API version may replace them with logical worker identifiers, but the first version must not expose more worker metadata than the existing job-detail APIs already provide.

## Web UI Follow-up

The Exception tab can consume the REST endpoint in a separate change. The first UI version should group records by attempt and show timestamp, pipeline, task group, task name, worker, exception type, and message. Stack traces should be collapsed by default.

Missing optional fields should be displayed as unavailable. The UI must not claim task-level precision when the engine only supplied a task-group failure.

## Compatibility

The feature is additive:

- existing jobs do not need configuration changes;
- existing REST fields and the final error message remain available;
- no checkpoint or savepoint payload is changed; and
- old failure paths can populate only the fields they know.

## Acceptance Criteria

1. A first-attempt task-group failure creates one record with attempt `0`.
2. A restored pipeline failure creates another record with the incremented attempt.
3. Duplicate delivery of one terminal state does not create a duplicate record.
4. Failures from different task groups in the same attempt remain separate.
5. A master failover preserves records and the next attempt number.
6. A finished job exposes the same records until the configured history expiration.
7. More than 100 failures evicts the oldest records deterministically.
8. Messages larger than 4 KiB and stack traces larger than 64 KiB are truncated at valid UTF-8 boundaries and expose the corresponding truncation flag.
9. A terminal job writes one bounded failure-history entry that expires with its corresponding finished-job record.
10. Failure-history write or cleanup errors do not change the job failure, restore, or terminal-state path.
11. Existing job-detail clients continue to receive the current `errorMsg` field.
12. Concurrent duplicate deliveries create one record and allocate one sequence number through the atomic job-entry update.
13. Secrets in messages and stack traces are redacted before HA and finished-history persistence.
14. Malformed `jobId` or `limit` input returns a controlled `400` response without exposing a stack trace or reflecting the invalid value.
15. The endpoint is covered by the same configured REST authentication boundary as existing job-detail endpoints.

## Delivery Plan

1. Agree on the record, attempt, storage, retention, and REST contracts.
2. Add the dedicated HA-backed running and finished history entries, atomic `EntryProcessor`, and structured task failure transport with unit tests.
3. Add capture-time sanitization, deduplication, retention, and restore tests.
4. Add the REST routing and endpoint with authentication-boundary, validation, backward-compatibility, and running/finished job API tests.
5. Add the Web UI history view in a separate pull request.
