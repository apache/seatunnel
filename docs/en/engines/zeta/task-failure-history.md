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
- keep the existing single `errorMessage` field for compatibility.

The first implementation should not add log aggregation, distributed tracing, or an unbounded exception archive. The Web UI is a follow-up after the backend contract is agreed.

## Attempt Model

An attempt belongs to a pipeline, not to an individual task.

- The initial pipeline execution is attempt `0`.
- The attempt increments before a restore is scheduled.
- Every failure captured during that execution carries the same attempt number.
- The current attempt must be stored in HA state so a new active master does not restart numbering at `0`.

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
  "stackTrace": "java.sql.SQLException: Connection reset\n..."
}
```

Field rules:

- `sequence` is monotonically increasing within one job and provides deterministic ordering when timestamps are equal.
- `timestamp`, `jobId`, `pipelineId`, `attempt`, and `taskGroupId` are required.
- `taskId`, `taskName`, `worker`, `exceptionType`, `message`, and `stackTrace` are optional because older or synthetic failure paths may not provide them.
- `exceptionType` must come from structured failure transport. It must not be inferred by parsing the formatted stack trace.
- `stackTrace` remains the diagnostic detail; `message` is the concise display value.

## Capture and Deduplication

`TaskExecutionState` is the natural task-to-master transport boundary. The implementation should extend that transport with structured failure fields and capture the record in the JobMaster before task resources are released.

Repeated delivery of the same terminal task-group state must not create duplicate rows. The first implementation can deduplicate on `(pipelineId, attempt, taskGroupId)`, because a task group reports one terminal failure for one pipeline attempt. A later retry has a different attempt number and remains visible.

Recording history is diagnostic and best effort. A history-store failure must be logged, but it must not block the original task failure or restore decision.

## Storage and Retention

Failure history should use a dedicated distributed map keyed by `jobId`. This gives the running and finished job paths one source of truth and preserves data during active-master changes.

The first version uses these bounds:

- retain at most 100 failure records per job;
- evict the oldest record when the limit is exceeded;
- do not apply a TTL while the job is active; and
- after the job reaches a terminal state, apply the existing `history-job-expire-minutes` policy used by `JobHistoryService`.

The initial limit should be a constant rather than a new user option. A configurable limit can be added later if operational evidence shows that 100 records is insufficient.

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
- read from the same store for running and finished jobs.

The current job-detail response and its `errorMessage` field remain unchanged. This keeps existing clients compatible while the UI adopts the history endpoint separately.

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
8. Failure-history write errors do not change the job failure or restore path.
9. Existing job-detail clients continue to receive the current `errorMessage` field.

## Delivery Plan

1. Agree on the record, attempt, storage, retention, and REST contracts.
2. Add the HA-backed model and structured task failure transport with unit tests.
3. Add capture, deduplication, retention, and restore tests.
4. Add the REST endpoint and running/finished job API tests.
5. Add the Web UI history view in a separate pull request.
