---
title: CDC Schema Evolution Contract
---

# CDC Schema Evolution Contract

## Purpose

This page defines the end-to-end contract that a CDC schema evolution path must satisfy when a
`SchemaChangeEvent` is enabled. It is a runtime contract, not only a connector feature list.

The contract applies to any source, transform, engine, and sink path that advertises schema
evolution support. A path that cannot satisfy this contract must fail explicitly or document a
connector-specific limitation before it processes the ambiguous stream.

## Scope

The first version is defined per table. Schema changes for different tables may proceed
independently as long as each table keeps its own ordering and recovery state. Cross-table atomic
DDL is outside this version and must not be implied by connector documentation.

The supported change types and connector matrix are documented in
[Schema Evolution Configuration](../introduction/configuration/schema-evolution.md). This page
defines the ordering, checkpoint, sink application, replay, and restore semantics around those
events.

## Contract Primitives

### Table identity

Every schema change event must carry the stable table identity used by the runtime and sink. The
identity is the key for ordering, coordination, sink apply, checkpoint state, and restore.

### Schema epoch

For one table, every schema change must have a monotonic epoch or equivalent durable identity. The
epoch is used to deduplicate replayed events and to tell whether a sink has already applied the same
schema change after recovery.

If a source or engine path cannot provide a stable event identity, that path must not claim
recoverable schema evolution support.

### Schema boundary

For one table, a schema change creates a hard boundary:

```text
old-schema records -> SchemaChangeEvent(epoch=N) -> new-schema records
```

Records using the new schema must not overtake the schema change. Records using the old schema must
not be released after the sink has moved to the new epoch unless the connector explicitly encodes
them as old-epoch records and the sink can handle both epochs.

## End-To-End Flow

The target flow for a schema-evolution-capable path is:

1. The source observes a DDL event and converts it to a `SchemaChangeEvent`.
2. The source closes the old-schema prefix for that table and records a durable boundary before
   new-schema data is released.
3. Each transform maps the event through `SeaTunnelTransform.mapSchemaChangeEvent` and refreshes
   downstream catalog state before it processes new-schema rows.
4. The engine waits until old-schema data that must be flushed or committed before the DDL is
   safely past the required checkpoint boundary.
5. The engine sends the event to every sink subtask that owns the table.
6. Every sink subtask applies the DDL or reports an explicit failure. A partial success must not be
   treated as success.
7. The engine records the applied epoch in checkpoint state before it releases or commits the
   new-schema data stream as durable.

## Ordering Rules

For each table:

- schema changes are processed in source order
- only one schema change may be in the active apply window
- new-schema records wait behind the schema event until all required sink subtasks have applied it
- replayed schema events are deduplicated by epoch or fail with an actionable error
- unsupported transforms or sinks fail before dropping, reordering, or silently ignoring the event

For different tables:

- schema changes may proceed independently in the first version
- a blocked table must not silently reorder another table's records
- connectors that share one physical sink transaction across tables must document whether that
  transaction creates a wider coordination boundary

## Recovery State Machine

The engine or runtime integration must be able to distinguish these states for each table epoch.

| State | Meaning | Recovery rule |
| --- | --- | --- |
| `OBSERVED` | The source saw the schema change, but the old-schema boundary is not durable yet. | Restore from the last checkpoint and re-read the event from the source offset. No sink DDL may have been applied. |
| `BOUNDARY_DURABLE` | Old-schema records before the boundary are checkpointed or otherwise safe. | Apply the schema change before releasing new-schema records. |
| `APPLYING` | One or more sink subtasks are applying the DDL. | Retry only through an idempotent sink apply path, or verify the external schema and fail with a clear error if the result is ambiguous. |
| `APPLIED_NOT_DURABLE` | The external sink schema was changed, but the runtime epoch state is not checkpointed yet. | On restore, detect the already-applied epoch and complete the runtime state, or fail before writing data if the sink cannot prove the applied schema. |
| `EPOCH_DURABLE` | The sink apply result and runtime epoch are durable. | New-schema records may continue from the restored checkpoint. Replayed events for the same epoch are treated as duplicates. |

## Failure Rules

The following cases must be deterministic:

| Failure point | Required behavior |
| --- | --- |
| Before the boundary checkpoint | Replay from the previous source checkpoint. The sink must not have received the DDL. |
| After the boundary checkpoint and before sink apply | Apply the DDL before releasing new-schema records. |
| During sink apply | Retry idempotently, complete all subtasks, or fail the job. Partial apply must be visible in the error. |
| After sink apply and before the next checkpoint | Restore by detecting the applied epoch, or fail fast if the sink cannot prove whether the DDL succeeded. |
| During restore | Rebuild source offsets, transform catalog state, sink epoch state, and pending events before any new-schema records are emitted. |
| Unsupported source, transform, engine, or sink path | Fail before ambiguous processing. Skipping or ignoring the event is not a valid schema evolution contract. |

## Component Responsibilities

### Source

The source is responsible for emitting schema changes with table identity, a stable epoch, the
post-change schema, and a checkpoint boundary that separates old-schema and new-schema records.
Current source APIs expose this boundary through `Collector.markSchemaChangeBeforeCheckpoint`,
`Collector.collect(SchemaChangeEvent)`, and `Collector.markSchemaChangeAfterCheckpoint`.

### Transform

A transform that changes table identity, column names, column order, or row shape must map the
schema change event consistently with the data rows it will emit after the boundary. If a transform
cannot map the event safely, it must fail instead of passing stale metadata downstream.

### Engine

The engine is responsible for serializing each table's schema epoch, waiting for the required
checkpoint boundary, broadcasting the event to all required sink subtasks, collecting success or
failure, and checkpointing the applied epoch.

### Sink

A sink that advertises schema evolution support must implement an idempotent or verifiable DDL apply
path through `SupportSchemaEvolutionSinkWriter.applySchemaChange`. It must report unsupported
change types and partial apply failures as task-failing errors with table and epoch context.

## Validation Requirements

Any implementation that claims this contract should include E2E coverage for:

- consecutive add, drop, rename, and modify column events
- failure before the schema-change boundary checkpoint
- failure after the boundary checkpoint but before sink apply
- failure during sink apply, including partial subtask success
- failure after sink apply but before the following checkpoint completes
- restore from checkpoints before and after the schema change
- multiple tables changing schema independently
- a sink that does not support the requested schema change

At least one positive path should use MySQL CDC to a schema-evolution-capable JDBC sink. At least
one negative path should use an unsupported sink and assert the explicit failure.

## Follow-Up Implementation Areas

This contract should be implemented through focused follow-up work:

- API/event metadata: stable per-table epoch, serialization compatibility, and transform mapping
  expectations
- engine coordination: recoverable per-table state machine, sink subtask acknowledgement, timeout,
  restore, and replay handling
- E2E recovery: fault injection for every failure point above, including duplicate DDL and
  unsupported-sink paths

## Code References

Start from these implementation points when changing code:

- `seatunnel-api/src/main/java/org/apache/seatunnel/api/table/schema/event/SchemaChangeEvent.java`
- `seatunnel-api/src/main/java/org/apache/seatunnel/api/source/Collector.java`
- `seatunnel-api/src/main/java/org/apache/seatunnel/api/transform/SeaTunnelTransform.java`
- `seatunnel-api/src/main/java/org/apache/seatunnel/api/sink/SupportSchemaEvolutionSinkWriter.java`
- `seatunnel-connectors-v2/connector-cdc/connector-cdc-base/src/main/java/org/apache/seatunnel/connectors/cdc/debezium/row/SeaTunnelRowDebeziumDeserializeSchema.java`
- `seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/task/flow/SourceFlowLifeCycle.java`
- `seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/task/flow/SinkFlowLifeCycle.java`
- `seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/checkpoint/CheckpointCoordinator.java`
- `seatunnel-translation/seatunnel-translation-flink/seatunnel-translation-flink-common/src/main/java/org/apache/seatunnel/translation/flink/schema/SchemaOperator.java`
- `seatunnel-translation/seatunnel-translation-flink/seatunnel-translation-flink-common/src/main/java/org/apache/seatunnel/translation/flink/schema/BroadcastSchemaSinkOperator.java`
