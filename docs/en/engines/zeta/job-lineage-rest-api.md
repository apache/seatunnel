# Job Lineage REST API Design

## Status

This document is a design proposal. It does not describe an API that is available in a released
SeaTunnel version. It must remain outside the published documentation navigation until the STIP
scope and public contract are accepted.

## Motivation

The Zeta REST API already returns a DAG inside `/job-info/{jobId}` for the built-in Web UI. That
representation is UI-oriented and is not documented as a stable lineage contract. External catalog,
governance, and operations tools therefore have no supported way to retrieve a job-level
source-to-transform-to-sink graph.

The first version should expose job-level execution lineage for both batch and streaming jobs. It
must not imply column-level precision or depend on sampled row traces.

## Scope

### Goals

- expose the Source, Transform, and Sink graph for one Zeta job;
- return the same model while the job is pending, running, or retained as finished history;
- use stable node identifiers within one job;
- expose known source and sink table paths without inferring table-to-table mappings;
- survive an active-master change through the state already used by job details;
- keep the current `/job-info/{jobId}` response unchanged;
- define deterministic ordering, errors, limits, and compatibility rules before implementation.

### Non-goals

The first version will not provide:

- column-level lineage;
- field-expression or transform-semantic analysis;
- record-level provenance;
- lineage across several jobs;
- a global dataset graph or external metadata-catalog integration;
- lineage for jobs running on Flink or Spark;
- historical versions of a graph after the finished-job retention period;
- live updates when a dynamic source discovers another table;
- a Web UI lineage screen.

## Terminology

- **Job lineage** is the job-scoped execution graph of Source, Transform, and Sink nodes.
- **Dataset metadata** is the set of table paths already known to a Source or Sink node when the
  job DAG information is built.
- **StainTrace** is sampled record tracing. It is useful for latency and record-flow analysis but is
  not an authoritative topology source.
- **Node ID** is stable only within one job lineage snapshot. It is not a connector, dataset, or
  cross-job identity.

## Existing Architecture

`JobMaster` lazily builds `JobDAGInfo` through `DAGUtils.getJobDAGInfo()`. The object contains:

- the job ID;
- pipeline-grouped edges;
- a vertex map with vertex ID, plugin type, connector name, and known table paths;
- execution-location metadata used by the current job-detail path.

For a running job, the coordinator returns this object from the active `JobMaster`. If the request
reaches a follower, the existing master operation path obtains it from the active master. When a job
finishes, `JobHistoryService` stores the same `JobDAGInfo` with the configured
`history-job-expire-minutes` retention.

The lineage endpoint should be a deterministic projection of this existing object. It should not
deserialize connector configuration, scan trace files, or add another Hazelcast map.

## Source of Truth

The first version uses `JobDAGInfo` as the only source of truth because it represents the execution
graph already exposed for running and finished Zeta jobs.

Consequences of this choice:

- lineage describes the graph executed by Zeta, not the original HOCON block layout;
- optimizer-created or chained Transform nodes are represented as the nodes present in
  `JobDAGInfo`; the endpoint does not reconstruct hidden components;
- node IDs are reused from `VertexInfo.vertexId` and are stable for the lifetime of that job;
- pipeline IDs are reused from `JobDAGInfo.pipelineEdges`;
- table paths are advisory metadata and do not create table-level lineage edges;
- dynamic tables discovered after `JobDAGInfo` is built are not added in the first version.

StainTrace must not be used to fill graph gaps. It is optional, sampled, stored separately, and may
contain only a subset of records and stages.

## REST Contract

Proposed endpoint:

```text
GET /job-lineage/{jobId}
```

A dedicated route avoids changing the current path parsing or response behavior of
`/job-info/{jobId}`.

Example response:

```json
{
  "schemaVersion": 1,
  "jobId": "733584788375093248",
  "graphKind": "EXECUTION",
  "idScope": "JOB",
  "nodes": [
    {
      "id": "1",
      "kind": "SOURCE",
      "name": "Jdbc",
      "datasets": ["catalog.sales.orders"],
      "datasetMetadata": "REPORTED"
    },
    {
      "id": "2",
      "kind": "TRANSFORM",
      "name": "Sql",
      "datasets": [],
      "datasetMetadata": "NOT_APPLICABLE"
    },
    {
      "id": "3",
      "kind": "SINK",
      "name": "Kafka",
      "datasets": ["default.default.orders"],
      "datasetMetadata": "REPORTED"
    }
  ],
  "edges": [
    {
      "pipelineId": 1,
      "sourceNodeId": "1",
      "targetNodeId": "2"
    },
    {
      "pipelineId": 1,
      "sourceNodeId": "2",
      "targetNodeId": "3"
    }
  ],
  "warnings": []
}
```

### Field Semantics

- `schemaVersion` is the version of this response contract. The first version is `1`.
- `graphKind` is `EXECUTION` in this proposal.
- `idScope` is `JOB`; clients must combine `jobId` and node `id` when storing a node.
- `kind` is one of `SOURCE`, `TRANSFORM`, or `SINK`.
- `name` is the connector type already present in `VertexInfo.connectorType`. It is display
  metadata, not an ID.
- `datasets` contains sorted, distinct table-path strings known for Source and Sink nodes.
- `datasetMetadata` is one of:
  - `REPORTED`: the response contains the non-default table paths present in `JobDAGInfo`; this does
    not assert that a dynamic connector has discovered every table;
  - `UNAVAILABLE`: no reliable dataset metadata was available;
  - `NOT_APPLICABLE`: used for Transform nodes in the first version.
- `warnings` contains objects with a stable `code` and, when the warning applies to one node, its
  `nodeId`. V1 defines `DATASET_METADATA_UNAVAILABLE` for a Source or Sink whose dataset metadata
  cannot be represented reliably. Clients must use `code`, not array position, as the warning
  identity.

Nodes are ordered by numeric vertex ID. Edges are ordered by pipeline ID, source node ID, and target
node ID. Dataset names are sorted. Warnings are ordered by `code` and then `nodeId`. The response is
therefore deterministic even though the internal maps do not guarantee iteration order.

## Dataset and Transform Semantics

Multi-table Sources and Sinks keep one graph node with several `datasets` values. The first version
does not claim which source table produced which sink table because that mapping is not represented
by `JobDAGInfo`.

A Transform or transform chain remains one execution node. The endpoint does not parse SQL,
expressions, schemas, or transform implementation classes. A transform that filters, splits, joins,
or renames data therefore changes graph connectivity only when that connectivity is already present
in `JobDAGInfo`.

For dynamic table discovery, the response is a snapshot of the metadata available when
`JobDAGInfo` was built. If a connector cannot enumerate tables at that time, the node uses
`UNAVAILABLE`; it must not expose `TablePath.DEFAULT` as if it were a real dataset.

## Batch, Streaming, Restore, and Failover

Batch and streaming jobs use the same response model because both are represented by a Zeta job
DAG.

A pipeline retry keeps the same job ID and graph. The legacy savepoint resume path can also reuse
the same job ID, so it keeps the same job-scoped lineage identity. A restore submitted as a new job,
including a submission that references a source job, receives a new job ID and therefore a separate
lineage snapshot. The first version does not create a cross-job restore edge. Clients must not
compare node IDs across different jobs.

An active-master change does not require a new lineage store. The active master can return or
reconstruct `JobDAGInfo` through the existing coordinator and `JobImmutableInformation` paths.
Finished lineage remains available only while the corresponding `JobDAGInfo` entry is retained by
`JobHistoryService`.

## Availability and Error Semantics

The endpoint returns:

- `200` with a complete graph for a known job with usable DAG information;
- `400` with code `INVALID_JOB_ID` for a missing, malformed, or non-positive job ID;
- `404` with code `JOB_NOT_FOUND` when the job is unknown or its finished history has expired;
- `409` with code `LINEAGE_UNAVAILABLE` when the job is known but no consistent DAG snapshot can be
  obtained;
- `413` with code `LINEAGE_GRAPH_TOO_LARGE` when the serialized response would exceed 8 MiB.

The implementation must validate every edge reference before returning `200`. A dangling edge,
duplicate node ID, or missing required node field makes the snapshot unavailable; the endpoint must
not return a partial graph.

Error responses use one JSON object with string fields `code` and `message`, for example:

```json
{
  "code": "JOB_NOT_FOUND",
  "message": "Job lineage is not available for the requested job"
}
```

They must not include a Java stack trace, raw request value, connector configuration, or internal
class name. Clients must branch on `code`; `message` is descriptive text and may be refined without
changing `schemaVersion`.

The lineage servlet owns exception translation at its request boundary. It must catch unexpected
failures and return the same `{code, message}` contract rather than allowing them to reach the
shared `ExceptionHandlingFilter`, whose existing fallback response includes a stack trace and uses
a different response shape. This proposal does not change that shared filter for other endpoints.

## Performance and Payload Bounds

Mapping is `O(nodes + edges + datasets)` and uses the existing cached `JobDAGInfo`. The endpoint
must not rebuild the execution plan for every request and must not scan StainTrace files.

The first implementation proposes an 8 MiB serialized-response limit. The graph is atomic, so it
is rejected rather than truncated or paginated. Truncation could leave edges without nodes and
would make the result unsafe for governance tools. The error may include node and edge counts, but
not dataset names.

The implementation should serialize once into a bounded buffer, check the byte count, and then
write the response. It must not create a second unbounded copy of the graph JSON.

## Security

The endpoint uses the same Jetty and `BasicAuthFilter` boundary as the other Zeta REST endpoints. It
does not introduce endpoint-specific authorization.

Connector names and table paths can disclose deployment topology and business dataset names.
Operators should not expose the REST service without authentication and network controls. The
response must not contain environment options, connector configuration, credentials, plugin JAR
URLs, master/worker addresses, or StainTrace payloads.

The first version inherits the existing cluster-wide authorization model: a caller admitted to the
REST API can query any retained job. Per-job authorization is outside this proposal.

## Compatibility and Versioning

This proposal is additive:

- `/job-info/{jobId}` remains unchanged;
- no field is added to the Java-serialized `JobDAGInfo` or `JobImmutableInformation` classes;
- no checkpoint, savepoint, or connector API changes are required;
- no new Hazelcast map or retention setting is introduced.

New optional response fields may be added while `schemaVersion` remains `1`. Removing a field,
changing its meaning, or changing an enum value requires a new schema version. The existing REST
paths are not URL-versioned, so the response version must remain explicit.

## Implementation Slices

1. Add an immutable REST response model and a pure mapper from `JobDAGInfo` with deterministic
   ordering, validation, dataset-status handling, and size accounting.
2. Add `JobLineageService` and a dedicated servlet registered at `/job-lineage`, reusing the current
   running/finished DAG lookup path without adding storage. The servlet handles all failures at its
   boundary and emits only the error contract defined above.
3. Add REST API documentation and security/retention notes after the contract is approved.
4. Consider a separate Web UI or external-catalog integration only after operational use of the
   endpoint.

## Test Plan

### Mapper tests

- a Source-Transform-Sink graph produces stable node and edge ordering;
- multiple pipelines preserve their pipeline IDs;
- multi-table Sources and Sinks return sorted, distinct datasets;
- Transform and transform-chain nodes do not claim dataset lineage;
- unavailable dataset metadata produces an explicit state and warning;
- a dangling edge or duplicate node ID fails closed;
- the byte limit rejects the whole graph rather than truncating it.

### Service and REST tests

- pending, running, and finished jobs return the same response model;
- representative batch and streaming jobs are supported;
- follower requests resolve through the active-master path;
- lineage remains available after active-master failover;
- finished lineage expires with existing job DAG history;
- malformed and unknown job IDs return controlled errors;
- authentication protects the endpoint when Basic Auth is enabled;
- `/job-info/{jobId}` output remains byte-for-byte unchanged for existing fixtures.

### Compatibility tests

- a legacy savepoint resume that reuses a job ID keeps the same job-scoped identity;
- a restore submitted with a new job ID receives an independent job-scoped graph;
- existing `JobDAGInfo` serialization is unchanged;
- no StainTrace configuration is required;
- no connector implementation change is required.

## Acceptance Criteria

1. A batch or streaming Zeta job exposes one deterministic Source-Transform-Sink graph.
2. Running and retained finished jobs use the same response schema.
3. Node IDs are stable within the job and explicitly not stable across jobs.
4. Multi-table metadata does not imply unsupported table-to-table or column lineage.
5. Dynamic or unavailable dataset metadata is marked rather than guessed.
6. Pipeline retry and active-master failover do not change the graph.
7. A separately submitted restore job receives its own graph and job-scoped IDs.
8. Unknown, expired, inconsistent, and oversized graphs return controlled errors.
9. The endpoint does not expose configuration, credentials, addresses, or trace payloads.
10. `/job-info/{jobId}`, checkpoint/savepoint formats, and Java serialization remain unchanged.
11. The endpoint adds no new HA state or retention configuration.
12. Column lineage and a Web UI lineage view remain separate follow-up work.

## Open Questions for Consensus

1. Is `/job-lineage/{jobId}` preferred over `/job-info/{jobId}/lineage`?
2. Is the existing execution-oriented `JobDAGInfo` the correct V1 source, or should the API expose a
   separately retained logical-plan snapshot?
3. Is 8 MiB an acceptable initial response limit?
4. Should a future connector capability distinguish complete from partial dynamic discovery, while
   V1 continues to describe existing table paths only as `REPORTED`?
