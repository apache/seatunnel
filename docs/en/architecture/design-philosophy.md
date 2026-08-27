---
sidebar_position: 2
title: Design Philosophy
---

# SeaTunnel Design Philosophy

## 1. Overview

This document explains the core design principles, philosophies, and trade-offs that shaped SeaTunnel's architecture. Understanding these principles helps contributors make consistent design decisions and users understand the system's strengths and limitations.

## 2. Core Design Principles

### 2.1 Engine Independence

**Principle**: Decouple connector logic from execution engines.

**Motivation**:
- Users may have existing infrastructure investments (Flink, Spark clusters)
- Different engines suit different scenarios (batch vs streaming, resource constraints)
- Connector developers shouldn't need to understand multiple engine APIs

**Implementation**:
- Unified SeaTunnel API layer abstracts engine-specific details
- Translation layer adapts SeaTunnel API to engine-specific APIs
- Aim for maximum connector reuse across engines (some engine-specific adaptation may still be required via the translation layer)

**Trade-offs**:
- **Pro**: High reusability - write once, run across engines via adapters
- **Pro**: Easier connector development - single API to learn
- **Con**: Cannot leverage engine-specific optimizations
- **Con**: Additional translation overhead
- **Mitigation**: Translation layer is thin and optimized; most overhead is in I/O, not translation

**Example**: Connectors only implement SeaTunnel API abstractions (Source/Sink/Transform), and different execution engines complete adaptation through the translation layer; thus connector logic is decoupled from engine API changes.

### 2.2 Separation of Coordination and Execution

**Principle**: Separate control logic (coordination) from data processing (execution).

**Motivation**:
- Coordination logic is single-threaded and lightweight
- Execution logic is parallel and resource-intensive
- Fault tolerance requires independent state management for each

**Implementation Principle**:

**Coordination Layer (Master-side)**:
- Location: Runs on master nodes with global view
- Core Responsibilities: Resource discovery, work distribution, failure detection, state coordination
- Characteristics: Single-threaded, lightweight, no actual data processing
- Managed State: Assignment plan, pending work units, global progress tracking

**Execution Layer (Worker-side)**:
- Location: Runs on worker nodes with independent parallel execution
- Core Responsibilities: Local data processing, progress reporting, checkpoint participation
- Characteristics: Multi-threaded, resource-intensive, handles large data volumes
- Managed State: Local processing progress, buffered data, execution context

**Communication Mechanism**:
- Coordination layer → Execution layer: Dispatches work via events (e.g., assign new data splits)
- Execution layer → Coordination layer: Reports progress via messages (e.g., split completed, request new work)
- During checkpoints: Each layer snapshots its own state independently

**Trade-offs**:
- **Pro**: Clear separation of concerns
- **Pro**: Enumerator can reassign splits on failures
- **Pro**: Committer enables global transaction coordination
- **Con**: Additional communication overhead
- **Con**: More complex API for connector developers
- **Mitigation**: Reasonable defaults; simple connectors can use trivial enumerators/committers

**Example**:
- Master side: Responsible for "discovering/generating work units (splits) + assignment + reclamation + state snapshots"
- Worker side: Responsible for "executing reads/writes + progress reporting + checkpoint participation"

The key reason for this design: Fault tolerance requires distinguishing between "control state" (assigned/pending splits) and "execution progress" (offset/position per split) to enable precise recovery and fast reassignment after failures.

### 2.3 Split-based Parallelism

**Principle**: Divide data sources into independently processable splits.

**Motivation**:
- Enable parallel processing without tight coordination
- Support dynamic load balancing and fault recovery
- Provide checkpoint granularity (per-split progress)

**Implementation**:
- Data sources divided into splits (file blocks, DB partitions, Kafka partitions, etc.)
- Enumerator generates splits lazily or eagerly
- Readers process splits independently
- Unprocessed splits can be reassigned on failure

**Trade-offs**:
- **Pro**: Excellent scalability - add workers to process more splits
- **Pro**: Fine-grained fault recovery - only failed splits need reprocessing
- **Pro**: Dynamic load balancing - assign more splits to idle workers
- **Con**: Split generation overhead for some sources
- **Con**: Requires state tracking per split
- **Mitigation**: Lazy split generation; split state is lightweight

**Example**:
```java
// JDBC Source: Split by partition or chunk
class JdbcSourceSplit implements SourceSplit {
    private final String splitId;
    private final String query; // SELECT * FROM table WHERE id >= ? AND id < ?
    private final long startOffset;
    private final long endOffset;
}

// File Source: Split by file or byte range
class FileSplit implements SourceSplit {
    private final String filePath;
    private final long startOffset;
    private final long length;
}
```

### 2.4 Exactly-Once Semantics through Two-Phase Commit

**Principle**: Guarantee exactly-once end-to-end data delivery.

**Motivation**:
- Data integration must not lose or duplicate data
- Failures can occur at any time (network, process crashes)
- External systems require transactional guarantees

**Implementation Principle**:

Two-phase commit protocol separates data writing into two independent phases:

1. **Prepare Phase**:
   - Timing: Triggered when checkpoint barrier arrives
   - Action: Writer generates "committable but not yet committed" credentials (e.g., transaction ID, temp file path)
   - Constraint: No externally visible side effects (data not visible to external systems)
   - State: Credential information persisted with checkpoint

2. **Commit Phase**:
   - Timing: After checkpoint completes successfully
   - Action: Coordinator atomically commits changes using credentials (e.g., commit transaction, move files)
   - Effect: Data becomes visible to external systems
   - Guarantee: Idempotent - repeated commits have no side effects

3. **Abort Handling**:
   - Timing: When checkpoint fails or times out
   - Action: Clean up temporary resources from prepare phase (e.g., rollback transaction, delete temp files)
   - Effect: Ensures no partial writes or inconsistent state

**Trade-offs**:
- **Pro**: Strong consistency guarantee
- **Pro**: Automatic recovery from failures
- **Con**: Requires transactional support in sinks (or idempotent operations)
- **Con**: Increased latency (data visible only after commit)
- **Con**: Additional state for commit info
- **Mitigation**: Optional feature; at-least-once mode available for non-transactional sinks

**Example**: A typical exactly-once implementation follows this pattern: "the writer first generates committable credentials (commit info), and after checkpoint succeeds, the coordinator performs the final commit". This approach delays side effects (visible changes to external systems) until after checkpoint success, avoiding duplicate visible writes during failure recovery.

### 2.5 Schema as First-Class Citizen

**Principle**: Treat schema as explicit, typed metadata propagated through pipelines.

**Motivation**:
- Data integration requires schema transformation and validation
- Schema evolution (DDL changes) must be handled explicitly
- Type mismatches should be caught early

**Implementation**:
- `CatalogTable` encapsulates complete table metadata
- `TableSchema` defines structure (columns, primary key, constraints)
- Schema propagated through Source → Transform → Sink
- `SchemaChangeEvent` represents DDL changes (ADD/DROP/MODIFY columns)

**Trade-offs**:
- **Pro**: Type safety - validate schema at job submission
- **Pro**: Schema evolution - handle DDL changes at runtime
- **Pro**: Better error messages - schema mismatches detected early
- **Con**: Additional complexity for schema-less sources
- **Con**: Schema discovery overhead for some sources
- **Mitigation**: Schema inference helpers; optional schema override

**Example**:
```java
// Source produces typed schema
CatalogTable catalogTable = CatalogTable.of(
    tableId,
    TableSchema.builder()
        .column("id", DataTypes.BIGINT())
        .column("name", DataTypes.STRING())
        .primaryKey("id")
        .build()
);

// Transform validates and modifies schema
public CatalogTable getProducedCatalogTable() {
    return inputCatalogTable.copy(
        TableSchema.builder()
            .column("id", DataTypes.BIGINT())
            .column("name_upper", DataTypes.STRING()) // Transformed
            .build()
    );
}
```

### 2.6 Plugin Architecture with Class Loader Isolation

**Principle**: Connectors are plugins loaded dynamically with isolated dependencies.

**Motivation**:
- Avoid dependency conflicts (e.g., multiple JDBC driver versions)
- Enable hot-pluggable connectors without core rebuild
- Reduce core distribution size

**Implementation**:
- Java SPI for connector discovery
- Each connector has isolated class loader
- Shade plugin dependencies to avoid conflicts
- Factory pattern for instantiation

**Trade-offs**:
- **Pro**: Dependency isolation - no version conflicts
- **Pro**: Smaller core distribution
- **Pro**: Easy to add third-party connectors
- **Con**: Class loader complexity
- **Con**: Some shared libraries (e.g., Guava) may have issues
- **Mitigation**: Careful shading; shared common libraries in core

**Example**:
```
seatunnel-engine/lib/              # Core libraries
connector-jdbc/lib/                # JDBC driver (isolated)
connector-kafka/lib/               # Kafka client (isolated)

# Each connector loaded by separate ClassLoader
ConnectorClassLoader(connector-jdbc) -> loads mysql-connector-java-8.0.26.jar
ConnectorClassLoader(connector-kafka) -> loads kafka-clients-3.0.0.jar
```

### 2.7 State Management with Checkpoint Storage Abstraction

**Principle**: Decouple state management from storage implementation.

**Motivation**:
- Different deployments need different storage (HDFS, S3, local, OSS)
- State size varies widely (KBs to TBs)
- Storage durability and performance requirements differ

**Implementation**:
- `CheckpointStorage` abstraction (FileSystem, HDFS, S3, OSS)
- Pluggable serialization for state
- Incremental checkpoint support
- Automatic state cleanup

**Trade-offs**:
- **Pro**: Flexibility - choose storage based on deployment
- **Pro**: Incremental checkpoints reduce overhead
- **Con**: Storage performance impacts checkpoint latency
- **Con**: Requires distributed file system for production
- **Mitigation**: Async checkpoint upload; configurable intervals

### 2.8 Multi-Table Synchronization

**Principle**: Support synchronizing multiple tables in a single job.

**Motivation**:
- Database migration often involves hundreds of tables
- Creating one job per table wastes resources
- Schema evolution must apply to all tables

**Implementation**:
- `MultiTableSource` / `MultiTableSink` wrap individual table sources/sinks
- `TablePath` routes records to correct table
- Schema changes propagated per table
- Replica support for throughput

**Trade-offs**:
- **Pro**: Resource efficiency - one job instead of hundreds
- **Pro**: Consistent snapshot across tables
- **Pro**: Centralized monitoring
- **Con**: One table failure can affect others
- **Con**: More complex error handling
- **Mitigation**: Configurable error tolerance; per-table metrics

## 3. Architectural Trade-offs

### 3.1 Simplicity vs Performance

**Choice**: Favor simplicity and correctness over extreme performance optimization.

**Rationale**:
- Data integration is I/O-bound, not CPU-bound
- Correct semantics (exactly-once) more critical than raw speed
- Simple code is maintainable and debuggable

**Evidence**:
- Network and disk I/O dominate processing time (> 90%)
- Translation layer overhead is negligible (< 1%)
- Code readability prioritized (e.g., clear state machine, no micro-optimizations)

### 3.2 Flexibility vs Ease of Use

**Choice**: Provide reasonable defaults while allowing advanced customization.

**Rationale**:
- Most users want simple configuration
- Power users need fine-grained control
- Both needs can be met with layered API

**Implementation**:
- High-level config for common cases (e.g., `jdbc://host:port/db`)
- Low-level options for experts (e.g., connection pool tuning)
- Sensible defaults (parallelism, checkpoint interval, buffer size)

### 3.3 Generality vs Specialization

**Choice**: General-purpose API with specialized implementations.

**Rationale**:
- Unified API simplifies learning and usage
- Different sources have unique characteristics (bounded vs unbounded, splitability)
- Specialization happens in connector implementations, not API

**Example**:
- `SourceSplitEnumerator` general enough for files, databases, and message queues
- File connector uses file-based splits
- Kafka connector uses partition-based splits
- JDBC connector uses query-based splits

### 3.4 Strong Consistency vs Latency

**Choice**: Offer both exactly-once (high latency) and at-least-once (low latency) modes.

**Rationale**:
- Some applications require strong consistency (financial, billing)
- Other applications tolerate duplicates for lower latency (logging, metrics)
- Let users choose based on requirements

**Configuration**:
```hocon
env {
  checkpoint.mode = "EXACTLY_ONCE"  # or "AT_LEAST_ONCE"
  checkpoint.interval = 60000       # ms
}
```

## 4. Evolution from V1 to V2

### 4.1 V1 Limitations

SeaTunnel V1 (pre-2.3.0) had significant architectural limitations:

1. **Engine-Specific Connectors**: Separate implementations for Spark and Flink
2. **No Unified API**: No abstraction layer, tight coupling to engines
3. **Limited Fault Tolerance**: Relied entirely on engine checkpointing
4. **No Schema Management**: Schema implicit, no evolution support
5. **Single-Table Only**: Multi-table synchronization not supported

### 4.2 V2 Improvements

SeaTunnel V2 (2.3.0+) redesigned the architecture:

| Aspect | V1 | V2 |
|--------|----|----|
| **API** | Engine-specific | Unified SeaTunnel API |
| **Connectors** | Duplicated code | Single implementation |
| **Fault Tolerance** | Engine-dependent | Explicit checkpoint protocol |
| **Schema** | Implicit | Explicit CatalogTable |
| **Multi-Table** | Not supported | Native support |
| **Engine Support** | Spark, Flink | Spark, Flink, Zeta |
| **Exactly-Once** | Partial | End-to-end with 2PC |

### 4.3 Migration Path

V1 and V2 connectors coexist but use different APIs:
- V1 connectors: `seatunnel-connectors/` (deprecated)
- V2 connectors: `seatunnel-connectors-v2/` (recommended)

V2 is the future; V1 is in maintenance mode.

## 5. Key Design Decisions

### 5.1 Why Separate Enumerator and Reader?

**Alternative**: Single component handles both split generation and reading.

**Decision**: Separate components.

**Reasoning**:
- Split generation is coordination logic (should run on master)
- Data reading is execution logic (should run on workers)
- Failure of one shouldn't affect the other
- Allows split reassignment without reader restart

### 5.2 Why Three-Level Sink Commit (Writer → Committer → AggregatedCommitter)?

**Alternative**: Two-level (Writer → Committer) or direct Writer commit.

**Decision**: Optional three-level commit.

**Reasoning**:
- **Writer**: Parallel, stateful, per-task
- **Committer**: Parallel, stateless, aggregates per-writer commits
- **AggregatedCommitter**: Single-threaded, stateful, global coordinator

Many sinks only need Writer + Committer; AggregatedCommitter is for complex cases (e.g., Hive table commit requiring single global operation).

### 5.3 Why LogicalDag → PhysicalPlan Separation?

**Alternative**: Directly generate physical execution plan from config.

**Decision**: Two-stage planning.

**Reasoning**:
- LogicalDag represents user intent (portable, engine-independent)
- PhysicalPlan represents execution strategy (engine-specific, optimized)
- Separation enables:
  - Cross-engine portability (same LogicalDag, different PhysicalPlans)
  - Optimization passes (fusion, split reassignment)
  - Testing (validate logical plan separately)

### 5.4 Why Pipeline-based Execution?

**Alternative**: Single global task graph.

**Decision**: Jobs divided into pipelines.

**Reasoning**:
- Independent checkpoint coordination per pipeline
- Clearer failure boundaries
- Easier to reason about data flow
- Supports complex DAGs (multiple sources/sinks)

### 5.5 Why Not Use Engine-Native Checkpoint?

**Alternative**: Rely entirely on Flink/Spark checkpoint mechanisms.

**Decision**: Explicit SeaTunnel checkpoint protocol.

**Reasoning**:
- Engine independence - need consistent semantics across engines
- Zeta engine wouldn't have checkpointing otherwise
- More control over exactly-once semantics
- Unified monitoring and observability

However, for Flink translation, SeaTunnel checkpoints align with Flink checkpoints to avoid duplication.

## 6. Lessons Learned

### 6.1 What Worked Well

1. **Engine Independence**: Validated by successful Zeta engine addition without API changes
2. **Split-based Parallelism**: Scales well to 1000+ parallel tasks
3. **Explicit Schema**: Caught many bugs early, enabled schema evolution
4. **Two-Phase Commit**: Reliable exactly-once semantics

### 6.2 What Could Be Better

1. **API Complexity**: Enumerator/Committer adds learning curve for simple connectors
2. **Class Loader Issues**: Occasional conflicts with shaded dependencies
3. **Checkpoint Latency**: Large state causes checkpoint delays
4. **Documentation Gaps**: Architecture docs lagged behind code

### 6.3 If Starting Over

1. **Simplify API**: Provide higher-level abstractions for simple sources/sinks
2. **Async I/O Support**: First-class async API for non-blocking connectors
3. **Built-in Metrics**: Standardized metrics collection in API
4. **Schema Registry Integration**: Tighter integration with external schema registries

## 7. Conclusion

SeaTunnel's architecture reflects careful trade-offs between competing concerns:
- Engine independence vs engine-specific optimization
- Simplicity vs flexibility
- Consistency vs latency
- Generality vs specialization

The V2 redesign addressed major V1 limitations while establishing principles for long-term evolution. Understanding these design philosophies helps contributors make consistent decisions and users understand SeaTunnel's strengths and appropriate use cases.

## 8. References

- [Architecture Overview](overview.md)
- [Source Architecture](api-design/source-architecture.md)
- [Sink Architecture](api-design/sink-architecture.md)
- [Checkpoint Mechanism](fault-tolerance/checkpoint-mechanism.md)

### Academic Papers

- Chandy-Lamport: ["Distributed Snapshots: Determining Global States of Distributed Systems"](https://lamport.azurewebsites.net/pubs/chandy.pdf)
- Flink: ["Apache Flink: Stream and Batch Processing in a Single Engine"](https://asterios.katsifodimos.com/assets/publications/flink-deb.pdf)
