---
sidebar_position: 1
title: Architecture Overview
---

# SeaTunnel Architecture Overview

## 1. Introduction

### 1.1 Design Goals

SeaTunnel is designed as a distributed multimodal data integration tool with the following core objectives:

- **Engine Independence**: Decouple connector logic from execution engines, enabling the same connectors to run on SeaTunnel Engine (Zeta), Apache Flink, or Apache Spark
- **High Performance**: Support large-scale data synchronization with ultra-high-performance throughput and low latency
- **Fault Tolerance**: Provide exactly-once semantics through distributed snapshots and two-phase commit
- **Ease of Use**: Offer simple configuration and a rich connector ecosystem
- **Extensibility**: Plugin-based architecture allowing easy addition of new connectors and transforms

### 1.2 Target Use Cases

- **Batch Data Synchronization**: Large-scale batch data migration between heterogeneous data sources
- **Real-time Data Integration**: Stream data capture and synchronization with CDC support
- **Data Lake/Warehouse Ingestion**: Efficient data loading to data lakes (Iceberg, Hudi, Delta Lake) and warehouses
- **Multi-table Synchronization**: Synchronizing multiple tables in a single job with schema evolution support

### 1.3 Recommended Reading Path

If you are using this section to build architectural understanding, read in this order:

- this page for the layered system view
- [Configuration And Option System](./configuration-and-option-system.md) for how plugin configuration is modeled and validated
- [Transform Plugin System](./transform-plugin-system.md) for how transform plugins fit between source, sink, schema, and engine translation
- [Table Schema and Type System](./table-schema-and-type-system.md) for how table metadata and portable types flow through the pipeline
- [CDC Pipeline Architecture](./cdc-pipeline-architecture.md) if you need the end-to-end view for changelog-style pipelines
- [Checkpoint Mechanism](./fault-tolerance/checkpoint-mechanism.md) and [Exactly-Once](./fault-tolerance/exactly-once.md) for consistency semantics
- [Resource Management](./engine/resource-management.md) for slot allocation and worker coordination
- [Plugin Discovery and Class Loading](./plugin-discovery-and-class-loading.md) for runtime plugin packaging and isolation
- [Translation Layer](./api-design/translation-layer.md) if you need to understand multi-engine support

## 2. Overall Architecture

SeaTunnel adopts a layered architecture that separates concerns and enables flexibility:

```mermaid
flowchart TD
    config["User Configuration Layer<br/>HOCON Config / SQL / Web UI"]
    api["SeaTunnel API Layer<br/>Source API / Sink API / Transform API / Table API"]
    connectors["Connector Ecosystem<br/>JDBC / Kafka / MySQL-CDC / Elasticsearch / Iceberg / ..."]
    translation["Translation Layer<br/>Flink adapters / Spark adapters / Context wrappers / Serialization adapters"]

    config --> api --> connectors --> translation
    translation --> zeta["SeaTunnel Engine (Zeta)<br/>Master / Worker / Checkpoint"]
    translation --> flink["Apache Flink<br/>JobManager / TaskManager / State"]
    translation --> spark["Apache Spark<br/>Driver / Executor / RDD / Dataset"]

    classDef layerBlue fill:#0f1d33,stroke:#5db8e2,stroke-width:2px,color:#f8fbff;
    classDef layerCyan fill:#0c2530,stroke:#2dd4bf,stroke-width:2px,color:#f8fbff;
    classDef layerPurple fill:#1f1a34,stroke:#8d7cf6,stroke-width:2px,color:#f8fbff;

    class config,api layerBlue;
    class connectors,translation layerCyan;
    class zeta,flink,spark layerPurple;
    linkStyle default stroke:#5db8e2,stroke-width:2px;
```

### 2.1 Layer Responsibilities

| Layer | Responsibility | Key Components |
|-------|---------------|----------------|
| **Configuration Layer** | Job definition, parameter configuration | HOCON parser, SQL parser, config validation |
| **API Layer** | Unified abstraction for connectors | Source/Sink/Transform interfaces, CatalogTable |
| **Connector Layer** | Data source/sink implementations | Various connectors (JDBC, Kafka, CDC, etc.) |
| **Translation Layer** | Engine-specific adaptation | Flink/Spark adapters, context wrappers |
| **Engine Layer** | Job execution and resource management | Scheduling, fault tolerance, state management |

## 3. Core Components

### 3.1 SeaTunnel API

The API layer provides engine-independent abstractions:

#### Source API
- **SeaTunnelSource**: Factory interface for creating readers and enumerators
- **SourceSplitEnumerator**: Master-side component for split generation and assignment
- **SourceReader**: Worker-side component for reading data from splits
- **SourceSplit**: Minimal serializable unit representing a data partition

**Key Design**: Separation of coordination (Enumerator) and execution (Reader) enables efficient parallel processing and fault tolerance.

**Code Reference**:
- [seatunnel-api/.../SeaTunnelSource.java](../../../seatunnel-api/src/main/java/org/apache/seatunnel/api/source/SeaTunnelSource.java)
- [seatunnel-api/.../SourceSplitEnumerator.java](../../../seatunnel-api/src/main/java/org/apache/seatunnel/api/source/SourceSplitEnumerator.java)

#### Sink API
- **SeaTunnelSink**: Factory interface for creating writers and optional commit strategies
- **SinkWriter**: Worker-side component for writing data
- **SinkCommitter**: Optional worker-side committer for per-writer commit operations
- **SinkAggregatedCommitter**: Optional global committer for coordinator-side aggregated commits

**Key Design**: Two-phase commit protocol (prepareCommit → commit) ensures exactly-once semantics.

**Code Reference**:
- [seatunnel-api/.../SeaTunnelSink.java](../../../seatunnel-api/src/main/java/org/apache/seatunnel/api/sink/SeaTunnelSink.java)
- [seatunnel-api/.../SinkWriter.java](../../../seatunnel-api/src/main/java/org/apache/seatunnel/api/sink/SinkWriter.java)

#### Transform API
- **SeaTunnelTransform**: Data transformation interface
- **SeaTunnelMapTransform**: 1:1 transformation
- **SeaTunnelFlatMapTransform**: 1:N transformation

**Code Reference**:
- [seatunnel-api/.../SeaTunnelTransform.java](../../../seatunnel-api/src/main/java/org/apache/seatunnel/api/transform/SeaTunnelTransform.java)

#### Table API
- **CatalogTable**: Complete table metadata (schema, partition keys, options)
- **TableSchema**: Schema definition (columns, primary key, constraints)
- **SchemaChangeEvent**: Represents DDL changes for schema evolution

**Code Reference**:
- [seatunnel-api/.../CatalogTable.java](../../../seatunnel-api/src/main/java/org/apache/seatunnel/api/table/catalog/CatalogTable.java)

### 3.2 SeaTunnel Engine (Zeta)

The native execution engine provides:

#### Master Components
- **CoordinatorService**: Manages all running JobMasters
- **JobMaster**: Manages single job lifecycle, generates physical plans, coordinates checkpoints
- **CheckpointCoordinator**: Coordinates distributed snapshots per pipeline
- **ResourceManager**: Manages worker resources and slot allocation

#### Worker Components
- **TaskExecutionService**: Deploys and executes tasks
- **SeaTunnelTask**: Executes Source/Transform/Sink logic
- **FlowLifeCycle**: Manages lifecycle of Source/Transform/Sink components

#### Execution Model
```mermaid
flowchart LR
    logical["LogicalDag"] --> plan["PhysicalPlan"] --> pipeline["SubPlan<br/>(Pipeline)"] --> vertex["PhysicalVertex"] --> taskGroup["TaskGroup"] --> task["SeaTunnelTask"]

    classDef layerBlue fill:#0f1d33,stroke:#5db8e2,stroke-width:2px,color:#f8fbff;
    classDef layerPurple fill:#1f1a34,stroke:#8d7cf6,stroke-width:2px,color:#f8fbff;

    class logical,plan,pipeline layerBlue;
    class vertex,taskGroup,task layerPurple;
    linkStyle default stroke:#5db8e2,stroke-width:2px;
```

**Code Reference**:
- [seatunnel-engine/.../server/CoordinatorService.java](../../../seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/CoordinatorService.java)
- [seatunnel-engine/.../server/master/JobMaster.java](../../../seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/master/JobMaster.java)

### 3.3 Translation Layer

Enables engine portability through adapter pattern:

- **FlinkSource/FlinkSink**: Adapts SeaTunnel API to Flink's Source/Sink interfaces
- **SparkSource/SparkSink**: Adapts SeaTunnel API to Spark's RDD/Dataset interfaces
- **Context Adapters**: Wraps engine-specific contexts (SourceReaderContext, SinkWriterContext)
- **Serialization Adapters**: Bridges SeaTunnel and engine serialization mechanisms

**Code Reference**:
- [seatunnel-translation/.../flink/source/FlinkSource.java](../../../seatunnel-translation/seatunnel-translation-flink/seatunnel-translation-flink-common/src/main/java/org/apache/seatunnel/translation/flink/source/FlinkSource.java)

### 3.4 Connector Ecosystem

All connectors follow a standardized structure:

| Area | Typical Files | Responsibility |
|------|---------------|----------------|
| Source entry | `[Name]Source.java`, `[Name]SourceReader.java`, `[Name]SourceSplitEnumerator.java`, `[Name]SourceSplit.java` | Read data, split work, and expose a unified Source contract |
| Sink entry | `[Name]Sink.java`, `[Name]SinkWriter.java` | Buffer, write, and commit data to the target system |
| Configuration | `config/[Name]Config.java` | Define connector options, validation rules, and defaults |
| SPI registration | `META-INF/services/TableSourceFactory`, `META-INF/services/TableSinkFactory` | Register factories for discovery and runtime loading |

**Discovery Mechanism**: Java SPI (Service Provider Interface) for dynamic connector loading.

## 4. Data Flow Model

### 4.1 Source Data Flow

```mermaid
flowchart TD
    source["Data Source"] --> enumerator["SourceSplitEnumerator<br/>Master side<br/>Generate splits / Assign readers"]
    enumerator -->|Split assignment| reader["SourceReader<br/>Worker side<br/>Read split / Emit records"]
    reader --> rowIn["SeaTunnelRow"]
    rowIn --> transform["Transform Chain<br/>(Optional)"]
    transform --> rowOut["SeaTunnelRow"]
    rowOut --> writer["SinkWriter<br/>Worker side<br/>Buffer records / Prepare commit"]
    writer -->|"optional worker-local commit"| committer["SinkCommitter<br/>Worker side<br/>Commit each writer change independently"]
    writer -. "optional aggregated commit path" .-> aggregatedTask["SinkAggregatedCommitterTask<br/>Coordinator side<br/>Collect commit infos from writers"]
    aggregatedTask --> aggregated["SinkAggregatedCommitter<br/>Coordinator side<br/>Perform one global commit"]
    committer --> sink["Data Sink"]
    aggregated --> sink

    classDef layerBlue fill:#0f1d33,stroke:#5db8e2,stroke-width:2px,color:#f8fbff;
    classDef layerCyan fill:#0c2530,stroke:#2dd4bf,stroke-width:2px,color:#f8fbff;
    classDef layerPurple fill:#1f1a34,stroke:#8d7cf6,stroke-width:2px,color:#f8fbff;

    class source,sink,rowIn,rowOut layerBlue;
    class enumerator,reader,transform layerCyan;
    class writer,committer,aggregatedTask,aggregated layerPurple;
    linkStyle default stroke:#5db8e2,stroke-width:2px;
```

### 4.2 Split-based Parallelism

- Data sources are divided into **Splits** (e.g., file blocks, database partitions, Kafka partitions)
- Each **SourceReader** processes one or more splits independently
- Dynamic split assignment enables load balancing and fault recovery
- Split state is checkpointed for exactly-once processing

### 4.3 Pipeline Execution

Jobs are divided into **Pipelines** (SubPlans):

The example below shows two independent subplans inside the same job. They do not directly exchange records with each other.

```mermaid
flowchart TB
    subgraph pipeline1["Pipeline 1"]
        direction LR
        sourceA["Source A"] --> transformA["Transform 1"] --> sinkA["Sink A"]
    end

    subgraph pipeline2["Pipeline 2"]
        direction LR
        sourceB["Source B"] --> transformB["Transform 2"] --> sinkB["Sink B"]
    end

    classDef layerCyan fill:#0c2530,stroke:#2dd4bf,stroke-width:2px,color:#f8fbff;
    classDef layerPurple fill:#1f1a34,stroke:#8d7cf6,stroke-width:2px,color:#f8fbff;

    class sourceA,sourceB,transformA,transformB layerCyan;
    class sinkA,sinkB layerPurple;
    style pipeline1 fill:#081425,stroke:#5db8e2,stroke-width:1.5px,color:#f8fbff;
    style pipeline2 fill:#081425,stroke:#5db8e2,stroke-width:1.5px,color:#f8fbff;
    linkStyle default stroke:#5db8e2,stroke-width:2px;
```

Each pipeline:
- Has independent parallelism configuration
- Maintains its own checkpoint coordinator
- Can execute concurrently or sequentially

## 5. Job Execution Flow

### 5.1 Submission Phase

```mermaid
sequenceDiagram
    participant Client
    participant CoordinatorService
    participant JobMaster
    participant ResourceManager

    Client->>CoordinatorService: Submit Job Config
    CoordinatorService->>CoordinatorService: Parse Config → LogicalDag
    CoordinatorService->>JobMaster: Create JobMaster
    JobMaster->>JobMaster: Generate PhysicalPlan
    JobMaster->>ResourceManager: Request Resources
    ResourceManager->>JobMaster: Allocate Slots
    JobMaster->>TaskExecutionService: Deploy Tasks
```

### 5.2 Execution Phase

1. **Task Initialization**
   - Deploy tasks to allocated slots
   - Initialize Source/Transform/Sink components
   - Restore state from checkpoint (if recovering)

2. **Data Processing**
   - SourceReader pulls data from splits
   - Data flows through transform chain
   - SinkWriter buffers and writes data

3. **Checkpoint Coordination**
   - CheckpointCoordinator triggers checkpoint
   - Checkpoint barriers flow through data pipeline
   - Tasks snapshot their state
   - Coordinator collects acknowledgements

4. **Commit Phase**
   - SinkWriter prepares commit information
   - A worker-local `SinkCommitter` commits each writer change independently, or a coordinator-side aggregated commit runs when configured
   - State persisted to checkpoint storage

### 5.3 State Machine

**Task State Transitions**:
```mermaid
stateDiagram-v2
    direction LR
    [*] --> CREATED
    CREATED --> INIT
    INIT --> WAITING_RESTORE: restore path
    INIT --> READY_START: fresh start
    WAITING_RESTORE --> READY_START
    READY_START --> STARTING
    STARTING --> RUNNING
    RUNNING --> PREPARE_CLOSE: normal completion
    PREPARE_CLOSE --> CLOSED
    INIT --> CANCELLING: external cancel
    WAITING_RESTORE --> CANCELLING
    READY_START --> CANCELLING
    STARTING --> CANCELLING
    RUNNING --> CANCELLING
    PREPARE_CLOSE --> CANCELLING
    CANCELLING --> CANCELED
```

**Failure Note**:
- `FAILED` exists as a runtime result, but task-level restart is handled by higher-level recovery logic rather than by a direct `FAILED → ...` edge in this state machine.

**Job State Transitions**:
```mermaid
stateDiagram-v2
    direction LR
    [*] --> CREATED
    CREATED --> SCHEDULED
    SCHEDULED --> RUNNING
    RUNNING --> FINISHED
    SCHEDULED --> FAILED
    RUNNING --> FAILED
    RUNNING --> CANCELING
    CANCELING --> CANCELED
```

## 6. Key Features

### 6.1 Fault Tolerance

**Checkpoint Mechanism**:
- Distributed snapshots inspired by Chandy-Lamport algorithm
- Checkpoint barriers propagate through data streams
- State stored in pluggable checkpoint storage (HDFS, S3, local)
- Automatic recovery from latest successful checkpoint

**Failover Strategy**:
- Task-level failover: Restart failed task and related pipeline
- Region-based failover: Minimize impact on unaffected tasks
- Split reassignment: Failed splits redistributed to healthy workers

### 6.2 Exactly-Once Semantics

**Two-Phase Commit Protocol**:
1. **Prepare Phase**: SinkWriter prepares commit info during checkpoint
2. **Commit Phase**: A worker-local `SinkCommitter` commits each writer change independently, or a coordinator-side aggregated commit performs one global commit after checkpoint success
3. **Abort Handling**: Roll back on failure before commit

**Idempotency**: `SinkCommitter` and `SinkAggregatedCommitter` operations must be idempotent to handle retries

### 6.3 Dynamic Resource Management

- **Slot-based Allocation**: Fine-grained resource management
- **Tag-based Filtering**: Assign tasks to specific worker groups
- **Load Balancing**: Multiple strategies (random, slot ratio, system load)
- **Dynamic Scaling**: Add/remove workers without job restart (future)

### 6.4 Schema Evolution

- **DDL Propagation**: Capture schema changes from source (ADD/DROP/MODIFY columns)
- **Schema Mapping**: Transform schema changes through pipeline
- **Dynamic Application**: Apply schema changes to sink tables
- **Compatibility Checks**: Validate schema changes before application

### 6.5 Multi-Table Support

- **Single Job, Multiple Tables**: Synchronize hundreds of tables in one job
- **Table Routing**: Route records to correct sink based on TablePath
- **Independent Schemas**: Each table maintains its own schema
- **Replica Support**: Multiple writer replicas per table for higher throughput

## 7. Module Structure

| Module | Representative subdirectories | Responsibility |
|--------|-------------------------------|----------------|
| `seatunnel-api` | `source`, `sink`, `transform`, `table` | Defines the core APIs, table model, and engine-neutral abstractions |
| `seatunnel-connectors-v2` | `connector-jdbc`, `connector-kafka`, `connector-cdc-mysql` | Implements source and sink connectors |
| `seatunnel-transforms-v2` | `transform-sql`, `transform-filter` | Provides reusable transform implementations |
| `seatunnel-engine` | `seatunnel-engine-core`, `seatunnel-engine-server`, `seatunnel-engine-storage` | Hosts Zeta execution, scheduling, and checkpoint storage |
| `seatunnel-translation` | `seatunnel-translation-flink`, `seatunnel-translation-spark` | Adapts SeaTunnel APIs to different execution engines |
| `seatunnel-formats` | `seatunnel-format-json`, `seatunnel-format-avro` | Handles data format serialization and parsing |
| `seatunnel-core` | CLI and submission entrypoints | Owns job submission and command-line capabilities |
| `seatunnel-e2e` | End-to-end test suites | Covers critical regression scenarios |

## 8. Design Principles

### 8.1 Separation of Concerns

- **API vs Implementation**: Clean API boundaries enable multiple implementations
- **Coordination vs Execution**: Enumerator and aggregated-commit orchestration handle coordination, while Reader/Writer execute on workers
- **Logical vs Physical**: LogicalDag (user intent) separate from PhysicalPlan (execution details)

### 8.2 Plugin Architecture

- **SPI-based Discovery**: Connectors loaded dynamically via Java SPI
- **Class Loader Isolation**: Each connector uses isolated class loader
- **Hot Pluggable**: Add connectors without rebuilding core

### 8.3 Engine Independence

- **Unified API**: Same connector code runs on any engine
- **Translation Layer**: Adapts API to engine specifics
- **No Engine Leakage**: Connector developers don't need engine knowledge

### 8.4 Scalability

- **Horizontal Scaling**: Add workers to increase throughput
- **Split-based Parallelism**: Fine-grained parallel processing
- **Stateless Workers**: Workers can be added/removed dynamically

### 8.5 Reliability

- **Distributed Checkpoints**: Consistent snapshots across distributed tasks
- **Incremental State**: Optimize checkpoint size for large state
- **Exactly-Once Guarantee**: End-to-end consistency

## 9. Next Steps

To dive deeper into specific architectural components:

- [Design Philosophy](design-philosophy.md) - Core design principles and trade-offs
- [Transform Plugin System](transform-plugin-system.md) - How transform plugins are structured, discovered, and used to shape rows and schema
- [Table Schema and Type System](table-schema-and-type-system.md) - How schema, metadata, and portable types are modeled across connectors and engines
- [CDC Pipeline Architecture](cdc-pipeline-architecture.md) - How snapshot, incremental change capture, and sink application work together
- [Source Architecture](api-design/source-architecture.md) - Deep dive into Source API design
- [Sink Architecture](api-design/sink-architecture.md) - Deep dive into Sink API design
- [Plugin Discovery and Class Loading](plugin-discovery-and-class-loading.md) - How factories, jars, and isolated dependencies are resolved at runtime
- [Engine Architecture](engine/engine-architecture.md) - SeaTunnel Engine internals
- [Checkpoint Mechanism](fault-tolerance/checkpoint-mechanism.md) - Fault tolerance implementation

For practical guides:

- [How to Create Your Connector](../developer/how-to-create-your-connector.md)
- [Quick Start](../getting-started/locally/quick-start-seatunnel-engine.md)

## 10. References

### 10.1 Related Concepts

- [Apache Flink](https://flink.apache.org/) - Inspiration for checkpoint and state management
- [Apache Kafka](https://kafka.apache.org/) - Consumer group model influenced split assignment
- [Chandy-Lamport Algorithm](https://en.wikipedia.org/wiki/Chandy-Lamport_algorithm) - Distributed snapshot algorithm
