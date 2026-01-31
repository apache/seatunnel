---
sidebar_position: 2
title: Exactly-Once Semantics
---

# Exactly-Once Semantics

## 1. Overview

### 1.1 Problem Background

Distributed data processing faces fundamental delivery guarantees challenges:

- **At-Most-Once**: Records may be lost (unacceptable for critical data)
- **At-Least-Once**: Records may be duplicated (causes counting errors, double charges)
- **Exactly-Once**: Each record processed exactly once (ideal but complex)

**Real-World Impact**:
```
Scenario: Financial transaction processing

At-Least-Once:
  Transaction $100 processed twice → User charged $200 ❌

Exactly-Once:
  Transaction $100 processed once → User charged $100 ✅
```

### 1.2 Design Goals

SeaTunnel's exactly-once semantics aims to:

1. **Verifiable End-to-End Consistency**: With checkpoint boundaries + sink transactional/idempotent commits, avoid data loss/duplication under the documented failure model
2. **Transparent Implementation**: Framework handles complexity, users configure minimally
3. **Performance Efficiency**: Minimize overhead while maintaining guarantee
4. **Failure Resilience**: Maintain guarantee across task/worker/master failures
5. **Broad Applicability**: Support transactional sinks and also provide practical semantics for non-transactional sinks (e.g., idempotent writes / at-least-once)

### 1.3 Consistency Levels

| Level | Guarantee | Use Cases | Implementation |
|-------|-----------|-----------|----------------|
| **At-Most-Once** | No duplicates, may lose | Non-critical logs | No retry |
| **At-Least-Once** | No loss, may duplicate | Idempotent processing | Retry without transaction |
| **Exactly-Once** | No loss, no duplicates | Financial, billing, audit | Checkpoint + 2PC |

## 2. Theoretical Foundation

### 2.1 Chandy-Lamport Algorithm

**Concept**: Distributed snapshot without stopping the entire system.

**Mechanism**:
1. Coordinator injects **barriers** (markers) into data streams
2. Upon receiving barrier, each operator:
   - Snapshots its local state
   - Forwards barrier downstream
3. When all operators snapshot, we have a **consistent global snapshot**

**Key Property**: Snapshot represents a consistent cut across distributed system state.

### 2.2 Two-Phase Commit Protocol

**Concept**: Atomic commitment across distributed participants.

**Phases**:
1. **Prepare Phase**: All participants prepare (avoid making changes externally visible)
2. **Commit Phase**: Coordinator decides commit/abort, all participants execute

**In SeaTunnel**:
- **Prepare**: `SinkWriter.prepareCommit(checkpointId)` during checkpoint
- **Commit**: `SinkCommitter.commit()` after checkpoint completes

## 3. Architecture for Exactly-Once

### 3.1 End-to-End Pipeline

```
┌──────────────────────────────────────────────────────────────┐
│                       Source                                  │
│  • Read from external system                                  │
│  • Track offsets/positions                                    │
│  • Snapshot offsets in checkpoint                             │
└──────────────────────────┬───────────────────────────────────┘
                           │
                           ▼ Checkpoint Barrier
┌──────────────────────────────────────────────────────────────┐
│                     Transform                                 │
│  • Process records                                            │
│  • Snapshot transform state (if any)                          │
└──────────────────────────┬───────────────────────────────────┘
                           │
                           ▼ Checkpoint Barrier
┌──────────────────────────────────────────────────────────────┐
│                     Sink Writer                               │
│  • Buffer writes                                              │
│  • prepareCommit(checkpointId) → Generate CommitInfo (PHASE 1)│
│  • Snapshot writer state                                      │
└──────────────────────────┬───────────────────────────────────┘
                           │
                           │ CommitInfo
                           ▼
┌──────────────────────────────────────────────────────────────┐
│              CheckpointCoordinator                            │
│  • Collect all CommitInfos                                    │
│  • Persist CompletedCheckpoint                                │
│  • Trigger commit phase                                       │
└──────────────────────────┬───────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│                    Sink Committer                             │
│  • commit(CommitInfos) → Apply changes (PHASE 2)              │
│  • Must be idempotent                                         │
└──────────────────────────┬───────────────────────────────────┘
                           │
                           ▼
                    External Sink
                 (Changes visible)
```

### 3.2 Key Components

**Source Offset Management**:
```java
public class KafkaSourceReader {
    private Map<TopicPartition, Long> currentOffsets;

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) {
        ConsumerRecords<K, V> records = consumer.poll(timeout);
        for (ConsumerRecord<K, V> record : records) {
            // Process record
            output.collect(convert(record));

            // Track offset
            currentOffsets.put(
                new TopicPartition(record.topic(), record.partition()),
                record.offset()
            );
        }
    }

    @Override
    public List<KafkaSourceState> snapshotState(long checkpointId) {
        // Snapshot offsets (will be committed after checkpoint completes)
        return Collections.singletonList(new KafkaSourceState(currentOffsets));
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // Commit offsets to Kafka (idempotent)
        consumer.commitSync(currentOffsets);
    }
}
```

**Sink Two-Phase Commit**:
```java
public class JdbcExactlyOnceSinkWriter {
    private XAConnection xaConnection;
    private Xid currentXid;

    @Override
    public void write(SeaTunnelRow element) {
        if (currentXid == null) {
            // Start XA transaction
            currentXid = generateXid();
            xaConnection.getXAResource().start(currentXid, XAResource.TMNOFLAGS);
        }

        // Execute INSERT (buffered in XA transaction)
        statement.executeUpdate(toSQL(element));
    }

    @Override
    public Optional<XidInfo> prepareCommit(long checkpointId) {
        if (currentXid == null) {
            return Optional.empty();
        }

        // PHASE 1: Prepare (no side effects)
        xaConnection.getXAResource().end(currentXid, XAResource.TMSUCCESS);
        xaConnection.getXAResource().prepare(currentXid);

        // Return XID for committer
        XidInfo xidInfo = new XidInfo(currentXid);
        currentXid = null;
        return Optional.of(xidInfo);
    }
}

public class JdbcSinkCommitter {
    @Override
    public List<XidInfo> commit(List<XidInfo> commitInfos) {
        List<XidInfo> failed = new ArrayList<>();

        for (XidInfo xidInfo : commitInfos) {
            try {
                // PHASE 2: Commit (side effects now visible)
                xaConnection.getXAResource().commit(xidInfo.getXid(), false);
            } catch (XAException e) {
                if (e.errorCode == XAException.XAER_NOTA) {
                    // Already committed (idempotent)
                    LOG.info("XID already committed: {}", xidInfo);
                } else {
                    failed.add(xidInfo);
                }
            }
        }

        return failed;
    }
}
```

## 4. Implementation Patterns

### 4.1 Transactional Sinks (XA)

**Supported Systems**: MySQL, PostgreSQL, Oracle, SQL Server

**Implementation**:
```java
public class JdbcExactlyOnceSink implements SeaTunnelSink<...> {
    @Override
    public SinkWriter<...> createWriter(Context context) {
        // Enable XA transactions
        XADataSource xaDataSource = createXADataSource();
        return new JdbcExactlyOnceSinkWriter(xaDataSource);
    }

    @Override
    public Optional<SinkCommitter<XidInfo>> createCommitter() {
        return Optional.of(new JdbcSinkCommitter(xaDataSource));
    }
}
```

**Pros**:
- Strong consistency guarantee
- Automatic rollback on failure

**Cons**:
- Requires database XA support
- Higher latency (2PC overhead)
- Lock contention during prepare phase

### 4.2 Idempotent Sinks (Upsert)

**Supported Systems**: Key-value stores, Elasticsearch (with doc ID)

**Implementation**:
```java
public class ElasticsearchSinkWriter {
    @Override
    public void write(SeaTunnelRow element) {
        // Use deterministic document ID
        String docId = extractPrimaryKey(element);

        IndexRequest request = new IndexRequest("my_index")
            .id(docId) // Idempotent key
            .source(toJson(element));

        bulkProcessor.add(request);
    }

    @Override
    public Optional<CommitInfo> prepareCommit(long checkpointId) {
        // Flush bulk processor
        bulkProcessor.flush();

        // No explicit commit needed (operations are idempotent)
        return Optional.empty();
    }
}
```

**Key**: Same primary key → same document → idempotent updates

**Pros**:
- No transaction overhead
- Lower latency

**Cons**:
- Requires unique key
- Cannot handle complex transactions

### 4.3 Log-Based Sinks (Kafka)

**Implementation**:
```java
public class KafkaSinkWriter {
    private KafkaProducer<K, V> producer;
    private String transactionId;

    public KafkaSinkWriter() {
        // Enable Kafka transactions
        Properties props = new Properties();
        props.put("transactional.id", generateTransactionalId());
        props.put("enable.idempotence", "true");

        producer = new KafkaProducer<>(props);
        producer.initTransactions();
    }

    @Override
    public void write(SeaTunnelRow element) {
        if (!transactionStarted) {
            producer.beginTransaction();
            transactionStarted = true;
        }

        ProducerRecord<K, V> record = convert(element);
        producer.send(record);
    }

    @Override
    public Optional<KafkaCommitInfo> prepareCommit(long checkpointId) {
        // PHASE 1: Prepare (flush, but don't commit)
        producer.flush();

        // Return transaction info
        return Optional.of(new KafkaCommitInfo(transactionId));
    }
}

public class KafkaSinkCommitter {
    @Override
    public List<KafkaCommitInfo> commit(List<KafkaCommitInfo> commitInfos) {
        for (KafkaCommitInfo info : commitInfos) {
            // PHASE 2: Commit transaction
            producer.commitTransaction();

            // Start new transaction for next checkpoint
            producer.beginTransaction();
        }
        return Collections.emptyList();
    }
}
```

### 4.4 File Sinks (Atomic Rename)

**Implementation**:
```java
public class FileSinkWriter {
    private String tempFilePath;
    private String finalFilePath;
    private OutputStream outputStream;

    @Override
    public void write(SeaTunnelRow element) {
        // Write to temporary file
        byte[] bytes = serialize(element);
        outputStream.write(bytes);
    }

    @Override
    public Optional<FileCommitInfo> prepareCommit(long checkpointId) {
        // PHASE 1: Close temp file (no rename yet)
        outputStream.close();

        return Optional.of(new FileCommitInfo(tempFilePath, finalFilePath));
    }
}

public class FileSinkCommitter {
    @Override
    public List<FileCommitInfo> commit(List<FileCommitInfo> commitInfos) {
        List<FileCommitInfo> failed = new ArrayList<>();

        for (FileCommitInfo info : commitInfos) {
            // PHASE 2: Atomic rename (file becomes visible)
            boolean success = fileSystem.rename(
                new Path(info.getTempFilePath()),
                new Path(info.getFinalFilePath())
            );

            if (!success) {
                failed.add(info);
            }
        }

        return failed;
    }
}
```

**Key**: Atomic rename ensures file is either fully visible or not visible.

## 5. Failure Scenarios and Recovery

### 5.1 Task Failure Before Checkpoint

```
Timeline:
  t0: Checkpoint N completed
  t1: Process records [1000-2000]
  t2: Task fails ❌
  t3: Restore from Checkpoint N
  t4: Reprocess records [1000-2000]

Result:
  ✅ No data loss (records reprocessed)
  ✅ No duplication (nothing committed before failure)
```

### 5.2 Task Failure After prepareCommit

```
Timeline:
  t0: Checkpoint N in progress
  t1: SinkWriter.prepareCommit(checkpointId) → XID-123 prepared
  t2: Task fails ❌ (before commit)
  t3: Restore from Checkpoint N-1
  t4: Reprocess records
  t5: New prepareCommit(checkpointId) → XID-124 prepared
  t6: Committer commits XID-124

Result:
  ✅ XID-123 never committed (automatically rolled back after timeout)
  ✅ XID-124 committed (correct data)
```

### 5.3 Committer Failure During Commit

```
Timeline:
  t0: Checkpoint N completed
  t1: Committer starts committing [XID-100, XID-101, XID-102]
  t2: Commits XID-100 ✅
  t3: Committer fails ❌ (XID-101, XID-102 not committed)
  t4: New committer retries [XID-100, XID-101, XID-102]
  t5: Commits XID-100 (already committed, idempotent) ✅
  t6: Commits XID-101 ✅
  t7: Commits XID-102 ✅

Result:
  ✅ All XIDs eventually committed
  ✅ No duplication (idempotent commit)
```

### 5.4 Network Partition

```
Timeline:
  t0: SinkWriter prepares XID-200
  t1: Checkpoint completes
  t2: Committer sends commit(XID-200)
  t3: Network partition ⚠️ (commit success, but ACK lost)
  t4: Committer retries commit(XID-200)
  t5: XID-200 already committed (idempotent)

Result:
  ✅ Data committed exactly once
  ✅ Idempotency prevents duplication
```

## 6. Idempotency Requirements

### 6.1 Why Idempotency Matters

**Problem**: Network failures, retries, and failover can cause duplicate commit attempts.

**Solution**: Committer operations must be idempotent.

```java
// ❌ BAD: Non-idempotent (calling twice inserts twice)
void commit(CommitInfo info) {
    statement.execute("INSERT INTO table VALUES (1, 'data')");
}

// ✅ GOOD: Idempotent (calling twice has same effect as once)
void commit(CommitInfo info) {
    statement.execute(
        "INSERT INTO table VALUES (1, 'data') " +
        "ON DUPLICATE KEY UPDATE data = VALUES(data)"
    );
}
```

### 6.2 Implementing Idempotency

**Strategy 1: Check-then-Execute**
```java
public List<XidInfo> commit(List<XidInfo> commitInfos) {
    for (XidInfo xid : commitInfos) {
        // Check if already committed
        if (isCommitted(xid)) {
            LOG.info("XID already committed: {}", xid);
            continue; // Idempotent
        }

        // Commit and record
        xaResource.commit(xid, false);
        recordCommit(xid);
    }
}
```

**Strategy 2: Database-Level Idempotency**
```sql
-- Unique constraint ensures idempotency
CREATE TABLE commits (
    xid VARCHAR(255) PRIMARY KEY,
    committed_at TIMESTAMP
);

-- Idempotent insert
INSERT IGNORE INTO commits (xid, committed_at)
VALUES ('XID-123', NOW());
```

**Strategy 3: Natural Idempotency (XA)**
```java
try {
    xaResource.commit(xid, false);
} catch (XAException e) {
    if (e.errorCode == XAException.XAER_NOTA) {
        // Transaction not found = already committed
        return; // Idempotent
    }
    throw e;
}
```

## 7. Performance Considerations

### 7.1 Checkpoint Interval Trade-offs

```
Short Interval (10-30s):
  ✅ Fast recovery (less reprocessing)
  ❌ Higher overhead (frequent snapshots)
  ❌ More commit operations

Long Interval (5-10min):
  ✅ Lower overhead (less frequent snapshots)
  ❌ Slower recovery (more reprocessing)
  ✅ Fewer commit operations
```

**Recommendation**: 60-120 seconds for most workloads

### 7.2 Batch Size Optimization

```java
public class OptimizedSinkWriter {
    private static final int BATCH_SIZE = 1000;
    private List<SeaTunnelRow> buffer = new ArrayList<>();

    @Override
    public void write(SeaTunnelRow element) {
        buffer.add(element);

        if (buffer.size() >= BATCH_SIZE) {
            // Batch insert (amortize overhead)
            statement.executeBatch();
            buffer.clear();
        }
    }
}
```

**Impact**: 1000x batch → ~10x throughput improvement

### 7.3 Async Checkpoint

```java
public List<StateT> snapshotState(long checkpointId) {
    // Quick: Copy state snapshot (in-memory)
    StateSnapshot snapshot = state.copy();

    // Async: Serialize and upload
    CompletableFuture.runAsync(() -> {
        byte[] serialized = serialize(snapshot);
        checkpointStorage.upload(checkpointId, serialized);
    });

    return snapshot;
}
```

**Impact**: Data processing continues while snapshot uploads

## 8. Configuration

### 8.1 Enable Exactly-Once

```hocon
env {
  # Checkpoint configuration
  checkpoint.interval = 60000 # 60 seconds
  checkpoint.timeout = 600000 # 10 minutes

  # Exactly-once mode (vs at-least-once)
  # This is implicit when using transactional sinks
}
```

### 8.2 Source Configuration

**Kafka**:
```hocon
source {
  Kafka {
    bootstrap.servers = "localhost:9092"
    topic = "my_topic"

    # Kafka consumer offset commit
    commit_on_checkpoint = true # Commit offsets after checkpoint
  }
}
```

**JDBC**:
```hocon
source {
  JDBC {
    url = "jdbc:mysql://..."

    # Query-based source (idempotent reprocessing)
    query = "SELECT * FROM table WHERE id >= ? AND id < ?"
  }
}
```

### 8.3 Sink Configuration

**JDBC (XA)**:
```hocon
sink {
  JDBC {
    url = "jdbc:mysql://..."

    # Enable XA transactions
    xa_data_source_class_name = "com.mysql.cj.jdbc.MysqlXADataSource"
    is_exactly_once = true
  }
}
```

**Kafka (Transactions)**:
```hocon
sink {
  Kafka {
    bootstrap.servers = "localhost:9092"
    topic = "output_topic"

    # Kafka transactions
    transaction.id = "seatunnel-kafka-sink"
    enable.idempotence = true
  }
}
```

## 9. Testing Exactly-Once

### 9.1 Functional Test

```java
@Test
public void testExactlyOnce() {
    // 1. Insert 1000 records
    insertRecords(1000);

    // 2. Trigger checkpoint
    coordinator.triggerCheckpoint();

    // 3. Simulate failure
    task.fail();

    // 4. Restore and continue
    task.restore(checkpointId);
    insertRecords(1000); // Same records reprocessed

    // 5. Verify: Should have exactly 1000 records (no duplicates)
    assertEquals(1000, countRecordsInSink());
}
```

### 9.2 Chaos Testing

```java
@Test
public void testExactlyOnceUnderChaos() {
    ChaosMonkey chaos = new ChaosMonkey()
        .killTaskRandomly(probability = 0.1)
        .injectNetworkDelay(maxDelayMs = 5000)
        .pauseCheckpointRandomly(probability = 0.05);

    // Run for 10 minutes with chaos
    runJobWithChaos(duration = 10 * 60 * 1000, chaos);

    // Verify: Input count == Output count
    assertEquals(countSource(), countSink());
}
```

### 9.3 Monitoring Verification

```
Metrics to Track:

source.records_read = 1,000,000
sink.records_written = 1,000,000
sink.records_committed = 1,000,000

✅ All counts match → Exactly-once verified
```

## 10. Best Practices

### 10.1 Choose Appropriate Sink

**Use Transactional Sinks (XA) for**:
- Financial transactions
- Billing systems
- Audit logs
- Critical data

**Use Idempotent Sinks for**:
- High-throughput scenarios
- Eventual consistency acceptable
- No transaction support

### 10.2 Handle Poisoned Records

```java
@Override
public void write(SeaTunnelRow element) {
    try {
        statement.executeUpdate(toSQL(element));
    } catch (SQLException e) {
        // Log poisoned record
        LOG.error("Failed to write record: {}", element, e);

        // Send to dead letter queue
        deadLetterQueue.send(element);

        // Don't fail entire checkpoint
    }
}
```

### 10.3 Monitor Checkpoint Health

**Key Metrics**:
- `checkpoint.duration`: Should be < 10% of interval
- `checkpoint.failure_rate`: Should be < 1%
- `checkpoint.size`: Monitor growth over time

**Alerts**:
```
Alert if checkpoint.duration > 300s
Alert if checkpoint.failure_rate > 5%
Alert if no checkpoint in 2x interval
```

## 11. Related Resources

- [Checkpoint Mechanism](checkpoint-mechanism.md)
- [Sink Architecture](../api-design/sink-architecture.md)
- [Source Architecture](../api-design/source-architecture.md)
- [Engine Architecture](../engine/engine-architecture.md)

## 12. References

### Academic Papers

- Chandy & Lamport (1985): ["Distributed Snapshots"](https://lamport.azurewebsites.net/pubs/chandy.pdf)
- Gray & Lamport (2006): ["Consensus on Transaction Commit"](https://lamport.azurewebsites.net/pubs/paxos-commit.pdf)
- Carbone et al. (2017): ["State Management in Apache Flink"](http://www.vldb.org/pvldb/vol10/p1718-carbone.pdf)

### Further Reading

- [Two-Phase Commit Protocol](https://en.wikipedia.org/wiki/Two-phase_commit_protocol)
- [XA Transactions](https://pubs.opengroup.org/onlinepubs/009680699/toc.pdf)
- [Kafka Exactly-Once](https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/)
