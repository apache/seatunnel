---
sidebar_position: 2
title: 精确一次语义
---

# 精确一次语义

## 1. 概述

### 1.1 问题背景

分布式数据处理面临基本的交付保证挑战:

- **至多一次**: 记录可能丢失(对关键数据不可接受)
- **至少一次**: 记录可能重复(导致计数错误、重复收费)
- **精确一次**: 每条记录恰好处理一次(理想但复杂)

**实际影响**:
```
场景: 金融交易处理

至少一次:
  交易 $100 处理两次 → 用户被收费 $200 ❌

精确一次:
  交易 $100 处理一次 → 用户被收费 $100 ✅
```

### 1.2 设计目标

SeaTunnel 的精确一次语义旨在:

1. **端到端保证**: 从数据源到目标端,无数据丢失或重复
2. **透明实现**: 框架处理复杂性,用户最少配置
3. **性能效率**: 在维护保证的同时最小化开销
4. **故障弹性**: 在任务/工作节点/主节点故障时维护保证
5. **广泛适用性**: 支持事务型和非事务型目标端

### 1.3 一致性级别

| 级别 | 保证 | 用例 | 实现 |
|------|------|------|------|
| **至多一次** | 无重复,可能丢失 | 非关键日志 | 无重试 |
| **至少一次** | 无丢失,可能重复 | 幂等处理 | 重试但无事务 |
| **精确一次** | 无丢失,无重复 | 金融、计费、审计 | 检查点 + 两阶段提交 |

## 2. 理论基础

### 2.1 Chandy-Lamport 算法

**概念**: 无需停止整个系统的分布式快照。

**机制**:
1. 协调器向数据流注入**屏障**(标记)
2. 收到屏障后,每个算子:
   - 快照其本地状态
   - 将屏障转发到下游
3. 当所有算子都完成快照时,我们有一个**一致的全局快照**

**关键属性**: 快照表示跨分布式系统状态的一致切割。

### 2.2 两阶段提交协议

**概念**: 跨分布式参与者的原子提交。

**阶段**:
1. **准备阶段**: 所有参与者准备(尚无副作用)
2. **提交阶段**: 协调器决定提交/中止,所有参与者执行

**在 SeaTunnel 中**:
- **准备**: 检查点期间的 `SinkWriter.prepareCommit()`
- **提交**: 检查点完成后的 `SinkCommitter.commit()`

## 3. 精确一次架构

### 3.1 端到端流水线

```
┌──────────────────────────────────────────────────────────────┐
│                       数据源                                  │
│  • 从外部系统读取                                             │
│  • 跟踪偏移量/位置                                            │
│  • 在检查点中快照偏移量                                        │
└──────────────────────────────┬───────────────────────────────┘
                               │
                               ▼ 检查点屏障
┌──────────────────────────────────────────────────────────────┐
│                     转换器                                    │
│  • 处理记录                                                   │
│  • 快照转换器状态(如果有)                                     │
└──────────────────────────────┬───────────────────────────────┘
                               │
                               ▼ 检查点屏障
┌──────────────────────────────────────────────────────────────┐
│                   目标端写入器                                │
│  • 缓冲写入                                                   │
│  • prepareCommit() → 生成 CommitInfo (阶段 1)                │
│  • 快照写入器状态                                             │
└──────────────────────────────┬───────────────────────────────┘
                               │
                               │ CommitInfo
                               ▼
┌──────────────────────────────────────────────────────────────┐
│              CheckpointCoordinator                            │
│  • 收集所有 CommitInfos                                       │
│  • 持久化 CompletedCheckpoint                                 │
│  • 触发提交阶段                                               │
└──────────────────────────────┬───────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────┐
│                  目标端提交器                                 │
│  • commit(CommitInfos) → 应用变更 (阶段 2)                   │
│  • 必须是幂等的                                               │
└──────────────────────────────┬───────────────────────────────┘
                               │
                               ▼
                    外部目标端
                 (变更可见)
```

### 3.2 关键组件

**数据源偏移量管理**:
```java
public class KafkaSourceReader {
    private Map<TopicPartition, Long> currentOffsets;

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) {
        ConsumerRecords<K, V> records = consumer.poll(timeout);
        for (ConsumerRecord<K, V> record : records) {
            // 处理记录
            output.collect(convert(record));

            // 跟踪偏移量
            currentOffsets.put(
                new TopicPartition(record.topic(), record.partition()),
                record.offset()
            );
        }
    }

    @Override
    public List<KafkaSourceState> snapshotState(long checkpointId) {
        // 快照偏移量(将在检查点完成后提交)
        return Collections.singletonList(new KafkaSourceState(currentOffsets));
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // 提交偏移量到 Kafka(幂等)
        consumer.commitSync(currentOffsets);
    }
}
```

**目标端两阶段提交**:
```java
public class JdbcExactlyOnceSinkWriter {
    private XAConnection xaConnection;
    private Xid currentXid;

    @Override
    public void write(SeaTunnelRow element) {
        if (currentXid == null) {
            // 开始 XA 事务
            currentXid = generateXid();
            xaConnection.getXAResource().start(currentXid, XAResource.TMNOFLAGS);
        }

        // 执行 INSERT(在 XA 事务中缓冲)
        statement.executeUpdate(toSQL(element));
    }

    @Override
    public Optional<XidInfo> prepareCommit() {
        if (currentXid == null) {
            return Optional.empty();
        }

        // 阶段 1: 准备(无副作用)
        xaConnection.getXAResource().end(currentXid, XAResource.TMSUCCESS);
        xaConnection.getXAResource().prepare(currentXid);

        // 返回 XID 给提交器
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
                // 阶段 2: 提交(副作用现在可见)
                xaConnection.getXAResource().commit(xidInfo.getXid(), false);
            } catch (XAException e) {
                if (e.errorCode == XAException.XAER_NOTA) {
                    // 已提交(幂等)
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

## 4. 实现模式

### 4.1 事务型目标端(XA)

**支持的系统**: MySQL、PostgreSQL、Oracle、SQL Server

**实现**:
```java
public class JdbcExactlyOnceSink implements SeaTunnelSink<...> {
    @Override
    public SinkWriter<...> createWriter(Context context) {
        // 启用 XA 事务
        XADataSource xaDataSource = createXADataSource();
        return new JdbcExactlyOnceSinkWriter(xaDataSource);
    }

    @Override
    public Optional<SinkCommitter<XidInfo>> createCommitter() {
        return Optional.of(new JdbcSinkCommitter(xaDataSource));
    }
}
```

**优点**:
- 强一致性保证
- 失败时自动回滚

**缺点**:
- 需要数据库 XA 支持
- 更高延迟(2PC 开销)
- 准备阶段期间锁争用

### 4.2 幂等目标端(Upsert)

**支持的系统**: 键值存储、Elasticsearch(带文档 ID)

**实现**:
```java
public class ElasticsearchSinkWriter {
    @Override
    public void write(SeaTunnelRow element) {
        // 使用确定性文档 ID
        String docId = extractPrimaryKey(element);

        IndexRequest request = new IndexRequest("my_index")
            .id(docId) // 幂等键
            .source(toJson(element));

        bulkProcessor.add(request);
    }

    @Override
    public Optional<CommitInfo> prepareCommit() {
        // 刷新批处理处理器
        bulkProcessor.flush();

        // 不需要显式提交(操作是幂等的)
        return Optional.empty();
    }
}
```

**关键**: 相同主键 → 相同文档 → 幂等更新

**优点**:
- 无事务开销
- 更低延迟

**缺点**:
- 需要唯一键
- 无法处理复杂事务

### 4.3 基于日志的目标端(Kafka)

**实现**:
```java
public class KafkaSinkWriter {
    private KafkaProducer<K, V> producer;
    private String transactionId;

    public KafkaSinkWriter() {
        // 启用 Kafka 事务
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
    public Optional<KafkaCommitInfo> prepareCommit() {
        // 阶段 1: 准备(刷新,但不提交)
        producer.flush();

        // 返回事务信息
        return Optional.of(new KafkaCommitInfo(transactionId));
    }
}

public class KafkaSinkCommitter {
    @Override
    public List<KafkaCommitInfo> commit(List<KafkaCommitInfo> commitInfos) {
        for (KafkaCommitInfo info : commitInfos) {
            // 阶段 2: 提交事务
            producer.commitTransaction();

            // 为下一个检查点开始新事务
            producer.beginTransaction();
        }
        return Collections.emptyList();
    }
}
```

### 4.4 文件目标端(原子重命名)

**实现**:
```java
public class FileSinkWriter {
    private String tempFilePath;
    private String finalFilePath;
    private OutputStream outputStream;

    @Override
    public void write(SeaTunnelRow element) {
        // 写入临时文件
        byte[] bytes = serialize(element);
        outputStream.write(bytes);
    }

    @Override
    public Optional<FileCommitInfo> prepareCommit() {
        // 阶段 1: 关闭临时文件(尚未重命名)
        outputStream.close();

        return Optional.of(new FileCommitInfo(tempFilePath, finalFilePath));
    }
}

public class FileSinkCommitter {
    @Override
    public List<FileCommitInfo> commit(List<FileCommitInfo> commitInfos) {
        List<FileCommitInfo> failed = new ArrayList<>();

        for (FileCommitInfo info : commitInfos) {
            // 阶段 2: 原子重命名(文件变得可见)
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

**关键**: 原子重命名确保文件要么完全可见要么不可见。

## 5. 故障场景和恢复

### 5.1 检查点前任务故障

```
时间线:
  t0: 检查点 N 完成
  t1: 处理记录 [1000-2000]
  t2: 任务失败 ❌
  t3: 从检查点 N 恢复
  t4: 重新处理记录 [1000-2000]

结果:
  ✅ 无数据丢失(记录重新处理)
  ✅ 无重复(故障前未提交任何内容)
```

### 5.2 prepareCommit 后任务故障

```
时间线:
  t0: 检查点 N 进行中
  t1: SinkWriter.prepareCommit() → XID-123 已准备
  t2: 任务失败 ❌ (提交前)
  t3: 从检查点 N-1 恢复
  t4: 重新处理记录
  t5: 新的 prepareCommit() → XID-124 已准备
  t6: 提交器提交 XID-124

结果:
  ✅ XID-123 从未提交(超时后自动回滚)
  ✅ XID-124 已提交(正确数据)
```

### 5.3 提交期间提交器故障

```
时间线:
  t0: 检查点 N 完成
  t1: 提交器开始提交 [XID-100, XID-101, XID-102]
  t2: 提交 XID-100 ✅
  t3: 提交器失败 ❌ (XID-101, XID-102 未提交)
  t4: 新提交器重试 [XID-100, XID-101, XID-102]
  t5: 提交 XID-100 (已提交,幂等) ✅
  t6: 提交 XID-101 ✅
  t7: 提交 XID-102 ✅

结果:
  ✅ 所有 XID 最终提交
  ✅ 无重复(幂等提交)
```

### 5.4 网络分区

```
时间线:
  t0: SinkWriter 准备 XID-200
  t1: 检查点完成
  t2: 提交器发送 commit(XID-200)
  t3: 网络分区 ⚠️ (提交成功,但 ACK 丢失)
  t4: 提交器重试 commit(XID-200)
  t5: XID-200 已提交(幂等)

结果:
  ✅ 数据恰好提交一次
  ✅ 幂等性防止重复
```

## 6. 幂等性要求

### 6.1 为什么幂等性很重要

**问题**: 网络故障、重试和故障转移可能导致重复的提交尝试。

**解决方案**: 提交器操作必须是幂等的。

```java
// ❌ 差: 非幂等(调用两次插入两次)
void commit(CommitInfo info) {
    statement.execute("INSERT INTO table VALUES (1, 'data')");
}

// ✅ 好: 幂等(调用两次与调用一次效果相同)
void commit(CommitInfo info) {
    statement.execute(
        "INSERT INTO table VALUES (1, 'data') " +
        "ON DUPLICATE KEY UPDATE data = VALUES(data)"
    );
}
```

### 6.2 实现幂等性

**策略 1: 检查后执行**
```java
public List<XidInfo> commit(List<XidInfo> commitInfos) {
    for (XidInfo xid : commitInfos) {
        // 检查是否已提交
        if (isCommitted(xid)) {
            LOG.info("XID already committed: {}", xid);
            continue; // 幂等
        }

        // 提交并记录
        xaResource.commit(xid, false);
        recordCommit(xid);
    }
}
```

**策略 2: 数据库级幂等性**
```sql
-- 唯一约束确保幂等性
CREATE TABLE commits (
    xid VARCHAR(255) PRIMARY KEY,
    committed_at TIMESTAMP
);

-- 幂等插入
INSERT IGNORE INTO commits (xid, committed_at)
VALUES ('XID-123', NOW());
```

**策略 3: 自然幂等性(XA)**
```java
try {
    xaResource.commit(xid, false);
} catch (XAException e) {
    if (e.errorCode == XAException.XAER_NOTA) {
        // 找不到事务 = 已提交
        return; // 幂等
    }
    throw e;
}
```

## 7. 性能考虑

### 7.1 检查点间隔权衡

```
短间隔(10-30s):
  ✅ 快速恢复(重新处理更少)
  ❌ 更高开销(频繁快照)
  ❌ 更多提交操作

长间隔(5-10分钟):
  ✅ 更低开销(快照更少)
  ❌ 恢复更慢(重新处理更多)
  ✅ 更少提交操作
```

**建议**: 大多数工作负载 60-120 秒

### 7.2 批量大小优化

```java
public class OptimizedSinkWriter {
    private static final int BATCH_SIZE = 1000;
    private List<SeaTunnelRow> buffer = new ArrayList<>();

    @Override
    public void write(SeaTunnelRow element) {
        buffer.add(element);

        if (buffer.size() >= BATCH_SIZE) {
            // 批量插入(分摊开销)
            statement.executeBatch();
            buffer.clear();
        }
    }
}
```

**影响**: 1000x 批量 → ~10x 吞吐量提升

### 7.3 异步检查点

```java
public List<StateT> snapshotState(long checkpointId) {
    // 快速: 复制状态快照(内存中)
    StateSnapshot snapshot = state.copy();

    // 异步: 序列化和上传
    CompletableFuture.runAsync(() -> {
        byte[] serialized = serialize(snapshot);
        checkpointStorage.upload(checkpointId, serialized);
    });

    return snapshot;
}
```

**影响**: 快照上传时数据处理继续

## 8. 配置

### 8.1 启用精确一次

```hocon
env {
  # 检查点配置
  checkpoint.interval = 60000 # 60 秒
  checkpoint.timeout = 600000 # 10 分钟

  # 精确一次模式(vs 至少一次)
  # 使用事务型目标端时这是隐式的
}
```

### 8.2 数据源配置

**Kafka**:
```hocon
source {
  Kafka {
    bootstrap.servers = "localhost:9092"
    topic = "my_topic"

    # Kafka 消费者偏移量提交
    commit_on_checkpoint = true # 检查点后提交偏移量
  }
}
```

**JDBC**:
```hocon
source {
  JDBC {
    url = "jdbc:mysql://..."

    # 基于查询的数据源(幂等重新处理)
    query = "SELECT * FROM table WHERE id >= ? AND id < ?"
  }
}
```

### 8.3 目标端配置

**JDBC (XA)**:
```hocon
sink {
  JDBC {
    url = "jdbc:mysql://..."

    # 启用 XA 事务
    xa_data_source_class_name = "com.mysql.cj.jdbc.MysqlXADataSource"
    is_exactly_once = true
  }
}
```

**Kafka (事务)**:
```hocon
sink {
  Kafka {
    bootstrap.servers = "localhost:9092"
    topic = "output_topic"

    # Kafka 事务
    transaction.id = "seatunnel-kafka-sink"
    enable.idempotence = true
  }
}
```

## 9. 测试精确一次

### 9.1 功能测试

```java
@Test
public void testExactlyOnce() {
    // 1. 插入 1000 条记录
    insertRecords(1000);

    // 2. 触发检查点
    coordinator.triggerCheckpoint();

    // 3. 模拟故障
    task.fail();

    // 4. 恢复并继续
    task.restore(checkpointId);
    insertRecords(1000); // 重新处理相同记录

    // 5. 验证: 应该恰好有 1000 条记录(无重复)
    assertEquals(1000, countRecordsInSink());
}
```

### 9.2 混沌测试

```java
@Test
public void testExactlyOnceUnderChaos() {
    ChaosMonkey chaos = new ChaosMonkey()
        .killTaskRandomly(probability = 0.1)
        .injectNetworkDelay(maxDelayMs = 5000)
        .pauseCheckpointRandomly(probability = 0.05);

    // 在混沌下运行 10 分钟
    runJobWithChaos(duration = 10 * 60 * 1000, chaos);

    // 验证: 输入计数 == 输出计数
    assertEquals(countSource(), countSink());
}
```

### 9.3 监控验证

```
要跟踪的指标:

source.records_read = 1,000,000
sink.records_written = 1,000,000
sink.records_committed = 1,000,000

✅ 所有计数匹配 → 精确一次验证
```

## 10. 最佳实践

### 10.1 选择适当的目标端

**使用事务型目标端(XA)用于**:
- 金融交易
- 计费系统
- 审计日志
- 关键数据

**使用幂等目标端用于**:
- 高吞吐量场景
- 可接受最终一致性
- 无事务支持

### 10.2 处理有毒记录

```java
@Override
public void write(SeaTunnelRow element) {
    try {
        statement.executeUpdate(toSQL(element));
    } catch (SQLException e) {
        // 记录有毒记录
        LOG.error("Failed to write record: {}", element, e);

        // 发送到死信队列
        deadLetterQueue.send(element);

        // 不要使整个检查点失败
    }
}
```

### 10.3 监控检查点健康

**关键指标**:
- `checkpoint.duration`: 应 < 间隔的 10%
- `checkpoint.failure_rate`: 应 < 1%
- `checkpoint.size`: 监控随时间增长

**警报**:
```
如果 checkpoint.duration > 300s 则告警
如果 checkpoint.failure_rate > 5% 则告警
如果在 2x 间隔内无检查点则告警
```

## 11. 相关资源

- [检查点机制](checkpoint-mechanism.md)
- [目标端架构](../api-design/sink-architecture.md)
- [数据源架构](../api-design/source-architecture.md)
- [引擎架构](../engine/engine-architecture.md)

## 12. 参考资料

### 学术论文

- Chandy & Lamport (1985): ["Distributed Snapshots"](https://lamport.azurewebsites.net/pubs/chandy.pdf)
- Gray & Lamport (2006): ["Consensus on Transaction Commit"](https://lamport.azurewebsites.net/pubs/paxos-commit.pdf)
- Carbone et al. (2017): ["State Management in Apache Flink"](http://www.vldb.org/pvldb/vol10/p1718-carbone.pdf)

### 进一步阅读

- [两阶段提交协议](https://en.wikipedia.org/wiki/Two-phase_commit_protocol)
- [XA 事务](https://pubs.opengroup.org/onlinepubs/009680699/toc.pdf)
- [Kafka 精确一次](https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/)
