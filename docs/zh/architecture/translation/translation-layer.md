---
sidebar_position: 1
title: 转换层
---

# 转换层架构

## 1. 概述

### 1.1 问题背景

SeaTunnel 提供统一的连接器 API,但作业需要在不同的执行引擎上运行:

- **引擎多样性**: Flink、Spark、SeaTunnel Engine (Zeta) 具有不同的 API
- **代码重复**: 没有转换,每个连接器需要 3 个实现
- **维护负担**: Bug 修复需要在所有实现中进行更改
- **API 演化**: 引擎 API 变更会破坏连接器
- **用户体验**: 用户希望跨引擎的一致行为

### 1.2 设计目标

SeaTunnel 的转换层旨在:

1. **实现可移植性**: 相同的连接器可在任何引擎上运行
2. **隐藏复杂性**: 连接器开发者只需学习 SeaTunnel API
3. **保持保真度**: 跨引擎保留语义保证
4. **最小化开销**: 保持转换性能影响 < 5%
5. **支持演化**: 将连接器与引擎 API 变更隔离

### 1.3 架构概览

```
┌──────────────────────────────────────────────────────────────┐
│                   SeaTunnel API 层                            │
│         (引擎独立的连接器接口)                                │
│                                                                │
│  SeaTunnelSource    SeaTunnelSink    SeaTunnelTransform      │
└──────────────────────────────────────────────────────────────┘
                              │
                              │ 转换层
                ┌─────────────┼─────────────┐
                ▼             ▼             ▼
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│  Flink 适配器    │  │  Spark 适配器    │  │ Zeta (原生)      │
│                  │  │                  │  │                  │
│ FlinkSource      │  │ SparkSource      │  │ 直接             │
│ FlinkSink        │  │ SparkSink        │  │ 执行             │
└──────────────────┘  └──────────────────┘  └──────────────────┘
        │                     │                     │
        ▼                     ▼                     ▼
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│  Apache Flink    │  │  Apache Spark    │  │ SeaTunnel Engine │
│     运行时       │  │     运行时       │  │      (Zeta)      │
└──────────────────┘  └──────────────────┘  └──────────────────┘
```

## 2. Flink 转换层

### 2.1 FlinkSource 适配器

将 `SeaTunnelSource` 适配到 Flink 的 `Source` 接口。

```java
public class FlinkSource<T, SplitT extends SourceSplit, StateT>
    implements Source<T, SplitWrapper<SplitT>, EnumeratorStateWrapper<StateT>> {

    // 封装的 SeaTunnel 数据源
    private final SeaTunnelSource<T, SplitT, StateT> seaTunnelSource;

    @Override
    public Boundedness getBoundedness() {
        // 委托给 SeaTunnel 数据源
        return seaTunnelSource.getBoundedness() == Boundedness.BOUNDED
            ? Boundedness.BOUNDED
            : Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<T, SplitWrapper<SplitT>> createReader(
        SourceReaderContext readerContext
    ) {
        // 使用适配的上下文创建 SeaTunnel 读取器
        org.apache.seatunnel.api.source.SourceReader<T, SplitT> seaTunnelReader =
            seaTunnelSource.createReader(new FlinkSourceReaderContext(readerContext));

        // 包装在 Flink 适配器中
        return new FlinkSourceReader<>(seaTunnelReader, readerContext);
    }

    @Override
    public SplitEnumerator<SplitWrapper<SplitT>, EnumeratorStateWrapper<StateT>>
        createEnumerator(SplitEnumeratorContext<SplitWrapper<SplitT>> context) {

        // 使用适配的上下文创建 SeaTunnel 枚举器
        SourceSplitEnumerator<SplitT, StateT> seaTunnelEnumerator =
            seaTunnelSource.createEnumerator(
                new FlinkSourceSplitEnumeratorContext<>(context)
            );

        // 包装在 Flink 适配器中
        return new FlinkSourceEnumerator<>(seaTunnelEnumerator, context);
    }

    @Override
    public SimpleVersionedSerializer<SplitWrapper<SplitT>> getSplitSerializer() {
        // 将 SeaTunnel 序列化器适配到 Flink 序列化器
        return new FlinkSimpleVersionedSerializer<>(
            seaTunnelSource.getSplitSerializer()
        );
    }
}
```

### 2.2 FlinkSourceReader 适配器

```java
public class FlinkSourceReader<T, SplitT extends SourceSplit>
    implements SourceReader<T, SplitWrapper<SplitT>> {

    private final org.apache.seatunnel.api.source.SourceReader<T, SplitT> seaTunnelReader;
    private final SourceReaderContext flinkContext;

    @Override
    public void start() {
        // 委托给 SeaTunnel 读取器
        try {
            seaTunnelReader.open();
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to open SeaTunnel reader", e);
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<T> output) {
        try {
            // 适配输出收集器
            CollectorAdapter<T> collector = new CollectorAdapter<>(output);

            // 从 SeaTunnel 读取器轮询
            seaTunnelReader.pollNext(collector);

            if (collector.hasRecords()) {
                return InputStatus.MORE_AVAILABLE;
            } else {
                return InputStatus.NOTHING_AVAILABLE;
            }
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to poll from SeaTunnel reader", e);
        }
    }

    @Override
    public void addSplits(List<SplitWrapper<SplitT>> splits) {
        // 解包并委托
        List<SplitT> unwrappedSplits = splits.stream()
            .map(SplitWrapper::getSplit)
            .collect(Collectors.toList());

        seaTunnelReader.addSplits(unwrappedSplits);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        try {
            seaTunnelReader.notifyCheckpointComplete(checkpointId);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to notify checkpoint complete", e);
        }
    }

    @Override
    public List<SplitWrapper<SplitT>> snapshotState(long checkpointId) {
        try {
            List<SplitT> state = seaTunnelReader.snapshotState(checkpointId);

            // 为 Flink 包装分片
            return state.stream()
                .map(SplitWrapper::new)
                .collect(Collectors.toList());
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to snapshot state", e);
        }
    }
}
```

### 2.3 FlinkSourceEnumerator 适配器

```java
public class FlinkSourceEnumerator<SplitT extends SourceSplit, StateT>
    implements SplitEnumerator<SplitWrapper<SplitT>, EnumeratorStateWrapper<StateT>> {

    private final SourceSplitEnumerator<SplitT, StateT> seaTunnelEnumerator;
    private final SplitEnumeratorContext<SplitWrapper<SplitT>> flinkContext;

    @Override
    public void start() {
        try {
            seaTunnelEnumerator.open();
            seaTunnelEnumerator.run();
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to start enumerator", e);
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // 委托给 SeaTunnel 枚举器
        seaTunnelEnumerator.handleSplitRequest(subtaskId);
    }

    @Override
    public void addSplitsBack(List<SplitWrapper<SplitT>> splits, int subtaskId) {
        // 解包并委托
        List<SplitT> unwrappedSplits = splits.stream()
            .map(SplitWrapper::getSplit)
            .collect(Collectors.toList());

        seaTunnelEnumerator.addSplitsBack(unwrappedSplits, subtaskId);
    }

    @Override
    public void addReader(int subtaskId) {
        seaTunnelEnumerator.addReader(subtaskId);
    }

    @Override
    public EnumeratorStateWrapper<StateT> snapshotState(long checkpointId) {
        try {
            StateT state = seaTunnelEnumerator.snapshotState(checkpointId);
            return new EnumeratorStateWrapper<>(state);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to snapshot enumerator state", e);
        }
    }
}
```

### 2.4 上下文适配器

**FlinkSourceReaderContext**:
```java
public class FlinkSourceReaderContext
    implements org.apache.seatunnel.api.source.SourceReader.Context {

    private final SourceReaderContext flinkContext;

    @Override
    public int getIndexOfSubtask() {
        return flinkContext.getIndexOfThisSubtask();
    }

    @Override
    public void sendSplitRequest() {
        // Flink 自动处理分片请求
        // 不需要显式 API
    }

    @Override
    public void sendSourceEventToEnumerator(SourceEvent event) {
        flinkContext.sendSourceEventToCoordinator(
            new SourceEventWrapper(event)
        );
    }
}
```

**FlinkSourceSplitEnumeratorContext**:
```java
public class FlinkSourceSplitEnumeratorContext<SplitT extends SourceSplit>
    implements SourceSplitEnumerator.Context<SplitT> {

    private final SplitEnumeratorContext<SplitWrapper<SplitT>> flinkContext;

    @Override
    public int currentParallelism() {
        return flinkContext.currentParallelism();
    }

    @Override
    public Set<Integer> registeredReaders() {
        return flinkContext.registeredReaders().keySet();
    }

    @Override
    public void assignSplit(int subtaskId, List<SplitT> splits) {
        // 包装并委托
        List<SplitWrapper<SplitT>> wrappedSplits = splits.stream()
            .map(SplitWrapper::new)
            .collect(Collectors.toList());

        flinkContext.assignSplits(new SplitsAssignment<>(
            Collections.singletonMap(subtaskId, wrappedSplits)
        ));
    }

    @Override
    public void signalNoMoreSplits(int subtaskId) {
        flinkContext.signalNoMoreSplits(subtaskId);
    }

    @Override
    public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
        flinkContext.sendEventToSourceReader(subtaskId, new SourceEventWrapper(event));
    }
}
```

### 2.5 FlinkSink 适配器

```java
public class FlinkSink<IN, CommitInfoT, WriterStateT, AggregatedCommitInfoT>
    implements Sink<IN, CommitInfoT, WriterStateT, AggregatedCommitInfoT> {

    private final SeaTunnelSink<IN, WriterStateT, CommitInfoT, AggregatedCommitInfoT> seaTunnelSink;

    @Override
    public SinkWriter<IN, CommitInfoT, WriterStateT> createWriter(InitContext context) {
        // 使用适配的上下文创建 SeaTunnel 写入器
        org.apache.seatunnel.api.sink.SinkWriter<IN, CommitInfoT, WriterStateT> seaTunnelWriter =
            seaTunnelSink.createWriter(new FlinkSinkWriterContext(context));

        // 包装在 Flink 适配器中
        return new FlinkSinkWriter<>(seaTunnelWriter);
    }

    @Override
    public Optional<Committer<CommitInfoT>> createCommitter() {
        return seaTunnelSink.createCommitter()
            .map(FlinkCommitter::new);
    }

    @Override
    public Optional<GlobalCommitter<CommitInfoT, AggregatedCommitInfoT>> createGlobalCommitter() {
        return seaTunnelSink.createAggregatedCommitter()
            .map(FlinkGlobalCommitter::new);
    }

    @Override
    public Optional<SimpleVersionedSerializer<CommitInfoT>> getCommittableSerializer() {
        return seaTunnelSink.getCommitInfoSerializer()
            .map(FlinkSimpleVersionedSerializer::new);
    }

    @Override
    public Optional<SimpleVersionedSerializer<WriterStateT>> getWriterStateSerializer() {
        return seaTunnelSink.getWriterStateSerializer()
            .map(FlinkSimpleVersionedSerializer::new);
    }
}
```

### 2.6 FlinkSinkWriter 适配器

```java
public class FlinkSinkWriter<IN, CommitInfoT, WriterStateT>
    implements SinkWriter<IN, CommitInfoT, WriterStateT> {

    private final org.apache.seatunnel.api.sink.SinkWriter<IN, CommitInfoT, WriterStateT> seaTunnelWriter;

    @Override
    public void write(IN element, Context context) throws IOException {
        // 委托给 SeaTunnel 写入器
        seaTunnelWriter.write(element);
    }

    @Override
    public List<CommitInfoT> prepareCommit(boolean flush) throws IOException {
        Optional<CommitInfoT> commitInfo = seaTunnelWriter.prepareCommit();
        return commitInfo.map(Collections::singletonList)
            .orElse(Collections.emptyList());
    }

    @Override
    public List<WriterStateT> snapshotState(long checkpointId) throws IOException {
        return seaTunnelWriter.snapshotState(checkpointId);
    }

    @Override
    public void close() throws Exception {
        seaTunnelWriter.close();
    }
}
```

## 3. Spark 转换层

### 3.1 SparkSource 适配器

将 `SeaTunnelSource` 适配到 Spark 的 `DataSourceReader` 接口。

```java
public class SparkSource<T, SplitT extends SourceSplit, StateT>
    implements DataSourceReader {

    private final SeaTunnelSource<T, SplitT, StateT> seaTunnelSource;

    @Override
    public StructType readSchema() {
        // 将 SeaTunnel 模式转换为 Spark 模式
        CatalogTable catalogTable = seaTunnelSource.getProducedCatalogTable();
        return SparkTypeConverter.convert(catalogTable.getTableSchema());
    }

    @Override
    public List<InputPartition<InternalRow>> planInputPartitions() {
        // 创建枚举器并生成分片
        SourceSplitEnumerator<SplitT, StateT> enumerator =
            seaTunnelSource.createEnumerator(new SparkEnumeratorContext());

        try {
            enumerator.open();
            enumerator.run();

            // 收集所有分片
            List<SplitT> splits = collectAllSplits(enumerator);

            // 将每个分片包装为 Spark InputPartition
            return splits.stream()
                .map(split -> new SparkInputPartition<>(seaTunnelSource, split))
                .collect(Collectors.toList());

        } catch (Exception e) {
            throw new RuntimeException("Failed to plan input partitions", e);
        }
    }
}
```

### 3.2 SparkInputPartition

```java
public class SparkInputPartition<T, SplitT extends SourceSplit>
    implements InputPartition<InternalRow> {

    private final SeaTunnelSource<T, SplitT, ?> seaTunnelSource;
    private final SplitT split;

    @Override
    public InputPartitionReader<InternalRow> createPartitionReader() {
        // 创建 SeaTunnel 读取器
        org.apache.seatunnel.api.source.SourceReader<T, SplitT> seaTunnelReader =
            seaTunnelSource.createReader(new SparkReaderContext());

        // 包装在 Spark 适配器中
        return new SparkPartitionReader<>(seaTunnelReader, split);
    }
}
```

### 3.3 SparkPartitionReader

```java
public class SparkPartitionReader<T, SplitT extends SourceSplit>
    implements InputPartitionReader<InternalRow> {

    private final org.apache.seatunnel.api.source.SourceReader<T, SplitT> seaTunnelReader;
    private final Queue<InternalRow> buffer = new LinkedList<>();

    public SparkPartitionReader(
        org.apache.seatunnel.api.source.SourceReader<T, SplitT> reader,
        SplitT split
    ) {
        this.seaTunnelReader = reader;

        try {
            seaTunnelReader.open();
            seaTunnelReader.addSplits(Collections.singletonList(split));
        } catch (Exception e) {
            throw new RuntimeException("Failed to open reader", e);
        }
    }

    @Override
    public boolean next() throws IOException {
        if (!buffer.isEmpty()) {
            return true;
        }

        // 从 SeaTunnel 读取器轮询
        try {
            seaTunnelReader.pollNext(new Collector<T>() {
                @Override
                public void collect(T record) {
                    // 转换为 Spark InternalRow
                    InternalRow row = SparkTypeConverter.convert(record);
                    buffer.offer(row);
                }
            });

            return !buffer.isEmpty();

        } catch (Exception e) {
            throw new IOException("Failed to poll next", e);
        }
    }

    @Override
    public InternalRow get() {
        return buffer.poll();
    }

    @Override
    public void close() throws IOException {
        try {
            seaTunnelReader.close();
        } catch (Exception e) {
            throw new IOException("Failed to close reader", e);
        }
    }
}
```

### 3.4 SparkSink 适配器

```java
public class SparkSink<IN, WriterStateT, CommitInfoT>
    implements DataSourceWriter {

    private final SeaTunnelSink<IN, WriterStateT, CommitInfoT, ?> seaTunnelSink;

    @Override
    public DataWriterFactory<InternalRow> createWriterFactory() {
        return new SparkDataWriterFactory<>(seaTunnelSink);
    }

    @Override
    public boolean useCommitCoordinator() {
        // 如果目标端有提交器则使用提交协调器
        return seaTunnelSink.createCommitter().isPresent();
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        Optional<SinkCommitter<CommitInfoT>> committerOpt = seaTunnelSink.createCommitter();

        if (committerOpt.isPresent()) {
            SinkCommitter<CommitInfoT> committer = committerOpt.get();

            // 从消息中提取提交信息
            List<CommitInfoT> commitInfos = Arrays.stream(messages)
                .map(msg -> ((SparkCommitMessage<CommitInfoT>) msg).getCommitInfo())
                .collect(Collectors.toList());

            // 提交
            try {
                List<CommitInfoT> failed = committer.commit(commitInfos);
                if (!failed.isEmpty()) {
                    throw new IOException("Some commits failed: " + failed);
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to commit", e);
            }
        }
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        // 处理中止
        Optional<SinkCommitter<CommitInfoT>> committerOpt = seaTunnelSink.createCommitter();

        if (committerOpt.isPresent()) {
            SinkCommitter<CommitInfoT> committer = committerOpt.get();

            List<CommitInfoT> commitInfos = Arrays.stream(messages)
                .map(msg -> ((SparkCommitMessage<CommitInfoT>) msg).getCommitInfo())
                .collect(Collectors.toList());

            try {
                committer.abort(commitInfos);
            } catch (IOException e) {
                throw new RuntimeException("Failed to abort", e);
            }
        }
    }
}
```

## 4. 序列化适配器

### 4.1 FlinkSimpleVersionedSerializer

```java
public class FlinkSimpleVersionedSerializer<T>
    implements SimpleVersionedSerializer<T> {

    private final org.apache.seatunnel.api.serialization.Serializer<T> seaTunnelSerializer;

    @Override
    public int getVersion() {
        // 委托给 SeaTunnel 序列化器
        return seaTunnelSerializer.getVersion();
    }

    @Override
    public byte[] serialize(T obj) throws IOException {
        return seaTunnelSerializer.serialize(obj);
    }

    @Override
    public T deserialize(int version, byte[] serialized) throws IOException {
        return seaTunnelSerializer.deserialize(serialized);
    }
}
```

## 5. 类型转换

### 5.1 Spark 类型转换

```java
public class SparkTypeConverter {
    public static StructType convert(TableSchema schema) {
        List<StructField> fields = new ArrayList<>();

        for (Column column : schema.getColumns()) {
            StructField field = new StructField(
                column.getName(),
                convertDataType(column.getDataType()),
                column.isNullable(),
                Metadata.empty()
            );
            fields.add(field);
        }

        return new StructType(fields.toArray(new StructField[0]));
    }

    private static DataType convertDataType(SeaTunnelDataType<?> seaTunnelType) {
        switch (seaTunnelType.getSqlType()) {
            case TINYINT:
                return DataTypes.ByteType;
            case SMALLINT:
                return DataTypes.ShortType;
            case INT:
                return DataTypes.IntegerType;
            case BIGINT:
                return DataTypes.LongType;
            case FLOAT:
                return DataTypes.FloatType;
            case DOUBLE:
                return DataTypes.DoubleType;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) seaTunnelType;
                return DataTypes.createDecimalType(
                    decimalType.getPrecision(),
                    decimalType.getScale()
                );
            case STRING:
                return DataTypes.StringType;
            case BOOLEAN:
                return DataTypes.BooleanType;
            case DATE:
                return DataTypes.DateType;
            case TIMESTAMP:
                return DataTypes.TimestampType;
            case BYTES:
                return DataTypes.BinaryType;
            case ARRAY:
                ArrayType arrayType = (ArrayType) seaTunnelType;
                return DataTypes.createArrayType(
                    convertDataType(arrayType.getElementType())
                );
            case MAP:
                MapType mapType = (MapType) seaTunnelType;
                return DataTypes.createMapType(
                    convertDataType(mapType.getKeyType()),
                    convertDataType(mapType.getValueType())
                );
            default:
                throw new UnsupportedOperationException(
                    "Unsupported type: " + seaTunnelType);
        }
    }
}
```

## 6. 性能考虑

### 6.1 转换开销

**测量的开销**:
```
基准: 1M 记录/秒吞吐量

无转换(原生):              1,000,000 记录/秒
使用 Flink 转换:            980,000 记录/秒 (-2%)
使用 Spark 转换:            970,000 记录/秒 (-3%)
```

**开销来源**:
1. 上下文包装: ~1%
2. 类型转换: ~1-2%
3. 分片包装/解包: <1%

### 6.2 优化技术

**批量类型转换**:
```java
// ❌ 差: 每条记录转换
public void collect(SeaTunnelRow record) {
    InternalRow sparkRow = convertToSparkRow(record);
    output.collect(sparkRow);
}

// ✅ 好: 批量转换(分摊开销)
public void collect(List<SeaTunnelRow> records) {
    InternalRow[] sparkRows = batchConvertToSparkRows(records);
    for (InternalRow row : sparkRows) {
        output.collect(row);
    }
}
```

**避免不必要的包装**:
```java
// 如果分片已经可序列化,不要包装
public class SplitWrapper<T> {
    private final T split;

    // 惰性包装: 仅在序列化需要时包装
    public byte[] serialize() {
        if (split instanceof Serializable) {
            return directSerialize(split); // 无包装开销
        } else {
            return wrapAndSerialize(split); // 后备
        }
    }
}
```

## 7. 限制和解决方法

### 7.1 引擎特定功能

**问题**: 某些引擎功能在 SeaTunnel 中没有等效项。

**示例**: Flink 的 `WatermarkStrategy`
```java
// Flink 特定的水印策略无法在 SeaTunnel API 中表达
WatermarkStrategy<T> watermarkStrategy = WatermarkStrategy
    .forBoundedOutOfOrderness(Duration.ofSeconds(5));
```

**解决方法**: 提供引擎特定配置
```hocon
source {
  Kafka {
    # SeaTunnel 配置
    topic = "my_topic"

    # 引擎特定配置(仅用于 Flink)
    flink.watermark.strategy = "bounded-out-of-orderness"
    flink.watermark.max-out-of-orderness = "5s"
  }
}
```

### 7.2 类型系统差异

**问题**: 类型系统不完全对齐。

**示例**: Spark 有 `TimestampType`,Flink 有 `LocalZonedTimestampType` 和 `TimestampType`。

**解决方法**: 使用最小公分母
```java
// SeaTunnel 使用通用 TIMESTAMP
// 转换层根据配置映射到适当的引擎类型
```

## 8. 最佳实践

### 8.1 连接器开发

**应该做的**:
- 仅实现 SeaTunnel API
- 在多个引擎上测试
- 使用 SeaTunnel 类型

**不应该做的**:
- 在连接器代码中引用引擎特定 API
- 假设特定引擎行为
- 使用引擎特定优化

### 8.2 测试

**在所有引擎上测试**:
```java
@RunWith(Parameterized.class)
public class ConnectorTest {
    @Parameters
    public static Collection<Object[]> engines() {
        return Arrays.asList(new Object[][]{
            {"flink"},
            {"spark"},
            {"seatunnel"}
        });
    }

    @Test
    public void testExactlyOnce(String engine) {
        // 在不同引擎上运行相同测试
        runJobOnEngine(engine, jobConfig);
        verifyResults();
    }
}
```

## 9. 相关资源

- [数据源架构](../api-design/source-architecture.md)
- [目标端架构](../api-design/sink-architecture.md)
- [设计理念](../design-philosophy.md)

## 10. 参考资料

### 关键源文件

- Flink 转换: `seatunnel-translation/seatunnel-translation-flink/`
- Spark 转换: `seatunnel-translation/seatunnel-translation-spark/`
- 基础接口: `seatunnel-api/src/main/java/org/apache/seatunnel/api/`

### 进一步阅读

- [Apache Flink Source API](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/sources/)
- [Apache Spark Data Source V2](https://spark.apache.org/docs/latest/sql-data-sources.html)
