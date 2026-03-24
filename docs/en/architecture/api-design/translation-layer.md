---
sidebar_position: 1
title: Translation Layer
---

# Translation Layer Architecture

## 1. Overview

### 1.1 Problem Background

SeaTunnel provides a unified connector API, but jobs need to run on different execution engines:

- **Engine Diversity**: Flink, Spark, SeaTunnel Engine (Zeta) have different APIs
- **Code Duplication**: Without translation, each connector needs 3 implementations
- **Maintenance Burden**: Bug fixes require changes in all implementations
- **API Evolution**: Engine API changes break connectors
- **User Experience**: Users want consistent behavior across engines

### 1.2 Design Goals

SeaTunnel's translation layer aims to:

1. **Enable Portability**: Same connector runs on any engine
2. **Hide Complexity**: Connector developers only learn SeaTunnel API
3. **Maintain Fidelity**: Preserve semantic guarantees across engines
4. **Minimize Overhead**: Keep translation overhead low (depends on connectors and type conversions)
5. **Support Evolution**: Isolate connectors from engine API changes

### 1.3 Architecture Overview

```
┌──────────────────────────────────────────────────────────────┐
│                   SeaTunnel API Layer                         │
│         (Engine-Independent Connector Interface)              │
│                                                                │
│  SeaTunnelSource    SeaTunnelSink    SeaTunnelTransform      │
└──────────────────────────────────────────────────────────────┘
                              │
                              │ Translation Layer
                ┌─────────────┼─────────────┐
                ▼             ▼             ▼
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│  Flink Adapter   │  │  Spark Adapter   │  │ Zeta (Native)    │
│                  │  │                  │  │                  │
│ FlinkSource      │  │ SparkSource      │  │ Direct           │
│ FlinkSink        │  │ SparkSink        │  │ Execution        │
└──────────────────┘  └──────────────────┘  └──────────────────┘
        │                     │                     │
        ▼                     ▼                     ▼
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│  Apache Flink    │  │  Apache Spark    │  │ SeaTunnel Engine │
│     Runtime      │  │     Runtime      │  │      (Zeta)      │
└──────────────────┘  └──────────────────┘  └──────────────────┘
```

## 2. Flink Translation Layer

### 2.1 FlinkSource Adapter

Adapts `SeaTunnelSource` to Flink's `Source` interface.

```java
public class FlinkSource<T, SplitT extends SourceSplit, StateT>
    implements Source<T, SplitWrapper<SplitT>, EnumeratorStateWrapper<StateT>> {

    // Wrapped SeaTunnel source
    private final SeaTunnelSource<T, SplitT, StateT> seaTunnelSource;

    @Override
    public Boundedness getBoundedness() {
        // Delegate to SeaTunnel source
        return seaTunnelSource.getBoundedness() == Boundedness.BOUNDED
            ? Boundedness.BOUNDED
            : Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<T, SplitWrapper<SplitT>> createReader(
        SourceReaderContext readerContext
    ) {
        // Create SeaTunnel reader with adapted context
        org.apache.seatunnel.api.source.SourceReader<T, SplitT> seaTunnelReader =
            seaTunnelSource.createReader(new FlinkSourceReaderContext(readerContext));

        // Wrap in Flink adapter
        return new FlinkSourceReader<>(seaTunnelReader, readerContext);
    }

    @Override
    public SplitEnumerator<SplitWrapper<SplitT>, EnumeratorStateWrapper<StateT>>
        createEnumerator(SplitEnumeratorContext<SplitWrapper<SplitT>> context) {

        // Create SeaTunnel enumerator with adapted context
        SourceSplitEnumerator<SplitT, StateT> seaTunnelEnumerator =
            seaTunnelSource.createEnumerator(
                new FlinkSourceSplitEnumeratorContext<>(context)
            );

        // Wrap in Flink adapter
        return new FlinkSourceEnumerator<>(seaTunnelEnumerator, context);
    }

    @Override
    public SimpleVersionedSerializer<SplitWrapper<SplitT>> getSplitSerializer() {
        // Adapt SeaTunnel serializer to Flink serializer
        return new FlinkSimpleVersionedSerializer<>(
            seaTunnelSource.getSplitSerializer()
        );
    }
}
```

### 2.2 FlinkSourceReader Adapter

```java
public class FlinkSourceReader<T, SplitT extends SourceSplit>
    implements SourceReader<T, SplitWrapper<SplitT>> {

    private final org.apache.seatunnel.api.source.SourceReader<T, SplitT> seaTunnelReader;
    private final SourceReaderContext flinkContext;

    @Override
    public void start() {
        // Delegate to SeaTunnel reader
        try {
            seaTunnelReader.open();
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to open SeaTunnel reader", e);
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<T> output) {
        try {
            // Adapt output collector
            CollectorAdapter<T> collector = new CollectorAdapter<>(output);

            // Poll from SeaTunnel reader
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
        // Unwrap and delegate
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

            // Wrap splits for Flink
            return state.stream()
                .map(SplitWrapper::new)
                .collect(Collectors.toList());
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to snapshot state", e);
        }
    }
}
```

### 2.3 FlinkSourceEnumerator Adapter

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
        // Delegate to SeaTunnel enumerator
        seaTunnelEnumerator.handleSplitRequest(subtaskId);
    }

    @Override
    public void addSplitsBack(List<SplitWrapper<SplitT>> splits, int subtaskId) {
        // Unwrap and delegate
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

### 2.4 Context Adapters

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
        // Flink automatically handles split requests
        // No explicit API needed
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
        // Wrap and delegate
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

### 2.5 FlinkSink Adapter

```java
public class FlinkSink<IN, CommitInfoT, WriterStateT, AggregatedCommitInfoT>
    implements Sink<IN, CommitInfoT, WriterStateT, AggregatedCommitInfoT> {

    private final SeaTunnelSink<IN, WriterStateT, CommitInfoT, AggregatedCommitInfoT> seaTunnelSink;

    @Override
    public SinkWriter<IN, CommitInfoT, WriterStateT> createWriter(InitContext context) {
        // Create SeaTunnel writer with adapted context
        org.apache.seatunnel.api.sink.SinkWriter<IN, CommitInfoT, WriterStateT> seaTunnelWriter =
            seaTunnelSink.createWriter(new FlinkSinkWriterContext(context));

        // Wrap in Flink adapter
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

### 2.6 FlinkSinkWriter Adapter

```java
public class FlinkSinkWriter<IN, CommitInfoT, WriterStateT>
    implements SinkWriter<IN, CommitInfoT, WriterStateT> {

    private final org.apache.seatunnel.api.sink.SinkWriter<IN, CommitInfoT, WriterStateT> seaTunnelWriter;
    private long checkpointId;

    @Override
    public void write(IN element, Context context) throws IOException {
        // Delegate to SeaTunnel writer
        seaTunnelWriter.write(element);
    }

    @Override
    public List<CommitInfoT> prepareCommit(boolean flush) throws IOException {
        Optional<CommitInfoT> commitInfo = seaTunnelWriter.prepareCommit(checkpointId);
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

## 3. Spark Translation Layer

Note: Spark 2.4 and Spark 3.x use different datasource APIs. SeaTunnel maintains separate Spark translation modules/adapters per Spark major version, so the exact adapter types and lifecycle hooks may differ.

### 3.1 SparkSource Adapter

Adapts `SeaTunnelSource` to Spark's `DataSourceReader` interface.

```java
public class SparkSource<T, SplitT extends SourceSplit, StateT>
    implements DataSourceReader {

    private final SeaTunnelSource<T, SplitT, StateT> seaTunnelSource;

    @Override
    public StructType readSchema() {
        // Convert SeaTunnel schema to Spark schema
        CatalogTable catalogTable = seaTunnelSource.getProducedCatalogTables().get(0);
        return SparkTypeConverter.convert(catalogTable.getTableSchema());
    }

    @Override
    public List<InputPartition<InternalRow>> planInputPartitions() {
        // Create enumerator and generate splits
        SourceSplitEnumerator<SplitT, StateT> enumerator =
            seaTunnelSource.createEnumerator(new SparkEnumeratorContext());

        try {
            enumerator.open();
            enumerator.run();

            // Collect all splits
            List<SplitT> splits = collectAllSplits(enumerator);

            // Wrap each split as Spark InputPartition
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
        // Create SeaTunnel reader
        org.apache.seatunnel.api.source.SourceReader<T, SplitT> seaTunnelReader =
            seaTunnelSource.createReader(new SparkReaderContext());

        // Wrap in Spark adapter
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

        // Poll from SeaTunnel reader
        try {
            seaTunnelReader.pollNext(new Collector<T>() {
                @Override
                public void collect(T record) {
                    // Convert to Spark InternalRow
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

### 3.4 SparkSink Adapter

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
        // Use commit coordinator if sink has committer
        return seaTunnelSink.createCommitter().isPresent();
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        Optional<SinkCommitter<CommitInfoT>> committerOpt = seaTunnelSink.createCommitter();

        if (committerOpt.isPresent()) {
            SinkCommitter<CommitInfoT> committer = committerOpt.get();

            // Extract commit infos from messages
            List<CommitInfoT> commitInfos = Arrays.stream(messages)
                .map(msg -> ((SparkCommitMessage<CommitInfoT>) msg).getCommitInfo())
                .collect(Collectors.toList());

            // Commit
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
        // Handle abort
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

## 4. Serialization Adapters

### 4.1 FlinkSimpleVersionedSerializer

```java
public class FlinkSimpleVersionedSerializer<T>
    implements SimpleVersionedSerializer<T> {

    private final org.apache.seatunnel.api.serialization.Serializer<T> seaTunnelSerializer;

    @Override
    public int getVersion() {
        // Delegate to SeaTunnel serializer
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

## 5. Type Conversion

### 5.1 Spark Type Conversion

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

## 6. Performance Considerations

### 6.1 Translation Overhead

Translation overhead depends on connector implementations, serialization, and type conversion complexity. Prefer measuring in your own workload rather than relying on fixed numbers.

### 6.2 Optimization Techniques

**Batch Type Conversion**:
```java
// ❌ BAD: Convert per record
public void collect(SeaTunnelRow record) {
    InternalRow sparkRow = convertToSparkRow(record);
    output.collect(sparkRow);
}

// ✅ GOOD: Batch convert (amortize overhead)
public void collect(List<SeaTunnelRow> records) {
    InternalRow[] sparkRows = batchConvertToSparkRows(records);
    for (InternalRow row : sparkRows) {
        output.collect(row);
    }
}
```

**Avoid Unnecessary Wrapping**:
```java
// If Split already serializable, don't wrap
public class SplitWrapper<T> {
    private final T split;

    // Lazy wrapping: only wrap when needed for serialization
    public byte[] serialize() {
        if (split instanceof Serializable) {
            return directSerialize(split); // No wrapping overhead
        } else {
            return wrapAndSerialize(split); // Fallback
        }
    }
}
```

## 7. Limitations and Workarounds

### 7.1 Engine-Specific Features

**Problem**: Some engine features have no SeaTunnel equivalent.

**Example**: Flink's `WatermarkStrategy`
```java
// Flink-specific watermark strategy cannot be expressed in SeaTunnel API
WatermarkStrategy<T> watermarkStrategy = WatermarkStrategy
    .forBoundedOutOfOrderness(Duration.ofSeconds(5));
```

**Workaround**: Provide engine-specific configuration
```hocon
source {
  Kafka {
    # SeaTunnel config
    topic = "my_topic"

    # Engine-specific config (for Flink only)
    flink.watermark.strategy = "bounded-out-of-orderness"
    flink.watermark.max-out-of-orderness = "5s"
  }
}
```

### 7.2 Type System Differences

**Problem**: Type systems don't fully align.

**Example**: Spark has `TimestampType`, Flink has `LocalZonedTimestampType` and `TimestampType`.

**Workaround**: Use least common denominator
```java
// SeaTunnel uses generic TIMESTAMP
// Translation layer maps to appropriate engine type based on config
```

## 8. Best Practices

### 8.1 Connector Development

**DO**:
- Implement SeaTunnel API only
- Test with multiple engines
- Use SeaTunnel types

**DON'T**:
- Reference engine-specific APIs in connector code
- Assume specific engine behavior
- Use engine-specific optimizations

### 8.2 Testing

**Test on All Engines**:
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
        // Run same test on different engines
        runJobOnEngine(engine, jobConfig);
        verifyResults();
    }
}
```

## 9. Related Resources

- [Source Architecture](../api-design/source-architecture.md)
- [Sink Architecture](../api-design/sink-architecture.md)
- [Design Philosophy](../design-philosophy.md)

## 10. References

### Key Source Files

- Flink Translation: `seatunnel-translation/seatunnel-translation-flink/`
- Spark Translation: `seatunnel-translation/seatunnel-translation-spark/`
- Base Interfaces: `seatunnel-api/src/main/java/org/apache/seatunnel/api/`

### Further Reading

- [Apache Flink Source API](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/sources/)
- [Apache Spark Data Source V2](https://spark.apache.org/docs/latest/sql-data-sources.html)
