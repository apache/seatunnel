/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.cdc.base.source.enumerator;

import org.apache.seatunnel.api.common.metrics.AbstractMetricsContext;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StopConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.source.IncrementalSource;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.ChunkSplitter;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.PendingSplitsState;
import org.apache.seatunnel.connectors.cdc.base.source.event.CompletedSnapshotSplitsAckEvent;
import org.apache.seatunnel.connectors.cdc.base.source.event.CompletedSnapshotSplitsReportEvent;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.IncrementalSourceStreamFetcher;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkKind;
import org.apache.seatunnel.connectors.cdc.base.utils.SourceRecordUtils;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.data.Envelope;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.util.LoggingContext;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Shared in-memory {@link IncrementalSource} fixture for reproducing and verifying checkpoint and
 * savepoint restore across the snapshot-to-incremental transition without external containers or
 * private state mutation.
 */
public final class InMemoryCdcRestoreTestFixture {

    public static final TableId TABLE_ID = TableId.parse("db1.table1");
    public static final String SPLIT_0_ID = "db1.table1:0";
    public static final String SPLIT_1_ID = "db1.table1:1";
    public static final long SPLIT_0_LOW_WATERMARK = 100L;
    public static final long SPLIT_0_HIGH_WATERMARK = 120L;
    public static final long SPLIT_1_LOW_WATERMARK = 95L;
    public static final long SPLIT_1_HIGH_WATERMARK = 108L;
    public static final long STARTUP_FALLBACK_OFFSET = 500L;

    public static final SeaTunnelRowType ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"id"}, new SeaTunnelDataType<?>[] {BasicType.LONG_TYPE});

    private static final Option<StartupMode> STARTUP_MODE_OPT =
            Options.key(SourceOptions.STARTUP_MODE_KEY)
                    .enumType(StartupMode.class)
                    .defaultValue(StartupMode.INITIAL)
                    .withDescription("Startup mode");

    private static final Option<StopMode> STOP_MODE_OPT =
            Options.key(SourceOptions.STOP_MODE_KEY)
                    .enumType(StopMode.class)
                    .defaultValue(StopMode.LATEST)
                    .withDescription("Stop mode");

    // The capture helper owns one entry per UUID-tagged run. Source wrappers look it up by tag,
    // and the capture helper removes it in finally.
    private static final Map<String, RunCoordinationState> RUN_STATES = new ConcurrentHashMap<>();

    private InMemoryCdcRestoreTestFixture() {}

    public static CatalogTable createCatalogTable() {
        return CatalogTable.of(
                TableIdentifier.of("test", TablePath.of("db1", "table1")),
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.LONG_TYPE, 20L, false, null, ""))
                        .build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                "");
    }

    private static RunCoordinationState registerRun(String runTag) {
        RunCoordinationState state = new RunCoordinationState();
        RUN_STATES.put(runTag, state);
        return state;
    }

    private static void unregisterRun(String runTag) {
        RUN_STATES.remove(runTag);
    }

    public static IncrementalSource<SeaTunnelRow, SourceConfig> createSource(
            String runTag,
            CatalogTable catalogTable,
            boolean exactlyOnce,
            StopMode stopMode,
            boolean holdTailSplitReport) {
        return new FixtureIncrementalSource(
                runTag, catalogTable, exactlyOnce, stopMode, holdTailSplitReport);
    }

    public static Offset offset(long pos) {
        return new FixtureOffset(pos);
    }

    public static long offsetPos(Offset offset) {
        return Long.parseLong(offset.getOffset().get("pos"));
    }

    public static void submitToStreamFetcherAndClose(SourceSplitBase split, boolean exactlyOnce) {
        IncrementalSourceStreamFetcher fetcher =
                new IncrementalSourceStreamFetcher(
                        new FixtureFetchTaskContext(exactlyOnce), 0, null);
        try {
            fetcher.submitTask(
                    new FetchTask<SourceSplitBase>() {
                        @Override
                        public void execute(Context context) {}

                        @Override
                        public boolean isRunning() {
                            return false;
                        }

                        @Override
                        public void shutdown() {}

                        @Override
                        public SourceSplitBase getSplit() {
                            return split;
                        }
                    });
        } finally {
            fetcher.close();
        }
    }

    /**
     * Captures the checkpoint after split 0 has been acknowledged and the reader has finished the
     * tail split. The reader wrapper withholds the tail completion report. After draining split 0's
     * acknowledgement, the reader and enumerator snapshot the same checkpoint ID. Reader state
     * retains the finished tail and its watermarks, while enumerator state still lists that split
     * as assigned without its completion watermark.
     */
    public static SerializedCheckpointSnapshot captureUnackedTailSplitCheckpoint(
            boolean exactlyOnce, int parallelism) throws Exception {
        String runTag = "serial-" + UUID.randomUUID();
        RunCoordinationState runState = registerRun(runTag);
        CatalogTable table = createCatalogTable();
        IncrementalSource<SeaTunnelRow, SourceConfig> phase1Source =
                createSource(runTag, table, exactlyOnce, StopMode.NEVER, true);

        Serializer<PendingSplitsState> enumSerializer = phase1Source.getEnumeratorStateSerializer();
        Serializer<SourceSplitBase> splitSerializer = phase1Source.getSplitSerializer();

        HarnessEnumeratorContext enumCtx = new HarnessEnumeratorContext(parallelism);
        SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> enumerator =
                phase1Source.createEnumerator(enumCtx);
        List<HarnessReaderContext> readerCtxs = new ArrayList<>();
        List<SourceReader<SeaTunnelRow, SourceSplitBase>> readers = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            HarnessReaderContext rCtx = new HarnessReaderContext(i, enumCtx);
            readerCtxs.add(rCtx);
            readers.add(phase1Source.createReader(rCtx));
        }

        try {
            enumerator.open();
            for (int i = 0; i < parallelism; i++) {
                enumCtx.register(i);
                enumerator.registerReader(i);
                readers.get(i).open();
            }
            enumerator.run();

            List<Long> phase1Rows = new ArrayList<>();
            Collector<SeaTunnelRow> collector = createRowCollector(phase1Rows);

            long deadlineNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (System.nanoTime() < deadlineNs) {
                for (int i = 0; i < parallelism; i++) {
                    drainAssignmentsAndEvents(i, enumCtx, readers.get(i), enumerator);
                    readers.get(i).pollNext(collector);
                    drainAssignmentsAndEvents(i, enumCtx, readers.get(i), enumerator);
                }
                if (runState.split0AckedByEnumerator.getCount() == 0
                        && runState.tailSplitWatermarksRecorded.getCount() == 0
                        && phase1Rows.size() == 2) {
                    break;
                }
                Thread.sleep(10L);
            }

            if (runState.split0AckedByEnumerator.getCount() != 0
                    || runState.tailSplitWatermarksRecorded.getCount() != 0) {
                throw new IllegalStateException(
                        "Timed out waiting for tail split unacked checkpoint window");
            }

            // Drain split0's ACK into its reader so only split1 remains in finishedUnackedSplits
            for (int i = 0; i < parallelism; i++) {
                drainAssignmentsAndEvents(i, enumCtx, readers.get(i), enumerator);
            }

            byte[] enumBytes = enumSerializer.serialize(enumerator.snapshotState(1L));
            List<List<byte[]>> readerSplitBytes = new ArrayList<>();
            for (int i = 0; i < parallelism; i++) {
                List<SourceSplitBase> splits = readers.get(i).snapshotState(1L);
                List<byte[]> serializedSplits = new ArrayList<>();
                for (SourceSplitBase split : splits) {
                    serializedSplits.add(splitSerializer.serialize(split));
                }
                readerSplitBytes.add(serializedSplits);
            }
            return new SerializedCheckpointSnapshot(enumBytes, readerSplitBytes);
        } finally {
            for (SourceReader<SeaTunnelRow, SourceSplitBase> reader : readers) {
                try {
                    reader.close();
                } catch (Exception ignored) {
                }
            }
            try {
                enumerator.close();
            } catch (Exception ignored) {
            }
            unregisterRun(runTag);
        }
    }

    private static void drainAssignmentsAndEvents(
            int subtaskId,
            HarnessEnumeratorContext enumCtx,
            SourceReader<SeaTunnelRow, SourceSplitBase> reader,
            SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> enumerator) {
        List<SourceSplitBase> newlyAssigned = enumCtx.drainAssignedSplits(subtaskId);
        if (!newlyAssigned.isEmpty()) {
            reader.addSplits(newlyAssigned);
        }
        SourceEvent toReader;
        while ((toReader = enumCtx.pollReaderEvent(subtaskId)) != null) {
            reader.handleSourceEvent(toReader);
        }
        if (enumCtx.consumeSplitRequest(subtaskId)) {
            enumerator.handleSplitRequest(subtaskId);
            List<SourceSplitBase> afterReq = enumCtx.drainAssignedSplits(subtaskId);
            if (!afterReq.isEmpty()) {
                reader.addSplits(afterReq);
            }
        }
        SourceEvent toEnum;
        while ((toEnum = enumCtx.pollEnumeratorEvent(subtaskId)) != null) {
            enumerator.handleSourceEvent(subtaskId, toEnum);
        }
        SourceEvent ackEvent;
        while ((ackEvent = enumCtx.pollReaderEvent(subtaskId)) != null) {
            reader.handleSourceEvent(ackEvent);
        }
    }

    private static Collector<SeaTunnelRow> createRowCollector(List<Long> sink) {
        final Object lock = new Object();
        return new Collector<SeaTunnelRow>() {
            @Override
            public void collect(SeaTunnelRow record) {
                sink.add(((Number) record.getField(0)).longValue());
            }

            @Override
            public Object getCheckpointLock() {
                return lock;
            }
        };
    }

    public static final class SerializedCheckpointSnapshot {
        private final byte[] enumeratorStateBytes;
        private final List<List<byte[]>> readerSplitBytes;

        private SerializedCheckpointSnapshot(
                byte[] enumeratorStateBytes, List<List<byte[]>> readerSplitBytes) {
            this.enumeratorStateBytes = enumeratorStateBytes;
            this.readerSplitBytes = readerSplitBytes;
        }

        public byte[] getEnumeratorStateBytes() {
            return enumeratorStateBytes;
        }

        public List<List<byte[]>> getReaderSplitBytes() {
            return readerSplitBytes;
        }
    }

    private static final class RunCoordinationState {
        // Signals that the enumerator sent the split 0 acknowledgement. Delivery to its reader
        // can still be pending in the component fixture's event queue.
        private final CountDownLatch split0AckedByEnumerator = new CountDownLatch(1);
        // Signals that the reader attempted to report the finished tail. The wrapper intercepted
        // that report before the enumerator could record its watermarks.
        private final CountDownLatch tailSplitWatermarksRecorded = new CountDownLatch(1);
    }

    private static final class FixtureIncrementalSource
            extends IncrementalSource<SeaTunnelRow, SourceConfig> {
        private static final long serialVersionUID = 1L;

        private final String runTag;
        private final boolean exactlyOnce;
        private final StopMode stopMode;
        private final boolean holdTailSplitReport;

        private FixtureIncrementalSource(
                String runTag,
                CatalogTable catalogTable,
                boolean exactlyOnce,
                StopMode stopMode,
                boolean holdTailSplitReport) {
            super(
                    ReadonlyConfig.fromMap(createSourceMap(exactlyOnce, stopMode)),
                    Collections.singletonList(catalogTable));
            this.runTag = runTag;
            this.exactlyOnce = exactlyOnce;
            this.stopMode = stopMode;
            this.holdTailSplitReport = holdTailSplitReport;
            this.configFactory = createSourceConfigFactory(this.readonlyConfig);
            this.dataSourceDialect = createDataSourceDialect(this.readonlyConfig);
        }

        private static Map<String, Object> createSourceMap(boolean exactlyOnce, StopMode stopMode) {
            Map<String, Object> map = new HashMap<>();
            map.put(SourceOptions.EXACTLY_ONCE.key(), String.valueOf(exactlyOnce));
            map.put(STOP_MODE_OPT.key(), stopMode == StopMode.LATEST ? "latest" : "never");
            return map;
        }

        @Override
        public Option<StartupMode> getStartupModeOption() {
            return STARTUP_MODE_OPT;
        }

        @Override
        public Option<StopMode> getStopModeOption() {
            return STOP_MODE_OPT;
        }

        @Override
        public SourceConfig.Factory<SourceConfig> createSourceConfigFactory(ReadonlyConfig config) {
            boolean eo = this.exactlyOnce;
            StopMode sm = this.stopMode;
            return subtask -> new FixtureSourceConfig(eo, sm);
        }

        @Override
        public DebeziumDeserializationSchema<SeaTunnelRow> createDebeziumDeserializationSchema(
                ReadonlyConfig config) {
            List<CatalogTable> tables = this.catalogTables;
            return new DebeziumDeserializationSchema<SeaTunnelRow>() {
                private static final long serialVersionUID = 1L;

                @Override
                public void deserialize(SourceRecord record, Collector<SeaTunnelRow> out) {
                    Struct value = (Struct) record.value();
                    Struct after = value.getStruct(Envelope.FieldName.AFTER);
                    long id = after.getInt64("id");
                    SeaTunnelRow row = new SeaTunnelRow(new Object[] {id});
                    row.setTableId(TABLE_ID.toString());
                    out.collect(row);
                }

                @Override
                public List<CatalogTable> getProducedType() {
                    return tables;
                }

                @Override
                public Map<TableId, byte[]> getHistoryTableChanges() {
                    return Collections.emptyMap();
                }
            };
        }

        /**
         * During capture, intercepts any completion report containing the tail split and forwards
         * other events. The tail stays in the reader's finished-unacknowledged state for restore.
         */
        @Override
        public SourceReader<SeaTunnelRow, SourceSplitBase> createReader(
                SourceReader.Context readerContext) throws Exception {
            if (!holdTailSplitReport) {
                return super.createReader(readerContext);
            }
            String tag = this.runTag;
            SourceReader.Context wrapped =
                    new SourceReader.Context() {
                        @Override
                        public int getIndexOfSubtask() {
                            return readerContext.getIndexOfSubtask();
                        }

                        @Override
                        public Boundedness getBoundedness() {
                            return readerContext.getBoundedness();
                        }

                        @Override
                        public void signalNoMoreElement() {
                            readerContext.signalNoMoreElement();
                        }

                        @Override
                        public void sendSplitRequest() {
                            readerContext.sendSplitRequest();
                        }

                        @Override
                        public void sendSourceEventToEnumerator(SourceEvent sourceEvent) {
                            if (sourceEvent instanceof CompletedSnapshotSplitsReportEvent) {
                                CompletedSnapshotSplitsReportEvent report =
                                        (CompletedSnapshotSplitsReportEvent) sourceEvent;
                                boolean hasTailSplit =
                                        report.getCompletedSnapshotSplitWatermarks().stream()
                                                .anyMatch(w -> SPLIT_1_ID.equals(w.getSplitId()));
                                if (hasTailSplit) {
                                    RunCoordinationState state = RUN_STATES.get(tag);
                                    if (state != null) {
                                        state.tailSplitWatermarksRecorded.countDown();
                                    }
                                    return;
                                }
                            }
                            readerContext.sendSourceEventToEnumerator(sourceEvent);
                        }

                        @Override
                        public MetricsContext getMetricsContext() {
                            return readerContext.getMetricsContext();
                        }

                        @Override
                        public EventListener getEventListener() {
                            return readerContext.getEventListener();
                        }
                    };
            return super.createReader(wrapped);
        }

        /**
         * Forwards enumerator events and signals when an acknowledgement for split 0 is sent. The
         * capture helper then drains the pending events before serializing reader state.
         */
        @Override
        public SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> createEnumerator(
                SourceSplitEnumerator.Context<SourceSplitBase> enumeratorContext) throws Exception {
            String tag = this.runTag;
            return super.createEnumerator(
                    new SourceSplitEnumerator.Context<SourceSplitBase>() {
                        @Override
                        public int currentParallelism() {
                            return enumeratorContext.currentParallelism();
                        }

                        @Override
                        public Set<Integer> registeredReaders() {
                            return enumeratorContext.registeredReaders();
                        }

                        @Override
                        public void assignSplit(int subtaskId, List<SourceSplitBase> splits) {
                            enumeratorContext.assignSplit(subtaskId, splits);
                        }

                        @Override
                        public void signalNoMoreSplits(int subtask) {
                            enumeratorContext.signalNoMoreSplits(subtask);
                        }

                        @Override
                        public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
                            enumeratorContext.sendEventToSourceReader(subtaskId, event);
                            if (event instanceof CompletedSnapshotSplitsAckEvent) {
                                CompletedSnapshotSplitsAckEvent ack =
                                        (CompletedSnapshotSplitsAckEvent) event;
                                if (ack.getCompletedSplits().contains(SPLIT_0_ID)) {
                                    RunCoordinationState state = RUN_STATES.get(tag);
                                    if (state != null) {
                                        state.split0AckedByEnumerator.countDown();
                                    }
                                }
                            }
                        }

                        @Override
                        public MetricsContext getMetricsContext() {
                            return enumeratorContext.getMetricsContext();
                        }

                        @Override
                        public EventListener getEventListener() {
                            return enumeratorContext.getEventListener();
                        }
                    });
        }

        @Override
        public DataSourceDialect<SourceConfig> createDataSourceDialect(ReadonlyConfig config) {
            return new FixtureDialect(this.runTag);
        }

        @Override
        public OffsetFactory createOffsetFactory(ReadonlyConfig config) {
            return new FixtureOffsetFactory();
        }

        @Override
        public Optional<String> driverName() {
            return Optional.empty();
        }

        @Override
        public String getPluginName() {
            return "in-memory-cdc-restore-test";
        }
    }

    private static final class FixtureSourceConfig implements SourceConfig {
        private static final long serialVersionUID = 1L;

        private final boolean exactlyOnce;
        private final StopMode stopMode;

        private FixtureSourceConfig(boolean exactlyOnce, StopMode stopMode) {
            this.exactlyOnce = exactlyOnce;
            this.stopMode = stopMode;
        }

        @Override
        public StartupConfig getStartupConfig() {
            return new StartupConfig(StartupMode.INITIAL, null, null, null);
        }

        @Override
        public StopConfig getStopConfig() {
            return new StopConfig(stopMode, null, null, null);
        }

        @Override
        public int getSplitSize() {
            return 100;
        }

        @Override
        public boolean isExactlyOnce() {
            return exactlyOnce;
        }
    }

    private static final class FixtureDialect implements DataSourceDialect<SourceConfig> {
        private static final long serialVersionUID = 1L;

        private final String runTag;

        private FixtureDialect(String runTag) {
            this.runTag = runTag;
        }

        @Override
        public String getName() {
            return "in-memory-cdc-dialect";
        }

        @Override
        public List<TableId> discoverDataCollections(SourceConfig sourceConfig) {
            return Collections.singletonList(TABLE_ID);
        }

        @Override
        public boolean isDataCollectionIdCaseSensitive(SourceConfig sourceConfig) {
            return false;
        }

        @Override
        public ChunkSplitter createChunkSplitter(SourceConfig sourceConfig) {
            return tableId ->
                    Arrays.asList(
                            new SnapshotSplit(
                                    SPLIT_0_ID,
                                    tableId,
                                    ROW_TYPE,
                                    new Object[] {1L},
                                    new Object[] {100L}),
                            new SnapshotSplit(
                                    SPLIT_1_ID, tableId, ROW_TYPE, new Object[] {100L}, null));
        }

        @Override
        public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
            return new FixtureFetchTask(this.runTag, sourceSplitBase);
        }

        @Override
        public FetchTask.Context createFetchTaskContext(
                SourceSplitBase sourceSplitBase, SourceConfig sourceConfig) {
            return new FixtureFetchTaskContext(sourceConfig.isExactlyOnce());
        }
    }

    private static final class FixtureFetchTask implements FetchTask<SourceSplitBase> {
        private static final Schema KEY_SCHEMA =
                SchemaBuilder.struct()
                        .name("db1.table1.Key")
                        .field("id", Schema.INT64_SCHEMA)
                        .build();
        private static final Schema ROW_SCHEMA =
                SchemaBuilder.struct()
                        .name("db1.table1.Value")
                        .field("id", Schema.INT64_SCHEMA)
                        .build();
        private static final Schema SOURCE_SCHEMA =
                SchemaBuilder.struct()
                        .name("io.debezium.connector.mysql.Source")
                        .field(AbstractSourceInfo.DATABASE_NAME_KEY, Schema.STRING_SCHEMA)
                        .field(AbstractSourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                        .build();
        private static final Schema ENVELOPE_SCHEMA =
                SchemaBuilder.struct()
                        .name("db1.table1.Envelope")
                        .field(Envelope.FieldName.AFTER, ROW_SCHEMA)
                        .field(Envelope.FieldName.SOURCE, SOURCE_SCHEMA)
                        .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                        .build();

        private final String runTag;
        private final SourceSplitBase split;
        private final AtomicBoolean running = new AtomicBoolean(false);

        private FixtureFetchTask(String runTag, SourceSplitBase split) {
            this.runTag = runTag;
            this.split = split;
        }

        @Override
        public void execute(Context context) throws Exception {
            running.set(true);
            try {
                Map<String, String> partition = Collections.singletonMap("server", "cdc-test");
                ChangeEventQueue<DataChangeEvent> queue = context.getQueue();
                if (split.isSnapshotSplit()) {
                    if (SPLIT_0_ID.equals(split.splitId())) {
                        queue.enqueue(
                                new DataChangeEvent(
                                        WatermarkEvent.create(
                                                partition,
                                                "db1.table1",
                                                split.splitId(),
                                                WatermarkKind.LOW,
                                                new FixtureOffset(SPLIT_0_LOW_WATERMARK))));
                        queue.enqueue(
                                new DataChangeEvent(
                                        dataRecord(partition, 1L, SPLIT_0_LOW_WATERMARK, "r")));
                        queue.enqueue(
                                new DataChangeEvent(
                                        WatermarkEvent.create(
                                                partition,
                                                "db1.table1",
                                                split.splitId(),
                                                WatermarkKind.HIGH,
                                                new FixtureOffset(SPLIT_0_HIGH_WATERMARK))));
                        if (context.isExactlyOnce()) {
                            queue.enqueue(
                                    new DataChangeEvent(
                                            WatermarkEvent.create(
                                                    partition,
                                                    "db1.table1",
                                                    split.splitId(),
                                                    WatermarkKind.END,
                                                    new FixtureOffset(SPLIT_0_HIGH_WATERMARK))));
                        }
                    } else {
                        queue.enqueue(
                                new DataChangeEvent(
                                        WatermarkEvent.create(
                                                partition,
                                                "db1.table1",
                                                split.splitId(),
                                                WatermarkKind.LOW,
                                                new FixtureOffset(SPLIT_1_LOW_WATERMARK))));
                        queue.enqueue(
                                new DataChangeEvent(
                                        dataRecord(partition, 100L, SPLIT_1_LOW_WATERMARK, "r")));
                        queue.enqueue(
                                new DataChangeEvent(
                                        WatermarkEvent.create(
                                                partition,
                                                "db1.table1",
                                                split.splitId(),
                                                WatermarkKind.HIGH,
                                                new FixtureOffset(SPLIT_1_HIGH_WATERMARK))));
                        if (context.isExactlyOnce()) {
                            queue.enqueue(
                                    new DataChangeEvent(
                                            WatermarkEvent.create(
                                                    partition,
                                                    "db1.table1",
                                                    split.splitId(),
                                                    WatermarkKind.END,
                                                    new FixtureOffset(SPLIT_1_HIGH_WATERMARK))));
                        }
                    }
                } else {
                    queue.enqueue(new DataChangeEvent(dataRecord(partition, 90L, 90L, "c")));
                    queue.enqueue(new DataChangeEvent(dataRecord(partition, 200L, 98L, "c")));
                    queue.enqueue(new DataChangeEvent(dataRecord(partition, 300L, 122L, "c")));
                }
            } finally {
                running.set(false);
            }
        }

        private static SourceRecord dataRecord(
                Map<String, String> partition, long id, long pos, String op) {
            Struct key = new Struct(KEY_SCHEMA).put("id", id);
            Struct after = new Struct(ROW_SCHEMA).put("id", id);
            Struct source =
                    new Struct(SOURCE_SCHEMA)
                            .put(AbstractSourceInfo.DATABASE_NAME_KEY, "db1")
                            .put(AbstractSourceInfo.TABLE_NAME_KEY, "table1");
            Struct envelope =
                    new Struct(ENVELOPE_SCHEMA)
                            .put(Envelope.FieldName.AFTER, after)
                            .put(Envelope.FieldName.SOURCE, source)
                            .put(Envelope.FieldName.OPERATION, op);
            return new SourceRecord(
                    partition,
                    Collections.singletonMap("pos", String.valueOf(pos)),
                    "db1.table1",
                    KEY_SCHEMA,
                    key,
                    ENVELOPE_SCHEMA,
                    envelope);
        }

        @Override
        public boolean isRunning() {
            return running.get();
        }

        @Override
        public void shutdown() {
            running.set(false);
        }

        @Override
        public SourceSplitBase getSplit() {
            return split;
        }
    }

    private static final class FixtureFetchTaskContext implements FetchTask.Context {
        private final boolean exactlyOnce;
        private final ChangeEventQueue<DataChangeEvent> queue =
                new ChangeEventQueue.Builder<DataChangeEvent>()
                        .maxBatchSize(16)
                        .maxQueueSize(64)
                        .pollInterval(Duration.ofMillis(5))
                        .loggingContextSupplier(
                                () -> LoggingContext.forConnector("test", "fixture", "cdc"))
                        .build();

        private FixtureFetchTaskContext(boolean exactlyOnce) {
            this.exactlyOnce = exactlyOnce;
        }

        @Override
        public void configure(SourceSplitBase sourceSplitBase) {}

        @Override
        public ChangeEventQueue<DataChangeEvent> getQueue() {
            return queue;
        }

        @Override
        public TableId getTableId(SourceRecord record) {
            return SourceRecordUtils.getTableId(record);
        }

        @Override
        public Tables.TableFilter getTableFilter() {
            return tableId -> true;
        }

        @Override
        public boolean isExactlyOnce() {
            return exactlyOnce;
        }

        @Override
        public Offset getStreamOffset(SourceRecord record) {
            Object pos = record.sourceOffset().get("pos");
            return new FixtureOffset(Long.parseLong(String.valueOf(pos)));
        }

        @Override
        public boolean isDataChangeRecord(SourceRecord record) {
            return SourceRecordUtils.isDataChangeRecord(record);
        }

        @Override
        public boolean isRecordBetween(
                SourceRecord record, Object[] splitStart, Object[] splitEnd) {
            Struct key = (Struct) record.key();
            long id = key.getInt64("id");
            if (splitStart != null && id < ((Number) splitStart[0]).longValue()) {
                return false;
            }
            if (splitEnd != null && id >= ((Number) splitEnd[0]).longValue()) {
                return false;
            }
            return true;
        }

        @Override
        public void rewriteOutputBuffer(
                Map<Struct, SourceRecord> outputBuffer, SourceRecord changeRecord) {}

        @Override
        public List<SourceRecord> formatMessageTimestamp(Collection<SourceRecord> snapshotRecords) {
            return new ArrayList<>(snapshotRecords);
        }

        @Override
        public void close() {}
    }

    private static final class FixtureOffsetFactory extends OffsetFactory {
        private static final long serialVersionUID = 1L;

        @Override
        public Offset earliest() {
            return new FixtureOffset(1L);
        }

        @Override
        public Offset neverStop() {
            return new FixtureNeverStopOffset();
        }

        @Override
        public Offset latest() {
            return new FixtureOffset(125L);
        }

        @Override
        public Offset specific(Map<String, String> offset) {
            return new FixtureOffset(Long.parseLong(offset.get("pos")));
        }

        @Override
        public Offset specific(String filename, Long position) {
            return new FixtureOffset(position);
        }

        @Override
        public Offset timestamp(long timestamp) {
            return new FixtureOffset(timestamp);
        }
    }

    private static class FixtureOffset extends Offset {
        private static final long serialVersionUID = 1L;

        private FixtureOffset(long pos) {
            this.offset = new HashMap<>(Collections.singletonMap("pos", String.valueOf(pos)));
        }

        @Override
        public int compareTo(Offset other) {
            if (other instanceof FixtureNeverStopOffset) {
                return this instanceof FixtureNeverStopOffset ? 0 : -1;
            }
            return Long.compare(
                    Long.parseLong(this.offset.get("pos")),
                    Long.parseLong(other.getOffset().get("pos")));
        }
    }

    private static final class FixtureNeverStopOffset extends FixtureOffset {
        private static final long serialVersionUID = 1L;

        private FixtureNeverStopOffset() {
            super(Long.MAX_VALUE);
        }

        @Override
        public boolean isNeverStop() {
            return true;
        }
    }

    private static final class HarnessEnumeratorContext
            implements SourceSplitEnumerator.Context<SourceSplitBase> {
        private final int parallelism;
        private final Set<Integer> registered = new LinkedHashSet<>();
        private final Map<Integer, List<SourceSplitBase>> assignedBySubtask =
                new ConcurrentHashMap<>();
        private final Map<Integer, Queue<SourceEvent>> toReaderEvents = new ConcurrentHashMap<>();
        private final Map<Integer, Queue<SourceEvent>> toEnumeratorEvents =
                new ConcurrentHashMap<>();
        private final Set<Integer> pendingSplitRequests = ConcurrentHashMap.newKeySet();

        private HarnessEnumeratorContext(int parallelism) {
            this.parallelism = parallelism;
        }

        private void register(int subtaskId) {
            registered.add(subtaskId);
            assignedBySubtask.computeIfAbsent(subtaskId, k -> new CopyOnWriteArrayList<>());
            toReaderEvents.computeIfAbsent(subtaskId, k -> new ConcurrentLinkedQueue<>());
            toEnumeratorEvents.computeIfAbsent(subtaskId, k -> new ConcurrentLinkedQueue<>());
        }

        private List<SourceSplitBase> drainAssignedSplits(int subtaskId) {
            List<SourceSplitBase> bag = assignedBySubtask.get(subtaskId);
            if (bag == null || bag.isEmpty()) {
                return Collections.emptyList();
            }
            List<SourceSplitBase> copy = new ArrayList<>(bag);
            bag.clear();
            return copy;
        }

        private SourceEvent pollReaderEvent(int subtaskId) {
            Queue<SourceEvent> q = toReaderEvents.get(subtaskId);
            return q == null ? null : q.poll();
        }

        private SourceEvent pollEnumeratorEvent(int subtaskId) {
            Queue<SourceEvent> q = toEnumeratorEvents.get(subtaskId);
            return q == null ? null : q.poll();
        }

        private boolean consumeSplitRequest(int subtaskId) {
            return pendingSplitRequests.remove(subtaskId);
        }

        @Override
        public int currentParallelism() {
            return parallelism;
        }

        @Override
        public Set<Integer> registeredReaders() {
            return Collections.unmodifiableSet(registered);
        }

        @Override
        public void assignSplit(int subtaskId, List<SourceSplitBase> splits) {
            assignedBySubtask
                    .computeIfAbsent(subtaskId, k -> new CopyOnWriteArrayList<>())
                    .addAll(splits);
        }

        @Override
        public void signalNoMoreSplits(int subtask) {}

        @Override
        public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
            toReaderEvents
                    .computeIfAbsent(subtaskId, k -> new ConcurrentLinkedQueue<>())
                    .add(event);
        }

        @Override
        public MetricsContext getMetricsContext() {
            return new AbstractMetricsContext() {};
        }

        @Override
        public EventListener getEventListener() {
            return null;
        }
    }

    private static final class HarnessReaderContext implements SourceReader.Context {
        private final int subtaskId;
        private final HarnessEnumeratorContext enumCtx;

        private HarnessReaderContext(int subtaskId, HarnessEnumeratorContext enumCtx) {
            this.subtaskId = subtaskId;
            this.enumCtx = enumCtx;
        }

        @Override
        public int getIndexOfSubtask() {
            return subtaskId;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.UNBOUNDED;
        }

        @Override
        public void signalNoMoreElement() {}

        @Override
        public void sendSplitRequest() {
            enumCtx.pendingSplitRequests.add(subtaskId);
        }

        @Override
        public void sendSourceEventToEnumerator(SourceEvent sourceEvent) {
            enumCtx.toEnumeratorEvents
                    .computeIfAbsent(subtaskId, k -> new ConcurrentLinkedQueue<>())
                    .add(sourceEvent);
        }

        @Override
        public MetricsContext getMetricsContext() {
            return new AbstractMetricsContext() {};
        }

        @Override
        public EventListener getEventListener() {
            return null;
        }
    }
}
