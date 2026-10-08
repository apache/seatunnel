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

package org.apache.seatunnel.core.starter.flink.execution;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SupportSinkDataPartition;
import org.apache.seatunnel.api.sink.multitablesink.MultiTableSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.RestoreTableSchemaEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.paimon.catalog.PaimonCatalog;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.PaimonSink;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.CloseableIterator;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Local integration regressions for #12243 through the production multi-table factory and
 * processor. Uses an in-process MiniCluster and temporary Paimon files, with checkpoint-driven
 * source phases; no external databases, containers, or services. Container entry coverage lives in
 * PaimonIT.
 */
class PaimonFlinkBucketRoutingTest {

    private static final Map<String, Progress> PROGRESS = new ConcurrentHashMap<>();

    @TempDir private Path temporaryDirectory;

    @Test
    @Timeout(120)
    void shouldDeliverBroadcastSchemaControlsToEveryPhysicalSubtask() throws Exception {
        try (Fixture fixture = new Fixture(1, 1)) {
            StreamExecutionEnvironment environment =
                    StreamExecutionEnvironment.createLocalEnvironment(2);
            environment.getConfig().disableClosureCleaner();
            environment.setRestartStrategy(RestartStrategies.noRestart());
            SeaTunnelRow broadcast = new SeaTunnelRow(0);
            Map<String, Object> options = new HashMap<>();
            options.put(
                    "schema_change_broadcast", new RestoreTableSchemaEvent(fixture.tables.get(0)));
            broadcast.setOptions(options);
            SeaTunnelSink sink = fixture.createWrappedSink(processor(jobContext()), jobContext());
            DataStream<SeaTunnelRow> controls =
                    SinkWriteRoutingPartitioner.prepare(
                            environment.fromElements(broadcast, broadcast), sink, 2, true);
            Map<Integer, Integer> received = new HashMap<>();
            try (CloseableIterator<Integer> rows =
                    controls.map(new SchemaDestination()).setParallelism(2).executeAndCollect()) {
                while (rows.hasNext()) {
                    received.merge(rows.next(), 1, Integer::sum);
                }
            }
            assertEquals(Integer.valueOf(1), received.get(0));
            assertEquals(Integer.valueOf(1), received.get(1));
            assertEquals(2, received.size());
        }
    }

    private static final class SchemaDestination extends RichMapFunction<SeaTunnelRow, Integer> {
        @Override
        public Integer map(SeaTunnelRow row) {
            int subtask = getRuntimeContext().getIndexOfThisSubtask();
            assertEquals(0, row.getArity());
            assertEquals((long) subtask, row.getOptions().get("schema_subtask_id"));
            return subtask;
        }
    }

    @ParameterizedTest
    @CsvSource({"1,false,2", "2,false,2", "1,true,2", "2,true,2", "2,false,1", "3,false,2"})
    @Timeout(120)
    void shouldRouteRunningStreamChangesBeforeAndAfterCompletedCheckpoint(
            int writers, boolean changesAfterCheckpoint, int sourceWriters) throws Exception {
        try (Fixture fixture = new Fixture(1, 1)) {
            JobClient client = fixture.submit(writers, changesAfterCheckpoint, true, sourceWriters);
            try {
                fixture.awaitRows(client);
                assertTrue(fixture.progress.completedInitial.containsKey(0));
                assertEquals(sourceWriters, fixture.progress.completedInitial.size());
                fixture.assertRows();
            } finally {
                cancelIfRunning(client);
            }
        }
    }

    @Test
    @Timeout(120)
    void shouldRouteRunningStreamMultipleTablesPartitionsAndBucketsThroughActualFlinkSink()
            throws Exception {
        try (Fixture fixture = new Fixture(2, 4)) {
            JobClient client = fixture.submit(2, true, true);
            try {
                fixture.awaitRows(client);
                fixture.assertRows();
            } finally {
                cancelIfRunning(client);
            }
        }
    }

    @Test
    void shouldRejectMixingFixedAndDynamicBucketsThroughActualFactory() throws Exception {
        try (Fixture fixture = new Fixture(2, 4, true)) {
            JobContext jobContext = jobContext();
            SeaTunnelSink sink = fixture.createWrappedSink(processor(jobContext), jobContext);
            assertTrue(sink instanceof MultiTableSink);
            UnsupportedOperationException failure =
                    assertThrows(
                            UnsupportedOperationException.class,
                            () -> SupportSinkDataPartition.resolve(sink, 2));
            assertTrue(failure.getMessage().contains("Cannot mix routed and unrouted sinks"));
        }
    }

    @Test
    void shouldRejectIndependentSourceTablesTargetingOnePhysicalPaimonTable() throws Exception {
        try (Fixture fixture = new Fixture(1, 1)) {
            JobContext jobContext = jobContext();
            SeaTunnelSink sink =
                    fixture.createDuplicateTargetWrappedSink(processor(jobContext), jobContext);
            assertTrue(sink instanceof MultiTableSink);
            IllegalArgumentException failure =
                    assertThrows(
                            IllegalArgumentException.class,
                            () -> SupportSinkDataPartition.resolve(sink, 2));
            assertTrue(failure.getMessage().contains("same physical target"));
        }
    }

    private static void cancelIfRunning(JobClient client) throws Exception {
        if (!client.getJobExecutionResult().isDone()) {
            client.cancel().get(20, TimeUnit.SECONDS);
            client.getJobExecutionResult()
                    .handle((result, failure) -> null)
                    .get(20, TimeUnit.SECONDS);
        }
    }

    private static JobContext jobContext() {
        JobContext context = new JobContext(12243L);
        context.setJobMode(JobMode.STREAMING);
        context.setEnableCheckpoint(true);
        return context;
    }

    private static SinkExecuteProcessor processor(JobContext context) {
        return new SinkExecuteProcessor(
                new ArrayList<>(),
                ConfigFactory.parseString("job.mode=STREAMING"),
                Collections.emptyList(),
                context);
    }

    private final class Fixture implements AutoCloseable {
        private final String runId = UUID.randomUUID().toString();
        private final Progress progress = new Progress();
        private final Map<TablePath, PaimonCatalog> catalogs = new LinkedHashMap<>();
        private final Map<TablePath, ReadonlyConfig> configs = new LinkedHashMap<>();
        private final List<CatalogTable> tables = new ArrayList<>();

        private Fixture(int tableCount, int buckets) throws Exception {
            this(tableCount, buckets, false);
        }

        private Fixture(int tableCount, int buckets, boolean dynamicLastTable) throws Exception {
            PROGRESS.put(runId, progress);
            for (int index = 0; index < tableCount; index++) {
                String tableName = "table_" + index;
                TablePath path = TablePath.of("routing_test", tableName);
                Map<String, Object> properties = new HashMap<>();
                properties.put("warehouse", temporaryDirectory.resolve("warehouse").toString());
                properties.put("plugin_name", "Paimon");
                properties.put("database", "routing_test");
                properties.put("table", tableName);
                Map<String, String> options = new HashMap<>();
                options.put(
                        "bucket",
                        Integer.toString(
                                dynamicLastTable && index == tableCount - 1 ? -1 : buckets));
                options.put("write-only", "true");
                properties.put("paimon.table.write-props", options);
                ReadonlyConfig config = ReadonlyConfig.fromMap(properties);
                configs.put(path, config);
                PaimonCatalog catalog = new PaimonCatalog("paimon", config);
                catalog.open();
                catalogs.put(path, catalog);
                catalog.createDatabase(path, true);
                TableSchema schema =
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "id",
                                                BasicType.INT_TYPE,
                                                (Long) null,
                                                false,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "part",
                                                BasicType.STRING_TYPE,
                                                (Long) null,
                                                false,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "value",
                                                BasicType.STRING_TYPE,
                                                (Long) null,
                                                true,
                                                null,
                                                null))
                                .primaryKey(PrimaryKey.of("pk", Arrays.asList("id", "part")))
                                .build();
                CatalogTable table =
                        CatalogTable.of(
                                TableIdentifier.of("paimon", "routing_test", tableName),
                                schema,
                                new HashMap<>(),
                                tableCount > 1
                                        ? Collections.singletonList("part")
                                        : Collections.emptyList(),
                                "fixed bucket routing regression");
                catalog.createTable(path, table, false);
                tables.add(table);
            }
        }

        private JobClient submit(int writers, boolean changesAfterCheckpoint, boolean persistent)
                throws Exception {
            return submit(writers, changesAfterCheckpoint, persistent, 2);
        }

        private JobClient submit(
                int writers, boolean changesAfterCheckpoint, boolean persistent, int sourceWriters)
                throws Exception {
            Configuration flinkConfig = new Configuration();
            StreamExecutionEnvironment environment =
                    StreamExecutionEnvironment.createLocalEnvironment(
                            Math.max(2, Math.max(writers, sourceWriters)), flinkConfig);
            environment.getConfig().disableClosureCleaner();
            environment.setRestartStrategy(RestartStrategies.noRestart());
            environment.enableCheckpointing(200);
            environment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
            JobContext jobContext = jobContext();
            SinkExecuteProcessor processor = processor(jobContext);
            SeaTunnelSink sink = createWrappedSink(processor, jobContext);
            assertTrue(sink instanceof MultiTableSink, "Even one supported table uses the wrapper");
            assertTrue(SupportSinkDataPartition.resolve(sink, writers).isPresent());
            DataStream<SeaTunnelRow> input =
                    environment
                            .addSource(
                                    new PhaseSource(
                                            runId,
                                            new ArrayList<>(configs.keySet()),
                                            changesAfterCheckpoint,
                                            persistent))
                            .uid("routing-source")
                            .setParallelism(sourceWriters);
            processor
                    .createVersionSpecificDataStreamSink(
                            new DataStreamTableInfo(input, tables, "input"),
                            sink,
                            writers,
                            ConfigFactory.empty())
                    .setParallelism(writers)
                    .uid("routing-sink");
            return environment.executeAsync("issue12243-fixed-bucket-routing");
        }

        private SeaTunnelSink createWrappedSink(
                SinkExecuteProcessor processor, JobContext jobContext) {
            Map<TablePath, SeaTunnelSink> sinks = new LinkedHashMap<>();
            int index = 0;
            for (Map.Entry<TablePath, ReadonlyConfig> entry : configs.entrySet()) {
                PaimonSink sink = new PaimonSink(entry.getValue(), tables.get(index++));
                sink.setJobContext(jobContext);
                sinks.put(entry.getKey(), sink);
            }
            return processor.tryGenerateMultiTableSink(
                    sinks,
                    ReadonlyConfig.fromMap(Collections.emptyMap()),
                    getClass().getClassLoader());
        }

        private SeaTunnelSink createDuplicateTargetWrappedSink(
                SinkExecuteProcessor processor, JobContext jobContext) {
            TablePath physicalTarget = configs.keySet().iterator().next();
            ReadonlyConfig config = configs.get(physicalTarget);
            CatalogTable table = tables.get(0);
            Map<TablePath, SeaTunnelSink> sinks = new LinkedHashMap<>();
            for (String sourceTable : Arrays.asList("source_a", "source_b")) {
                PaimonSink sink = new PaimonSink(config, table);
                sink.setJobContext(jobContext);
                sinks.put(TablePath.of("routing_test", sourceTable), sink);
            }
            return processor.tryGenerateMultiTableSink(
                    sinks,
                    ReadonlyConfig.fromMap(Collections.emptyMap()),
                    getClass().getClassLoader());
        }

        private void awaitRows(JobClient client) throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (System.nanoTime() < deadline) {
                if (client.getJobExecutionResult().isDone()) {
                    client.getJobExecutionResult().get(5, TimeUnit.SECONDS);
                    throw new AssertionError(
                            "Streaming job ended before the expected committed rows");
                }
                boolean complete = true;
                for (Map.Entry<TablePath, PaimonCatalog> entry : catalogs.entrySet()) {
                    complete &=
                            expectedRows(entry.getKey())
                                    .equals(readRows(entry.getValue(), entry.getKey()));
                }
                if (complete) {
                    return;
                }
                TimeUnit.MILLISECONDS.sleep(50);
            }
            assertRows();
        }

        private void assertRows() throws Exception {
            for (Map.Entry<TablePath, PaimonCatalog> entry : catalogs.entrySet()) {
                assertEquals(
                        expectedRows(entry.getKey()),
                        readRows(entry.getValue(), entry.getKey()),
                        "Every complete primary key and value in " + entry.getKey());
            }
        }

        @Override
        public void close() throws Exception {
            try {
                for (PaimonCatalog catalog : catalogs.values()) {
                    catalog.close();
                }
            } finally {
                PROGRESS.remove(runId);
            }
        }
    }

    private static Map<List<Object>, String> readRows(PaimonCatalog catalog, TablePath path)
            throws Exception {
        FileStoreTable table = (FileStoreTable) catalog.getPaimonTable(path);
        ReadBuilder read = table.newReadBuilder();
        Map<List<Object>, String> rows = new LinkedHashMap<>();
        try (RecordReader<InternalRow> reader = read.newRead().createReader(read.newScan().plan());
                RecordReaderIterator<InternalRow> iterator = new RecordReaderIterator<>(reader)) {
            while (iterator.hasNext()) {
                InternalRow row = iterator.next();
                rows.put(
                        Arrays.asList(row.getInt(0), row.getString(1).toString()),
                        row.getString(2).toString());
            }
        }
        return rows;
    }

    private static Map<List<Object>, String> expectedRows(TablePath table) {
        Map<List<Object>, String> expected = new LinkedHashMap<>();
        for (int id = 0; id < 50; id++) {
            for (int part = 0; part < 2; part++) {
                expected.put(Arrays.asList(id, "part-" + part), table + ":before");
            }
        }
        expected.remove(Arrays.asList(49, "part-1"));
        expected.put(Arrays.asList(48, "part-0"), table + ":after-3");
        return expected;
    }

    private static final class Progress {
        private final Map<Integer, Long> completedInitial = new ConcurrentHashMap<>();
    }

    /** Checkpoint completion, rather than elapsed time, permits changes from another source. */
    private static final class PhaseSource extends RichParallelSourceFunction<SeaTunnelRow>
            implements CheckpointedFunction, CheckpointListener {
        private final String runId;
        private final List<TablePath> tables;
        private final boolean changesAfterCheckpoint;
        private final boolean persistent;
        private final Map<Long, Integer> savedPhases = new ConcurrentHashMap<>();
        private transient ListState<Integer> state;
        private transient CountDownLatch initialCheckpoint;
        private transient CountDownLatch changesCheckpoint;
        private transient CountDownLatch cancelled;
        private volatile boolean running = true;
        private volatile int phase;

        private PhaseSource(
                String runId,
                List<TablePath> tables,
                boolean changesAfterCheckpoint,
                boolean persistent) {
            this.runId = runId;
            this.tables = tables;
            this.changesAfterCheckpoint = changesAfterCheckpoint;
            this.persistent = persistent;
        }

        @Override
        public void run(SourceContext<SeaTunnelRow> context) throws Exception {
            int subtask = getRuntimeContext().getIndexOfThisSubtask();
            synchronized (context.getCheckpointLock()) {
                if (subtask == 0) {
                    for (TablePath table : tables) {
                        for (int id = 0; id < 50; id++) {
                            for (int part = 0; part < 2; part++) {
                                emit(context, table, RowKind.INSERT, id, part, "before");
                            }
                        }
                    }
                    if (!changesAfterCheckpoint) {
                        emitChanges(context);
                    }
                }
                phase = changesAfterCheckpoint ? 1 : 2;
            }
            if (changesAfterCheckpoint) {
                if (!awaitCheckpoint(initialCheckpoint)) {
                    return;
                }
                synchronized (context.getCheckpointLock()) {
                    if (subtask == 1) {
                        emitChanges(context);
                    }
                    phase = 2;
                }
            }
            if (!awaitCheckpoint(changesCheckpoint)) {
                return;
            }
            if (persistent) {
                cancelled.await();
            }
        }

        private boolean awaitCheckpoint(CountDownLatch latch) throws Exception {
            if (!latch.await(60, TimeUnit.SECONDS)) {
                throw new IOException(
                        "Timed out waiting for the source phase's completed checkpoint");
            }
            return running;
        }

        private void emitChanges(SourceContext<SeaTunnelRow> context) {
            for (TablePath table : tables) {
                emit(context, table, RowKind.DELETE, 49, 1, "before");
                for (int update = 0; update < 4; update++) {
                    emit(context, table, RowKind.UPDATE_AFTER, 48, 0, "after-" + update);
                }
            }
        }

        private static void emit(
                SourceContext<SeaTunnelRow> context,
                TablePath table,
                RowKind kind,
                int id,
                int part,
                String value) {
            SeaTunnelRow row =
                    new SeaTunnelRow(new Object[] {id, "part-" + part, table + ":" + value});
            row.setTableId(table.toString());
            row.setRowKind(kind);
            context.collect(row);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            state.update(Collections.singletonList(phase));
            savedPhases.put(context.getCheckpointId(), phase);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            initialCheckpoint = new CountDownLatch(1);
            changesCheckpoint = new CountDownLatch(1);
            cancelled = new CountDownLatch(1);
            state =
                    context.getOperatorStateStore()
                            .getListState(new ListStateDescriptor<>("phase", Integer.class));
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            Integer saved = savedPhases.get(checkpointId);
            if (saved == null) {
                return;
            }
            int subtask = getRuntimeContext().getIndexOfThisSubtask();
            if (saved >= 1) {
                initialCheckpoint.countDown();
            }
            if (saved >= 2) {
                changesCheckpoint.countDown();
                PROGRESS.get(runId).completedInitial.put(subtask, checkpointId);
            }
            savedPhases.keySet().removeIf(id -> id <= checkpointId);
        }

        @Override
        public void cancel() {
            running = false;
            for (CountDownLatch latch :
                    Arrays.asList(initialCheckpoint, changesCheckpoint, cancelled)) {
                if (latch != null) {
                    while (latch.getCount() > 0) {
                        latch.countDown();
                    }
                }
            }
        }
    }
}
