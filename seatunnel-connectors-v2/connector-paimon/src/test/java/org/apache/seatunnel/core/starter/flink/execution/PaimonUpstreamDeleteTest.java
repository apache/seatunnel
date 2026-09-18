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
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.paimon.catalog.PaimonCatalog;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.PaimonSink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class PaimonUpstreamDeleteTest {
    @TempDir private Path temporaryDirectory;

    @org.junit.jupiter.api.RepeatedTest(3)
    @Timeout(120)
    void shouldDeleteWithTwoWriters() throws Exception {
        runPipeline(2, false);
    }

    @org.junit.jupiter.api.RepeatedTest(3)
    @Timeout(120)
    void shouldDeleteAfterCheckpointWithTwoWriters() throws Exception {
        runPipeline(2, true);
    }

    @Test
    @Timeout(120)
    void singleWriterControl() throws Exception {
        runPipeline(1, false);
    }

    @Test
    @Timeout(120)
    void singleWriterCheckpointControl() throws Exception {
        runPipeline(1, true);
    }

    private void runPipeline(int sinkParallelism, boolean checkpointed) throws Exception {
        for (Class<?> type :
                Arrays.asList(
                        PaimonSink.class,
                        org.apache.seatunnel.connectors.seatunnel.paimon.sink.PaimonSinkWriter
                                .class,
                        SinkExecuteProcessor.class,
                        org.apache.seatunnel.translation.flink.sink.FlinkSink.class,
                        SeaTunnelRow.class)) {
            System.out.println(
                    "UPSTREAM_CLASS "
                            + type.getName()
                            + " "
                            + type.getProtectionDomain().getCodeSource().getLocation());
        }
        JobContext jobContext = new JobContext(12243L);
        jobContext.setJobMode(JobMode.STREAMING);
        jobContext.setEnableCheckpoint(true);
        Map<TablePath, SeaTunnelSink> sinks = new LinkedHashMap<>();
        List<CatalogTable> tables = new ArrayList<>();
        List<PaimonCatalog> catalogs = new ArrayList<>();
        List<SeaTunnelRow> rows = new ArrayList<>();
        try {
            for (int tableIndex = 0; tableIndex < 1; tableIndex++) {
                String tableName = "table_" + tableIndex;
                Map<String, Object> properties = new HashMap<>();
                properties.put("warehouse", temporaryDirectory.resolve("warehouse").toString());
                properties.put("plugin_name", "Paimon");
                properties.put("database", "routing_test");
                properties.put("table", tableName);
                Map<String, String> options = new HashMap<>();
                options.put("bucket", "1");
                options.put("write-only", "true");
                properties.put("paimon.table.write-props", options);
                ReadonlyConfig config = ReadonlyConfig.fromMap(properties);
                PaimonCatalog catalog = new PaimonCatalog("paimon", config);
                catalog.open();
                catalogs.add(catalog);
                TablePath tablePath = TablePath.of("routing_test", tableName);
                catalog.createDatabase(tablePath, true);
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
                                Collections.emptyList(),
                                "routing regression");
                catalog.createTable(tablePath, table, false);
                tables.add(table);
                PaimonSink sink = new PaimonSink(config, table);
                sink.setJobContext(jobContext);
                sinks.put(tablePath, sink);
                for (int identifier = 0; identifier < 100; identifier++) {
                    rows.add(row(tablePath, RowKind.INSERT, identifier, "before"));
                }
                rows.add(row(tablePath, RowKind.DELETE, 99, "before"));
                for (int update = 0; update < 4; update++) {
                    rows.add(row(tablePath, RowKind.UPDATE_AFTER, 98, "after-" + update));
                }
            }
            StreamExecutionEnvironment environment =
                    StreamExecutionEnvironment.createLocalEnvironment(2);
            environment.getConfig().disableClosureCleaner();
            environment.setRestartStrategy(
                    org.apache.flink.api.common.restartstrategy.RestartStrategies.noRestart());
            DataStream<SeaTunnelRow> input;
            if (checkpointed) {
                environment.enableCheckpointing(200);
                input = environment.addSource(new CheckpointOrderedSource(rows)).setParallelism(2);
            } else {
                input =
                        environment
                                .fromCollection(rows)
                                .partitionCustom(
                                        (Partitioner<Integer>) (writer, count) -> writer,
                                        (KeySelector<SeaTunnelRow, Integer>)
                                                value ->
                                                        value.getRowKind() == RowKind.INSERT
                                                                ? 0
                                                                : 1)
                                .map(
                                        (MapFunction<SeaTunnelRow, SeaTunnelRow>)
                                                value -> {
                                                    if (value.getRowKind() != RowKind.INSERT) {
                                                        Thread.sleep(200);
                                                    }
                                                    return value;
                                                })
                                .setParallelism(2);
            }
            SeaTunnelSink sink = sinks.values().iterator().next();
            SinkExecuteProcessor processor =
                    new SinkExecuteProcessor(
                            new ArrayList<>(),
                            ConfigFactory.parseString("job.mode=STREAMING"),
                            Collections.emptyList(),
                            jobContext);
            processor
                    .createVersionSpecificDataStreamSink(
                            new DataStreamTableInfo(input, tables, "input"),
                            sink,
                            sinkParallelism,
                            ConfigFactory.empty())
                    .setParallelism(sinkParallelism)
                    .uid("upstream-sink");
            environment.execute("issue12243-upstream-delete");
            FileStoreTable table =
                    (FileStoreTable)
                            catalogs.get(0).getPaimonTable(sinks.keySet().iterator().next());
            ReadBuilder read = table.newReadBuilder();
            Map<List<Object>, String> actual = new HashMap<>();
            try (RecordReader<InternalRow> reader =
                            read.newRead().createReader(read.newScan().plan());
                    RecordReaderIterator<InternalRow> iterator =
                            new RecordReaderIterator<>(reader)) {
                while (iterator.hasNext()) {
                    InternalRow value = iterator.next();
                    actual.put(
                            Arrays.asList(value.getInt(0), value.getString(1).toString()),
                            value.getString(2).toString());
                }
            }
            Map<List<Object>, String> expected = new HashMap<>();
            for (int id = 0; id < 99; id++) {
                expected.put(Arrays.asList(id, "part-" + id % 2), id == 98 ? "after-3" : "before");
            }
            System.out.println(
                    "UPSTREAM_RESULT sinkParallelism="
                            + sinkParallelism
                            + ", checkpointed="
                            + checkpointed
                            + ", options="
                            + table.schema().options()
                            + ", primaryKeys="
                            + table.schema().primaryKeys()
                            + ", partitionKeys="
                            + table.schema().partitionKeys()
                            + ", snapshot="
                            + table.snapshotManager().latestSnapshotId()
                            + ", count="
                            + actual.size()
                            + ", deletedKeyValue="
                            + actual.get(Arrays.asList(99, "part-1"))
                            + ", updatedKeyValue="
                            + actual.get(Arrays.asList(98, "part-0")));
            org.junit.jupiter.api.Assertions.assertAll(
                    () -> assertEquals(99, actual.size(), "committed row count"),
                    () ->
                            assertFalse(
                                    actual.containsKey(Arrays.asList(99, "part-1")),
                                    "DELETE must remove complete primary key"),
                    () ->
                            assertEquals(
                                    "after-3",
                                    actual.get(Arrays.asList(98, "part-0")),
                                    "UPDATE must replace old value"),
                    () -> assertEquals(expected, actual, "all primary keys and values"));
        } finally {
            for (PaimonCatalog catalog : catalogs) {
                catalog.close();
            }
        }
    }

    /** Emit INSERTs from subtask 0, then changes from subtask 1 after a completed checkpoint. */
    private static final class CheckpointOrderedSource
            extends RichParallelSourceFunction<SeaTunnelRow>
            implements CheckpointedFunction, CheckpointListener {
        private final List<SeaTunnelRow> rows;
        private final Map<Long, Integer> phases = new java.util.concurrent.ConcurrentHashMap<>();
        private volatile boolean running = true;
        private volatile int phase;
        private volatile int completedPhase;

        private CheckpointOrderedSource(List<SeaTunnelRow> rows) {
            this.rows = rows;
        }

        @Override
        public void run(SourceContext<SeaTunnelRow> context) throws Exception {
            synchronized (context.getCheckpointLock()) {
                if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
                    for (SeaTunnelRow row : rows) {
                        if (row.getRowKind() == RowKind.INSERT) {
                            context.collect(row);
                        }
                    }
                }
                phase = 1;
            }
            while (running && completedPhase < 1) {
                Thread.sleep(10);
            }
            synchronized (context.getCheckpointLock()) {
                if (getRuntimeContext().getIndexOfThisSubtask() == 1) {
                    for (SeaTunnelRow row : rows) {
                        if (row.getRowKind() != RowKind.INSERT) {
                            context.collect(row);
                        }
                    }
                }
                phase = 2;
            }
            while (running && completedPhase < 2) {
                Thread.sleep(10);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            phases.put(context.getCheckpointId(), phase);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) {}

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            Integer saved = phases.get(checkpointId);
            if (saved != null) {
                completedPhase = Math.max(completedPhase, saved);
                phases.keySet().removeIf(id -> id <= checkpointId);
            }
        }
    }

    private static SeaTunnelRow row(TablePath table, RowKind kind, int identifier, String value) {
        SeaTunnelRow row =
                new SeaTunnelRow(new Object[] {identifier, "part-" + identifier % 2, value});
        row.setTableId(table.toString());
        row.setRowKind(kind);
        return row;
    }
}
