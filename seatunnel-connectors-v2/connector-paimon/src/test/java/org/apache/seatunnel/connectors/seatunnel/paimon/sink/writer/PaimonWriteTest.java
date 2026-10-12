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

package org.apache.seatunnel.connectors.seatunnel.paimon.sink.writer;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.paimon.catalog.PaimonCatalog;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonHadoopConfiguration;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.PaimonSink;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.PaimonSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.bucket.PaimonBucketAssignerFactory;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.state.PaimonSinkState;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.IndexIncrement;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class PaimonWriteTest {

    private PaimonCatalog paimonCatalog;
    private TableSchema.Builder schemaBuilder;
    private final String CATALOG_NAME = "paimon_catalog";
    private final String DATABASE_NAME = "test_default";
    private final String TABLE_NAME = "test_table";
    private PaimonSinkWriter paimonSinkWriter;
    private ReadonlyConfig readonlyConfig;
    private SinkWriter.Context context;
    private final String commitUser = UUID.randomUUID().toString();

    @BeforeEach
    public void before() {

        Map<String, Object> properties = new HashMap<>();
        properties.put("warehouse", "/tmp/paimon");
        properties.put("plugin_name", "Paimon");
        properties.put("database", DATABASE_NAME);
        properties.put("table", TABLE_NAME);
        Map<String, String> writeProps = new HashMap<>();
        writeProps.put("write-only", "true");
        properties.put("paimon.table.write-props", writeProps);
        readonlyConfig = ReadonlyConfig.fromMap(properties);
        paimonCatalog = new PaimonCatalog(CATALOG_NAME, readonlyConfig);
        paimonCatalog.open();
        paimonCatalog.createDatabase(TablePath.of(DATABASE_NAME, TABLE_NAME), false);
        this.schemaBuilder =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "c_map",
                                        new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE),
                                        (Long) null,
                                        true,
                                        null,
                                        null))
                        .column(
                                PhysicalColumn.of(
                                        "c_array",
                                        ArrayType.STRING_ARRAY_TYPE,
                                        (Long) null,
                                        false,
                                        null,
                                        "c_array"))
                        .column(
                                PhysicalColumn.of(
                                        "c_string",
                                        BasicType.STRING_TYPE,
                                        (Long) null,
                                        false,
                                        null,
                                        "c_string"))
                        .column(
                                PhysicalColumn.of(
                                        "c_boolean",
                                        BasicType.BOOLEAN_TYPE,
                                        (Long) null,
                                        false,
                                        null,
                                        "c_boolean"))
                        .column(
                                PhysicalColumn.of(
                                        "c_tinyint",
                                        BasicType.INT_TYPE,
                                        (Long) null,
                                        false,
                                        null,
                                        "c_tinyint"))
                        .column(
                                PhysicalColumn.of(
                                        "c_smallint",
                                        BasicType.INT_TYPE,
                                        (Long) null,
                                        false,
                                        null,
                                        "c_smallint"))
                        .column(
                                PhysicalColumn.of(
                                        "c_int",
                                        BasicType.INT_TYPE,
                                        (Long) null,
                                        false,
                                        null,
                                        "c_int"))
                        .column(
                                PhysicalColumn.of(
                                        "c_bigint",
                                        BasicType.LONG_TYPE,
                                        (Long) null,
                                        false,
                                        null,
                                        "c_bigint"))
                        .column(
                                PhysicalColumn.of(
                                        "c_float",
                                        BasicType.FLOAT_TYPE,
                                        (Long) null,
                                        false,
                                        null,
                                        "c_float"))
                        .column(
                                PhysicalColumn.of(
                                        "c_double",
                                        BasicType.DOUBLE_TYPE,
                                        (Long) null,
                                        false,
                                        null,
                                        "c_double"))
                        .column(
                                PhysicalColumn.of(
                                        "c_decimal",
                                        new DecimalType(10, 2),
                                        (Long) null,
                                        false,
                                        null,
                                        "c_decimal"))
                        .column(
                                PhysicalColumn.of(
                                        "c_bytes",
                                        BasicType.BYTE_TYPE,
                                        (Long) null,
                                        false,
                                        null,
                                        "c_bytes"))
                        .column(
                                PhysicalColumn.of(
                                        "c_date",
                                        LocalTimeType.LOCAL_DATE_TYPE,
                                        (Long) null,
                                        false,
                                        null,
                                        "c_date"))
                        .column(
                                PhysicalColumn.of(
                                        "c_timestamp",
                                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                        (Long) null,
                                        false,
                                        null,
                                        "c_timestamp"))
                        .column(
                                PhysicalColumn.of(
                                        "c_time",
                                        LocalTimeType.LOCAL_TIME_TYPE,
                                        (Long) null,
                                        false,
                                        null,
                                        "c_time"));
        paimonCatalog.createTable(
                TablePath.of(DATABASE_NAME, TABLE_NAME),
                CatalogTable.of(
                        TableIdentifier.of(CATALOG_NAME, DATABASE_NAME, TABLE_NAME),
                        schemaBuilder.build(),
                        new HashMap<>(),
                        new ArrayList<>(),
                        "test table"),
                false);

        context =
                new SinkWriter.Context() {
                    @Override
                    public int getIndexOfSubtask() {
                        return 0;
                    }

                    @Override
                    public MetricsContext getMetricsContext() {
                        return null;
                    }

                    @Override
                    public EventListener getEventListener() {
                        return null;
                    }
                };
    }

    @Test
    void testWriterStateSerializerIsRegistered() throws Exception {
        assertWriterStateRoundTrip(Collections.emptyList(), 0L);
    }

    @Test
    void testWriterStateSerializerPreservesDataFiles() throws Exception {
        CommitMessageImpl message = dataCommitMessage("partition-a", 1, "data");
        Assertions.assertFalse(message.isEmpty());
        assertWriterStateRoundTrip(Collections.singletonList(message), 42L);
    }

    @Test
    void testWriterStateSerializerPreservesCompactionFiles() throws Exception {
        CommitMessageImpl message =
                new CommitMessageImpl(
                        BinaryRow.singleColumn("partition-b"),
                        2,
                        4,
                        DataIncrement.emptyIncrement(),
                        new CompactIncrement(
                                Collections.singletonList(dataFile("before.parquet")),
                                Collections.singletonList(dataFile("after.parquet")),
                                Collections.singletonList(dataFile("compact-changelog.parquet"))));
        Assertions.assertFalse(message.isEmpty());
        assertWriterStateRoundTrip(Collections.singletonList(message), 43L);
    }

    @Test
    void testWriterStateSerializerPreservesIndexFiles() throws Exception {
        CommitMessageImpl message =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        null,
                        DataIncrement.emptyIncrement(),
                        CompactIncrement.emptyIncrement(),
                        new IndexIncrement(
                                Collections.singletonList(
                                        new IndexFileMeta("hash", "new.index", 128L, 10L)),
                                Collections.singletonList(
                                        new IndexFileMeta("hash", "old.index", 64L, 5L))));
        Assertions.assertFalse(message.isEmpty());
        assertWriterStateRoundTrip(Collections.singletonList(message), 44L);
    }

    @Test
    void testWriterStateSerializerPreservesMultipleMessagesAndCheckpointBoundary()
            throws Exception {
        List<CommitMessage> messages =
                new ArrayList<>(
                        Arrays.asList(
                                dataCommitMessage("partition-a", 0, "first"),
                                dataCommitMessage("partition-a", 1, "second"),
                                dataCommitMessage("partition-b", 0, "third")));
        PaimonSinkState restored =
                assertWriterStateRoundTrip(messages, (long) Integer.MAX_VALUE + 1L);

        // Restoring a checkpoint must not share the mutable message list with the original state.
        messages.clear();
        Assertions.assertEquals(3, restored.getCommitTables().size());
        assertWriterStateRoundTrip(restored.getCommitTables(), restored.getCheckpointId());
    }

    private PaimonSinkState assertWriterStateRoundTrip(
            List<CommitMessage> messages, long checkpointId) throws Exception {
        PaimonSink sink =
                new PaimonSink(
                        readonlyConfig,
                        CatalogTable.of(
                                TableIdentifier.of(CATALOG_NAME, DATABASE_NAME, TABLE_NAME),
                                schemaBuilder.build(),
                                new HashMap<>(),
                                new ArrayList<>(),
                                "test table"));
        Assertions.assertTrue(sink.getWriterStateSerializer().isPresent());
        Serializer<PaimonSinkState> serializer = sink.getWriterStateSerializer().get();
        PaimonSinkState state = new PaimonSinkState(messages, "commit-user", checkpointId);
        PaimonSinkState restored = serializer.deserialize(serializer.serialize(state));
        Assertions.assertNotSame(state, restored);
        Assertions.assertEquals(state.getCommitUser(), restored.getCommitUser());
        Assertions.assertEquals(checkpointId, restored.getCheckpointId());
        Assertions.assertEquals(messages.size(), restored.getCommitTables().size());
        for (int i = 0; i < messages.size(); i++) {
            CommitMessageImpl expected = (CommitMessageImpl) messages.get(i);
            CommitMessageImpl actual = (CommitMessageImpl) restored.getCommitTables().get(i);
            Assertions.assertNotSame(expected, actual);
            Assertions.assertEquals(expected.partition(), actual.partition());
            Assertions.assertEquals(expected.bucket(), actual.bucket());
            Assertions.assertEquals(expected.totalBuckets(), actual.totalBuckets());
            Assertions.assertEquals(expected.newFilesIncrement(), actual.newFilesIncrement());
            Assertions.assertEquals(expected.compactIncrement(), actual.compactIncrement());
            Assertions.assertEquals(expected.indexIncrement(), actual.indexIncrement());
        }
        return restored;
    }

    private CommitMessageImpl dataCommitMessage(String partition, int bucket, String prefix) {
        return new CommitMessageImpl(
                BinaryRow.singleColumn(partition),
                bucket,
                4,
                new DataIncrement(
                        Collections.singletonList(dataFile(prefix + "-new.parquet")),
                        Collections.singletonList(dataFile(prefix + "-deleted.parquet")),
                        Collections.singletonList(dataFile(prefix + "-changelog.parquet"))),
                CompactIncrement.emptyIncrement());
    }

    private DataFileMeta dataFile(String name) {
        return DataFileMeta.forAppend(
                name,
                128L,
                10L,
                SimpleStats.EMPTY_STATS,
                1L,
                10L,
                0L,
                Collections.emptyList(),
                null,
                FileSource.APPEND,
                Collections.emptyList(),
                null);
    }

    @Test
    void testWaitCompaction() throws Exception {

        JobContext jobContext = new JobContext();
        jobContext.setJobMode(JobMode.STREAMING);
        TablePath tablePath = TablePath.of(DATABASE_NAME, TABLE_NAME);
        paimonSinkWriter =
                new PaimonSinkWriter(
                        context,
                        readonlyConfig,
                        paimonCatalog.getTable(tablePath),
                        paimonCatalog.getPaimonTable(tablePath),
                        commitUser,
                        jobContext,
                        new PaimonSinkConfig(readonlyConfig),
                        new PaimonHadoopConfiguration(),
                        new PaimonBucketAssignerFactory());
        Assertions.assertFalse(paimonSinkWriter.waitCompaction());

        jobContext.setJobMode(JobMode.BATCH);
        paimonSinkWriter =
                new PaimonSinkWriter(
                        context,
                        readonlyConfig,
                        paimonCatalog.getTable(tablePath),
                        paimonCatalog.getPaimonTable(tablePath),
                        commitUser,
                        jobContext,
                        new PaimonSinkConfig(readonlyConfig),
                        new PaimonHadoopConfiguration(),
                        new PaimonBucketAssignerFactory());
        Assertions.assertTrue(paimonSinkWriter.waitCompaction());

        Map<String, Object> properties = new HashMap<>();
        properties.put("warehouse", "/tmp/paimon");
        properties.put("plugin_name", "Paimon");
        properties.put("database", DATABASE_NAME);
        properties.put("table", TABLE_NAME);
        Map<String, String> writeProps = new HashMap<>();
        writeProps.put("changelog-producer", "lookup");
        properties.put("paimon.table.write-props", writeProps);
        readonlyConfig = ReadonlyConfig.fromMap(properties);
        paimonSinkWriter =
                new PaimonSinkWriter(
                        context,
                        readonlyConfig,
                        paimonCatalog.getTable(tablePath),
                        paimonCatalog.getPaimonTable(tablePath),
                        commitUser,
                        jobContext,
                        new PaimonSinkConfig(readonlyConfig),
                        new PaimonHadoopConfiguration(),
                        new PaimonBucketAssignerFactory());
        Assertions.assertTrue(paimonSinkWriter.waitCompaction());

        writeProps.put("changelog-producer", "full-compaction");
        readonlyConfig = ReadonlyConfig.fromMap(properties);
        paimonSinkWriter =
                new PaimonSinkWriter(
                        context,
                        readonlyConfig,
                        paimonCatalog.getTable(tablePath),
                        paimonCatalog.getPaimonTable(tablePath),
                        commitUser,
                        jobContext,
                        new PaimonSinkConfig(readonlyConfig),
                        new PaimonHadoopConfiguration(),
                        new PaimonBucketAssignerFactory());
        Assertions.assertTrue(paimonSinkWriter.waitCompaction());
    }

    @AfterEach
    public void after() {
        paimonCatalog.dropDatabase(TablePath.of(DATABASE_NAME, TABLE_NAME), false);
        paimonCatalog.close();
    }
}
