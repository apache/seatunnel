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

package org.apache.seatunnel.e2e.connector.iceberg;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergTableLoader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.utils.HadoopStreamFileIO;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.DependencyJar;

import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.testcontainers.containers.Container;

import com.github.luben.zstd.Zstd;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCatalogType.HADOOP;
import static org.awaitility.Awaitility.given;

@Slf4j
@DisabledOnContainer(
        value = {TestContainerId.SPARK_2_4},
        type = {},
        disabledReason = "")
@DisabledOnOs(OS.WINDOWS)
public class IcebergSinkIT extends TestSuiteBase {

    private static final String CATALOG_DIR = "/tmp/seatunnel_mnt/iceberg/hadoop-sink/";
    private static final String V3_TABLE = "iceberg_v3_table";
    private static final String V3_COPY_TABLE = "iceberg_v3_copy";
    private static final String V3_UPSERT_TABLE = "iceberg_v3_upsert";
    private static final String V4_TABLE = "iceberg_v4_table";

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                // Iceberg's Hadoop catalog cannot reliably create the mounted warehouse's
                // metadata and data directories from the job containers.
                container.execInContainer(
                        "sh",
                        "-c",
                        "mkdir -p " + CATALOG_DIR + "seatunnel_namespace/iceberg_sink_table/data");
                container.execInContainer(
                        "sh",
                        "-c",
                        "mkdir -p "
                                + CATALOG_DIR
                                + "seatunnel_namespace/iceberg_sink_table/metadata");
                container.execInContainer("sh", "-c", "chmod -R 777  " + CATALOG_DIR);

                DependencyJar.of(Zstd.class)
                        .copyTo(container, "/tmp/seatunnel/plugins/Iceberg/lib");
            };

    @TestTemplate
    public void testInsertAndCheckDataE2e(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult textWriteResult =
                container.executeJob("/iceberg/fake_to_iceberg.conf");
        Assertions.assertEquals(0, textWriteResult.getExitCode());
        // stream stage
        given().ignoreExceptions()
                .await()
                .atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(100, loadIcebergTable().size());
                        });
    }

    @TestTemplate
    public void testORCFileFormatWrite(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult textWriteResult =
                container.executeJob("/iceberg/fake_to_orc_iceberg.conf");
        Assertions.assertEquals(0, textWriteResult.getExitCode());
    }

    @TestTemplate
    public void testPartitionKeysPlaceholderE2e(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob(
                        "/iceberg/fake_to_iceberg_with_partition_keys_placeholder.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        Table table = loadIcebergTableObject();
        Assertions.assertFalse(table.spec().isUnpartitioned());
        Assertions.assertEquals(2, table.spec().fields().size());

        List<PartitionField> fields = table.spec().fields();
        Assertions.assertTrue(containsPartitionField(table, fields, "c_bigint", "bucket[16]"));
        Assertions.assertTrue(containsPartitionField(table, fields, "c_timestamp", "identity"));
    }

    @TestTemplate
    public void testFormatV3WriteAndReadE2e(TestContainer container)
            throws IOException, InterruptedException {
        resetTableDirectory(container, V3_TABLE);
        resetTableDirectory(container, V3_COPY_TABLE);

        Container.ExecResult writeResult = container.executeJob("/iceberg/fake_to_iceberg_v3.conf");
        Assertions.assertEquals(0, writeResult.getExitCode(), writeResult.getStderr());
        Table table = loadIcebergTableObject(V3_TABLE);
        Assertions.assertFalse(table.spec().isUnpartitioned());
        // SeaTunnel's Spark translation carries TIMESTAMP in microseconds and TIMESTAMP_TZ in
        // milliseconds, so the precision is cut before the rows reach the sink
        boolean nanosecondRows = container.identifier().getEngineType() != EngineType.SPARK;
        assertFormatV3Table(table, nanosecondRows);

        // Read the v3 table through the source and write it into a second auto-created v3 table:
        // the nanosecond precision must survive the round trip.
        Container.ExecResult copyResult =
                container.executeJob("/iceberg/iceberg_v3_to_iceberg_v3.conf");
        Assertions.assertEquals(0, copyResult.getExitCode(), copyResult.getStderr());
        assertFormatV3Table(loadIcebergTableObject(V3_COPY_TABLE), nanosecondRows);
    }

    @TestTemplate
    public void testFormatV3UpsertRejectedE2e(TestContainer container)
            throws IOException, InterruptedException {
        resetTableDirectory(container, V3_UPSERT_TABLE);

        Container.ExecResult execResult =
                container.executeJob("/iceberg/fake_to_iceberg_v3_upsert.conf");
        Assertions.assertNotEquals(0, execResult.getExitCode());
        String output = execResult.getStdout() + execResult.getStderr();
        Assertions.assertTrue(
                output.contains("Upsert and CDC writes are not supported for Iceberg tables"),
                "the job should report that upsert into a format version 3 table is unsupported");
    }

    @TestTemplate
    public void testUnsupportedFormatVersionRejectedE2e(TestContainer container)
            throws IOException, InterruptedException {
        resetTableDirectory(container, V4_TABLE);

        Container.ExecResult execResult =
                container.executeJob("/iceberg/fake_to_iceberg_unsupported_format_version.conf");
        Assertions.assertNotEquals(0, execResult.getExitCode());
        String output = execResult.getStdout() + execResult.getStderr();
        Assertions.assertTrue(
                output.contains("Unsupported Iceberg table format-version '4'"),
                "the job should report the unsupported format-version");
    }

    private void assertFormatV3Table(Table table, boolean nanosecondRows) throws IOException {
        Assertions.assertEquals(
                3, ((HasTableOperations) table).operations().current().formatVersion());
        Schema schema = table.schema();
        Assertions.assertEquals("timestamp", schema.findType("ts_micros").toString());
        Assertions.assertEquals("timestamp_ns", schema.findType("ts_nanos").toString());
        Assertions.assertEquals("timestamptz_ns", schema.findType("tstz_nanos").toString());

        Map<Long, Record> rows = new HashMap<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
            for (Record record : records) {
                rows.put((Long) record.getField("id"), record);
            }
        }
        Assertions.assertEquals(3, rows.size());
        Assertions.assertEquals(
                LocalDateTime.parse("2024-03-10T10:00:00.123456"),
                rows.get(1L).getField("ts_micros"));
        ChronoUnit precision = nanosecondRows ? ChronoUnit.NANOS : ChronoUnit.MICROS;
        ChronoUnit offsetPrecision = nanosecondRows ? ChronoUnit.NANOS : ChronoUnit.MILLIS;
        Assertions.assertEquals(
                LocalDateTime.parse("2024-03-10T10:00:00.123456789").truncatedTo(precision),
                rows.get(1L).getField("ts_nanos"));
        Assertions.assertEquals(
                LocalDateTime.parse("2024-03-11T10:00:00.000000001").truncatedTo(precision),
                rows.get(2L).getField("ts_nanos"));
        Assertions.assertEquals(
                LocalDateTime.parse("2024-03-12T23:59:59.999999999").truncatedTo(precision),
                rows.get(3L).getField("ts_nanos"));
        Assertions.assertEquals(
                OffsetDateTime.parse("2024-03-10T10:00:00.123456789+08:00")
                        .truncatedTo(offsetPrecision)
                        .toInstant(),
                ((OffsetDateTime) rows.get(1L).getField("tstz_nanos")).toInstant());
        Assertions.assertEquals(
                OffsetDateTime.parse("2024-03-12T23:59:59.999999999-05:00")
                        .truncatedTo(offsetPrecision)
                        .toInstant(),
                ((OffsetDateTime) rows.get(3L).getField("tstz_nanos")).toInstant());
    }

    /**
     * Starts each run from an empty table location. The directories are created from inside the job
     * container, see {@link #extendedFactory}.
     */
    private void resetTableDirectory(TestContainer container, String table)
            throws IOException, InterruptedException {
        String tableDir = CATALOG_DIR + "seatunnel_namespace/" + table;
        container.executeExtraCommands(
                c ->
                        c.execInContainer(
                                "sh",
                                "-c",
                                "rm -rf "
                                        + tableDir
                                        + " && mkdir -p "
                                        + tableDir
                                        + "/data "
                                        + tableDir
                                        + "/metadata && chmod -R 777 "
                                        + CATALOG_DIR));
    }

    private static boolean containsPartitionField(
            Table table, List<PartitionField> fields, String sourceFieldName, String transform) {
        return fields.stream()
                .anyMatch(
                        field ->
                                sourceFieldName.equals(
                                                table.schema().findField(field.sourceId()).name())
                                        && transform.equals(field.transform().toString()));
    }

    private Table loadIcebergTableObject() throws IOException {
        return loadIcebergTableObject("iceberg_sink_table");
    }

    private Table loadIcebergTableObject(String tableName) throws IOException {
        Map<String, Object> configs = new HashMap<>();
        Map<String, Object> catalogProps = new HashMap<>();
        catalogProps.put("type", HADOOP.getType());
        catalogProps.put("warehouse", "file://" + CATALOG_DIR);
        // Parquet 1.16 needs Hadoop 3.3+ to open Hadoop files itself, see HadoopStreamFileIO
        catalogProps.put("io-impl", HadoopStreamFileIO.class.getName());
        configs.put(IcebergCommonOptions.KEY_CATALOG_NAME.key(), "seatunnel_test");
        configs.put(IcebergCommonOptions.KEY_NAMESPACE.key(), "seatunnel_namespace");
        configs.put(IcebergCommonOptions.KEY_TABLE.key(), tableName);
        configs.put(IcebergCommonOptions.CATALOG_PROPS.key(), catalogProps);
        try (IcebergTableLoader tableLoader =
                IcebergTableLoader.create(
                        new IcebergSourceConfig(ReadonlyConfig.fromMap(configs)))) {
            tableLoader.open();
            return tableLoader.loadTable();
        }
    }

    private List<Record> loadIcebergTable() {
        List<Record> results = new ArrayList<>();
        try {
            Table table = loadIcebergTableObject();
            try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
                for (Record record : records) {
                    results.add(record);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return results;
    }
}
