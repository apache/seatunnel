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

package org.apache.seatunnel.e2e.connector.paimon;

import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.core.starter.utils.CompressionUtils;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.lang3.StringUtils;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.DateTimeUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.given;

@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason =
                "Spark and Flink engine can not auto create paimon table on worker node in local file(e.g flink tm) by savemode feature which can lead error")
@Slf4j
public class PaimonSinkCDCIT extends TestSuiteBase implements TestResource {

    private static String CATALOG_ROOT_DIR = "/tmp/";
    private static final String NAMESPACE = "paimon";
    private static final String NAMESPACE_TAR = "paimon.tar.gz";
    private static final String CATALOG_DIR = CATALOG_ROOT_DIR + NAMESPACE + "/";
    private static final String TARGET_TABLE = "st_test";
    private static final String FAKE_TABLE1 = "FakeTable1";
    private static final String FAKE_DATABASE1 = "FakeDatabase1";
    private static final String FAKE_TABLE2 = "FakeTable1";
    private static final String FAKE_DATABASE2 = "FakeDatabase2";
    private String CATALOG_ROOT_DIR_WIN = "C:/Users/";
    private String CATALOG_DIR_WIN = CATALOG_ROOT_DIR_WIN + NAMESPACE + "/";
    private boolean isWindows;
    private boolean changeLogEnabled = false;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        this.isWindows =
                System.getProperties().getProperty("os.name").toUpperCase().contains("WINDOWS");
        CATALOG_ROOT_DIR_WIN = CATALOG_ROOT_DIR_WIN + System.getProperty("user.name") + "/tmp/";
        CATALOG_DIR_WIN = CATALOG_ROOT_DIR_WIN + NAMESPACE + "/";
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {}

    @TestTemplate
    public void testSinkWithMultipleInBatchMode(TestContainer container) throws Exception {
        Container.ExecResult execOneResult =
                container.executeJob("/fake_cdc_sink_paimon_case9.conf");
        Assertions.assertEquals(0, execOneResult.getExitCode());

        Container.ExecResult execTwoResult =
                container.executeJob("/fake_cdc_sink_paimon_case10.conf");
        Assertions.assertEquals(0, execTwoResult.getExitCode());

        given().ignoreExceptions()
                .await()
                .atLeast(100L, TimeUnit.MILLISECONDS)
                .atMost(30L, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            // copy paimon to local
                            container.executeExtraCommands(containerExtendedFactory);
                            List<PaimonRecord> paimonRecords =
                                    loadPaimonData("seatunnel_namespace9", TARGET_TABLE);
                            Assertions.assertEquals(3, paimonRecords.size());
                            paimonRecords.forEach(
                                    paimonRecord -> {
                                        if (paimonRecord.getPkId() == 1) {
                                            Assertions.assertEquals("A", paimonRecord.getName());
                                        }
                                        if (paimonRecord.getPkId() == 2
                                                || paimonRecord.getPkId() == 3) {
                                            Assertions.assertEquals("CCC", paimonRecord.getName());
                                        }
                                    });
                        });
    }

    @TestTemplate
    public void testFakeCDCSinkPaimon(TestContainer container) throws Exception {
        Container.ExecResult execResult = container.executeJob("/fake_cdc_sink_paimon_case1.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        given().ignoreExceptions()
                .await()
                .atLeast(100L, TimeUnit.MILLISECONDS)
                .atMost(30L, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            // copy paimon to local
                            container.executeExtraCommands(containerExtendedFactory);
                            List<PaimonRecord> paimonRecords =
                                    loadPaimonData("seatunnel_namespace1", TARGET_TABLE);
                            Assertions.assertEquals(2, paimonRecords.size());
                            paimonRecords.forEach(
                                    paimonRecord -> {
                                        if (paimonRecord.getPkId() == 1) {
                                            Assertions.assertEquals("A_1", paimonRecord.getName());
                                        }
                                        if (paimonRecord.getPkId() == 3) {
                                            Assertions.assertEquals("C", paimonRecord.getName());
                                        }
                                    });
                        });
    }

    @TestTemplate
    public void testSinkWithIncompatibleSchema(TestContainer container) throws Exception {
        Container.ExecResult execResult = container.executeJob("/fake_cdc_sink_paimon_case1.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Container.ExecResult errResult =
                container.executeJob("/fake_cdc_sink_paimon_case1_with_error_schema.conf");
        Assertions.assertEquals(1, errResult.getExitCode());
        Assertions.assertTrue(
                errResult
                        .getStderr()
                        .contains(
                                "[Paimon: The source filed with schema 'name INT', except filed schema of sink is '`name` INT'; but the filed in sink table which actual schema is '`name` STRING'. Please check schema of sink table.]"));
    }

    @TestTemplate
    public void testFakeMultipleTableSinkPaimon(TestContainer container) throws Exception {
        Container.ExecResult execResult = container.executeJob("/fake_cdc_sink_paimon_case2.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        given().ignoreExceptions()
                .await()
                .atLeast(100L, TimeUnit.MILLISECONDS)
                .atMost(30L, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            // copy paimon to local
                            container.executeExtraCommands(containerExtendedFactory);
                            // Check FakeDatabase1.FakeTable1
                            List<PaimonRecord> fake1PaimonRecords =
                                    loadPaimonData(FAKE_DATABASE1, FAKE_TABLE1);
                            Assertions.assertEquals(2, fake1PaimonRecords.size());
                            fake1PaimonRecords.forEach(
                                    paimonRecord -> {
                                        if (paimonRecord.getPkId() == 1) {
                                            Assertions.assertEquals("A_1", paimonRecord.getName());
                                        }
                                        if (paimonRecord.getPkId() == 3) {
                                            Assertions.assertEquals("C", paimonRecord.getName());
                                        }
                                    });
                            // Check FakeDatabase2.FakeTable1
                            List<PaimonRecord> fake2PaimonRecords =
                                    loadPaimonData(FAKE_DATABASE2, FAKE_TABLE2);
                            Assertions.assertEquals(2, fake2PaimonRecords.size());
                            fake2PaimonRecords.forEach(
                                    paimonRecord -> {
                                        if (paimonRecord.getPkId() == 100) {
                                            Assertions.assertEquals(
                                                    "A_100", paimonRecord.getName());
                                        }
                                        if (paimonRecord.getPkId() == 200) {
                                            Assertions.assertEquals("C", paimonRecord.getName());
                                        }
                                    });
                        });
    }

    @TestTemplate
    public void testFakeCDCSinkPaimonWithMultipleBucket(TestContainer container) throws Exception {
        Container.ExecResult execResult = container.executeJob("/fake_cdc_sink_paimon_case3.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        given().ignoreExceptions()
                .await()
                .atLeast(100L, TimeUnit.MILLISECONDS)
                .atMost(30L, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            // copy paimon to local
                            container.executeExtraCommands(containerExtendedFactory);
                            Table table = getTable("seatunnel_namespace3", TARGET_TABLE);
                            String bucket = table.options().get(CoreOptions.BUCKET.key());
                            Assertions.assertTrue(StringUtils.isNoneBlank(bucket));
                            Assertions.assertEquals(2, Integer.valueOf(bucket));
                            List<PaimonRecord> paimonRecords =
                                    loadPaimonData("seatunnel_namespace3", TARGET_TABLE);
                            Assertions.assertEquals(2, paimonRecords.size());
                            paimonRecords.forEach(
                                    paimonRecord -> {
                                        if (paimonRecord.getPkId() == 1) {
                                            Assertions.assertEquals("A_1", paimonRecord.getName());
                                        }
                                        if (paimonRecord.getPkId() == 3) {
                                            Assertions.assertEquals("C", paimonRecord.getName());
                                        }
                                    });
                        });
    }

    @TestTemplate
    public void testFakeCDCSinkPaimonWithPartition(TestContainer container) throws Exception {
        Container.ExecResult execResult = container.executeJob("/fake_cdc_sink_paimon_case4.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        given().ignoreExceptions()
                .await()
                .atLeast(100L, TimeUnit.MILLISECONDS)
                .atMost(30L, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            // copy paimon to local
                            container.executeExtraCommands(containerExtendedFactory);
                            Table table = getTable("seatunnel_namespace4", TARGET_TABLE);
                            List<String> partitionKeys = table.partitionKeys();
                            List<String> primaryKeys = table.primaryKeys();
                            Assertions.assertTrue(partitionKeys.contains("dt"));
                            Assertions.assertEquals(2, primaryKeys.size());
                            Assertions.assertTrue(primaryKeys.contains("pk_id"));
                            Assertions.assertTrue(primaryKeys.contains("dt"));
                            ReadBuilder readBuilder = table.newReadBuilder();
                            TableScan.Plan plan = readBuilder.newScan().plan();
                            TableRead tableRead = readBuilder.newRead();
                            List<PaimonRecord> result = new ArrayList<>();
                            try (RecordReader<InternalRow> reader = tableRead.createReader(plan)) {
                                reader.forEachRemaining(
                                        row -> {
                                            result.add(
                                                    new PaimonRecord(
                                                            row.getLong(0),
                                                            row.getString(1).toString(),
                                                            row.getString(2).toString()));
                                            log.info(
                                                    "key_id:"
                                                            + row.getLong(0)
                                                            + ", name:"
                                                            + row.getString(1)
                                                            + ", dt:"
                                                            + row.getString(2));
                                        });
                            }
                            Assertions.assertEquals(2, result.size());
                            List<PaimonRecord> filterRecords =
                                    result.stream()
                                            .filter(record -> record.pkId == 1)
                                            .collect(Collectors.toList());
                            Assertions.assertEquals(1, filterRecords.size());
                            PaimonRecord paimonRecord = filterRecords.get(0);
                            Assertions.assertEquals("A_1", paimonRecord.getName());
                            Assertions.assertEquals("2024-03-20", paimonRecord.getDt());
                        });
    }

    @TestTemplate
    public void testFakeCDCSinkPaimonWithParquet(TestContainer container) throws Exception {
        Container.ExecResult execResult = container.executeJob("/fake_cdc_sink_paimon_case5.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        given().ignoreExceptions()
                .await()
                .atLeast(100L, TimeUnit.MILLISECONDS)
                .atMost(30L, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            // copy paimon to local
                            container.executeExtraCommands(containerExtendedFactory);
                            Table table = getTable("seatunnel_namespace5", TARGET_TABLE);
                            String fileFormat = table.options().get(CoreOptions.FILE_FORMAT.key());
                            Assertions.assertTrue(StringUtils.isNoneBlank(fileFormat));
                            Assertions.assertEquals("parquet", fileFormat);
                            List<PaimonRecord> paimonRecords =
                                    loadPaimonData("seatunnel_namespace5", TARGET_TABLE);
                            Assertions.assertEquals(2, paimonRecords.size());
                            paimonRecords.forEach(
                                    paimonRecord -> {
                                        if (paimonRecord.getPkId() == 1) {
                                            Assertions.assertEquals("A_1", paimonRecord.getName());
                                        }
                                        if (paimonRecord.getPkId() == 3) {
                                            Assertions.assertEquals("C", paimonRecord.getName());
                                        }
                                    });
                        });
    }

    @TestTemplate
    public void testFakeCDCSinkPaimonWithAvro(TestContainer container) throws Exception {
        Container.ExecResult execResult = container.executeJob("/fake_cdc_sink_paimon_case6.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        given().ignoreExceptions()
                .await()
                .atLeast(100L, TimeUnit.MILLISECONDS)
                .atMost(30L, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            // copy paimon to local
                            container.executeExtraCommands(containerExtendedFactory);
                            Table table = getTable("seatunnel_namespace6", TARGET_TABLE);
                            String fileFormat = table.options().get(CoreOptions.FILE_FORMAT.key());
                            Assertions.assertTrue(StringUtils.isNoneBlank(fileFormat));
                            Assertions.assertEquals("avro", fileFormat);
                            List<PaimonRecord> paimonRecords =
                                    loadPaimonData("seatunnel_namespace6", TARGET_TABLE);
                            Assertions.assertEquals(2, paimonRecords.size());
                            paimonRecords.forEach(
                                    paimonRecord -> {
                                        if (paimonRecord.getPkId() == 1) {
                                            Assertions.assertEquals("A_1", paimonRecord.getName());
                                        }
                                        if (paimonRecord.getPkId() == 3) {
                                            Assertions.assertEquals("C", paimonRecord.getName());
                                        }
                                    });
                        });
    }

    @TestTemplate
    public void testFakeCDCSinkPaimonWithTimestampNAndRead(TestContainer container)
            throws Exception {
        Container.ExecResult execResult = container.executeJob("/fake_cdc_sink_paimon_case7.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        given().ignoreExceptions()
                .await()
                .atLeast(100L, TimeUnit.MILLISECONDS)
                .atMost(30L, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            // copy paimon to local
                            container.executeExtraCommands(containerExtendedFactory);
                            FileStoreTable table =
                                    (FileStoreTable) getTable("seatunnel_namespace7", TARGET_TABLE);
                            List<DataField> fields = table.schema().fields();
                            for (DataField field : fields) {
                                if (field.name().equalsIgnoreCase("one_time")) {
                                    Assertions.assertEquals(
                                            0, ((TimestampType) field.type()).getPrecision());
                                }
                                if (field.name().equalsIgnoreCase("two_time")) {
                                    Assertions.assertEquals(
                                            3, ((TimestampType) field.type()).getPrecision());
                                }
                                if (field.name().equalsIgnoreCase("three_time")) {
                                    Assertions.assertEquals(
                                            6, ((TimestampType) field.type()).getPrecision());
                                }
                                if (field.name().equalsIgnoreCase("four_time")) {
                                    Assertions.assertEquals(
                                            9, ((TimestampType) field.type()).getPrecision());
                                }
                            }
                            ReadBuilder readBuilder = table.newReadBuilder();
                            TableScan.Plan plan = readBuilder.newScan().plan();
                            TableRead tableRead = readBuilder.newRead();
                            List<PaimonRecord> result = new ArrayList<>();
                            try (RecordReader<InternalRow> reader = tableRead.createReader(plan)) {
                                reader.forEachRemaining(
                                        row ->
                                                result.add(
                                                        new PaimonRecord(
                                                                row.getLong(0),
                                                                row.getString(1).toString(),
                                                                row.getTimestamp(2, 0),
                                                                row.getTimestamp(3, 3),
                                                                row.getTimestamp(4, 6),
                                                                row.getTimestamp(5, 9))));
                            }
                            Assertions.assertEquals(2, result.size());
                            for (PaimonRecord paimonRecord : result) {
                                Assertions.assertEquals(
                                        paimonRecord.oneTime.toString(), "2024-03-10T10:00:12");
                                Assertions.assertEquals(
                                        paimonRecord.twoTime.toString(), "2024-03-10T10:00:00.123");
                                Assertions.assertEquals(
                                        paimonRecord.threeTime.toString(),
                                        "2024-03-10T10:00:00.123456");
                                Assertions.assertEquals(
                                        paimonRecord.fourTime.toString(),
                                        "2024-03-10T10:00:00.123456789");
                            }
                        });

        Container.ExecResult readResult =
                container.executeJob("/paimon_to_assert_with_timestampN.conf");
        Assertions.assertEquals(0, readResult.getExitCode());
    }

    @TestTemplate
    public void testFakeSinkPaimonWithDate(TestContainer container) throws Exception {
        Container.ExecResult execResult = container.executeJob("/fake_cdc_sink_paimon_case8.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        given().ignoreExceptions()
                .await()
                .atLeast(100L, TimeUnit.MILLISECONDS)
                .atMost(30L, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            // copy paimon to local
                            container.executeExtraCommands(containerExtendedFactory);
                            FileStoreTable table =
                                    (FileStoreTable) getTable("seatunnel_namespace8", TARGET_TABLE);
                            List<DataField> fields = table.schema().fields();
                            for (DataField field : fields) {
                                if (field.name().equalsIgnoreCase("one_date")) {
                                    Assertions.assertTrue(field.type() instanceof DateType);
                                }
                            }
                            ReadBuilder readBuilder = table.newReadBuilder();
                            TableScan.Plan plan = readBuilder.newScan().plan();
                            TableRead tableRead = readBuilder.newRead();
                            List<PaimonRecord> result = new ArrayList<>();
                            try (RecordReader<InternalRow> reader = tableRead.createReader(plan)) {
                                reader.forEachRemaining(
                                        row ->
                                                result.add(
                                                        new PaimonRecord(
                                                                row.getLong(0),
                                                                row.getString(1).toString(),
                                                                row.getInt(2))));
                            }
                            Assertions.assertEquals(3, result.size());
                            for (PaimonRecord paimonRecord : result) {
                                if (paimonRecord.getPkId() == 1) {
                                    Assertions.assertEquals(
                                            paimonRecord.oneDate,
                                            DateTimeUtils.toInternal(
                                                    LocalDate.parse("2024-03-20")));
                                } else {
                                    Assertions.assertEquals(
                                            paimonRecord.oneDate,
                                            DateTimeUtils.toInternal(
                                                    LocalDate.parse("2024-03-10")));
                                }
                            }
                        });
    }

    @TestTemplate
    public void testFakeSinkPaimonWithFullTypeAndReadWithFilter(TestContainer container)
            throws Exception {
        Container.ExecResult writeResult =
                container.executeJob("/fake_to_paimon_with_full_type.conf");
        Assertions.assertEquals(0, writeResult.getExitCode());
        Container.ExecResult readResult =
                container.executeJob("/paimon_to_assert_with_filter1.conf");
        Assertions.assertEquals(0, readResult.getExitCode());
        Container.ExecResult readResult2 =
                container.executeJob("/paimon_to_assert_with_filter2.conf");
        Assertions.assertEquals(0, readResult2.getExitCode());
        Container.ExecResult readResult3 =
                container.executeJob("/paimon_to_assert_with_filter3.conf");
        Assertions.assertEquals(0, readResult3.getExitCode());
        Container.ExecResult readResult4 =
                container.executeJob("/paimon_to_assert_with_filter4.conf");
        Assertions.assertEquals(0, readResult4.getExitCode());
    }

    @TestTemplate
    public void testSinkPaimonTruncateTable(TestContainer container) throws Exception {
        Container.ExecResult writeResult =
                container.executeJob("/fake_sink_paimon_truncate_with_local_case1.conf");
        Assertions.assertEquals(0, writeResult.getExitCode());
        Container.ExecResult readResult =
                container.executeJob("/fake_sink_paimon_truncate_with_local_case2.conf");
        Assertions.assertEquals(0, readResult.getExitCode());
        given().ignoreExceptions()
                .await()
                .atLeast(100L, TimeUnit.MILLISECONDS)
                .atMost(30L, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            // copy paimon to local
                            container.executeExtraCommands(containerExtendedFactory);
                            List<PaimonRecord> paimonRecords =
                                    loadPaimonData("seatunnel_namespace10", TARGET_TABLE);
                            Assertions.assertEquals(2, paimonRecords.size());
                            paimonRecords.forEach(
                                    paimonRecord -> {
                                        if (paimonRecord.getPkId() == 1) {
                                            Assertions.assertEquals("Aa", paimonRecord.getName());
                                        }
                                        if (paimonRecord.getPkId() == 2) {
                                            Assertions.assertEquals("Bb", paimonRecord.getName());
                                        }
                                        Assertions.assertEquals(200, paimonRecord.getScore());
                                    });
                            List<Long> ids =
                                    paimonRecords.stream()
                                            .map(PaimonRecord::getPkId)
                                            .collect(Collectors.toList());
                            Assertions.assertFalse(ids.contains(3L));
                        });
    }

    @TestTemplate
    public void testChangelogLookup(TestContainer container) throws Exception {
        // create Piamon table (changelog-producer=lookup)
        Container.ExecResult writeResult =
                container.executeJob("/changelog_fake_cdc_sink_paimon_case1_ddl.conf");
        Assertions.assertEquals(0, writeResult.getExitCode());
        TimeUnit.SECONDS.sleep(20);
        String[] jobIds =
                new String[] {
                    JobIdGenerator.newJobId(), JobIdGenerator.newJobId(), JobIdGenerator.newJobId()
                };
        log.info("jobIds: {}", Arrays.toString(jobIds));
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        // read changelog and write to append only paimon table
        futures.add(
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                container.executeJob("/changelog_paimon_to_paimon.conf", jobIds[0]);
                            } catch (Exception e) {
                                throw new SeaTunnelException(e);
                            }
                        }));
        TimeUnit.SECONDS.sleep(10);
        // dml: insert data
        futures.add(
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                container.executeJob(
                                        "/changelog_fake_cdc_sink_paimon_case1_insert_data.conf",
                                        jobIds[1]);
                            } catch (Exception e) {
                                throw new SeaTunnelException(e);
                            }
                        }));
        // dml: update and delete data
        TimeUnit.SECONDS.sleep(10);
        futures.add(
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                container.executeJob(
                                        "/changelog_fake_cdc_sink_paimon_case1_update_data.conf",
                                        jobIds[2]);
                            } catch (Exception e) {
                                throw new SeaTunnelException(e);
                            }
                        }));
        // stream job running 30 seconds
        TimeUnit.SECONDS.sleep(30);
        // cancel stream job
        container.cancelJob(jobIds[1]);
        container.cancelJob(jobIds[2]);
        container.cancelJob(jobIds[0]);
        changeLogEnabled = true;
        TimeUnit.SECONDS.sleep(10);
        // copy paimon to local
        container.executeExtraCommands(containerExtendedFactory);
        List<PaimonRecord> paimonRecords1 = loadPaimonData("seatunnel_namespace", "st_test_sink");
        List<String> actual1 =
                paimonRecords1.stream()
                        .map(PaimonRecord::toChangeLogLookUp)
                        .collect(Collectors.toList());
        log.info("paimon records: {}", actual1);
        Assertions.assertEquals(8, actual1.size());
        Assertions.assertEquals(
                Arrays.asList(
                        "[+I, 1, A, 100, +I]",
                        "[+I, 2, B, 100, +I]",
                        "[+I, 3, C, 100, +I]",
                        "[+I, 1, A, 100, -U]",
                        "[+I, 1, Aa, 200, +U]",
                        "[+I, 2, B, 100, -U]",
                        "[+I, 2, Bb, 90, +U]",
                        "[+I, 3, C, 100, -D]"),
                actual1);
        List<PaimonRecord> paimonRecords2 = loadPaimonData("seatunnel_namespace", "st_test_lookup");
        List<String> actual2 =
                paimonRecords2.stream()
                        .map(PaimonRecord::toChangeLogFull)
                        .collect(Collectors.toList());
        log.info("paimon records: {}", actual2);
        Assertions.assertEquals(2, actual2.size());
        Assertions.assertEquals(Arrays.asList("[+U, 1, Aa, 200]", "[+I, 2, Bb, 90]"), actual2);
        changeLogEnabled = false;
        futures.forEach(future -> future.cancel(true));
    }

    @TestTemplate
    public void testChangelogFullCompaction(TestContainer container) throws Exception {
        String jobId = JobIdGenerator.newJobId();
        log.info("jobId: {}", jobId);
        CompletableFuture<Void> voidCompletableFuture =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                container.executeJob(
                                        "/changelog_fake_cdc_sink_paimon_case2.conf", jobId);
                            } catch (Exception e) {
                                throw new SeaTunnelException(e);
                            }
                        });
        // stream job running 20 seconds
        TimeUnit.SECONDS.sleep(20);
        changeLogEnabled = true;
        // cancel stream job
        container.cancelJob(jobId);
        TimeUnit.SECONDS.sleep(5);
        // copy paimon to local
        container.executeExtraCommands(containerExtendedFactory);
        List<PaimonRecord> paimonRecords = loadPaimonData("seatunnel_namespace", "st_test_full");
        List<String> actual =
                paimonRecords.stream()
                        .map(PaimonRecord::toChangeLogFull)
                        .collect(Collectors.toList());
        log.info("paimon records: {}", actual);
        Assertions.assertEquals(2, actual.size());
        Assertions.assertEquals(Arrays.asList("[+U, 1, Aa, 200]", "[+I, 2, Bb, 90]"), actual);
        changeLogEnabled = false;
        voidCompletableFuture.cancel(true);
    }

    protected final ContainerExtendedFactory containerExtendedFactory =
            container -> {
                if (isWindows) {
                    FileUtils.deleteFile(CATALOG_ROOT_DIR_WIN + NAMESPACE_TAR);
                    FileUtils.deleteFile(CATALOG_ROOT_DIR_WIN + "paimon.tar");
                    FileUtils.createNewDir(CATALOG_ROOT_DIR_WIN);
                } else {
                    FileUtils.deleteFile(CATALOG_ROOT_DIR + NAMESPACE_TAR);
                    FileUtils.createNewDir(CATALOG_DIR);
                }

                container.execInContainer(
                        "sh",
                        "-c",
                        "cd "
                                + CATALOG_ROOT_DIR
                                + " && tar -czvf "
                                + NAMESPACE_TAR
                                + " "
                                + NAMESPACE);
                container.copyFileFromContainer(
                        CATALOG_ROOT_DIR + NAMESPACE_TAR,
                        (isWindows ? CATALOG_ROOT_DIR_WIN : CATALOG_ROOT_DIR) + NAMESPACE_TAR);
                if (isWindows) {
                    extractFilesWin();
                } else {
                    extractFiles();
                }
            };

    private void extractFiles() {
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command(
                "sh", "-c", "cd " + CATALOG_ROOT_DIR + " && tar -zxvf " + NAMESPACE_TAR);
        try {
            Process process = processBuilder.start();
            // wait command completed
            int exitCode = process.waitFor();
            if (exitCode == 0) {
                log.info("Extract files successful.");
            } else {
                log.error("Extract files failed with exit code " + exitCode);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void extractFilesWin() {
        try {
            CompressionUtils.unGzip(
                    new File(CATALOG_ROOT_DIR_WIN + NAMESPACE_TAR), new File(CATALOG_ROOT_DIR_WIN));
            CompressionUtils.unTar(
                    new File(CATALOG_ROOT_DIR_WIN + "paimon.tar"), new File(CATALOG_ROOT_DIR_WIN));
        } catch (IOException | ArchiveException e) {
            throw new RuntimeException(e);
        }
    }

    private List<PaimonRecord> loadPaimonData(String dbName, String tbName) throws Exception {
        FileStoreTable table = (FileStoreTable) getTable(dbName, tbName);
        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan.Plan plan = readBuilder.newScan().plan();
        TableRead tableRead = readBuilder.newRead();
        List<PaimonRecord> result = new ArrayList<>();
        log.info(
                "====================================Paimon data===========================================");
        log.info(
                "==========================================================================================");
        log.info(
                "==========================================================================================");
        try (RecordReader<InternalRow> reader = tableRead.createReader(plan)) {
            reader.forEachRemaining(
                    row -> {
                        PaimonRecord paimonRecord;
                        if (changeLogEnabled) {
                            paimonRecord =
                                    new PaimonRecord(
                                            row.getRowKind(),
                                            row.getLong(0),
                                            row.getString(1).toString());
                        } else {
                            paimonRecord =
                                    new PaimonRecord(row.getLong(0), row.getString(1).toString());
                        }
                        if (table.schema().fieldNames().contains("score")) {
                            paimonRecord.setScore(row.getInt(2));
                        }
                        if (table.schema().fieldNames().contains("op")) {
                            paimonRecord.setOp(row.getString(3).toString());
                        }
                        result.add(paimonRecord);
                        log.info(
                                "rowKind:"
                                        + row.getRowKind().shortString()
                                        + ", key_id:"
                                        + row.getLong(0)
                                        + ", name:"
                                        + row.getString(1));
                    });
        }
        log.info(
                "==========================================================================================");
        log.info(
                "==========================================================================================");
        log.info(
                "==========================================================================================");
        return result;
    }

    protected Table getTable(String dbName, String tbName) {
        try {
            return getCatalog().getTable(getIdentifier(dbName, tbName));
        } catch (Catalog.TableNotExistException e) {
            // do something
            throw new RuntimeException("table not exist");
        }
    }

    private Identifier getIdentifier(String dbName, String tbName) {
        return Identifier.create(dbName, tbName);
    }

    private Catalog getCatalog() {
        Options options = new Options();
        if (isWindows) {
            options.set("warehouse", CATALOG_DIR_WIN);
        } else {
            options.set("warehouse", "file://" + CATALOG_DIR);
        }
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        return catalog;
    }
}
