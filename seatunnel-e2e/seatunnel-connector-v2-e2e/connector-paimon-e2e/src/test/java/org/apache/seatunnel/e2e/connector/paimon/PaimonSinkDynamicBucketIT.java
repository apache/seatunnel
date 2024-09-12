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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.connectors.seatunnel.paimon.catalog.PaimonCatalogLoader;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonSinkConfig;
import org.apache.seatunnel.core.starter.utils.CompressionUtils;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.crosspartition.IndexBootstrap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.TimestampType;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.given;

@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason =
                "Spark and Flink engine can not auto create paimon table on worker node in local file(e.g flink tm) by savemode feature which can lead error")
@Slf4j
public class PaimonSinkDynamicBucketIT extends TestSuiteBase implements TestResource {

    private static String CATALOG_ROOT_DIR = "/tmp/";
    private static final String NAMESPACE = "paimon";
    private static final String NAMESPACE_TAR = "paimon.tar.gz";
    private static final String CATALOG_DIR = CATALOG_ROOT_DIR + NAMESPACE + "/";
    private String CATALOG_ROOT_DIR_WIN = "C:/Users/";
    private String CATALOG_DIR_WIN = CATALOG_ROOT_DIR_WIN + NAMESPACE + "/";
    private boolean isWindows;

    private Map<String, Object> PAIMON_SINK_PROPERTIES;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        this.isWindows =
                System.getProperties().getProperty("os.name").toUpperCase().contains("WINDOWS");
        CATALOG_ROOT_DIR_WIN = CATALOG_ROOT_DIR_WIN + System.getProperty("user.name") + "/tmp/";
        CATALOG_DIR_WIN = CATALOG_ROOT_DIR_WIN + NAMESPACE + "/";
        Map<String, Object> map = new HashMap<>();
        map.put("warehouse", "hdfs:///tmp/paimon");
        map.put("database", "default");
        map.put("table", "st_test5");
        Map<String, Object> paimonHadoopConf = new HashMap<>();
        paimonHadoopConf.put("fs.defaultFS", "hdfs://nameservice1");
        paimonHadoopConf.put("dfs.nameservices", "nameservice1");
        paimonHadoopConf.put("dfs.ha.namenodes.nameservice1", "nn1,nn2");
        paimonHadoopConf.put("dfs.namenode.rpc-address.nameservice1.nn1", "dp06:8020");
        paimonHadoopConf.put("dfs.namenode.rpc-address.nameservice1.nn2", "dp07:8020");
        paimonHadoopConf.put(
                "dfs.client.failover.proxy.provider.nameservice1",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        paimonHadoopConf.put("dfs.client.use.datanode.hostname", "true");
        map.put("paimon.hadoop.conf", paimonHadoopConf);
        this.PAIMON_SINK_PROPERTIES = map;
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {}

    @TestTemplate
    public void testWriteAndReadPaimon(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult textWriteResult =
                container.executeJob("/fake_to_dynamic_bucket_paimon_case1.conf");
        Assertions.assertEquals(0, textWriteResult.getExitCode());
        Container.ExecResult readResult = container.executeJob("/paimon_to_assert.conf");
        Assertions.assertEquals(0, readResult.getExitCode());
        Container.ExecResult readProjectionResult =
                container.executeJob("/paimon_projection_to_assert.conf");
        Assertions.assertEquals(0, readProjectionResult.getExitCode());
    }

    @TestTemplate
    public void testBucketCount(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult textWriteResult =
                container.executeJob("/fake_to_dynamic_bucket_paimon_case2.conf");
        Assertions.assertEquals(0, textWriteResult.getExitCode());
        given().ignoreExceptions()
                .await()
                .atLeast(100L, TimeUnit.MILLISECONDS)
                .atMost(30L, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            // copy paimon to local
                            container.executeExtraCommands(containerExtendedFactory);
                            FileStoreTable table =
                                    (FileStoreTable) getTable("default", "st_test_2");
                            IndexBootstrap indexBootstrap = new IndexBootstrap(table);
                            List<String> fieldNames =
                                    IndexBootstrap.bootstrapType(table.schema()).getFieldNames();
                            int bucketIndexOf = fieldNames.indexOf("_BUCKET");
                            Set<Integer> bucketList = new HashSet<>();
                            try (RecordReader<InternalRow> recordReader =
                                    indexBootstrap.bootstrap(1, 0)) {
                                recordReader.forEachRemaining(
                                        row -> bucketList.add(row.getInt(bucketIndexOf)));
                            }
                            Assertions.assertEquals(2, bucketList.size());
                        });
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SEATUNNEL})
    @Disabled(
            "Spark and Flink engine can not auto create paimon table on worker node in local file, this e2e case work on hdfs environment, please set up your own HDFS environment in the test case file and the below setup")
    public void testPaimonBucketCountOnSparkAndFlink(TestContainer container)
            throws IOException, InterruptedException, Catalog.TableNotExistException {
        PaimonSinkConfig paimonSinkConfig =
                new PaimonSinkConfig(ReadonlyConfig.fromMap(PAIMON_SINK_PROPERTIES));
        PaimonCatalogLoader paimonCatalogLoader = new PaimonCatalogLoader(paimonSinkConfig);
        Catalog catalog = paimonCatalogLoader.loadCatalog();
        Identifier identifier = Identifier.create("default", "st_test_5");
        if (catalog.tableExists(identifier)) {
            catalog.dropTable(identifier, true);
        }
        Container.ExecResult textWriteResult =
                container.executeJob("/fake_to_dynamic_bucket_paimon_case5.conf");
        Assertions.assertEquals(0, textWriteResult.getExitCode());
        given().ignoreExceptions()
                .await()
                .atLeast(100L, TimeUnit.MILLISECONDS)
                .atMost(30L, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
                            IndexBootstrap indexBootstrap = new IndexBootstrap(table);
                            List<String> fieldNames =
                                    IndexBootstrap.bootstrapType(table.schema()).getFieldNames();
                            int bucketIndexOf = fieldNames.indexOf("_BUCKET");
                            Set<Integer> bucketList = new HashSet<>();
                            try (RecordReader<InternalRow> recordReader =
                                    indexBootstrap.bootstrap(1, 0)) {
                                recordReader.forEachRemaining(
                                        row -> bucketList.add(row.getInt(bucketIndexOf)));
                            }
                            Assertions.assertEquals(4, bucketList.size());
                        });
    }

    @TestTemplate
    public void testParallelismBucketCount(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult textWriteResult =
                container.executeJob("/fake_to_dynamic_bucket_paimon_case3.conf");
        Assertions.assertEquals(0, textWriteResult.getExitCode());
        given().ignoreExceptions()
                .await()
                .atLeast(100L, TimeUnit.MILLISECONDS)
                .atMost(30L, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            // copy paimon to local
                            container.executeExtraCommands(containerExtendedFactory);
                            FileStoreTable table =
                                    (FileStoreTable) getTable("default", "st_test_3");
                            IndexBootstrap indexBootstrap = new IndexBootstrap(table);
                            RowPartitionKeyExtractor keyExtractor =
                                    new RowPartitionKeyExtractor(table.schema());
                            SimpleBucketIndex simpleBucketIndex =
                                    new SimpleBucketIndex(1, 0, 50000);
                            try (RecordReader<InternalRow> recordReader =
                                    indexBootstrap.bootstrap(1, 0)) {
                                recordReader.forEachRemaining(
                                        row ->
                                                simpleBucketIndex.assign(
                                                        keyExtractor
                                                                .trimmedPrimaryKey(row)
                                                                .hashCode()));
                            }
                            Assertions.assertEquals(
                                    6, simpleBucketIndex.getBucketInformation().size());
                            Assertions.assertEquals(
                                    50000, simpleBucketIndex.getBucketInformation().get(0));
                        });
    }

    @TestTemplate
    public void testCDCParallelismBucketCount(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult textWriteResult =
                container.executeJob("/fake_to_dynamic_bucket_paimon_case4.conf");
        Assertions.assertEquals(0, textWriteResult.getExitCode());
        given().ignoreExceptions()
                .await()
                .atLeast(100L, TimeUnit.MILLISECONDS)
                .atMost(120L, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            // copy paimon to local
                            container.executeExtraCommands(containerExtendedFactory);
                            FileStoreTable table =
                                    (FileStoreTable) getTable("default", "st_test_4");
                            IndexBootstrap indexBootstrap = new IndexBootstrap(table);
                            List<String> fieldNames =
                                    IndexBootstrap.bootstrapType(table.schema()).getFieldNames();
                            int bucketIndexOf = fieldNames.indexOf("_BUCKET");
                            Map<String, Integer> hashBucketMap = new HashMap<>();
                            try (RecordReader<InternalRow> recordReader =
                                    indexBootstrap.bootstrap(1, 0)) {
                                recordReader.forEachRemaining(
                                        row -> {
                                            int bucket = row.getInt(bucketIndexOf);
                                            int pkHash = row.getInt(0);
                                            hashBucketMap.put(bucket + "_" + pkHash, bucket);
                                        });
                            }
                            HashMap<Integer, Long> bucketCountMap =
                                    hashBucketMap.entrySet().stream()
                                            .collect(
                                                    Collectors.groupingBy(
                                                            Map.Entry::getValue,
                                                            HashMap::new,
                                                            Collectors.counting()));
                            Assertions.assertEquals(4, bucketCountMap.size());
                            Assertions.assertEquals(5, bucketCountMap.get(0));
                        });
    }

    @TestTemplate
    public void testCDCWrite(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult textWriteResult =
                container.executeJob("/fake_cdc_to_dynamic_bucket_paimon_case.conf");
        Assertions.assertEquals(0, textWriteResult.getExitCode());
        given().ignoreExceptions()
                .await()
                .atLeast(100L, TimeUnit.MILLISECONDS)
                .atMost(30L, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            // copy paimon to local
                            container.executeExtraCommands(containerExtendedFactory);
                            FileStoreTable table =
                                    (FileStoreTable) getTable("default", "st_test_3");
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

    protected Table getTable(String dbName, String tbName) {
        Options options = new Options();
        if (isWindows) {
            options.set("warehouse", CATALOG_DIR_WIN);
        } else {
            options.set("warehouse", "file://" + CATALOG_DIR);
        }
        try {
            Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
            return catalog.getTable(Identifier.create(dbName, tbName));
        } catch (Catalog.TableNotExistException e) {
            // do something
            throw new RuntimeException("table not exist");
        }
    }
}
