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
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.UniqueDatabase;
import org.apache.seatunnel.connectors.seatunnel.paimon.catalog.PaimonCatalogLoader;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonSinkConfig;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.crosspartition.IndexBootstrap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.seatunnel.e2e.common.container.AbstractTestContainer.HOST_VOLUME_MOUNT_PATH;
import static org.awaitility.Awaitility.given;

@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason =
                "Spark and Flink engine can not auto create paimon table on worker node in local file(e.g flink tm) by savemode feature which can lead error")
@Slf4j
public class PaimonSinkDynamicBucketIT extends TestSuiteBase implements TestResource {

    private boolean isWindows;
    private static final String NAMESPACE = "paimon";

    private Map<String, Object> PAIMON_SINK_PROPERTIES;

    private static final String MYSQL_DATABASE = "bucket";
    private static final String SOURCE_TABLE = "test_dynamic_bucket";

    private static final String MYSQL_HOST = "mysql_e2e";
    private static final String MYSQL_USER_NAME = "mysqluser";
    private static final String MYSQL_USER_PASSWORD = "mysqlpw";

    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V8_0);

    private final UniqueDatabase bucketDatabase =
            new UniqueDatabase(
                    MYSQL_CONTAINER, MYSQL_DATABASE, "mysqluser", "mysqlpw", MYSQL_DATABASE);

    private static MySqlContainer createMySqlContainer(MySqlVersion version) {
        return new MySqlContainer(version)
                .withConfigurationOverride("docker/server-gtids/my.cnf")
                .withSetupSQL("docker/setup.sql")
                .withNetwork(NETWORK)
                .withNetworkAliases(MYSQL_HOST)
                .withDatabaseName(MYSQL_DATABASE)
                .withUsername(MYSQL_USER_NAME)
                .withPassword(MYSQL_USER_PASSWORD)
                .withLogConsumer(
                        new Slf4jLogConsumer(DockerLoggerFactory.getLogger("mysql-docker-image")));
    }

    private String driverUrl() {
        return "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar";
    }

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/MySQL-CDC/lib && cd /tmp/seatunnel/plugins/MySQL-CDC/lib && wget "
                                        + driverUrl());
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        this.isWindows =
                System.getProperties().getProperty("os.name").toUpperCase().contains("WINDOWS");
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
        log.info("The second stage: Starting Mysql containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        log.info("Mysql Containers are started");
        bucketDatabase.createAndInitialize();
        log.info("Mysql ddl execution is complete");
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (MYSQL_CONTAINER != null) {
            MYSQL_CONTAINER.close();
        }
    }

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
    public void testWriteForDifferentParallelism(TestContainer container)
            throws IOException, InterruptedException, SQLException {
        // parallelism = 3
        Container.ExecResult textWriteResult1 =
                container.executeJob("/mysql_jdbc_to_dynamic_bucket_paimon_case1.conf");
        Assertions.assertEquals(0, textWriteResult1.getExitCode());
        try (Connection jdbcConnection = bucketDatabase.getJdbcConnection();
                Statement statement = jdbcConnection.createStatement()) {
            statement.executeUpdate(
                    "update bucket.test_dynamic_bucket set version = '2' where id <= 102");
            statement.executeUpdate(
                    "update bucket.test_dynamic_bucket set version = '3' where id = 105");
            statement.executeUpdate(
                    "update bucket.test_dynamic_bucket set version = '4' where id = 109");
        }
        // parallelism = 1
        Container.ExecResult textWriteResult2 =
                container.executeJob("/mysql_jdbc_to_dynamic_bucket_paimon_case2.conf");
        Assertions.assertEquals(0, textWriteResult2.getExitCode());
        List<String> parallelism_1 = verifyData(container);

        // parallelism = 2
        Container.ExecResult textWriteResult3 =
                container.executeJob("/mysql_jdbc_to_dynamic_bucket_paimon_case3.conf");
        Assertions.assertEquals(0, textWriteResult3.getExitCode());

        List<String> parallelism_2 = verifyData(container);
        Assertions.assertEquals(parallelism_1, parallelism_2);
    }

    private List<String> verifyData(TestContainer container) {
        List<InternalRow> actual = new ArrayList<>();
        given().ignoreExceptions()
                .await()
                .atLeast(100L, TimeUnit.MILLISECONDS)
                .atMost(30L, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            FileStoreTable table =
                                    (FileStoreTable) getTable("mysql_to_paimon", SOURCE_TABLE);
                            RowType rowType = table.rowType();
                            String[] fields = new String[] {"id", "version"};
                            int[] projection = getProjection(fields, rowType);
                            DataType[] projectionDataTypes =
                                    getProjectionFieldTypes(fields, rowType);
                            ReadBuilder readBuilder =
                                    table.newReadBuilder().withProjection(projection);
                            List<Split> splits = readBuilder.newScan().plan().splits();

                            try (RecordReader<InternalRow> reader =
                                    readBuilder.newRead().executeFilter().createReader(splits)) {

                                reader.forEachRemaining(
                                        row -> {
                                            GenericRow binaryRow =
                                                    new GenericRow(projectionDataTypes.length);
                                            for (int i = 0; i < projectionDataTypes.length; i++) {
                                                DataType type = projectionDataTypes[i];
                                                binaryRow.setField(
                                                        i,
                                                        InternalRow.createFieldGetter(type, i)
                                                                .getFieldOrNull(row));
                                            }
                                            actual.add(binaryRow);
                                        });
                            }
                            Assertions.assertEquals(10, actual.size());
                        });
        return actual.stream().map(Object::toString).collect(Collectors.toList());
    }

    private static DataType[] getProjectionFieldTypes(String[] projection, RowType rowType) {
        List<String> fieldNames = rowType.getFieldNames();
        Map<String, Integer> collect =
                IntStream.range(0, fieldNames.size())
                        .boxed()
                        .collect(Collectors.toMap(fieldNames::get, Function.identity()));
        return Arrays.stream(projection)
                .map(field -> rowType.getTypeAt(collect.get(field)))
                .toArray(DataType[]::new);
    }

    private int[] getProjection(String[] projection, RowType rowType) {
        return Arrays.stream(projection).mapToInt(rowType::getFieldIndex).toArray();
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
                container.executeJob("/fake_to_dynamic_bucket_paimon_case8.conf");
        Assertions.assertEquals(0, textWriteResult.getExitCode());
        Container.ExecResult textWriteResult1 =
                container.executeJob("/fake_to_dynamic_bucket_paimon_case4.conf");
        Assertions.assertEquals(0, textWriteResult1.getExitCode());
        given().ignoreExceptions()
                .await()
                .atLeast(100L, TimeUnit.MILLISECONDS)
                .atMost(120L, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
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
                            Assertions.assertEquals(2, bucketCountMap.size());
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
                            FileStoreTable table =
                                    (FileStoreTable) getTable("default", "st_test_cdc_write");
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
                                        "2024-03-10T10:00:12", paimonRecord.oneTime.toString());
                                Assertions.assertEquals(
                                        "2024-03-10T10:00:00.123", paimonRecord.twoTime.toString());
                                Assertions.assertEquals(
                                        "2024-03-10T10:00:00.123456",
                                        paimonRecord.threeTime.toString());
                                Assertions.assertEquals(
                                        "2024-03-10T10:00:00.123456789",
                                        paimonRecord.fourTime.toString());
                            }
                        });
    }

    @TestTemplate
    public void primaryFullTypeAndLoadData(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult writeResult =
                container.executeJob("/fake_to_dynamic_bucket_paimon_case6.conf");
        Assertions.assertEquals(0, writeResult.getExitCode());

        given().ignoreExceptions()
                .await()
                .atLeast(100L, TimeUnit.MILLISECONDS)
                .atMost(60L, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            FileStoreTable table =
                                    (FileStoreTable) getTable("full_type", "st_test");
                            List<String> primaryKeys = table.schema().primaryKeys();
                            Assertions.assertEquals(12, primaryKeys.size());
                            List<PaimonRecordWithFullType> paimonSourceRecords =
                                    loadPaimonDataWithFullType(table);
                            Assertions.assertEquals(6, paimonSourceRecords.size());
                        });
        // load full_type.st_test table data and initialize the PaimonBucketAssigner class
        Container.ExecResult writeResult1 =
                container.executeJob("/fake_to_dynamic_bucket_paimon_case7.conf");
        Assertions.assertEquals(0, writeResult1.getExitCode());
    }

    protected Table getTable(String dbName, String tbName) {
        Options options = new Options();
        String warehouse =
                String.format(
                        "%s%s/%s", isWindows ? "" : "file://", HOST_VOLUME_MOUNT_PATH, NAMESPACE);
        options.set("warehouse", warehouse);
        try {
            Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
            return catalog.getTable(Identifier.create(dbName, tbName));
        } catch (Catalog.TableNotExistException e) {
            // do something
            throw new RuntimeException("table not exist");
        }
    }

    private List<PaimonRecordWithFullType> loadPaimonDataWithFullType(FileStoreTable table) {
        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan.Plan plan = readBuilder.newScan().plan();
        TableRead tableRead = readBuilder.newRead();
        List<PaimonRecordWithFullType> result = new ArrayList<>();
        try (RecordReader<InternalRow> reader = tableRead.createReader(plan)) {
            reader.forEachRemaining(
                    row -> {
                        InternalMap internalMap = row.getMap(0);
                        InternalArray keyArray = internalMap.keyArray();
                        InternalArray valueArray = internalMap.valueArray();
                        HashMap<Object, Object> map = new HashMap<>(internalMap.size());
                        for (int i = 0; i < internalMap.size(); i++) {
                            map.put(keyArray.getString(i), valueArray.getString(i));
                        }
                        InternalArray internalArray = row.getArray(1);
                        int[] intArray = internalArray.toIntArray();
                        PaimonRecordWithFullType paimonRecordWithFullType =
                                new PaimonRecordWithFullType(
                                        map,
                                        intArray,
                                        row.getString(2),
                                        row.getBoolean(3),
                                        row.getByte(4),
                                        row.getShort(5),
                                        row.getInt(6),
                                        row.getLong(7),
                                        row.getFloat(8),
                                        row.getDouble(9),
                                        row.getDecimal(10, 30, 8),
                                        row.getString(11),
                                        row.getInt(12),
                                        row.getTimestamp(13, 6),
                                        row.getInt(14));
                        result.add(paimonRecordWithFullType);
                    });
        } catch (IOException e) {
            throw new SeaTunnelException(e);
        }
        return result;
    }
}
