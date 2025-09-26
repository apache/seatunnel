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

package mongodb;

import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.UniqueDatabase;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;
import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.hdfs.HdfsStorage;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;
import org.apache.seatunnel.engine.server.checkpoint.ActionState;
import org.apache.seatunnel.engine.server.checkpoint.ActionStateKey;
import org.apache.seatunnel.engine.server.checkpoint.CompletedCheckpoint;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.testcontainers.shaded.org.awaitility.Awaitility.await;
import static org.testcontainers.shaded.org.awaitility.Awaitility.with;
import static org.testcontainers.shaded.org.awaitility.Durations.TWO_SECONDS;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK},
        disabledReason = "Currently SPARK do not support cdc")
public class MongodbCDCIT extends TestSuiteBase implements TestResource {

    // ----------------------------------------------------------------------------
    // mongodb
    protected static final String MONGODB_DATABASE = "inventory";

    protected static final String MONGODB_COLLECTION_1 = "products";
    protected static final String MONGODB_COLLECTION_2 = "orders";
    protected MongoDBContainer mongodbContainer;

    protected MongoClient client;

    // ----------------------------------------------------------------------------
    // mysql
    private static final String MYSQL_HOST = "mysql_e2e";

    private static final String MYSQL_USER_NAME = "st_user";

    private static final String MYSQL_USER_PASSWORD = "seatunnel";

    private static final String MYSQL_DATABASE = "mongodb_cdc";

    private static final String DEFAULT_CHECKPOINT_PATH = "/tmp/seatunnel/checkpoint_snapshot";

    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer();

    // mysql sink table query sql
    private static final String SINK_SQL_PRODUCTS = "select name,description,weight from products";

    private static final String SINK_SQL_ORDERS =
            "select order_number,order_date,quantity,product_id from orders order by order_number asc";

    private static final String MYSQL_DRIVER_JAR =
            "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.16/mysql-connector-java-8.0.16.jar";

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, MYSQL_DATABASE);

    private static MySqlContainer createMySqlContainer() {
        MySqlContainer mySqlContainer = new MySqlContainer(MySqlVersion.V8_0);
        mySqlContainer.withNetwork(NETWORK);
        mySqlContainer.withNetworkAliases(MYSQL_HOST);
        mySqlContainer.withDatabaseName(MYSQL_DATABASE);
        mySqlContainer.withUsername(MYSQL_USER_NAME);
        mySqlContainer.withPassword(MYSQL_USER_PASSWORD);
        mySqlContainer.withLogConsumer(
                new Slf4jLogConsumer(DockerLoggerFactory.getLogger("Mysql-Docker-Image")));
        // For local test use
        mySqlContainer.setPortBindings(Collections.singletonList("3310:3306"));
        return mySqlContainer;
    }

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && wget "
                                        + MYSQL_DRIVER_JAR);
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    @BeforeAll
    @Override
    public void startUp() {
        log.info("The first stage:Starting Mysql containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        log.info("Mysql Containers are started");
        inventoryDatabase.createAndInitialize();
        log.info("Mysql ddl-a execution is complete");

        log.info("The second stage:Starting Mongodb containers...");
        mongodbContainer = new MongoDBContainer(NETWORK);
        // For local test use
        mongodbContainer.setPortBindings(Collections.singletonList("27017:27017"));
        mongodbContainer.withLogConsumer(
                new Slf4jLogConsumer(DockerLoggerFactory.getLogger("Mongodb-Docker-Image")));

        Startables.deepStart(Stream.of(mongodbContainer)).join();
        mongodbContainer.executeCommandFileInSeparateDatabase(MONGODB_DATABASE);
        initConnection();
        log.info("Mongodb Container are started");
    }

    @TestTemplate
    public void testMongodbCdcToMysqlCheckDataE2e(TestContainer container)
            throws InterruptedException {
        cleanSourceTable();
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/mongodbcdc_to_mysql.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException();
                    }
                    return null;
                });
        TimeUnit.SECONDS.sleep(10);
        // insert update delete
        upsertDeleteSourceTable();
        TimeUnit.SECONDS.sleep(20);
        assertionsSourceAndSink(MONGODB_COLLECTION_1, SINK_SQL_PRODUCTS);

        cleanSourceTable();
        TimeUnit.SECONDS.sleep(20);
        assertionsSourceAndSink(MONGODB_COLLECTION_1, SINK_SQL_PRODUCTS);
    }

    @TestTemplate
    public void testMongodbCdcMultiTableToMysqlE2e(TestContainer container)
            throws InterruptedException {
        cleanSourceTable();
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/mongodb_multi_table_cdc_to_mysql.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException();
                    }
                    return null;
                });
        TimeUnit.SECONDS.sleep(20);
        // insert update delete
        upsertDeleteSourceTable();
        TimeUnit.SECONDS.sleep(20);
        assertionsSourceAndSink(MONGODB_COLLECTION_1, SINK_SQL_PRODUCTS);
        assertionsSourceAndSink(MONGODB_COLLECTION_2, SINK_SQL_ORDERS);

        cleanSourceTable();
        TimeUnit.SECONDS.sleep(20);
        assertionsSourceAndSink(MONGODB_COLLECTION_1, SINK_SQL_PRODUCTS);
        assertionsSourceAndSink(MONGODB_COLLECTION_2, SINK_SQL_ORDERS);

        mongodbContainer.executeCommandFileInDatabase("inventory", MONGODB_DATABASE);

        // test drop collection
        mongodbContainer.executeCommandInDatabase(
                "db." + MONGODB_COLLECTION_2 + ".drop", MONGODB_DATABASE);

        MongoDatabase mongoDatabase = client.getDatabase(MONGODB_DATABASE);
        MongoCollection<Document> collection1 = mongoDatabase.getCollection(MONGODB_COLLECTION_1);

        Document document = new Document();
        document.put("name", "soap5677");
        document.put("description", "versatile cleaning essential for home and industry");
        document.put("weight", "4000");
        collection1.insertOne(document);

        collection1.updateOne(
                Filters.eq("name", "soap5677"),
                Updates.set("description", "versatile cleaning essential"));

        TimeUnit.SECONDS.sleep(10);
        assertionsSourceAndSink(MONGODB_COLLECTION_1, SINK_SQL_PRODUCTS);
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason =
                    "This case requires obtaining the task health status and manually canceling the canceled task, which is currently only supported by the zeta engine.")
    public void testMongodbCdcMetadataTrans(TestContainer container) throws InterruptedException {
        cleanSourceTable();
        Long jobId = JobIdGenerator.newJobId();
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob(
                                "/mongodbcdc_metadata_trans.conf", String.valueOf(jobId));
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException();
                    }
                    return null;
                });
        TimeUnit.SECONDS.sleep(10);
        // insert update delete
        upsertDeleteSourceTable();
        TimeUnit.SECONDS.sleep(20);
        await().atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            String jobStatus = container.getJobStatus(String.valueOf(jobId));
                            Assertions.assertEquals("RUNNING", jobStatus);
                        });

        try {
            Container.ExecResult cancelJobResult = container.cancelJob(String.valueOf(jobId));
            Assertions.assertEquals(0, cancelJobResult.getExitCode(), cancelJobResult.getStderr());
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason = "Currently SPARK and FLINK do not support restore")
    public void testSavepointRecovery(TestContainer container)
            throws InterruptedException, IOException {
        cleanSourceTable();
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        String jobConfigFile = "/mongodbcdc_to_mysql.conf";
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob(jobConfigFile, jobId);
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException();
                    }
                    return null;
                });
        TimeUnit.SECONDS.sleep(10);
        upsertDeleteSourceTable();
        Assertions.assertEquals(0, container.savepointJob(jobId).getExitCode());
        TimeUnit.SECONDS.sleep(10);
        // restore 1
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.restoreJob(jobConfigFile, jobId);
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });
        mongodbContainer.executeCommandFileInDatabase("inventory", MONGODB_DATABASE);
        TimeUnit.SECONDS.sleep(10);
        // Verify data consistency after recovery
        assertionsSourceAndSink(MONGODB_COLLECTION_1, SINK_SQL_PRODUCTS);
    }

    @TestTemplate
    @DisabledOnOs(OS.WINDOWS)
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason = "Currently SPARK and FLINK do not support restore")
    public void testResumeTokenFailureRecovery(TestContainer container) throws Exception {
        cleanSourceTable();
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        String jobConfigFile = "/mongodbcdc_to_mysql.conf";

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob(jobConfigFile, jobId);
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        TimeUnit.SECONDS.sleep(10);

        upsertDeleteSourceTable();

        TimeUnit.SECONDS.sleep(20);

        assertionsSourceAndSink(MONGODB_COLLECTION_1, SINK_SQL_PRODUCTS);

        // savepoint
        Assertions.assertEquals(0, container.savepointJob(jobId).getExitCode());
        TimeUnit.SECONDS.sleep(5);

        // modify resume token
        modifyResumeTokenInCheckpoint(jobId, container);

        // restore
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.restoreJob(jobConfigFile, jobId);
                    } catch (Exception e) {
                        log.error("Restore task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        TimeUnit.SECONDS.sleep(30);

        mongodbContainer.executeCommandFileInDatabase("inventory", MONGODB_DATABASE);

        TimeUnit.SECONDS.sleep(20);

        assertionsSourceAndSink(MONGODB_COLLECTION_1, SINK_SQL_PRODUCTS);
    }

    /**
     * Directly modifying the resume-token in the checkpoint is to simulate a scenario where the
     * resume-token fails, as it is not possible to directly specify the savepoint file storage
     * location, and dynamic table additions and deletions cannot normally reproduce this exception
     *
     * @param jobId jobId
     * @param container container
     * @throws Exception
     */
    public void modifyResumeTokenInCheckpoint(String jobId, TestContainer container)
            throws Exception {
        ContainerExtendedFactory containerExtendedFactory =
                new ContainerExtendedFactory() {
                    @Override
                    public void extend(GenericContainer<?> container)
                            throws IOException, InterruptedException {
                        FileUtils.createNewDir(DEFAULT_CHECKPOINT_PATH);
                        container.execInContainer(
                                "sh",
                                "-c",
                                "cd "
                                        + DEFAULT_CHECKPOINT_PATH
                                        + " && tar -czvf checkpoint.tar.gz "
                                        + jobId);
                        container.copyFileFromContainer(
                                DEFAULT_CHECKPOINT_PATH + "/checkpoint.tar.gz",
                                DEFAULT_CHECKPOINT_PATH + "/checkpoint.tar.gz");
                        extractFiles();
                    }

                    private void extractFiles() {
                        ProcessBuilder processBuilder = new ProcessBuilder();
                        processBuilder.command(
                                "sh",
                                "-c",
                                "cd "
                                        + DEFAULT_CHECKPOINT_PATH
                                        + "/"
                                        + " && tar -zxvf checkpoint.tar.gz");
                        try {
                            Process process = processBuilder.start();
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
                };

        container.executeExtraCommands(containerExtendedFactory);

        Map<String, String> config = new HashMap<>();
        config.put("storage.type", "hdfs");
        config.put("namespace", DEFAULT_CHECKPOINT_PATH);
        config.put("fs.defaultFS", "file:///tmp/");
        HdfsStorage hdfsStorage = new HdfsStorage(config);

        ProtoStuffSerializer serializer = new ProtoStuffSerializer();
        PipelineState pipelineState =
                hdfsStorage.getLatestCheckpointByJobIdAndPipelineId(jobId, "1");
        CompletedCheckpoint checkpoint =
                serializer.deserialize(pipelineState.getStates(), CompletedCheckpoint.class);

        Map<ActionStateKey, ActionState> taskStates = checkpoint.getTaskStates();

        taskStates.entrySet().stream()
                .findFirst()
                .ifPresent(
                        entry -> {
                            ActionState state = entry.getValue();
                            state.getSubtaskStates().stream()
                                    .findFirst()
                                    .ifPresent(
                                            subtaskState -> {
                                                List<byte[]> stateBytes = subtaskState.getState();
                                                DefaultSerializer<IncrementalSplit>
                                                        mongoSplitSerializer =
                                                                new DefaultSerializer<>();
                                                IncrementalSplit incrementalSplit = null;
                                                try {
                                                    incrementalSplit =
                                                            mongoSplitSerializer.deserialize(
                                                                    stateBytes.get(0));
                                                    log.info(
                                                            "before modify incrementalSplit result {}",
                                                            incrementalSplit);
                                                    for (Map.Entry<String, String> entry1 :
                                                            incrementalSplit
                                                                    .getStartupOffset()
                                                                    .getOffset()
                                                                    .entrySet()) {
                                                        if (entry1.getValue().contains("_data")) {
                                                            entry1.setValue(
                                                                    entry1.getValue()
                                                                                    .substring(
                                                                                            0, 21)
                                                                            + "FF"
                                                                            + entry1.getValue()
                                                                                    .substring(23));
                                                            subtaskState
                                                                    .getState()
                                                                    .set(
                                                                            0,
                                                                            mongoSplitSerializer
                                                                                    .serialize(
                                                                                            incrementalSplit));
                                                        }
                                                    }
                                                } catch (IOException e) {
                                                    throw new RuntimeException(e);
                                                }
                                                log.info(
                                                        "after modify incrementalSplit result {}",
                                                        incrementalSplit);
                                            });
                        });

        byte[] states = serializer.serialize(checkpoint);
        hdfsStorage.storeCheckPoint(
                PipelineState.builder()
                        .checkpointId(checkpoint.getCheckpointId())
                        .jobId(String.valueOf(jobId))
                        .pipelineId(checkpoint.getPipelineId())
                        .states(states)
                        .build());

        // copy latestFileName to container
        List<String> fileNames = hdfsStorage.getFileNames(DEFAULT_CHECKPOINT_PATH + "/" + jobId);
        String latestFileName =
                hdfsStorage.getLatestCheckpointFileNameByJobIdAndPipelineId(fileNames, "1");

        String latestFilePath = DEFAULT_CHECKPOINT_PATH + "/" + jobId + "/" + latestFileName;
        container.copyAbsolutePathToContainer(latestFilePath, latestFilePath);
    }

    private void assertionsSourceAndSink(String mongodbCollection, String sinkMysqlQuery) {
        List<List<Object>> expected =
                readMongodbData(mongodbCollection).stream()
                        .peek(e -> e.remove("_id"))
                        .map(Document::entrySet)
                        .map(Set::stream)
                        .map(
                                entryStream ->
                                        entryStream
                                                .map(
                                                        entry -> {
                                                            Object value = entry.getValue();
                                                            if (value instanceof Long) {
                                                                return new Long(value.toString());
                                                            }
                                                            if (value instanceof Number) {
                                                                return new BigDecimal(
                                                                                value.toString())
                                                                        .intValue();
                                                            }
                                                            if (value instanceof ObjectId) {
                                                                return ((ObjectId) value)
                                                                        .toString();
                                                            }
                                                            return value;
                                                        })
                                                .collect(Collectors.toCollection(ArrayList::new)))
                        .collect(Collectors.toList());
        log.info("Print mongodb source data: \n{}", expected);
        with().pollInterval(TWO_SECONDS)
                .pollDelay(500, TimeUnit.MILLISECONDS)
                .await()
                .atMost(450, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(expected, querySql(sinkMysqlQuery));
                        });
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                MYSQL_CONTAINER.getJdbcUrl(),
                MYSQL_CONTAINER.getUsername(),
                MYSQL_CONTAINER.getPassword());
    }

    private List<List<Object>> querySql(String querySql) {
        try (Connection connection = getJdbcConnection();
                ResultSet resultSet = connection.createStatement().executeQuery(querySql)) {
            List<List<Object>> result = new ArrayList<>();
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                ArrayList<Object> objects = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    objects.add(resultSet.getObject(i));
                }
                log.info("Print mysql sink data: {} ", objects);
                result.add(objects);
            }
            log.info("============================= mysql data ================================");
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void truncateMysqlTable(String tableName) {
        String checkTableExistsSql =
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?";
        String truncateTableSql = String.format("TRUNCATE TABLE %s", tableName);

        try (Connection connection = getJdbcConnection();
                PreparedStatement checkStmt = connection.prepareStatement(checkTableExistsSql)) {
            checkStmt.setString(1, MYSQL_DATABASE);
            checkStmt.setString(2, tableName);
            try (ResultSet rs = checkStmt.executeQuery()) {
                if (rs.next() && rs.getInt(1) > 0) {
                    try (Statement truncateStmt = connection.createStatement()) {
                        truncateStmt.executeUpdate(truncateTableSql);
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Error checking if table exists: " + tableName, e);
        }
    }

    private void upsertDeleteSourceTable() {
        mongodbContainer.executeCommandFileInDatabase("inventoryDDL", MONGODB_DATABASE);
    }

    private void cleanSourceTable() {
        mongodbContainer.executeCommandFileInDatabase("inventoryClean", MONGODB_DATABASE);
        truncateMysqlTable(MONGODB_COLLECTION_1);
        truncateMysqlTable(MONGODB_COLLECTION_2);
    }

    public void initConnection() {
        String ipAddress = mongodbContainer.getHost();
        Integer port = mongodbContainer.getFirstMappedPort();
        String url =
                String.format(
                        "mongodb://%s:%s@%s:%d/%s?authSource=admin",
                        "superuser", "superpw", ipAddress, port, MONGODB_DATABASE);
        client = MongoClients.create(url);
    }

    protected List<Document> readMongodbData(String collection) {
        MongoCollection<Document> sinkTable =
                client.getDatabase(MONGODB_DATABASE).getCollection(collection);
        // If the cursor has been traversed, it will automatically close without explicitly closing.
        MongoCursor<Document> cursor = sinkTable.find().sort(Sorts.ascending("_id")).cursor();
        List<Document> documents = new ArrayList<>();
        while (cursor.hasNext()) {
            documents.add(cursor.next());
        }
        return documents;
    }

    @AfterAll
    @Override
    public void tearDown() {
        // close Container
        if (Objects.nonNull(client)) {
            client.close();
        }
        MYSQL_CONTAINER.close();
        if (mongodbContainer != null) {
            mongodbContainer.close();
        }
    }
}
