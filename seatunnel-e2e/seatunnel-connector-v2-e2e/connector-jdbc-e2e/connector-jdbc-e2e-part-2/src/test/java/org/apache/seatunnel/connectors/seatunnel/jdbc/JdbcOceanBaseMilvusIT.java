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

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.milvus.MilvusContainer;
import org.testcontainers.oceanbase.OceanBaseCEContainer;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.DataType;
import io.milvus.grpc.MutationResult;
import io.milvus.param.ConnectParam;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.index.CreateIndexParam;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason = "Currently SPARK and FLINK not support adapt")
public class JdbcOceanBaseMilvusIT extends TestSuiteBase implements TestResource {

    private static final String IMAGE = "oceanbase/oceanbase-ce:vector";

    private static final String HOSTNAME = "e2e_oceanbase_vector";
    private static final int PORT = 2881;
    private static final String USERNAME = "root@test";
    private static final String PASSWORD = "";
    private static final String OCEANBASE_DATABASE = "seatunnel";
    private GenericContainer<?> dbServer;
    private Connection connection;
    private JdbcCase jdbcCase;
    private static final String OCEANBASE_SINK = "simple_example";

    private static final String HOST = "HOST";
    private static final String OCEANBASE_JDBC_TEMPLATE = "jdbc:oceanbase://" + HOST + ":%s/%s";
    private static final String OCEANBASE_DRIVER_CLASS = "com.oceanbase.jdbc.Driver";

    private static final String MILVUS_HOST = "milvus-e2e";
    private static final String MILVUS_IMAGE = "milvusdb/milvus:2.4-20240711-7e2a9d6b";
    private static final String TOKEN = "root:Milvus";
    private MilvusContainer container;
    private MilvusServiceClient milvusClient;
    private static final String COLLECTION_NAME = "simple_example";
    private static final String ID_FIELD = "book_id";
    private static final String VECTOR_FIELD = "book_intro";
    private static final String TITLE_FIELD = "book_title";
    private static final Integer VECTOR_DIM = 4;
    private static final Gson gson = new Gson();

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && wget "
                                        + driverUrl());
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    String driverUrl() {
        return "https://repo1.maven.org/maven2/com/oceanbase/oceanbase-client/2.4.12/oceanbase-client-2.4.12.jar";
    }

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        dbServer = initOceanbaseContainer();

        Startables.deepStart(Stream.of(dbServer)).join();
        jdbcCase = getJdbcCase();
        given().ignoreExceptions()
                .await()
                .atMost(360, TimeUnit.SECONDS)
                .untilAsserted(() -> this.initializeJdbcConnection(jdbcCase.getJdbcUrl()));

        createSchemaIfNeeded();
        createNeededTables();
        this.container =
                new MilvusContainer(MILVUS_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MILVUS_HOST);
        Startables.deepStart(Stream.of(this.container)).join();
        log.info("Milvus host is {}", container.getHost());
        log.info("Milvus container started");
        Awaitility.given().ignoreExceptions().await().atMost(720L, TimeUnit.SECONDS);
        this.initMilvus();
        this.initSourceData();
    }

    private void initMilvus()
            throws SQLException, ClassNotFoundException, InstantiationException,
                    IllegalAccessException {
        milvusClient =
                new MilvusServiceClient(
                        ConnectParam.newBuilder()
                                .withUri(this.container.getEndpoint())
                                .withToken(TOKEN)
                                .build());
    }

    private void initSourceData() {
        // Define fields
        List<FieldType> fieldsSchema =
                Arrays.asList(
                        FieldType.newBuilder()
                                .withName(ID_FIELD)
                                .withDataType(DataType.Int64)
                                .withPrimaryKey(true)
                                .withAutoID(false)
                                .build(),
                        FieldType.newBuilder()
                                .withName(VECTOR_FIELD)
                                .withDataType(DataType.FloatVector)
                                .withDimension(VECTOR_DIM)
                                .build(),
                        FieldType.newBuilder()
                                .withName(TITLE_FIELD)
                                .withDataType(DataType.VarChar)
                                .withMaxLength(64)
                                .build());

        // Create the collection with 3 fields
        R<RpcStatus> ret =
                milvusClient.createCollection(
                        CreateCollectionParam.newBuilder()
                                .withCollectionName(COLLECTION_NAME)
                                .withFieldTypes(fieldsSchema)
                                .build());
        if (ret.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException("Failed to create collection! Error: " + ret.getMessage());
        }

        // Specify an index type on the vector field.
        ret =
                milvusClient.createIndex(
                        CreateIndexParam.newBuilder()
                                .withCollectionName(COLLECTION_NAME)
                                .withFieldName(VECTOR_FIELD)
                                .withIndexType(IndexType.FLAT)
                                .withMetricType(MetricType.L2)
                                .build());
        if (ret.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException(
                    "Failed to create index on vector field! Error: " + ret.getMessage());
        }

        // Call loadCollection() to enable automatically loading data into memory for searching
        milvusClient.loadCollection(
                LoadCollectionParam.newBuilder().withCollectionName(COLLECTION_NAME).build());

        log.info("Collection created");

        // Insert 10 records into the collection
        List<JsonObject> rows = new ArrayList<>();
        for (long i = 1L; i <= 10; ++i) {

            JsonObject row = new JsonObject();
            row.add(ID_FIELD, gson.toJsonTree(i));
            List<Float> vector = Arrays.asList((float) i, (float) i, (float) i, (float) i);
            row.add(VECTOR_FIELD, gson.toJsonTree(vector));
            row.addProperty(TITLE_FIELD, "Tom and Jerry " + i);
            rows.add(row);
        }

        R<MutationResult> insertRet =
                milvusClient.insert(
                        InsertParam.newBuilder()
                                .withCollectionName(COLLECTION_NAME)
                                .withRows(rows)
                                .build());
        if (insertRet.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException("Failed to insert! Error: " + insertRet.getMessage());
        }
        log.info("Milvus test data created");
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        if (milvusClient != null) {
            milvusClient.close();
        }
        if (dbServer != null) {
            dbServer.close();
        }
        if (container != null) {
            container.close();
        }
    }

    @TestTemplate
    public void testMilvusToOceanBase(TestContainer container) throws Exception {
        try {
            Container.ExecResult execResult =
                    container.executeJob("/jdbc_milvus_source_and_oceanbase_sink.conf");
            Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
        } finally {
            clearTable(jdbcCase.getDatabase(), jdbcCase.getSchema(), jdbcCase.getSinkTable());
        }
    }

    @TestTemplate
    public void testFakeToOceanBase(TestContainer container)
            throws IOException, InterruptedException {
        try {
            Container.ExecResult execResult =
                    container.executeJob("/jdbc_fake_to_oceanbase_sink.conf");
            Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
        } finally {
            clearTable(jdbcCase.getDatabase(), jdbcCase.getSchema(), jdbcCase.getSinkTable());
        }
    }

    @TestTemplate
    public void testOceanBaseToMilvus(TestContainer container) throws Exception {
        try {
            initOceanBaseTestData();
            Container.ExecResult execResult =
                    container.executeJob("/jdbc_oceanbase_source_and_milvus_sink.conf");
            Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
        } finally {
            clearTable(jdbcCase.getDatabase(), jdbcCase.getSchema(), jdbcCase.getSinkTable());
        }
    }

    private void initOceanBaseTestData() {
        try (Statement statement = connection.createStatement()) {
            statement.execute(insertTable());
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException exception) {
                throw new SeaTunnelRuntimeException(JdbcITErrorCode.CLEAR_TABLE_FAILED, exception);
            }
            throw new SeaTunnelRuntimeException(JdbcITErrorCode.CLEAR_TABLE_FAILED, e);
        }
    }

    public String insertTable() {
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();
        String columns =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        List<Object[]> fields =
                testDataSet.getValue().stream()
                        .map(SeaTunnelRow::getFields)
                        .collect(Collectors.toList());

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder
                .append("INSERT INTO ")
                .append(buildTableInfoWithSchema(OCEANBASE_DATABASE, OCEANBASE_SINK))
                .append(" (")
                .append(columns)
                .append(") VALUES ");

        int valuesCount = fields.size();
        for (int i = 0; i < valuesCount; i++) {
            String fieldData = Arrays.toString(fields.get(i));
            sqlBuilder.append("(").append(fieldData, 1, fieldData.length() - 1).append(")");

            if (i < valuesCount - 1) {
                sqlBuilder.append(", ");
            }
        }
        return sqlBuilder.toString();
    }

    private void clearTable(String database, String schema, String table) {
        clearTable(database, table);
    }

    public void clearTable(String schema, String table) {
        try (Statement statement = connection.createStatement()) {
            statement.execute("TRUNCATE TABLE " + buildTableInfoWithSchema(schema, table));
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException exception) {
                throw new SeaTunnelRuntimeException(JdbcITErrorCode.CLEAR_TABLE_FAILED, exception);
            }
            throw new SeaTunnelRuntimeException(JdbcITErrorCode.CLEAR_TABLE_FAILED, e);
        }
    }

    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        String jdbcUrl =
                String.format(OCEANBASE_JDBC_TEMPLATE, dbServer.getMappedPort(PORT), "test");

        return JdbcCase.builder()
                .dockerImage(IMAGE)
                .networkAliases(HOSTNAME)
                .containerEnv(containerEnv)
                .driverClass(OCEANBASE_DRIVER_CLASS)
                .host(HOST)
                .port(PORT)
                .localPort(dbServer.getMappedPort(PORT))
                .jdbcTemplate(OCEANBASE_JDBC_TEMPLATE)
                .jdbcUrl(jdbcUrl)
                .userName(USERNAME)
                .password(PASSWORD)
                .database(OCEANBASE_DATABASE)
                .sinkTable(OCEANBASE_SINK)
                .createSql(createSqlTemplate())
                .build();
    }

    private void initializeJdbcConnection(String jdbcUrl)
            throws SQLException, InstantiationException, IllegalAccessException {
        Driver driver = (Driver) loadDriverClass().newInstance();
        Properties props = new Properties();

        if (StringUtils.isNotBlank(jdbcCase.getUserName())) {
            props.put("user", jdbcCase.getUserName());
        }

        if (StringUtils.isNotBlank(jdbcCase.getPassword())) {
            props.put("password", jdbcCase.getPassword());
        }

        if (dbServer != null) {
            jdbcUrl = jdbcUrl.replace(HOST, dbServer.getHost());
        }

        this.connection = driver.connect(jdbcUrl, props);
        connection.setAutoCommit(false);
    }

    private Class<?> loadDriverClass() {
        try {
            return Class.forName(jdbcCase.getDriverClass());
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to load driver class: " + jdbcCase.getDriverClass(), e);
        }
    }

    private void createSchemaIfNeeded() {
        String sql = "CREATE DATABASE IF NOT EXISTS " + OCEANBASE_DATABASE;
        try {
            connection.prepareStatement(sql).executeUpdate();
        } catch (Exception e) {
            throw new SeaTunnelRuntimeException(
                    JdbcITErrorCode.CREATE_TABLE_FAILED, "Fail to execute sql " + sql, e);
        }
        log.info("oceanbase schema created,sql is" + sql);
    }

    String createSqlTemplate() {
        return "CREATE TABLE IF NOT EXISTS %s\n"
                + "(\n"
                + "book_id varchar(20) NOT NULL,\n"
                + "book_intro vector(4) DEFAULT NULL,\n"
                + "book_title varchar(64) DEFAULT NULL,\n"
                + "primary key (book_id)\n"
                + ");";
    }

    OceanBaseCEContainer initOceanbaseContainer() {
        return new OceanBaseCEContainer(IMAGE)
                .withEnv("MODE", "slim")
                .withEnv("OB_DATAFILE_SIZE", "2G")
                .withNetwork(NETWORK)
                .withNetworkAliases(HOSTNAME)
                .withExposedPorts(PORT)
                .withImagePullPolicy(PullPolicy.alwaysPull())
                .waitingFor(Wait.forLogMessage(".*boot success!.*", 1))
                .withStartupTimeout(Duration.ofMinutes(5))
                .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(IMAGE)));
    }

    private void createNeededTables() {
        try (Statement statement = connection.createStatement()) {
            String createTemplate = jdbcCase.getCreateSql();

            if (!jdbcCase.isUseSaveModeCreateTable()) {
                if (jdbcCase.getSinkCreateSql() != null) {
                    createTemplate = jdbcCase.getSinkCreateSql();
                }
                String createSink =
                        String.format(
                                createTemplate,
                                buildTableInfoWithSchema(
                                        jdbcCase.getDatabase(),
                                        jdbcCase.getSchema(),
                                        jdbcCase.getSinkTable()));
                statement.execute(createSink);
                log.info("oceanbase table created,sql is" + createSink);
            }

            connection.commit();
        } catch (Exception exception) {
            log.error(ExceptionUtils.getMessage(exception));
            throw new SeaTunnelRuntimeException(JdbcITErrorCode.CREATE_TABLE_FAILED, exception);
        }
        log.info("oceanbase table created success!");
    }

    private String buildTableInfoWithSchema(String database, String schema, String table) {
        return buildTableInfoWithSchema(database, table);
    }

    public String quoteIdentifier(String field) {
        return "`" + field + "`";
    }

    public String buildTableInfoWithSchema(String schema, String table) {
        if (StringUtils.isNotBlank(schema)) {
            return quoteIdentifier(schema) + "." + quoteIdentifier(table);
        } else {
            return quoteIdentifier(table);
        }
    }

    private String[] getFieldNames() {
        return new String[] {
            "book_id", "book_intro", "book_title",
        };
    }

    private Pair<String[], List<SeaTunnelRow>> initTestData() {
        String[] fieldNames = getFieldNames();

        List<SeaTunnelRow> rows = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                i + 100,
                                "'"
                                        + DoubleStream.generate(() -> random.nextDouble() * 10)
                                                .limit(VECTOR_DIM)
                                                .mapToObj(num -> String.format("%.4f", num))
                                                .collect(Collectors.joining(", ", "[", "]"))
                                        + "'",
                                "\"" + "test" + i + "\"",
                            });
            rows.add(row);
        }
        return Pair.of(fieldNames, rows);
    }
}
