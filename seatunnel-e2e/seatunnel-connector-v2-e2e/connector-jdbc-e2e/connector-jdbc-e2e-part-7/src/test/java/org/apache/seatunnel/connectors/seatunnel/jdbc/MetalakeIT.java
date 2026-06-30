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

import org.apache.seatunnel.shade.com.google.common.collect.Lists;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.shade.org.apache.commons.lang3.tuple.Pair;

import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.seatunnel.e2e.common.container.AbstractTestContainer.SEATUNNEL_HOME;
import static org.awaitility.Awaitility.given;

public class MetalakeIT extends TestSuiteBase {

    private static final String GRAVITINO_BASE_URL = "http://gravitino:8090";
    private static final String METALAKE_URL =
            GRAVITINO_BASE_URL + "/api/metalakes/test_metalake/catalogs/";

    protected GenericContainer<?> dbServer;
    protected GenericContainer<?> gravitinoContainer;

    protected JdbcCase jdbcCase;

    protected Connection connection;

    protected Catalog catalog;

    protected static final String HOST = "HOST";

    private static final String MYSQL_IMAGE = "mysql:8.0.43";
    private static final String MYSQL_CONTAINER_HOST = "mysql-e2e";
    private static final String MYSQL_DATABASE = "seatunnel";
    private static final String MYSQL_SOURCE = "source";
    private static final String MYSQL_SINK = "sink";
    private static final String CATALOG_DATABASE = "catalog_database";

    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "Abc!@#135_seatunnel";
    private static final int MYSQL_PORT = 3306;
    private static final String MYSQL_URL = "jdbc:mysql://" + HOST + ":%s/%s?useSSL=false";

    private static final String GRAVITINO_IMAGE = "apache/gravitino:latest";
    private static final int GRAVITINO_PORT = 8090;
    private static final String DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
    private static final String SEATUNNEL_YAML = "/config/seatunnel.yaml";

    private static final List<String> CONFIG_FILE =
            Lists.newArrayList("/mysql_to_mysql_with_metalake.conf");
    private static final String CREATE_SQL =
            "CREATE TABLE IF NOT EXISTS %s\n"
                    + "(\n"
                    + "    `c-bit_1`                bit(1)                DEFAULT NULL,\n"
                    + "    `c_bit_8`                bit(8)                DEFAULT NULL,\n"
                    + "    `c_bit_16`               bit(16)               DEFAULT NULL,\n"
                    + "    `c_bit_32`               bit(32)               DEFAULT NULL,\n"
                    + "    `c_bit_64`               bit(64)               DEFAULT NULL,\n"
                    + "    `c_bigint_30`            BIGINT(40)  unsigned  DEFAULT NULL,\n"
                    + "    UNIQUE (c_bigint_30)\n"
                    + ");";

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                ContainerUtil.copyFileIntoContainers(
                        SEATUNNEL_YAML,
                        Paths.get(SEATUNNEL_HOME, "config", "seatunnel.yaml").toString(),
                        container);
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && wget "
                                        + driverUrl()
                                        + " --no-check-certificate");
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    @BeforeAll
    public void startUp() throws Exception {
        dbServer = initContainer().withImagePullPolicy(PullPolicy.alwaysPull());
        Startables.deepStart(Stream.of(dbServer)).join();
        startGravitinoContainer();

        jdbcCase = getJdbcCase();

        given().ignoreExceptions()
                .await()
                .atMost(360, TimeUnit.SECONDS)
                .untilAsserted(() -> this.initializeJdbcConnection(jdbcCase.getJdbcUrl()));

        createNeededTables();
        insertTestData();
    }

    @AfterAll
    public void tearDown() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
        if (connection != null) {
            connection.close();
        }
        if (dbServer != null) {
            dbServer.close();
        }
        if (gravitinoContainer != null) {
            gravitinoContainer.close();
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK},
            disabledReason = "Metalake config is currently applied by Zeta starter.")
    public void testMetalake(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob(
                        "/jdbc_mysql_source_to_assert_sink_with_metalake.conf",
                        Arrays.asList(
                                "env.metalake_enabled=true",
                                "env.metalake_type=gravitino",
                                "env.metalake_url=" + METALAKE_URL));
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK},
            disabledReason = "Spark starter does not support MetadataProvider yet.")
    public void testDataSource(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob(
                        "/jdbc_mysql_source_to_assert_sink_with_datasource_enable.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    String driverUrl() {
        return "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar";
    }

    private void startGravitinoContainer() throws Exception {
        gravitinoContainer =
                new GenericContainer<>(GRAVITINO_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases("gravitino")
                        .withExposedPorts(GRAVITINO_PORT)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(
                                                "gravitino:" + GRAVITINO_IMAGE)))
                        .waitingFor(Wait.forHttp("/api/metalakes").forPort(GRAVITINO_PORT));
        gravitinoContainer.start();
        createMetalakeAndCatalog();
    }

    private void createMetalakeAndCatalog() throws Exception {
        Container.ExecResult createResult =
                gravitinoContainer.execInContainer(
                        "bash",
                        "-c",
                        "curl -L 'http://localhost:8090/api/metalakes' -H 'Content-Type: application/json' -H 'Accept: application/vnd.gravitino.v1+json' -d '{\"name\":\"test_metalake\",\"comment\":\"for metalake test\",\"properties\":{}}'"
                                + " && curl -L 'http://localhost:8090/api/metalakes/test_metalake/catalogs' -H 'Content-Type: application/json' -H 'Accept: application/vnd.gravitino.v1+json' -d '{\"name\":\"test_catalog\",\"type\":\"relational\",\"provider\":\"jdbc-mysql\",\"comment\":\"for metalake test\",\"properties\":{\"jdbc-driver\":\"com.mysql.cj.jdbc.Driver\",\"jdbc-url\":\"jdbc:mysql://mysql-e2e:3306/seatunnel?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true\",\"jdbc-user\":\"root\",\"jdbc-password\":\"Abc!@#135_seatunnel\"}}'");
        Assertions.assertEquals(0, createResult.getExitCode(), createResult.getStderr());
    }

    protected GenericContainer<?> initContainer() {
        DockerImageName imageName = DockerImageName.parse(MYSQL_IMAGE);

        GenericContainer<?> container =
                new MySQLContainer<>(imageName)
                        .withUsername(MYSQL_USERNAME)
                        .withPassword(MYSQL_PASSWORD)
                        .withDatabaseName(MYSQL_DATABASE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MYSQL_CONTAINER_HOST)
                        .withExposedPorts(MYSQL_PORT)
                        .waitingFor(Wait.forHealthcheck())
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(MYSQL_IMAGE)));

        container.setPortBindings(
                Lists.newArrayList(String.format("%s:%s", MYSQL_PORT, MYSQL_PORT)));

        return container;
    }

    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        String jdbcUrl = String.format(MYSQL_URL, MYSQL_PORT, MYSQL_DATABASE);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(MYSQL_DATABASE, MYSQL_SOURCE, fieldNames);

        return JdbcCase.builder()
                .dockerImage(MYSQL_IMAGE)
                .networkAliases(MYSQL_CONTAINER_HOST)
                .containerEnv(containerEnv)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                .port(MYSQL_PORT)
                .localPort(MYSQL_PORT)
                .jdbcTemplate(MYSQL_URL)
                .jdbcUrl(jdbcUrl)
                .userName(MYSQL_USERNAME)
                .password(MYSQL_PASSWORD)
                .database(MYSQL_DATABASE)
                .sourceTable(MYSQL_SOURCE)
                .sinkTable(MYSQL_SINK)
                .createSql(CREATE_SQL)
                .configFile(CONFIG_FILE)
                .insertSql(insertSql)
                .testData(testDataSet)
                .catalogDatabase(CATALOG_DATABASE)
                .catalogTable(MYSQL_SINK)
                .tablePathFullName(MYSQL_DATABASE + "." + MYSQL_SOURCE)
                .build();
    }

    protected void initializeJdbcConnection(String jdbcUrl)
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

    protected void createNeededTables() {
        try (Statement statement = connection.createStatement()) {
            String createTemplate = jdbcCase.getCreateSql();

            String createSource =
                    String.format(
                            createTemplate,
                            buildTableInfoWithSchema(
                                    jdbcCase.getDatabase(),
                                    jdbcCase.getSchema(),
                                    jdbcCase.getSourceTable()));
            statement.execute(createSource);

            if (jdbcCase.getAdditionalSqlOnSource() != null) {
                String additionalSql =
                        String.format(
                                jdbcCase.getAdditionalSqlOnSource(),
                                buildTableInfoWithSchema(
                                        jdbcCase.getDatabase(),
                                        jdbcCase.getSchema(),
                                        jdbcCase.getSourceTable()));
                statement.execute(additionalSql);
            }

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
            }

            if (jdbcCase.getAdditionalSqlOnSink() != null) {
                String additionalSql =
                        String.format(
                                jdbcCase.getAdditionalSqlOnSink(),
                                buildTableInfoWithSchema(
                                        jdbcCase.getDatabase(),
                                        jdbcCase.getSchema(),
                                        jdbcCase.getSinkTable()));
                statement.execute(additionalSql);
            }

            connection.commit();
        } catch (Exception exception) {
            throw new RuntimeException("Create MetalakeIT tables failed", exception);
        }
    }

    protected void insertTestData() {
        try (PreparedStatement preparedStatement =
                connection.prepareStatement(jdbcCase.getInsertSql())) {

            List<SeaTunnelRow> rows = jdbcCase.getTestData().getValue();

            for (SeaTunnelRow row : rows) {
                for (int index = 0; index < row.getArity(); index++) {
                    preparedStatement.setObject(index + 1, row.getField(index));
                }
                preparedStatement.addBatch();
            }

            preparedStatement.executeBatch();

            connection.commit();
        } catch (Exception exception) {
            throw new RuntimeException("Insert MetalakeIT test data failed", exception);
        }
    }

    Pair<String[], List<SeaTunnelRow>> initTestData() {
        String[] fieldNames =
                new String[] {
                    "c-bit_1", "c_bit_8", "c_bit_16", "c_bit_32", "c_bit_64", "c_bigint_30",
                };

        List<SeaTunnelRow> rows = new ArrayList<>();
        BigDecimal bigintValue = new BigDecimal("2844674407371055000");
        BigDecimal decimalValue = new BigDecimal("999999999999999999999999999899");
        for (int i = 0; i < 100; i++) {
            byte byteArr = Integer.valueOf(i).byteValue();
            SeaTunnelRow row;
            if (i == 99) {
                row =
                        new SeaTunnelRow(
                                new Object[] {
                                    (byte) 0,
                                    new byte[] {byteArr},
                                    new byte[] {byteArr, byteArr},
                                    new byte[] {byteArr, byteArr, byteArr, byteArr},
                                    new byte[] {
                                        byteArr, byteArr, byteArr, byteArr, byteArr, byteArr,
                                        byteArr, byteArr
                                    },
                                    // https://github.com/apache/seatunnel/issues/5559 this value
                                    // cannot set null, this null
                                    // value column's row will be lost in
                                    // jdbc_mysql_source_and_sink_parallel.conf,jdbc_mysql_source_and_sink_parallel_upper_lower.conf.
                                    bigintValue.add(BigDecimal.valueOf(i)),
                                });
            } else {
                row =
                        new SeaTunnelRow(
                                new Object[] {
                                    i % 2 == 0 ? (byte) 1 : (byte) 0,
                                    new byte[] {byteArr},
                                    new byte[] {byteArr, byteArr},
                                    new byte[] {byteArr, byteArr, byteArr, byteArr},
                                    new byte[] {
                                        byteArr, byteArr, byteArr, byteArr, byteArr, byteArr,
                                        byteArr, byteArr
                                    },
                                    bigintValue.add(BigDecimal.valueOf(i)),
                                });
            }
            rows.add(row);
        }

        return Pair.of(fieldNames, rows);
    }

    public String insertTable(String schema, String table, String... fields) {
        String columns =
                Arrays.stream(fields).map(this::quoteIdentifier).collect(Collectors.joining(", "));
        String placeholders = Arrays.stream(fields).map(f -> "?").collect(Collectors.joining(", "));

        return "INSERT INTO "
                + buildTableInfoWithSchema(schema, table)
                + " ("
                + columns
                + " )"
                + " VALUES ("
                + placeholders
                + ")";
    }

    protected Class<?> loadDriverClass() {
        try {
            return Class.forName(jdbcCase.getDriverClass());
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to load driver class: " + jdbcCase.getDriverClass(), e);
        }
    }

    protected String buildTableInfoWithSchema(String database, String schema, String table) {
        return buildTableInfoWithSchema(database, table);
    }

    public String buildTableInfoWithSchema(String schema, String table) {
        if (StringUtils.isNotBlank(schema)) {
            return quoteIdentifier(schema) + "." + quoteIdentifier(table);
        } else {
            return quoteIdentifier(table);
        }
    }

    public String quoteIdentifier(String field) {
        return "`" + field + "`";
    }
}
