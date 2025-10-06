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
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import com.github.dockerjava.api.DockerClient;

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

import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;
import static org.awaitility.Awaitility.given;

public class MetalakeIT extends SeaTunnelContainer {

    protected GenericContainer<?> dbServer;

    protected JdbcCase jdbcCase;

    protected Connection connection;

    protected Catalog catalog;

    protected DockerClient dockerClient = DockerClientFactory.lazyClient();

    protected static final String HOST = "HOST";

    private static final String MYSQL_IMAGE = "mysql:8.0";
    private static final String MYSQL_CONTAINER_HOST = "mysql-e2e";
    private static final String MYSQL_DATABASE = "seatunnel";
    private static final String MYSQL_SOURCE = "source";
    private static final String MYSQL_SINK = "sink";
    private static final String CATALOG_DATABASE = "catalog_database";

    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "Abc!@#135_seatunnel";
    private static final int MYSQL_PORT = 3306;
    private static final String MYSQL_URL = "jdbc:mysql://" + HOST + ":%s/%s?useSSL=false";
    private static final String URL = "jdbc:mysql://" + HOST + ":3306/seatunnel";

    private static final String SQL = "select * from seatunnel.source";

    private static final String DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";

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

    @BeforeEach
    @Override
    public void startUp() throws Exception {
        // super.startUp();
        server =
                new GenericContainer<>(getDockerImage())
                        .withNetwork(NETWORK)
                        .withEnv("TZ", "UTC")
                        .withEnv("METALAKE_ENABLED", "true")
                        .withEnv("METALAKE_TYPE", "gravitino")
                        .withEnv(
                                "METALAKE_URL",
                                "http://127.0.0.1:8090/api/metalakes/test_metalake/catalogs/")
                        .withCommand(buildStartCommand())
                        .withNetworkAliases("server")
                        .withExposedPorts()
                        .withFileSystemBind("/tmp", "/opt/hive")
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(
                                                "seatunnel-engine:" + JDK_DOCKER_IMAGE)))
                        .waitingFor(Wait.forLogMessage(".*received new worker register:.*", 1));
        copySeaTunnelStarterToContainer(server);
        server.setPortBindings(Arrays.asList("5801:5801", "8080:8080"));
        server.withCopyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/"),
                Paths.get(SEATUNNEL_HOME, "config").toString());

        server.withCopyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-shade/seatunnel-hadoop3-3.1.4-uber/target/seatunnel-hadoop3-3.1.4-uber.jar"),
                Paths.get(SEATUNNEL_HOME, "lib/seatunnel-hadoop3-3.1.4-uber.jar").toString());
        // execute extra commands
        executeExtraCommands(server);
        server.start();

        server.execInContainer(
                "bash",
                "-c",
                "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && wget "
                        + driverUrl()
                        + " --no-check-certificate"
                        + "&& mkdir -p /tmp/gravitino && cd /tmp/gravitino && curl -C - --retry 5 -L -k -o gravitino-0.9.1-bin.tar.gz https://dlcdn.apache.org/gravitino/0.9.1/gravitino-0.9.1-bin.tar.gz && tar -zxvf gravitino-0.9.1-bin.tar.gz && cd /tmp/gravitino/gravitino-0.9.1-bin && ./bin/gravitino.sh start");

        server.execInContainer(
                "bash",
                "-c",
                "sleep 60 && curl -L 'http://127.0.0.1:8090/api/metalakes' -H 'Content-Type: application/json' -H 'Accept: application/vnd.gravitino.v1+json' -d '{\"name\":\"test_metalake\",\"comment\":\"for metalake test\",\"properties\":{}}'"
                        + "&& curl -L 'http://127.0.0.1:8090/api/metalakes/test_metalake/catalogs' -H 'Content-Type: application/json' -H 'Accept: application/vnd.gravitino.v1+json' -d '{\"name\":\"test_catalog\",\"type\":\"relational\",\"provider\":\"jdbc-mysql\",\"comment\":\"for metalake test\",\"properties\":{\"jdbc-driver\":\"com.mysql.cj.jdbc.Driver\",\"jdbc-url\":\"not used\",\"jdbc-user\":\"root\",\"jdbc-password\":\"Abc!@#135_seatunnel\"}}'");

        dbServer = initContainer().withImagePullPolicy(PullPolicy.alwaysPull());

        Startables.deepStart(Stream.of(dbServer)).join();

        jdbcCase = getJdbcCase();

        given().ignoreExceptions()
                .await()
                .atMost(360, TimeUnit.SECONDS)
                .untilAsserted(() -> this.initializeJdbcConnection(jdbcCase.getJdbcUrl()));

        createNeededTables();
        insertTestData();
    }

    @AfterEach
    @Override
    public void tearDown() throws Exception {
        if (catalog != null) {
            catalog.close();
        }

        if (connection != null) {
            connection.close();
        }

        if (dbServer != null) {
            dbServer.close();
            try {
                dockerClient.removeImageCmd(dbServer.getDockerImageName()).exec();
            } catch (Exception ignored) {
                ignored.printStackTrace();
            }
        }

        super.tearDown();
    }

    @Test
    public void TestMetalake() throws IOException, InterruptedException {
        Container.ExecResult execResult =
                executeJob("/jdbc_mysql_source_to_assert_sink_with_metalake.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    String driverUrl() {
        return "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar";
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
            exception.printStackTrace();
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
            exception.printStackTrace();
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
