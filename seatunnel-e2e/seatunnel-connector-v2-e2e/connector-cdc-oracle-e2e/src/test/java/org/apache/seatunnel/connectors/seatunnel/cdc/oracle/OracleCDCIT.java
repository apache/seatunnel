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
package org.apache.seatunnel.connectors.seatunnel.cdc.oracle;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertNotNull;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason = "Currently SPARK and FLINK do not support cdc")
public class OracleCDCIT extends TestSuiteBase implements TestResource {

    private static final String ORACLE_IMAGE = "jark/oracle-xe-11g-r2-cdc:0.1";

    private static final String HOST = "oracle-host";

    private static final Integer ORACLE_PORT = 1521;

    static {
        System.setProperty("oracle.jdbc.timezoneAsRegion", "false");
    }

    public static final OracleContainer ORACLE_CONTAINER =
            new OracleContainer(ORACLE_IMAGE)
                    .withNetwork(NETWORK)
                    .withNetworkAliases(HOST)
                    .withExposedPorts(ORACLE_PORT)
                    .withLogConsumer(
                            new Slf4jLogConsumer(
                                    DockerLoggerFactory.getLogger("oracle-docker-image")));

    public static final String CONNECTOR_USER = "dbzuser";

    public static final String CONNECTOR_PWD = "dbz";

    public static final String SCHEMA_USER = "debezium";

    public static final String SCHEMA_PWD = "dbz";

    public static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");

    private static final String SOURCE_SQL = "select * from DEBEZIUM.FULL_TYPES ORDER BY ID";
    private static final String SINK_SQL = "select * from DEBEZIUM.FULL_TYPES_SINK ORDER BY ID";

    private String driverUrl() {
        return "https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/12.2.0.1/ojdbc8-12.2.0.1.jar";
    }

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Oracle-CDC/lib && cd /tmp/seatunnel/plugins/Oracle-CDC/lib && wget "
                                        + driverUrl());
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        ORACLE_CONTAINER.setPortBindings(
                Lists.newArrayList(String.format("%s:%s", ORACLE_PORT, ORACLE_PORT)));
        log.info("Starting containers...");
        Startables.deepStart(Stream.of(ORACLE_CONTAINER)).join();
        log.info("Containers are started.");
    }

    @TestTemplate
    public void test(TestContainer container) throws Exception {
        createAndInitialize(ORACLE_CONTAINER, "column_type_test");

        CompletableFuture<Void> executeJobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                container.executeJob("/oraclecdc_to_console.conf");
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                            return null;
                        });

        // snapshot stage
        await().atMost(240000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    querySql(SINK_SQL), querySql(SOURCE_SQL));
                        });

        // insert update delete
        updateSourceTable();

        // stream stage
        await().atMost(240000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    querySql(SOURCE_SQL), querySql(SINK_SQL));
                        });
    }

    public static void createAndInitialize(OracleContainer oracleContainer, String sqlFile)
            throws Exception {
        final String ddlFile = String.format("ddl/%s.sql", sqlFile);
        final URL ddlTestFile = OracleCDCIT.class.getClassLoader().getResource(ddlFile);
        assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
        try (Connection connection = testConnection(oracleContainer);
                Statement statement = connection.createStatement()) {

            final List<String> statements =
                    Arrays.stream(
                                    Files.readAllLines(Paths.get(ddlTestFile.toURI())).stream()
                                            .map(String::trim)
                                            .filter(x -> !x.startsWith("--") && !x.isEmpty())
                                            .map(
                                                    x -> {
                                                        final Matcher m =
                                                                COMMENT_PATTERN.matcher(x);
                                                        return m.matches() ? m.group(1) : x;
                                                    })
                                            .collect(Collectors.joining("\n"))
                                            .split(";"))
                            .collect(Collectors.toList());

            for (String stmt : statements) {
                statement.execute(stmt);
            }
        }
    }

    public static Connection testConnection(OracleContainer oracleContainer) throws SQLException {
        return DriverManager.getConnection(oracleContainer.getJdbcUrl(), SCHEMA_USER, SCHEMA_PWD);
    }

    private List<List<Object>> querySql(String sql) {
        try (Connection connection = getJdbcConnection(ORACLE_CONTAINER)) {
            ResultSet resultSet = connection.createStatement().executeQuery(sql);
            List<List<Object>> result = new ArrayList<>();
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                ArrayList<Object> objects = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    objects.add(resultSet.getObject(i));
                }
                result.add(objects);
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void executeSql(String sql) {
        try (Connection connection = getJdbcConnection(ORACLE_CONTAINER)) {
            connection.createStatement().execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static Connection getJdbcConnection(OracleContainer oracleContainer)
            throws SQLException {
        return DriverManager.getConnection(
                oracleContainer.getJdbcUrl(), CONNECTOR_USER, CONNECTOR_PWD);
    }

    private void updateSourceTable() {
        executeSql(
                "INSERT INTO DEBEZIUM.FULL_TYPES VALUES (2, 'vc2', 'vc2', 'nvc2', 'c', 'nc',1.1, 2.22, 3.33, 8.888, 4.4444, 5.555, 6.66, 1234.567891, 1234.567891, 77.323,1, 22, 333, 4444, 5555, 1, 99, 9999, 999999999, 999999999999999999,94, 9949, 999999994, 999999999999999949, 99999999999999999999999999999999999949,TO_DATE('2022-10-30', 'yyyy-mm-dd'),TO_TIMESTAMP('2022-10-30 12:34:56.00789', 'yyyy-mm-dd HH24:MI:SS.FF5'),TO_TIMESTAMP('2022-10-30 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5'),TO_TIMESTAMP('2022-10-30 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5'),TO_TIMESTAMP('2022-10-30 12:34:56.125456789', 'yyyy-mm-dd HH24:MI:SS.FF9'),TO_TIMESTAMP_TZ('2022-10-30 01:34:56.00789', 'yyyy-mm-dd HH24:MI:SS.FF5'))");

        executeSql(
                "INSERT INTO DEBEZIUM.FULL_TYPES VALUES (\n"
                        + "    3, 'vc2', 'vc2', 'nvc2', 'c', 'nc',\n"
                        + "    1.1, 2.22, 3.33, 8.888, 4.4444, 5.555, 6.66, 1234.567891, 1234.567891, 77.323,\n"
                        + "    1, 22, 333, 4444, 5555, 1, 99, 9999, 999999999, 999999999999999999,\n"
                        + "    94, 9949, 999999994, 999999999999999949, 99999999999999999999999999999999999949,\n"
                        + "    TO_DATE('2022-10-30', 'yyyy-mm-dd'),\n"
                        + "    TO_TIMESTAMP('2022-10-30 12:34:56.00789', 'yyyy-mm-dd HH24:MI:SS.FF5'),\n"
                        + "    TO_TIMESTAMP('2022-10-30 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5'),\n"
                        + "    TO_TIMESTAMP('2022-10-30 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5'),\n"
                        + "    TO_TIMESTAMP('2022-10-30 12:34:56.125456789', 'yyyy-mm-dd HH24:MI:SS.FF9'),\n"
                        + "    TO_TIMESTAMP_TZ('2022-10-30 01:34:56.00789', 'yyyy-mm-dd HH24:MI:SS.FF5')\n"
                        + ")");

        executeSql("DELETE FROM DEBEZIUM.FULL_TYPES where id = 2");

        executeSql("UPDATE DEBEZIUM.FULL_TYPES SET VAL_VARCHAR = 'vc3' where id = 1");
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        ORACLE_CONTAINER.stop();
    }
}
