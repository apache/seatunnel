/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.apache.commons.lang3.tuple.Pair;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.function.Executable;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class JdbcOracleMultipleTablesIT extends TestSuiteBase implements TestResource {
    private static final String ORACLE_IMAGE = "gvenzl/oracle-xe:21-slim-faststart";
    private static final String ORACLE_NETWORK_ALIASES = "e2e_oracleDb";
    private static final int ORACLE_PORT = 1521;
    private static final String USERNAME = "TESTUSER";
    private static final String PASSWORD = "testPassword";
    private static final String DATABASE = "XE";
    private static final String SCHEMA = USERNAME;
    private static final Pair<String[], List<SeaTunnelRow>> TEST_DATASET = generateTestDataset();
    private static final List<String> TABLES = Arrays.asList("TABLE1", "TABLE2");
    private static final List<String> SOURCE_TABLES =
            TABLES.stream().map(table -> SCHEMA + "." + table).collect(Collectors.toList());

    private static final List<String> SINK_TABLES =
            TABLES.stream()
                    .map(table -> SCHEMA + "." + "SINK_" + table)
                    .collect(Collectors.toList());
    private static final String CREATE_TABLE_SQL =
            "create table %s\n"
                    + "(\n"
                    + "    VARCHAR_10_COL                varchar2(10),\n"
                    + "    CHAR_10_COL                   char(10),\n"
                    + "    CLOB_COL                      clob,\n"
                    + "    NUMBER_1             number(1),\n"
                    + "    NUMBER_6             number(6),\n"
                    + "    NUMBER_10             number(10),\n"
                    + "    NUMBER_3_SF_2_DP              number(3, 2),\n"
                    + "    NUMBER_7_SF_N2_DP             number(7, -2),\n"
                    + "    INTEGER_COL                   integer,\n"
                    + "    FLOAT_COL                     float(10),\n"
                    + "    REAL_COL                      real,\n"
                    + "    BINARY_FLOAT_COL              binary_float,\n"
                    + "    BINARY_DOUBLE_COL             binary_double,\n"
                    + "    DATE_COL                      date,\n"
                    + "    TIMESTAMP_WITH_3_FRAC_SEC_COL timestamp(3),\n"
                    + "    TIMESTAMP_WITH_LOCAL_TZ       timestamp with local time zone,\n"
                    + "    XML_TYPE_COL                  \"SYS\".\"XMLTYPE\""
                    + ")";

    private OracleContainer oracleContainer;
    private Connection connection;

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && wget "
                                        + "https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/12.2.0.1/ojdbc8-12.2.0.1.jar && wget https://repo1.maven.org/maven2/com/oracle/database/xml/xdb6/12.2.0.1/xdb6-12.2.0.1.jar && wget https://repo1.maven.org/maven2/com/oracle/database/xml/xmlparserv2/12.2.0.1/xmlparserv2-12.2.0.1.jar");
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        DockerImageName imageName = DockerImageName.parse(ORACLE_IMAGE);
        oracleContainer =
                new OracleContainer(imageName)
                        .withUsername(USERNAME)
                        .withPassword(PASSWORD)
                        .withDatabaseName(SCHEMA)
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("sql/oracle_init.sql"),
                                "/container-entrypoint-startdb.d/init.sql")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(ORACLE_NETWORK_ALIASES)
                        .withExposedPorts(ORACLE_PORT)
                        .withImagePullPolicy((PullPolicy.alwaysPull()))
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(ORACLE_IMAGE)));

        oracleContainer.setPortBindings(
                Lists.newArrayList(String.format("%s:%s", ORACLE_PORT, ORACLE_PORT)));

        Startables.deepStart(Stream.of(oracleContainer)).join();

        connection = oracleContainer.createConnection("");
        createTables(SOURCE_TABLES);
        createTables(SINK_TABLES);
        initSourceTablesData();
    }

    @TestTemplate
    public void testOracleJdbcMultipleTableE2e(TestContainer container)
            throws IOException, InterruptedException, SQLException {
        clearSinkTables();

        Container.ExecResult execResult =
                container.executeJob("/jdbc_oracle_source_with_multiple_tables_to_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        List<Executable> asserts =
                TABLES.stream()
                        .map(
                                (Function<String, Executable>)
                                        table ->
                                                () ->
                                                        Assertions.assertIterableEquals(
                                                                query(
                                                                        String.format(
                                                                                "SELECT * FROM %s.%s order by INTEGER_COL asc",
                                                                                SCHEMA, table)),
                                                                query(
                                                                        String.format(
                                                                                "SELECT * FROM %s.%s order by INTEGER_COL asc",
                                                                                SCHEMA,
                                                                                "SINK_" + table))))
                        .collect(Collectors.toList());
        Assertions.assertAll(asserts);
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        if (oracleContainer != null) {
            oracleContainer.close();
        }
    }

    private void createTables(List<String> tables) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            tables.forEach(
                    tableName -> {
                        try {
                            statement.execute(String.format(CREATE_TABLE_SQL, tableName));
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
    }

    private void initSourceTablesData() throws SQLException {
        String columns = Arrays.stream(TEST_DATASET.getLeft()).collect(Collectors.joining(", "));
        String placeholders =
                Arrays.stream(TEST_DATASET.getLeft())
                        .map(f -> "?")
                        .collect(Collectors.joining(", "));
        for (String table : SOURCE_TABLES) {
            String sql =
                    "INSERT INTO " + table + " (" + columns + " ) VALUES (" + placeholders + ")";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                for (SeaTunnelRow row : TEST_DATASET.getRight()) {
                    for (int i = 0; i < row.getArity(); i++) {
                        statement.setObject(i + 1, row.getField(i));
                    }
                    statement.addBatch();
                }
                statement.executeBatch();
            }
        }
    }

    private List<List<Object>> query(String sql) {
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            List<List<Object>> result = new ArrayList<>();
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                ArrayList<Object> objects = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    objects.add(resultSet.getString(i));
                }
                result.add(objects);
                log.debug(String.format("Print query, sql: %s, data: %s", sql, objects));
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void clearSinkTables() throws SQLException {
        for (String table : SINK_TABLES) {
            String sql = "truncate table " + table;
            try (Statement statement = connection.createStatement()) {
                statement.execute(sql);
            }
        }
    }

    private static Pair<String[], List<SeaTunnelRow>> generateTestDataset() {
        String[] fieldNames =
                new String[] {
                    "VARCHAR_10_COL",
                    "CHAR_10_COL",
                    "CLOB_COL",
                    "NUMBER_1",
                    "NUMBER_6",
                    "NUMBER_10",
                    "NUMBER_3_SF_2_DP",
                    "NUMBER_7_SF_N2_DP",
                    "INTEGER_COL",
                    "FLOAT_COL",
                    "REAL_COL",
                    "BINARY_FLOAT_COL",
                    "BINARY_DOUBLE_COL",
                    "DATE_COL",
                    "TIMESTAMP_WITH_3_FRAC_SEC_COL",
                    "TIMESTAMP_WITH_LOCAL_TZ",
                    "XML_TYPE_COL"
                };
        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                String.format("f%s", i),
                                String.format("f%s", i),
                                String.format("f%s", i),
                                1,
                                i * 10,
                                i * 1000,
                                BigDecimal.valueOf(1.1),
                                BigDecimal.valueOf(2400),
                                i,
                                Float.parseFloat("2.2"),
                                Float.parseFloat("2.2"),
                                Float.parseFloat("22.2"),
                                Double.parseDouble("2.2"),
                                Date.valueOf(LocalDate.now()),
                                Timestamp.valueOf(LocalDateTime.now()),
                                Timestamp.valueOf(LocalDateTime.now()),
                                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><project xmlns=\"http://maven.apache.org/POM/4.0.0\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd\"><name>SeaTunnel : E2E : Connector V2 : Oracle XMLType</name></project>"
                            });
            rows.add(row);
        }

        return Pair.of(fieldNames, rows);
    }
}
