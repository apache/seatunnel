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

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import org.junit.jupiter.api.Assertions;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Slf4j
public class JdbcPrestoIT extends AbstractJdbcIT {
    protected static final String PRESTO_IMAGE = "prestodb/presto";

    private static final String PRESTO_ALIASES = "e2e_presto";
    private static final String DRIVER_CLASS = "com.facebook.presto.jdbc.PrestoDriver";
    private static final int PRESTO_PORT = 8080;
    private static final String PRESTO_URL = "jdbc:presto://" + HOST + ":%s/";
    private static final String USERNAME = "presto";
    private static final String DATABASE = "memory.default";
    private static final String SOURCE_TABLE = "presto_e2e_source_table";
    private static final String SINK_TABLE = "presto_e2e_sink_table";
    private static final String CATALOG_TABLE = "e2e_table_catalog";
    private static final Integer GEN_ROWS = 100;
    private static final List<String> CONFIG_FILE =
            Lists.newArrayList("/jdbc_presto_source_and_sink_with_full_type.conf");

    private static final String CREATE_SQL =
            "CREATE TABLE IF NOT EXISTS %s (\n"
                    + "  id                     BIGINT,\n"
                    + "boolean_col              BOOLEAN,\n"
                    + "tinyint_col              TINYINT,\n"
                    + "smallint_col             SMALLINT,\n"
                    + "integer_col              INTEGER,\n"
                    + "bigint_col               BIGINT,\n"
                    + "decimal_col              DECIMAL(22,4),\n"
                    + "real_col                 REAL,\n"
                    + "double_col               DOUBLE,\n"
                    + "char_col                 CHAR,\n"
                    + "varchar_col              VARCHAR,\n"
                    + "date_col                 DATE,\n"
                    + "time_col                 TIME,\n"
                    + "timestamp_col            TIMESTAMP,\n"
                    + "varbinary_col            VARBINARY"
                    + ")";

    private static final String[] fieldNames =
            new String[] {
                "id",
                "boolean_col",
                "tinyint_col",
                "smallint_col",
                "integer_col",
                "bigint_col",
                "decimal_col",
                "real_col",
                "double_col",
                "char_col",
                "varchar_col",
                "date_col",
                "time_col",
                "timestamp_col",
                "varbinary_col"
            };

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && curl -O "
                                        + driverUrl());
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    @Override
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

        // maybe the Presto server is still initializing
        int tryTimes = 5;
        for (int i = 0; i < tryTimes; i++) {
            try (Statement statement = connection.createStatement()) {
                statement.executeQuery(" select 1 ");
                break;
            } catch (SQLException ignored) {
                log.info("the Presto server is still initializing. wait it ");
            }
            try {
                Thread.sleep(15 * 1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        this.connection.setAutoCommit(false);
    }

    @Override
    JdbcCase getJdbcCase() {
        String jdbcUrl = String.format(PRESTO_URL, PRESTO_PORT, DATABASE);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(DATABASE, SOURCE_TABLE, fieldNames);

        return JdbcCase.builder()
                .dockerImage(PRESTO_IMAGE)
                .networkAliases(PRESTO_ALIASES)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                .port(PRESTO_PORT)
                .localPort(PRESTO_PORT)
                .jdbcTemplate(PRESTO_URL)
                .jdbcUrl(jdbcUrl)
                .userName(USERNAME)
                .database(DATABASE)
                .sourceTable(SOURCE_TABLE)
                .sinkTable(SINK_TABLE)
                .catalogDatabase(DATABASE)
                .catalogTable(CATALOG_TABLE)
                .createSql(CREATE_SQL)
                .configFile(CONFIG_FILE)
                .insertSql(insertSql)
                .testData(testDataSet)
                .build();
    }

    @Override
    protected void insertTestData() {
        try (PreparedStatement preparedStatement =
                connection.prepareStatement(jdbcCase.getInsertSql())) {

            List<SeaTunnelRow> rows = jdbcCase.getTestData().getValue();

            for (SeaTunnelRow row : rows) {
                for (int index = 0; index < row.getArity(); index++) {
                    preparedStatement.setObject(index + 1, row.getField(index));
                }
                preparedStatement.executeUpdate();
            }
            connection.commit();
        } catch (Exception exception) {
            log.error(ExceptionUtils.getMessage(exception));
            throw new SeaTunnelRuntimeException(JdbcITErrorCode.INSERT_DATA_FAILED, exception);
        }
    }

    @Override
    String driverUrl() {
        return "https://repo1.maven.org/maven2/com/facebook/presto/presto-jdbc/0.279/presto-jdbc-0.279.jar";
    }

    @Override
    public String quoteIdentifier(String field) {
        return field;
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        List<SeaTunnelRow> rows = new ArrayList<>();
        for (Integer i = 0; i < GEN_ROWS; i++) {

            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                i,
                                i % 2 == 0,
                                i.byteValue(),
                                i.shortValue(),
                                i,
                                Long.valueOf(i),
                                BigDecimal.valueOf(i * 1.0001).setScale(4, RoundingMode.DOWN),
                                Float.parseFloat("1.1"),
                                Double.parseDouble("1.111"),
                                String.valueOf(i).substring(0, 1),
                                String.valueOf(i),
                                Date.valueOf(LocalDate.now()),
                                Time.valueOf(LocalTime.of(12, 10, 0)),
                                Timestamp.valueOf(LocalDateTime.of(2024, 12, 12, 10, 0)),
                                "test".getBytes()
                            });
            rows.add(row);
        }

        return Pair.of(fieldNames, rows);
    }

    @Override
    protected void clearTable(String database, String schema, String table) {}

    @Override
    GenericContainer<?> initContainer() {
        GenericContainer<?> container =
                new GenericContainer<>(PRESTO_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(PRESTO_ALIASES)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(PRESTO_IMAGE)));
        container.setPortBindings(
                Lists.newArrayList(String.format("%s:%s", PRESTO_PORT, PRESTO_PORT)));

        return container;
    }
}
