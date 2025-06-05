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

import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

@Slf4j
public class JdbcTrinoIT extends AbstractJdbcIT {
    protected static final String TRINO_IMAGE = "trinodb/trino";

    private static final String TRINO_ALIASES = "e2e-trino";
    private static final String DRIVER_CLASS = "io.trino.jdbc.TrinoDriver";
    private static final int TRINO_PORT = 28080;
    private static final String TRINO_URL = "jdbc:trino://" + HOST + ":%s/memory?timezone=UTC";
    private static final String USERNAME = "trino";
    private static final String DATABASE = "memory.default";
    private static final String SOURCE_TABLE = "trino_e2e_source_table";
    private static final List<String> CONFIG_FILE =
            Lists.newArrayList("/jdbc_trino_source_and_assert.conf");

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
                    + "varbinary_col            VARBINARY,\n"
                    + "json_col                 json\n"
                    + ")";

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

        // maybe the TRINO  server is still initializing
        int tryTimes = 5;
        for (int i = 0; i < tryTimes; i++) {
            try (Statement statement = connection.createStatement()) {
                statement.executeQuery(" select 1 ");
                break;
            } catch (SQLException ignored) {
                log.info("the Trino server is still initializing. wait it ");
            }
            try {
                Thread.sleep(15 * 1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
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
        } catch (Exception exception) {
            log.error(ExceptionUtils.getMessage(exception));
            throw new SeaTunnelRuntimeException(JdbcITErrorCode.CREATE_TABLE_FAILED, exception);
        }
    }

    @Override
    JdbcCase getJdbcCase() {
        String jdbcUrl = String.format(TRINO_URL, TRINO_PORT, DATABASE);
        return JdbcCase.builder()
                .dockerImage(TRINO_IMAGE)
                .networkAliases(TRINO_ALIASES)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                .port(TRINO_PORT)
                .localPort(TRINO_PORT)
                .jdbcTemplate(TRINO_URL)
                .jdbcUrl(jdbcUrl)
                .userName(USERNAME)
                .database(DATABASE)
                .sourceTable(SOURCE_TABLE)
                .catalogDatabase(DATABASE)
                .createSql(CREATE_SQL)
                .configFile(CONFIG_FILE)
                .useSaveModeCreateTable(true)
                .build();
    }

    @Override
    protected void insertTestData() {
        try (Statement statement = connection.createStatement()) {
            for (int i = 1; i <= 3; i++) {
                statement.execute(
                        "insert into memory.default.trino_e2e_source_table\n"
                                + "values(\n"
                                + "1,\n"
                                + "true,\n"
                                + "cast(127 as tinyint),\n"
                                + "cast(32767 as smallint),\n"
                                + "3,\n"
                                + "1234567890,\n"
                                + "55.0005,\n"
                                + "67.89,\n"
                                + "123.45,\n"
                                + "'8',\n"
                                + "'VarcharCol',\n"
                                + "date '2024-01-01',\n"
                                + "time '12:01:01',\n"
                                + "timestamp '2024-01-01 12:01:01',\n"
                                + "VARBINARY 'str',\n"
                                + "json '{\"key\":\"val\"}'\n"
                                + ")");
            }
        } catch (Exception exception) {
            log.error(ExceptionUtils.getMessage(exception));
            throw new SeaTunnelRuntimeException(JdbcITErrorCode.INSERT_DATA_FAILED, exception);
        }
    }

    @Override
    String driverUrl() {
        return "https://repo1.maven.org/maven2/io/trino/trino-jdbc/460/trino-jdbc-460.jar";
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        return null;
    }

    @Override
    public String quoteIdentifier(String field) {
        return field;
    }

    @Override
    protected void clearTable(String database, String schema, String table) {
        // do nothing.
    }

    @Override
    GenericContainer<?> initContainer() {
        GenericContainer<?> container =
                new GenericContainer<>(TRINO_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(TRINO_ALIASES)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(TRINO_IMAGE)));
        container.setPortBindings(Lists.newArrayList(String.format("%s:%s", TRINO_PORT, "8080")));

        return container;
    }
}
