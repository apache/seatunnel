/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;
import org.apache.seatunnel.shade.org.apache.commons.lang3.tuple.Pair;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.e2e.common.TestSuiteBase;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcSybaseIT extends AbstractJdbcIT {

    private static final String SYBASE_IMAGE = "ifnazar/sybase_15_7";
    private static final String SYBASE_CONTAINER_HOST = "sybase";
    private static final String SYBASE_SOURCE = "source_table";
    private static final String SYBASE_SINK = "sink_table";
    private static final String SYBASE_DATABASE = "master";
    private static final String SYBASE_SCHEMA = "dbo";
    private static final int SYBASE_CONTAINER_PORT = 5000;
    private static final String SYBASE_URL =
            "jdbc:jtds:sybase://" + AbstractJdbcIT.HOST + ":%s/" + SYBASE_DATABASE;
    private static final String DRIVER_CLASS = "net.sourceforge.jtds.jdbc.Driver";
    private static final List<String> CONFIG_FILE =
            Lists.newArrayList("/jdbc_sybase_source_and_assert.conf");

    private static final String CREATE_SQL =
            "CREATE TABLE %s (\n"
                    + "\tINT_TEST int NOT NULL,\n"
                    + "\tBIGINT_TEST bigint NULL,\n"
                    + "\tBIT_TEST bit NOT NULL,\n"
                    + "\tCHAR_TEST char(255) NULL,\n"
                    + "\tDATE_TEST date NULL,\n"
                    + "\tDATETIME_TEST datetime NULL,\n"
                    + "\tDECIMAL_TEST decimal(18,2) NULL,\n"
                    + "\tFLOAT_TEST float NULL,\n"
                    + "\tMONEY_TEST money NULL,\n"
                    + "\tNUMERIC_TEST numeric(18,2) NULL,\n"
                    + "\tREAL_TEST real NULL,\n"
                    + "\tSMALLDATETIME_TEST smalldatetime NULL,\n"
                    + "\tSMALLINT_TEST smallint NULL,\n"
                    + "\tTEXT_TEST text NULL,\n"
                    + "\tTIME_TEST time NULL,\n"
                    + "\tTINYINT_TEST tinyint NULL,\n"
                    + "\tVARCHAR_TEST varchar(255) NULL\n"
                    + ")";

    private static final String SINK_CREATE_SQL = CREATE_SQL;

    private String username;
    private String password;

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        String jdbcUrl = String.format(SYBASE_URL, SYBASE_CONTAINER_PORT);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(SYBASE_SCHEMA, SYBASE_SOURCE, fieldNames);

        return JdbcCase.builder()
                .dockerImage(SYBASE_IMAGE)
                .networkAliases(SYBASE_CONTAINER_HOST)
                .containerEnv(containerEnv)
                .driverClass(DRIVER_CLASS)
                .host(AbstractJdbcIT.HOST)
                .port(SYBASE_CONTAINER_PORT)
                .localPort(SYBASE_CONTAINER_PORT)
                .jdbcTemplate(SYBASE_URL)
                .jdbcUrl(jdbcUrl)
                .userName(username)
                .password(password)
                .database(SYBASE_DATABASE)
                .schema(SYBASE_SCHEMA)
                .sourceTable(SYBASE_SOURCE)
                .sinkTable(SYBASE_SINK)
                .createSql(CREATE_SQL)
                .sinkCreateSql(SINK_CREATE_SQL)
                .configFile(CONFIG_FILE)
                .insertSql(insertSql)
                .testData(testDataSet)
                .tablePathFullName(
                        TablePath.of(SYBASE_DATABASE, SYBASE_SCHEMA, SYBASE_SOURCE).getFullName())
                .build();
    }

    @Override
    protected void createSchemaIfNeeded() {
        // No custom user-defined type creation needed for V1 Sybase scope
    }

    @Override
    String driverUrl() {
        return "https://repo1.maven.org/maven2/net/sourceforge/jtds/jtds/1.3.1/jtds-1.3.1.jar";
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        String[] fieldNames =
                new String[] {
                    "INT_TEST",
                    "BIGINT_TEST",
                    "BIT_TEST",
                    "CHAR_TEST",
                    "DATE_TEST",
                    "DATETIME_TEST",
                    "DECIMAL_TEST",
                    "FLOAT_TEST",
                    "MONEY_TEST",
                    "NUMERIC_TEST",
                    "REAL_TEST",
                    "SMALLDATETIME_TEST",
                    "SMALLINT_TEST",
                    "TEXT_TEST",
                    "TIME_TEST",
                    "TINYINT_TEST",
                    "VARCHAR_TEST"
                };

        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                i, // INT_TEST
                                (long) i, // BIGINT_TEST
                                i % 2 == 0, // BIT_TEST
                                "CharValue" + i, // CHAR_TEST
                                java.sql.Date.valueOf(LocalDate.now()), // DATE_TEST
                                java.sql.Timestamp.valueOf(LocalDateTime.now()), // DATETIME_TEST
                                new BigDecimal("123.45"), // DECIMAL_TEST
                                3.14d, // FLOAT_TEST
                                new BigDecimal("567.89"), // MONEY_TEST
                                new BigDecimal("987.65"), // NUMERIC_TEST
                                2.71f, // REAL_TEST
                                java.sql.Timestamp.valueOf(
                                        LocalDateTime.now().withNano(0)), // SMALL DATETIME_TEST
                                (short) 123, // SMALLINT_TEST
                                "TextValue" + i, // TEXT_TEST
                                java.sql.Time.valueOf(LocalTime.now()), // TIME_TEST
                                (short) 5, // TINYINT_TEST
                                "VarCharValue" + i // VARCHAR_TEST
                            });
            rows.add(row);
        }

        return Pair.of(fieldNames, rows);
    }

    @Override
    GenericContainer<?> initContainer() {
        GenericContainer<?> container =
                new GenericContainer<>(DockerImageName.parse(SYBASE_IMAGE))
                        .withNetwork(TestSuiteBase.NETWORK)
                        .withNetworkAliases(SYBASE_CONTAINER_HOST)
                        .withCreateContainerCmdModifier(cmd -> cmd.withHostName("dksybase"))
                        .withCommand("bash", "/sybase/start")
                        .withStartupTimeout(Duration.ofMinutes(5))
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(SYBASE_IMAGE)));

        container.setPortBindings(
                Lists.newArrayList(
                        String.format("%s:%s", SYBASE_CONTAINER_PORT, SYBASE_CONTAINER_PORT)));
        container.withExposedPorts(SYBASE_CONTAINER_PORT);
        container.withPrivilegedMode(true);

        username = "sa";
        password = "password";

        return container;
    }

    @Override
    public String quoteIdentifier(String field) {
        return "\"" + field + "\"";
    }

    @Override
    public void clearTable(String schema, String table) {
        try {
            connection.createStatement().execute("TRUNCATE TABLE " + quoteIdentifier(table));
        } catch (Exception e) {
            // Ignore
        }
    }

    @Override
    protected String buildTableInfoWithSchema(String database, String schema, String table) {
        return buildTableInfoWithSchema(schema, table);
    }
}
