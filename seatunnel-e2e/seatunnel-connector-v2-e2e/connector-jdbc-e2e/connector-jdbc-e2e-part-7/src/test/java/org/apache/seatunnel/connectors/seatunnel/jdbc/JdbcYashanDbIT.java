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
import org.apache.seatunnel.shade.org.apache.commons.lang3.tuple.Pair;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.awaitility.Awaitility;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
public class JdbcYashanDbIT extends AbstractJdbcIT {

    private static final String YASHANDB_IMAGE = "yasdb/yashandb:23.4.7.100";
    private static final String YASHANDB_CONTAINER_HOST = "e2e_yashandb";
    private static final String YASHANDB_SCHEMA = "SYS";
    private static final String YASHANDB_DATABASE = "SYS";
    private static final String YASHANDB_SOURCE = "E2E_TABLE_SOURCE";
    private static final String YASHANDB_SINK = "E2E_TABLE_SINK";
    private static final String YASHANDB_USERNAME = "SYS";
    private static final String YASHANDB_PASSWORD = "Cod-2022";
    private static final int YASHANDB_PORT = 1688;
    private static final String YASHANDB_URL = "jdbc:yasdb://" + HOST + ":%s/%s";

    private static final String DRIVER_CLASS = "com.yashandb.jdbc.Driver";

    private static final List<String> CONFIG_FILE =
            Lists.newArrayList(
                    "/jdbc_yashandb_source_and_sink.conf", "/jdbc_yashandb_source_to_sink.conf");

    private static final String CREATE_SQL =
            "CREATE TABLE %s (\n"
                    + "    YAS_TINYINT       TINYINT,\n"
                    + "    YAS_SMALLINT      SMALLINT,\n"
                    + "    YAS_INT           INT,\n"
                    + "    YAS_BIGINT        BIGINT,\n"
                    + "    YAS_FLOAT         FLOAT,\n"
                    + "    YAS_DOUBLE        DOUBLE,\n"
                    + "    YAS_NUMBER        NUMBER(10, 2),\n"
                    + "    YAS_CHAR          CHAR(20),\n"
                    + "    YAS_VARCHAR       VARCHAR(200),\n"
                    + "    YAS_CLOB          CLOB,\n"
                    + "    YAS_DATE          DATE,\n"
                    + "    YAS_TIMESTAMP     TIMESTAMP,\n"
                    + "    YAS_BOOLEAN       BOOLEAN,\n"
                    + "    PRIMARY KEY(YAS_INT)\n"
                    + ")";

    private static final String[] fieldNames =
            new String[] {
                "YAS_TINYINT",
                "YAS_SMALLINT",
                "YAS_INT",
                "YAS_BIGINT",
                "YAS_FLOAT",
                "YAS_DOUBLE",
                "YAS_NUMBER",
                "YAS_CHAR",
                "YAS_VARCHAR",
                "YAS_CLOB",
                "YAS_DATE",
                "YAS_TIMESTAMP",
                "YAS_BOOLEAN"
            };

    @Override
    JdbcCase getJdbcCase() {
        String jdbcUrl = String.format(YASHANDB_URL, YASHANDB_PORT, YASHANDB_DATABASE);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(YASHANDB_SCHEMA, YASHANDB_SOURCE, fieldNames);

        return JdbcCase.builder()
                .dockerImage(YASHANDB_IMAGE)
                .networkAliases(YASHANDB_CONTAINER_HOST)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                .port(YASHANDB_PORT)
                .localPort(YASHANDB_PORT)
                .jdbcTemplate(YASHANDB_URL)
                .jdbcUrl(jdbcUrl)
                .userName(YASHANDB_USERNAME)
                .password(YASHANDB_PASSWORD)
                .schema(YASHANDB_SCHEMA)
                .database(YASHANDB_DATABASE)
                .sourceTable(YASHANDB_SOURCE)
                .sinkTable(YASHANDB_SINK)
                .createSql(CREATE_SQL)
                .configFile(CONFIG_FILE)
                .insertSql(insertSql)
                .testData(testDataSet)
                .build();
    }

    @Override
    void checkResult(String executeKey, TestContainer container, Container.ExecResult execResult) {
        defaultCompare(executeKey, fieldNames, "YAS_INT");
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        List<SeaTunnelRow> rows = new ArrayList<>();
        LocalDate baseDate = LocalDate.of(2024, 1, 1);
        LocalDateTime baseDateTime = LocalDateTime.of(2024, 1, 1, 9, 0, 0);
        for (int i = 0; i < 100; i++) {
            LocalDate rowDate = baseDate.plusDays(i);
            LocalDateTime rowDateTime = baseDateTime.plusSeconds(i);
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                (byte) i,
                                (short) i,
                                i,
                                (long) i,
                                1.1f,
                                1.1d,
                                BigDecimal.valueOf(i, 2),
                                String.format("char_%s", i),
                                String.format("varchar_%s", i),
                                String.format("clob_%s", i),
                                Date.valueOf(rowDate),
                                Timestamp.valueOf(rowDateTime),
                                i % 2 == 0
                            });
            rows.add(row);
        }

        return Pair.of(fieldNames, rows);
    }

    @Override
    protected void beforeStartUP() {
        // YashanDB port opens well before the database is fully deployed (~156s).
        // Wait for the init success message in container logs before proceeding.
        Awaitility.await()
                .atMost(5, TimeUnit.MINUTES)
                .until(() -> dbServer.getLogs().contains("yashandb init success"));
        log.info("YashanDB initialization completed");
    }

    @Override
    protected GenericContainer<?> initContainer() {
        GenericContainer<?> container =
                new GenericContainer<>(YASHANDB_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(YASHANDB_CONTAINER_HOST)
                        .withEnv("SYS_PASSWD", YASHANDB_PASSWORD)
                        .waitingFor(
                                Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(5)))
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(YASHANDB_IMAGE)));
        container.addExposedPort(YASHANDB_PORT);

        return container;
    }

    @Override
    public String quoteIdentifier(String field) {
        return "\"" + field + "\"";
    }

    @Override
    protected void clearTable(String database, String schema, String table) {
        clearTable(schema, table);
    }

    @Override
    protected String buildTableInfoWithSchema(String database, String schema, String table) {
        return buildTableInfoWithSchema(schema, table);
    }
}
