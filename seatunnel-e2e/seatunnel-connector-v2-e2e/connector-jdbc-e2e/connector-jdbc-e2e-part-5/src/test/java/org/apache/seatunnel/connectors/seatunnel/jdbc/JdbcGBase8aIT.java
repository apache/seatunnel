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
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.apache.commons.lang3.tuple.Pair;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcGBase8aIT extends AbstractJdbcIT {

    private static final String GBASE_IMAGE = "shihd/gbase8a:1.0";
    private static final String GBASE_CONTAINER_HOST = "e2e_gbase8aDb";
    private static final String GBASE_DATABASE = "seatunnel";
    private static final String GBASE_SOURCE = "e2e_table_source";
    private static final String GBASE_SINK = "e2e_table_sink";

    private static final String GBASE_USERNAME = "root";
    private static final String GBASE_PASSWORD = "root";
    private static final int GBASE_PORT = 5258;
    private static final String GBASE_URL =
            "jdbc:gbase://"
                    + HOST
                    + ":%s/gbase?useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true";

    private static final String DRIVER_CLASS = "com.gbase.jdbc.Driver";

    private static final List<String> CONFIG_FILE =
            Lists.newArrayList("/jdbc_gbase8a_source_to_assert.conf");
    private static final String CREATE_SQL =
            "CREATE TABLE %s\n"
                    + "(\n"
                    + "    varchar_10_col varchar(10)        DEFAULT NULL,\n"
                    + "    char_10_col    char(10)           DEFAULT NULL,\n"
                    + "    text_col       text,\n"
                    + "    decimal_col    decimal(10, 0)     DEFAULT NULL,\n"
                    + "    float_col      float(12, 0)       DEFAULT NULL,\n"
                    + "    int_col        int(11)            DEFAULT NULL,\n"
                    + "    tinyint_col    tinyint(4)         DEFAULT NULL,\n"
                    + "    smallint_col   smallint(6)        DEFAULT NULL,\n"
                    + "    double_col     double(22, 0)      DEFAULT NULL,\n"
                    + "    bigint_col     bigint(20)         DEFAULT NULL,\n"
                    + "    date_col       date               DEFAULT NULL,\n"
                    + "    timestamp_col  timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
                    + "    datetime_col   datetime           DEFAULT NULL,\n"
                    + "    blob_col       blob\n"
                    + ");";

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        String jdbcUrl = String.format(GBASE_URL, GBASE_PORT);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(GBASE_DATABASE, GBASE_SOURCE, fieldNames);

        return JdbcCase.builder()
                .dockerImage(GBASE_IMAGE)
                .networkAliases(GBASE_CONTAINER_HOST)
                .containerEnv(containerEnv)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                .port(GBASE_PORT)
                .localPort(GBASE_PORT)
                .jdbcTemplate(GBASE_URL)
                .jdbcUrl(jdbcUrl)
                .userName(GBASE_USERNAME)
                .password(GBASE_PASSWORD)
                .database(GBASE_DATABASE)
                .sourceTable(GBASE_SOURCE)
                .sinkTable(GBASE_SINK)
                .createSql(CREATE_SQL)
                .configFile(CONFIG_FILE)
                .insertSql(insertSql)
                .testData(testDataSet)
                .build();
    }

    @Override
    void compareResult() {}

    @Override
    String driverUrl() {
        return "https://www.gbase8.cn/wp-content/uploads/2020/10/gbase-connector-java-8.3.81.53-build55.5.7-bin_min_mix.jar";
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        String[] fieldNames =
                new String[] {
                    "varchar_10_col",
                    "char_10_col",
                    "text_col",
                    "decimal_col",
                    "float_col",
                    "int_col",
                    "tinyint_col",
                    "smallint_col",
                    "double_col",
                    "bigint_col",
                    "date_col",
                    "timestamp_col",
                    "datetime_col",
                    "blob_col"
                };

        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                String.format("f1_%s", i),
                                String.format("f1_%s", i),
                                String.format("f1_text_%s", i),
                                BigDecimal.valueOf(i, 10),
                                Float.parseFloat("1.1"),
                                i,
                                Short.valueOf("1"),
                                Short.valueOf("1"),
                                Double.parseDouble("1.1"),
                                Long.parseLong("1"),
                                Date.valueOf(LocalDate.now()),
                                new Timestamp(System.currentTimeMillis()),
                                Timestamp.valueOf(LocalDateTime.now()),
                                "test".getBytes()
                            });
            rows.add(row);
        }

        return Pair.of(fieldNames, rows);
    }

    @Override
    protected Class<?> loadDriverClass() {
        return super.loadDriverClassFromUrl();
    }

    @Override
    GenericContainer<?> initContainer() {
        GenericContainer<?> container =
                new GenericContainer<>(GBASE_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(GBASE_CONTAINER_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(GBASE_IMAGE)));

        container.setPortBindings(
                Lists.newArrayList(String.format("%s:%s", GBASE_PORT, GBASE_PORT)));

        return container;
    }

    @Override
    protected void createSchemaIfNeeded() {
        String sql = "CREATE DATABASE " + GBASE_DATABASE;
        try {
            connection.prepareStatement(sql).executeUpdate();
        } catch (Exception e) {
            throw new SeaTunnelRuntimeException(
                    JdbcITErrorCode.CREATE_TABLE_FAILED, "Fail to execute sql " + sql, e);
        }
    }
}
