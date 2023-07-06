package org.apache.seatunnel.connectors.seatunnel.jdbc;
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

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.apache.commons.lang3.tuple.Pair;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcKingbaseIT extends AbstractJdbcIT {
    private static final String KINGBASE_IMAGE = "godmeowicesun/kinKINGBASE";
    private static final String KINGBASE_CONTAINER_HOST = "e2e_KINGBASEDb";
    private static final String KINGBASE_DATABASE = "seatunnel";
    private static final String KINGBASE_SOURCE = "e2e_table_source";
    private static final String KINGBASE_SINK = "e2e_table_sink";

    private static final String KINGBASE_USERNAME = "root";
    private static final String KINGBASE_PASSWORD = "root";
    private static final int KINGBASE_PORT = 54321;
    private static final String KINGBASE_URL = "jdbc:kingbase8://" + HOST + ":%s/test";
    private static final String DRIVER_CLASS = "com.kingbase8.Driver";

    private static final List<String> CONFIG_FILE =
            Lists.newArrayList("/jdbc_kingbase_source_and_sink.conf");
    private static final String CREATE_SQL =
            "CREATE TABLE IF NOT EXISTS kb_e2e_source_table (\n"
                    + "  gid SERIAL PRIMARY KEY,\n"
                    + "  text_col TEXT,\n"
                    + "  varchar_col VARCHAR(255),\n"
                    + "  char_col CHAR(10),\n"
                    + "  boolean_col bool,\n"
                    + "  smallint_col int2,\n"
                    + "  integer_col int4,\n"
                    + "  bigint_col BIGINT,\n"
                    + "  decimal_col DECIMAL(10, 2),\n"
                    + "  numeric_col NUMERIC(8, 4),\n"
                    + "  real_col float4,\n"
                    + "  double_precision_col float8,\n"
                    + "  smallserial_col SMALLSERIAL,\n"
                    + "  serial_col SERIAL,\n"
                    + "  bigserial_col BIGSERIAL,\n"
                    + "  date_col DATE,\n"
                    + "  timestamp_col TIMESTAMP,\n"
                    + "  bpchar_col BPCHAR(10),\n"
                    + "  age INT NOT null,\n"
                    + "  name VARCHAR(255) NOT null,\n"
                    + "  point geometry(POINT, 4326),\n"
                    + "  linestring geometry(LINESTRING, 4326),\n"
                    + "  polygon_colums geometry(POLYGON, 4326),\n"
                    + "  multipoint geometry(MULTIPOINT, 4326),\n"
                    + "  multilinestring geometry(MULTILINESTRING, 4326),\n"
                    + "  multipolygon geometry(MULTIPOLYGON, 4326),\n"
                    + "  geometrycollection geometry(GEOMETRYCOLLECTION, 4326),\n"
                    + "  geog geography(POINT, 4326)\n"
                    + ")";

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        String jdbcUrl = String.format(KINGBASE_URL, KINGBASE_PORT);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(KINGBASE_DATABASE, KINGBASE_SOURCE, fieldNames);

        return JdbcCase.builder()
                .dockerImage(KINGBASE_IMAGE)
                .networkAliases(KINGBASE_CONTAINER_HOST)
                .containerEnv(containerEnv)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                .port(KINGBASE_PORT)
                .localPort(KINGBASE_PORT)
                .jdbcTemplate(KINGBASE_URL)
                .jdbcUrl(jdbcUrl)
                .userName(KINGBASE_USERNAME)
                .password(KINGBASE_PASSWORD)
                .database(KINGBASE_DATABASE)
                .sourceTable(KINGBASE_SOURCE)
                .sinkTable(KINGBASE_SINK)
                .createSql(CREATE_SQL)
                .configFile(CONFIG_FILE)
                .insertSql(insertSql)
                .testData(testDataSet)
                .build();
    }

    @Override
    void compareResult() throws SQLException, IOException {}

    @Override
    String driverUrl() {
        return "https://repo1.maven.org/maven2/cn/com/kingbase/kingbase8/8.6.0/kingbase8-8.6.0.jar";
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
    GenericContainer<?> initContainer() {
        GenericContainer<?> container =
                new GenericContainer<>(KINGBASE_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(KINGBASE_CONTAINER_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(KINGBASE_IMAGE)));

        container.setPortBindings(
                Lists.newArrayList(String.format("%s:%s", KINGBASE_PORT, KINGBASE_PORT)));

        return container;
    }

    @Override
    protected void createSchemaIfNeeded() {
        String sql = "CREATE DATABASE " + KINGBASE_DATABASE;
        try {
            connection.prepareStatement(sql).executeUpdate();
        } catch (Exception e) {
            throw new SeaTunnelRuntimeException(
                    JdbcITErrorCode.CREATE_TABLE_FAILED, "Fail to execute sql " + sql, e);
        }
    }
}
