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

import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.apache.commons.lang3.tuple.Pair;

import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class JdbcKingbaseIT extends AbstractJdbcIT {
    private static final String KINGBASE_IMAGE = "huzhihui/kingbase:v8r6";
    private static final String KINGBASE_CONTAINER_HOST = "e2e_KINGBASEDb";
    private static final String KINGBASE_DATABASE = "test";
    private static final String KINGBASE_SCHEMA = "public";
    private static final String KINGBASE_SOURCE = "e2e_table_source";
    private static final String KINGBASE_SINK = "e2e_table_sink";

    private static final String KINGBASE_USERNAME = "SYSTEM";
    private static final String KINGBASE_PASSWORD = "123456";
    private static final int KINGBASE_PORT = 54321;
    private static final String KINGBASE_URL = "jdbc:kingbase8://" + HOST + ":%s/test";
    private static final String DRIVER_CLASS = "com.kingbase8.Driver";

    private static final List<String> CONFIG_FILE =
            Lists.newArrayList("/jdbc_kingbase_source_and_sink.conf");
    private static final String CREATE_SQL =
            "create table %s \n" +
                    "(\n" +
                    "    c1  SMALLSERIAL,\n" +
                    "    c2  SERIAL,\n" +
                    "    c3  BIGSERIAL,\n" +
                    "    c4  BYTEA,\n" +
                    "    c5  _BYTEA,\n" +
                    "    c6  INT2,\n" +
                    "    c7  _INT2,\n" +
                    "    c8  INT4,\n" +
                    "    c9  _INT4,\n" +
                    "    c10 INT8,\n" +
                    "    c11 _INT8,\n" +
                    "    c12 FLOAT4,\n" +
                    "    c13 _FLOAT4,\n" +
                    "    c14 FLOAT8,\n" +
                    "    c15 _FLOAT8,\n" +
                    "    c16 NUMERIC,\n" +
                    "    c17 BOOL,\n" +
                    "    c18 _BOOL,\n" +
                    "    c19 TIMESTAMP,\n" +
                    "    c20 DATE,\n" +
                    "    c21 TIME,\n" +
                    "    c22 TEXT,\n" +
                    "    c23 _TEXT,\n" +
                    "    c24 BPCHAR,\n" +
                    "    c25 _BPCHAR,\n" +
                    "    c26 CHARACTER,\n" +
                    "    c27 VARCHAR,\n" +
                    "    c28 _VARCHAR\n" +
                    ");\n";

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        String jdbcUrl = String.format(KINGBASE_URL, KINGBASE_PORT);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(KINGBASE_SCHEMA, KINGBASE_SOURCE, fieldNames);

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
    void compareResult() throws SQLException, IOException {
    }

    @Override
    String driverUrl() {
        return "https://repo1.maven.org/maven2/cn/com/kingbase/kingbase8/8.6.0/kingbase8-8.6.0.jar";
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        String[] fieldNames =
                new String[]{
                        "c1",
                        "c2",
                        "c3",
                        "c4",
                        "c5",
                        "c6",
                        "c7",
                        "c8",
                        "c9",
                        "c10",
                        "c11",
                        "c12",
                        "c13",
                        "c14",
                        "c15",
                        "c16",
                        "c17",
                        "c18",
                        "c19",
                        "c20",
                        "c21",
                        "c22",
                        "c23",
                        "c24",
                        "c25",
                        "c26",
                        "c27",
                        "c28"
                };
        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[]{
                                    i,
                                    Long.parseLong(String.valueOf(i)),
                                    Long.parseLong(String.valueOf(i)),
                                    new byte[]{1, 2},
                                    new Byte[]{1, 2},
                                    (short) i,
                                    new Short[]{1, 2},
                                    i,
                                    new Integer[]{1, 2},
                                    Long.parseLong(String.valueOf(i)),
                                    new Long[]{1L, 2L},
                                    Float.parseFloat("1.1"),
                                    new Float[]{1.1F, 1.2F},
                                    Double.parseDouble("1.1"),
                                    new Double[]{1.1, 1.2},
                                    BigDecimal.valueOf(i, 10),
                                    true,
                                    new Boolean[]{false, true},
                                    LocalDateTime.now(),
                                    LocalDate.now(),
                                    LocalTime.now(),
                                    String.valueOf(i),
                                    new String[]{"1", "2"},
                                    String.valueOf(i),
                                    new String[]{"1", "2"},
                                    String.valueOf(i),
                                    String.valueOf(i),
                                    new String[]{"1", "2"}
                            });
            rows.add(row);
        }

        return Pair.of(fieldNames, rows);
    }

    @Override
    GenericContainer<?> initContainer() {
        GenericContainer<?> container = new GenericContainer<>(KINGBASE_IMAGE)
                .withEnv("KINGBASE_SYSTEM_PASSWORD", "123456")
                .withFileSystemBind("license.dat", "/home/kingbase/license.dat")
                .withLogConsumer(
                        new Slf4jLogConsumer(
                                DockerLoggerFactory.getLogger(KINGBASE_IMAGE)));
        container.setPortBindings(
                Lists.newArrayList(String.format("%s:%s", KINGBASE_PORT, KINGBASE_PORT)));
        return container;
    }

    protected void createNeededTables() {
        try (Statement statement = connection.createStatement()) {
            String createTemplate = jdbcCase.getCreateSql();

            String createSource =
                    String.format(
                            createTemplate, KINGBASE_SCHEMA + "." + jdbcCase.getSourceTable());
            String createSink =
                    String.format(
                            createTemplate, KINGBASE_SCHEMA + "." + jdbcCase.getSinkTable());

            statement.execute(createSource);
            statement.execute(createSink);

            connection.commit();
        } catch (Exception exception) {
            log.error(ExceptionUtils.getMessage(exception));
            throw new SeaTunnelRuntimeException(JdbcITErrorCode.CREATE_TABLE_FAILED, exception);
        }
    }
    public String insertTable(String schema, String table, String... fields) {
        String columns =
                String.join(", ", fields);
        String placeholders = Arrays.stream(fields).map(f -> "?").collect(Collectors.joining(", "));

        return "INSERT INTO "
                + schema +
                "." +
                table
                + " ("
                + columns
                + " )"
                + " VALUES ("
                + placeholders
                + ")";
    }

}
