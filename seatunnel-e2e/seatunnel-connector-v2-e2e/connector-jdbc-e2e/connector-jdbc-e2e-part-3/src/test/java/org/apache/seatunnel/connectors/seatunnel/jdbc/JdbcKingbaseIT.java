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

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.ExceptionUtils;

import org.apache.commons.lang3.tuple.Pair;

import org.junit.jupiter.api.Disabled;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerLoggerFactory;

import java.math.BigDecimal;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * If you want to run this e2e, you need to download km license from
 * https://www.kingbase.com.cn/sqwjxz/index.htm and modify the KM_LICENSE_PATH variable to the
 * address where you downloaded the certificate. Also, remove the @Disabled annotation. The spark
 * engine does not support the TIME type.Two environment variables need to be added to the spark
 * container: "LANG"="C.UTF-8", "JAVA_TOOL_OPTIONS"="-Dfile.encoding=UTF8"
 */
@Disabled("Due to copyright reasons, you need to download the trial version km license yourself")
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
    private static final String KM_LICENSE_PATH = "KM_LICENSE_PATH";

    private static final List<String> CONFIG_FILE =
            Lists.newArrayList("/jdbc_kingbase_source_and_sink.conf");
    private static final String CREATE_SQL =
            "create table %s \n"
                    + "(\n"
                    + "    c1  SMALLSERIAL,\n"
                    + "    c2  SERIAL,\n"
                    + "    c3  BIGSERIAL,\n"
                    + "    c5  INT2,\n"
                    + "    c7  INT4,\n"
                    + "    c9 INT8,\n"
                    + "    c11 FLOAT4,\n"
                    + "    c13 FLOAT8,\n"
                    + "    c15 NUMERIC,\n"
                    + "    c16 BOOL,\n"
                    + "    c18 TIMESTAMP,\n"
                    + "    c19 DATE,\n"
                    + "    c20 TIME,\n"
                    + "    c21 TEXT,\n"
                    + "    c23 BPCHAR,\n"
                    + "    c25 CHARACTER,\n"
                    + "    c26 VARCHAR\n"
                    + ");\n";

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
    String driverUrl() {
        return "https://repo1.maven.org/maven2/cn/com/kingbase/kingbase8/8.6.0/kingbase8-8.6.0.jar";
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        String[] fieldNames =
                new String[] {
                    "c1", "c2", "c3", "c5", "c7", "c9", "c11", "c13", "c15", "c16", "c18", "c19",
                    "c20", "c21", "c23", "c25", "c26"
                };
        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                i,
                                Long.parseLong(String.valueOf(i)),
                                Long.parseLong(String.valueOf(i)),
                                (short) i,
                                i,
                                Long.parseLong(String.valueOf(i)),
                                Float.parseFloat("1.1"),
                                Double.parseDouble("1.1"),
                                BigDecimal.valueOf(i, 10),
                                true,
                                LocalDateTime.now(),
                                LocalDate.now(),
                                LocalTime.now(),
                                String.valueOf(i),
                                String.valueOf(i),
                                String.valueOf(1),
                                String.valueOf(i)
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
                        .withEnv("KINGBASE_SYSTEM_PASSWORD", "123456")
                        .withFileSystemBind(KM_LICENSE_PATH, "/home/kingbase/license.dat")
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
                    String.format(createTemplate, KINGBASE_SCHEMA + "." + jdbcCase.getSinkTable());

            statement.execute(createSource);
            statement.execute(createSink);

            connection.commit();
        } catch (Exception exception) {
            log.error(ExceptionUtils.getMessage(exception));
            throw new SeaTunnelRuntimeException(JdbcITErrorCode.CREATE_TABLE_FAILED, exception);
        }
    }

    public String insertTable(String schema, String table, String... fields) {
        String columns = String.join(", ", fields);
        String placeholders = Arrays.stream(fields).map(f -> "?").collect(Collectors.joining(", "));

        return "INSERT INTO "
                + schema
                + "."
                + table
                + " ("
                + columns
                + " )"
                + " VALUES ("
                + placeholders
                + ")";
    }

    public void clearTable(String schema, String table) {}
}
