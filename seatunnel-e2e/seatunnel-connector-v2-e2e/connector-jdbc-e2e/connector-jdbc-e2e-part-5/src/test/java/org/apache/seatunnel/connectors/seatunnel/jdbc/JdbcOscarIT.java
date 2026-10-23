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

import org.apache.seatunnel.shade.com.google.common.collect.Lists;
import org.apache.seatunnel.shade.org.apache.commons.lang3.tuple.Pair;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerLoggerFactory;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcOscarIT extends AbstractJdbcIT {
    private static final String OSCAR_IMAGE = "shentongdata/shentongdb:251217-825.2-linux64";
    private static final String OSCAR_CONTAINER_HOST = "e2e_shentongdb";

    private static final String OSCAR_DATABASE = "OSRDB";
    private static final String OSCAR_SCHEMA = "SYSDBA";
    private static final String OSCAR_SOURCE = "E2E_TABLE_SOURCE";
    private static final String OSCAR_SINK = "E2E_TABLE_SINK";
    private static final String OSCAR_USERNAME = "SYSDBA";
    private static final String OSCAR_PASSWORD = "szoscar55";
    private static final int OSCAR_PORT = 2003;
    private static final String OSCAR_URL = "jdbc:oscar://" + HOST + ":%s";

    private static final String DRIVER_CLASS = "com.oscar.Driver";

    private static final List<String> CONFIG_FILE =
            Lists.newArrayList("/jdbc_oscar_source_and_sink.conf");
    private static final String CREATE_SQL =
            "create table if not exists %s"
                    + "(\n"
                    + "    OSCAR_BIT              BIT,\n"
                    + "    OSCAR_INT1              INT1,\n"
                    + "    OSCAR_INTEGER          INTEGER,\n"
                    + "    OSCAR_TINYINT          TINYINT,\n"
                    + "\n"
                    + "    OSCAR_SMALLINT         SMALLINT,\n"
                    + "    OSCAR_BIGINT           BIGINT,\n"
                    + "\n"
                    + "    OSCAR_NUMERIC          NUMERIC,\n"
                    + "    OSCAR_NUMBER           NUMBER,\n"
                    + "    OSCAR_DECIMAL          DECIMAL,\n"
                    + "\n"
                    + "    OSCAR_REAL             REAL,\n"
                    + "    OSCAR_FLOAT            FLOAT,\n"
                    + "    OSCAR_DOUBLE_PRECISION DOUBLE PRECISION,\n"
                    + "    OSCAR_DOUBLE           DOUBLE,\n"
                    + "\n"
                    + "    OSCAR_CHAR             CHAR,\n"
                    + "    OSCAR_CHARACTER        CHARACTER,\n"
                    + "    OSCAR_VARCHAR          VARCHAR(10),\n"
                    + "    OSCAR_VARCHAR2         VARCHAR2(10),\n"
                    + "    OSCAR_TEXT             TEXT,\n"
                    + "    OSCAR_LONG             LONG,\n"
                    + "    OSCAR_CLOB             CLOB,\n"
                    + "\n"
                    + "    OSCAR_TIMESTAMP        TIMESTAMP,\n"
                    + "    OSCAR_DATETIME         DATETIME,\n"
                    + "    OSCAR_DATE             DATE,\n"
                    + "\n"
                    + "    OSCAR_BLOB             BLOB\n"
                    + ")";

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        String jdbcUrl = String.format(OSCAR_URL, OSCAR_PORT);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(OSCAR_SCHEMA, OSCAR_SOURCE, fieldNames);

        return JdbcCase.builder()
                .dockerImage(OSCAR_IMAGE)
                .networkAliases(OSCAR_CONTAINER_HOST)
                .containerEnv(containerEnv)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                .port(OSCAR_PORT)
                .localPort(OSCAR_PORT)
                .jdbcTemplate(OSCAR_URL)
                .jdbcUrl(jdbcUrl)
                .userName(OSCAR_USERNAME)
                .password(OSCAR_PASSWORD)
                .database(OSCAR_DATABASE)
                .sourceTable(OSCAR_SOURCE)
                .sinkTable(OSCAR_SINK)
                .createSql(CREATE_SQL)
                .configFile(CONFIG_FILE)
                .insertSql(insertSql)
                .testData(testDataSet)
                .tablePathFullName(String.format("%s.%s", OSCAR_SCHEMA, OSCAR_SOURCE))
                .build();
    }

    @Override
    String driverUrl() {
        return "https://repo1.maven.org/maven2/com/shentongdata/oscarJDBC8/4.1.152/oscarJDBC8-4.1.152.jar";
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        String[] fieldNames =
                new String[] {
                    "OSCAR_BIT",
                    "OSCAR_INT1",
                    "OSCAR_INTEGER",
                    "OSCAR_TINYINT",
                    "OSCAR_SMALLINT",
                    "OSCAR_BIGINT",
                    "OSCAR_NUMERIC",
                    "OSCAR_NUMBER",
                    "OSCAR_DECIMAL",
                    "OSCAR_REAL",
                    "OSCAR_FLOAT",
                    "OSCAR_DOUBLE_PRECISION",
                    "OSCAR_DOUBLE",
                    "OSCAR_CHAR",
                    "OSCAR_CHARACTER",
                    "OSCAR_VARCHAR",
                    "OSCAR_VARCHAR2",
                    "OSCAR_TEXT",
                    "OSCAR_LONG",
                    "OSCAR_CLOB",
                    "OSCAR_TIMESTAMP",
                    "OSCAR_DATETIME",
                    "OSCAR_DATE",
                    "OSCAR_BLOB"
                };

        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                i % 2 == 0,
                                i,
                                i,
                                Short.valueOf("1"),
                                Byte.valueOf("1"),
                                Long.parseLong("1"),
                                BigDecimal.valueOf(i, 0),
                                BigDecimal.valueOf(i, 18),
                                BigDecimal.valueOf(i, 18),
                                Float.parseFloat("1.1"),
                                Float.parseFloat("1.1"),
                                Double.parseDouble("1.1"),
                                Double.parseDouble("1.1"),
                                'f',
                                'f',
                                String.format("f1_%s", i),
                                String.format("f1_%s", i),
                                String.format("f1_%s", i),
                                String.format("{\"aa\":\"bb_%s\"}", i),
                                String.format("f1_%s", i),
                                Timestamp.valueOf(LocalDateTime.now()),
                                new Timestamp(System.currentTimeMillis()),
                                Date.valueOf(LocalDate.now()),
                                null
                            });
            rows.add(row);
        }

        return Pair.of(fieldNames, rows);
    }

    protected String buildTableInfoWithSchema(String database, String schema, String table) {
        return buildTableInfoWithSchema(schema, table);
    }

    protected void clearTable(String database, String schema, String table) {
        clearTable(schema, table);
    }

    @Override
    protected GenericContainer<?> initContainer() {

        GenericContainer<?> container =
                new GenericContainer<>(OSCAR_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(OSCAR_CONTAINER_HOST)
                        .withExposedPorts(2003)
                        .withStartupTimeout(Duration.ofSeconds(3600))
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(OSCAR_IMAGE)));
        container.setPortBindings(Lists.newArrayList(String.format("%s:%s", 2003, 2003)));

        return container;
    }

    @Override
    public String quoteIdentifier(String field) {
        return "\"" + field + "\"";
    }
}
