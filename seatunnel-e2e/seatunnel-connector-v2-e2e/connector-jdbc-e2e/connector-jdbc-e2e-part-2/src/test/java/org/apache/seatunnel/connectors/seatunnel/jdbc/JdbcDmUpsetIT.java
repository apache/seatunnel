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

public class JdbcDmUpsetIT extends AbstractJdbcIT {

    private static final String DM_IMAGE = "laglangyue/dmdb8";
    private static final String DM_CONTAINER_HOST = "e2e_dmdb";

    private static final String DM_DATABASE = "SYSDBA";
    private static final String DM_SOURCE = "e2e_table_source_upset";
    private static final String DM_SINK = "e2e_table_sink_upset";
    private static final String DM_USERNAME = "SYSDBA";
    private static final String DM_PASSWORD = "SYSDBA";
    private static final int DM_PORT = 5336;
    private static final String DM_URL = "jdbc:dm://" + HOST + ":%s";

    private static final String DRIVER_CLASS = "dm.jdbc.driver.DmDriver";

    private static final List<String> CONFIG_FILE =
            Lists.newArrayList("/jdbc_dm_source_and_dm_upset_sink.conf");
    private static final String CREATE_SQL =
            "create table if not exists %s"
                    + "(\n"
                    + "    DM_BIT              BIT,\n"
                    + "    DM_INT              INT,\n"
                    + "    DM_INTEGER          INTEGER,\n"
                    + "    DM_TINYINT          TINYINT,\n"
                    + "\n"
                    + "    DM_BYTE             BYTE,\n"
                    + "    DM_SMALLINT         SMALLINT,\n"
                    + "    DM_BIGINT           BIGINT,\n"
                    + "\n"
                    + "    DM_NUMBER           NUMBER,\n"
                    + "    DM_DECIMAL          DECIMAL,\n"
                    + "    DM_FLOAT            FLOAT,\n"
                    + "    DM_DOUBLE_PRECISION DOUBLE PRECISION,\n"
                    + "    DM_DOUBLE           DOUBLE,\n"
                    + "\n"
                    + "    DM_CHAR             CHAR,\n"
                    + "    DM_VARCHAR          VARCHAR,\n"
                    + "    DM_VARCHAR2         VARCHAR2,\n"
                    + "    DM_TEXT             TEXT,\n"
                    + "    DM_LONG             LONG,\n"
                    + "\n"
                    + "    DM_TIMESTAMP        TIMESTAMP,\n"
                    + "    DM_DATETIME         DATETIME,\n"
                    + "    DM_DATE             DATE\n"
                    + ")";

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        String jdbcUrl = String.format(DM_URL, DM_PORT);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(DM_DATABASE, DM_SOURCE, fieldNames);

        return JdbcCase.builder()
                .dockerImage(DM_IMAGE)
                .networkAliases(DM_CONTAINER_HOST)
                .containerEnv(containerEnv)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                .port(DM_PORT)
                .localPort(DM_PORT)
                .jdbcTemplate(DM_URL)
                .jdbcUrl(jdbcUrl)
                .userName(DM_USERNAME)
                .password(DM_PASSWORD)
                .database(DM_DATABASE)
                .sourceTable(DM_SOURCE)
                .sinkTable(DM_SINK)
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
        return "https://repo1.maven.org/maven2/com/dameng/DmJdbcDriver18/8.1.1.193/DmJdbcDriver18-8.1.1.193.jar";
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        String[] fieldNames =
                new String[] {
                    "DM_BIT",
                    "DM_INT",
                    "DM_INTEGER",
                    "DM_TINYINT",
                    "DM_BYTE",
                    "DM_SMALLINT",
                    "DM_BIGINT",
                    "DM_NUMBER",
                    "DM_DECIMAL",
                    "DM_FLOAT",
                    "DM_DOUBLE_PRECISION",
                    "DM_DOUBLE",
                    "DM_CHAR",
                    "DM_VARCHAR",
                    "DM_VARCHAR2",
                    "DM_TEXT",
                    "DM_LONG",
                    "DM_TIMESTAMP",
                    "DM_DATETIME",
                    "DM_DATE"
                };

        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                i % 2 == 0 ? (byte) 1 : (byte) 0,
                                i,
                                i,
                                Short.valueOf("1"),
                                Byte.valueOf("1"),
                                i,
                                Long.parseLong("1"),
                                BigDecimal.valueOf(i, 18),
                                BigDecimal.valueOf(i, 18),
                                Float.parseFloat("1.1"),
                                Double.parseDouble("1.1"),
                                Double.parseDouble("1.1"),
                                'f',
                                String.format("f1_%s", i),
                                String.format("f1_%s", i),
                                String.format("f1_%s", i),
                                String.format("{\"aa\":\"bb_%s\"}", i),
                                Timestamp.valueOf(LocalDateTime.now()),
                                new Timestamp(System.currentTimeMillis()),
                                Date.valueOf(LocalDate.now())
                            });
            rows.add(row);
        }

        return Pair.of(fieldNames, rows);
    }

    @Override
    protected GenericContainer<?> initContainer() {
        GenericContainer<?> container =
                new GenericContainer<>(DM_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(DM_CONTAINER_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(DM_IMAGE)));
        container.setPortBindings(Lists.newArrayList(String.format("%s:%s", 5336, 5236)));

        return container;
    }

    @Override
    public String quoteIdentifier(String field) {
        return "\"" + field + "\"";
    }
}
