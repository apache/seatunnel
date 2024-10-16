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

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.xugu.XuguCatalog;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.apache.commons.lang3.tuple.Pair;

import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class JdbcXuguIT extends AbstractJdbcIT {

    private static final String XUGU_IMAGE = "xugudb/xugudb:v12";
    private static final String XUGU_CONTAINER_HOST = "e2e_xugudb";
    private static final String XUGU_SCHEMA = "SYSDBA";
    private static final String XUGU_DATABASE = "SYSTEM";
    private static final String XUGU_SOURCE = "e2e_table_source";
    private static final String XUGU_SINK = "e2e_table_sink";
    private static final String CATALOG_DATABASE = "catalog_database";
    private static final String CATALOG_TABLE = "e2e_table_catalog";
    private static final String XUGU_USERNAME = "SYSDBA";
    private static final String XUGU_PASSWORD = "SYSDBA";
    private static final int XUGU_PORT = 5138;
    private static final String XUGU_URL = "jdbc:xugu://" + HOST + ":%s/%s";

    private static final String DRIVER_CLASS = "com.xugu.cloudjdbc.Driver";

    private static final List<String> CONFIG_FILE =
            Lists.newArrayList(
                    "/jdbc_xugu_source_and_upsert_sink.conf", "/jdbc_xugu_source_and_sink.conf");
    private static final String CREATE_SQL =
            "create table if not exists %s"
                    + "(\n"
                    + "    XUGU_NUMERIC                   NUMERIC(10,2),\n"
                    + "    XUGU_NUMBER                    NUMBER(10,2),\n"
                    + "    XUGU_INTEGER                   INTEGER,\n"
                    + "    XUGU_INT                       INT,\n"
                    + "    XUGU_BIGINT                    BIGINT,\n"
                    + "    XUGU_TINYINT                   TINYINT,\n"
                    + "    XUGU_SMALLINT                  SMALLINT,\n"
                    + "    XUGU_FLOAT                     FLOAT,\n"
                    + "    XUGU_DOUBLE                    DOUBLE,\n"
                    + "    XUGU_CHAR                      CHAR,\n"
                    + "    XUGU_NCHAR                     NCHAR,\n"
                    + "    XUGU_VARCHAR                   VARCHAR,\n"
                    + "    XUGU_VARCHAR2                  VARCHAR2,\n"
                    + "    XUGU_CLOB                      CLOB,\n"
                    + "    XUGU_DATE                      DATE,\n"
                    + "    XUGU_TIME                      TIME,\n"
                    + "    XUGU_TIMESTAMP                 TIMESTAMP,\n"
                    + "    XUGU_DATETIME                  DATETIME,\n"
                    + "    XUGU_TIME_WITH_TIME_ZONE       TIME WITH TIME ZONE,\n"
                    + "    XUGU_TIMESTAMP_WITH_TIME_ZONE  TIMESTAMP WITH TIME ZONE,\n"
                    + "    XUGU_BINARY                    BINARY,\n"
                    + "    XUGU_BLOB                      BLOB,\n"
                    + "    XUGU_GUID                      GUID,\n"
                    + "    XUGU_BOOLEAN                   BOOLEAN,\n"
                    + "    CONSTRAINT \"XUGU_PK\" PRIMARY KEY(XUGU_INT)"
                    + ")";
    private static final String[] fieldNames =
            new String[] {
                "XUGU_NUMERIC",
                "XUGU_NUMBER",
                "XUGU_INTEGER",
                "XUGU_INT",
                "XUGU_BIGINT",
                "XUGU_TINYINT",
                "XUGU_SMALLINT",
                "XUGU_FLOAT",
                "XUGU_DOUBLE",
                "XUGU_CHAR",
                "XUGU_NCHAR",
                "XUGU_VARCHAR",
                "XUGU_VARCHAR2",
                "XUGU_CLOB",
                "XUGU_DATE",
                "XUGU_TIME",
                "XUGU_TIMESTAMP",
                "XUGU_DATETIME",
                "XUGU_TIME_WITH_TIME_ZONE",
                "XUGU_TIMESTAMP_WITH_TIME_ZONE",
                "XUGU_BINARY",
                "XUGU_BLOB",
                "XUGU_GUID",
                "XUGU_BOOLEAN"
            };

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        String jdbcUrl = String.format(XUGU_URL, XUGU_PORT, XUGU_DATABASE);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(XUGU_SCHEMA, XUGU_SOURCE, fieldNames);

        return JdbcCase.builder()
                .dockerImage(XUGU_IMAGE)
                .networkAliases(XUGU_CONTAINER_HOST)
                .containerEnv(containerEnv)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                .port(XUGU_PORT)
                .localPort(XUGU_PORT)
                .jdbcTemplate(XUGU_URL)
                .jdbcUrl(jdbcUrl)
                .userName(XUGU_USERNAME)
                .password(XUGU_PASSWORD)
                .schema(XUGU_SCHEMA)
                .database(XUGU_DATABASE)
                .sourceTable(XUGU_SOURCE)
                .sinkTable(XUGU_SINK)
                .catalogDatabase(CATALOG_DATABASE)
                .catalogSchema(XUGU_SCHEMA)
                .catalogTable(CATALOG_TABLE)
                .createSql(CREATE_SQL)
                .configFile(CONFIG_FILE)
                .insertSql(insertSql)
                .testData(testDataSet)
                .tablePathFullName(XUGU_DATABASE + "." + XUGU_SCHEMA + "." + XUGU_SOURCE)
                .build();
    }

    @Override
    void checkResult(String executeKey, TestContainer container, Container.ExecResult execResult) {
        defaultCompare(executeKey, fieldNames, "XUGU_INT");
    }

    @Override
    String driverUrl() {
        return "https://repo1.maven.org/maven2/com/xugudb/xugu-jdbc/12.2.0/xugu-jdbc-12.2.0.jar";
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                BigDecimal.valueOf(1.12),
                                BigDecimal.valueOf(i, 2),
                                i,
                                i,
                                Long.parseLong("1"),
                                i,
                                i,
                                Float.parseFloat("1.1"),
                                Double.parseDouble("1.1"),
                                String.format("f1_%s", i),
                                String.format("f1_%s", i),
                                String.format("f1_%s", i),
                                String.format("f1_%s", i),
                                String.format("f1_%s", i),
                                Date.valueOf(LocalDate.now()),
                                Time.valueOf(LocalTime.now()),
                                new Timestamp(System.currentTimeMillis()),
                                Timestamp.valueOf(LocalDateTime.now()),
                                Time.valueOf(LocalTime.now()),
                                new Timestamp(System.currentTimeMillis()),
                                null,
                                null,
                                null,
                                false
                            });
            rows.add(row);
        }

        return Pair.of(fieldNames, rows);
    }

    @Override
    protected GenericContainer<?> initContainer() {
        GenericContainer<?> container =
                new GenericContainer<>(XUGU_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(XUGU_CONTAINER_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(XUGU_IMAGE)));
        container.setPortBindings(Lists.newArrayList(String.format("%s:%s", XUGU_PORT, XUGU_PORT)));

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

    @Override
    protected void initCatalog() {
        String jdbcUrl = jdbcCase.getJdbcUrl().replace(HOST, dbServer.getHost());
        catalog =
                new XuguCatalog(
                        "xugu",
                        jdbcCase.getUserName(),
                        jdbcCase.getPassword(),
                        JdbcUrlUtil.getUrlInfo(jdbcUrl),
                        XUGU_SCHEMA);
        catalog.open();
    }
}
