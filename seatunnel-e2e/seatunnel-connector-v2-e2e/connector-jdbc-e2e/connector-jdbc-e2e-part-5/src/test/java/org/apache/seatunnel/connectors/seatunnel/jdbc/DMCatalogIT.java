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
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.dm.DamengCatalog;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.Driver;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class DMCatalogIT extends AbstractJdbcIT {

    private static final String DM_IMAGE = "laglangyue/dmdb8";
    private static final String DM_CONTAINER_HOST = "e2e_dmdb";

    private static final String DM_DATABASE = "TESTUSER";
    private static final String DM_SOURCE = "e2e_table_source";
    private static final String DM_SINK = "e2e_table_sink";
    private static final String CATALOG_TABLE = "e2e_table_source_catalog";
    private static final String DM_USERNAME = "TESTUSER";
    private static final String DM_PASSWORD = "testPassword";
    private static final int DM_PORT = 5236;
    private static final String DM_URL = "jdbc:dm://" + HOST + ":%s";

    private static final String DRIVER_CLASS = "dm.jdbc.driver.DmDriver";

    private static final List<String> CONFIG_FILE =
            Lists.newArrayList("/jdbc_dm_source_and_sink_lower.conf");
    private static final String CREATE_SQL =
            "create table if not exists %s"
                    + "(\n"
                    + "    \"dm_bit\"              BIT,\n"
                    + "    \"dm_int\"              INT,\n"
                    + "    \"dm_integer\"          INTEGER,\n"
                    + "    \"dm_pls_integer\"      PLS_INTEGER,\n"
                    + "    \"dm_tinyint\"          TINYINT,\n"
                    + "\n"
                    + "    \"dm_byte\"             BYTE,\n"
                    + "    \"dm_smallint\"         SMALLINT,\n"
                    + "    \"dm_bigint\"           BIGINT,\n"
                    + "\n"
                    + "    \"dm_numeric\"          NUMERIC,\n"
                    + "    \"dm_number\"           NUMBER,\n"
                    + "    \"dm_decimal\"          DECIMAL,\n"
                    + "    \"dm_dec\"              DEC,\n"
                    + "\n"
                    + "    \"dm_real\"             REAL,\n"
                    + "    \"dm_float\"            FLOAT,\n"
                    + "    \"dm_double_precision\" DOUBLE PRECISION,\n"
                    + "    \"dm_double\"           DOUBLE,\n"
                    + "\n"
                    + "    \"dm_char\"             CHAR,\n"
                    + "    \"dm_character\"        CHARACTER,\n"
                    + "    \"dm_varchar\"          VARCHAR,\n"
                    + "    \"dm_varchar2\"         VARCHAR2,\n"
                    + "    \"dm_text\"             TEXT,\n"
                    + "    \"dm_long\"             LONG,\n"
                    + "    \"dm_longvarchar\"      LONGVARCHAR,\n"
                    + "    \"dm_clob\"             CLOB,\n"
                    + "\n"
                    + "    \"dm_timestamp\"        TIMESTAMP,\n"
                    + "    \"dm_datetime\"         DATETIME,\n"
                    + "    \"dm_date\"             DATE,\n"
                    + "\n"
                    + "    \"dm_blob\"             BLOB,\n"
                    + "    \"dm_binary\"           BINARY,\n"
                    + "    \"dm_varbinary\"        VARBINARY,\n"
                    + "    \"dm_longvarbinary\"    LONGVARBINARY,\n"
                    + "    \"dm_image\"            IMAGE,\n"
                    + "    \"dm_bfile\"            BFILE,\n"
                    + "    constraint PK_T_COL primary key (\"dm_int\")"
                    + ");";

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
                .database("DAMENG")
                .schema(DM_DATABASE)
                .sourceTable(DM_SOURCE)
                .sinkTable(DM_SINK)
                .createSql(CREATE_SQL)
                .configFile(CONFIG_FILE)
                .insertSql(insertSql)
                .testData(testDataSet)
                .catalogDatabase("DAMENG")
                .useSaveModeCreateTable(true)
                .catalogSchema(DM_DATABASE)
                .catalogTable(CATALOG_TABLE)
                .build();
    }

    @Override
    String driverUrl() {
        return "https://repo1.maven.org/maven2/com/dameng/DmJdbcDriver18/8.1.1.193/DmJdbcDriver18-8.1.1.193.jar";
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        String[] fieldNames =
                new String[] {
                    "dm_bit",
                    "dm_int",
                    "dm_integer",
                    "dm_pls_integer",
                    "dm_tinyint",
                    "dm_byte",
                    "dm_smallint",
                    "dm_bigint",
                    "dm_numeric",
                    "dm_number",
                    "dm_decimal",
                    "dm_dec",
                    "dm_real",
                    "dm_float",
                    "dm_double_precision",
                    "dm_double",
                    "dm_char",
                    "dm_character",
                    "dm_varchar",
                    "dm_varchar2",
                    "dm_text",
                    "dm_long",
                    "dm_longvarchar",
                    "dm_clob",
                    "dm_timestamp",
                    "dm_datetime",
                    "dm_date",
                    "dm_blob",
                    "dm_binary",
                    "dm_varbinary",
                    "dm_longvarbinary",
                    "dm_image",
                    "dm_bfile"
                };

        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                i % 2 == 0 ? (byte) 1 : (byte) 0,
                                i,
                                i,
                                i,
                                Short.valueOf("1"),
                                Byte.valueOf("1"),
                                i,
                                Long.parseLong("1"),
                                BigDecimal.valueOf(i, 0),
                                BigDecimal.valueOf(i, 18),
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
                                String.format("f1_%s", i),
                                Timestamp.valueOf(LocalDateTime.now()),
                                new Timestamp(System.currentTimeMillis()),
                                Date.valueOf(LocalDate.now()),
                                null,
                                null,
                                null,
                                null,
                                null,
                                null
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
                        .withExposedPorts(DM_PORT)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(DM_IMAGE)));
        container.setPortBindings(Lists.newArrayList(String.format("%s:%s", 5236, 5236)));

        return container;
    }

    @Override
    public String quoteIdentifier(String field) {
        return "\"" + field + "\"";
    }

    @Override
    protected void initCatalog() {
        String jdbcUrl = jdbcCase.getJdbcUrl().replace(HOST, dbServer.getHost());
        catalog =
                new DamengCatalog(
                        "dameng",
                        jdbcCase.getUserName(),
                        jdbcCase.getPassword(),
                        JdbcUrlUtil.getUrlInfo(jdbcUrl),
                        null);
        catalog.open();
    }

    protected String buildTableInfoWithSchema(String database, String schema, String table) {
        return buildTableInfoWithSchema(schema, table);
    }

    protected void clearTable(String database, String schema, String table) {
        clearTable(schema, table);
    }

    @Override
    protected void beforeStartUP() {
        try {
            URLClassLoader urlClassLoader =
                    new URLClassLoader(
                            new URL[] {new URL(driverUrl())},
                            AbstractJdbcIT.class.getClassLoader());
            Thread.currentThread().setContextClassLoader(urlClassLoader);
            Driver driver =
                    (Driver) urlClassLoader.loadClass(jdbcCase.getDriverClass()).newInstance();
            Properties props = new Properties();

            if (StringUtils.isNotBlank(jdbcCase.getUserName())) {
                props.put("user", "SYSDBA");
            }

            if (StringUtils.isNotBlank(jdbcCase.getPassword())) {
                props.put("password", "SYSDBA");
            }

            Connection dmCon =
                    driver.connect(jdbcCase.getJdbcUrl().replace(HOST, dbServer.getHost()), props);
            dmCon.setAutoCommit(false);

            createDBAUser(dmCon);
        } catch (Exception e) {
            throw new SeaTunnelRuntimeException(JdbcITErrorCode.CREATE_TABLE_FAILED, e);
        }
    }

    protected void createDBAUser(Connection dnCon) {
        try (Statement statement = dnCon.createStatement()) {

            String createUser = "CREATE USER TESTUSER IDENTIFIED BY testPassword;";
            String updateUserDBA = "GRANT DBA TO TESTUSER;";
            statement.execute(createUser);
            statement.execute(updateUserDBA);

            dnCon.commit();
        } catch (Exception exception) {
            throw new SeaTunnelRuntimeException(JdbcITErrorCode.CREATE_TABLE_FAILED, exception);
        }
    }
}
