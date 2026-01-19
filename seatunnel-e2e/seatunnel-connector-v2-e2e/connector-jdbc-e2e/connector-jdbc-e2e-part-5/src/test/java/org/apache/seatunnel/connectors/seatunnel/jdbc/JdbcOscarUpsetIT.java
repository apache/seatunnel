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
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.shade.org.apache.commons.lang3.tuple.Pair;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.Driver;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class JdbcOscarUpsetIT extends AbstractJdbcIT {

    private static final String OSCAR_IMAGE = "shentongdata/shentongdb:251217-825.2-linux64";
    private static final String OSCAR_CONTAINER_HOST = "e2e_shentongdb_upset";

    private static final String OSCAR_DATABASE = "OSRDB";
    private static final String OSCAR_SCHEMA = "SYSDBA2";
    private static final String OSCAR_SOURCE = "E2E_TABLE_SOURCE_UPSET";
    private static final String OSCAR_SINK = "E2E_TABLE_SINK_UPSET";
    private static final String OSCAR_USERNAME = "SYSDBA2";
    private static final String OSCAR_PASSWORD = "testPassword";
    private static final int DOCKET_PORT = 2003;
    private static final int JDBC_PORT = 2003;
    private static final String OSCAR_URL = "jdbc:oscar://" + HOST + ":%s";

    private static final String DRIVER_CLASS = "com.oscar.Driver";

    private static final List<String> CONFIG_FILE =
            Lists.newArrayList("/jdbc_oscar_source_and_oscar_upset_sink.conf");
    private static final String CREATE_SQL =
            "create table if not exists %s"
                    + "(\n"
                    + "    OSCAR_BIT              BIT,\n"
                    + "    OSCAR_INT              INT,\n"
                    + "    OSCAR_INTEGER          INTEGER,\n"
                    + "    OSCAR_TINYINT          TINYINT,\n"
                    + "\n"
                    + "    OSCAR_SMALLINT         SMALLINT,\n"
                    + "    OSCAR_BIGINT           BIGINT,\n"
                    + "\n"
                    + "    OSCAR_NUMBER           NUMBER,\n"
                    + "    OSCAR_DECIMAL          DECIMAL,\n"
                    + "    OSCAR_FLOAT            FLOAT,\n"
                    + "    OSCAR_DOUBLE_PRECISION DOUBLE PRECISION,\n"
                    + "    OSCAR_DOUBLE           DOUBLE,\n"
                    + "\n"
                    + "    OSCAR_CHAR             CHAR,\n"
                    + "    OSCAR_VARCHAR          VARCHAR(10),\n"
                    + "    OSCAR_VARCHAR2         VARCHAR(10),\n"
                    + "    OSCAR_TEXT             TEXT,\n"
                    + "    OSCAR_LONG             LONG,\n"
                    + "\n"
                    + "    OSCAR_TIMESTAMP        TIMESTAMP,\n"
                    + "    OSCAR_DATETIME         DATETIME,\n"
                    + "    OSCAR_DATE             DATE\n"
                    + ")";
    private static final String CREATE_SINKTABLE_SQL =
            "create table if not exists %s"
                    + "(\n"
                    + "    OSCAR_BIT              BIT,\n"
                    + "    OSCAR_INT              INT,\n"
                    + "    OSCAR_INTEGER          INTEGER,\n"
                    + "    OSCAR_TINYINT          TINYINT,\n"
                    + "\n"
                    + "    OSCAR_SMALLINT         SMALLINT,\n"
                    + "    OSCAR_BIGINT           BIGINT,\n"
                    + "\n"
                    + "    OSCAR_NUMBER           NUMBER,\n"
                    + "    OSCAR_DECIMAL          DECIMAL,\n"
                    + "    OSCAR_FLOAT            FLOAT,\n"
                    + "    OSCAR_DOUBLE_PRECISION DOUBLE PRECISION,\n"
                    + "    OSCAR_DOUBLE           DOUBLE,\n"
                    + "\n"
                    + "    OSCAR_CHAR             CHAR,\n"
                    + "    OSCAR_VARCHAR          VARCHAR,\n"
                    + "    OSCAR_VARCHAR2         VARCHAR2,\n"
                    + "    OSCAR_TEXT             TEXT,\n"
                    + "    OSCAR_LONG             LONG,\n"
                    + "\n"
                    + "    OSCAR_TIMESTAMP        TIMESTAMP,\n"
                    + "    OSCAR_DATETIME         DATETIME,\n"
                    + "    OSCAR_DATE             DATE,\n"
                    + "    CONSTRAINT OSCARPKID PRIMARY KEY (OSCAR_BIT) \n"
                    + ")";

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        String jdbcUrl = String.format(OSCAR_URL, JDBC_PORT);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(OSCAR_SCHEMA, OSCAR_SOURCE, fieldNames);

        return JdbcCase.builder()
                .dockerImage(OSCAR_IMAGE)
                .networkAliases(OSCAR_CONTAINER_HOST)
                .containerEnv(containerEnv)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                .port(DOCKET_PORT)
                .localPort(DOCKET_PORT)
                .jdbcTemplate(OSCAR_URL)
                .jdbcUrl(jdbcUrl)
                .userName(OSCAR_USERNAME)
                .password(OSCAR_PASSWORD)
                .database(OSCAR_DATABASE)
                .schema(OSCAR_SCHEMA)
                .sourceTable(OSCAR_SOURCE)
                .sinkTable(OSCAR_SINK)
                .createSql(CREATE_SQL)
                .configFile(CONFIG_FILE)
                .insertSql(insertSql)
                .testData(testDataSet)
                .build();
    }

    @Override
    protected void createNeededTables() {
        try (Statement statement = connection.createStatement()) {
            String createTemplate = jdbcCase.getCreateSql();

            String createSource =
                    String.format(
                            createTemplate,
                            buildTableInfoWithSchema(
                                    jdbcCase.getSchema(), jdbcCase.getSourceTable()));
            String createSink =
                    String.format(
                            CREATE_SINKTABLE_SQL,
                            buildTableInfoWithSchema(
                                    jdbcCase.getSchema(), jdbcCase.getSinkTable()));

            statement.execute(createSource);
            statement.execute(createSink);
            connection.commit();
        } catch (Exception exception) {
            throw new SeaTunnelRuntimeException(JdbcITErrorCode.CREATE_TABLE_FAILED, exception);
        }
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
                    "OSCAR_INT",
                    "OSCAR_INTEGER",
                    "OSCAR_TINYINT",
                    "OSCAR_SMALLINT",
                    "OSCAR_BIGINT",
                    "OSCAR_NUMBER",
                    "OSCAR_DECIMAL",
                    "OSCAR_FLOAT",
                    "OSCAR_DOUBLE_PRECISION",
                    "OSCAR_DOUBLE",
                    "OSCAR_CHAR",
                    "OSCAR_VARCHAR",
                    "OSCAR_VARCHAR2",
                    "OSCAR_TEXT",
                    "OSCAR_LONG",
                    "OSCAR_TIMESTAMP",
                    "OSCAR_DATETIME",
                    "OSCAR_DATE"
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
                new GenericContainer<>(OSCAR_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(OSCAR_CONTAINER_HOST)
                        .withExposedPorts(JDBC_PORT)
                        .withStartupTimeout(Duration.ofSeconds(3600))
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(OSCAR_IMAGE)));
        container.setPortBindings(
                Lists.newArrayList(String.format("%s:%s", JDBC_PORT, DOCKET_PORT)));
        return container;
    }

    @Override
    public String quoteIdentifier(String field) {
        return "\"" + field + "\"";
    }

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
                props.put("password", "szoscar55");
            }

            Connection oscarCon =
                    driver.connect(
                            String.format(OSCAR_URL, DOCKET_PORT).replace(HOST, dbServer.getHost()),
                            props);
            oscarCon.setAutoCommit(false);

            createDBAUser(oscarCon);
        } catch (Exception e) {
            throw new SeaTunnelRuntimeException(JdbcITErrorCode.CREATE_TABLE_FAILED, e);
        }
    }

    protected void createDBAUser(Connection dnCon) {
        try (Statement statement = dnCon.createStatement()) {

            String createUser =
                    "CREATE USER SYSDBA2 WITH  DEFAULT TABLESPACE USERS PASSWORD 'testPassword';";
            String updateUserDBA = "GRANT ROLE SYSDBA TO USER SYSDBA2;";
            statement.execute(createUser);
            statement.execute(updateUserDBA);

            dnCon.commit();
        } catch (Exception exception) {
            throw new SeaTunnelRuntimeException(JdbcITErrorCode.CREATE_TABLE_FAILED, exception);
        }
    }
}
