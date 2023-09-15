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
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sqlserver.SqlServerCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sqlserver.SqlServerURLParser;
import org.apache.seatunnel.e2e.common.TestSuiteBase;

import org.apache.commons.lang3.tuple.Pair;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcSqlServerIT extends AbstractJdbcIT {

    private static final String SQLSERVER_IMAGE = "mcr.microsoft.com/mssql/server:2022-latest";
    private static final String SQLSERVER_CONTAINER_HOST = "sqlserver";
    private static final String SQLSERVER_SOURCE = "source";
    private static final String SQLSERVER_SINK = "sink";
    private static final String SQLSERVER_DATABASE = "master";
    private static final String SQLSERVER_SCHEMA = "dbo";
    private static final String SQLSERVER_CATALOG_DATABASE = "catalog_test";

    private static final int SQLSERVER_CONTAINER_PORT = 1433;
    private static final String SQLSERVER_URL =
            "jdbc:sqlserver://"
                    + AbstractJdbcIT.HOST
                    + ":%s;encrypt=false;databaseName="
                    + SQLSERVER_DATABASE;
    private static final String DRIVER_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    private static final List<String> CONFIG_FILE =
            Lists.newArrayList("/jdbc_sqlserver_source_to_sink.conf");
    private static final String CREATE_SQL =
            "CREATE TABLE %s (\n"
                    + "  [age] bigint  NOT NULL,\n"
                    + "  [name] varchar(255) COLLATE Chinese_PRC_CI_AS  NULL\n"
                    + ")";

    private String username;

    private String password;

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        String jdbcUrl = String.format(SQLSERVER_URL, SQLSERVER_CONTAINER_PORT);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable("", SQLSERVER_SOURCE, fieldNames);

        return JdbcCase.builder()
                .dockerImage(SQLSERVER_IMAGE)
                .networkAliases(SQLSERVER_CONTAINER_HOST)
                .containerEnv(containerEnv)
                .driverClass(DRIVER_CLASS)
                .host(AbstractJdbcIT.HOST)
                .port(SQLSERVER_CONTAINER_PORT)
                .localPort(SQLSERVER_CONTAINER_PORT)
                .jdbcTemplate(SQLSERVER_URL)
                .jdbcUrl(jdbcUrl)
                .userName(username)
                .password(password)
                .database(SQLSERVER_DATABASE)
                .schema(SQLSERVER_SCHEMA)
                .sourceTable(SQLSERVER_SOURCE)
                .sinkTable(SQLSERVER_SINK)
                .catalogDatabase(SQLSERVER_CATALOG_DATABASE)
                .catalogSchema(SQLSERVER_SCHEMA)
                .catalogTable(SQLSERVER_SINK)
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
        return "https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/9.4.1.jre8/mssql-jdbc-9.4.1.jre8.jar";
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        String[] fieldNames =
                new String[] {
                    "age", "name",
                };

        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                i, "f_" + i,
                            });
            rows.add(row);
        }

        return Pair.of(fieldNames, rows);
    }

    @Override
    GenericContainer<?> initContainer() {
        DockerImageName imageName = DockerImageName.parse(SQLSERVER_IMAGE);

        MSSQLServerContainer<?> container =
                new MSSQLServerContainer<>(imageName)
                        .withNetwork(TestSuiteBase.NETWORK)
                        .withNetworkAliases(SQLSERVER_CONTAINER_HOST)
                        .acceptLicense()
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(SQLSERVER_IMAGE)));

        container.setPortBindings(
                Lists.newArrayList(
                        String.format(
                                "%s:%s", SQLSERVER_CONTAINER_PORT, SQLSERVER_CONTAINER_PORT)));

        try {
            Class.forName(container.getDriverClassName());
        } catch (ClassNotFoundException e) {
            throw new SeaTunnelRuntimeException(
                    JdbcITErrorCode.DRIVER_NOT_FOUND, "Not found suitable driver for mssql", e);
        }

        username = container.getUsername();
        password = container.getPassword();

        return container;
    }

    @Override
    public String quoteIdentifier(String field) {
        return "[" + field + "]";
    }

    @Override
    public void clearTable(String schema, String table) {
        // do nothing.
    }

    @Override
    protected String buildTableInfoWithSchema(String database, String schema, String table) {
        return buildTableInfoWithSchema(schema, table);
    }

    @Override
    protected void initCatalog() {
        catalog =
                new SqlServerCatalog(
                        "sqlserver",
                        jdbcCase.getUserName(),
                        jdbcCase.getPassword(),
                        SqlServerURLParser.parse(
                                jdbcCase.getJdbcUrl().replace(HOST, dbServer.getHost())),
                        SQLSERVER_SCHEMA);
        catalog.open();
    }
}
