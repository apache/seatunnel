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

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.e2e.common.TestSuiteBase;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class JdbcWildcardSqlServerIT extends AbstractJdbcWildcardIT {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcWildcardSqlServerIT.class);
    private static final String DATABASE_TYPE = "sqlserver";
    private static final String SQLSERVER_IMAGE = "mcr.microsoft.com/mssql/server:2019-latest";
    private static final String SQLSERVER_CONTAINER_HOST = "sqlserver-e2e";
    private static final String SQLSERVER_DATABASE = "master";
    private static final String SQLSERVER_SOURCE_DATABASE = "source";
    private static final String SQLSERVER_SINK_DATABASE = "sink";
    private static final String SQLSERVER_SCHEMA = "dbo";
    private static final int SQLSERVER_CONTAINER_PORT = 1433;
    private static final String SQLSERVER_URL =
            "jdbc:sqlserver://" + HOST + ":%s;databaseName=%s;encrypt=false";
    private static final String DRIVER_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    private static final String CREATE_DATABASE_TEMPLATE = "CREATE DATABASE %s";
    private static final String CREATE_TABLE_TEMPLATE =
            "CREATE TABLE %s ([id] INT NOT NULL, [name] VARCHAR(255), [desc] VARCHAR(255), PRIMARY KEY ([id]))";

    private String username;

    private String password;

    @Override
    JdbcWildcardCase getJdbcWildcardsCase() {
        Pair<String[], List<SeaTunnelRow>> testData = initTestData();
        String columns =
                Arrays.stream(testData.getLeft())
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String placeholders =
                Arrays.stream(testData.getLeft()).map(f -> "?").collect(Collectors.joining(", "));
        String INSERT_DATA_TEMPLATE =
                "INSERT INTO %s (" + columns + ") VALUES (" + placeholders + ")";
        return JdbcWildcardCase.builder()
                .databaseType(DATABASE_TYPE)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                .userName(username)
                .password(password)
                .port(SQLSERVER_CONTAINER_PORT)
                .jdbcUrl(String.format(SQLSERVER_URL, SQLSERVER_CONTAINER_PORT, SQLSERVER_DATABASE))
                .configFile("/jdbc_wildcard_sqlserver_source_to_sink.conf")
                .sourceDatabase(SQLSERVER_SOURCE_DATABASE)
                .sinkDatabase(SQLSERVER_SINK_DATABASE)
                .createDatabaseTemplate(CREATE_DATABASE_TEMPLATE)
                .createTableTemplate(CREATE_TABLE_TEMPLATE)
                .insertDataTableTemplate(INSERT_DATA_TEMPLATE)
                .sourceTable(Lists.newArrayList("test1", "test2"))
                .sinkTable(Lists.newArrayList("sink_test1", "sink_test2"))
                .testData(testData)
                .build();
    }

    @Override
    protected void createSchemaIfNeeded() {
        // create user-defined type
        String sql = "CREATE TYPE UDTDECIMAL FROM decimal(12, 2);";
        try {
            connection.prepareStatement(sql).executeUpdate();
        } catch (Exception e) {
            throw new SeaTunnelRuntimeException(
                    JdbcITErrorCode.CREATE_TABLE_FAILED, "Fail to execute sql " + sql, e);
        }
    }

    @Override
    String driverUrl() {
        return "https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/9.4.1.jre8/mssql-jdbc-9.4.1.jre8.jar";
    }

    @Override
    GenericContainer<?> initContainer() {
        DockerImageName imageName = DockerImageName.parse(SQLSERVER_IMAGE);

        MSSQLServerContainer<?> container =
                new MSSQLServerContainer<>(imageName)
                        .withNetwork(TestSuiteBase.NETWORK)
                        .withNetworkAliases(SQLSERVER_CONTAINER_HOST)
                        .acceptLicense()
                        .withLogConsumer(new Slf4jLogConsumer(LOG));

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
    protected String buildDatabaseWithSchema(String database) {
        return quoteIdentifier(database) + "." + quoteIdentifier(SQLSERVER_SCHEMA);
    }

    @Override
    protected String buildTableInfoWithSchema(String database, String table) {
        if (StringUtils.isNotBlank(database)) {
            return quoteIdentifier(database)
                    + "."
                    + quoteIdentifier(SQLSERVER_SCHEMA)
                    + "."
                    + quoteIdentifier(table);
        } else {
            return quoteIdentifier(table);
        }
    }
}
