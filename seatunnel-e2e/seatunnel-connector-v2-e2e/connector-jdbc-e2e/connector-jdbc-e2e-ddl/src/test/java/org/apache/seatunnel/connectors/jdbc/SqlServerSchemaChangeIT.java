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

package org.apache.seatunnel.connectors.jdbc;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class SqlServerSchemaChangeIT extends AbstractSchemaChangeBaseIT {

    private static final String DATABASE_TYPE = "SqlServer";
    private static final String SQLSERVER_IMAGE = "kurthunter/sqlserver:v2";
    private static final String SQLSERVER_CONTAINER_HOST = "e2e_sqlserver";
    private static final String SQLSERVER_DATABASE = "master";
    private static final String SQLSERVER_SCHEMA = "dbo";
    private static final String SQLSERVER_USER = "sa";
    private static final String ACCEPT_EULA = "ACCEPT_EULA";
    private static final String Y = "Y";
    private static final String SA_PASSWORD = "SA_PASSWORD";
    private static final String SQLSERVER_PASSWORD = "paanssy1234$";
    private static final int SQLSERVER_PORT = 1433;
    private final String SQLSERVER_JDBC_URL =
            "jdbc:sqlserver://%s:%s;databaseName=%s;"
                    + "useBulkCopyForBatchInsert=true;delayLoadingLobs=true;useFmtOnly=false;integratedSecurity=false;xaTransactionCompatible=true";
    private static final String DRIVER_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

    private static final String SQLSERVER_DRIVER_JAR =
            "https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/9.2.1.jre8/mssql-jdbc-9.2.1.jre8.jar";

    private final String schemaEvolutionCaseConfig =
            "/mysqlcdc_to_sqlserver_with_schema_change.conf";
    private final String schemaEvolutionCaseExactlyOnceConfig =
            "/mysqlcdc_to_sqlserver_with_schema_change_exactly_once.conf";
    private final String QUERY_COLUMNS =
            "SELECT REPLACE(REPLACE(COLUMN_NAME, '[', ''), ']', '') COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' ORDER BY COLUMN_NAME";

    @Override
    protected SchemaChangeCase getSchemaChangeCase() {
        return SchemaChangeCase.builder()
                .jdbcUrl(SQLSERVER_JDBC_URL)
                .username(SQLSERVER_USER)
                .password(SQLSERVER_PASSWORD)
                .driverUrl(SQLSERVER_DRIVER_JAR)
                .port(SQLSERVER_PORT)
                .driverClassName(DRIVER_CLASS)
                .databaseName(SQLSERVER_DATABASE)
                .schemaName(SQLSERVER_SCHEMA)
                .schemaEvolutionCase(schemaEvolutionCaseConfig)
                .schemaEvolutionCaseExactlyOnce(schemaEvolutionCaseExactlyOnceConfig)
                .sinkTable1(SINK_TABLE1)
                .sinkTable2(SINK_TABLE2)
                .sinkQueryColumns(QUERY_COLUMNS)
                .openExactlyOnce(true)
                .build();
    }

    @Override
    protected GenericContainer initSinkContainer() {
        GenericContainer<?> container =
                new GenericContainer<>(SQLSERVER_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(SQLSERVER_CONTAINER_HOST)
                        .withEnv(ACCEPT_EULA, Y)
                        .withEnv(SA_PASSWORD, SQLSERVER_PASSWORD)
                        .withExposedPorts(SQLSERVER_PORT)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(SQLSERVER_IMAGE)));
        container.setPortBindings(
                Lists.newArrayList(String.format("%d:%d", SQLSERVER_PORT, SQLSERVER_PORT)));

        // Execute initialization commands for XA after the container starts
        container.start();
        try {
            container.execInContainer(
                    "/opt/mssql-tools/bin/sqlcmd",
                    "-S",
                    SQLSERVER_CONTAINER_HOST,
                    "-U",
                    SQLSERVER_USER,
                    "-P",
                    SQLSERVER_PASSWORD,
                    "-Q",
                    "IF NOT EXISTS (SELECT * FROM sys.objects WHERE name = 'xp_sqljdbc_xa_init_ex') "
                            + "EXEC sp_sqljdbc_xa_install");
        } catch (IOException | InterruptedException e) {
            log.error("Failed to install XA procedures :" + e.getMessage());
            throw new RuntimeException(e);
        }
        return container;
    }

    @Override
    protected String sinkDatabaseType() {
        return DATABASE_TYPE;
    }
}
