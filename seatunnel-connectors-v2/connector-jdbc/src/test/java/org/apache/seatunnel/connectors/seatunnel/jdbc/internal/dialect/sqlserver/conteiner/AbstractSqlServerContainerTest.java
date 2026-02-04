/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlserver.conteiner;

import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sqlserver.SqlServerCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Base class for SQL Server tests using Testcontainers. Provides a shared SQL Server container for
 * all tests in the same test suite.
 */

@DisabledOnOs(OS.WINDOWS)
@Testcontainers
public abstract class AbstractSqlServerContainerTest {

    private static final String MSSQL_IMAGE = "mcr.microsoft.com/mssql/server:2022-latest";
    protected static MSSQLServerContainer<?> MSSQL_CONTAINER;

    @BeforeAll
    static void startContainer(){
        MSSQL_CONTAINER = new MSSQLServerContainer<>(DockerImageName.parse(MSSQL_IMAGE))
                .acceptLicense();

        MSSQL_CONTAINER.start();
    }

    @AfterAll
    static void stopContainer(){
        if(MSSQL_CONTAINER != null){
            MSSQL_CONTAINER.stop();
        }
    }

    // ==================== CORE CONNECTION METHODS ====================

    protected String getJdbcUrl(){
        return MSSQL_CONTAINER.getJdbcUrl();
    }

    protected String getUsername(){
        return MSSQL_CONTAINER.getUsername();
    }

    protected String getPassword(){
        return MSSQL_CONTAINER.getPassword();
    }

    protected Connection getConnection() throws SQLException{
        return DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
    }

    // ==================== HELPER METHODS FOR CATALOG ====================

    protected JdbcUrlUtil.UrlInfo getJdbcUrlInfo() {
        return JdbcUrlUtil.getUrlInfo(getJdbcUrl());
    }
    protected SqlServerCatalog createSqlServerCatalog() {
        return new SqlServerCatalog(
                DatabaseIdentifier.SQLSERVER,
                getUsername(),
                getPassword(),
                getJdbcUrlInfo(),
                "dbo",
                null // default properties
        );
    }
    // ==================== DATABASE SETUP/TEARDOWN ====================

    protected void executeSql(String sql) throws SQLException{
        try(Connection conn = getConnection();
            Statement stmt = conn.createStatement()){
            stmt.execute(sql);
        }
    }

    protected void createTestTable(String tableName) throws SQLException {

        String sql =
                String.format(
                        "CREATE TABLE %s ("
                                + "id INT IDENTITY(1,1) PRIMARY KEY, "
                                + "name VARCHAR(100), "
                                + "age INT, "
                                + "created_at DATETIME DEFAULT CURRENT_TIMESTAMP"
                                + ")",
                        tableName);
        executeSql(sql);
    }

    protected void dropTableIfExists(String tableName) throws SQLException {
        executeSql(String.format("DROP TABLE IF EXISTS %s", tableName));
    }

    protected Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                MSSQL_CONTAINER.getJdbcUrl(),
                MSSQL_CONTAINER.getUsername(),
                MSSQL_CONTAINER.getPassword());
    }
}
