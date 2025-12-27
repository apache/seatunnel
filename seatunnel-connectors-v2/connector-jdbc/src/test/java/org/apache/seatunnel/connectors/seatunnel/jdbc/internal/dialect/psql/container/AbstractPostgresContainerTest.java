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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.container;

import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Base class for PostgreSQL tests using Testcontainers. Provides a shared PostgreSQL container for
 * all tests in the same test suite.
 */
@Testcontainers
public abstract class AbstractPostgresContainerTest {

    // Use a lightweight PostgreSQL image without PostGIS extensions
    private static final String POSTGRES_IMAGE = "postgres:15-alpine";

    protected static PostgreSQLContainer<?> POSTGRES_CONTAINER;

    @BeforeAll
    static void startContainer() {
        POSTGRES_CONTAINER =
                new PostgreSQLContainer<>(DockerImageName.parse(POSTGRES_IMAGE))
                        .withDatabaseName("testdb")
                        .withUsername("test")
                        .withPassword("test")
                        .withCommand("postgres -c max_prepared_transactions=100");
        POSTGRES_CONTAINER.start();
    }

    @AfterAll
    static void stopContainer() {
        if (POSTGRES_CONTAINER != null) {
            POSTGRES_CONTAINER.stop();
        }
    }

    // ==================== CORE CONNECTION METHODS ====================

    protected String getJdbcUrl() {
        return POSTGRES_CONTAINER.getJdbcUrl();
    }

    protected String getUsername() {
        return POSTGRES_CONTAINER.getUsername();
    }

    protected String getPassword() {
        return POSTGRES_CONTAINER.getPassword();
    }

    protected Connection getConnection() throws SQLException {
        return DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
    }

    // ==================== HELPER METHODS FOR CATALOG ====================

    /**
     * Creates JdbcUrlUtil.UrlInfo needed for PostgresCatalog constructor. This mimics what
     * JdbcPostgresIT does.
     */
    protected JdbcUrlUtil.UrlInfo getJdbcUrlInfo() {
        return JdbcUrlUtil.getUrlInfo(getJdbcUrl());
    }

    /**
     * Helper to create a PostgresCatalog instance with current container settings. You'll need to
     * import PostgresCatalog class in your test.
     */
    protected org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql.PostgresCatalog
            createPostgresCatalog() {
        return new org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql.PostgresCatalog(
                DatabaseIdentifier.POSTGRESQL,
                getUsername(),
                getPassword(),
                getJdbcUrlInfo(),
                "public", // default schema
                null // default properties
                );
    }

    // ==================== DATABASE SETUP/TEARDOWN ====================

    /** Executes a SQL statement (useful for test setup/cleanup). */
    protected void executeSql(String sql) throws SQLException {
        try (Connection conn = getConnection();
                java.sql.Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    /** Creates a simple test table for dialect/catalog tests. */
    protected void createTestTable(String tableName) throws SQLException {
        String sql =
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s ("
                                + "id SERIAL PRIMARY KEY, "
                                + "name VARCHAR(100), "
                                + "age INT, "
                                + "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
                                + ")",
                        tableName);
        executeSql(sql);
    }

    /** Drops a table if it exists. */
    protected void dropTableIfExists(String tableName) throws SQLException {
        executeSql(String.format("DROP TABLE IF EXISTS %s", tableName));
    }

    /** Gets the jdbc connection fot the dialectConteinerTests */
    protected Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                POSTGRES_CONTAINER.getJdbcUrl(),
                POSTGRES_CONTAINER.getUsername(),
                POSTGRES_CONTAINER.getPassword());
    }
}
