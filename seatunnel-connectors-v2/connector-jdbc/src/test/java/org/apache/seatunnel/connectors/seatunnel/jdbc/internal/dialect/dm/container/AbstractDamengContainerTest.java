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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dm.container;

import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.dm.DamengCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

/**
 * Base class for Dameng (DM8) integration tests using Testcontainers.
 *
 * <p>Uses the {@code onlyoffice/damengdb:8.1.2} Docker image since official Dameng images require
 * registration and are not publicly available for automated CI/CD. Tests are disabled on Windows.
 *
 * @see <a href="https://hub.docker.com/r/onlyoffice/damengdb">onlyoffice/damengdb on Docker Hub</a>
 */
@Slf4j
@DisabledOnOs(OS.WINDOWS)
@Testcontainers
public abstract class AbstractDamengContainerTest {

    private static final String DM_IMAGE = "onlyoffice/damengdb:8.1.2";
    protected static final int DM_PORT = 5236;
    protected static final String DM_USERNAME = "SYSDBA";
    protected static final String DM_PASSWORD = "SYSDBA001";
    protected static final String DM_SCHEMA = "SYSDBA";
    protected static final String DM_DATABASE = "DAMENG";
    protected static final String DM_DRIVER_CLASS = "dm.jdbc.driver.DmDriver";

    protected static GenericContainer<?> DM_CONTAINER;

    @BeforeAll
    static void startContainer() {
        try {
            DM_CONTAINER =
                    new GenericContainer<>(DockerImageName.parse(DM_IMAGE))
                            .withExposedPorts(DM_PORT)
                            .withEnv("PAGE_SIZE", "16")
                            .withEnv("LD_LIBRARY_PATH", "/opt/dmdbms/bin")
                            .withEnv("EXTENT_SIZE", "32")
                            .withEnv("BLANK_PAD_MODE", "1")
                            .withEnv("LOG_SIZE", "1024")
                            .withEnv("UNICODE_FLAG", "1")
                            .withEnv("INSTANCE_NAME", "dm8_test")
                            .waitingFor(Wait.forLogMessage(".*SYSTEM IS READY.*\\n", 1))
                            .withStartupTimeout(Duration.ofMinutes(5));
            DM_CONTAINER.start();
            log.info(
                    "Dameng container started at: {}:{}",
                    DM_CONTAINER.getHost(),
                    DM_CONTAINER.getMappedPort(DM_PORT));
        } catch (Exception e) {
            log.error("Failed to start Dameng container", e);
            Assumptions.assumeTrue(
                    false, "Dameng container not available, skipping integration tests: " + e);
        }
    }

    @AfterAll
    static void stopContainer() {
        if (DM_CONTAINER != null) {
            try {
                DM_CONTAINER.stop();
                log.info("Dameng container stopped");
            } catch (Exception e) {
                log.warn("Failed to stop Dameng container", e);
            }
        }
    }

    // ==================== CORE CONNECTION METHODS ====================

    protected String getJdbcUrl() {
        return String.format(
                "jdbc:dm://%s:%d", DM_CONTAINER.getHost(), DM_CONTAINER.getMappedPort(DM_PORT));
    }

    protected String getUsername() {
        return DM_USERNAME;
    }

    protected String getPassword() {
        return DM_PASSWORD;
    }

    protected Connection getConnection() throws SQLException {
        return DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
    }

    // ==================== HELPER METHODS FOR CATALOG ====================

    protected JdbcUrlUtil.UrlInfo getJdbcUrlInfo() {
        return JdbcUrlUtil.getUrlInfo(getJdbcUrl());
    }

    protected DamengCatalog createDamengCatalog() {
        return new DamengCatalog(
                DatabaseIdentifier.DAMENG,
                getUsername(),
                getPassword(),
                getJdbcUrlInfo(),
                DM_SCHEMA,
                DM_DRIVER_CLASS);
    }

    // ==================== DATABASE SETUP/TEARDOWN ====================

    protected void executeSql(String sql) throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    protected void createTestTable(String schemaName, String tableName) throws SQLException {
        String sql =
                String.format(
                        "CREATE TABLE \"%s\".\"%s\" ("
                                + "\"ID\" INT PRIMARY KEY, "
                                + "\"NAME\" VARCHAR(100), "
                                + "\"AGE\" INT, "
                                + "\"CREATED_AT\" TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
                                + ")",
                        schemaName, tableName);
        executeSql(sql);
    }

    protected void dropTableIfExists(String schemaName, String tableName) throws SQLException {
        String checkSql =
                String.format(
                        "SELECT COUNT(*) FROM ALL_TABLES WHERE OWNER = '%s' AND TABLE_NAME = '%s'",
                        schemaName, tableName);
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            java.sql.ResultSet rs = stmt.executeQuery(checkSql);
            if (rs.next() && rs.getInt(1) > 0) {
                stmt.execute(String.format("DROP TABLE \"%s\".\"%s\"", schemaName, tableName));
            }
        }
    }
}
