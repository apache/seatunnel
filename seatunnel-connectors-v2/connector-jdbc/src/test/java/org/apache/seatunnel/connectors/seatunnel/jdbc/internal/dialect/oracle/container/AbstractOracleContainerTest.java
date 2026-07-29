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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.container;

import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleURLParser;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

/**
 * Base class for Oracle Testcontainers-based unit tests. Provides shared Oracle container setup and
 * connection management.
 */
@DisabledOnOs(OS.WINDOWS)
public abstract class AbstractOracleContainerTest {

    protected static final String ORACLE_IMAGE = "gvenzl/oracle-free:23-slim";
    protected static final String USERNAME = "TESTUSER";
    protected static final String PASSWORD = "testPassword";
    protected static final String DATABASE = "XE";
    protected static final String SCHEMA = USERNAME;

    protected static OracleContainer oracleContainer;
    protected static Connection connection;
    protected static OracleCatalog catalog;

    @BeforeAll
    public static void startContainer() throws SQLException {
        DockerImageName imageName =
                DockerImageName.parse(ORACLE_IMAGE).asCompatibleSubstituteFor("gvenzl/oracle-xe");

        oracleContainer =
                new OracleContainer(imageName)
                        .withDatabaseName(DATABASE)
                        .withUsername(USERNAME)
                        .withPassword(PASSWORD)
                        .withSharedMemorySize(1073741824L) // 1GB shared memory
                        .withStartupTimeout(Duration.ofMinutes(5)) // Increase timeout for ARM
                        .withEnv("ORACLE_CHARACTERSET", "AL32UTF8")
                        .withReuse(false);

        oracleContainer.start();

        String jdbcUrl = oracleContainer.getJdbcUrl();
        connection =
                DriverManager.getConnection(
                        jdbcUrl, oracleContainer.getUsername(), oracleContainer.getPassword());

        catalog =
                new OracleCatalog(
                        "oracle",
                        oracleContainer.getUsername(),
                        oracleContainer.getPassword(),
                        OracleURLParser.parse(jdbcUrl),
                        SCHEMA,
                        null);
        catalog.open();
    }

    @AfterAll
    public static void stopContainer() {
        try {
            if (catalog != null) {
                catalog.close();
            }
        } catch (Exception e) {
            // Ignore to ensure subsequent cleanup continues
        }

        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (Exception e) {
            // Ignore to ensure subsequent cleanup continues
        }

        try {
            if (oracleContainer != null) {
                oracleContainer.stop();
            }
        } catch (Exception e) {
            // Ignore cleanup exception
        }
    }

    protected void executeSql(String sql) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }

    protected static String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }
}
