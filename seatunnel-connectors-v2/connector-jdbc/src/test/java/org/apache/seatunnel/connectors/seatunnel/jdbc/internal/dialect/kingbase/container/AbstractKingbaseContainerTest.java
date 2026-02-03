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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.kingbase.container;

import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.kingbase.KingbaseCatalog;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

/**
 * Base class for Kingbase Testcontainers-based unit tests. Provides shared Kingbase container setup
 * and connection management.
 *
 * <p>NOTE: The Kingbase license must be provided via the KINGBASE_LICENSE environment variable. In
 * CI, this should be configured as a GitHub Secret. The license has a validity period of 1 year.
 * Current license was generated on 2026-01-31 and will expire around 2027-01-31.
 */
@Slf4j
@DisabledOnOs(OS.WINDOWS)
public abstract class AbstractKingbaseContainerTest {

    protected static final String KINGBASE_IMAGE = "chyiyaqing/kingbase:v8r6";
    protected static final String USERNAME = "kingbase";
    protected static final String PASSWORD = "kingbase";
    protected static final String DATABASE = "test";
    protected static final String SCHEMA = "public";
    protected static final int KINGBASE_PORT = 54321;
    protected static final String KINGBASE_LICENSE_ENV = "KINGBASE_LICENSE";
    protected static final String CONTAINER_LICENSE_PATH = "/opt/kingbase/Server/bin/license.dat";

    protected static GenericContainer<?> kingbaseContainer;
    protected static Connection connection;
    protected static KingbaseCatalog catalog;

    @BeforeAll
    public static void startContainer() throws SQLException {
        String licenseContent = System.getenv(KINGBASE_LICENSE_ENV);
        log.info("KINGBASE_LICENSE content: {}", licenseContent);
        if (licenseContent == null || licenseContent.isEmpty()) {
            throw new IllegalStateException(
                    "Environment variable "
                            + KINGBASE_LICENSE_ENV
                            + " is not set. Please set it with the license file content.");
        }

        DockerImageName imageName = DockerImageName.parse(KINGBASE_IMAGE);

        kingbaseContainer =
                new GenericContainer<>(imageName)
                        .withExposedPorts(KINGBASE_PORT)
                        .withEnv("SYSTEM_USER", USERNAME)
                        .withEnv("SYSTEM_PWD", PASSWORD)
                        .withCopyToContainer(
                                Transferable.of(licenseContent), CONTAINER_LICENSE_PATH)
                        .waitingFor(Wait.forListeningPort())
                        .withStartupTimeout(Duration.ofMinutes(3));

        kingbaseContainer.start();

        String host = kingbaseContainer.getHost();
        Integer mappedPort = kingbaseContainer.getMappedPort(KINGBASE_PORT);
        String jdbcUrl = String.format("jdbc:kingbase8://%s:%d/%s", host, mappedPort, DATABASE);

        connection = DriverManager.getConnection(jdbcUrl, USERNAME, PASSWORD);

        catalog =
                new KingbaseCatalog(
                        "kingbase",
                        USERNAME,
                        PASSWORD,
                        JdbcUrlUtil.getUrlInfo(jdbcUrl),
                        SCHEMA,
                        null);
        catalog.open();
    }

    @AfterAll
    public static void stopContainer() throws SQLException {
        if (catalog != null) {
            catalog.close();
        }
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
        if (kingbaseContainer != null) {
            kingbaseContainer.stop();
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
