/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.e2e.connector.tidb;

import org.apache.seatunnel.e2e.common.TestSuiteBase;

import org.apache.commons.lang3.RandomUtils;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import com.alibaba.dcm.DnsCacheManipulator;
import lombok.extern.slf4j.Slf4j;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertNotNull;

/** Utility class for tidb tests. */
@Slf4j
public class TiDBTestBase extends TestSuiteBase {
    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");

    public static final String PD_SERVICE_NAME = "pd0";
    public static final String TIKV_SERVICE_NAME = "tikv0";
    public static final String TIDB_SERVICE_NAME = "tidb0";

    public static final String TIDB_USER = "root";
    public static final String TIDB_PASSWORD = "";

    public static final int TIDB_PORT = 4000;
    public static final int TIKV_PORT_ORIGIN = 20160;
    public static final int PD_PORT_ORIGIN = 2379;
    public static int pdPort = PD_PORT_ORIGIN + RandomUtils.nextInt(0, 1000);

    public static final GenericContainer<?> PD =
            new FixedHostPortGenericContainer<>("pingcap/pd:v6.1.0")
                    .withFileSystemBind("src/test/resources/config/pd.toml", "/pd.toml")
                    .withFixedExposedPort(pdPort, PD_PORT_ORIGIN)
                    .withCommand(
                            "--name=pd0",
                            "--client-urls=http://0.0.0.0:" + pdPort + ",http://0.0.0.0:2379",
                            "--peer-urls=http://0.0.0.0:2380",
                            "--advertise-client-urls=http://pd0:" + pdPort + ",http://pd0:2379",
                            "--advertise-peer-urls=http://pd0:2380",
                            "--initial-cluster=pd0=http://pd0:2380",
                            "--data-dir=/data/pd0",
                            "--config=/pd.toml",
                            "--log-file=/logs/pd0.log")
                    .withNetwork(NETWORK)
                    .withNetworkAliases(PD_SERVICE_NAME)
                    .withStartupTimeout(Duration.ofSeconds(120))
                    .withLogConsumer(new Slf4jLogConsumer(log));

    public static final GenericContainer<?> TIKV =
            new FixedHostPortGenericContainer<>("pingcap/tikv:v6.1.0")
                    .withFixedExposedPort(TIKV_PORT_ORIGIN, TIKV_PORT_ORIGIN)
                    .withFileSystemBind("src/test/resources/config/tikv.toml", "/tikv.toml")
                    .withCommand(
                            "--addr=0.0.0.0:20160",
                            "--advertise-addr=tikv0:20160",
                            "--data-dir=/data/tikv0",
                            "--pd=pd0:2379",
                            "--config=/tikv.toml",
                            "--log-file=/logs/tikv0.log")
                    .withNetwork(NETWORK)
                    .dependsOn(PD)
                    .withNetworkAliases(TIKV_SERVICE_NAME)
                    .withStartupTimeout(Duration.ofSeconds(120))
                    .withLogConsumer(new Slf4jLogConsumer(log));

    public static final GenericContainer<?> TIDB =
            new GenericContainer<>("pingcap/tidb:v6.1.0")
                    .withExposedPorts(TIDB_PORT)
                    .withFileSystemBind("src/test/resources/config/tidb.toml", "/tidb.toml")
                    .withCommand(
                            "--store=tikv",
                            "--path=pd0:2379",
                            "--config=/tidb.toml",
                            "--advertise-address=tidb0")
                    .withNetwork(NETWORK)
                    .dependsOn(TIKV)
                    .withNetworkAliases(TIDB_SERVICE_NAME)
                    .withStartupTimeout(Duration.ofSeconds(120))
                    .withLogConsumer(new Slf4jLogConsumer(log));

    public static void startContainers() throws Exception {
        // Add jvm dns cache for flink to invoke pd interface.
        DnsCacheManipulator.setDnsCache(PD_SERVICE_NAME, "127.0.0.1");
        DnsCacheManipulator.setDnsCache(TIKV_SERVICE_NAME, "127.0.0.1");
        log.info("Starting containers...");
        Startables.deepStart(Stream.of(PD, TIKV, TIDB)).join();
        log.info("Containers are started.");
    }

    public static void stopContainers() {
        DnsCacheManipulator.removeDnsCache(PD_SERVICE_NAME);
        DnsCacheManipulator.removeDnsCache(TIKV_SERVICE_NAME);
        Stream.of(TIKV, PD, TIDB).forEach(GenericContainer::stop);
    }

    public String getJdbcUrl() {
        return "jdbc:mysql://" + TIDB.getContainerIpAddress() + ":" + TIDB.getMappedPort(TIDB_PORT);
    }

    protected Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(getJdbcUrl(), TIDB_USER, TIDB_PASSWORD);
    }

    private static void dropTestDatabase(Connection connection, String databaseName)
            throws SQLException {
        try {
            Awaitility.await(String.format("Dropping database %s", databaseName))
                    .atMost(120, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                try {
                                    String sql =
                                            String.format(
                                                    "DROP DATABASE IF EXISTS %s", databaseName);
                                    connection.createStatement().execute(sql);
                                    return true;
                                } catch (SQLException e) {
                                    log.warn(
                                            String.format(
                                                    "DROP DATABASE %s failed: {}", databaseName),
                                            e.getMessage());
                                    return false;
                                }
                            });
        } catch (ConditionTimeoutException e) {
            throw new IllegalStateException("Failed to drop test database", e);
        }
    }

    protected void initializeTidbTable(String... sqlFiles) {
        for (String sqlFile : sqlFiles) {
            initializeTidbTable(sqlFile);
        }
    }

    /**
     * Executes a JDBC statement using the default jdbc config without autocommitting the
     * connection.
     */
    protected void initializeTidbTable(String sqlFile) {
        final String ddlFile = String.format("ddl/%s.sql", sqlFile);
        final URL ddlTestFile = TiDBTestBase.class.getClassLoader().getResource(ddlFile);
        assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            dropTestDatabase(connection, sqlFile);
            final List<String> statements =
                    Arrays.stream(
                                    Files.readAllLines(Paths.get(ddlTestFile.toURI())).stream()
                                            .map(String::trim)
                                            .filter(x -> !x.startsWith("--") && !x.isEmpty())
                                            .map(
                                                    x -> {
                                                        final Matcher m =
                                                                COMMENT_PATTERN.matcher(x);
                                                        return m.matches() ? m.group(1) : x;
                                                    })
                                            .collect(Collectors.joining("\n"))
                                            .split(";"))
                            .collect(Collectors.toList());
            for (String stmt : statements) {
                statement.execute(stmt);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
