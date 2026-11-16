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
import org.apache.seatunnel.e2e.common.container.ContainerTcpProxy;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
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
import java.util.Objects;
import java.util.Properties;
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

    /** How long each container may take to start. */
    private static final Duration CONTAINER_STARTUP_TIMEOUT = Duration.ofSeconds(120);

    /**
     * How long to wait after the containers start until TiDB answers a query: as long as a
     * container may take to start.
     */
    private static final Duration TIDB_QUERY_WAIT = CONTAINER_STARTUP_TIMEOUT;

    /** How often to try a query while waiting for TiDB. */
    private static final Duration TIDB_QUERY_POLL_INTERVAL = Duration.ofSeconds(1);

    /**
     * Connect timeout and socket read timeout of one attempt. The read timeout also ends an attempt
     * on a connection that TiDB accepts but never greets, and one whose query gets no answer.
     */
    private static final Duration TIDB_QUERY_ATTEMPT_TIMEOUT = Duration.ofSeconds(10);

    /**
     * How long {@link #getJdbcConnection()} retries a connection that TiDB refuses or closes, as it
     * can right after it starts answering.
     */
    private static final Duration TIDB_CONNECT_WAIT = Duration.ofSeconds(30);

    /**
     * Socket read timeout of the connections of the tests. Longer than {@link
     * #TIDB_QUERY_ATTEMPT_TIMEOUT}, because their DDL can take seconds, but it still ends a
     * statement that gets no answer.
     */
    private static final Duration TIDB_STATEMENT_TIMEOUT = Duration.ofSeconds(60);

    /** How much of the TiDB container log a startup failure includes. */
    private static final int TIDB_LOG_TAIL_CHARS = 8000;

    /** The error of the last failed query of the wait, which becomes the cause of its timeout. */
    private volatile Exception lastTidbQueryFailure;

    public static final GenericContainer<?> PD =
            new GenericContainer<>("pingcap/pd:v6.1.0")
                    .withFileSystemBind("src/test/resources/config/pd.toml", "/pd.toml")
                    .withExposedPorts(PD_PORT_ORIGIN)
                    .withCommand(
                            "--name=pd0",
                            "--client-urls=http://0.0.0.0:2379",
                            "--peer-urls=http://0.0.0.0:2380",
                            "--advertise-client-urls=http://pd0:2379",
                            "--advertise-peer-urls=http://pd0:2380",
                            "--initial-cluster=pd0=http://pd0:2380",
                            "--data-dir=/data/pd0",
                            "--config=/pd.toml",
                            "--log-file=/logs/pd0.log")
                    .withNetwork(NETWORK)
                    .withNetworkAliases(PD_SERVICE_NAME)
                    .withStartupTimeout(CONTAINER_STARTUP_TIMEOUT)
                    .withLogConsumer(new Slf4jLogConsumer(log));

    public static final GenericContainer<?> TIKV =
            new GenericContainer<>("pingcap/tikv:v6.1.0")
                    .withExposedPorts(TIKV_PORT_ORIGIN)
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
                    .withStartupTimeout(CONTAINER_STARTUP_TIMEOUT)
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
                    .withStartupTimeout(CONTAINER_STARTUP_TIMEOUT)
                    .withLogConsumer(new Slf4jLogConsumer(log));

    public void startContainers() throws Exception {
        log.info("Starting containers...");
        Startables.deepStart(Stream.of(PD, TIKV, TIDB)).join();
        waitUntilTidbAnswersQueries();
        ContainerTcpProxy proxy =
                startContainerTcpProxy(
                        Arrays.asList(
                                ContainerTcpProxy.PortMapping.of(
                                        PD_PORT_ORIGIN,
                                        PD.getHost(),
                                        PD.getMappedPort(PD_PORT_ORIGIN)),
                                ContainerTcpProxy.PortMapping.of(
                                        TIKV_PORT_ORIGIN,
                                        TIKV.getHost(),
                                        TIKV.getMappedPort(TIKV_PORT_ORIGIN))));
        // PD returns its advertised PD and TiKV addresses to clients. Resolve those Docker aliases
        // to the isolated TCP proxy in this JVM while containers keep using Docker DNS.
        DnsCacheManipulator.setDnsCache(PD_SERVICE_NAME, proxy.getLoopbackAddress());
        DnsCacheManipulator.setDnsCache(TIKV_SERVICE_NAME, proxy.getLoopbackAddress());
        log.info("Containers are started.");
    }

    /**
     * With testcontainers 1.21.4, the port wait of the TiDB container could pass before TiDB served
     * connections (#13263): Docker accepts on the mapped port at once, and the wait did not fail
     * when its check inside the container failed. A connection made that early was closed before
     * TiDB sent anything, so wait until a query works.
     */
    private void waitUntilTidbAnswersQueries() {
        long start = System.currentTimeMillis();
        lastTidbQueryFailure = null;
        try {
            Awaitility.await("TiDB answers queries")
                    .atMost(TIDB_QUERY_WAIT)
                    .pollInterval(TIDB_QUERY_POLL_INTERVAL)
                    .until(this::tidbAnswersQuery);
        } catch (ConditionTimeoutException e) {
            Exception last = lastTidbQueryFailure;
            IllegalStateException failure =
                    new IllegalStateException(
                            String.format(
                                    "TiDB did not answer queries within %d s, last error: %s",
                                    TIDB_QUERY_WAIT.getSeconds(),
                                    last == null ? "none" : last.getMessage()),
                            last == null ? e : last);
            if (last != null) {
                failure.addSuppressed(e);
            }
            throw failure;
        }
        log.info("TiDB answered queries after {} ms", System.currentTimeMillis() - start);
    }

    /**
     * Tries one query. A container that has stopped fails the wait at once with the end of its log,
     * since it will not answer later.
     */
    private boolean tidbAnswersQuery() {
        if (!TIDB.isRunning()) {
            String logs = TIDB.getLogs();
            throw new IllegalStateException(
                    "The TiDB container stopped before TiDB answered queries. End of its log:\n"
                            + logs.substring(Math.max(0, logs.length() - TIDB_LOG_TAIL_CHARS)));
        }
        try (Connection connection =
                        DriverManager.getConnection(
                                getJdbcUrl(), jdbcProperties(TIDB_QUERY_ATTEMPT_TIMEOUT));
                Statement statement = connection.createStatement()) {
            return statement.execute("SELECT 1");
        } catch (SQLException | RuntimeException e) {
            // Connector/J can also fail with an unchecked CJException; both mean "not yet".
            log.info("TiDB does not answer queries yet: {}", e.getMessage());
            lastTidbQueryFailure = e;
            return false;
        }
    }

    /**
     * The properties of every test connection: the user, a connect timeout of {@link
     * #TIDB_QUERY_ATTEMPT_TIMEOUT}, and {@code socketTimeout} as the read timeout.
     */
    private static Properties jdbcProperties(Duration socketTimeout) {
        Properties properties = new Properties();
        properties.setProperty("user", TIDB_USER);
        properties.setProperty("password", TIDB_PASSWORD);
        properties.setProperty(
                "connectTimeout", String.valueOf(TIDB_QUERY_ATTEMPT_TIMEOUT.toMillis()));
        properties.setProperty("socketTimeout", String.valueOf(socketTimeout.toMillis()));
        return properties;
    }

    public static void stopContainers() {
        DnsCacheManipulator.removeDnsCache(PD_SERVICE_NAME);
        DnsCacheManipulator.removeDnsCache(TIKV_SERVICE_NAME);
        Stream.of(TIKV, PD, TIDB).forEach(GenericContainer::stop);
    }

    public String getJdbcUrl() {
        return "jdbc:mysql://" + TIDB.getContainerIpAddress() + ":" + TIDB.getMappedPort(TIDB_PORT);
    }

    /**
     * Opens a test connection, trying again for up to {@link #TIDB_CONNECT_WAIT} when TiDB refuses
     * or closes it: a successful query in {@link #waitUntilTidbAnswersQueries()} does not rule out
     * that the next connection is closed before the greeting.
     */
    protected Connection getJdbcConnection() throws SQLException {
        try {
            return Awaitility.await("TiDB accepts a connection")
                    .atMost(TIDB_CONNECT_WAIT)
                    .pollDelay(Duration.ZERO)
                    .pollInterval(TIDB_QUERY_POLL_INTERVAL)
                    .ignoreExceptionsInstanceOf(SQLException.class)
                    .until(
                            () ->
                                    DriverManager.getConnection(
                                            getJdbcUrl(), jdbcProperties(TIDB_STATEMENT_TIMEOUT)),
                            Objects::nonNull);
        } catch (ConditionTimeoutException e) {
            throw new SQLException("TiDB did not accept a connection", e);
        }
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
