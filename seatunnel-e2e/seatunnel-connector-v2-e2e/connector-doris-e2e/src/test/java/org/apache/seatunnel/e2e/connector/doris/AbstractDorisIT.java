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

package org.apache.seatunnel.e2e.connector.doris;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

@Slf4j
public abstract class AbstractDorisIT extends TestSuiteBase implements TestResource {

    protected GenericContainer<?> container;
    private static final String DOCKER_IMAGE = "apache/doris:doris-all-in-one-2.1.0";
    protected static final String HOST = "doris_e2e";
    protected static final int QUERY_PORT = 9030;
    protected static final int HTTP_PORT = 8030;
    protected static final int BE_HTTP_PORT = 8040;
    protected static final String URL = "jdbc:mysql://%s:" + QUERY_PORT;
    protected static final String USERNAME = "root";
    protected static final String PASSWORD = "";
    protected Connection jdbcConnection;
    private static final String SET_SQL =
            "ADMIN SET FRONTEND CONFIG (\"enable_batch_delete_by_default\" = \"true\")";
    private static final String SET_CONNECTIONS =
            "SET PROPERTY FOR 'root' 'max_user_connections' = '10000'";
    private static final String SHOW_FE = "SHOW FRONTENDS";
    private static final String SHOW_BE = "SHOW BACKENDS";
    private static final String DROP_BE = "ALTER SYSTEM DROPP BACKEND \"127.0.0.1:9050\"";
    private static final String ADD_BE = "ALTER SYSTEM ADD BACKEND \"%s:9050\"";
    protected static final String DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
    protected static final String DRIVER_JAR =
            "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar";

    @BeforeAll
    @Override
    public void startUp() throws InterruptedException {
        container =
                new GenericContainer<>(DOCKER_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST)
                        .withPrivilegedMode(true)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(DOCKER_IMAGE)));
        container.setPortBindings(
                Lists.newArrayList(
                        String.format("%s:%s", QUERY_PORT, QUERY_PORT),
                        String.format("%s:%s", HTTP_PORT, HTTP_PORT),
                        String.format("%s:%s", BE_HTTP_PORT, BE_HTTP_PORT)));
        Startables.deepStart(Stream.of(container)).join();
        log.info("doris container started");
        given().ignoreExceptions()
                .await()
                .atMost(360, TimeUnit.SECONDS)
                .untilAsserted(this::initializeJdbcConnection);
    }

    protected void initializeJdbcConnection()
            throws SQLException, ClassNotFoundException, MalformedURLException,
                    InstantiationException, IllegalAccessException {
        URLClassLoader urlClassLoader =
                new URLClassLoader(new URL[] {new URL(DRIVER_JAR)}, DorisIT.class.getClassLoader());
        Thread.currentThread().setContextClassLoader(urlClassLoader);
        Driver driver = (Driver) urlClassLoader.loadClass(DRIVER_CLASS).newInstance();
        Properties props = new Properties();
        props.put("user", USERNAME);
        props.put("password", PASSWORD);
        jdbcConnection = driver.connect(String.format(URL, container.getHost()), props);
        initializeBE();
        try (Statement statement = jdbcConnection.createStatement()) {
            statement.execute(SET_SQL);
            statement.execute(SET_CONNECTIONS);
            ResultSet resultSet = null;
            do {
                if (resultSet != null) {
                    resultSet.close();
                }
                resultSet = statement.executeQuery(SHOW_BE);
            } while (!isBeReady(resultSet, Duration.ofSeconds(1L)));
        }
    }

    // The Host of the official image [apache/doris:doris-all-in-one-2.1.0] BE is 127.0.0.1, causing
    // cross-container access failure. Delete the BE and add it again
    private void initializeBE() {
        try (Statement statement = jdbcConnection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(SHOW_FE);
            String feIp = null;
            while (resultSet.next()) {
                feIp = resultSet.getString("Host");
            }
            statement.execute(DROP_BE);
            statement.execute(String.format(ADD_BE, feIp));

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isBeReady(ResultSet rs, Duration duration) throws SQLException {
        if (rs.next()) {
            String isAlive = rs.getString("Alive").trim();
            String totalCap = rs.getString("TotalCapacity").trim();
            LockSupport.parkNanos(duration.toNanos());
            return "true".equalsIgnoreCase(isAlive) && !"0.000".equalsIgnoreCase(totalCap);
        }
        return false;
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (container != null) {
            container.close();
        }
        if (jdbcConnection != null) {
            jdbcConnection.close();
        }
    }
}
