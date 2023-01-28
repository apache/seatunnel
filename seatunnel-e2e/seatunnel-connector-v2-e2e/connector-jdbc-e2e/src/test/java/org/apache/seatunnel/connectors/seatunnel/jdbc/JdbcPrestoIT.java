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

import static org.awaitility.Awaitility.given;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.TrinoContainer;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Slf4j
public class JdbcPrestoIT extends TestSuiteBase implements TestResource {

    private static final String DOCKER_IMAGE = "trinodb/trino";
    private static final String DRIVER_CLASS = "io.trino.jdbc.TrinoDriver";
    private static final String HOST = "e2e_presto";
    private static final String URL = "jdbc:trino://%s:5236";

    private Connection jdbcConnection;
    private TrinoContainer trino;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        trino = new TrinoContainer(DOCKER_IMAGE)
            .withNetworkAliases(HOST);
        trino.setPortBindings(Lists.newArrayList(
            String.format("%s:%s", 8080, 8080)));
        Startables.deepStart(Stream.of(trino)).join();
        log.info("Trino container started");
        Class.forName(DRIVER_CLASS);
        given().ignoreExceptions()
            .await()
            .atMost(180, TimeUnit.SECONDS)
            .untilAsserted(this::initializeJdbcConnection);
    }

    private void initializeJdbcConnection() throws SQLException {
        jdbcConnection = DriverManager.getConnection(String.format(URL, trino.getHost()));
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (jdbcConnection != null) {
            jdbcConnection.close();
        }
        if (trino != null) {
            trino.close();
        }
    }

    @TestTemplate
    @DisplayName("JDBC-Presto end to end test")
    public void testJdbcPresto() {
        try (Statement statement = jdbcConnection.createStatement()) {
            statement.execute("SELECT 1");
        }
        catch (Exception e) {
            log.error("Test presto server failed!");
        }
    }
}
