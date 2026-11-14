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

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.DependencyJar;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;

public class JdbcCockroachDBIT extends TestSuiteBase implements TestResource {
    private GenericContainer<?> database;

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container ->
                    DependencyJar.ofClassName("org.postgresql.Driver")
                            .copyTo(container, "/tmp/seatunnel/plugins/Jdbc/lib");

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        database =
                new GenericContainer<>("cockroachdb/cockroach:v24.3.0")
                        .withCreateContainerCmdModifier(
                                cmd -> cmd.withEntrypoint("/cockroach/cockroach"))
                        .withCommand(
                                "start-single-node",
                                "--insecure",
                                "--store=type=mem,size=1GiB",
                                "--cache=64MiB",
                                "--max-sql-memory=128MiB")
                        .withNetwork(NETWORK)
                        .withNetworkAliases("cockroachdb")
                        .withExposedPorts(26257)
                        .waitingFor(
                                Wait.forLogMessage(".*node startup completed.*", 1)
                                        .withStartupTimeout(Duration.ofMinutes(2)));
        database.start();
        try (Connection connection = connect();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE TABLE source_rows (id INT8 PRIMARY KEY, name STRING, amount DECIMAL(12,2))");
            statement.execute(
                    "INSERT INTO source_rows VALUES (1,'alpha',12.34),(2,'你好',-9.50),(3,NULL,NULL),(4,'',0)");
            statement.execute(
                    "CREATE TABLE sink_rows (id INT8 PRIMARY KEY, name STRING, amount DECIMAL(12,2), preserved STRING DEFAULT 'default')");
        }
    }

    @BeforeEach
    void resetTarget() throws Exception {
        try (Connection connection = connect();
                Statement statement = connection.createStatement()) {
            statement.execute("DELETE FROM sink_rows");
            statement.execute("INSERT INTO sink_rows VALUES (1,'before',0,'keep')");
        }
    }

    @TestTemplate
    void readsHashPartitionsAndUpserts(TestContainer container) throws Exception {
        Container.ExecResult result = container.executeJob("/jdbc_cockroachdb_source_sink.conf");
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
        // Replaying the bounded input must update existing keys rather than insert duplicates.
        result = container.executeJob("/jdbc_cockroachdb_source_sink.conf");
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
        try (Connection connection = connect();
                Statement statement = connection.createStatement()) {
            try (ResultSet rows =
                    statement.executeQuery(
                            "SELECT count(*) FROM ((SELECT id,name,amount FROM source_rows EXCEPT SELECT id,name,amount FROM sink_rows) UNION ALL (SELECT id,name,amount FROM sink_rows EXCEPT SELECT id,name,amount FROM source_rows)) AS differences")) {
                Assertions.assertTrue(rows.next());
                Assertions.assertEquals(0, rows.getLong(1));
            }
            try (ResultSet rows =
                    statement.executeQuery("SELECT id,preserved FROM sink_rows ORDER BY id")) {
                for (int id = 1; id <= 4; id++) {
                    Assertions.assertTrue(rows.next());
                    Assertions.assertEquals(id, rows.getLong(1));
                    Assertions.assertEquals(id == 1 ? "keep" : "default", rows.getString(2));
                }
                Assertions.assertFalse(rows.next());
            }
        }
    }

    private Connection connect() throws Exception {
        return DriverManager.getConnection(
                "jdbc:postgresql://"
                        + database.getHost()
                        + ":"
                        + database.getMappedPort(26257)
                        + "/defaultdb?sslmode=disable",
                "root",
                "");
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (database != null) {
            database.stop();
        }
    }
}
