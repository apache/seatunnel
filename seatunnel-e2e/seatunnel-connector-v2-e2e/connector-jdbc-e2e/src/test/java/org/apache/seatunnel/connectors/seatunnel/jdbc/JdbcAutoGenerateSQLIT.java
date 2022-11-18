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
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Slf4j
public class JdbcAutoGenerateSQLIT extends TestSuiteBase implements TestResource {
    private static final String PG_IMAGE = "postgres:alpine3.16";
    private static final String PG_DRIVER_JAR = "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar";
    private PostgreSQLContainer<?> postgreSQLContainer;

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory = container -> {
        Container.ExecResult extraCommands = container.execInContainer("bash", "-c", "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && curl -O " + PG_DRIVER_JAR);
        Assertions.assertEquals(0, extraCommands.getExitCode());
    };

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        postgreSQLContainer = new PostgreSQLContainer<>(DockerImageName.parse(PG_IMAGE))
            .withNetwork(NETWORK)
            .withNetworkAliases("postgresql")
            .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(PG_IMAGE)));
        Startables.deepStart(Stream.of(postgreSQLContainer)).join();
        log.info("PostgreSQL container started");
        Class.forName(postgreSQLContainer.getDriverClassName());
        given().ignoreExceptions()
            .await()
            .atLeast(100, TimeUnit.MILLISECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .atMost(2, TimeUnit.MINUTES)
            .untilAsserted(() -> initializeJdbcTable());
    }

    @TestTemplate
    public void testAutoGenerateSQL(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/jdbc_sink_auto_generate_sql.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testAutoGenerateUpsertSQL(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/jdbc_sink_auto_generate_upsql_sql.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    private void initializeJdbcTable() {
        try (Connection connection = DriverManager.getConnection(postgreSQLContainer.getJdbcUrl(),
            postgreSQLContainer.getUsername(), postgreSQLContainer.getPassword())) {
            Statement statement = connection.createStatement();
            String sink = "create table sink(\n" +
                "user_id BIGINT NOT NULL PRIMARY KEY,\n" +
                "name varchar(255),\n" +
                "age INT\n" +
                ")";
            statement.execute(sink);
        } catch (SQLException e) {
            throw new RuntimeException("Initializing PostgreSql table failed!", e);
        }
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (postgreSQLContainer != null) {
            postgreSQLContainer.stop();
        }
    }
}
