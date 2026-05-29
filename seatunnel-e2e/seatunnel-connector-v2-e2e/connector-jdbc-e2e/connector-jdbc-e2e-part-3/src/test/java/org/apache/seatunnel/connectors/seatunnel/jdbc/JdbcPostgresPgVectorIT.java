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
import org.apache.seatunnel.e2e.common.util.JdbcUtil;

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

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

@Slf4j
public class JdbcPostgresPgVectorIT  extends TestSuiteBase implements TestResource {

    private static final String PGVECTOR_IMAGE = "pgvector/pgvector:pg16";
    private static final String PG_DRIVER_JAR =
            "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar";
    private static final String PGVECTOR_SOURCE_DDL =
            "CREATE EXTENSION IF NOT EXISTS vector;\n"
                    + "CREATE TABLE IF NOT EXISTS pgvector_e2e_source_table (\n"
                    + "  id SERIAL PRIMARY KEY,\n"
                    + "  name VARCHAR(255),\n"
                    + "  embedding vector(3)\n"
                    + ")";
    private static final String PGVECTOR_SINK_DDL =
            "CREATE EXTENSION IF NOT EXISTS vector;\n"
                    + "CREATE TABLE IF NOT EXISTS pgvector_e2e_sink_table (\n"
                    + "  id SERIAL PRIMARY KEY,\n"
                    + "  name VARCHAR(255),\n"
                    + "  embedding vector(3)\n"
                    + ")";

    private PostgreSQLContainer<?> PGVECTOR_CONTAINER;

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && curl -O "
                                        + PG_DRIVER_JAR);
                Assertions.assertEquals(0, extraCommands.getExitCode());
            };

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        PGVECTOR_CONTAINER =
                new PostgreSQLContainer<>(
                                DockerImageName.parse(PGVECTOR_IMAGE)
                                        .asCompatibleSubstituteFor("postgres"))
                        .withNetwork(TestSuiteBase.NETWORK)
                        .withNetworkAliases("pgvector")
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(PGVECTOR_IMAGE)));
        Startables.deepStart(Stream.of(PGVECTOR_CONTAINER)).join();
        log.info("PgVector container started");
        Class.forName(PGVECTOR_CONTAINER.getDriverClassName());
        given().ignoreExceptions()
                .await()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(this::initializeJdbcTable);
        log.info("pgvector data initialization succeeded");
    }

    @TestTemplate
    public void testPgVectorSourceAndSink(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/jdbc_postgres_pgvector_source_and_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), "pgvector job run failed");

        List<List<Object>> src =
                querySql("select id, name, embedding::text from pgvector_e2e_source_table order by id");
        List<List<Object>> dst =
                querySql("select id, name, embedding::text from pgvector_e2e_sink_table order by id");
        Assertions.assertFalse(src.isEmpty());
        Assertions.assertEquals(src.size(), dst.size());
        for (int i = 0; i < src.size(); i++) {
            Assertions.assertEquals(src.get(i).get(0), dst.get(i).get(0));
            Assertions.assertEquals(src.get(i).get(1), dst.get(i).get(1));
            assertVectorEquals(
                    src.get(i).get(2).toString(), dst.get(i).get(2).toString());
        }
        log.info("pgvector e2e test completed");
    }

    private void assertVectorEquals(String expected, String actual) {
        double[] expectedArr = parseVector(expected);
        double[] actualArr = parseVector(actual);
        Assertions.assertEquals(expectedArr.length, actualArr.length);
        for (int i = 0; i < expectedArr.length; i++) {
            Assertions.assertEquals(
                    expectedArr[i], actualArr[i], 1e-6, "Vector mismatch at index " + i);
        }
    }

    private double[] parseVector(String vectorStr) {
        String content = vectorStr.replace("[", "").replace("]", "").trim();
        String[] parts = content.split(",");
        double[] result = new double[parts.length];
        for (int i = 0; i < parts.length; i++) {
            result[i] = Double.parseDouble(parts[i].trim());
        }
        return result;
    }

    private void initializeJdbcTable() {
        try (Connection connection = getJdbcConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(PGVECTOR_SOURCE_DDL);
            statement.execute(PGVECTOR_SINK_DDL);
            statement.execute(
                    "INSERT INTO pgvector_e2e_source_table (name, embedding) VALUES "
                            + "('item1', '[0.1,0.2,0.3]'), "
                            + "('item2', '[0.4,0.5,0.6]'), "
                            + "('item3', '[0.7,0.8,0.9]')");
        } catch (SQLException e) {
            throw new RuntimeException("Initializing pgvector table failed!", e);
        }
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                PGVECTOR_CONTAINER.getJdbcUrl(),
                PGVECTOR_CONTAINER.getUsername(),
                PGVECTOR_CONTAINER.getPassword());
    }

    private List<List<Object>> querySql(String sql) {
        return JdbcUtil.querySql(
                sql,
                () -> {
                    try {
                        return this.getJdbcConnection();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (PGVECTOR_CONTAINER != null) {
            PGVECTOR_CONTAINER.stop();
        }
    }
}
