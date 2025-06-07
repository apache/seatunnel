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
package org.apache.seatunnel.e2e.connector.graphql;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;
import static org.junit.Assert.assertNotNull;

@Slf4j
public class GraphQLIT extends TestSuiteBase implements TestResource {

    private static final String IMAGE = "hasura/graphql-engine:v2.36.10.cli-migrations-v3";
    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");
    private GenericContainer<?> genericContainer;
    private static final String PG_IMAGE = "postgres:14-alpine";
    private PostgreSQLContainer<?> postgreSQLContainer;

    private final String pgName = "postgresql";

    @BeforeAll
    @Override
    public void startUp() throws ClassNotFoundException {
        postgreSQLContainer =
                new PostgreSQLContainer<>(DockerImageName.parse(PG_IMAGE))
                        .withNetwork(TestSuiteBase.NETWORK)
                        .withNetworkAliases(pgName)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(PG_IMAGE)));
        Startables.deepStart(Stream.of(postgreSQLContainer)).join();
        log.info("PostgreSQL container started");
        Class.forName(postgreSQLContainer.getDriverClassName());
        given().ignoreExceptions()
                .await()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(2, TimeUnit.MINUTES);
        initializePostgresTable(postgreSQLContainer, "pg");

        this.genericContainer =
                new GenericContainer<>(DockerImageName.parse(IMAGE))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("graphql")
                        .withExposedPorts(8080)
                        .withEnv("HASURA_GRAPHQL_DATABASE_URL", getPgUrl())
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(IMAGE)));
        genericContainer.setPortBindings(Lists.newArrayList(String.format("%s:%s", 18080, 8080)));
        Startables.deepStart(Stream.of(genericContainer)).join();
    }

    public void checkTableData() {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT * FROM source;")) {
            boolean hasData = resultSet.next();
            log.info("Table 'source' has data: {}", hasData);
            if (hasData) {
                do {
                    int id = resultSet.getInt("id");
                    boolean valBool = resultSet.getBoolean("val_bool");
                    short valInt8 = resultSet.getShort("val_int8");
                    short valInt16 = resultSet.getShort("val_int16");
                    int valInt32 = resultSet.getInt("val_int32");
                    long valInt64 = resultSet.getLong("val_int64");
                    float valFloat = resultSet.getFloat("val_float");
                    double valDouble = resultSet.getDouble("val_double");
                    java.math.BigDecimal valDecimal = resultSet.getBigDecimal("val_decimal");
                    String valString = resultSet.getString("val_string");
                    java.sql.Timestamp valUnixtimeMicros =
                            resultSet.getTimestamp("val_unixtime_micros");

                    log.info(
                            "ID: {}, val_bool: {}, val_int8: {}, val_int16: {}, val_int32: {}, val_int64: {}, "
                                    + "val_float: {}, val_double: {}, val_decimal: {}, val_string: {}, val_unixtime_micros: {}",
                            id,
                            valBool,
                            valInt8,
                            valInt16,
                            valInt32,
                            valInt64,
                            valFloat,
                            valDouble,
                            valDecimal,
                            valString,
                            valUnixtimeMicros);
                } while (resultSet.next());
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to check table data", e);
        }
    }

    private String getPgUrl() {
        return "postgresql://"
                + postgreSQLContainer.getUsername()
                + ":"
                + postgreSQLContainer.getPassword()
                + "@"
                + pgName
                + ":"
                + 5432
                + "/"
                + postgreSQLContainer.getDatabaseName();
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (genericContainer != null) {
            genericContainer.stop();
        }
        if (postgreSQLContainer != null) {
            postgreSQLContainer.stop();
        }
    }

    @TestTemplate
    public void testGraphQLSourceAndSink(TestContainer container)
            throws IOException, InterruptedException {
        checkTableData();
        Container.ExecResult execResult1 = container.executeJob("/graphql_to_assert.conf");
        Assertions.assertEquals(0, execResult1.getExitCode());
        Container.ExecResult execResult = container.executeJob("/fake_to_graphql.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                postgreSQLContainer.getJdbcUrl(),
                postgreSQLContainer.getUsername(),
                postgreSQLContainer.getPassword());
    }

    protected void initializePostgresTable(PostgreSQLContainer container, String sqlFile) {
        final String ddlFile = String.format("ddl/%s.sql", sqlFile);
        final URL ddlTestFile = GraphQLIT.class.getClassLoader().getResource(ddlFile);
        assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
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
                                            .split(";\n"))
                            .collect(Collectors.toList());
            for (String stmt : statements) {
                statement.execute(stmt);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
