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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceFactory;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

/**
 * This test case is used to test that the jdbc connector returns the expected error when
 * encountering an unsupported data type. If a certain type is supported and the test case becomes
 * invalid, we need to find a replacement to allow the test case t o continue to be executed,
 * instead of deleting it.
 */
@Slf4j
public class JdbcErrorIT extends TestSuiteBase implements TestResource {
    private static final String PG_IMAGE = "postgis/postgis";
    private PostgreSQLContainer<?> POSTGRESQL_CONTAINER;
    private static final String PG_SOURCE_DDL1 =
            "CREATE TABLE IF NOT EXISTS pg_e2e_source_table1 (\n"
                    + "  gid SERIAL PRIMARY KEY,"
                    + " timearray1 timestamp[],"
                    + " timearray2 timestamp[]\n"
                    + ")";
    private static final String PG_SOURCE_DDL2 =
            "CREATE TABLE IF NOT EXISTS pg_e2e_source_table2 (\n"
                    + "  gid SERIAL PRIMARY KEY,"
                    + " str VARCHAR(255),"
                    + " timearray2 timestamp[]\n"
                    + ")";
    private static final String PG_SOURCE_DDL3 =
            "CREATE TABLE IF NOT EXISTS pg_e2e_source_table3 (\n"
                    + "  gid SERIAL PRIMARY KEY,"
                    + " str1 VARCHAR(255),"
                    + " str2 VARCHAR(255)\n"
                    + ")";

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        POSTGRESQL_CONTAINER =
                new PostgreSQLContainer<>(
                                DockerImageName.parse(PG_IMAGE)
                                        .asCompatibleSubstituteFor("postgres"))
                        .withNetwork(TestSuiteBase.NETWORK)
                        .withNetworkAliases("postgresql")
                        .withCommand("postgres -c max_prepared_transactions=100")
                        .withDatabaseName("seatunnel")
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(PG_IMAGE)));
        Startables.deepStart(Stream.of(POSTGRESQL_CONTAINER)).join();
        log.info("PostgreSQL container started");
        Class.forName(POSTGRESQL_CONTAINER.getDriverClassName());
        given().ignoreExceptions()
                .await()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(this::initializeJdbcTable);
        log.info("pg data initialization succeeded. Procedure");
    }

    @Test
    void testThrowMultiTableAndFieldsInfoWhenDataTypeUnsupported() {
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        new HashMap<String, Object>() {
                            {
                                put("url", POSTGRESQL_CONTAINER.getJdbcUrl());
                                put("driver", "org.postgresql.Driver");
                                put("user", POSTGRESQL_CONTAINER.getUsername());
                                put("password", POSTGRESQL_CONTAINER.getPassword());
                                put(
                                        "table_list",
                                        new ArrayList<Map<String, Object>>() {
                                            {
                                                add(
                                                        new HashMap<String, Object>() {
                                                            {
                                                                put(
                                                                        "table_path",
                                                                        "seatunnel.public.pg_e2e_source_table1");
                                                            }
                                                        });
                                                add(
                                                        new HashMap<String, Object>() {
                                                            {
                                                                put(
                                                                        "table_path",
                                                                        "seatunnel.public.pg_e2e_source_table2");
                                                                put(
                                                                        "query",
                                                                        "select * from seatunnel.public.pg_e2e_source_table2");
                                                            }
                                                        });
                                                add(
                                                        new HashMap<String, Object>() {
                                                            {
                                                                put(
                                                                        "table_path",
                                                                        "seatunnel.public.pg_e2e_source_table3");
                                                            }
                                                        });
                                            }
                                        });
                            }
                        });
        TableSourceFactoryContext context =
                new TableSourceFactoryContext(
                        config, Thread.currentThread().getContextClassLoader());
        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> {
                            SeaTunnelSource source =
                                    new JdbcSourceFactory().createSource(context).createSource();
                            source.getProducedCatalogTables();
                        });
        Assertions.assertEquals(
                "ErrorCode:[COMMON-21], ErrorDescription:['Postgres' tables unsupported get catalog table，"
                        + "the corresponding field types in the following tables are not supported:"
                        + " '{\"seatunnel.public.pg_e2e_source_table1\":{\"timearray1\":\"_timestamp\",\"timearray2\":\"_timestamp\"},"
                        + "\"select * from seatunnel.public.pg_e2e_source_table2\":{\"timearray2\":\"_timestamp\"}}']",
                exception.getMessage());
        Map<String, Map<String, String>> result = new LinkedHashMap<>();
        result.put(
                "seatunnel.public.pg_e2e_source_table1",
                new HashMap<String, String>() {
                    {
                        put("timearray1", "_timestamp");
                        put("timearray2", "_timestamp");
                    }
                });
        result.put(
                "select * from seatunnel.public.pg_e2e_source_table2",
                new HashMap<String, String>() {
                    {
                        put("timearray2", "_timestamp");
                    }
                });
        Assertions.assertEquals(result, exception.getParamsValueAs("tableUnsupportedTypes"));
    }

    private void initializeJdbcTable() {
        try (Connection connection = getJdbcConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(PG_SOURCE_DDL1);
            statement.execute(PG_SOURCE_DDL2);
            statement.execute(PG_SOURCE_DDL3);
        } catch (SQLException e) {
            throw new RuntimeException("Initializing PostgreSql table failed!", e);
        }
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                POSTGRESQL_CONTAINER.getJdbcUrl(),
                POSTGRESQL_CONTAINER.getUsername(),
                POSTGRESQL_CONTAINER.getPassword());
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (POSTGRESQL_CONTAINER != null) {
            POSTGRESQL_CONTAINER.stop();
        }
    }
}
