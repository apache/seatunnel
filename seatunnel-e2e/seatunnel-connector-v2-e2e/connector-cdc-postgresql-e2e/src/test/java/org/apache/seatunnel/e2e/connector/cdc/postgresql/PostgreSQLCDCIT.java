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

package org.apache.seatunnel.e2e.connector.cdc.postgresql;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.MountableFile;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK},
        disabledReason = "Currently SPARK do not support cdc")
public class PostgreSQLCDCIT extends TestSuiteBase implements TestResource {

    // postgres
    private static final String POSTGRES_HOST = "postgres_cdc_e2e";
    private static final String POSTGRES_USER_NAME = "postgres";
    private static final String POSTGRES_USER_PASSWORD = "123456";
    private static final String POSTGRES_DATABASE = "cdc_e2e";
    private static final PostgreSQLContainer POSTGRES_CONTAINER = createPostgresContainer();

    // postgres source table query sql
    private static final String SOURCE_SQL =
            "select \n"
                    + "gid,\n"
                    + "text_col,\n"
                    + "varchar_col,\n"
                    + "char_col,\n"
                    + "boolean_col,\n"
                    + "smallint_col,\n"
                    + "integer_col,\n"
                    + "bigint_col,\n"
                    + "decimal_col,\n"
                    + "numeric_col,\n"
                    + "real_col,\n"
                    + "double_precision_col,\n"
                    + "smallserial_col,\n"
                    + "serial_col,\n"
                    + "bigserial_col,\n"
                    + "date_col,\n"
                    + "timestamp_col,\n"
                    + "bpchar_col,\n"
                    + "age,\n"
                    + "name\n"
                    + " from public.postgres_cdc_e2e_source_table";
    private static final String SINK_SQL =
            "select\n"
                    + "  gid,\n"
                    + "   text_col,\n"
                    + "   varchar_col,\n"
                    + "   char_col,\n"
                    + "   boolean_col,\n"
                    + "   smallint_col,\n"
                    + "   integer_col,\n"
                    + "   bigint_col,\n"
                    + "   decimal_col,\n"
                    + "   numeric_col,\n"
                    + "   real_col,\n"
                    + "   double_precision_col,\n"
                    + "   smallserial_col,\n"
                    + "   serial_col,\n"
                    + "   bigserial_col,\n"
                    + "   date_col,\n"
                    + "   timestamp_col,\n"
                    + "   bpchar_col,"
                    + "  age,\n"
                    + "  name \n"
                    + "from\n"
                    + "  public.postgres_cdc_e2e_sink_table";

    private static PostgreSQLContainer createPostgresContainer() {
        return new PostgreSQLContainer<>("postgres:11.1")
                .withNetwork(NETWORK)
                .withNetworkAliases(POSTGRES_HOST)
                .withCopyFileToContainer(
                        MountableFile.forClasspathResource("docker/postgres.cnf"),
                        "/usr/share/postgresql/postgresql.conf.sample")
                .withUsername(POSTGRES_USER_NAME)
                .withPassword(POSTGRES_USER_PASSWORD)
                .withInitScript("ddl/postgres_cdc.sql")
                .withDatabaseName(POSTGRES_DATABASE);
    }

    @BeforeAll
    @Override
    public void startUp() throws ClassNotFoundException, InterruptedException {
        log.info("The second stage: Starting postgres containers...");
        Startables.deepStart(Stream.of(POSTGRES_CONTAINER)).join();
        log.info("postgres Containers are started");
        log.info("postgres ddl execution is complete");
    }

    @TestTemplate
    public void testPostgresCDCCheckDataE2e(TestContainer container)
            throws IOException, InterruptedException {

        CompletableFuture<Void> executeJobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                container.executeJob("/postgres_cdc_to_postgres.conf");
                            } catch (Exception e) {
                                log.error("Commit task exception :" + e.getMessage());
                                throw new RuntimeException(e);
                            }
                            return null;
                        });
        await().atMost(600000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            log.info(querySql(SINK_SQL).toString());
                            Assertions.assertIterableEquals(
                                    querySql(SOURCE_SQL), querySql(SINK_SQL));
                        });

        // insert update delete
        upsertDeleteSourceTable();

        // stream stage
        await().atMost(600000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    querySql(SOURCE_SQL), querySql(SINK_SQL));
                        });
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                POSTGRES_CONTAINER.getJdbcUrl(),
                POSTGRES_CONTAINER.getUsername(),
                POSTGRES_CONTAINER.getPassword());
    }

    private List<List<Object>> querySql(String sql) {
        try (Connection connection = getJdbcConnection()) {
            ResultSet resultSet = connection.createStatement().executeQuery(sql);
            List<List<Object>> result = new ArrayList<>();
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                ArrayList<Object> objects = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {

                    objects.add(resultSet.getObject(i));
                }
                if (sql.equals(SINK_SQL)) {
                    log.info("Print postgres Cdc Sink data:" + objects);
                }
                if (sql.equals(SOURCE_SQL)) {
                    log.info("Print postgres Cdc Source data:" + objects);
                }
                result.add(objects);
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    // Execute SQL
    private void executeSql(String sql) {
        try (Connection connection = getJdbcConnection()) {
            connection.createStatement().execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void upsertDeleteSourceTable() {

        executeSql(
                "INSERT INTO public.postgres_cdc_e2e_source_table (gid,\n"
                        + "                             text_col,\n"
                        + "                             varchar_col,\n"
                        + "                             char_col,\n"
                        + "                             boolean_col,\n"
                        + "                             smallint_col,\n"
                        + "                             integer_col,\n"
                        + "                             bigint_col,\n"
                        + "                             decimal_col,\n"
                        + "                             numeric_col,\n"
                        + "                             real_col,\n"
                        + "                             double_precision_col,\n"
                        + "                             smallserial_col,\n"
                        + "                             serial_col,\n"
                        + "                             bigserial_col,\n"
                        + "                             date_col,\n"
                        + "                             timestamp_col,\n"
                        + "                             bpchar_col,\n"
                        + "                             age,\n"
                        + "                             name\n"
                        + "                           )\n"
                        + "                         VALUES\n"
                        + "                           (\n"
                        + "                             '4',\n"
                        + "                             'Hello World',\n"
                        + "                             'Test',\n"
                        + "                             'Testing',\n"
                        + "                             true,\n"
                        + "                             10,\n"
                        + "                             100,\n"
                        + "                             1000,\n"
                        + "                             10.55,\n"
                        + "                             8.8888,\n"
                        + "                             3.14,\n"
                        + "                             3.14159265,\n"
                        + "                             1,\n"
                        + "                             100,\n"
                        + "                             10000,\n"
                        + "                             '2023-05-07',\n"
                        + "                             '2023-05-07 14:30:00',\n"
                        + "                             'Testing',\n"
                        + "                             21,\n"
                        + "                             'Leblanc');\n");

        executeSql(
                "INSERT INTO public.postgres_cdc_e2e_source_table (gid,\n"
                        + "                             text_col,\n"
                        + "                             varchar_col,\n"
                        + "                             char_col,\n"
                        + "                             boolean_col,\n"
                        + "                             smallint_col,\n"
                        + "                             integer_col,\n"
                        + "                             bigint_col,\n"
                        + "                             decimal_col,\n"
                        + "                             numeric_col,\n"
                        + "                             real_col,\n"
                        + "                             double_precision_col,\n"
                        + "                             smallserial_col,\n"
                        + "                             serial_col,\n"
                        + "                             bigserial_col,\n"
                        + "                             date_col,\n"
                        + "                             timestamp_col,\n"
                        + "                             bpchar_col,\n"
                        + "                             age,\n"
                        + "                             name\n"
                        + "                           )\n"
                        + "                         VALUES\n"
                        + "                           (\n"
                        + "                             '5',\n"
                        + "                             'Hello World',\n"
                        + "                             'Test',\n"
                        + "                             'Testing',\n"
                        + "                             true,\n"
                        + "                             10,\n"
                        + "                             100,\n"
                        + "                             1000,\n"
                        + "                             10.55,\n"
                        + "                             8.8888,\n"
                        + "                             3.14,\n"
                        + "                             3.14159265,\n"
                        + "                             1,\n"
                        + "                             100,\n"
                        + "                             10000,\n"
                        + "                             '2023-05-07',\n"
                        + "                             '2023-05-07 14:30:00',\n"
                        + "                             'Testing',\n"
                        + "                             21,\n"
                        + "                             'Leblanc');\n");

        executeSql("UPDATE public.postgres_cdc_e2e_source_table SET name = 'test' where gid = 2");

        executeSql("DELETE FROM public.postgres_cdc_e2e_source_table");
    }

    @Override
    @AfterAll
    public void tearDown() {
        // close Container
        if (POSTGRES_CONTAINER != null) {
            POSTGRES_CONTAINER.close();
        }
    }
}
