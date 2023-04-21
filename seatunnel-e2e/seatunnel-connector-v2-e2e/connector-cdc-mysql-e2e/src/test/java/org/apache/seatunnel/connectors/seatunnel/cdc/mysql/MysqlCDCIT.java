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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql;

import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.UniqueDatabase;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

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
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason = "Currently SPARK and FLINK do not support cdc")
public class MysqlCDCIT extends TestSuiteBase implements TestResource {

    // mysql
    private static final String MYSQL_HOST = "mysql_cdc_e2e";
    private static final String MYSQL_USER_NAME = "st_user";
    private static final String MYSQL_USER_PASSWORD = "seatunnel";
    private static final String MYSQL_DATABASE = "mysql_cdc";
    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V8_0);

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, MYSQL_DATABASE, "mysqluser", "mysqlpw");

    // mysql statement
    private static final String SOURCE_SQL = "select * from mysql_cdc.products_cdc";
    private static final String SINK_SQL = "select * from mysql_cdc.products_cdc_sink";

    private static MySqlContainer createMySqlContainer(MySqlVersion version) {
        MySqlContainer mySqlContainer =
                new MySqlContainer(version)
                        .withConfigurationOverride("docker/server-gtids/my.cnf")
                        .withSetupSQL("docker/setup.sql")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MYSQL_HOST)
                        .withDatabaseName(MYSQL_DATABASE)
                        .withUsername(MYSQL_USER_NAME)
                        .withPassword(MYSQL_USER_PASSWORD)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger("mysql-docker-image")));

        return mySqlContainer;
    }

    @BeforeAll
    @Override
    public void startUp() throws ClassNotFoundException, InterruptedException {
        log.info("The second stage: Starting Mysql containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        log.info("Mysql Containers are started");
        inventoryDatabase.createAndInitialize();
        log.info("Mysql ddl execution is complete");
    }

    @TestTemplate
    public void testMysqlCdcCheckDataE2e(TestContainer container)
            throws IOException, InterruptedException {

        CompletableFuture<Void> executeJobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                container.executeJob("/mysqlcdc_to_console.conf");
                            } catch (Exception e) {
                                log.error("Commit task exception :" + e.getMessage());
                                throw new RuntimeException(e);
                            }
                            return null;
                        });
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            log.info(querySql(SINK_SQL).toString());
                            Assertions.assertIterableEquals(
                                    querySql(SOURCE_SQL), querySql(SINK_SQL));
                        });

        // insert update delete
        upsertDeleteSourceTable();

        // stream stage
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    querySql(SOURCE_SQL), querySql(SINK_SQL));
                        });
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                MYSQL_CONTAINER.getJdbcUrl(),
                MYSQL_CONTAINER.getUsername(),
                MYSQL_CONTAINER.getPassword());
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
                    log.info("Print Mysql Cdc Sink data:" + objects);
                }
                if (sql.equals(SOURCE_SQL)) {
                    log.info("Print Mysql Cdc Source data:" + objects);
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
    //
    private void upsertDeleteSourceTable() {
        executeSql(
                "INSERT INTO mysql_cdc.products_cdc\n"
                        + "VALUES (111,\"zhilinli\",\"basketball\",99.14)\n");
        executeSql(
                "INSERT INTO mysql_cdc.products_cdc \n"
                        + " VALUES (112,\"jack\",\"big forward\",13.14);\n");

        executeSql("DELETE FROM mysql_cdc.products_cdc where id = 102");

        executeSql("UPDATE mysql_cdc.products_cdc SET name = 'Leblanc' where id = 101");
    }

    @Override
    @AfterAll
    public void tearDown() {
        // close Container
        MYSQL_CONTAINER.close();
    }
}
