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
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK},
        disabledReason = "Currently SPARK do not support cdc")
public class Mysql9_0CDCIT extends AbstractMysqlCDCITBase {

    private static final String VECTOR_SOURCE_TABLE = MYSQL_DATABASE + ".mysql_cdc_vector_source";
    private static final String VECTOR_SINK_TABLE = MYSQL_DATABASE + ".mysql_cdc_vector_sink";

    public Mysql9_0CDCIT() {
        // Initialize the container
        this.MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V9_0);
        this.inventoryDatabase =
                new UniqueDatabase(
                        MYSQL_CONTAINER, MYSQL_DATABASE, "mysqluser", "mysqlpw", MYSQL_DATABASE);
    }

    @Override
    protected MySqlContainer createMySqlContainer(MySqlVersion version) {
        return new MySqlContainer(version)
                .withConfigurationOverride("docker/server-gtids/my9-0.cnf")
                .withSetupSQL("docker/setup.sql")
                .withNetwork(NETWORK)
                .withNetworkAliases(MYSQL_HOST)
                .withDatabaseName(MYSQL_DATABASE)
                .withUsername(MYSQL_USER_NAME)
                .withPassword(MYSQL_USER_PASSWORD)
                .withLogConsumer(
                        new Slf4jLogConsumer(DockerLoggerFactory.getLogger("mysql-docker-image")));
    }

    /**
     * VECTOR must reach the sink as its stored little-endian float bytes from both the snapshot
     * (JDBC) and the binlog (row event) paths. The literal hex values fail if either path decodes,
     * truncates or re-encodes the value; [1.5,2.5] is 0x3FC00000, 0x40200000 in little-endian.
     */
    @TestTemplate
    public void testMysqlCdcVectorColumn(TestContainer container) throws Exception {
        executeSql(
                "CREATE TABLE IF NOT EXISTS "
                        + VECTOR_SOURCE_TABLE
                        + " (id INT PRIMARY KEY, f_vector VECTOR(2), f_vector_null VECTOR(2))");
        executeSql(
                "CREATE TABLE IF NOT EXISTS "
                        + VECTOR_SINK_TABLE
                        + " (id INT PRIMARY KEY, f_vector VARBINARY(8), f_vector_null VARBINARY(8))");
        executeSql("DELETE FROM " + VECTOR_SOURCE_TABLE);
        executeSql("DELETE FROM " + VECTOR_SINK_TABLE);
        executeSql(
                "INSERT INTO "
                        + VECTOR_SOURCE_TABLE
                        + " VALUES (1, STRING_TO_VECTOR('[1.5,2.5]'), NULL)");

        CompletableFuture.runAsync(
                () -> {
                    try {
                        container.executeJob("/mysqlcdc_to_mysql_with_vector.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });
        await().atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        Arrays.asList(Arrays.asList("1", "0000C03F00002040", null)),
                                        queryVectorHex(VECTOR_SINK_TABLE)));

        executeSql(
                "INSERT INTO "
                        + VECTOR_SOURCE_TABLE
                        + " VALUES (2, STRING_TO_VECTOR('[2.5,1.5]'), STRING_TO_VECTOR('[1.5,2.5]'))");
        executeSql(
                "UPDATE "
                        + VECTOR_SOURCE_TABLE
                        + " SET f_vector = STRING_TO_VECTOR('[2.5,2.5]') WHERE id = 1");
        await().atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        Arrays.asList(
                                                Arrays.asList("1", "0000204000002040", null),
                                                Arrays.asList(
                                                        "2",
                                                        "000020400000C03F",
                                                        "0000C03F00002040")),
                                        queryVectorHex(VECTOR_SINK_TABLE)));
    }

    private void executeSql(String sql) throws SQLException {
        try (Connection connection = openConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private List<List<String>> queryVectorHex(String table) throws SQLException {
        List<List<String>> rows = new ArrayList<>();
        try (Connection connection = openConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery(
                                "SELECT id, HEX(f_vector), HEX(f_vector_null) FROM "
                                        + table
                                        + " ORDER BY id")) {
            while (resultSet.next()) {
                rows.add(
                        Arrays.asList(
                                resultSet.getString(1),
                                resultSet.getString(2),
                                resultSet.getString(3)));
            }
        }
        return rows;
    }

    private Connection openConnection() throws SQLException {
        return DriverManager.getConnection(
                MYSQL_CONTAINER.getJdbcUrl(),
                MYSQL_CONTAINER.getUsername(),
                MYSQL_CONTAINER.getPassword());
    }
}
