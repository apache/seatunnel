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
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.DependencyJar;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
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
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

/**
 * Smoke test of MySQL-CDC on the MySQL 5.x versions the connector documents.
 *
 * <p>The binlog client was replaced by Debezium's fork for MySQL 9.0 support, and that client is
 * used for every server version. The shared MySQL CDC suite only runs on 8.0, 8.4 and 9.0, so each
 * subclass proves the snapshot, the binlog handshake, GTID parsing where the server has GTIDs, and
 * insert, update and delete row events on one 5.x version. It runs one job on the Zeta engine only,
 * because the binlog client is engine independent and the MySQL CDC IT lane is close to its time
 * limit.
 */
@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason =
                "The binlog client is engine independent; one engine keeps the lane within budget")
public abstract class AbstractMysqlCDCSmokeITBase extends TestSuiteBase implements TestResource {

    private static final String MYSQL_DATABASE = "mysql_cdc";
    private static final String SOURCE_TABLE = MYSQL_DATABASE + ".mysql_smoke_source";
    private static final String SINK_TABLE = MYSQL_DATABASE + ".mysql_smoke_sink";

    private final MySqlContainer mysqlContainer;
    private final String jobConfigFile;

    /**
     * @param version server image version
     * @param myCnf configuration override; it must enable row-based binary logging
     * @param networkAlias host name used by the job config to reach this server
     * @param jobConfigFile job config reading {@code mysql_smoke_source} on that host
     */
    protected AbstractMysqlCDCSmokeITBase(
            MySqlVersion version, String myCnf, String networkAlias, String jobConfigFile) {
        this.mysqlContainer =
                new MySqlContainer(version)
                        .withConfigurationOverride(myCnf)
                        .withSetupSQL("docker/setup.sql")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(networkAlias)
                        .withDatabaseName(MYSQL_DATABASE)
                        .withUsername("mysqluser")
                        .withPassword("mysqlpw")
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(
                                                "mysql-" + version.getVersion())));
        this.jobConfigFile = jobConfigFile;
    }

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container ->
                    DependencyJar.ofClassName("com.mysql.cj.jdbc.Driver")
                            .copyTo(container, "/tmp/seatunnel/plugins/MySQL-CDC/lib");

    @BeforeAll
    @Override
    public void startUp() {
        Startables.deepStart(Stream.of(mysqlContainer)).join();
    }

    @AfterAll
    @Override
    public void tearDown() {
        mysqlContainer.close();
    }

    /**
     * Two snapshot rows, then an insert, an update and a delete from the binlog. The final rows are
     * asserted literally, so a client that drops, misreads or misorders row events fails.
     */
    @TestTemplate
    public void testMysqlCdcSmoke(TestContainer container) throws Exception {
        for (String table : Arrays.asList(SOURCE_TABLE, SINK_TABLE)) {
            executeSql(
                    "CREATE TABLE IF NOT EXISTS "
                            + table
                            + " (id INT PRIMARY KEY, name VARCHAR(32), amount DECIMAL(10, 2))");
            executeSql("DELETE FROM " + table);
        }
        executeSql("INSERT INTO " + SOURCE_TABLE + " VALUES (1, 'a', 1.50), (2, 'b', 2.50)");

        CompletableFuture<Void> job =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                container.executeJob(jobConfigFile);
                            } catch (Exception e) {
                                log.error("Commit task exception :" + e.getMessage());
                                throw new RuntimeException(e);
                            }
                        });
        awaitSinkRows(
                job,
                Arrays.asList(Arrays.asList("1", "a", "1.50"), Arrays.asList("2", "b", "2.50")));

        executeSql("INSERT INTO " + SOURCE_TABLE + " VALUES (3, 'c', 3.50)");
        executeSql("UPDATE " + SOURCE_TABLE + " SET name = 'a2', amount = 9.99 WHERE id = 1");
        executeSql("DELETE FROM " + SOURCE_TABLE + " WHERE id = 2");
        awaitSinkRows(
                job,
                Arrays.asList(Arrays.asList("1", "a2", "9.99"), Arrays.asList("3", "c", "3.50")));
    }

    /**
     * A streaming job never finishes on its own, so a done future means it failed or stopped early.
     * Throwing a non-assertion exception ends the bounded wait at once with the job's cause instead
     * of letting it poll the sink until the timeout.
     */
    private void awaitSinkRows(CompletableFuture<Void> job, List<List<String>> expected) {
        await().atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            if (job.isDone()) {
                                throw new IllegalStateException(
                                        "MySQL CDC smoke job ended before the sink caught up",
                                        job.handle((ignored, error) -> error).join());
                            }
                            Assertions.assertEquals(expected, querySinkRows());
                        });
    }

    private List<List<String>> querySinkRows() throws SQLException {
        List<List<String>> rows = new ArrayList<>();
        try (Connection connection = openConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery(
                                "SELECT id, name, amount FROM " + SINK_TABLE + " ORDER BY id")) {
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

    private void executeSql(String sql) throws SQLException {
        try (Connection connection = openConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private Connection openConnection() throws SQLException {
        return DriverManager.getConnection(
                mysqlContainer.getJdbcUrl(),
                mysqlContainer.getUsername(),
                mysqlContainer.getPassword());
    }
}
