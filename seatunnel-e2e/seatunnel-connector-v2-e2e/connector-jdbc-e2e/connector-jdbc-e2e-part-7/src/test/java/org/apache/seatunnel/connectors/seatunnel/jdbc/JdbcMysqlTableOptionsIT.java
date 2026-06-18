/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;
import org.apache.seatunnel.shade.org.apache.commons.lang3.tuple.Pair;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MySqlCatalog;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.Assertions;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcMysqlTableOptionsIT extends AbstractJdbcIT {

    private static final String MYSQL_IMAGE = "mysql:8.0.43";
    private static final String MYSQL_CONTAINER_HOST = "mysql-e2e-table-options";
    private static final String MYSQL_DATABASE = "seatunnel";
    private static final String MYSQL_SOURCE = "source";
    private static final String MYSQL_SINK = "sink_table_options";

    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "Abc!@#135_seatunnel";
    private static final int MYSQL_PORT = 33064;
    private static final String MYSQL_URL = "jdbc:mysql://" + HOST + ":%s/%s?useSSL=false";

    private static final String DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";

    private static final List<String> CONFIG_FILE =
            Lists.newArrayList("/jdbc_mysql_sink_with_table_options.conf");

    private static final String CREATE_SQL =
            "CREATE TABLE IF NOT EXISTS %s (\n"
                    + "    `id`   BIGINT       NOT NULL,\n"
                    + "    `name` VARCHAR(255) DEFAULT NULL,\n"
                    + "    PRIMARY KEY (`id`)\n"
                    + ");";

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        String jdbcUrl = String.format(MYSQL_URL, MYSQL_PORT, MYSQL_DATABASE);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();
        String insertSql = insertTable(MYSQL_DATABASE, MYSQL_SOURCE, fieldNames);

        return JdbcCase.builder()
                .dockerImage(MYSQL_IMAGE)
                .networkAliases(MYSQL_CONTAINER_HOST)
                .containerEnv(containerEnv)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                .port(MYSQL_PORT)
                .localPort(MYSQL_PORT)
                .jdbcTemplate(MYSQL_URL)
                .jdbcUrl(jdbcUrl)
                .userName(MYSQL_USERNAME)
                .password(MYSQL_PASSWORD)
                .database(MYSQL_DATABASE)
                .sourceTable(MYSQL_SOURCE)
                .sinkTable(MYSQL_SINK)
                .createSql(CREATE_SQL)
                .configFile(CONFIG_FILE)
                .insertSql(insertSql)
                .testData(testDataSet)
                .useSaveModeCreateTable(true)
                .build();
    }

    @Override
    void checkResult(String executeKey, TestContainer container, Container.ExecResult execResult) {
        try (Statement statement = connection.createStatement()) {
            ResultSet createTableResult =
                    statement.executeQuery(
                            String.format(
                                    "SHOW CREATE TABLE `%s`.`%s`", MYSQL_DATABASE, MYSQL_SINK));
            Assertions.assertTrue(createTableResult.next());
            String createTableSql = createTableResult.getString(2).toLowerCase();
            Assertions.assertTrue(createTableSql.contains("engine=innodb"), createTableSql);
            Assertions.assertTrue(createTableSql.contains("charset=utf8mb4"), createTableSql);
            Assertions.assertTrue(
                    createTableSql.contains("collate=utf8mb4_unicode_ci"), createTableSql);

            ResultSet countResult =
                    statement.executeQuery(
                            String.format(
                                    "SELECT COUNT(*) FROM `%s`.`%s`", MYSQL_DATABASE, MYSQL_SINK));
            Assertions.assertTrue(countResult.next());
            Assertions.assertEquals(3, countResult.getInt(1));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    String driverUrl() {
        return "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar";
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        String[] fieldNames = new String[] {"id", "name"};
        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            rows.add(new SeaTunnelRow(new Object[] {(long) i, "name_" + i}));
        }
        return Pair.of(fieldNames, rows);
    }

    @Override
    protected GenericContainer<?> initContainer() {
        DockerImageName imageName = DockerImageName.parse(MYSQL_IMAGE);

        GenericContainer<?> container =
                new MySQLContainer<>(imageName)
                        .withImagePullPolicy(PullPolicy.ageBased(Duration.ofDays(7)))
                        .withUsername(MYSQL_USERNAME)
                        .withPassword(MYSQL_PASSWORD)
                        .withDatabaseName(MYSQL_DATABASE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MYSQL_CONTAINER_HOST)
                        .withExposedPorts(MYSQL_PORT)
                        .waitingFor(Wait.forHealthcheck())
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(MYSQL_IMAGE)));

        container.setPortBindings(Lists.newArrayList(String.format("%s:%s", MYSQL_PORT, 3306)));

        return container;
    }

    @Override
    protected void initCatalog() {
        catalog =
                new MySqlCatalog(
                        "mysql",
                        jdbcCase.getUserName(),
                        jdbcCase.getPassword(),
                        JdbcUrlUtil.getUrlInfo(
                                jdbcCase.getJdbcUrl().replace(HOST, dbServer.getHost())),
                        null);
        catalog.open();
    }
}
