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

package org.apache.seatunnel.e2e.flink.jdbc;

import org.apache.seatunnel.e2e.flink.FlinkContainer;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Stream;

public class FakeSourceToJdbcIT extends FlinkContainer {

    private static final Logger LOGGER = LoggerFactory.getLogger(FakeSourceToJdbcIT.class);

    private static final String DRIVER_CLASS_NAME = "com.mysql.cj.jdbc.Driver";
    private static final String URL = "jdbc:mysql://localhost:3306/seatunnel";
    private static final String USER = "root";
    private static final String PASSWORD = "123";

    private GenericContainer<?> mysqlServer;

    @After
    public void closeClickhouseContainer() {
        if (mysqlServer != null) {
            mysqlServer.stop();
        }
    }

    @Before
    @SuppressWarnings("magicnumber")
    public void startMysqlContainer() throws InterruptedException {
        mysqlServer = new GenericContainer<>("bitnami/mysql:8.0")
            .withNetwork(NETWORK)
            .withNetworkAliases("mysql")
            .withEnv("MYSQL_ROOT_USER", USER)
            .withEnv("MYSQL_ROOT_PASSWORD", PASSWORD)
            .withEnv("MYSQL_AUTHENTICATION_PLUGIN", "mysql_native_password")
            .withEnv("MYSQL_DATABASE", "seatunnel")
            .withLogConsumer(new Slf4jLogConsumer(LOGGER));
        mysqlServer.setPortBindings(Lists.newArrayList("3306:3306"));
        Startables.deepStart(Stream.of(mysqlServer)).join();
        LOGGER.info("Mysql 8.0 container started");
        Thread.sleep(5000L);
        initTable();
    }

    private void initTable() {
        try {
            Class.forName(DRIVER_CLASS_NAME);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("jdbc connection init failed.", e);
        }
        try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD)) {
            Statement statement = connection.createStatement();
            statement.execute("CREATE TABLE `fake_sink` (\n" +
                "  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
                "  `name` varchar(64),\n" +
                "  `age` bigint,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  UNIQUE KEY `uniq_name` (`name`) USING BTREE\n" +
                ") ENGINE=InnoDB;");
        } catch (SQLException e) {
            throw new RuntimeException("jdbc connection init failed.", e);
        }
    }

    @Test
    public void testFakeSourceToJdbcSink() throws IOException, InterruptedException, SQLException {
        Container.ExecResult execResult = executeSeaTunnelFlinkJob("/jdbc/fakesource_to_jdbc.conf");
        Assert.assertEquals(0, execResult.getExitCode());
        // query result
        try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD)) {
            Statement statement = connection.createStatement();
            final ResultSet resultSet = statement.executeQuery("select * from fake_sink");
            List<String> result = Lists.newArrayList();
            while (resultSet.next()) {
                result.add(resultSet.getString("name"));
            }
            Assert.assertFalse(result.isEmpty());
        } catch (SQLException e) {
            throw new RuntimeException("jdbc connection init failed.", e);
        }
    }
}
