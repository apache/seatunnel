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

package org.apache.seatunnel.e2e.flink.v2.doris;

import org.apache.seatunnel.e2e.flink.FlinkContainer;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.lifecycle.Startables;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class FakeSourceToDorisIT extends FlinkContainer {
    private static final Logger LOG = LoggerFactory.getLogger(FakeSourceToDorisIT.class);

    private static final String DORIS_DRIVER = "com.mysql.cj.jdbc.Driver";
    private static final String DORIS_CONNECTION_URL = "jdbc:mysql://localhost:9030?rewriteBatchedStatements=true";
    private static final String DORIS_PASSWD = "";
    private static final String DORIS_USERNAME = "root";

    private static final String DORIS_DATABASE = "test";
    private static final String DORIS_TABLE = "seatunnel";
    private static final String DORIS_DATABASE_DDL = "CREATE DATABASE IF NOT EXISTS `" + DORIS_DATABASE + "`";
    private static final String DORIS_USE_DATABASE = "USE `" + DORIS_DATABASE + "`";
    private static final String DORIS_TABLE_DDL = "CREATE TABLE IF NOT EXISTS `" + DORIS_DATABASE + "`.`" + DORIS_TABLE + "` ( " +
        "  `user_id` LARGEINT NOT NULL COMMENT 'id'," +
        "  `date` DATE NOT NULL COMMENT 'date'," +
        "  `city` VARCHAR(20) COMMENT 'city'," +
        "  `age` SMALLINT COMMENT 'age'," +
        "  `sex` TINYINT COMMENT 'sec'," +
        "  `last_visit_date` DATETIME REPLACE DEFAULT '1970-01-01 00:00:00' ," +
        "  `cost` BIGINT SUM DEFAULT '0' ," +
        "  `max_dwell_time` INT MAX DEFAULT '0' ," +
        "  `min_dwell_time` INT MIN DEFAULT '99999'" +
        ") AGGREGATE KEY(`user_id`, `date`, `city`, `age`, `sex`) DISTRIBUTED BY HASH(`user_id`) BUCKETS 1 PROPERTIES (" +
        "  'replication_allocation' = 'tag.location.default: 1'" +
        ");";

    private static final String DORIS_TRUNCATE_TABLE = "TRUNCATE TABLE `" + DORIS_TABLE + "`";
    private static final String DORIS_SELECT_TABLE = "SELECT COUNT(*) FROM `" + DORIS_TABLE + "`";

    //thanks zhaomin1432 provided the doris images.
    private static final String DORIS_IMAGE_NAME = "zhaomin1423/doris:1.0.0-b2";
    private static final int DORIS_FE_PORT = 8030;
    private static final int DORIS_QUERY_PORT = 8040;
    private static final int DORIS_BE_PORT = 9030;

    private GenericContainer<?> dorisStandaloneServer;
    private Connection connection;

    @BeforeEach
    public void beforeEach() throws InterruptedException {
        dorisStandaloneServer = new GenericContainer<>(DORIS_IMAGE_NAME)
            .withNetwork(NETWORK)
            .withNetworkAliases("seatunnel-doris-network")
            .withLogConsumer(new Slf4jLogConsumer(LOG));
        List<String> portBindings = Lists.newArrayList();
        portBindings.add(String.format("%s:%s", DORIS_FE_PORT, DORIS_FE_PORT));
        portBindings.add(String.format("%s:%s", DORIS_QUERY_PORT, DORIS_QUERY_PORT));
        portBindings.add(String.format("%s:%s", DORIS_BE_PORT, DORIS_BE_PORT));
        dorisStandaloneServer.setPortBindings(portBindings);
        Startables.deepStart(Stream.of(dorisStandaloneServer)).join();
        Thread.sleep(TimeUnit.MINUTES.toMillis(1));
        dorisStandaloneServer.waitingFor(new HostPortWaitStrategy());
        LOG.info("Doris frontend endpoint and backend endpoint started.");
        initializeDoris();
    }

    private void initializeDoris() {
        try {
            connection = getDorisConnection();
            Statement statement = connection.createStatement();
            statement.execute(DORIS_DATABASE_DDL);
            statement.execute(DORIS_USE_DATABASE);
            statement.execute(DORIS_TABLE_DDL);
            statement.execute(DORIS_TRUNCATE_TABLE);
            statement.close();
            LOG.info("initialized connection.");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static Connection getDorisConnection() throws ClassNotFoundException, SQLException {
        Class.forName(DORIS_DRIVER);
        return DriverManager.getConnection(DORIS_CONNECTION_URL, DORIS_USERNAME, DORIS_PASSWD);
    }

    private int queryDorisTableCount() {
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(DORIS_SELECT_TABLE);) {
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            throw new IllegalArgumentException(e);
        }
        return -1;
    }

    @AfterEach
    public void afterEach() throws SQLException {
        if (Objects.nonNull(connection)) {
            connection.close();
        }
        if (Objects.nonNull(dorisStandaloneServer)) {
            dorisStandaloneServer.close();
        }
    }

    //Caused by some reasons, doris image can't run in Mac M1.
    @Test
    public void testFakeSourceToDorisSink() throws IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelFlinkJob("/doris/fakesource_to_doris.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(10, queryDorisTableCount());
    }
}
