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

package org.apache.seatunnel.e2e.spark.v2.phoenix;

import org.apache.seatunnel.e2e.spark.SparkContainer;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Stream;

@Slf4j
public class FakeSourceToIoPhoenixIT extends SparkContainer {

    private static final String PHOENIX_DOCKER_IMAGE = "iteblog/hbase-phoenix-docker:1.0";
    private static final String PHOENIX_HOST = "spark_e2e_phoenix_sink";
    private static final int PHOENIX_PORT = 8765;
    private static final String PHOENIX_CONNECT_URL = "jdbc:phoenix:thin:url=http://localhost:8765;serialization=PROTOBUF";
    private static final String PHOENIX_JDBC_DRIVER = "org.apache.phoenix.queryserver.client.Driver";

    private GenericContainer<?> phoenixServer;

    @BeforeEach
    public void startPhoenixContainer() throws Exception {
        phoenixServer = new GenericContainer<>(PHOENIX_DOCKER_IMAGE)
                .withNetwork(NETWORK)
                .withNetworkAliases(PHOENIX_HOST)
                .withLogConsumer(new Slf4jLogConsumer(log));
        phoenixServer.setPortBindings(Lists.newArrayList(
                String.format("%s:8765", PHOENIX_PORT)));
        Startables.deepStart(Stream.of(phoenixServer)).join();
        log.info("phoenix container started");
        initializePhoenixTable();
    }

    private void initializePhoenixTable() {
        try  {
            Class.forName(PHOENIX_JDBC_DRIVER);
            Connection connection = DriverManager.getConnection(PHOENIX_CONNECT_URL);
            Statement statement = connection.createStatement();
            String sql = "create table test.test(\n" +
                    " name VARCHAR PRIMARY KEY,\n" +
                    " age INTEGER)";
            statement.execute(sql);
        } catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException("Initializing  table failed!", e);
        }
    }

    /**
     * fake source -> Phoenix sink
     */
    //@Test
    public void testFakeSourceToPhoenix() throws Exception {
        Container.ExecResult execResult = executeSeaTunnelSparkJob("/phoenix/fakesource_to_phoenix.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        // query result
        String sql = "select * from test.test";
        Class.forName(PHOENIX_JDBC_DRIVER);
        try (Connection connection = DriverManager.getConnection(PHOENIX_CONNECT_URL)) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            List<String> result = Lists.newArrayList();
            while (resultSet.next()) {
                result.add(resultSet.getString("name"));
            }
            Assertions.assertFalse(result.isEmpty());
        }
    }

    @AfterEach
    public void closePhoenixContainer() {
        if (phoenixServer != null) {
            phoenixServer.stop();
        }
    }
}
