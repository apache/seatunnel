/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.sink.JdbcSink;
import org.apache.seatunnel.connectors.seatunnel.jdbc.sink.JdbcSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSource;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import com.mysql.cj.jdbc.ConnectionImpl;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;
import java.util.stream.Stream;

public class SimpleJdbcConnectionProviderTest {

    private MySqlContainer mc;
    private static final String SQL = "select * from test";
    private static final String MYSQL_CONTAINER_HOST = "mysql-e2e";
    private static final String MYSQL_DATABASE = "seatunnel";

    private static final String MYSQL_USERNAME = "root1";
    private static final String MYSQL_PASSWORD = "Abc!@#135_seatunnel";
    private static final int MYSQL_PORT = 3306;

    @BeforeEach
    void before() throws Exception {
        mc =
                new MySqlContainer((MySqlVersion.V8_0))
                        .withUsername(MYSQL_USERNAME)
                        .withPassword(MYSQL_PASSWORD)
                        .withDatabaseName(MYSQL_DATABASE)
                        .withNetwork(Network.newNetwork())
                        .withNetworkAliases(MYSQL_CONTAINER_HOST)
                        .withExposedPorts(MYSQL_PORT)
                        .waitingFor(Wait.forHealthcheck())
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger("mysql-docker-image")));

        Startables.deepStart(Stream.of(mc)).join();
        create("CREATE TABLE IF NOT EXISTS test (`id` int(11))");
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(mc.getJdbcUrl(), mc.getUsername(), mc.getPassword());
    }

    private void create(String sql) {
        try (Connection connection = getJdbcConnection()) {
            connection.createStatement().execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void defaultSinkParametersTest() throws IOException, SQLException, ClassNotFoundException {
        // case1 url not contains parameters and properties not contains parameters
        JdbcSink jdbcSink1 = new JdbcSink();
        HashMap<String, Object> map1 = getMap();
        map1.put("url", mc.getJdbcUrl());
        Config config1 = ConfigFactory.parseMap(map1);
        Properties connectionProperties1 = getSinkProperties(jdbcSink1, config1);
        Assertions.assertEquals(connectionProperties1.get("rewriteBatchedStatements"), "true");

        // case2 url contains parameters and properties not contains parameters
        JdbcSink jdbcSink2 = new JdbcSink();
        HashMap<String, Object> map2 = getMap();
        map2.put("url", mc.getJdbcUrl() + "?rewriteBatchedStatements=false");
        Config config2 = ConfigFactory.parseMap(map2);
        Properties connectionProperties2 = getSinkProperties(jdbcSink2, config2);
        Assertions.assertEquals(connectionProperties2.get("rewriteBatchedStatements"), "true");

        // case3 url not contains parameters and properties not contains parameters
        JdbcSink jdbcSink3 = new JdbcSink();
        HashMap<String, Object> map3 = getMap();
        HashMap<String, String> properties3 = new HashMap<>();
        properties3.put("rewriteBatchedStatements", "false");
        map3.put("properties", properties3);
        map3.put("url", mc.getJdbcUrl());
        Config config3 = ConfigFactory.parseMap(map3);
        Properties connectionProperties3 = getSinkProperties(jdbcSink3, config3);
        Assertions.assertEquals(connectionProperties3.get("rewriteBatchedStatements"), "false");

        // case3 url contains parameters and properties contains parameters
        JdbcSink jdbcSink4 = new JdbcSink();
        HashMap<String, Object> map4 = getMap();
        HashMap<String, String> properties4 = new HashMap<>();
        properties4.put("useSSL", "true");
        properties4.put("rewriteBatchedStatements", "false");
        map4.put("properties", properties4);
        map4.put("url", mc.getJdbcUrl() + "?useSSL=false&rewriteBatchedStatements=true");
        Config config4 = ConfigFactory.parseMap(map4);
        Properties connectionProperties4 = getSinkProperties(jdbcSink4, config4);
        Assertions.assertEquals(connectionProperties4.get("useSSL"), "true");
        Assertions.assertEquals(connectionProperties4.get("rewriteBatchedStatements"), "false");
    }

    @Test
    void defaultSourceParametersTest() throws IOException, SQLException, ClassNotFoundException {
        // case1 url not contains parameters and properties not contains parameters
        JdbcSource jdbcSource1 = new JdbcSource();
        HashMap<String, Object> map1 = getMap();
        map1.put("url", mc.getJdbcUrl());
        map1.put("query", SQL);
        Config config1 = ConfigFactory.parseMap(map1);
        Properties connectionProperties1 = getSourceProperties(jdbcSource1, config1);
        Assertions.assertEquals(connectionProperties1.get("rewriteBatchedStatements"), "true");

        // case2 url contains parameters and properties not contains parameters
        JdbcSource jdbcSource2 = new JdbcSource();
        HashMap<String, Object> map2 = getMap();
        map2.put("url", mc.getJdbcUrl() + "?rewriteBatchedStatements=false");
        map2.put("query", SQL);
        Config config2 = ConfigFactory.parseMap(map2);
        Properties connectionProperties2 = getSourceProperties(jdbcSource2, config2);
        Assertions.assertEquals(connectionProperties2.get("rewriteBatchedStatements"), "true");

        // case3 url not contains parameters and properties not contains parameters
        JdbcSource jdbcSource3 = new JdbcSource();
        HashMap<String, Object> map3 = getMap();
        HashMap<String, String> properties3 = new HashMap<>();
        properties3.put("rewriteBatchedStatements", "false");
        map3.put("properties", properties3);
        map3.put("url", mc.getJdbcUrl());
        map3.put("query", SQL);
        Config config3 = ConfigFactory.parseMap(map3);
        Properties connectionProperties3 = getSourceProperties(jdbcSource3, config3);
        Assertions.assertEquals(connectionProperties3.get("rewriteBatchedStatements"), "false");

        // case3 url contains parameters and properties contains parameters
        JdbcSource jdbcSource4 = new JdbcSource();
        HashMap<String, Object> map4 = getMap();
        HashMap<String, String> properties4 = new HashMap<>();
        properties4.put("useSSL", "true");
        properties4.put("rewriteBatchedStatements", "false");
        map4.put("properties", properties4);
        map4.put("url", mc.getJdbcUrl() + "?useSSL=false&rewriteBatchedStatements=true");
        map4.put("query", SQL);
        Config config4 = ConfigFactory.parseMap(map4);
        Properties connectionProperties4 = getSourceProperties(jdbcSource4, config4);
        Assertions.assertEquals(connectionProperties4.get("useSSL"), "true");
        Assertions.assertEquals(connectionProperties4.get("rewriteBatchedStatements"), "false");
    }

    @NotNull private HashMap<String, Object> getMap() {
        HashMap<String, Object> map = new HashMap<>();
        map.put("driver", "com.mysql.cj.jdbc.Driver");
        map.put("user", mc.getUsername());
        map.put("password", mc.getPassword());
        return map;
    }

    private Properties getSinkProperties(JdbcSink jdbcSink, Config config)
            throws IOException, SQLException, ClassNotFoundException {
        jdbcSink.setTypeInfo(
                new SeaTunnelRowType(
                        new String[] {"id"}, new SeaTunnelDataType<?>[] {BasicType.INT_TYPE}));
        jdbcSink.prepare(config);
        JdbcSinkWriter jdbcSinkWriter = (JdbcSinkWriter) jdbcSink.createWriter(null);
        JdbcConnectionProvider connectionProvider =
                (JdbcConnectionProvider) getFieldValue(jdbcSinkWriter, "connectionProvider");
        ConnectionImpl connection = (ConnectionImpl) connectionProvider.getOrEstablishConnection();
        Properties connectionProperties = connection.getProperties();
        return connectionProperties;
    }

    private Properties getSourceProperties(JdbcSource jdbcSource, Config config)
            throws IOException, SQLException, ClassNotFoundException {
        jdbcSource.prepare(config);
        JdbcConnectionProvider connectionProvider =
                (JdbcConnectionProvider) getFieldValue(jdbcSource, "jdbcConnectionProvider");
        ConnectionImpl connection = (ConnectionImpl) connectionProvider.getOrEstablishConnection();
        Properties connectionProperties = connection.getProperties();
        return connectionProperties;
    }

    private static Object getFieldValue(Object object, String name) {
        Class objClass = object.getClass();
        Field[] fields = objClass.getDeclaredFields();
        for (Field field : fields) {
            try {
                String fieldName = field.getName();
                if (fieldName.equalsIgnoreCase(name)) {
                    field.setAccessible(true);
                    return field.get(object);
                }
            } catch (SecurityException e) {

            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    @AfterEach
    public void tearDown() {
        // close Container
        if (mc != null) {
            mc.close();
        }
    }
}
