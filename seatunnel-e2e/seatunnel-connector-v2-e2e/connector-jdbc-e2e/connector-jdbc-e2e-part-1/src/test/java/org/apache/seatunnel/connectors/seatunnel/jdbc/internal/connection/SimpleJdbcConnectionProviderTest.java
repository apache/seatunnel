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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.sink.JdbcSink;
import org.apache.seatunnel.connectors.seatunnel.jdbc.sink.JdbcSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSource;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.common.collect.Lists;
import com.mysql.cj.jdbc.ConnectionImpl;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK})
public class SimpleJdbcConnectionProviderTest extends TestSuiteBase implements TestResource {
    private GenericContainer<?> mc;
    private static final String SQL = "select * from test";

    private static final String DOCKER_IMAGE = "d87904488/starrocks-starter:2.2.1";

    private static final String NETWORK_ALIASES = "e2e_starRocksdb";

    private static final String URL = "jdbc:mysql://HOST:9030/test";

    private static final String USERNAME = "root";
    private static final String PASSWORD = "";

    private static final String CREATE_SQL =
            "create table test.test (\n"
                    + "  BIGINT_COL     BIGINT,\n"
                    + "  LARGEINT_COL   LARGEINT,\n"
                    + "  SMALLINT_COL   SMALLINT,\n"
                    + "  TINYINT_COL    TINYINT,\n"
                    + "  BOOLEAN_COL    BOOLEAN,\n"
                    + "  DECIMAL_COL    DECIMAL,\n"
                    + "  DOUBLE_COL     DOUBLE,\n"
                    + "  FLOAT_COL      FLOAT,\n"
                    + "  INT_COL        INT,\n"
                    + "  CHAR_COL       CHAR,\n"
                    + "  VARCHAR_11_COL VARCHAR(11),\n"
                    + "  STRING_COL     STRING,\n"
                    + "  DATETIME_COL   DATETIME,\n"
                    + "  DATE_COL       DATE\n"
                    + ")ENGINE=OLAP\n"
                    + "DUPLICATE KEY(`BIGINT_COL`)\n"
                    + "DISTRIBUTED BY HASH(`BIGINT_COL`) BUCKETS 1\n"
                    + "PROPERTIES (\n"
                    + "\"replication_num\" = \"1\",\n"
                    + "\"in_memory\" = \"false\","
                    + "\"storage_format\" = \"DEFAULT\""
                    + ");";

    @BeforeAll
    @Override
    public void startUp() {
        mc =
                new GenericContainer<>(DOCKER_IMAGE)
                        .withNetwork(Network.newNetwork())
                        .withNetworkAliases(NETWORK_ALIASES)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(DOCKER_IMAGE)));
        mc.setPortBindings(Lists.newArrayList(String.format("%s:%s", 9030, 9030)));

        Startables.deepStart(Stream.of(mc)).join();

        given().ignoreExceptions()
                .await()
                .atMost(120, TimeUnit.SECONDS)
                .untilAsserted(() -> this.getJdbcConnection());
        create(CREATE_SQL);
    }

    private Connection getJdbcConnection() throws SQLException {

        return DriverManager.getConnection(
                getUrl() + "?createDatabaseIfNotExist=true", USERNAME, PASSWORD);
    }

    private String getUrl() {
        return URL.replace("HOST", mc.getHost());
    }

    private void create(String sql) {
        try (Connection connection = getJdbcConnection()) {
            connection.createStatement().execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @TestTemplate
    void parametersTest(TestContainer container)
            throws SQLException, IOException, ClassNotFoundException {
        defaultSinkParametersTest();
        defaultSourceParametersTest();
    }

    void defaultSinkParametersTest() throws IOException, SQLException, ClassNotFoundException {
        // case1 url not contains parameters and properties not contains parameters
        JdbcSink jdbcSink1 = new JdbcSink();
        HashMap<String, Object> map1 = getMap();
        map1.put("url", getUrl());
        Config config1 = ConfigFactory.parseMap(map1);
        Properties connectionProperties1 = getSinkProperties(jdbcSink1, config1);
        Assertions.assertEquals(connectionProperties1.get("rewriteBatchedStatements"), "true");

        // case2 url contains parameters and properties not contains parameters
        JdbcSink jdbcSink2 = new JdbcSink();
        HashMap<String, Object> map2 = getMap();
        map2.put("url", getUrl() + "?rewriteBatchedStatements=false");
        Config config2 = ConfigFactory.parseMap(map2);
        Properties connectionProperties2 = getSinkProperties(jdbcSink2, config2);
        Assertions.assertEquals(connectionProperties2.get("rewriteBatchedStatements"), "true");

        // case3 url not contains parameters and properties not contains parameters
        JdbcSink jdbcSink3 = new JdbcSink();
        HashMap<String, Object> map3 = getMap();
        HashMap<String, String> properties3 = new HashMap<>();
        properties3.put("rewriteBatchedStatements", "false");
        map3.put("properties", properties3);
        map3.put("url", getUrl());
        Config config3 = ConfigFactory.parseMap(map3);
        Properties connectionProperties3 = getSinkProperties(jdbcSink3, config3);
        Assertions.assertEquals(connectionProperties3.get("rewriteBatchedStatements"), "false");

        // case4 url contains parameters and properties contains parameters
        JdbcSink jdbcSink4 = new JdbcSink();
        HashMap<String, Object> map4 = getMap();
        HashMap<String, String> properties4 = new HashMap<>();
        properties4.put("useSSL", "true");
        properties4.put("rewriteBatchedStatements", "false");
        map4.put("properties", properties4);
        map4.put("url", getUrl() + "?useSSL=false&rewriteBatchedStatements=true");
        Config config4 = ConfigFactory.parseMap(map4);
        Properties connectionProperties4 = getSinkProperties(jdbcSink4, config4);
        Assertions.assertEquals(connectionProperties4.get("useSSL"), "true");
        Assertions.assertEquals(connectionProperties4.get("rewriteBatchedStatements"), "false");
    }

    void defaultSourceParametersTest() throws IOException, SQLException, ClassNotFoundException {
        // case1 url not contains parameters and properties not contains parameters
        JdbcSource jdbcSource1 = new JdbcSource();
        HashMap<String, Object> map1 = getMap();
        map1.put("url", getUrl());
        map1.put("query", SQL);
        Config config1 = ConfigFactory.parseMap(map1);
        Properties connectionProperties1 = getSourceProperties(jdbcSource1, config1);
        Assertions.assertEquals(connectionProperties1.get("rewriteBatchedStatements"), "true");

        // case2 url contains parameters and properties not contains parameters
        JdbcSource jdbcSource2 = new JdbcSource();
        HashMap<String, Object> map2 = getMap();
        map2.put("url", getUrl() + "?rewriteBatchedStatements=false");
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
        map3.put("url", getUrl());
        map3.put("query", SQL);
        Config config3 = ConfigFactory.parseMap(map3);
        Properties connectionProperties3 = getSourceProperties(jdbcSource3, config3);
        Assertions.assertEquals(connectionProperties3.get("rewriteBatchedStatements"), "false");

        // case4 url contains parameters and properties contains parameters
        JdbcSource jdbcSource4 = new JdbcSource();
        HashMap<String, Object> map4 = getMap();
        HashMap<String, String> properties4 = new HashMap<>();
        properties4.put("useSSL", "true");
        properties4.put("rewriteBatchedStatements", "false");
        map4.put("properties", properties4);
        map4.put("url", getUrl() + "?useSSL=false&rewriteBatchedStatements=true");
        map4.put("query", SQL);
        Config config4 = ConfigFactory.parseMap(map4);
        Properties connectionProperties4 = getSourceProperties(jdbcSource4, config4);
        Assertions.assertEquals(connectionProperties4.get("useSSL"), "true");
        Assertions.assertEquals(connectionProperties4.get("rewriteBatchedStatements"), "false");
    }

    @NotNull private HashMap<String, Object> getMap() {
        HashMap<String, Object> map = new HashMap<>();
        map.put("driver", "com.mysql.cj.jdbc.Driver");
        map.put("user", USERNAME);
        map.put("password", PASSWORD);
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

    @AfterAll
    @Override
    public void tearDown() throws SQLException {
        // close Container
        if (mc != null) {
            mc.close();
        }
    }
}
