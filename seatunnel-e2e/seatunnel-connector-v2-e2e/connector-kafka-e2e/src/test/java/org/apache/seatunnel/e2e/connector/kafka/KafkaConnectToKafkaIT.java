/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.e2e.connector.kafka;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.common.collect.Lists;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK})
public class KafkaConnectToKafkaIT extends TestSuiteBase implements TestResource {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaConnectToKafkaIT.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    // kafka
    private static final String KAFKA_IMAGE_NAME = "confluentinc/cp-kafka:latest";

    private static final String KAFKA_JDBC_TOPIC = "jdbc_source_record";

    private static final String KAFKA_HOST = "kafka_connect_source_record";

    private static KafkaContainer KAFKA_CONTAINER;

    private KafkaProducer<byte[], byte[]> kafkaProducer;

    // -----------------------------------mysql-----------------------------------------
    private static MySqlContainer MYSQL_CONTAINER;
    private static final String MYSQL_DATABASE = "seatunnel";
    private static final String MYSQL_HOST = "kafka_to_mysql_e2e";
    private static final int MYSQL_PORT = 3306;
    private static final String MYSQL_DRIVER_JAR =
            "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar";

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && curl -O "
                                        + MYSQL_DRIVER_JAR);
                Assertions.assertEquals(0, extraCommands.getExitCode());
            };

    private static MySqlContainer createMySqlContainer(MySqlVersion version) {
        MySqlContainer mySqlContainer =
                new MySqlContainer(version)
                        .withConfigurationOverride("docker/server-gtids/my.cnf")
                        .withSetupSQL("docker/setup.sql")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MYSQL_HOST)
                        .withDatabaseName("seatunnel")
                        .withUsername("st_user")
                        .withPassword("seatunnel")
                        .withLogConsumer(new Slf4jLogConsumer(LOG));
        mySqlContainer.setPortBindings(
                com.google.common.collect.Lists.newArrayList(
                        String.format("%s:%s", MYSQL_PORT, MYSQL_PORT)));
        return mySqlContainer;
    }

    private void createKafkaContainer() {
        KAFKA_CONTAINER =
                new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE_NAME))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(KAFKA_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(KAFKA_IMAGE_NAME)));
    }

    @BeforeAll
    @Override
    public void startUp() {

        LOG.info("The first stage: Starting Kafka containers...");
        createKafkaContainer();
        Startables.deepStart(Stream.of(KAFKA_CONTAINER)).join();
        LOG.info("Kafka Containers are started");

        given().ignoreExceptions()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(this::initKafkaProducer);

        LOG.info("The second stage: Starting Mysql containers...");
        MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V8_0);
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        LOG.info("Mysql Containers are started");

        given().ignoreExceptions()
                .await()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(this::initializeDatabase);

        given().ignoreExceptions()
                .await()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(this::initializeJdbcTable);

        log.info("Write 3 records to topic " + KAFKA_JDBC_TOPIC);
        generateConnectJdbcRecord();
    }

    @TestTemplate
    public void testJdbcRecordKafkaToMysql(TestContainer container)
            throws IOException, InterruptedException, SQLException {
        Container.ExecResult execResult =
                container.executeJob("/kafkasource_jdbc_record_to_mysql.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
        List<Object> actual = new ArrayList<>();
        try (Connection connection =
                DriverManager.getConnection(
                        MYSQL_CONTAINER.getJdbcUrl(),
                        MYSQL_CONTAINER.getUsername(),
                        MYSQL_CONTAINER.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                ResultSet resultSet =
                        statement.executeQuery("select * from seatunnel.jdbc_sink order by id");
                while (resultSet.next()) {
                    List<Object> row =
                            Arrays.asList(
                                    resultSet.getInt("id"),
                                    resultSet.getString("name"),
                                    resultSet.getString("description"),
                                    resultSet.getString("weight"));
                    actual.add(row);
                }
            }
        }
        List<Object> expected =
                Lists.newArrayList(
                        Arrays.asList(15, "test", "test", "20"),
                        Arrays.asList(16, "test-001", "test", "30"),
                        Arrays.asList(18, "sdc", "sdc", "sdc"));
        Assertions.assertIterableEquals(expected, actual);

        try (Connection connection =
                DriverManager.getConnection(
                        MYSQL_CONTAINER.getJdbcUrl(),
                        MYSQL_CONTAINER.getUsername(),
                        MYSQL_CONTAINER.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("truncate table seatunnel.jdbc_sink");
                LOG.info("testJdbcRecordKafkaToMysql truncate table sink");
            }
        }
    }

    @SneakyThrows
    public void generateConnectJdbcRecord() {
        String[] jdbcSourceRecords = {
            "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"int64\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"string\",\"optional\":true,\"field\":\"description\"},{\"type\":\"string\",\"optional\":true,\"field\":\"weight\"}],\"optional\":false,\"name\":\"test_database_001.seatunnel_test_cdc\"},\"payload\":{\"id\":15,\"name\":\"test\",\"description\":\"test\",\"weight\":\"20\"}}",
            "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"int64\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"string\",\"optional\":true,\"field\":\"description\"},{\"type\":\"string\",\"optional\":true,\"field\":\"weight\"}],\"optional\":false,\"name\":\"test_database_001.seatunnel_test_cdc\"},\"payload\":{\"id\":16,\"name\":\"test-001\",\"description\":\"test\",\"weight\":\"30\"}}",
            "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"int64\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"string\",\"optional\":true,\"field\":\"description\"},{\"type\":\"string\",\"optional\":true,\"field\":\"weight\"}],\"optional\":false,\"name\":\"test_database_001.seatunnel_test_cdc\"},\"payload\":{\"id\":18,\"name\":\"sdc\",\"description\":\"sdc\",\"weight\":\"sdc\"}}"
        };
        for (String value : jdbcSourceRecords) {
            JsonNode jsonNode = objectMapper.readTree(value);
            byte[] bytes = objectMapper.writeValueAsBytes(jsonNode);
            ProducerRecord<byte[], byte[]> producerRecord =
                    new ProducerRecord<>(KAFKA_JDBC_TOPIC, null, bytes);
            kafkaProducer.send(producerRecord).get();
        }
    }

    private void initKafkaProducer() {
        Properties props = new Properties();
        String bootstrapServers = KAFKA_CONTAINER.getBootstrapServers();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        kafkaProducer = new KafkaProducer<>(props);
    }

    @Override
    public void tearDown() {
        MYSQL_CONTAINER.close();
        KAFKA_CONTAINER.close();
    }

    protected void initializeDatabase() {
        try (Connection connection =
                DriverManager.getConnection(
                        MYSQL_CONTAINER.getJdbcUrl(),
                        MYSQL_CONTAINER.getUsername(),
                        MYSQL_CONTAINER.getPassword())) {
            Statement statement = connection.createStatement();
            String sql = "CREATE DATABASE IF NOT EXISTS " + MYSQL_DATABASE;
            statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException("Initializing Mysql database failed!", e);
        }
    }

    private void initializeJdbcTable() {
        try (Connection connection =
                DriverManager.getConnection(
                        MYSQL_CONTAINER.getJdbcUrl(),
                        MYSQL_CONTAINER.getUsername(),
                        MYSQL_CONTAINER.getPassword())) {
            Statement statement = connection.createStatement();
            String jdbcSink =
                    "CREATE TABLE IF NOT EXISTS seatunnel.jdbc_sink(\n"
                            + "id INT NOT NULL PRIMARY KEY,\n"
                            + "name varchar(255),\n"
                            + "description varchar(255),\n"
                            + "weight varchar(255)"
                            + ")";
            statement.execute(jdbcSink);
        } catch (SQLException e) {
            throw new RuntimeException("Initializing Mysql table failed!", e);
        }
    }
}
