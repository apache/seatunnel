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

package org.apache.seatunnel.e2e.connector.kafka;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

@DisabledOnContainer(
        value = {},
        type = {EngineType.SEATUNNEL, EngineType.SPARK})
public class DebeziumToKafkaIT extends TestSuiteBase implements TestResource {

    private static final Logger LOG = LoggerFactory.getLogger(DebeziumToKafkaIT.class);

    private static GenericContainer<?> DEBEZIUM_CONTAINER;

    private static final String DEBEZIUM_DOCKER_IMAGE = "debezium/connect:2.1";

    private static final String DEBEZIUM_HOST = "debezium_e2e";

    private static final int DEBEZIUM_PORT = 8083;

    // ----------------------------------------------------------------------------
    // kafka
    private static final String KAFKA_IMAGE_NAME = "confluentinc/cp-kafka:6.2.1";

    private static final int KAFKA_PORT = 9095;

    private static final String KAFKA_HOST = "kafka";

    private KafkaProducer<String, String> producer;

    private KafkaContainer KAFKA_CONTAINER;

    // ----------------------------------------------------------------------------
    // mysql
    private static final String MYSQL_IMAGE = "quay.io/debezium/example-mysql:2.1";

    private static final String MYSQL_HOST = "mysql";

    private static final int MYSQL_PORT = 3306;

    private static GenericContainer<?> MYSQL_CONTAINER;

    // ----------------------------------------------------------------------------
    // postgres
    private static final String PG_IMAGE = "postgres:alpine3.16";

    private static final int PG_PORT = 5432;

    private static final String PG_DRIVER_JAR =
            "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar";

    private static PostgreSQLContainer<?> POSTGRESQL_CONTAINER;

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                Path jsonPath =
                        ContainerUtil.getResourcesFile("/debezium/register-mysql.json").toPath();
                container.copyFileToContainer(
                        MountableFile.forHostPath(jsonPath),
                        "/tmp/seatunnel/plugins/Jdbc/register-mysql.json");
                Container.ExecResult extraCommand =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "cd /tmp/seatunnel/plugins/Jdbc && curl -i -X POST -H \"Accept:application/json\" -H  \"Content-Type:application/json\" http://"
                                        + getLinuxLocalIp()
                                        + ":8083/connectors/ -d @register-mysql.json");
                Assertions.assertEquals(0, extraCommand.getExitCode());

                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && curl -O "
                                        + PG_DRIVER_JAR);
                Assertions.assertEquals(0, extraCommands.getExitCode());

                given().ignoreExceptions()
                        .await()
                        .atLeast(100, TimeUnit.MILLISECONDS)
                        .pollInterval(500, TimeUnit.MILLISECONDS)
                        .atMost(2, TimeUnit.MINUTES)
                        .untilAsserted(this::updateMysqlTable);
            };

    private void createDebeziumContainer() {
        DEBEZIUM_CONTAINER =
                new GenericContainer<>(DEBEZIUM_DOCKER_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(DEBEZIUM_HOST)
                        .withEnv("GROUP_ID", "1")
                        .withEnv("CONFIG_STORAGE_TOPIC", "my-connect-configs")
                        .withEnv("OFFSET_STORAGE_TOPIC", "my-connect-offsets")
                        .withEnv("BOOTSTRAP_SERVERS", "kafka:9092")
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(DEBEZIUM_DOCKER_IMAGE)));
        DEBEZIUM_CONTAINER.setPortBindings(
                com.google.common.collect.Lists.newArrayList(
                        String.format("%s:%s", DEBEZIUM_PORT, DEBEZIUM_PORT)));
    }

    private void createKafkaContainer() {
        KAFKA_CONTAINER =
                new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE_NAME))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(KAFKA_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(KAFKA_IMAGE_NAME)));
        KAFKA_CONTAINER.setPortBindings(
                com.google.common.collect.Lists.newArrayList(
                        String.format("%s:%s", KAFKA_PORT, KAFKA_PORT)));
    }

    private void createPostgreSQLContainer() throws ClassNotFoundException {
        POSTGRESQL_CONTAINER =
                new PostgreSQLContainer<>(DockerImageName.parse(PG_IMAGE))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("postgresql")
                        .withExposedPorts(PG_PORT)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(PG_IMAGE)));
    }

    @BeforeAll
    @Override
    public void startUp() throws Exception {

        LOG.info("The first stage: Starting Kafka containers...");
        createKafkaContainer();
        Startables.deepStart(Stream.of(KAFKA_CONTAINER)).join();
        LOG.info("Kafka Containers are started");

        LOG.info("The second stage: Starting Mysql containers...");
        MYSQL_CONTAINER =
                new GenericContainer<>(DockerImageName.parse(MYSQL_IMAGE))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MYSQL_HOST)
                        .withExposedPorts(MYSQL_PORT)
                        .withEnv("MYSQL_ROOT_PASSWORD", "debezium")
                        .withEnv("MYSQL_USER", "mysqluser")
                        .withEnv("MYSQL_PASSWORD", "mysqlpw")
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(MYSQL_IMAGE)));
        MYSQL_CONTAINER.setPortBindings(
                com.google.common.collect.Lists.newArrayList(
                        String.format("%s:%s", MYSQL_PORT, MYSQL_PORT)));
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        LOG.info("Mysql Containers are started");

        Awaitility.given()
                .ignoreExceptions()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(180, TimeUnit.SECONDS)
                .untilAsserted(() -> initKafkaProducer());

        LOG.info("The third stage: Starting Canal containers...");
        createDebeziumContainer();
        Startables.deepStart(Stream.of(DEBEZIUM_CONTAINER)).join();
        LOG.info("Canal Containers are started");

        LOG.info("The fourth stage: Starting PostgreSQL container...");
        createPostgreSQLContainer();
        Startables.deepStart(Stream.of(POSTGRESQL_CONTAINER)).join();
        Class.forName(POSTGRESQL_CONTAINER.getDriverClassName());
        LOG.info("postgresql Containers are started");

        given().ignoreExceptions()
                .await()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(this::initializeJdbcTable);
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (producer != null) {
            producer.close();
        }
    }

    @TestTemplate
    public void testKafakSinkDebeziumFormat(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/kafkasource_debezium_to_kafka.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        List<String> expectedResult =
                Arrays.asList(
                        "{\"before\":null,\"after\":{\"id\":101,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":3.14},\"op\":\"c\"}",
                        "{\"before\":null,\"after\":{\"id\":102,\"name\":\"car battery\",\"description\":\"12V car battery\",\"weight\":8.1},\"op\":\"c\"}",
                        "{\"before\":null,\"after\":{\"id\":103,\"name\":\"12-pack drill bits\",\"description\":\"12-pack of drill bits with sizes ranging from #40 to #3\",\"weight\":0.8},\"op\":\"c\"}",
                        "{\"before\":null,\"after\":{\"id\":104,\"name\":\"hammer\",\"description\":\"12oz carpenter's hammer\",\"weight\":0.75},\"op\":\"c\"}",
                        "{\"before\":null,\"after\":{\"id\":105,\"name\":\"hammer\",\"description\":\"14oz carpenter's hammer\",\"weight\":0.875},\"op\":\"c\"}",
                        "{\"before\":null,\"after\":{\"id\":106,\"name\":\"hammer\",\"description\":\"16oz carpenter's hammer\",\"weight\":1.0},\"op\":\"c\"}",
                        "{\"before\":null,\"after\":{\"id\":107,\"name\":\"rocks\",\"description\":\"box of assorted rocks\",\"weight\":5.3},\"op\":\"c\"}",
                        "{\"before\":null,\"after\":{\"id\":108,\"name\":\"jacket\",\"description\":\"water resistent black wind breaker\",\"weight\":0.1},\"op\":\"c\"}",
                        "{\"before\":null,\"after\":{\"id\":109,\"name\":\"spare tire\",\"description\":\"24 inch spare tire\",\"weight\":22.2},\"op\":\"c\"}",
                        "{\"before\":{\"id\":101,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":3.14},\"after\":null,\"op\":\"d\"}",
                        "{\"before\":null,\"after\":{\"id\":101,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":4.56},\"op\":\"c\"}",
                        "{\"before\":{\"id\":109,\"name\":\"spare tire\",\"description\":\"24 inch spare tire\",\"weight\":22.2},\"after\":null,\"op\":\"d\"}");
        String topicName = "test-debezium-sink";
        ArrayList<String> data = getKafkaConsumerData(topicName);
        Assertions.assertEquals(expectedResult, data);
    }

    private void initKafkaProducer() {
        Properties props = new Properties();
        String bootstrapServers = KAFKA_CONTAINER.getBootstrapServers();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producer = new KafkaProducer<>(props);
    }

    private Properties kafkaConsumerConfig() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "seatunnel-debezium-sink-group");
        props.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                OffsetResetStrategy.EARLIEST.toString().toLowerCase());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }

    private void initializeJdbcTable() {
        try (Connection connection =
                DriverManager.getConnection(
                        POSTGRESQL_CONTAINER.getJdbcUrl(),
                        POSTGRESQL_CONTAINER.getUsername(),
                        POSTGRESQL_CONTAINER.getPassword())) {
            Statement statement = connection.createStatement();
            String sink =
                    "create table sink(\n"
                            + "id INT NOT NULL PRIMARY KEY,\n"
                            + "name varchar(255),\n"
                            + "description varchar(255),\n"
                            + "weight varchar(255)"
                            + ")";
            statement.execute(sink);
        } catch (SQLException e) {
            throw new RuntimeException("Initializing PostgreSql table failed!", e);
        }
    }

    private void updateMysqlTable() {

        try (Connection connection =
                DriverManager.getConnection(
                        "jdbc:mysql://0.0.0.0:3306/inventory", "mysqluser", "mysqlpw")) {
            Statement stmt = connection.createStatement();
            stmt.execute("SET FOREIGN_KEY_CHECKS=0");
            stmt.execute("UPDATE products SET weight = '4.56' WHERE name = 'scooter'"); // update
            stmt.execute("DELETE FROM products WHERE name  = 'spare tire'"); // delete
        } catch (SQLException e) {
            throw new RuntimeException("Update Mysql table failed!", e);
        }
    }

    private ArrayList<String> getKafkaConsumerData(String topicName) {
        ArrayList<String> data = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaConsumerConfig())) {
            ArrayList<String> topics = new ArrayList<>();
            topics.add(topicName);
            consumer.subscribe(topics);
            ConsumerRecords<String, String> consumerRecords =
                    consumer.poll(Duration.ofSeconds(10000));
            for (ConsumerRecord<String, String> record : consumerRecords) {
                data.add(record.value());
            }
        }
        return data;
    }

    public String getLinuxLocalIp() {
        String ip = "";
        try {
            Enumeration<NetworkInterface> networkInterfaces =
                    NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface networkInterface = networkInterfaces.nextElement();
                Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
                while (inetAddresses.hasMoreElements()) {
                    InetAddress inetAddress = inetAddresses.nextElement();
                    if (!inetAddress.isLoopbackAddress() && inetAddress instanceof Inet4Address) {
                        ip = inetAddress.getHostAddress();
                    }
                }
            }
        } catch (SocketException ex) {
            ex.printStackTrace();
        }
        return ip;
    }
}
