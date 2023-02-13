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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.UniqueDatabase;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.file.Path;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

@DisabledOnContainer(value = {}, type = {EngineType.SEATUNNEL, EngineType.SPARK})
public class DebeziumToKafkaIT extends TestSuiteBase implements TestResource {

    private static final Logger LOG = LoggerFactory.getLogger(DebeziumToKafkaIT.class);

    private static DockerComposeContainer COMPOSE_CONTAINER;

    //----------------------------------------------------------------------------
    // kafka

    private static final String KAFKA_TOPIC = "customer";

    private static final int KAFKA_PORT = 9092;

    private KafkaConsumer<String, String> kafkaConsumer;

    //----------------------------------------------------------------------------
    // postgres
    private static final String PG_IMAGE = "postgres:alpine3.16";

    private static final String PG_DRIVER_JAR = "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar";

    private static PostgreSQLContainer<?> POSTGRESQL_CONTAINER;

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory = container -> {
        Container.ExecResult extraCommands = container.execInContainer("bash", "-c", "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && curl -O " + PG_DRIVER_JAR);
        Assertions.assertEquals(0, extraCommands.getExitCode());

        Path jsonPath = ContainerUtil.getResourcesFile("/debezium/register-mysql.json").toPath();
        container.copyFileToContainer(MountableFile.forHostPath(jsonPath), "/tmp/seatunnel/plugins/Jdbc/register-mysql.json");
        Container.ExecResult extraCommand = container.execInContainer("bash", "-c", "cd /tmp/seatunnel/plugins/Jdbc && curl -i -X POST -H \"Accept:application/json\" -H  \"Content-Type:application/json\" http://" + getLinuxLocalIp() + ":8083/connectors/ -d @register-mysql.json");
        Assertions.assertEquals(0, extraCommand.getExitCode());
    };

    private void createPostgreSQLContainer() throws ClassNotFoundException {
        POSTGRESQL_CONTAINER = new PostgreSQLContainer<>(DockerImageName.parse(PG_IMAGE))
                .withNetwork(NETWORK)
                .withNetworkAliases("postgresql")
                .withExposedPorts(5432)
                .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(PG_IMAGE)));
    }

    @BeforeAll
    @Override
    public void startUp() throws ClassNotFoundException {
        COMPOSE_CONTAINER =
                new DockerComposeContainer(
                        new File("src/test/resources/docker/docker-compose-mysql.yaml"));
        Startables.deepStart(Stream.of(COMPOSE_CONTAINER)).join();

        LOG.info("The stage: Starting PostgreSQL container...");
        createPostgreSQLContainer();
        Startables.deepStart(Stream.of(POSTGRESQL_CONTAINER)).join();
        Class.forName(POSTGRESQL_CONTAINER.getDriverClassName());
        LOG.info("postgresql Containers are started");

        given().ignoreExceptions()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(180, TimeUnit.SECONDS)
                .untilAsserted(this::initKafkaConsumer);

        given().ignoreExceptions()
                .await()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(this::initializeJdbcTable);
    }

    @TestTemplate
    public void testKafakSinkDebeziumFormat(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/kafkasource_debezium_to_kafka.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
        ArrayList<Object> result = new ArrayList<>();
        List<String> expectedResult =
                Arrays.asList(
                        "{\"data\":{\"id\":101,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":\"3.14\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":102,\"name\":\"car battery\",\"description\":\"12V car battery\",\"weight\":\"8.1\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":103,\"name\":\"12-pack drill bits\",\"description\":\"12-pack of drill bits with sizes ranging from #40 to #3\",\"weight\":\"0.8\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":104,\"name\":\"hammer\",\"description\":\"12oz carpenter's hammer\",\"weight\":\"0.75\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":105,\"name\":\"hammer\",\"description\":\"14oz carpenter's hammer\",\"weight\":\"0.875\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":106,\"name\":\"hammer\",\"description\":\"16oz carpenter's hammer\",\"weight\":\"1.0\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":107,\"name\":\"rocks\",\"description\":\"box of assorted rocks\",\"weight\":\"5.3\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":108,\"name\":\"jacket\",\"description\":\"water resistent black wind breaker\",\"weight\":\"0.1\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":109,\"name\":\"spare tire\",\"description\":\"24 inch spare tire\",\"weight\":\"22.2\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":101,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":\"3.14\"},\"type\":\"DELETE\"}",
                        "{\"data\":{\"id\":101,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":\"4.56\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":107,\"name\":\"rocks\",\"description\":\"box of assorted rocks\",\"weight\":\"5.3\"},\"type\":\"DELETE\"}",
                        "{\"data\":{\"id\":107,\"name\":\"rocks\",\"description\":\"box of assorted rocks\",\"weight\":\"7.88\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":109,\"name\":\"spare tire\",\"description\":\"24 inch spare tire\",\"weight\":\"22.2\"},\"type\":\"DELETE\"}"
                );

        ArrayList<String> topics = new ArrayList<>();
        topics.add(KAFKA_TOPIC);
        kafkaConsumer.subscribe(topics);
        ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(10000));
        for (ConsumerRecord<String, String> record : consumerRecords) {
            result.add(record.value());
        }
        Assertions.assertEquals(expectedResult, result);
    }

    private void initKafkaConsumer() {
        Properties prop = new Properties();
        String bootstrapServers = getBootstrapServers();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "ONE");
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        kafkaConsumer = new KafkaConsumer<>(prop);
    }

    private void initializeJdbcTable() {
        try (Connection connection = DriverManager.getConnection(POSTGRESQL_CONTAINER.getJdbcUrl(),
                POSTGRESQL_CONTAINER.getUsername(), POSTGRESQL_CONTAINER.getPassword())) {
            Statement statement = connection.createStatement();
            String sink = "create table sink(\n" +
                    "id INT NOT NULL PRIMARY KEY,\n" +
                    "first_name varchar(255),\n" +
                    "last_name varchar(255),\n" +
                    "email varchar(255)" +
                    ")";
            statement.execute(sink);
        } catch (SQLException e) {
            throw new RuntimeException("Initializing PostgreSql table failed!", e);
        }
    }

    @Override
    public void tearDown() {
        COMPOSE_CONTAINER.close();
        POSTGRESQL_CONTAINER.close();
    }

    public String getBootstrapServers() {
        return String.format("PLAINTEXT://%s:%s", getLinuxLocalIp(), 9092);
    }

    public String getLinuxLocalIp() {
        String ip = "";
        try {
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
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
