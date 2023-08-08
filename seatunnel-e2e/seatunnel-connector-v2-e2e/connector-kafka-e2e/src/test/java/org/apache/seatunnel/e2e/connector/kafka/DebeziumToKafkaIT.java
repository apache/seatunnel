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

import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

@DisabledOnContainer(
        value = {},
        type = {EngineType.SEATUNNEL, EngineType.SPARK})
@Slf4j
public class DebeziumToKafkaIT extends TestSuiteBase implements TestResource {

    private static final Logger LOG = LoggerFactory.getLogger(DebeziumToKafkaIT.class);

    private static GenericContainer<?> DEBEZIUM_CONTAINER;

    private static final String DEBEZIUM_DOCKER_IMAGE = "quay.io/debezium/connect:2.3.0.Final";

    private static final String DEBEZIUM_HOST = "debezium_e2e";

    private static final int DEBEZIUM_PORT = 8083;

    // ----------------------------------------kafka------------------------------------
    private static final String KAFKA_IMAGE_NAME = "confluentinc/cp-kafka:7.0.9";
    private static final String KAFKA_HOST = "kafka_dbz_e2e";
    private KafkaConsumer<String, String> kafkaConsumer;
    private KafkaContainer KAFKA_CONTAINER;
    private String KAFKA_TOPIC = "test-debezium-sink";

    // -------------------------------------mysql---------------------------------------
    private static final String MYSQL_HOST = "mysql";
    private static MySqlContainer MYSQL_CONTAINER;

    // -----------------------------------------postgres-----------------------------------
    private static final String PG_IMAGE = "postgres:alpine3.16";

    private static final int PG_PORT = 5432;

    private static final String PG_DRIVER_JAR =
            "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar";

    private static PostgreSQLContainer<?> POSTGRESQL_CONTAINER;

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && curl -O "
                                        + PG_DRIVER_JAR);
                Assertions.assertEquals(0, extraCommands.getExitCode());
            };

    private void createDebeziumContainer() {
        DEBEZIUM_CONTAINER =
                new GenericContainer<>(DEBEZIUM_DOCKER_IMAGE)
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("/debezium/register-mysql.json"),
                                "/tmp/seatunnel/plugins/Jdbc/register-mysql.json")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(DEBEZIUM_HOST)
                        .withExposedPorts(DEBEZIUM_PORT)
                        .withEnv("GROUP_ID", "1")
                        .withEnv("CONFIG_STORAGE_TOPIC", "my-connect-configs")
                        .withEnv("OFFSET_STORAGE_TOPIC", "my-connect-offsets")
                        .withEnv("STATUS_STORAGE_TOPIC", "my-connect-status")
                        .withEnv("BOOTSTRAP_SERVERS", KAFKA_HOST + ":9092")
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(DEBEZIUM_DOCKER_IMAGE)))
                        .dependsOn(KAFKA_CONTAINER, MYSQL_CONTAINER);
        DEBEZIUM_CONTAINER.setWaitStrategy(
                (new HttpWaitStrategy())
                        .forPath("/connectors")
                        .forPort(DEBEZIUM_PORT)
                        .withStartupTimeout(Duration.ofSeconds(120)));
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
    }

    private void createMysqlContainer() {
        MYSQL_CONTAINER =
                new MySqlContainer(MySqlVersion.V8_0)
                        .withConfigurationOverride("docker/server-gtids/my.cnf")
                        .withSetupSQL("docker/setup.sql")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MYSQL_HOST)
                        .withDatabaseName("debezium")
                        .withUsername("st_user")
                        .withPassword("seatunnel")
                        .withLogConsumer(new Slf4jLogConsumer(LOG));
    }

    private void createPostgreSQLContainer() {
        POSTGRESQL_CONTAINER =
                new PostgreSQLContainer<>(DockerImageName.parse(PG_IMAGE))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("postgresql_e2e")
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(PG_IMAGE)));
        POSTGRESQL_CONTAINER.setPortBindings(
                Lists.newArrayList(String.format("%s:%s", PG_PORT, PG_PORT)));
    }

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        LOG.info("The first stage: Starting Kafka containers...");
        createKafkaContainer();
        Startables.deepStart(Stream.of(KAFKA_CONTAINER)).join();

        LOG.info("The second stage: Starting Mysql containers...");
        createMysqlContainer();
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();

        LOG.info("The third stage: Starting Debezium Connector containers...");
        createDebeziumContainer();
        Startables.deepStart(Stream.of(DEBEZIUM_CONTAINER)).join();

        LOG.info("The fourth stage: Starting PostgreSQL container...");
        createPostgreSQLContainer();
        Startables.deepStart(Stream.of(POSTGRESQL_CONTAINER)).join();
        Class.forName(POSTGRESQL_CONTAINER.getDriverClassName());

        Awaitility.given()
                .ignoreExceptions()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(3, TimeUnit.MINUTES)
                .untilAsserted(this::initializeSourceTableData);

        given().ignoreExceptions()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(3, TimeUnit.MINUTES)
                .untilAsserted(this::initKafkaConsumer);

        given().ignoreExceptions()
                .await()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(5, TimeUnit.MINUTES)
                .untilAsserted(this::initializeSinkJdbcTable);

        Container.ExecResult extraCommand =
                DEBEZIUM_CONTAINER.execInContainer(
                        "bash",
                        "-c",
                        "cd /tmp/seatunnel/plugins/Jdbc && curl -i -X POST -H \"Accept:application/json\" -H  \"Content-Type:application/json\" http://"
                                + getLinuxLocalIp()
                                + ":8083/connectors/ -d @register-mysql.json");
        Assertions.assertEquals(0, extraCommand.getExitCode());
        // ensure debezium has handled the data
        Thread.sleep(30 * 1000);
        updateSourceTableData();
        Thread.sleep(30 * 1000);
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        MYSQL_CONTAINER.close();
        KAFKA_CONTAINER.close();
        DEBEZIUM_CONTAINER.close();
        POSTGRESQL_CONTAINER.close();
    }

    @TestTemplate
    public void testKafkaSinkDebeziumFormat(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/kafkasource_debezium_to_kafka.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
        ArrayList<String> result = new ArrayList<>();
        kafkaConsumer.subscribe(Lists.newArrayList(KAFKA_TOPIC));
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            ConsumerRecords<String, String> consumerRecords =
                                    kafkaConsumer.poll(Duration.ofMillis(1000));
                            for (ConsumerRecord<String, String> record : consumerRecords) {
                                result.add(record.value());
                            }
                            Assertions.assertEquals(12, result.size());
                        });
    }

    @TestTemplate
    public void testDebeziumFormatKafkaCdcToPgsql(TestContainer container)
            throws IOException, InterruptedException, SQLException {
        Container.ExecResult execResult =
                container.executeJob("/kafkasource_debezium_cdc_to_pgsql.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
        Set<List<Object>> actual = new HashSet<>();
        try (Connection connection =
                DriverManager.getConnection(
                        POSTGRESQL_CONTAINER.getJdbcUrl(),
                        POSTGRESQL_CONTAINER.getUsername(),
                        POSTGRESQL_CONTAINER.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                ResultSet resultSet = statement.executeQuery("select * from sink order by id");
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
        Set<List<Object>> expected =
                Stream.<List<Object>>of(
                                Arrays.asList(101, "scooter", "Small 2-wheel scooter", "4.56"),
                                Arrays.asList(102, "car battery", "12V car battery", "8.1"),
                                Arrays.asList(
                                        103,
                                        "12-pack drill bits",
                                        "12-pack of drill bits with sizes ranging from #40 to #3",
                                        "0.8"),
                                Arrays.asList(104, "hammer", "12oz carpenter's hammer", "0.75"),
                                Arrays.asList(105, "hammer", "14oz carpenter's hammer", "0.875"),
                                Arrays.asList(106, "hammer", "16oz carpenter's hammer", "1"),
                                Arrays.asList(107, "rocks", "box of assorted rocks", "5.3"),
                                Arrays.asList(
                                        108, "jacket", "water resistent black wind breaker", "0.1"))
                        .collect(Collectors.toSet());
        Assertions.assertIterableEquals(expected, actual);

        try (Connection connection =
                DriverManager.getConnection(
                        POSTGRESQL_CONTAINER.getJdbcUrl(),
                        POSTGRESQL_CONTAINER.getUsername(),
                        POSTGRESQL_CONTAINER.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("truncate table sink");
                LOG.info("testDebeziumFormatKafkaCdcToPgsql truncate table sink");
            }
        }
    }

    public void initializeSourceTableData() throws Exception {
        try (Connection connection =
                        DriverManager.getConnection(
                                MYSQL_CONTAINER.getJdbcUrl(),
                                MYSQL_CONTAINER.getUsername(),
                                MYSQL_CONTAINER.getPassword());
                Statement statement = connection.createStatement()) {
            statement.execute("create database if not exists debezium");
            statement.execute(
                    "CREATE TABLE if not exists debezium.products (\n"
                            + "  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,\n"
                            + "  name VARCHAR(255) NOT NULL DEFAULT 'SeaTunnel',\n"
                            + "  description VARCHAR(512),\n"
                            + "  weight VARCHAR(512)\n"
                            + ");");
            statement.execute(
                    "INSERT INTO debezium.products\n"
                            + "VALUES (101,\"scooter\",\"Small 2-wheel scooter\",\"3.14\"),\n"
                            + "       (102,\"car battery\",\"12V car battery\",\"8.1\"),\n"
                            + "       (103,\"12-pack drill bits\",\"12-pack of drill bits with sizes ranging from #40 to #3\","
                            + "\"0.8\"),\n"
                            + "       (104,\"hammer\",\"12oz carpenter's hammer\",\"0.75\"),\n"
                            + "       (105,\"hammer\",\"14oz carpenter's hammer\",\"0.875\"),\n"
                            + "       (106,\"hammer\",\"16oz carpenter's hammer\",\"1.0\"),\n"
                            + "       (107,\"rocks\",\"box of assorted rocks\",\"5.3\"),\n"
                            + "       (108,\"jacket\",\"water resistent black wind breaker\",\"0.1\"),\n"
                            + "       (109,\"spare tire\",\"24 inch spare tire\",\"22.2\")");
        }
    }

    public void updateSourceTableData() throws Exception {
        try (Connection connection =
                        DriverManager.getConnection(
                                MYSQL_CONTAINER.getJdbcUrl(),
                                MYSQL_CONTAINER.getUsername(),
                                MYSQL_CONTAINER.getPassword());
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "UPDATE debezium.products SET weight = '4.56' WHERE name = 'scooter'");
            statement.execute("DELETE FROM debezium.products WHERE name  = \"spare tire\"");
        }
    }

    private void initializeSinkJdbcTable() {
        try (Connection connection =
                        DriverManager.getConnection(
                                POSTGRESQL_CONTAINER.getJdbcUrl(),
                                POSTGRESQL_CONTAINER.getUsername(),
                                POSTGRESQL_CONTAINER.getPassword());
                Statement statement = connection.createStatement()) {
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

    private void initKafkaConsumer() {
        Properties prop = new Properties();
        String bootstrapServers = KAFKA_CONTAINER.getBootstrapServers();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prop.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "seatunnel-debezium-sink-group");
        kafkaConsumer = new KafkaConsumer<>(prop);
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
            log.warn("Failed to get linux local ip, it will return [\"\"] ", ex);
        }
        return ip;
    }
}
