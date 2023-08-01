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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

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
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.given;

@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK},
        disabledReason = "Spark engine will lose the row kind of record")
public class CanalToKafkaIT extends TestSuiteBase implements TestResource {

    private static final Logger LOG = LoggerFactory.getLogger(CanalToKafkaIT.class);

    private static GenericContainer<?> CANAL_CONTAINER;

    private static final String CANAL_DOCKER_IMAGE = "chinayin/canal:1.1.6";

    private static final String CANAL_HOST = "canal_e2e";

    // ----------------------------------------------------------------------------
    // kafka
    private static final String KAFKA_IMAGE_NAME = "confluentinc/cp-kafka:7.0.9";

    private static final String KAFKA_TOPIC = "test-canal-sink";

    private static final String KAFKA_HOST = "kafka_e2e";

    private static KafkaContainer KAFKA_CONTAINER;

    private KafkaConsumer<String, String> kafkaConsumer;

    // ----------------------------------------------------------------------------
    // mysql
    private static final String MYSQL_HOST = "mysql_e2e";

    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer();

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "canal", "mysqluser", "mysqlpw");

    // ----------------------------------------------------------------------------
    // postgres
    private static final String PG_IMAGE = "postgres:alpine3.16";

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

    private static MySqlContainer createMySqlContainer() {
        return new MySqlContainer(MySqlVersion.V8_0)
                .withConfigurationOverride("docker/server-gtids/my.cnf")
                .withSetupSQL("docker/setup.sql")
                .withNetwork(NETWORK)
                .withNetworkAliases(MYSQL_HOST)
                .withDatabaseName("canal")
                .withUsername("st_user")
                .withPassword("seatunnel")
                .withLogConsumer(new Slf4jLogConsumer(LOG));
    }

    private void createCanalContainer() {
        CANAL_CONTAINER =
                new GenericContainer<>(CANAL_DOCKER_IMAGE)
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("canal/canal.properties"),
                                "/app/server/conf/canal.properties")
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("canal/instance.properties"),
                                "/app/server/conf/example/instance.properties")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(CANAL_HOST)
                        //                        .withCommand()
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(CANAL_DOCKER_IMAGE)));
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

    private void createPostgreSQLContainer() {
        POSTGRESQL_CONTAINER =
                new PostgreSQLContainer<>(DockerImageName.parse(PG_IMAGE))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("postgresql")
                        .withExposedPorts(5432)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(PG_IMAGE)));
    }

    @BeforeAll
    @Override
    public void startUp() throws ClassNotFoundException, InterruptedException {

        LOG.info("The first stage: Starting Kafka containers...");
        createKafkaContainer();
        Startables.deepStart(Stream.of(KAFKA_CONTAINER)).join();
        LOG.info("Kafka Containers are started");

        LOG.info("The second stage: Starting Mysql containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        LOG.info("Mysql Containers are started");

        LOG.info("The third stage: Starting Canal containers...");
        createCanalContainer();
        Startables.deepStart(Stream.of(CANAL_CONTAINER)).join();
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

        given().ignoreExceptions()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(180, TimeUnit.SECONDS)
                .untilAsserted(this::initKafkaConsumer);
        inventoryDatabase.createAndInitialize();
        // ensure canal has handled the data
        Thread.sleep(10 * 1000);
    }

    @TestTemplate
    public void testKafkaSinkCanalFormat(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/canalFormatIT/kafka_source_canal_to_kafka.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
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
                        "{\"data\":{\"id\":109,\"name\":\"spare tire\",\"description\":\"24 inch spare tire\",\"weight\":\"22.2\"},\"type\":\"DELETE\"}");

        ArrayList<String> result = new ArrayList<>();
        ArrayList<String> topics = new ArrayList<>();
        topics.add(KAFKA_TOPIC);
        kafkaConsumer.subscribe(topics);
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            ConsumerRecords<String, String> consumerRecords =
                                    kafkaConsumer.poll(Duration.ofMillis(1000));
                            for (ConsumerRecord<String, String> record : consumerRecords) {
                                result.add(record.value());
                            }
                            Assertions.assertEquals(expectedResult, result);
                        });
    }

    @TestTemplate
    public void testCanalFormatKafkaCdcToPgsql(TestContainer container)
            throws IOException, InterruptedException, SQLException {
        Container.ExecResult execResult =
                container.executeJob("/canalFormatIT/kafka_source_canal_cdc_to_pgsql.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
        List<Object> actual = new ArrayList<>();
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
        List<Object> expected =
                Lists.newArrayList(
                        Arrays.asList(101, "scooter", "Small 2-wheel scooter", "4.56"),
                        Arrays.asList(102, "car battery", "12V car battery", "8.1"),
                        Arrays.asList(
                                103,
                                "12-pack drill bits",
                                "12-pack of drill bits with sizes ranging from #40 to #3",
                                "0.8"),
                        Arrays.asList(104, "hammer", "12oz carpenter's hammer", "0.75"),
                        Arrays.asList(105, "hammer", "14oz carpenter's hammer", "0.875"),
                        Arrays.asList(106, "hammer", "16oz carpenter's hammer", "1.0"),
                        Arrays.asList(107, "rocks", "box of assorted rocks", "7.88"),
                        Arrays.asList(108, "jacket", "water resistent black wind breaker", "0.1"));
        Assertions.assertIterableEquals(expected, actual);

        try (Connection connection =
                DriverManager.getConnection(
                        POSTGRESQL_CONTAINER.getJdbcUrl(),
                        POSTGRESQL_CONTAINER.getUsername(),
                        POSTGRESQL_CONTAINER.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("truncate table sink");
                LOG.info("testCanalFormatKafkaCdcToPgsql truncate table sink");
            }
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
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "CONF");
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        kafkaConsumer = new KafkaConsumer<>(prop);
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

    @Override
    public void tearDown() {
        MYSQL_CONTAINER.close();
        KAFKA_CONTAINER.close();
        CANAL_CONTAINER.close();
    }
}
