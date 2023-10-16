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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.given;

@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK},
        disabledReason = "Spark engine will lose the row kind of record")
public class KafkaFormatIT extends TestSuiteBase implements TestResource {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaFormatIT.class);

    // ---------------------------Ogg Format Parameter---------------------------------------
    private static final String OGG_DATA_PATH = "/ogg/ogg_data.txt";
    private static final String OGG_KAFKA_SOURCE_TOPIC = "test-ogg-source";
    private static final String OGG_KAFKA_SINK_TOPIC = "test-ogg-sink";

    // ---------------------------Canal Format Parameter---------------------------------------

    private static final String CANAL_KAFKA_SINK_TOPIC = "test-canal-sink";
    private static final String CANAL_MYSQL_DATABASE = "canal";

    // ---------------------------Compatible Format Parameter---------------------------------------
    private static final String COMPATIBLE_DATA_PATH = "/compatible/compatible_data.txt";
    private static final String COMPATIBLE_KAFKA_SINK_TOPIC = "jdbc_source_record";

    // Used to map local data paths to kafa topics that need to be written to kafka
    private static LinkedHashMap<String, String> LOCAL_DATA_TO_KAFKA_MAPPING;

    static {
        LOCAL_DATA_TO_KAFKA_MAPPING =
                new LinkedHashMap<String, String>() {
                    {
                        put(OGG_DATA_PATH, OGG_KAFKA_SOURCE_TOPIC);
                        put(COMPATIBLE_DATA_PATH, COMPATIBLE_KAFKA_SINK_TOPIC);
                    }
                };
    }

    // ---------------------------Canal Container---------------------------------------
    private static GenericContainer<?> CANAL_CONTAINER;

    private static final String CANAL_DOCKER_IMAGE = "chinayin/canal:1.1.6";

    private static final String CANAL_HOST = "canal_e2e";

    // ---------------------------Debezium Container---------------------------------------

    private static GenericContainer<?> DEBEZIUM_CONTAINER;

    private static final String DEBEZIUM_DOCKER_IMAGE = "quay.io/debezium/connect:2.3.0.Final";

    private static final String DEBEZIUM_HOST = "debezium_e2e";

    private static final int DEBEZIUM_PORT = 8083;
    private static final String DEBEZIUM_KAFKA_TOPIC = "test-debezium-sink";
    private static final String DEBEZIUM_MYSQL_DATABASE = "debezium";

    // ---------------------------Kafka Container---------------------------------------
    private static final String KAFKA_IMAGE_NAME = "confluentinc/cp-kafka:7.0.9";

    private static final String KAFKA_HOST = "kafka_e2e";

    private static KafkaContainer KAFKA_CONTAINER;

    private KafkaConsumer<String, String> kafkaConsumer;

    // ---------------------------Mysql Container---------------------------------------
    private static final String MYSQL_HOST = "mysql_e2e";
    private static final String MYSQL_USER_NAME = "st_user";
    private static final String MYSQL_PASSWORD = "seatunnel";
    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer();

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(
                    MYSQL_CONTAINER,
                    CANAL_MYSQL_DATABASE,
                    "mysqluser",
                    "mysqlpw",
                    "initialize_format");

    // --------------------------- Postgres Container-------------------------------------
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
                .withDatabaseName(CANAL_MYSQL_DATABASE)
                .withDatabaseName(DEBEZIUM_MYSQL_DATABASE)
                .withUsername(MYSQL_USER_NAME)
                .withPassword(MYSQL_PASSWORD)
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
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(CANAL_DOCKER_IMAGE)));
    }

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
    public void startUp() throws ClassNotFoundException, InterruptedException, IOException {

        LOG.info("The first stage: Starting Kafka containers...");
        createKafkaContainer();
        Startables.deepStart(Stream.of(KAFKA_CONTAINER)).join();
        LOG.info("Kafka Containers are started");

        LOG.info("The second stage: Starting Mysql containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        LOG.info("Mysql Containers are started");

        LOG.info("The third stage: Starting Debezium Connector containers...");
        createDebeziumContainer();
        Startables.deepStart(Stream.of(DEBEZIUM_CONTAINER)).join();
        LOG.info("Debezium Containers are started");

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

        LOG.info("start init Mysql DDl...");
        inventoryDatabase.createAndInitialize();
        LOG.info("end init Mysql DDl...");

        // local file ogg data send kafka
        given().ignoreExceptions()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(3, TimeUnit.MINUTES)
                .untilAsserted(this::initOggDataToKafka);

        // debezium configuration information
        Container.ExecResult extraCommand =
                DEBEZIUM_CONTAINER.execInContainer(
                        "bash",
                        "-c",
                        "cd /tmp/seatunnel/plugins/Jdbc && curl -i -X POST -H \"Accept:application/json\" -H  \"Content-Type:application/json\" http://"
                                + getLinuxLocalIp()
                                + ":8083/connectors/ -d @register-mysql.json");
        Assertions.assertEquals(0, extraCommand.getExitCode());
        // ensure debezium has handled the data
        Thread.sleep(40 * 1000);
        Awaitility.given()
                .ignoreExceptions()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(3, TimeUnit.MINUTES)
                .untilAsserted(this::updateDebeziumSourceTableData);
    }

    @TestTemplate
    public void testFormatCheck(TestContainer container) throws IOException, InterruptedException {
        LOG.info("====================== Check Canal======================");
        Container.ExecResult execCanalResultKafka =
                container.executeJob("/canalFormatIT/kafka_source_canal_to_kafka.conf");
        Assertions.assertEquals(
                0, execCanalResultKafka.getExitCode(), execCanalResultKafka.getStderr());
        Container.ExecResult execCanalResultToPgSql =
                container.executeJob("/canalFormatIT/kafka_source_canal_cdc_to_pgsql.conf");
        Assertions.assertEquals(
                0, execCanalResultToPgSql.getExitCode(), execCanalResultToPgSql.getStderr());
        // Check Canal
        checkCanalFormat();

        LOG.info("====================== Check Ogg======================");
        Container.ExecResult execOggResultKafka =
                container.executeJob("/oggFormatIT/kafka_source_ogg_to_kafka.conf");
        Assertions.assertEquals(
                0, execOggResultKafka.getExitCode(), execOggResultKafka.getStderr());
        // check ogg kafka to postgresql
        Container.ExecResult execOggResultToPgSql =
                container.executeJob("/oggFormatIT/kafka_source_ogg_to_pgsql.conf");
        Assertions.assertEquals(
                0, execOggResultToPgSql.getExitCode(), execOggResultToPgSql.getStderr());

        // Check Ogg
        checkOggFormat();

        LOG.info("======================  Check debezium ====================== ");
        Container.ExecResult execDebeziumResultKafka =
                container.executeJob("/debeziumFormatIT/kafkasource_debezium_to_kafka.conf");
        Assertions.assertEquals(
                0, execDebeziumResultKafka.getExitCode(), execDebeziumResultKafka.getStderr());

        Container.ExecResult execDebeziumResultToPgSql =
                container.executeJob("/debeziumFormatIT/kafkasource_debezium_cdc_to_pgsql.conf");
        Assertions.assertEquals(
                0, execDebeziumResultToPgSql.getExitCode(), execDebeziumResultToPgSql.getStderr());
        // Check debezium
        checkDebeziumFormat();

        LOG.info("======================  Check Compatible ====================== ");
        Container.ExecResult execCompatibleResultToPgSql =
                container.executeJob("/compatibleFormatIT/kafkasource_jdbc_record_to_pgsql.conf");
        Assertions.assertEquals(
                0,
                execCompatibleResultToPgSql.getExitCode(),
                execCompatibleResultToPgSql.getStderr());

        // Check Compatible
        checkCompatibleFormat();
    }

    public void checkCanalFormat() {
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
        topics.add(CANAL_KAFKA_SINK_TOPIC);
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

        LOG.info("==================== start kafka canal format to pg check ====================");

        Set<List<Object>> postgreSinkTableList = getPostgreSinkTableList();

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
                                Arrays.asList(106, "hammer", "16oz carpenter's hammer", "1.0"),
                                Arrays.asList(107, "rocks", "box of assorted rocks", "7.88"),
                                Arrays.asList(
                                        108, "jacket", "water resistent black wind breaker", "0.1"))
                        .collect(Collectors.toSet());
        Assertions.assertIterableEquals(expected, postgreSinkTableList);
    }

    public void checkOggFormat() {
        List<String> kafkaExpectedResult =
                Arrays.asList(
                        "{\"data\":{\"id\":101,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":\"3.140000104904175\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":102,\"name\":\"car battery\",\"description\":\"12V car battery\",\"weight\":\"8.100000381469727\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":103,\"name\":\"12-pack drill bits\",\"description\":\"12-pack of drill bits with sizes ranging from #40 to #3\",\"weight\":\"0.800000011920929\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":104,\"name\":\"hammer\",\"description\":\"12oz carpenter's hammer\",\"weight\":\"0.75\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":105,\"name\":\"hammer\",\"description\":\"14oz carpenter's hammer\",\"weight\":\"0.875\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":106,\"name\":\"hammer\",\"description\":\"16oz carpenter's hammer\",\"weight\":\"1\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":107,\"name\":\"rocks\",\"description\":\"box of assorted rocks\",\"weight\":\"5.300000190734863\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":108,\"name\":\"jacket\",\"description\":\"water resistent black wind breaker\",\"weight\":\"0.10000000149011612\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":109,\"name\":\"spare tire\",\"description\":\"24 inch spare tire\",\"weight\":\"22.200000762939453\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":106,\"name\":\"hammer\",\"description\":\"16oz carpenter's hammer\",\"weight\":\"1\"},\"type\":\"DELETE\"}",
                        "{\"data\":{\"id\":106,\"name\":\"hammer\",\"description\":\"18oz carpenter hammer\",\"weight\":\"1\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":107,\"name\":\"rocks\",\"description\":\"box of assorted rocks\",\"weight\":\"5.300000190734863\"},\"type\":\"DELETE\"}",
                        "{\"data\":{\"id\":107,\"name\":\"rocks\",\"description\":\"box of assorted rocks\",\"weight\":\"5.099999904632568\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":110,\"name\":\"jacket\",\"description\":\"water resistent white wind breaker\",\"weight\":\"0.20000000298023224\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":111,\"name\":\"scooter\",\"description\":\"Big 2-wheel scooter \",\"weight\":\"5.179999828338623\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":110,\"name\":\"jacket\",\"description\":\"water resistent white wind breaker\",\"weight\":\"0.20000000298023224\"},\"type\":\"DELETE\"}",
                        "{\"data\":{\"id\":110,\"name\":\"jacket\",\"description\":\"new water resistent white wind breaker\",\"weight\":\"0.5\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":111,\"name\":\"scooter\",\"description\":\"Big 2-wheel scooter \",\"weight\":\"5.179999828338623\"},\"type\":\"DELETE\"}",
                        "{\"data\":{\"id\":111,\"name\":\"scooter\",\"description\":\"Big 2-wheel scooter \",\"weight\":\"5.170000076293945\"},\"type\":\"INSERT\"}",
                        "{\"data\":{\"id\":111,\"name\":\"scooter\",\"description\":\"Big 2-wheel scooter \",\"weight\":\"5.170000076293945\"},\"type\":\"DELETE\"}");

        ArrayList<String> checkKafkaConsumerResult = new ArrayList<>();
        ArrayList<String> topics = new ArrayList<>();
        topics.add(OGG_KAFKA_SINK_TOPIC);
        kafkaConsumer.subscribe(topics);
        // check ogg kafka to kafka
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            ConsumerRecords<String, String> consumerRecords =
                                    kafkaConsumer.poll(Duration.ofMillis(1000));
                            for (ConsumerRecord<String, String> record : consumerRecords) {
                                checkKafkaConsumerResult.add(record.value());
                            }
                            Assertions.assertEquals(kafkaExpectedResult, checkKafkaConsumerResult);
                        });

        LOG.info("==================== start kafka ogg format to pg check ====================");

        Set<List<Object>> postgresqlEexpectedResult = getPostgreSinkTableList();
        Set<List<Object>> checkArraysResult =
                Stream.<List<Object>>of(
                                Arrays.asList(
                                        101,
                                        "scooter",
                                        "Small 2-wheel scooter",
                                        "3.140000104904175"),
                                Arrays.asList(
                                        102, "car battery", "12V car battery", "8.100000381469727"),
                                Arrays.asList(
                                        103,
                                        "12-pack drill bits",
                                        "12-pack of drill bits with sizes ranging from #40 to #3",
                                        "0.800000011920929"),
                                Arrays.asList(104, "hammer", "12oz carpenter's hammer", "0.75"),
                                Arrays.asList(105, "hammer", "14oz carpenter's hammer", "0.875"),
                                Arrays.asList(106, "hammer", "18oz carpenter hammer", "1"),
                                Arrays.asList(
                                        107, "rocks", "box of assorted rocks", "5.099999904632568"),
                                Arrays.asList(
                                        108,
                                        "jacket",
                                        "water resistent black wind breaker",
                                        "0.10000000149011612"),
                                Arrays.asList(
                                        109,
                                        "spare tire",
                                        "24 inch spare tire",
                                        "22.200000762939453"),
                                Arrays.asList(
                                        110,
                                        "jacket",
                                        "new water resistent white wind breaker",
                                        "0.5"))
                        .collect(Collectors.toSet());
        Assertions.assertIterableEquals(postgresqlEexpectedResult, checkArraysResult);
    }

    private void checkDebeziumFormat() {
        ArrayList<String> result = new ArrayList<>();
        kafkaConsumer.subscribe(Lists.newArrayList(DEBEZIUM_KAFKA_TOPIC));
        Awaitility.await()
                .atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            ConsumerRecords<String, String> consumerRecords =
                                    kafkaConsumer.poll(Duration.ofMillis(1000));
                            for (ConsumerRecord<String, String> record : consumerRecords) {
                                result.add(record.value());
                            }
                            Assertions.assertEquals(12, result.size());
                        });
        LOG.info(
                "==================== start kafka debezium format to pg check ====================");
        Set<List<Object>> actual = getPostgreSinkTableList();
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
    }

    private void checkCompatibleFormat() {
        LOG.info(
                "==================== start kafka Compatible format to pg check ====================");
        Set<List<Object>> actual = getPostgreSinkTableList();
        Set<List<Object>> expected =
                Stream.<List<Object>>of(
                                Arrays.asList(15, "test", "test", "20"),
                                Arrays.asList(16, "test-001", "test", "30"),
                                Arrays.asList(18, "sdc", "sdc", "sdc"))
                        .collect(Collectors.toSet());
        Assertions.assertIterableEquals(expected, actual);
    }

    // Initialize the kafka Consumer
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

    // Example Initialize the pg sink table
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

    // Initialize ogg data to kafka
    private void initOggDataToKafka() {
        String bootstrapServers = KAFKA_CONTAINER.getBootstrapServers();
        Properties props = new Properties();

        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        for (String localPath : LOCAL_DATA_TO_KAFKA_MAPPING.keySet()) {
            String kafkaTopic = LOCAL_DATA_TO_KAFKA_MAPPING.get(localPath);
            InputStream inputStream = KafkaFormatIT.class.getResourceAsStream(localPath);
            if (inputStream != null) {
                try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        ProducerRecord<String, String> record =
                                new ProducerRecord<>(kafkaTopic, null, line);
                        producer.send(record).get();
                    }
                } catch (IOException | InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }
        producer.close();
    }

    // Get result data
    public Set<List<Object>> getPostgreSinkTableList() {
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
            // truncate e2e sink table
            try (Statement statement = connection.createStatement()) {
                statement.execute("truncate table sink");
                LOG.info("truncate table sink");
                if (statement != null) {
                    statement.close();
                }
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return actual;
    }
    // Modify debezium data
    public void updateDebeziumSourceTableData() throws Exception {
        MYSQL_CONTAINER.setDatabaseName(DEBEZIUM_MYSQL_DATABASE);
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
            LOG.warn("Failed to get linux local ip, it will return [\"\"] ", ex);
        }
        return ip;
    }

    @Override
    public void tearDown() {
        if (MYSQL_CONTAINER != null) {
            MYSQL_CONTAINER.close();
        }
        if (KAFKA_CONTAINER != null) {
            KAFKA_CONTAINER.close();
        }
        if (CANAL_CONTAINER != null) {
            CANAL_CONTAINER.close();
        }
        if (DEBEZIUM_CONTAINER != null) {
            DEBEZIUM_CONTAINER.close();
        }
    }
}
