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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

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
import org.apache.seatunnel.e2e.common.util.DependencyJar;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import com.mysql.cj.jdbc.Driver;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * Validates the documented MySQL CDC to Kafka recipe with metadata enrichment and SQL field
 * shaping.
 */
@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason =
                "This recipe validates a MySQL CDC streaming pipeline and is kept on the zeta engine to match the getting-started documentation path.")
public class KafkaRecipeIT extends TestSuiteBase implements TestResource {

    /** Kafka image used by the documented recipe scenario. */
    private static final String KAFKA_IMAGE_NAME = "confluentinc/cp-kafka:7.0.9";
    /** Network alias referenced by the recipe config. */
    private static final String KAFKA_HOST = "kafkaCluster";
    /** MySQL hostname referenced by the recipe config. */
    private static final String MYSQL_HOST = "mysql_cdc_e2e";
    /** Source database consumed by the CDC job. */
    private static final String MYSQL_DATABASE = "shop";
    /** Application user used for DML verification. */
    private static final String MYSQL_USER_NAME = "mysqluser";
    /** Shared password for the MySQL test users. */
    private static final String MYSQL_USER_PASSWORD = "mysqlpw";
    /** Administrative MySQL user used to create the CDC account. */
    private static final String MYSQL_ROOT_USER_NAME = "root";
    /** CDC reader account configured inside the documented job. */
    private static final String MYSQL_CDC_USER_NAME = "st_user_source";
    /** Kafka topic populated by the documented sink config. */
    private static final String TOPIC_NAME = "recipe_mysql_orders";
    /** Kafka service used by the recipe job. */
    private KafkaContainer kafkaContainer;
    /** MySQL service used by the recipe job. */
    private MySqlContainer mysqlContainer;
    /** Helper that materializes the documented source table and seed rows. */
    private UniqueDatabase shopDatabase;

    /**
     * Injects the MySQL JDBC driver into the MySQL-CDC plugin directory so the recipe can run in
     * the same way as the documented Docker validation.
     */
    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container ->
                    DependencyJar.of(Driver.class)
                            .copyTo(container, "/tmp/seatunnel/plugins/MySQL-CDC/lib");

    /** Starts the external systems and prepares the documented source and sink prerequisites. */
    @BeforeEach
    @Override
    public void startUp() throws Exception {
        kafkaContainer =
                new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE_NAME))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(KAFKA_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(KAFKA_IMAGE_NAME)));
        mysqlContainer =
                new MySqlContainer(MySqlVersion.V8_0)
                        .withConfigurationOverride("docker/server-gtids/my.cnf")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MYSQL_HOST)
                        .withDatabaseName(MYSQL_DATABASE)
                        .withUsername(MYSQL_USER_NAME)
                        .withPassword(MYSQL_USER_PASSWORD)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger("mysql-docker-image")));
        Startables.deepStart(Stream.of(kafkaContainer, mysqlContainer)).join();
        createCdcUser();
        shopDatabase = new UniqueDatabase(mysqlContainer, MYSQL_DATABASE);
        shopDatabase.setTemplateName("recipe_shop_orders").createAndInitialize();
        createKafkaTopic(TOPIC_NAME);
    }

    /** Verifies snapshot rows, updates, inserts, payload shaping, and Kafka headers. */
    @TestTemplate
    public void testMysqlCdcToKafkaWithTransforms(TestContainer container)
            throws InterruptedException {
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        AtomicReference<Throwable> jobFailure = new AtomicReference<>();
        CompletableFuture.runAsync(
                () -> {
                    try {
                        container.executeJob(
                                "/kafka/mysqlcdc_to_kafka_with_transforms.conf", jobId);
                    } catch (Exception e) {
                        jobFailure.set(e);
                        throw new RuntimeException(e);
                    }
                });

        Awaitility.await()
                .atMost(120, TimeUnit.SECONDS)
                .pollInterval(Duration.ofSeconds(3))
                .untilAsserted(
                        () -> {
                            assertNoJobFailure(jobFailure);
                            List<ConsumerRecord<String, String>> records =
                                    getKafkaRecordData(TOPIC_NAME);
                            Assertions.assertTrue(records.size() >= 2, "snapshot records missing");
                            ConsumerRecord<String, String> snapshot1001 =
                                    findLatestRecordByOrderId(records, 1001L);
                            ConsumerRecord<String, String> snapshot1002 =
                                    findLatestRecordByOrderId(records, 1002L);
                            Assertions.assertNotNull(snapshot1001);
                            Assertions.assertNotNull(snapshot1002);
                            Map<String, String> headers =
                                    convertHeadersToMap(snapshot1001.headers());
                            Assertions.assertEquals("shop", headers.get("source_database"));
                            Assertions.assertEquals("orders", headers.get("source_table"));
                            Assertions.assertEquals("+I", headers.get("change_type"));

                            ObjectNode payload = parsePayload(snapshot1001.value());
                            Assertions.assertEquals(1001L, payload.get("order_id").asLong());
                            Assertions.assertEquals("CREATED", payload.get("status_name").asText());
                            Assertions.assertEquals(
                                    "shop.orders", payload.get("source_name").asText());
                            Assertions.assertEquals(
                                    "mysql_cdc", payload.get("sync_source").asText());
                            Assertions.assertFalse(payload.has("source_database"));
                            Assertions.assertFalse(payload.has("source_table"));
                            Assertions.assertFalse(payload.has("change_type"));

                            ObjectNode payload1002 = parsePayload(snapshot1002.value());
                            Assertions.assertEquals(1002L, payload1002.get("order_id").asLong());
                            Assertions.assertEquals(
                                    "PAID", payload1002.get("status_name").asText());
                        });

        executeSql(
                "UPDATE shop.orders SET status = 2, amount = 39.99 WHERE id = 1001",
                "INSERT INTO shop.orders (id, order_no, user_id, status, amount) VALUES "
                        + "(1003, 'ORD-1003', 503, 0, 59.99)");

        Awaitility.await()
                .atMost(120, TimeUnit.SECONDS)
                .pollInterval(Duration.ofSeconds(3))
                .untilAsserted(
                        () -> {
                            assertNoJobFailure(jobFailure);
                            List<ConsumerRecord<String, String>> records =
                                    getKafkaRecordData(TOPIC_NAME);
                            Assertions.assertTrue(records.size() >= 4, "cdc records missing");

                            ConsumerRecord<String, String> latest1001 =
                                    findLatestRecordByOrderId(records, 1001L);
                            ConsumerRecord<String, String> latest1003 =
                                    findLatestRecordByOrderId(records, 1003L);
                            Assertions.assertNotNull(latest1001);
                            Assertions.assertNotNull(latest1003);

                            Map<String, String> headers1001 =
                                    convertHeadersToMap(latest1001.headers());
                            Map<String, String> headers1003 =
                                    convertHeadersToMap(latest1003.headers());
                            Assertions.assertEquals("+U", headers1001.get("change_type"));
                            Assertions.assertEquals("+I", headers1003.get("change_type"));

                            ObjectNode payload1001 = parsePayload(latest1001.value());
                            ObjectNode payload1003 = parsePayload(latest1003.value());
                            Assertions.assertEquals(
                                    "SHIPPED", payload1001.get("status_name").asText());
                            Assertions.assertEquals(
                                    "CREATED", payload1003.get("status_name").asText());
                            Assertions.assertEquals(
                                    "mysql_cdc", payload1001.get("sync_source").asText());
                            Assertions.assertEquals(
                                    "mysql_cdc", payload1003.get("sync_source").asText());
                        });
    }

    /** Releases the Kafka and MySQL containers after each validation run. */
    @AfterEach
    @Override
    public void tearDown() {
        if (kafkaContainer != null) {
            kafkaContainer.close();
        }
        if (mysqlContainer != null) {
            mysqlContainer.close();
        }
    }

    /** Fails the awaiting assertion as soon as the async job thread surfaces an exception. */
    private static void assertNoJobFailure(AtomicReference<Throwable> jobFailure) {
        if (jobFailure.get() != null) {
            Assertions.fail(jobFailure.get());
        }
    }

    /** Creates the Kafka topic expected by the documented sink config. */
    private void createKafkaTopic(String topicName) {
        Properties adminProps = new Properties();
        adminProps.put(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            NewTopic topic = new NewTopic(topicName, 1, (short) 1);
            adminClient.createTopics(Arrays.asList(topic)).all().get(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Interrupted while creating Kafka topic " + topicName, e);
        } catch (ExecutionException | java.util.concurrent.TimeoutException e) {
            throw new IllegalStateException("Failed to create Kafka topic " + topicName, e);
        }
    }

    /** Applies source-table updates that should be captured by the CDC job. */
    private void executeSql(String... statements) {
        try (Connection connection =
                        DriverManager.getConnection(
                                mysqlContainer.getJdbcUrl(MYSQL_DATABASE),
                                MYSQL_USER_NAME,
                                MYSQL_USER_PASSWORD);
                Statement statement = connection.createStatement()) {
            for (String sql : statements) {
                statement.execute(sql);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to execute MySQL SQL", e);
        }
    }

    /** Creates the CDC account with the privileges required by the recipe. */
    private void createCdcUser() {
        executeAdminSql(
                "CREATE USER IF NOT EXISTS '"
                        + MYSQL_CDC_USER_NAME
                        + "'@'%' IDENTIFIED BY '"
                        + MYSQL_USER_PASSWORD
                        + "'",
                "GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES ON *.* TO '"
                        + MYSQL_CDC_USER_NAME
                        + "'@'%'");
    }

    /** Executes administrative SQL statements as MySQL root. */
    private void executeAdminSql(String... statements) {
        try (Connection connection =
                        DriverManager.getConnection(
                                mysqlContainer.getJdbcUrl(MYSQL_DATABASE),
                                MYSQL_ROOT_USER_NAME,
                                MYSQL_USER_PASSWORD);
                Statement statement = connection.createStatement()) {
            for (String sql : statements) {
                statement.execute(sql);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to execute MySQL admin SQL", e);
        }
    }

    /** Builds a fresh consumer configuration so every assertion reads from the earliest offset. */
    private Properties kafkaConsumerConfig() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "recipe-kafka-" + UUID.randomUUID());
        props.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                OffsetResetStrategy.EARLIEST.toString().toLowerCase());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }

    /** Reads the current records from the recipe topic in offset order. */
    private List<ConsumerRecord<String, String>> getKafkaRecordData(String topicName) {
        KafkaConsumer<String, String> consumer = null;
        try {
            List<ConsumerRecord<String, String>> data = new ArrayList<>();
            consumer = new KafkaConsumer<>(kafkaConsumerConfig());
            consumer.subscribe(Arrays.asList(topicName));
            Map<TopicPartition, Long> offsets =
                    consumer.endOffsets(Arrays.asList(new TopicPartition(topicName, 0)));
            long endOffset = offsets.entrySet().iterator().next().getValue();
            long lastProcessedOffset = -1L;

            do {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    if (lastProcessedOffset < record.offset()) {
                        data.add(record);
                    }
                    lastProcessedOffset = record.offset();
                }
            } while (lastProcessedOffset < endOffset - 1);
            return data;
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
    }

    /** Returns the latest Kafka record for one business key so CDC updates can be asserted. */
    private static ConsumerRecord<String, String> findLatestRecordByOrderId(
            List<ConsumerRecord<String, String>> records, long orderId) throws IOException {
        ConsumerRecord<String, String> latest = null;
        ObjectMapper objectMapper = new ObjectMapper();
        for (ConsumerRecord<String, String> record : records) {
            ObjectNode payload = objectMapper.readValue(record.value(), ObjectNode.class);
            if (payload.has("order_id") && payload.get("order_id").asLong() == orderId) {
                latest = record;
            }
        }
        return latest;
    }

    /** Converts Kafka headers into a string map for stable assertions. */
    private static Map<String, String> convertHeadersToMap(Headers headers) {
        Map<String, String> map = new HashMap<>();
        for (Header header : headers) {
            map.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
        }
        return map;
    }

    /** Parses a JSON Kafka payload into a mutable object tree for field-level checks. */
    private static ObjectNode parsePayload(String payload) throws IOException {
        return new ObjectMapper().readValue(payload, ObjectNode.class);
    }
}
