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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.config.PostgresSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.PostgresDialect;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.given;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK},
        disabledReason = "Currently SPARK do not support cdc")
public class PostgresCDCIT extends TestSuiteBase implements TestResource {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresCDCIT.class);
    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");
    private static final String USERNAME = "postgres";
    private static final String PASSWORD = "postgres";

    private static final String POSTGRES_HOST = "postgres_cdc_e2e";

    private static final String POSTGRESQL_DATABASE = "postgres_cdc";
    private static final String POSTGRESQL_SCHEMA = "inventory";

    private static final String SOURCE_TABLE_1 = "postgres_cdc_table_1";
    private static final String SOURCE_TABLE_2 = "postgres_cdc_table_2";
    private static final String SOURCE_TABLE_3 = "postgres_cdc_table_3";
    private static final String SOURCE_TABLE_4 = "postgres_cdc_table_4";
    private static final String SOURCE_TABLE_5 = "postgres_cdc_table_5";
    private static final String SINK_TABLE_1 = "sink_postgres_cdc_table_1";
    private static final String SINK_TABLE_2 = "sink_postgres_cdc_table_2";
    private static final String SINK_TABLE_3 = "sink_postgres_cdc_table_3";
    private static final String SINK_TABLE_4 = "sink_postgres_cdc_table_4";
    private static final String SINK_TABLE_5 = "sink_postgres_cdc_table_5";

    private static final String SOURCE_TABLE_NO_PRIMARY_KEY = "full_types_no_primary_key";

    private static final String SOURCE_TABLE_NO_PRIMARY_KEY_DEBEZIUM =
            "full_types_no_primary_key_with_debezium";

    private static final String SOURCE_SQL_TEMPLATE = "select * from %s.%s order by id";
    private static final String GENERATED_SLOT_PREFIX = "seatunnel_";
    /**
     * Debezium JSON change events can lag under CI load, so snapshot and DML record waits use the
     * same timeout budget.
     */
    private static final long DEBEZIUM_JSON_RECORD_WAIT_TIMEOUT_SECONDS = 180L;

    // kafka container
    private static final String KAFKA_IMAGE_NAME = "confluentinc/cp-kafka:7.0.9";

    private static final String KAFKA_HOST = "kafka_e2e";

    private static KafkaContainer KAFKA_CONTAINER;

    private static KafkaConsumer<String, String> kafkaConsumer;

    private static final String DEBEZIUM_JSON_TOPIC = "debezium_json_topic";
    // use newer version of postgresql image to support pgoutput plugin
    // when testing postgres 13, only 13-alpine supports both amd64 and arm64
    protected static final DockerImageName PG_IMAGE =
            DockerImageName.parse("debezium/postgres:11").asCompatibleSubstituteFor("postgres");

    public static final PostgreSQLContainer<?> POSTGRES_CONTAINER =
            new PostgreSQLContainer<>(PG_IMAGE)
                    .withNetwork(NETWORK)
                    .withNetworkAliases(POSTGRES_HOST)
                    .withUsername(USERNAME)
                    .withPassword(PASSWORD)
                    .withDatabaseName(POSTGRESQL_DATABASE)
                    .withLogConsumer(new Slf4jLogConsumer(LOG))
                    .withCommand(
                            "postgres",
                            "-c",
                            // default
                            "fsync=off",
                            "-c",
                            "max_replication_slots=20");

    private void createKafkaContainer() {
        KAFKA_CONTAINER =
                new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE_NAME))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(KAFKA_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(KAFKA_IMAGE_NAME)));
    }

    private String driverUrl() {
        return "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.5.1/postgresql-42.5.1.jar";
    }

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Postgres-CDC/lib && cd /tmp/seatunnel/plugins/Postgres-CDC/lib && wget "
                                        + driverUrl());
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    @BeforeAll
    @Override
    public void startUp() {
        log.info("The second stage: Starting Postgres containers...");
        POSTGRES_CONTAINER.setPortBindings(
                Lists.newArrayList(
                        String.format(
                                "%s:%s",
                                PostgreSQLContainer.POSTGRESQL_PORT,
                                PostgreSQLContainer.POSTGRESQL_PORT)));
        Startables.deepStart(Stream.of(POSTGRES_CONTAINER)).join();

        log.info("Postgres Containers are started");
        initializePostgresTable(POSTGRES_CONTAINER, "inventory");

        LOG.info("The third stage: Starting Kafka containers...");
        createKafkaContainer();
        Startables.deepStart(Stream.of(KAFKA_CONTAINER)).join();
        LOG.info("Kafka Containers are started");

        given().ignoreExceptions()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(180, TimeUnit.SECONDS)
                .untilAsserted(this::createTopic);
        LOG.info("Kafka create topic: " + DEBEZIUM_JSON_TOPIC);
    }

    // Initialize the kafka Topic
    private void createTopic() {
        Properties props = new Properties();
        props.put(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());

        try (AdminClient adminClient = AdminClient.create(props)) {
            // Create a new topic
            NewTopic newTopic = new NewTopic(DEBEZIUM_JSON_TOPIC, 1, (short) 1);

            // Create the topic (async operation)
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();

            System.out.println("Topic " + DEBEZIUM_JSON_TOPIC + " created successfully");
        } catch (InterruptedException | ExecutionException e) {
            System.err.println("Error creating topic: " + e.getMessage());
        }
    }
    // Initialize the kafka Consumer

    private Properties kafkaConsumerConfig() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
        props.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                OffsetResetStrategy.EARLIEST.toString().toLowerCase());
        props.put(
                ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                IsolationLevel.READ_COMMITTED.name().toLowerCase());
        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        return props;
    }

    /**
     * Replication slots are shared inside the reused Postgres test container, so each CDC job needs
     * an isolated slot to avoid cross-test collisions when streaming jobs overlap.
     */
    private String createSlotName() {
        return GENERATED_SLOT_PREFIX + Long.toHexString(JobIdGenerator.newJobId());
    }

    private String toSlotVariable(String slotName) {
        return "slot_name=" + slotName;
    }

    @BeforeEach
    public void beforeEach() {
        cleanupGeneratedReplicationSlots();
    }

    @AfterEach
    public void afterEach() {
        cleanupGeneratedReplicationSlots();
    }

    private List<String> getKafkaData() {
        long endOffset;
        long lastProcessedOffset = -1L;
        List<String> data = new ArrayList<>();
        kafkaConsumer.subscribe(Collections.singletonList(PostgresCDCIT.DEBEZIUM_JSON_TOPIC));
        Map<TopicPartition, Long> offsets =
                kafkaConsumer.endOffsets(
                        Collections.singletonList(
                                new TopicPartition(PostgresCDCIT.DEBEZIUM_JSON_TOPIC, 0)));
        endOffset = offsets.entrySet().iterator().next().getValue();
        log.info("End offset: {}", endOffset);
        do {
            ConsumerRecords<String, String> consumerRecords =
                    kafkaConsumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : consumerRecords) {
                data.add(record.value());
                lastProcessedOffset = record.offset();
            }
            log.info("Data size: {}", data.size());
        } while (lastProcessedOffset < endOffset - 1);

        return data;
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason = "Currently Only support Zeta engine")
    public void testPostgresCdcWithDebeziumJsonFormat(TestContainer container) {
        String slotName = createSlotName();
        String slotVariable = toSlotVariable(slotName);
        try {

            log.info(
                    "Table {} has {} rows.",
                    SOURCE_TABLE_NO_PRIMARY_KEY_DEBEZIUM,
                    query(getQuerySQL(POSTGRESQL_SCHEMA, SOURCE_TABLE_NO_PRIMARY_KEY_DEBEZIUM)));

            Properties props = kafkaConsumerConfig();
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "group-" + slotName);
            kafkaConsumer = new KafkaConsumer<>(props);

            CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            container.executeJob(
                                    "/postgrescdc_to_postgres_with_debezium_to_kafka.conf",
                                    Collections.singletonList(slotVariable));
                        } catch (Exception e) {
                            log.error("Commit task exception :" + e.getMessage());
                            throw new RuntimeException(e);
                        }
                        return null;
                    });
            AtomicReference<Integer> dataSize = new AtomicReference<>(0);

            await().atMost(DEBEZIUM_JSON_RECORD_WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                dataSize.updateAndGet(v -> v + getKafkaData().size());
                                Assertions.assertEquals(1, dataSize.get());
                            });
            // The snapshot row can reach Kafka before the WAL stream is fully attached.
            // Wait for the replication slot to become active so the following DML is emitted as
            // incremental change events instead of being skipped during the snapshot handoff.
            waitForReplicationSlotActive(slotName);
            // Keep each row-level mutation isolated so the exactly-once Kafka sink does not
            // collapse same-key changes into only the final visible state under CI pressure.
            insertSourceTableRow(POSTGRESQL_SCHEMA, SOURCE_TABLE_NO_PRIMARY_KEY_DEBEZIUM, 2);
            awaitKafkaRecordCount(dataSize, 2);
            insertSourceTableRow(POSTGRESQL_SCHEMA, SOURCE_TABLE_NO_PRIMARY_KEY_DEBEZIUM, 3);
            awaitKafkaRecordCount(dataSize, 3);
            deleteSourceTableRow(POSTGRESQL_SCHEMA, SOURCE_TABLE_NO_PRIMARY_KEY_DEBEZIUM, 2);
            awaitKafkaRecordCount(dataSize, 4);
            updateSourceTableBigField(
                    POSTGRESQL_SCHEMA, SOURCE_TABLE_NO_PRIMARY_KEY_DEBEZIUM, 3, 10000);
            awaitKafkaRecordCount(dataSize, 5);
        } finally {
            clearTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_NO_PRIMARY_KEY_DEBEZIUM);
            if (kafkaConsumer != null) {
                kafkaConsumer.close();
            }
        }
    }

    @TestTemplate
    public void testMPostgresCdcCheckDataE2e(TestContainer container) {
        String slotVariable = toSlotVariable(createSlotName());

        try {
            CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            container.executeJob(
                                    "/postgrescdc_to_postgres.conf",
                                    Collections.singletonList(slotVariable));
                        } catch (Exception e) {
                            log.error("Commit task exception :" + e.getMessage());
                            throw new RuntimeException(e);
                        }
                        return null;
                    });
            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertIterableEquals(
                                        query(getQuerySQL(POSTGRESQL_SCHEMA, SOURCE_TABLE_1)),
                                        query(getQuerySQL(POSTGRESQL_SCHEMA, SINK_TABLE_1)));
                            });

            // insert update delete
            upsertDeleteSourceTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_1);

            // stream stage
            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertIterableEquals(
                                        query(getQuerySQL(POSTGRESQL_SCHEMA, SOURCE_TABLE_1)),
                                        query(getQuerySQL(POSTGRESQL_SCHEMA, SINK_TABLE_1)));
                            });
        } finally {
            // Clear related content to ensure that multiple operations are not affected
            clearTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_1);
            clearTable(POSTGRESQL_SCHEMA, SINK_TABLE_1);
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason =
                    "Heartbeat action query is currently only supported by the zeta engine.")
    public void testMPostgresCdcCheckDataE2eWithHeartbeat(TestContainer container) {
        String slotVariable = toSlotVariable(createSlotName());
        executeSql(
                "CREATE TABLE IF NOT EXISTS "
                        + POSTGRESQL_SCHEMA
                        + ".heartbeat ("
                        + "  ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
                        + ");");
        clearTable(POSTGRESQL_SCHEMA, "heartbeat");

        try {
            CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            container.executeJob(
                                    "/postgrescdc_to_postgres_with_heartbeat.conf",
                                    Collections.singletonList(slotVariable));
                        } catch (Exception e) {
                            log.error("Commit task exception :" + e.getMessage());
                            throw new RuntimeException(e);
                        }
                        return null;
                    });
            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertIterableEquals(
                                        query(getQuerySQL(POSTGRESQL_SCHEMA, SOURCE_TABLE_1)),
                                        query(getQuerySQL(POSTGRESQL_SCHEMA, SINK_TABLE_1)));
                            });

            // insert update delete
            upsertDeleteSourceTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_1);

            // stream stage
            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertIterableEquals(
                                        query(getQuerySQL(POSTGRESQL_SCHEMA, SOURCE_TABLE_1)),
                                        query(getQuerySQL(POSTGRESQL_SCHEMA, SINK_TABLE_1)));
                            });

            await().atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                List<List<Object>> query =
                                        query("SELECT * FROM " + POSTGRESQL_SCHEMA + ".heartbeat");
                                Assertions.assertFalse(query.isEmpty());
                            });
        } finally {
            // Clear related content to ensure that multiple operations are not affected
            clearTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_1);
            clearTable(POSTGRESQL_SCHEMA, SINK_TABLE_1);
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason =
                    "This case requires obtaining the task health status and manually canceling the canceled task, which is currently only supported by the zeta engine.")
    public void testMPostgresCdcMetadataTrans(TestContainer container) throws InterruptedException {

        Long jobId = JobIdGenerator.newJobId();
        String slotVariable = toSlotVariable(createSlotName());
        CompletableFuture.runAsync(
                () -> {
                    try {
                        container.executeJob(
                                "/postgrescdc_to_postgres.conf",
                                String.valueOf(jobId),
                                slotVariable);
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });
        TimeUnit.SECONDS.sleep(10);
        // insert update delete
        upsertDeleteSourceTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_1);

        TimeUnit.SECONDS.sleep(20);
        await().atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            String jobStatus = container.getJobStatus(String.valueOf(jobId));
                            Assertions.assertEquals("RUNNING", jobStatus);
                        });

        try {
            Container.ExecResult cancelJobResult = container.cancelJob(String.valueOf(jobId));
            Assertions.assertEquals(0, cancelJobResult.getExitCode(), cancelJobResult.getStderr());
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // Clear related content to ensure that multiple operations are not affected
            clearTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_1);
            clearTable(POSTGRESQL_SCHEMA, SINK_TABLE_1);
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK},
            disabledReason = "Currently SPARK do not support cdc")
    public void testPostgresCdcMultiTableE2e(TestContainer container) {
        String slotVariable = toSlotVariable(createSlotName());

        try {
            CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            container.executeJob(
                                    "/pgcdc_to_pg_with_multi_table_mode_two_table.conf",
                                    Collections.singletonList(slotVariable));
                        } catch (Exception e) {
                            log.error("Commit task exception :" + e.getMessage());
                            throw new RuntimeException(e);
                        }
                        return null;
                    });

            // stream stage
            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertAll(
                                            () ->
                                                    Assertions.assertIterableEquals(
                                                            query(
                                                                    getQuerySQL(
                                                                            POSTGRESQL_SCHEMA,
                                                                            SOURCE_TABLE_1)),
                                                            query(
                                                                    getQuerySQL(
                                                                            POSTGRESQL_SCHEMA,
                                                                            SINK_TABLE_1))),
                                            () ->
                                                    Assertions.assertIterableEquals(
                                                            query(
                                                                    getQuerySQL(
                                                                            POSTGRESQL_SCHEMA,
                                                                            SOURCE_TABLE_2)),
                                                            query(
                                                                    getQuerySQL(
                                                                            POSTGRESQL_SCHEMA,
                                                                            SINK_TABLE_2)))));

            // insert update delete
            upsertDeleteSourceTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_1);
            upsertDeleteSourceTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_2);

            // stream stage
            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertAll(
                                            () ->
                                                    Assertions.assertIterableEquals(
                                                            query(
                                                                    getQuerySQL(
                                                                            POSTGRESQL_SCHEMA,
                                                                            SOURCE_TABLE_1)),
                                                            query(
                                                                    getQuerySQL(
                                                                            POSTGRESQL_SCHEMA,
                                                                            SINK_TABLE_1))),
                                            () ->
                                                    Assertions.assertIterableEquals(
                                                            query(
                                                                    getQuerySQL(
                                                                            POSTGRESQL_SCHEMA,
                                                                            SOURCE_TABLE_2)),
                                                            query(
                                                                    getQuerySQL(
                                                                            POSTGRESQL_SCHEMA,
                                                                            SINK_TABLE_2)))));
        } finally {
            // Clear related content to ensure that multiple operations are not affected
            clearTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_1);
            clearTable(POSTGRESQL_SCHEMA, SINK_TABLE_1);
            clearTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_2);
            clearTable(POSTGRESQL_SCHEMA, SINK_TABLE_2);
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason = "Currently SPARK and FLINK do not support restore")
    public void testMultiTableWithRestore(TestContainer container)
            throws IOException, InterruptedException {
        Long jobId = JobIdGenerator.newJobId();
        String slotVariable = toSlotVariable(createSlotName());
        try {
            CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            return container.executeJob(
                                    "/pgcdc_to_pg_with_multi_table_mode_one_table.conf",
                                    String.valueOf(jobId),
                                    slotVariable);
                        } catch (Exception e) {
                            log.error("Commit task exception :" + e.getMessage());
                            throw new RuntimeException(e);
                        }
                    });

            // insert update delete
            upsertDeleteSourceTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_1);

            // stream stage
            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertAll(
                                            () ->
                                                    Assertions.assertIterableEquals(
                                                            query(
                                                                    getQuerySQL(
                                                                            POSTGRESQL_SCHEMA,
                                                                            SOURCE_TABLE_1)),
                                                            query(
                                                                    getQuerySQL(
                                                                            POSTGRESQL_SCHEMA,
                                                                            SINK_TABLE_1)))));

            Assertions.assertEquals(0, container.savepointJob(String.valueOf(jobId)).getExitCode());

            // Restore job with add a new table
            CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            container.restoreJob(
                                    "/pgcdc_to_pg_with_multi_table_mode_two_table.conf",
                                    String.valueOf(jobId),
                                    slotVariable);
                        } catch (Exception e) {
                            log.error("Commit task exception :" + e.getMessage());
                            throw new RuntimeException(e);
                        }
                        return null;
                    });

            upsertDeleteSourceTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_2);

            // stream stage
            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertAll(
                                            () ->
                                                    Assertions.assertIterableEquals(
                                                            query(
                                                                    getQuerySQL(
                                                                            POSTGRESQL_SCHEMA,
                                                                            SOURCE_TABLE_1)),
                                                            query(
                                                                    getQuerySQL(
                                                                            POSTGRESQL_SCHEMA,
                                                                            SINK_TABLE_1))),
                                            () ->
                                                    Assertions.assertIterableEquals(
                                                            query(
                                                                    getQuerySQL(
                                                                            POSTGRESQL_SCHEMA,
                                                                            SOURCE_TABLE_2)),
                                                            query(
                                                                    getQuerySQL(
                                                                            POSTGRESQL_SCHEMA,
                                                                            SINK_TABLE_2)))));

            log.info("****************** container logs start ******************");
            String containerLogs = container.getServerLogs();
            log.info(containerLogs);
            // pg cdc logs contain ERROR
            // Assertions.assertFalse(containerLogs.contains("ERROR"));
            log.info("****************** container logs end ******************");
        } finally {
            // Clear related content to ensure that multiple operations are not affected
            clearTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_1);
            clearTable(POSTGRESQL_SCHEMA, SINK_TABLE_1);
            clearTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_2);
            clearTable(POSTGRESQL_SCHEMA, SINK_TABLE_2);
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason = "Currently SPARK and FLINK do not support restore")
    public void testAddFieldWithRestore(TestContainer container)
            throws IOException, InterruptedException {
        Long jobId = JobIdGenerator.newJobId();
        String slotVariable = toSlotVariable(createSlotName());
        try {
            CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            return container.executeJob(
                                    "/postgrescdc_to_postgres_test_add_Filed.conf",
                                    String.valueOf(jobId),
                                    slotVariable);
                        } catch (Exception e) {
                            log.error("Commit task exception :" + e.getMessage());
                            throw new RuntimeException(e);
                        }
                    });

            // stream stage
            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertAll(
                                            () ->
                                                    Assertions.assertIterableEquals(
                                                            query(
                                                                    getQuerySQL(
                                                                            POSTGRESQL_SCHEMA,
                                                                            SOURCE_TABLE_3)),
                                                            query(
                                                                    getQuerySQL(
                                                                            POSTGRESQL_SCHEMA,
                                                                            SINK_TABLE_3)))));

            Assertions.assertEquals(0, container.savepointJob(String.valueOf(jobId)).getExitCode());

            // add field add insert source table data
            addFieldsForTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_3);
            addFieldsForTable(POSTGRESQL_SCHEMA, SINK_TABLE_3);
            insertSourceTableForAddFields(POSTGRESQL_SCHEMA, SOURCE_TABLE_3);

            // Restore job
            CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            container.restoreJob(
                                    "/postgrescdc_to_postgres_test_add_Filed.conf",
                                    String.valueOf(jobId),
                                    slotVariable);
                        } catch (Exception e) {
                            log.error("Commit task exception :" + e.getMessage());
                            throw new RuntimeException(e);
                        }
                        return null;
                    });

            // stream stage
            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertAll(
                                            () ->
                                                    Assertions.assertIterableEquals(
                                                            query(
                                                                    getQuerySQL(
                                                                            POSTGRESQL_SCHEMA,
                                                                            SOURCE_TABLE_3)),
                                                            query(
                                                                    getQuerySQL(
                                                                            POSTGRESQL_SCHEMA,
                                                                            SINK_TABLE_3)))));

            log.info("****************** container logs start ******************");
            String containerLogs = container.getServerLogs();
            log.info(containerLogs);
            // pg cdc logs contain ERROR
            // Assertions.assertFalse(containerLogs.contains("ERROR"));
            log.info("****************** container logs end ******************");
        } finally {
            // Clear related content to ensure that multiple operations are not affected
            clearTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_3);
            clearTable(POSTGRESQL_SCHEMA, SINK_TABLE_3);
        }
    }

    @TestTemplate
    public void testPostgresCdcCheckDataWithNoPrimaryKey(TestContainer container) throws Exception {
        String slotVariable = toSlotVariable(createSlotName());

        try {
            CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            container.executeJob(
                                    "/postgrescdc_to_postgres_with_no_primary_key.conf",
                                    Collections.singletonList(slotVariable));
                        } catch (Exception e) {
                            log.error("Commit task exception :" + e.getMessage());
                            throw new RuntimeException(e);
                        }
                        return null;
                    });

            // snapshot stage
            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertIterableEquals(
                                        query(
                                                getQuerySQL(
                                                        POSTGRESQL_SCHEMA,
                                                        SOURCE_TABLE_NO_PRIMARY_KEY)),
                                        query(getQuerySQL(POSTGRESQL_SCHEMA, SINK_TABLE_1)));
                            });

            // insert update delete
            upsertDeleteSourceTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_NO_PRIMARY_KEY);

            // stream stage
            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertIterableEquals(
                                        query(
                                                getQuerySQL(
                                                        POSTGRESQL_SCHEMA,
                                                        SOURCE_TABLE_NO_PRIMARY_KEY)),
                                        query(getQuerySQL(POSTGRESQL_SCHEMA, SINK_TABLE_1)));
                            });
        } finally {
            clearTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_NO_PRIMARY_KEY);
            clearTable(POSTGRESQL_SCHEMA, SINK_TABLE_1);
        }
    }

    @TestTemplate
    public void testPostgresCdcCheckDataWithCustomPrimaryKey(TestContainer container) {
        String slotVariable = toSlotVariable(createSlotName());

        try {
            CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            container.executeJob(
                                    "/postgrescdc_to_postgres_with_custom_primary_key.conf",
                                    Collections.singletonList(slotVariable));
                        } catch (Exception e) {
                            log.error("Commit task exception :" + e.getMessage());
                            throw new RuntimeException(e);
                        }
                        return null;
                    });

            // snapshot stage
            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertIterableEquals(
                                        query(
                                                getQuerySQL(
                                                        POSTGRESQL_SCHEMA,
                                                        SOURCE_TABLE_NO_PRIMARY_KEY)),
                                        query(getQuerySQL(POSTGRESQL_SCHEMA, SINK_TABLE_1)));
                            });

            // insert update delete
            upsertDeleteSourceTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_NO_PRIMARY_KEY);

            // stream stage
            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertIterableEquals(
                                        query(
                                                getQuerySQL(
                                                        POSTGRESQL_SCHEMA,
                                                        SOURCE_TABLE_NO_PRIMARY_KEY)),
                                        query(getQuerySQL(POSTGRESQL_SCHEMA, SINK_TABLE_1)));
                            });
        } finally {
            clearTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_NO_PRIMARY_KEY);
            clearTable(POSTGRESQL_SCHEMA, SINK_TABLE_1);
        }
    }

    @TestTemplate
    public void testPostgresCdcCheckDataWithIntervalDataType(TestContainer container)
            throws Exception {
        String slotVariable = toSlotVariable(createSlotName());

        try {
            CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            container.executeJob(
                                    "/postgrescdc_to_postgres_with_interval_data_type.conf",
                                    Collections.singletonList(slotVariable));
                        } catch (Exception e) {
                            log.error("Commit task exception :" + e.getMessage());
                            throw new RuntimeException(e);
                        }
                        return null;
                    });

            // stream stage
            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertIterableEquals(
                                        query(getQuerySQL(POSTGRESQL_SCHEMA, SOURCE_TABLE_4)),
                                        query(getQuerySQL(POSTGRESQL_SCHEMA, SINK_TABLE_4)));
                            });
        } finally {
            clearTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_4);
            clearTable(POSTGRESQL_SCHEMA, SINK_TABLE_4);
        }
    }

    @TestTemplate
    public void testPostgresCdcCheckDataWithNetworkAddressTypes(TestContainer container) {
        String slotVariable = toSlotVariable(createSlotName());
        try {
            CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            container.executeJob(
                                    "/postgrescdc_to_postgres_with_network_address_types.conf",
                                    Collections.singletonList(slotVariable));
                        } catch (Exception e) {
                            log.error("Commit task exception :" + e.getMessage());
                            throw new RuntimeException(e);
                        }
                        return null;
                    });

            // stream stage
            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertIterableEquals(
                                        query(getQuerySQL(POSTGRESQL_SCHEMA, SOURCE_TABLE_5)),
                                        query(getQuerySQL(POSTGRESQL_SCHEMA, SINK_TABLE_5)));
                            });
        } finally {
            clearTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_5);
            clearTable(POSTGRESQL_SCHEMA, SINK_TABLE_5);
        }
    }

    @Test
    public void testDialectCheckDisabledCDCTable() throws SQLException {
        JdbcSourceConfigFactory factory =
                new PostgresSourceConfigFactory()
                        .hostname(POSTGRES_CONTAINER.getHost())
                        .port(5432)
                        .username("postgres")
                        .password("postgres")
                        .databaseList(POSTGRESQL_DATABASE);
        PostgresDialect dialect =
                new PostgresDialect((PostgresSourceConfigFactory) factory, Collections.emptyList());
        try (JdbcConnection connection = dialect.openJdbcConnection(factory.create(0))) {
            SeaTunnelException exception =
                    Assertions.assertThrows(
                            SeaTunnelException.class,
                            () ->
                                    dialect.checkAllTablesEnabledCapture(
                                            connection,
                                            Collections.singletonList(
                                                    TableId.parse(SINK_TABLE_1))));
            Assertions.assertEquals(
                    "Table sink_postgres_cdc_table_1 does not have a full replica identity, please execute: ALTER TABLE sink_postgres_cdc_table_1 REPLICA IDENTITY FULL;",
                    exception.getMessage());
        }
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                POSTGRES_CONTAINER.getJdbcUrl(),
                POSTGRES_CONTAINER.getUsername(),
                POSTGRES_CONTAINER.getPassword());
    }

    /**
     * The Postgres container is shared across all test methods in this class, so stale generated
     * replication slots must be dropped before the next CDC job starts.
     */
    private void cleanupGeneratedReplicationSlots() {
        for (String slotName : listGeneratedReplicationSlots()) {
            await().ignoreExceptions()
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(() -> dropReplicationSlot(slotName));
        }
    }

    /**
     * The Debezium JSON Kafka test verifies every row-level change event, so it must wait until the
     * WAL stream is active before mutating the source table.
     */
    private void waitForReplicationSlotActive(String slotName) {
        await().ignoreExceptions()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        isReplicationSlotActive(slotName),
                                        "Replication slot is not active yet: " + slotName));
    }

    private List<String> listGeneratedReplicationSlots() {
        List<String> slotNames = new ArrayList<>();
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery(
                                "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE '"
                                        + GENERATED_SLOT_PREFIX
                                        + "%'")) {
            while (resultSet.next()) {
                slotNames.add(resultSet.getString("slot_name"));
            }
            return slotNames;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to query generated replication slots", e);
        }
    }

    private boolean isReplicationSlotActive(String slotName) {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery(
                                "SELECT active FROM pg_replication_slots WHERE slot_name = '"
                                        + slotName
                                        + "'")) {
            return resultSet.next() && resultSet.getBoolean("active");
        } catch (SQLException e) {
            throw new RuntimeException("Failed to query replication slot activity: " + slotName, e);
        }
    }

    private void dropReplicationSlot(String slotName) {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery(
                                "SELECT active FROM pg_replication_slots WHERE slot_name = '"
                                        + slotName
                                        + "'")) {
            if (!resultSet.next()) {
                return;
            }
            Assertions.assertFalse(
                    resultSet.getBoolean("active"),
                    "Replication slot is still active: " + slotName);
            statement.execute("SELECT pg_drop_replication_slot('" + slotName + "')");
        } catch (SQLException e) {
            throw new RuntimeException("Failed to drop replication slot: " + slotName, e);
        }
    }

    protected void initializePostgresTable(PostgreSQLContainer container, String sqlFile) {
        final String ddlFile = String.format("ddl/%s.sql", sqlFile);
        final URL ddlTestFile = PostgresCDCIT.class.getClassLoader().getResource(ddlFile);
        Assertions.assertNotNull(ddlTestFile, "Cannot locate " + ddlFile);
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            final List<String> statements =
                    Arrays.stream(
                                    Files.readAllLines(Paths.get(ddlTestFile.toURI())).stream()
                                            .map(String::trim)
                                            .filter(x -> !x.startsWith("--") && !x.isEmpty())
                                            .map(
                                                    x -> {
                                                        final Matcher m =
                                                                COMMENT_PATTERN.matcher(x);
                                                        return m.matches() ? m.group(1) : x;
                                                    })
                                            .collect(Collectors.joining("\n"))
                                            .split(";\n"))
                            .collect(Collectors.toList());
            for (String stmt : statements) {
                statement.execute(stmt);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<List<Object>> query(String sql) {
        try (Connection connection = getJdbcConnection()) {
            ResultSet resultSet = connection.createStatement().executeQuery(sql);
            List<List<Object>> result = new ArrayList<>();
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                ArrayList<Object> objects = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    Object object = resultSet.getObject(i);
                    if (object instanceof byte[]) {
                        byte[] bytes = (byte[]) object;
                        object = new String(bytes, StandardCharsets.UTF_8);
                    }
                    objects.add(object);
                }
                log.debug(
                        String.format("Print Postgres-CDC query, sql: %s, data: %s", sql, objects));
                result.add(objects);
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    // Execute SQL
    private void executeSql(String sql) {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("SET search_path TO inventory;");
            statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void addFieldsForTable(String database, String tableName) {

        executeSql("ALTER TABLE " + database + "." + tableName + " ADD COLUMN f_big BIGINT");
    }

    private void insertSourceTableForAddFields(String database, String tableName) {
        executeSql(
                "INSERT INTO "
                        + database
                        + "."
                        + tableName
                        + " VALUES (2, '2', 32767, 65535, 2147483647);");
    }

    /** Wait until the Debezium JSON test observes the expected number of Kafka change records. */
    private void awaitKafkaRecordCount(AtomicReference<Integer> dataSize, int expectedCount) {
        await().atMost(DEBEZIUM_JSON_RECORD_WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            dataSize.updateAndGet(v -> v + getKafkaData().size());
                            Assertions.assertEquals(expectedCount, dataSize.get());
                        });
    }

    /**
     * Insert one row so the CDC test can assert the emitted Kafka record before the next mutation.
     */
    private void insertSourceTableRow(String database, String tableName, int id) {
        executeSql(
                "INSERT INTO "
                        + database
                        + "."
                        + tableName
                        + " VALUES ("
                        + id
                        + ", '2', 32767, 65535, 2147483647, 5.5, 6.6, 123.12345, 404.4443, true,\n"
                        + "        'Hello World', 'a', 'abc', 'abcd..xyz', '2020-07-17 18:00:22.123', '2020-07-17 18:00:22.123456',\n"
                        + "        '2020-07-17', '18:00:22', 500, 88, '192.168.1.1');");
    }

    /** Delete one row after its insert event is already visible in Kafka. */
    private void deleteSourceTableRow(String database, String tableName, int id) {
        executeSql("DELETE FROM " + database + "." + tableName + " where id = " + id + ";");
    }

    /** Update the inserted row in a separate step so CI can assert the incremental change event. */
    private void updateSourceTableBigField(String database, String tableName, int id, int value) {
        executeSql(
                "UPDATE "
                        + database
                        + "."
                        + tableName
                        + " SET f_big = "
                        + value
                        + " where id = "
                        + id
                        + ";");
    }

    private void upsertDeleteSourceTable(String database, String tableName) {

        executeSql(
                "INSERT INTO "
                        + database
                        + "."
                        + tableName
                        + " VALUES (2, '2', 32767, 65535, 2147483647, 5.5, 6.6, 123.12345, 404.4443, true,\n"
                        + "        'Hello World', 'a', 'abc', 'abcd..xyz', '2020-07-17 18:00:22.123', '2020-07-17 18:00:22.123456',\n"
                        + "        '2020-07-17', '18:00:22', 500, 88, '192.168.1.1');");

        executeSql(
                "INSERT INTO "
                        + database
                        + "."
                        + tableName
                        + " VALUES (3, '2', 32767, 65535, 2147483647, 5.5, 6.6, 123.12345, 404.4443, true,\n"
                        + "        'Hello World', 'a', 'abc', 'abcd..xyz', '2020-07-17 18:00:22.123', '2020-07-17 18:00:22.123456',\n"
                        + "        '2020-07-17', '18:00:22', 500, 88,'192.168.1.1');");

        executeSql("DELETE FROM " + database + "." + tableName + " where id = 2;");

        executeSql("UPDATE " + database + "." + tableName + " SET f_big = 10000 where id = 3;");
    }

    private String getQuerySQL(String database, String tableName) {
        return String.format(SOURCE_SQL_TEMPLATE, database, tableName);
    }

    @Override
    @AfterAll
    public void tearDown() {
        // close Container
        if (POSTGRES_CONTAINER != null) {
            POSTGRES_CONTAINER.close();
        }
    }

    private void clearTable(String database, String tableName) {
        executeSql("truncate table " + database + "." + tableName);
    }
}
