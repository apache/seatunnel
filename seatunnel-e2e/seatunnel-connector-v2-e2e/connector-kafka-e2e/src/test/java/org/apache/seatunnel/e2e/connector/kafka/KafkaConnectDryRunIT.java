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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.factory.SupportSourceDryRunValidation;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.kafka.sink.KafkaSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.kafka.source.KafkaSourceFactory;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TransactionListing;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringSerializer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises the factory-level metadata contract against Kafka without submitting a job. */
@Slf4j
public class KafkaConnectDryRunIT extends TestSuiteBase implements TestResource {

    private static final String IMAGE = "confluentinc/cp-kafka:7.0.9";
    private static final String TOPIC = "connect-dry-run-orders";
    private static final String PASSWORD = "test-only-password";
    private KafkaContainer kafka;
    private AdminClient admin;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        kafka =
                new KafkaContainer(DockerImageName.parse(IMAGE))
                        .withEnv(
                                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                                "BROKER:PLAINTEXT,PLAINTEXT:SASL_PLAINTEXT")
                        .withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
                        .withEnv(
                                "KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG",
                                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                                        + "user_test=\""
                                        + PASSWORD
                                        + "\";")
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(IMAGE)));
        kafka.start();
        log.info("Authenticated Kafka dry-run fixture started");
        admin = AdminClient.create(clientProperties(PASSWORD));
        admin.createTopics(Collections.singleton(new NewTopic(TOPIC, 1, (short) 1)))
                .all()
                .get(30, TimeUnit.SECONDS);
        await().atMost(60, TimeUnit.SECONDS)
                .ignoreExceptions()
                .untilAsserted(
                        () ->
                                assertTrue(
                                        admin.describeTopics(Collections.singleton(TOPIC))
                                                .allTopicNames().get(10, TimeUnit.SECONDS)
                                                .get(TOPIC).partitions().stream()
                                                .allMatch(
                                                        partition -> partition.leader() != null)));
        Properties producerProperties = clientProperties(PASSWORD);
        producerProperties.put("key.serializer", StringSerializer.class.getName());
        producerProperties.put("value.serializer", StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties)) {
            producer.send(new ProducerRecord<>(TOPIC, "seed", "record-must-remain-untouched"))
                    .get(30, TimeUnit.SECONDS);
        }
    }

    @AfterAll
    @Override
    public void tearDown() {
        try {
            if (admin != null) {
                admin.close(Duration.ofSeconds(1));
            }
        } finally {
            if (kafka != null) {
                kafka.stop();
            }
        }
    }

    @Test
    public void testAuthenticatedLiteralAndPatternValidationHasNoDataOrGroupSideEffects()
            throws Exception {
        Set<String> topicsBefore = admin.listTopics().names().get(30, TimeUnit.SECONDS);
        Set<String> groupsBefore = groups();
        long earliestBefore = offset(OffsetSpec.earliest());
        long latestBefore = offset(OffsetSpec.latest());

        validate(TOPIC, false, PASSWORD);
        validate("connect-dry-run-.*", true, PASSWORD);
        validate("future-topic-.*", true, PASSWORD);

        assertEquals(topicsBefore, admin.listTopics().names().get(30, TimeUnit.SECONDS));
        assertEquals(groupsBefore, groups());
        assertEquals(earliestBefore, offset(OffsetSpec.earliest()));
        assertEquals(latestBefore, offset(OffsetSpec.latest()));
        assertEquals(1L, latestBefore - earliestBefore);
    }

    @Test
    public void testMissingTopicFailsWithoutAutomaticCreation() throws Exception {
        String missing = "connect-dry-run-missing";
        ExecutionException exception =
                assertThrows(ExecutionException.class, () -> validate(missing, false, PASSWORD));
        assertTrue(exception.getCause() instanceof UnknownTopicOrPartitionException);
        assertFalse(admin.listTopics().names().get(30, TimeUnit.SECONDS).contains(missing));
    }

    @Test
    public void testIncorrectCredentialsFailValidation() {
        ExecutionException exception =
                assertThrows(ExecutionException.class, () -> validate(TOPIC, false, "incorrect"));
        assertTrue(exception.getCause() instanceof AuthenticationException);
    }

    private void validate(String topic, boolean pattern, String password) throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        Map<String, Object> options = new HashMap<>();
        options.put("bootstrap.servers", kafka.getBootstrapServers());
        options.put("topic", topic);
        options.put("pattern", pattern);
        options.put("consumer.group", "dry-run-must-not-create-this-group");
        options.put("kafka.config", new HashMap<>(clientProperties(password)));
        TableSourceFactoryContext context =
                new TableSourceFactoryContext(ReadonlyConfig.fromMap(options), classLoader);
        SupportSourceDryRunValidation validation = new KafkaSourceFactory();
        validation.validateConnectionForDryRun(context, validation.inferSchemaForDryRun(context));
    }

    @Test
    public void testSinkValidationDoesNotWriteCreateTopicsOrInitializeTransactions()
            throws Exception {
        Set<String> topicsBefore = allTopics();
        Set<String> transactionsBefore = transactions();
        long earliestBefore = offset(OffsetSpec.earliest());
        long latestBefore = offset(OffsetSpec.latest());

        validateSink(TOPIC, 0, PASSWORD);
        validateSink("prefix-${route}", null, PASSWORD);

        assertEquals(topicsBefore, allTopics());
        assertEquals(transactionsBefore, transactions());
        assertEquals(earliestBefore, offset(OffsetSpec.earliest()));
        assertEquals(latestBefore, offset(OffsetSpec.latest()));
        assertEquals(1L, latestBefore - earliestBefore);
    }

    @Test
    public void testSinkMissingTopicAndInvalidPartitionFailWithoutCreation() throws Exception {
        Set<String> topicsBefore = allTopics();
        IllegalArgumentException missing =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> validateSink("sink-dry-run-missing", null, PASSWORD));
        assertTrue(missing.getMessage().contains("target topic does not exist"));
        IllegalArgumentException outOfRange =
                assertThrows(
                        IllegalArgumentException.class, () -> validateSink(TOPIC, 1, PASSWORD));
        assertEquals(
                "Kafka sink connect dry-run: partition is outside the target topic's range",
                outOfRange.getMessage());
        IllegalArgumentException negative =
                assertThrows(
                        IllegalArgumentException.class, () -> validateSink(TOPIC, -1, PASSWORD));
        assertEquals(
                "Kafka sink connect dry-run: partition must not be negative",
                negative.getMessage());
        assertEquals(topicsBefore, allTopics());
    }

    @Test
    public void testSinkIncorrectCredentialsFailWithoutExposingPassword() {
        IllegalArgumentException failure =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> validateSink(TOPIC, null, "incorrect-secret"));
        assertTrue(failure.getMessage().contains("authentication failed"));
        assertFalse(failure.getMessage().contains("incorrect-secret"));
        assertNull(failure.getCause());
    }

    private void validateSink(String topic, Integer partition, String password) throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put("bootstrap.servers", kafka.getBootstrapServers());
        options.put("topic", topic);
        options.put("semantics", "EXACTLY_ONCE");
        options.put("transaction_prefix", "dry-run-must-not-initialize");
        options.put("kafka.config", new HashMap<>(clientProperties(password)));
        if (partition != null) {
            options.put("partition", partition);
        }
        CatalogTable table =
                CatalogTable.of(
                        TableIdentifier.of("test", "db", "input"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "route",
                                                BasicType.STRING_TYPE,
                                                0,
                                                true,
                                                null,
                                                null))
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "");
        new KafkaSinkFactory()
                .validateConnectionForDryRun(
                        new TableSinkFactoryContext(
                                table,
                                ReadonlyConfig.fromMap(options),
                                getClass().getClassLoader()));
    }

    private Set<String> allTopics() throws Exception {
        return admin.listTopics(new ListTopicsOptions().listInternal(true))
                .names()
                .get(30, TimeUnit.SECONDS);
    }

    private Set<String> transactions() throws Exception {
        return admin.listTransactions().all().get(30, TimeUnit.SECONDS).stream()
                .map(TransactionListing::transactionalId)
                .collect(Collectors.toSet());
    }

    private Properties clientProperties(String password) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafka.getBootstrapServers());
        properties.put("security.protocol", "SASL_PLAINTEXT");
        properties.put("sasl.mechanism", "PLAIN");
        properties.put(
                "sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                        + "username=\"test\" password=\""
                        + password
                        + "\";");
        properties.put("default.api.timeout.ms", "10000");
        properties.put("request.timeout.ms", "10000");
        return properties;
    }

    private long offset(OffsetSpec spec) throws Exception {
        TopicPartition partition = new TopicPartition(TOPIC, 0);
        return admin.listOffsets(Collections.singletonMap(partition, spec))
                .all()
                .get(30, TimeUnit.SECONDS)
                .get(partition)
                .offset();
    }

    private Set<String> groups() throws Exception {
        return admin.listConsumerGroups().all().get(30, TimeUnit.SECONDS).stream()
                .map(ConsumerGroupListing::groupId)
                .collect(Collectors.toSet());
    }
}
