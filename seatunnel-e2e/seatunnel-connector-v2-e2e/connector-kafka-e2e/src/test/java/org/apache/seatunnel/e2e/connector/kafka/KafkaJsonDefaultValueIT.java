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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.given;

/**
 * Integration test for {@code schema.columns[].defaultValue} support in the JSON format
 * deserialization used by the Kafka source.
 *
 * <p>Produces three JSON messages to the source topic: one missing the fields entirely, one with
 * explicit {@code null} values, and one with real values. The SeaTunnel job reads them through the
 * Kafka source (JSON format + schema with defaultValue) and writes them to a sink Kafka topic. The
 * test then asserts that missing and {@code null} fields are filled with the configured default
 * value, while present fields keep their real value.
 */
@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.FLINK, EngineType.SPARK},
        disabledReason =
                "The JSON format defaultValue deserialization fix lives in the shared "
                        + "seatunnel-format-json module (engine-agnostic); it is verified "
                        + "end-to-end on the default Zeta engine.")
public class KafkaJsonDefaultValueIT extends TestSuiteBase implements TestResource {

    private static final String KAFKA_IMAGE_NAME = "confluentinc/cp-kafka:7.0.9";

    private static final String KAFKA_HOST = "kafka_e2e";

    private static final String SOURCE_TOPIC = "test-json-default-value-source";

    private static final String SINK_TOPIC = "test-json-default-value-sink";

    private static final List<String> KAFKA_TOPICS = Arrays.asList(SOURCE_TOPIC, SINK_TOPIC);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private KafkaContainer kafkaContainer;

    private KafkaConsumer<String, String> kafkaConsumer;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        createKafkaContainer();
        Startables.deepStart(Stream.of(kafkaContainer)).join();

        given().ignoreExceptions()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(180, TimeUnit.SECONDS)
                .untilAsserted(this::initializeKafkaTopics);

        given().ignoreExceptions()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(180, TimeUnit.SECONDS)
                .untilAsserted(this::waitForKafkaTopicsReady);

        initKafkaConsumer();
        produceDataToKafka();
    }

    private void createKafkaContainer() {
        kafkaContainer =
                new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE_NAME))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(KAFKA_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(KAFKA_IMAGE_NAME)));
    }

    private void initKafkaConsumer() {
        Properties prop = new Properties();
        String bootstrapServers = kafkaContainer.getBootstrapServers();
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

    private void initializeKafkaTopics() {
        try (AdminClient adminClient = createKafkaAdmin()) {
            Set<String> existingTopics = adminClient.listTopics().names().get(30, TimeUnit.SECONDS);
            List<NewTopic> topicsToCreate =
                    KAFKA_TOPICS.stream()
                            .filter(topic -> !existingTopics.contains(topic))
                            .map(topic -> new NewTopic(topic, 1, (short) 1))
                            .collect(Collectors.toList());
            if (!topicsToCreate.isEmpty()) {
                adminClient.createTopics(topicsToCreate).all().get(60, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while initializing Kafka topics", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new RuntimeException("Failed to initialize Kafka topics", e);
        }
    }

    private void waitForKafkaTopicsReady() {
        try (AdminClient adminClient = createKafkaAdmin()) {
            Map<String, org.apache.kafka.clients.admin.TopicDescription> topicDescriptions =
                    adminClient
                            .describeTopics(KAFKA_TOPICS)
                            .allTopicNames()
                            .get(30, TimeUnit.SECONDS);
            Assertions.assertEquals(
                    new HashSet<>(KAFKA_TOPICS).size(),
                    topicDescriptions.size(),
                    "Kafka topics are not ready yet");
            // Topic existence in the admin metadata view does not guarantee the broker the
            // job's own Kafka client connects to has finished assigning/propagating the
            // partition leader (UnknownTopicOrPartitionException). Wait until every
            // partition of every topic has a non-null leader before submitting the job.
            for (org.apache.kafka.clients.admin.TopicDescription description :
                    topicDescriptions.values()) {
                for (org.apache.kafka.common.TopicPartitionInfo partition :
                        description.partitions()) {
                    Assertions.assertNotNull(
                            partition.leader(),
                            "Kafka topic "
                                    + description.name()
                                    + " partition "
                                    + partition.partition()
                                    + " has no leader yet");
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for Kafka topics", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new RuntimeException("Kafka topics are not ready yet", e);
        }
    }

    private AdminClient createKafkaAdmin() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        return AdminClient.create(props);
    }

    private void produceDataToKafka() throws ExecutionException, InterruptedException {
        String bootstrapServers = kafkaContainer.getBootstrapServers();
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            // 1. Field missing entirely -> defaultValue should be applied for every column
            producer.send(new ProducerRecord<>(SOURCE_TOPIC, null, "{\"name\":\"Alice\"}")).get();
            // 2. Explicit null -> defaultValue should be applied for every column
            producer.send(
                            new ProducerRecord<>(
                                    SOURCE_TOPIC,
                                    null,
                                    "{\"name\":\"Bob\",\"age\":null,\"score\":null,\"status\":null,"
                                            + "\"flag\":null,\"count\":null,\"ratio\":null,"
                                            + "\"amount\":null,\"birthday\":null,\"created_at\":null}"))
                    .get();
            // 3. Real values (incl. scientific notation) -> should be kept as-is
            producer.send(
                            new ProducerRecord<>(
                                    SOURCE_TOPIC,
                                    null,
                                    "{\"name\":\"Charlie\",\"age\":25,\"score\":2e3,\"status\":\"OK\","
                                            + "\"flag\":false,\"count\":200,\"ratio\":2.5,"
                                            + "\"amount\":99.99,\"birthday\":\"2024-06-15\","
                                            + "\"created_at\":\"2024-06-15 08:00:00\"}"))
                    .get();
        }
    }

    @TestTemplate
    public void testJsonDefaultValue(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob(
                        "/jsonDefaultValueIT/kafka_source_json_default_value_to_kafka.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        // Accumulate across retries (KafkaConsumer is not replayable across polls:
        // its position advances with each poll, so resetting the list inside the
        // retry could never converge if the broker splits the records across polls)
        List<String> result = new ArrayList<>();
        kafkaConsumer.subscribe(Collections.singletonList(SINK_TOPIC));
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            ConsumerRecords<String, String> consumerRecords =
                                    kafkaConsumer.poll(Duration.ofMillis(1000));
                            for (ConsumerRecord<String, String> record : consumerRecords) {
                                result.add(record.value());
                            }
                            Assertions.assertEquals(3, result.size());
                        });

        // Key assertions off the name field rather than list position so the test
        // does not depend on sink-topic record ordering
        Map<String, JsonNode> rowsByName = new HashMap<>();
        for (String value : result) {
            JsonNode node = OBJECT_MAPPER.readTree(value);
            rowsByName.put(node.get("name").asText(), node);
        }

        // Field missing -> defaultValue applied for every column
        JsonNode alice = rowsByName.get("Alice");
        Assertions.assertEquals(18, alice.get("age").asInt());
        Assertions.assertEquals(0.0, alice.get("score").asDouble());
        Assertions.assertEquals("PENDING", alice.get("status").asText());
        Assertions.assertEquals(true, alice.get("flag").asBoolean());
        Assertions.assertEquals(100L, alice.get("count").asLong());
        Assertions.assertEquals(1.5, alice.get("ratio").asDouble());
        Assertions.assertEquals(10.5, alice.get("amount").asDouble());
        Assertions.assertEquals("2024-01-01", alice.get("birthday").asText());
        Assertions.assertEquals("2024-01-01T12:30:45", alice.get("created_at").asText());

        // Explicit null -> defaultValue applied for every column
        JsonNode bob = rowsByName.get("Bob");
        Assertions.assertEquals(18, bob.get("age").asInt());
        Assertions.assertEquals(0.0, bob.get("score").asDouble());
        Assertions.assertEquals("PENDING", bob.get("status").asText());
        Assertions.assertEquals(true, bob.get("flag").asBoolean());
        Assertions.assertEquals(100L, bob.get("count").asLong());
        Assertions.assertEquals(1.5, bob.get("ratio").asDouble());
        Assertions.assertEquals(10.5, bob.get("amount").asDouble());
        Assertions.assertEquals("2024-01-01", bob.get("birthday").asText());
        Assertions.assertEquals("2024-01-01T12:30:45", bob.get("created_at").asText());

        // Real values (incl. scientific notation) -> kept as-is
        JsonNode charlie = rowsByName.get("Charlie");
        Assertions.assertEquals(25, charlie.get("age").asInt());
        Assertions.assertEquals(2000.0, charlie.get("score").asDouble()); // JSON 2e3
        Assertions.assertEquals("OK", charlie.get("status").asText());
        Assertions.assertEquals(false, charlie.get("flag").asBoolean());
        Assertions.assertEquals(200L, charlie.get("count").asLong());
        Assertions.assertEquals(2.5, charlie.get("ratio").asDouble());
        Assertions.assertEquals(99.99, charlie.get("amount").asDouble());
        Assertions.assertEquals("2024-06-15", charlie.get("birthday").asText());
        Assertions.assertEquals("2024-06-15T08:00:00", charlie.get("created_at").asText());
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (kafkaConsumer != null) {
            kafkaConsumer.close();
        }
        if (kafkaContainer != null) {
            kafkaContainer.close();
        }
    }
}
