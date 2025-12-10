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
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

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
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testcontainers.shaded.org.awaitility.Awaitility.given;

@Slf4j
public class KafkaMultiTableSinkIT extends TestSuiteBase implements TestResource {

    private static final String KAFKA_IMAGE_NAME = "confluentinc/cp-kafka:7.0.9";
    private static final String KAFKA_HOST = "kafkaCluster";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private KafkaContainer kafkaContainer;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        kafkaContainer =
                new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE_NAME))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(KAFKA_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(KAFKA_IMAGE_NAME)));
        Startables.deepStart(Stream.of(kafkaContainer)).join();
        log.info("Kafka container started");
        given().ignoreExceptions()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(180, SECONDS)
                .untilAsserted(this::initKafkaAdmin);
    }

    private void initKafkaAdmin() {
        Properties adminProps = new Properties();
        adminProps.put(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            adminClient.listTopics().names().get();
            log.info("Kafka admin client initialized successfully");
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize Kafka admin client", e);
        }
    }

    private void deleteTopics(String... topicNames) {
        Properties adminProps = new Properties();
        adminProps.put(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            Set<String> existingTopics = adminClient.listTopics().names().get();
            List<String> topicsToDelete = new ArrayList<>();
            for (String topicName : topicNames) {
                if (existingTopics.contains(topicName)) {
                    topicsToDelete.add(topicName);
                }
            }
            if (!topicsToDelete.isEmpty()) {
                DeleteTopicsResult deleteResult = adminClient.deleteTopics(topicsToDelete);
                deleteResult.all().get(30, SECONDS);
                log.info("Deleted topics: {}", topicsToDelete);
                Thread.sleep(2000);
            }
        } catch (Exception e) {
            log.warn("Failed to delete topics: {}", Arrays.toString(topicNames), e);
        }
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (kafkaContainer != null) {
            kafkaContainer.close();
        }
    }

    @TestTemplate
    public void testMultiTableSinkKafka(TestContainer container)
            throws IOException, InterruptedException {
        deleteTopics("test_multi_table_topic_1", "test_multi_table_topic_2");
        Container.ExecResult execResult =
                container.executeJob("/kafka/fake_to_kafka_multi_table_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        // Verify data in topic_1
        List<String> topic1Data = getKafkaConsumerListData("test_multi_table_topic_1");
        Assertions.assertEquals(100, topic1Data.size(), "Topic 1 should have 100 records");
        for (String data : topic1Data) {
            JsonNode jsonNode = OBJECT_MAPPER.readTree(data);
            Assertions.assertTrue(jsonNode.has("c_string"));
            Assertions.assertTrue(jsonNode.has("c_int"));
            Assertions.assertTrue(jsonNode.has("c_bigint"));
        }

        // Verify data in topic_2
        List<String> topic2Data = getKafkaConsumerListData("test_multi_table_topic_2");
        Assertions.assertEquals(200, topic2Data.size(), "Topic 2 should have 200 records");
        for (String data : topic2Data) {
            JsonNode jsonNode = OBJECT_MAPPER.readTree(data);
            Assertions.assertTrue(jsonNode.has("c_string"));
            Assertions.assertTrue(jsonNode.has("c_double"));
            Assertions.assertTrue(jsonNode.has("c_timestamp"));
        }

        log.info(
                "Multi-table sink test passed: topic_1 has {} records, topic_2 has {} records",
                topic1Data.size(),
                topic2Data.size());
    }

    @TestTemplate
    public void testMultiTableSinkKafkaWithPartitionKey(TestContainer container)
            throws IOException, InterruptedException {
        deleteTopics("test_multi_table_partition_topic_1", "test_multi_table_partition_topic_2");
        Container.ExecResult execResult =
                container.executeJob(
                        "/kafka/fake_to_kafka_multi_table_sink_with_partition_key.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        // Verify data in topic with partition key
        List<String> topic1Data = getKafkaConsumerListData("test_multi_table_partition_topic_1");
        Assertions.assertEquals(
                100, topic1Data.size(), "Partition topic 1 should have 100 records");

        List<String> topic2Data = getKafkaConsumerListData("test_multi_table_partition_topic_2");
        Assertions.assertEquals(
                150, topic2Data.size(), "Partition topic 2 should have 150 records");

        log.info(
                "Multi-table sink with partition key test passed: partition_topic_1 has {} records, partition_topic_2 has {} records",
                topic1Data.size(),
                topic2Data.size());
    }

    private List<String> getKafkaConsumerListData(String topicName) {
        List<String> data = new ArrayList<>();
        Properties props = kafkaConsumerConfig();
        // Make sure we start from earliest to capture all records
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topicName));

            // Poll for initial records
            ConsumerRecords<String, String> initialRecords =
                    consumer.poll(Duration.ofMillis(5000)); // Wait 5 seconds initially
            for (ConsumerRecord<String, String> record : initialRecords) {
                if (record.value() != null) {
                    data.add(record.value());
                }
            }

            // Continue polling for up to 30 seconds or until no new records for 3 consecutive polls
            long startTime = System.currentTimeMillis();
            long emptyPollCount = 0;
            long maxTime = 30000; // 30 seconds max

            while ((System.currentTimeMillis() - startTime) < maxTime && emptyPollCount < 3) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2000));

                if (records.isEmpty()) {
                    emptyPollCount++;
                } else {
                    emptyPollCount = 0; // Reset counter when we get data
                    for (ConsumerRecord<String, String> record : records) {
                        if (record.value() != null) {
                            data.add(record.value());
                        }
                    }
                }
            }
        }
        return data;
    }

    private Properties kafkaConsumerConfig() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "seatunnel-multi-table-test-group-" + System.currentTimeMillis());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return props;
    }
}
