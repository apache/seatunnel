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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import org.junit.jupiter.api.Assertions;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;

/**
 * Shared base for Kafka connector e2e tests.
 *
 * <p>Kafka topic creation is eventually consistent: an {@link AdminClient} describing a topic does
 * not guarantee that the broker the job's own Kafka client connects to has finished
 * assigning/propagating the partition leader. A job submitted too early can therefore fail with
 * {@code UnknownTopicOrPartitionException} inside {@code KafkaSourceSplitEnumerator.getTopicInfo}.
 * This class provides two checks to run after topic creation and before job submission:
 *
 * <ul>
 *   <li>{@link #waitForKafkaTopicsReady(Collection)} — waits until every partition of every topic
 *       reports a non-null leader in the admin metadata view.
 *   <li>{@link #warmUpKafkaTopics(Collection)} — produces and consumes a throwaway record per topic
 *       to force leader propagation end-to-end through the broker, then {@code deleteRecords}s the
 *       records so they do not leak into jobs reading with {@code start_mode = earliest}.
 * </ul>
 */
public abstract class AbstractKafkaIT extends TestSuiteBase implements TestResource {

    private static final String WARM_UP_GROUP_ID = "kafka-e2e-warmup";
    private static final String WARM_UP_RECORD_VALUE = "__warmup__";

    /** Bootstrap servers of the Kafka container, provided by the concrete test class. */
    protected abstract String kafkaBootstrapServers();

    /**
     * Waits until the given topics are visible in the admin metadata view with a non-null leader on
     * every partition.
     */
    protected void waitForKafkaTopicsReady(Collection<String> topics) {
        try (AdminClient adminClient = AdminClient.create(adminProperties())) {
            Map<String, TopicDescription> topicDescriptions =
                    adminClient.describeTopics(topics).allTopicNames().get(30, TimeUnit.SECONDS);
            Assertions.assertEquals(
                    topics.size(), topicDescriptions.size(), "Kafka topics are not ready yet");
            // Topic existence in the admin metadata view does not guarantee the broker the job's
            // own Kafka client connects to has finished assigning/propagating the partition
            // leader (UnknownTopicOrPartitionException). Wait until every partition of every
            // topic has a non-null leader before submitting the job.
            for (TopicDescription description : topicDescriptions.values()) {
                for (TopicPartitionInfo partition : description.partitions()) {
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

    /**
     * Forces the Kafka broker to finish metadata propagation for the given topics before a job is
     * submitted. Producing and consuming a throwaway record on each topic exercises the full
     * produce/consume path end-to-end and makes leader assignment observable through the broker;
     * the records are then deleted so they do not leak into jobs reading with {@code start_mode =
     * earliest}.
     */
    protected void warmUpKafkaTopics(Collection<String> topics)
            throws ExecutionException, InterruptedException, TimeoutException {
        String bootstrapServers = kafkaBootstrapServers();

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            for (String topic : topics) {
                producer.send(new ProducerRecord<>(topic, 0, null, WARM_UP_RECORD_VALUE)).get();
            }
        }

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, WARM_UP_GROUP_ID);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(topics);
            await().atMost(30000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                ConsumerRecords<String, String> records =
                                        consumer.poll(Duration.ofMillis(1000));
                                Assertions.assertTrue(
                                        records.count() > 0,
                                        "warm-up record not readable from " + topics);
                            });
        }

        // Remove the throwaway records so jobs (start_mode = earliest) do not read them
        try (AdminClient adminClient = AdminClient.create(adminProperties())) {
            Map<TopicPartition, RecordsToDelete> recordsToDelete =
                    topics.stream()
                            .collect(
                                    Collectors.toMap(
                                            topic -> new TopicPartition(topic, 0),
                                            topic -> RecordsToDelete.beforeOffset(1L)));
            adminClient.deleteRecords(recordsToDelete).all().get(30, TimeUnit.SECONDS);
        }
    }

    private Properties adminProperties() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers());
        return props;
    }
}
