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

package org.apache.seatunnel.e2e.flink.kafka;

import org.apache.seatunnel.e2e.flink.FlinkContainer;

import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;

public class FakeSourceToKafkaIT extends FlinkContainer {
    private static final String KAFKA_DOCKER_IMAGE = "bitnami/kafka:latest";
    private static final String ZOOKEEPER_DOCKER_IMAGE = "bitnami/zookeeper:latest";
    private static final Logger LOGGER = LoggerFactory.getLogger(FakeSourceToKafkaIT.class);
    private GenericContainer<?> kafkaServer;
    private GenericContainer<?> zookeeperServer;
    private Consumer<byte[], byte[]> consumer;

    @Before
    @SuppressWarnings("magicnumber")
    public void startKafkaContainer() throws InterruptedException {
        zookeeperServer = new GenericContainer<>(ZOOKEEPER_DOCKER_IMAGE)
                .withNetwork(NETWORK)
                .withNetworkAliases("zookeeper")
                .withLogConsumer(new Slf4jLogConsumer(LOGGER))
                .withEnv("ALLOW_ANONYMOUS_LOGIN", "yes");
        zookeeperServer.setPortBindings(Lists.newArrayList("2181:2181"));
        zookeeperServer.setHostAccessible(true);

        Startables.deepStart(Stream.of(zookeeperServer)).join();

        LOGGER.info("Zookeeper container started");
        // wait for zookeeper fully start
        Thread.sleep(5000L);

        kafkaServer = new GenericContainer<>(KAFKA_DOCKER_IMAGE)
                .withNetwork(NETWORK)
                .withNetworkAliases("kafka")
                .withLogConsumer(new Slf4jLogConsumer(LOGGER))
                .withEnv("KAFKA_CFG_ZOOKEEPER_CONNECT", "zookeeper:2181")
                .withEnv("ALLOW_PLAINTEXT_LISTENER", "yes")
                .withEnv("KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP", "CLIENT:PLAINTEXT,EXTERNAL:PLAINTEXT")
                .withEnv("KAFKA_CFG_LISTENERS", "CLIENT://:9092,EXTERNAL://:9093")
                .withEnv("KAFKA_CFG_ADVERTISED_LISTENERS", "CLIENT://kafka:9092,EXTERNAL://localhost:9093")
                .withEnv("KAFKA_CFG_INTER_BROKER_LISTENER_NAME", "CLIENT")
                ;

        kafkaServer.setPortBindings(Lists.newArrayList("9092:9092", "9093:9093"));
        kafkaServer.setHostAccessible(true);

        Startables.deepStart(Stream.of(kafkaServer)).join();
        LOGGER.info("Kafka container started");
        // wait for Kafka fully start
        Thread.sleep(5000L);

        createKafkaConsumer();
    }

    /**
     * Test send csv formatted msg to kafka from fake source by flink batch mode.
     */
    @Test
    @SuppressWarnings("magicnumber")
    public void testFakeSourceToKafkaSinkWithCsvFormat() throws IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelFlinkJob("/kafka/fakesource_to_kafka_with_csv_format.conf");
        Assert.assertEquals(0, execResult.getExitCode());

        final TopicPartition topicPartition = new TopicPartition("test_1", 0);
        final Map<TopicPartition, Long> topicPartitionLongMap = consumer.endOffsets(Collections.singletonList(topicPartition));
        final Long count = Optional.ofNullable(topicPartitionLongMap.get(topicPartition)).orElse(0L);
        Assert.assertEquals(3, count.longValue());

        consumer.unsubscribe();
    }

    /**
     * Test send json formatted msg to kafka from fake source by flink batch mode.
     */
    @Test
    @SuppressWarnings("magicnumber")
    public void testFakeSourceToKafkaSinkWithJsonFormat() throws IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelFlinkJob("/kafka/fakesource_to_kafka_with_json_format.conf");
        Assert.assertEquals(0, execResult.getExitCode());

        final TopicPartition topicPartition = new TopicPartition("test_2", 0);
        final Map<TopicPartition, Long> topicPartitionLongMap = consumer.endOffsets(Collections.singletonList(topicPartition));
        final Long count = Optional.ofNullable(topicPartitionLongMap.get(topicPartition)).orElse(0L);
        Assert.assertEquals(3, count.longValue());
        consumer.unsubscribe();
    }

    private void createKafkaConsumer() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9093");
        props.put("key.deserializer", ByteArrayDeserializer.class.getName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());
        props.put("group.id", "test");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "false");

        consumer = new KafkaConsumer<>(props);
    }

    @After
    public void closeClickhouseContainer() {
        if (kafkaServer != null) {
            kafkaServer.stop();
        }
        if (zookeeperServer != null) {
            zookeeperServer.stop();
        }
        if (consumer != null) {
            consumer.close();
        }
    }
}
