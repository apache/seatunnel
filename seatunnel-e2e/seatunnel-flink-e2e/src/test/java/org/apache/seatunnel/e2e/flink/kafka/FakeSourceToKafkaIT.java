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

import org.apache.seatunnel.e2e.flink.FlinkKafkaContainer;

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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class FakeSourceToKafkaIT extends FlinkKafkaContainer {
    private static final Logger LOGGER = LoggerFactory.getLogger(FakeSourceToKafkaIT.class);
    private Consumer<byte[], byte[]> consumer;

    @Before
    @SuppressWarnings("magicnumber")
    public void startKafkaContainer() {
        LOGGER.info("init kafka consumer");
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
        if (consumer != null) {
            consumer.close();
        }
    }
}
