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

package org.apache.seatunnel.connectors.seatunnel.kafka.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsWithSplitIds;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitsAddition;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Tests the Kafka source reader consumer property mapping.
 *
 * <p>This guards the offset commit contract between SeaTunnel checkpoint commits and Kafka auto
 * commit.
 */
public class KafkaPartitionSplitReaderTest {

    /** Verifies that checkpoint commits disable Kafka auto commit. */
    @Test
    void testBuildConsumerPropertiesDisablesAutoCommitWhenCheckpointCommitEnabled() {
        KafkaSourceConfig kafkaSourceConfig = new KafkaSourceConfig(createConfig(true));

        Properties properties =
                KafkaPartitionSplitReader.buildConsumerProperties(
                        kafkaSourceConfig, 1, "seatunnel-test");

        Assertions.assertEquals(
                "false", properties.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
    }

    /** Verifies that Kafka auto commit stays enabled when checkpoint commits are disabled. */
    @Test
    void testBuildConsumerPropertiesEnablesAutoCommitWhenCheckpointCommitDisabled() {
        KafkaSourceConfig kafkaSourceConfig = new KafkaSourceConfig(createConfig(false));

        Properties properties =
                KafkaPartitionSplitReader.buildConsumerProperties(
                        kafkaSourceConfig, 2, "seatunnel-test");

        Assertions.assertEquals(
                "true", properties.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
    }

    @Test
    void shouldFinishSplitWhenConsumerPositionReachesStoppingOffsetAfterControlRecord()
            throws Exception {
        KafkaSourceConfig sourceConfig = Mockito.mock(KafkaSourceConfig.class);
        SourceReader.Context context = Mockito.mock(SourceReader.Context.class);
        KafkaConsumer<byte[], byte[]> consumer = Mockito.mock(KafkaConsumer.class);
        TopicPartition partition = new TopicPartition("transactional-topic", 0);

        Mockito.when(sourceConfig.getProperties()).thenReturn(new Properties());
        Mockito.when(sourceConfig.getConsumerGroup()).thenReturn("test-group");
        Mockito.when(sourceConfig.getBootstrap()).thenReturn("localhost:1");
        Mockito.when(sourceConfig.getPollTimeout()).thenReturn(1L);
        Mockito.when(context.getIndexOfSubtask()).thenReturn(0);
        Mockito.when(consumer.assignment()).thenReturn(Collections.singleton(partition));
        Mockito.when(consumer.position(partition)).thenReturn(0L, 101L);
        Mockito.when(consumer.poll(Mockito.any(Duration.class)))
                .thenReturn(ConsumerRecords.empty());

        KafkaPartitionSplitReader reader = new KafkaPartitionSplitReader(sourceConfig, context);
        setConsumer(reader, consumer);
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(
                                new KafkaSourceSplit(null, partition, 0L, 101L))));

        RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>> records = reader.fetch();

        Assertions.assertTrue(records.finishedSplits().contains(partition.toString()));
    }

    /** Creates the minimum Kafka source config used by the tests. */
    private ReadonlyConfig createConfig(boolean commitOnCheckpoint) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", "localhost:9092");
        configMap.put("group.id", "test-group");
        configMap.put("topic", "test-topic");
        configMap.put("schema", createSchema());
        configMap.put("format", "text");
        configMap.put("commit_on_checkpoint", commitOnCheckpoint);
        return ReadonlyConfig.fromMap(configMap);
    }

    /** Creates a minimal text schema for constructing {@link KafkaSourceConfig}. */
    private Map<String, Object> createSchema() {
        Map<String, Object> schemaFields = new HashMap<>();
        schemaFields.put("id", "int");

        Map<String, Object> schema = new HashMap<>();
        schema.put("fields", schemaFields);
        return schema;
    }

    private void setConsumer(
            KafkaPartitionSplitReader reader, KafkaConsumer<byte[], byte[]> consumer)
            throws Exception {
        Field consumerField = KafkaPartitionSplitReader.class.getDeclaredField("consumer");
        consumerField.setAccessible(true);
        consumerField.set(reader, consumer);
    }
}
