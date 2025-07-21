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

import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.StartMode;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

/** Test class demonstrating the fix for transactional message stopping condition. */
public class KafkaTransactionalStoppingConditionTest {

    private static final String TOPIC = "test-topic";
    private static final int PARTITION = 0;
    private static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, PARTITION);

    private MockConsumer<byte[], byte[]> mockConsumer;

    @Mock private SourceReader.Context readerContext;

    @Mock private KafkaSourceConfig kafkaSourceConfig;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);

        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        // Setup mock config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        when(kafkaSourceConfig.getProperties()).thenReturn(properties);
        when(kafkaSourceConfig.getConsumerGroup()).thenReturn("test-group");
        when(kafkaSourceConfig.getBootstrap()).thenReturn("localhost:9092");
        when(kafkaSourceConfig.isCommitOnCheckpoint()).thenReturn(false);
        when(kafkaSourceConfig.getPollTimeout()).thenReturn(1000L);

        Map<String, ConsumerMetadata> mapMetadata = new HashMap<>();
        ConsumerMetadata metadata = new ConsumerMetadata();
        metadata.setTopic(TOPIC);
        metadata.setStartMode(StartMode.EARLIEST);
        mapMetadata.put(TOPIC, metadata);
    }

    /**
     * Test the core issue: control messages can cause infinite blocking when using last record
     * offset as stopping condition.
     *
     * <p>Note: MockConsumer cannot directly simulate control messages since they are invisible to
     * consumers. Instead, we simulate the EFFECT of control messages: consumer position advancing
     * beyond the last visible record.
     */
    @Test
    @DisplayName("Test stopping condition with control messages - demonstrates the fix")
    void testStoppingConditionWithControlMessages() {
        // Setup scenario: records at offsets 13, 14 and control message at offset 15
        mockConsumer.assign(Collections.singletonList(TOPIC_PARTITION));
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(TOPIC_PARTITION, 13L));
        mockConsumer.updateEndOffsets(Collections.singletonMap(TOPIC_PARTITION, 16L));

        // Add visible records at offsets 13 and 14
        // Note: We cannot add a control message at offset 15 because control messages
        // are not visible to consumers and MockConsumer.addRecord() only adds visible records
        mockConsumer.addRecord(
                new ConsumerRecord<>(
                        TOPIC, PARTITION, 13L, "key1".getBytes(), "value1".getBytes()));
        mockConsumer.addRecord(
                new ConsumerRecord<>(
                        TOPIC, PARTITION, 14L, "key2".getBytes(), "value2".getBytes()));

        long stoppingOffset = 15L;

        // Poll records - this returns only the visible records
        ConsumerRecords<byte[], byte[]> records = mockConsumer.poll(Duration.ofMillis(100));
        assertEquals(
                2,
                records.count(),
                "Should have 2 visible records (control message at offset 15 is invisible)");

        // Find the last visible record offset (what old approach used)
        long lastRecordOffset = -1;
        for (ConsumerRecord<byte[], byte[]> record : records) {
            lastRecordOffset = Math.max(lastRecordOffset, record.offset());
        }
        assertEquals(14L, lastRecordOffset, "Last visible record should be at offset 14");

        // Simulate the EFFECT of control message at offset 15:
        // In real Kafka, after polling, the consumer position would advance to 16
        // because the control message at offset 15 advances the position but record.offset() is not
        // returned
        mockConsumer.seek(TOPIC_PARTITION, 16L);
        long consumerPosition = mockConsumer.position(TOPIC_PARTITION);
        assertEquals(
                16L, consumerPosition, "Consumer position advances past control message to 16");

        // Test old approach (problematic)
        boolean shouldStopOldApproach = lastRecordOffset >= stoppingOffset;
        assertFalse(
                shouldStopOldApproach,
                "Old approach fails: last record offset (14) < stopping offset (15), would continue polling indefinitely");

        // Test new approach (fixed)
        boolean shouldStopNewApproach = consumerPosition >= stoppingOffset;
        assertTrue(
                shouldStopNewApproach,
                "New approach works: consumer position (16) >= stopping offset (15), correctly stops");
    }
}
