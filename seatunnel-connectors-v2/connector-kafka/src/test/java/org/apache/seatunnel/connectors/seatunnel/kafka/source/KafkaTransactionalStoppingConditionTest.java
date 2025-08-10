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

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsWithSplitIds;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitsAddition;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.StartMode;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Test class demonstrating the fix for transactional message stopping condition. */
public class KafkaTransactionalStoppingConditionTest {

    private static final String TOPIC = "test-topic";
    private static final int PARTITION = 0;
    private static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, PARTITION);

    private MockConsumer<byte[], byte[]> mockConsumer;

    @Mock private KafkaSourceConfig kafkaSourceConfig;

    ConsumerRecord<byte[], byte[]> consumerRecord1 =
            new ConsumerRecord<>(TOPIC, PARTITION, 13L, "key1".getBytes(), "value1".getBytes());
    ConsumerRecord<byte[], byte[]> consumerRecord2 =
            new ConsumerRecord<>(TOPIC, PARTITION, 14L, "key2".getBytes(), "value2".getBytes());

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        when(kafkaSourceConfig.getProperties()).thenReturn(properties);
        when(kafkaSourceConfig.getConsumerGroup()).thenReturn("test-group");
        when(kafkaSourceConfig.getBootstrap()).thenReturn("localhost:9092");
        when(kafkaSourceConfig.isCommitOnCheckpoint()).thenReturn(false);
        when(kafkaSourceConfig.getPollTimeout()).thenReturn(1000L);

        Map<TablePath, ConsumerMetadata> mapMetadata = new HashMap<>();
        ConsumerMetadata metadata = new ConsumerMetadata();
        metadata.setTopic(TOPIC);
        metadata.setStartMode(StartMode.EARLIEST);
        TablePath tablePath = TablePath.of("default", TOPIC);
        mapMetadata.put(tablePath, metadata);

        when(kafkaSourceConfig.getMapMetadata()).thenReturn(mapMetadata);
    }

    @Test
    @DisplayName("Test StoppingConditionWithControlMessages")
    void testStoppingConditionWithControlMessages() throws IOException {

        KafkaConsumer<byte[], byte[]> kafkaConsumer = mock(KafkaConsumer.class);

        // Setup scenario: records at offsets 13, 14 and control message at offset 15
        mockConsumer.assign(Collections.singletonList(TOPIC_PARTITION));
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(TOPIC_PARTITION, 13L));
        mockConsumer.updateEndOffsets(Collections.singletonMap(TOPIC_PARTITION, 16L));

        // Add visible records at offsets 13 and 14
        // Note: We cannot add a control message at offset 15 because control messages
        // are not visible to consumers and MockConsumer.addRecord() only adds visible records
        mockConsumer.addRecord(consumerRecord1);
        mockConsumer.addRecord(consumerRecord2);

        long stoppingOffset = 15L;

        // Poll records - this returns only the visible records
        ConsumerRecords<byte[], byte[]> records = mockConsumer.poll(Duration.ofMillis(100));
        assertEquals(
                2,
                records.count(),
                "Should have 2 visible records (control message at offset 15 is invisible)");

        when(kafkaConsumer.poll(any(Duration.class))).thenReturn(records);

        // Find the last visible record offset (what old approach used)
        long lastRecordOffset = -1;
        for (ConsumerRecord<byte[], byte[]> record : records) {
            lastRecordOffset = Math.max(lastRecordOffset, record.offset());
        }
        assertEquals(14L, lastRecordOffset, "Last visible record should be at offset 14");
        // Test old approach (problematic)
        boolean shouldStopOldApproach = lastRecordOffset >= stoppingOffset;
        assertFalse(
                shouldStopOldApproach,
                "Old approach fails: last record offset (14) < stopping offset (15), would continue polling indefinitely");

        // Simulate the EFFECT of control message at offset 15:
        // In real Kafka, after polling, the consumer position would advance to 16
        // because the control message at offset 15 advances the position but record.offset() is not
        // returned
        mockConsumer.seek(TOPIC_PARTITION, 16L);
        long consumerPosition = mockConsumer.position(TOPIC_PARTITION);
        // Test new approach (fixed)
        boolean shouldStopNewApproach = consumerPosition >= stoppingOffset;
        assertTrue(
                shouldStopNewApproach,
                "New approach works: consumer position (16) >= stopping offset (15), correctly stops");

        when(kafkaConsumer.position(TOPIC_PARTITION)).thenReturn(consumerPosition);
        when(kafkaConsumer.assignment()).thenReturn(mockConsumer.assignment());

        KafkaPartitionSplitReader reader =
                new KafkaPartitionSplitReader(kafkaSourceConfig, kafkaConsumer);

        // Create a KafkaSourceSplit with the stopping offset and add it to the reader
        TablePath tablePath = TablePath.of("default", "test-topic");
        KafkaSourceSplit split =
                new KafkaSourceSplit(tablePath, TOPIC_PARTITION, 13L, stoppingOffset);
        SplitsAddition<KafkaSourceSplit> splitsAddition =
                new SplitsAddition<>(Collections.singletonList(split));
        reader.handleSplitsChanges(splitsAddition);
        RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>> result = reader.fetch();
        assertNotNull(result, "Fetch result should not be null");

        // Verify the result contains the expected finished splits due to stopping condition
        assertTrue(
                result.finishedSplits().contains(TOPIC_PARTITION.toString()),
                "Result should contain finished split when consumer position >= stopping offset");

        String splitId = result.nextSplit();
        if (splitId != null) {
            int recordCount = 0;
            ConsumerRecord<byte[], byte[]> record;
            while ((record = result.nextRecordFromSplit()) != null) {
                recordCount++;
            }
            assertEquals(2, recordCount, "Should return 2 visible records (offsets 13, 14)");
        }
    }
}
