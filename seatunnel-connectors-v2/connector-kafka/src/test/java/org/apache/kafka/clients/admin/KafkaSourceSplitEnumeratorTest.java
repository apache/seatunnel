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

package org.apache.kafka.clients.admin;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.StartMode;
import org.apache.seatunnel.connectors.seatunnel.kafka.source.ConsumerMetadata;
import org.apache.seatunnel.connectors.seatunnel.kafka.source.KafkaSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.kafka.source.KafkaSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.kafka.source.KafkaSourceSplitEnumerator;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

class KafkaSourceSplitEnumeratorTest {

    AdminClient adminClient = Mockito.mock(KafkaAdminClient.class);
    KafkaSourceConfig kafkaSourceConfig = Mockito.mock(KafkaSourceConfig.class);
    // prepare
    TopicPartition partition0 = new TopicPartition("test", 0);
    TopicPartition partition2 = new TopicPartition("test", 2);

    @BeforeEach
    void init() {
        Mockito.when(kafkaSourceConfig.getMapMetadata()).thenReturn(Collections.emptyMap());
        Mockito.when(kafkaSourceConfig.isIgnoreNoLeaderPartition()).thenReturn(false);

        Mockito.when(adminClient.listOffsets(Mockito.any(java.util.Map.class)))
                .thenAnswer(
                        invocation -> {
                            Map<TopicPartition, OffsetSpec> requestedOffsets =
                                    invocation.getArgument(0);
                            Map<
                                            TopicPartition,
                                            KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>>
                                    offsets = new HashMap<>();
                            requestedOffsets
                                    .keySet()
                                    .forEach(
                                            tp ->
                                                    offsets.put(
                                                            tp,
                                                            KafkaFuture.completedFuture(
                                                                    new ListOffsetsResult
                                                                            .ListOffsetsResultInfo(
                                                                            0,
                                                                            0,
                                                                            Optional.of(0)))));
                            return new ListOffsetsResult(offsets);
                        });

        List<TopicPartitionInfo> mockTopicPartition = new ArrayList<>();
        TopicPartitionInfo topicPartitionWithLeader =
                new TopicPartitionInfo(
                        0,
                        new Node(1, "127.0.0.1", 9092),
                        Collections.emptyList(),
                        Collections.emptyList());
        TopicPartitionInfo topicPartitionInfoNoLeader =
                new TopicPartitionInfo(2, null, Collections.emptyList(), Collections.emptyList());
        mockTopicPartition.add(topicPartitionWithLeader);
        mockTopicPartition.add(topicPartitionInfoNoLeader);

        Mockito.when(adminClient.describeTopics(Mockito.any(java.util.Collection.class)))
                .thenReturn(
                        DescribeTopicsResult.ofTopicNames(
                                new HashMap<String, KafkaFuture<TopicDescription>>() {
                                    {
                                        put(
                                                partition0.topic(),
                                                KafkaFuture.completedFuture(
                                                        new TopicDescription(
                                                                partition0.topic(),
                                                                false,
                                                                mockTopicPartition)));
                                    }
                                }));
    }

    @Test
    void addSplitsBackShouldPreserveCheckpointOffsetsDuringRestore() {
        Map<TopicPartition, KafkaSourceSplit> assignedSplit = new HashMap<>();
        Map<TopicPartition, KafkaSourceSplit> pendingSplit = new HashMap<>();
        List<KafkaSourceSplit> splits =
                Collections.singletonList(
                        new KafkaSourceSplit(TablePath.DEFAULT, partition0, 123L, Long.MAX_VALUE));
        KafkaSourceSplitEnumerator enumerator =
                new KafkaSourceSplitEnumerator(
                        adminClient, kafkaSourceConfig, pendingSplit, assignedSplit, true, true);
        enumerator.addSplitsBack(splits, 1);
        Assertions.assertEquals(splits.size(), pendingSplit.size());
        Assertions.assertTrue(assignedSplit.isEmpty());
        Assertions.assertEquals(123L, pendingSplit.get(partition0).getStartOffset());
        Assertions.assertEquals(Long.MAX_VALUE, pendingSplit.get(partition0).getEndOffset());
    }

    @Test
    void addSplitsBackShouldAdvanceOffsetsAfterInitialization() throws Exception {
        Map<TopicPartition, KafkaSourceSplit> assignedSplit =
                new HashMap<TopicPartition, KafkaSourceSplit>() {
                    {
                        put(partition0, new KafkaSourceSplit(null, partition0));
                    }
                };
        Map<TopicPartition, KafkaSourceSplit> pendingSplit = new HashMap<>();
        List<KafkaSourceSplit> splits =
                Collections.singletonList(new KafkaSourceSplit(null, partition0, 10L, 15L));
        KafkaSourceSplitEnumerator enumerator =
                new KafkaSourceSplitEnumerator(adminClient, pendingSplit, assignedSplit, true);
        setInitialized(enumerator, true);
        enumerator.addSplitsBack(splits, 1);
        Assertions.assertEquals(pendingSplit.size(), splits.size());
        Assertions.assertNull(assignedSplit.get(partition0));
        Assertions.assertEquals(16L, pendingSplit.get(partition0).getStartOffset());
        Assertions.assertTrue(pendingSplit.get(partition0).getEndOffset() == Long.MAX_VALUE);
    }

    @Test
    void setPartitionStartOffsetShouldNotOverrideRestoredSplits() throws Exception {
        ConsumerMetadata metadata = new ConsumerMetadata();
        metadata.setTopic("test");
        metadata.setStartMode(StartMode.EARLIEST);
        Mockito.when(kafkaSourceConfig.getMapMetadata())
                .thenReturn(Collections.singletonMap(TablePath.DEFAULT, metadata));

        Map<TopicPartition, KafkaSourceSplit> assignedSplit = new HashMap<>();
        Map<TopicPartition, KafkaSourceSplit> pendingSplit = new HashMap<>();
        KafkaSourceSplitEnumerator enumerator =
                new KafkaSourceSplitEnumerator(
                        adminClient, kafkaSourceConfig, pendingSplit, assignedSplit, true, true);

        enumerator.addSplitsBack(
                Collections.singletonList(
                        new KafkaSourceSplit(TablePath.DEFAULT, partition0, 123L, Long.MAX_VALUE)),
                0);
        enumerator.fetchPendingPartitionSplit();
        invokeSetPartitionStartOffset(enumerator);

        Assertions.assertEquals(123L, pendingSplit.get(partition0).getStartOffset());
        Assertions.assertEquals(Long.MAX_VALUE, pendingSplit.get(partition0).getEndOffset());
        Assertions.assertEquals(0L, pendingSplit.get(partition2).getStartOffset());
    }

    @Test
    void addStreamingSplits() throws ExecutionException, InterruptedException {
        // test
        Map<TopicPartition, KafkaSourceSplit> assignedSplit =
                new HashMap<TopicPartition, KafkaSourceSplit>();
        Map<TopicPartition, KafkaSourceSplit> pendingSplit = new HashMap<>();

        List<KafkaSourceSplit> splits =
                Arrays.asList(
                        new KafkaSourceSplit(null, partition0),
                        new KafkaSourceSplit(null, partition2));
        KafkaSourceSplitEnumerator enumerator =
                new KafkaSourceSplitEnumerator(adminClient, pendingSplit, assignedSplit, true);
        enumerator.fetchPendingPartitionSplit();
        Assertions.assertEquals(pendingSplit.size(), splits.size());
        Assertions.assertNotNull(pendingSplit.get(partition0));
        Assertions.assertTrue(pendingSplit.get(partition0).getEndOffset() == Long.MAX_VALUE);
    }

    @Test
    void addplits() throws ExecutionException, InterruptedException {
        // test
        Map<TopicPartition, KafkaSourceSplit> assignedSplit =
                new HashMap<TopicPartition, KafkaSourceSplit>();
        Map<TopicPartition, KafkaSourceSplit> pendingSplit = new HashMap<>();
        List<KafkaSourceSplit> splits =
                Arrays.asList(
                        new KafkaSourceSplit(null, partition0),
                        new KafkaSourceSplit(null, partition2));

        KafkaSourceSplitEnumerator enumerator =
                new KafkaSourceSplitEnumerator(adminClient, pendingSplit, assignedSplit, false);
        enumerator.fetchPendingPartitionSplit();
        Assertions.assertEquals(pendingSplit.size(), splits.size());
        Assertions.assertNotNull(pendingSplit.get(partition0));
        Assertions.assertTrue(pendingSplit.get(partition0).getEndOffset() == 0);
    }

    @Test
    void testIgnoreNoLeaderPartition() throws ExecutionException, InterruptedException {

        Map<TopicPartition, KafkaSourceSplit> assignedSplit = new HashMap<>();
        Map<TopicPartition, KafkaSourceSplit> pendingSplit = new HashMap<>();

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("group.id", "test");
        configMap.put("topic", "test");
        configMap.put("ignore_no_leader_partition", "false");
        KafkaSourceConfig sourceConfig = new KafkaSourceConfig(ReadonlyConfig.fromMap(configMap));
        KafkaSourceSplitEnumerator enumerator =
                new KafkaSourceSplitEnumerator(
                        adminClient, sourceConfig, pendingSplit, assignedSplit);
        enumerator.fetchPendingPartitionSplit();

        Assertions.assertEquals(2, pendingSplit.size());
        Assertions.assertNotNull(pendingSplit.get(partition0));
        Assertions.assertNotNull(pendingSplit.get(partition2));

        pendingSplit.clear();
        assignedSplit.clear();

        configMap.put("ignore_no_leader_partition", "true");
        configMap.put("partition-discovery.interval-millis", 5000L);
        sourceConfig = new KafkaSourceConfig(ReadonlyConfig.fromMap(configMap));
        enumerator =
                new KafkaSourceSplitEnumerator(
                        adminClient, sourceConfig, pendingSplit, assignedSplit);
        enumerator.fetchPendingPartitionSplit();
        Assertions.assertEquals(1, pendingSplit.size());
        Assertions.assertNotNull(pendingSplit.get(partition0));
        Assertions.assertNull(pendingSplit.get(partition2));

        // Test partition restoration: simulate partition2 getting a leader
        // Create new mock topic partition list with partition2 now having a leader
        List<TopicPartitionInfo> restoredMockTopicPartition = new ArrayList<>();
        TopicPartitionInfo topicPartitionWithLeader =
                new TopicPartitionInfo(
                        0,
                        new Node(1, "127.0.0.1", 9092),
                        Collections.emptyList(),
                        Collections.emptyList());
        TopicPartitionInfo restoredTopicPartitionWithLeader =
                new TopicPartitionInfo(
                        2,
                        new Node(2, "127.0.0.1", 9093), // partition2 now has a leader
                        Collections.emptyList(),
                        Collections.emptyList());
        restoredMockTopicPartition.add(topicPartitionWithLeader);
        restoredMockTopicPartition.add(restoredTopicPartitionWithLeader);

        // Update the mock to return the restored partition information
        Mockito.when(adminClient.describeTopics(Mockito.any(java.util.Collection.class)))
                .thenReturn(
                        DescribeTopicsResult.ofTopicNames(
                                new HashMap<String, KafkaFuture<TopicDescription>>() {
                                    {
                                        put(
                                                partition0.topic(),
                                                KafkaFuture.completedFuture(
                                                        new TopicDescription(
                                                                partition0.topic(),
                                                                false,
                                                                restoredMockTopicPartition)));
                                    }
                                }));

        // Test that dynamic partition discovery detects the restored partition
        enumerator.fetchPendingPartitionSplit();

        // After partition restoration, both partitions should be available
        Assertions.assertEquals(2, pendingSplit.size());
        Assertions.assertNotNull(pendingSplit.get(partition0));
        Assertions.assertNotNull(pendingSplit.get(partition2));
    }

    private void setInitialized(KafkaSourceSplitEnumerator enumerator, boolean initialized)
            throws Exception {
        Field initializedField = KafkaSourceSplitEnumerator.class.getDeclaredField("initialized");
        initializedField.setAccessible(true);
        initializedField.set(enumerator, initialized);
    }

    private void invokeSetPartitionStartOffset(KafkaSourceSplitEnumerator enumerator)
            throws Exception {
        Method method =
                KafkaSourceSplitEnumerator.class.getDeclaredMethod("setPartitionStartOffset");
        method.setAccessible(true);
        method.invoke(enumerator);
    }
}
