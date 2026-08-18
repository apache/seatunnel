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

package org.apache.seatunnel.connectors.seatunnel.rocketmq.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.common.RocketMqAdminUtil;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.common.StartMode;

import org.apache.rocketmq.common.admin.TopicOffset;
import org.apache.rocketmq.common.message.MessageQueue;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

class RocketMqSourceSplitEnumeratorTest {

    @Test
    void testRun_usesPerTopicTimestampOverrides() throws Exception {
        ConsumerMetadata metadata = new ConsumerMetadata();
        metadata.setTopics(Arrays.asList("topic_a", "topic_b"));
        metadata.setStartMode(StartMode.CONSUME_FROM_TIMESTAMP);
        metadata.setStartOffsetsTimestamp(1_000L);

        TopicTableConfig topicAConfig = new TopicTableConfig();
        topicAConfig.setStartMode(StartMode.CONSUME_FROM_TIMESTAMP);
        topicAConfig.setStartTimestamp(2_000L);

        Map<String, TopicTableConfig> topicConfigs = new HashMap<>();
        topicConfigs.put("topic_a", topicAConfig);

        MessageQueue topicAQueue = new MessageQueue("topic_a", "broker-a", 0);
        MessageQueue topicBQueue = new MessageQueue("topic_b", "broker-b", 0);

        SourceSplitEnumerator.Context<RocketMqSourceSplit> context = mockContext();

        try (MockedStatic<RocketMqAdminUtil> mockedAdmin =
                Mockito.mockStatic(RocketMqAdminUtil.class)) {
            mockedAdmin
                    .when(() -> RocketMqAdminUtil.offsetTopics(any(), eq(metadata.getTopics())))
                    .thenReturn(
                            Collections.singletonList(
                                    topicOffsets(
                                            queueOffset(topicAQueue, 0L, 20L),
                                            queueOffset(topicBQueue, 0L, 10L))));
            mockedAdmin
                    .when(
                            () ->
                                    RocketMqAdminUtil.searchOffsetsByTimestamp(
                                            any(),
                                            eq(Collections.singletonList(topicAQueue)),
                                            eq(2_000L)))
                    .thenReturn(Collections.singletonMap(topicAQueue, 12L));
            mockedAdmin
                    .when(
                            () ->
                                    RocketMqAdminUtil.searchOffsetsByTimestamp(
                                            any(),
                                            eq(Collections.singletonList(topicBQueue)),
                                            eq(1_000L)))
                    .thenReturn(Collections.singletonMap(topicBQueue, 7L));

            RocketMqSourceSplitEnumerator enumerator =
                    new RocketMqSourceSplitEnumerator(metadata, topicConfigs, context, -1L);

            enumerator.run();
        }

        Map<String, RocketMqSourceSplit> splitsByTopic = captureAssignedSplits(context);
        Assertions.assertEquals(12L, splitsByTopic.get("topic_a").getStartOffset());
        Assertions.assertEquals(7L, splitsByTopic.get("topic_b").getStartOffset());
    }

    @Test
    void testRun_fallsBackToFirstOffsetWhenGroupOffsetsAreMissing() throws Exception {
        ConsumerMetadata metadata = new ConsumerMetadata();
        metadata.setTopics(Collections.singletonList("topic_group"));
        metadata.setStartMode(StartMode.CONSUME_FROM_GROUP_OFFSETS);

        MessageQueue messageQueue = new MessageQueue("topic_group", "broker-group", 0);

        SourceSplitEnumerator.Context<RocketMqSourceSplit> context = mockContext();

        try (MockedStatic<RocketMqAdminUtil> mockedAdmin =
                Mockito.mockStatic(RocketMqAdminUtil.class)) {
            mockedAdmin
                    .when(() -> RocketMqAdminUtil.offsetTopics(any(), eq(metadata.getTopics())))
                    .thenReturn(
                            Collections.singletonList(
                                    topicOffsets(queueOffset(messageQueue, 5L, 18L))));
            mockedAdmin
                    .when(
                            () ->
                                    RocketMqAdminUtil.currentOffsets(
                                            any(),
                                            eq(metadata.getTopics()),
                                            eq(Collections.singleton(messageQueue))))
                    .thenReturn(Collections.emptyMap());
            mockedAdmin
                    .when(() -> RocketMqAdminUtil.flatOffsetTopics(any(), eq(metadata.getTopics())))
                    .thenReturn(topicOffsets(queueOffset(messageQueue, 5L, 18L)));

            RocketMqSourceSplitEnumerator enumerator =
                    new RocketMqSourceSplitEnumerator(
                            metadata, Collections.emptyMap(), context, -1L);

            enumerator.run();
        }

        Map<String, RocketMqSourceSplit> splitsByTopic = captureAssignedSplits(context);
        Assertions.assertEquals(5L, splitsByTopic.get("topic_group").getStartOffset());
    }

    @Test
    void testRun_matchesSpecificOffsetsByTopicAndQueueId() throws Exception {
        ConsumerMetadata metadata = new ConsumerMetadata();
        metadata.setTopics(Collections.singletonList("topic_specific"));
        metadata.setStartMode(StartMode.CONSUME_FROM_SPECIFIC_OFFSETS);
        metadata.setSpecificStartOffsets(
                Collections.singletonMap(new MessageQueue("topic_specific", null, 0), 33L));

        MessageQueue messageQueue = new MessageQueue("topic_specific", "broker-specific", 0);

        SourceSplitEnumerator.Context<RocketMqSourceSplit> context = mockContext();

        try (MockedStatic<RocketMqAdminUtil> mockedAdmin =
                Mockito.mockStatic(RocketMqAdminUtil.class)) {
            mockedAdmin
                    .when(() -> RocketMqAdminUtil.offsetTopics(any(), eq(metadata.getTopics())))
                    .thenReturn(
                            Collections.singletonList(
                                    topicOffsets(queueOffset(messageQueue, 0L, 50L))));

            RocketMqSourceSplitEnumerator enumerator =
                    new RocketMqSourceSplitEnumerator(
                            metadata, Collections.emptyMap(), context, -1L);

            enumerator.run();
        }

        Map<String, RocketMqSourceSplit> splitsByTopic = captureAssignedSplits(context);
        Assertions.assertEquals(33L, splitsByTopic.get("topic_specific").getStartOffset());
    }

    /**
     * Verifies that when an enumerator is restored from a checkpoint, the splits returned via
     * addSplitsBack() retain their checkpoint offsets and are NOT reset to the broker's offset.
     *
     * <p>Scenario: the checkpoint recorded startOffset=50 for a queue whose broker minOffset=0.
     * After restore, consumption must resume from 50, not restart from 0.
     */
    @Test
    void testRestore_preservesCheckpointOffsets() throws Exception {
        ConsumerMetadata metadata = new ConsumerMetadata();
        metadata.setTopics(Collections.singletonList("topic_restore"));
        metadata.setStartMode(StartMode.CONSUME_FROM_LAST_OFFSET);

        MessageQueue messageQueue = new MessageQueue("topic_restore", "broker-restore", 0);

        // The split saved in the checkpoint carries startOffset=50
        final long checkpointOffset = 50L;
        RocketMqSourceSplit checkpointSplit =
                new RocketMqSourceSplit(messageQueue, checkpointOffset, 99L);

        Set<RocketMqSourceSplit> checkpointSplits = new HashSet<>();
        checkpointSplits.add(checkpointSplit);
        RocketMqSourceState checkpointState = new RocketMqSourceState(checkpointSplits);

        SourceSplitEnumerator.Context<RocketMqSourceSplit> context = mockContext();

        try (MockedStatic<RocketMqAdminUtil> mockedAdmin =
                Mockito.mockStatic(RocketMqAdminUtil.class)) {
            // Broker reports the queue exists with minOffset=0, maxOffset=100
            mockedAdmin
                    .when(() -> RocketMqAdminUtil.offsetTopics(any(), any()))
                    .thenReturn(
                            Collections.singletonList(
                                    topicOffsets(queueOffset(messageQueue, 0L, 100L))));

            RocketMqSourceSplitEnumerator enumerator =
                    new RocketMqSourceSplitEnumerator(
                            metadata, Collections.emptyMap(), context, -1L, checkpointState);

            // Simulate the engine returning the checkpoint split before run()
            enumerator.addSplitsBack(Collections.singletonList(checkpointSplit.copy()), 0);

            enumerator.run();
        }

        Map<String, RocketMqSourceSplit> splitsByTopic = captureAssignedSplits(context);
        Assertions.assertEquals(
                checkpointOffset,
                splitsByTopic.get("topic_restore").getStartOffset(),
                "Checkpoint offset must be preserved after restore; offset must not be reset");
    }

    /**
     * Verifies that when restoring from a checkpoint, newly discovered queues (not present at
     * snapshot time) are assigned with offsets derived from the configured start mode, while the
     * restored splits keep their checkpoint offsets.
     */
    @Test
    void testRestore_newQueueUsesConfiguredStartMode() throws Exception {
        ConsumerMetadata metadata = new ConsumerMetadata();
        metadata.setTopics(Collections.singletonList("topic_mixed"));
        metadata.setStartMode(StartMode.CONSUME_FROM_LAST_OFFSET);

        MessageQueue restoredQueue = new MessageQueue("topic_mixed", "broker", 0);
        MessageQueue newQueue = new MessageQueue("topic_mixed", "broker", 1);

        final long checkpointOffset = 75L;
        final long newQueueLastOffset = 200L;
        RocketMqSourceSplit restoredSplit =
                new RocketMqSourceSplit(restoredQueue, checkpointOffset, 99L);

        Set<RocketMqSourceSplit> checkpointSplits = new HashSet<>();
        checkpointSplits.add(restoredSplit);
        RocketMqSourceState checkpointState = new RocketMqSourceState(checkpointSplits);

        SourceSplitEnumerator.Context<RocketMqSourceSplit> context =
                Mockito.mock(SourceSplitEnumerator.Context.class);
        Mockito.when(context.currentParallelism()).thenReturn(2);

        try (MockedStatic<RocketMqAdminUtil> mockedAdmin =
                Mockito.mockStatic(RocketMqAdminUtil.class)) {
            // Both queues now exist on the broker
            mockedAdmin
                    .when(() -> RocketMqAdminUtil.offsetTopics(any(), any()))
                    .thenReturn(
                            Collections.singletonList(
                                    topicOffsets(
                                            queueOffset(restoredQueue, 0L, 99L),
                                            queueOffset(newQueue, 0L, newQueueLastOffset))));
            mockedAdmin
                    .when(() -> RocketMqAdminUtil.flatOffsetTopics(any(), any()))
                    .thenReturn(
                            topicOffsets(
                                    queueOffset(restoredQueue, 0L, 99L),
                                    queueOffset(newQueue, 0L, newQueueLastOffset)));

            RocketMqSourceSplitEnumerator enumerator =
                    new RocketMqSourceSplitEnumerator(
                            metadata, Collections.emptyMap(), context, -1L, checkpointState);

            enumerator.addSplitsBack(Collections.singletonList(restoredSplit.copy()), 0);

            enumerator.run();
        }

        // Capture all assigned splits across all subtasks
        ArgumentCaptor<List> splitsCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(context, Mockito.atLeastOnce())
                .assignSplit(any(Integer.class), splitsCaptor.capture());

        List<RocketMqSourceSplit> allSplits = new ArrayList<>();
        for (List captured : splitsCaptor.getAllValues()) {
            for (Object obj : captured) {
                allSplits.add((RocketMqSourceSplit) obj);
            }
        }
        Map<String, RocketMqSourceSplit> splitsByQueue =
                allSplits.stream()
                        .collect(
                                Collectors.toMap(
                                        s ->
                                                s.getMessageQueue().getTopic()
                                                        + "-"
                                                        + s.getMessageQueue().getQueueId(),
                                        s -> s));

        // Restored split must keep checkpoint offset
        Assertions.assertEquals(
                checkpointOffset,
                splitsByQueue.get("topic_mixed-0").getStartOffset(),
                "Restored split must keep its checkpoint offset");

        // New split must get the broker's last offset (CONSUME_FROM_LAST_OFFSET)
        Assertions.assertEquals(
                newQueueLastOffset,
                splitsByQueue.get("topic_mixed-1").getStartOffset(),
                "New split must use the configured start mode offset");
    }

    private SourceSplitEnumerator.Context<RocketMqSourceSplit> mockContext() {
        SourceSplitEnumerator.Context<RocketMqSourceSplit> context =
                Mockito.mock(SourceSplitEnumerator.Context.class);
        Mockito.when(context.currentParallelism()).thenReturn(1);
        return context;
    }

    private Map<String, RocketMqSourceSplit> captureAssignedSplits(
            SourceSplitEnumerator.Context<RocketMqSourceSplit> context) {
        ArgumentCaptor<List> splitsCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(context).assignSplit(eq(0), splitsCaptor.capture());

        List<RocketMqSourceSplit> splits = splitsCaptor.getValue();
        Assertions.assertEquals(1, splitsCaptor.getAllValues().size());
        return splits.stream()
                .collect(
                        Collectors.toMap(
                                split -> split.getMessageQueue().getTopic(), split -> split));
    }

    private Map<MessageQueue, TopicOffset> topicOffsets(
            Map.Entry<MessageQueue, TopicOffset>... entries) {
        Map<MessageQueue, TopicOffset> offsets = new LinkedHashMap<>();
        for (Map.Entry<MessageQueue, TopicOffset> entry : entries) {
            offsets.put(entry.getKey(), entry.getValue());
        }
        return offsets;
    }

    private Map.Entry<MessageQueue, TopicOffset> queueOffset(
            MessageQueue messageQueue, long minOffset, long maxOffset) {
        TopicOffset topicOffset = new TopicOffset();
        topicOffset.setMinOffset(minOffset);
        topicOffset.setMaxOffset(maxOffset);
        return new AbstractMap.SimpleImmutableEntry<>(messageQueue, topicOffset);
    }
}
