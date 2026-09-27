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

package org.apache.seatunnel.connectors.seatunnel.rocketmq.common;

import org.apache.seatunnel.connectors.seatunnel.rocketmq.exception.RocketMqConnectorException;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.common.admin.OffsetWrapper;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Covers the contract of {@link RocketMqAdminUtil#currentOffsets}: an empty result must mean the
 * lookup succeeded and nothing was committed for the requested queues, never that the lookup
 * failed. Callers rewind to the first offset on an empty map, so conflating the two silently
 * re-delivers a whole topic.
 *
 * <p>{@code examineConsumeStats} resolves its route from the group's retry topic rather than from
 * the requested topic, so {@code TOPIC_NOT_EXIST} covers both "this group has never registered" and
 * "a route was lost". The two must be separated by re-resolving the requested topic, and the tests
 * below pin both directions.
 */
class RocketMqAdminUtilTest {

    private static final String GROUP = "test-group";
    private static final String TOPIC = "test-topic";
    private static final String OTHER_TOPIC = "other-test-topic";

    /**
     * A group that has never registered has no retry topic, so the lookup fails with
     * TOPIC_NOT_EXIST while the requested topic still resolves. That is a genuine cold start and
     * must return an empty map so the caller applies the configured start mode.
     */
    @Test
    void testCurrentOffsets_retryTopicMissingWhileRouteHealthyReturnsEmpty() throws Exception {
        DefaultMQAdminExt adminClient = Mockito.mock(DefaultMQAdminExt.class);
        Mockito.when(adminClient.examineConsumeStats(GROUP, TOPIC))
                .thenThrow(
                        new MQClientException(
                                ResponseCode.TOPIC_NOT_EXIST,
                                "No topic route info in name server for the topic: %RETRY%"
                                        + GROUP));
        Mockito.when(adminClient.examineTopicRouteInfo(TOPIC)).thenReturn(new TopicRouteData());

        MessageQueue messageQueue = new MessageQueue(TOPIC, "broker-a", 0);

        Map<MessageQueue, Long> offsets =
                RocketMqAdminUtil.currentOffsets(
                        adminClient,
                        GROUP,
                        Collections.singletonList(TOPIC),
                        Collections.singleton(messageQueue));

        Assertions.assertTrue(offsets.isEmpty());
    }

    /**
     * The name server drops routes per broker, so a real outage takes the requested topic's route
     * along with the retry topic's. Committed offsets may well exist, so this must surface as a
     * failure rather than as "nothing committed".
     */
    @Test
    void testCurrentOffsets_routeOutageSurfacesAsFailure() throws Exception {
        DefaultMQAdminExt adminClient = Mockito.mock(DefaultMQAdminExt.class);
        Mockito.when(adminClient.examineConsumeStats(GROUP, TOPIC))
                .thenThrow(
                        new MQClientException(
                                ResponseCode.TOPIC_NOT_EXIST,
                                "No topic route info in name server for the topic: %RETRY%"
                                        + GROUP));
        Mockito.when(adminClient.examineTopicRouteInfo(TOPIC))
                .thenThrow(
                        new MQClientException(
                                ResponseCode.TOPIC_NOT_EXIST,
                                "No topic route info in name server for the topic: " + TOPIC));

        MessageQueue messageQueue = new MessageQueue(TOPIC, "broker-a", 0);

        Assertions.assertThrows(
                RocketMqConnectorException.class,
                () ->
                        RocketMqAdminUtil.currentOffsets(
                                adminClient,
                                GROUP,
                                Collections.singletonList(TOPIC),
                                Collections.singleton(messageQueue)));
    }

    /**
     * The route probe only applies to TOPIC_NOT_EXIST. Any other failure surfaces even when the
     * requested topic resolves perfectly well.
     */
    @Test
    void testCurrentOffsets_otherResponseCodeSurfacesEvenWhenRouteHealthy() throws Exception {
        DefaultMQAdminExt adminClient = Mockito.mock(DefaultMQAdminExt.class);
        Mockito.when(adminClient.examineConsumeStats(GROUP, TOPIC))
                .thenThrow(
                        new MQClientException(
                                ResponseCode.CONSUMER_NOT_ONLINE,
                                "Not found the consumer group consume stats"));
        Mockito.when(adminClient.examineTopicRouteInfo(TOPIC)).thenReturn(new TopicRouteData());

        MessageQueue messageQueue = new MessageQueue(TOPIC, "broker-a", 0);

        Assertions.assertThrows(
                RocketMqConnectorException.class,
                () ->
                        RocketMqAdminUtil.currentOffsets(
                                adminClient,
                                GROUP,
                                Collections.singletonList(TOPIC),
                                Collections.singleton(messageQueue)));
    }

    /**
     * A successful lookup that matches none of the requested queues is the legitimate empty case,
     * for example a newly discovered queue that has no entry in the consume stats yet. It must
     * still return an empty map so the caller can cold-start that queue.
     */
    @Test
    void testCurrentOffsets_successfulLookupWithNoMatchingQueueReturnsEmpty() throws Exception {
        MessageQueue requestedQueue = new MessageQueue(TOPIC, "broker-a", 1);
        MessageQueue committedQueue = new MessageQueue(TOPIC, "broker-a", 0);

        OffsetWrapper offsetWrapper = new OffsetWrapper();
        offsetWrapper.setConsumerOffset(42L);
        Map<MessageQueue, OffsetWrapper> offsetTable = new HashMap<>();
        offsetTable.put(committedQueue, offsetWrapper);

        ConsumeStats consumeStats = new ConsumeStats();
        consumeStats.setOffsetTable((HashMap<MessageQueue, OffsetWrapper>) offsetTable);

        DefaultMQAdminExt adminClient = Mockito.mock(DefaultMQAdminExt.class);
        Mockito.when(adminClient.examineConsumeStats(GROUP, TOPIC)).thenReturn(consumeStats);

        Map<MessageQueue, Long> offsets =
                RocketMqAdminUtil.currentOffsets(
                        adminClient,
                        GROUP,
                        Collections.singletonList(TOPIC),
                        Collections.singleton(requestedQueue));

        Assertions.assertTrue(offsets.isEmpty());
    }

    /** A successful lookup that does match returns the committed offset unchanged. */
    @Test
    void testCurrentOffsets_successfulLookupReturnsCommittedOffset() throws Exception {
        MessageQueue messageQueue = new MessageQueue(TOPIC, "broker-a", 0);

        OffsetWrapper offsetWrapper = new OffsetWrapper();
        offsetWrapper.setConsumerOffset(42L);
        Map<MessageQueue, OffsetWrapper> offsetTable = new HashMap<>();
        offsetTable.put(messageQueue, offsetWrapper);

        ConsumeStats consumeStats = new ConsumeStats();
        consumeStats.setOffsetTable((HashMap<MessageQueue, OffsetWrapper>) offsetTable);

        DefaultMQAdminExt adminClient = Mockito.mock(DefaultMQAdminExt.class);
        Mockito.when(adminClient.examineConsumeStats(GROUP, TOPIC)).thenReturn(consumeStats);

        Map<MessageQueue, Long> offsets =
                RocketMqAdminUtil.currentOffsets(
                        adminClient,
                        GROUP,
                        Collections.singletonList(TOPIC),
                        Collections.singleton(messageQueue));

        Assertions.assertEquals(Collections.singletonMap(messageQueue, 42L), offsets);
    }

    /**
     * RocketMqSourceOptions.TOPICS accepts a comma separated list, so the lookup loops over several
     * topics and merges their consume stats. Pinning that here keeps the loop from being rewritten
     * into one that returns the first topic's offsets and drops the rest.
     */
    @Test
    void testCurrentOffsets_multipleTopicsAreMergedAcrossTheLoop() throws Exception {
        MessageQueue firstQueue = new MessageQueue(TOPIC, "broker-a", 0);
        MessageQueue secondQueue = new MessageQueue(OTHER_TOPIC, "broker-a", 0);

        DefaultMQAdminExt adminClient = Mockito.mock(DefaultMQAdminExt.class);
        Mockito.when(adminClient.examineConsumeStats(GROUP, TOPIC))
                .thenReturn(consumeStatsFor(firstQueue, 42L));
        Mockito.when(adminClient.examineConsumeStats(GROUP, OTHER_TOPIC))
                .thenReturn(consumeStatsFor(secondQueue, 7L));

        Map<MessageQueue, Long> offsets =
                RocketMqAdminUtil.currentOffsets(
                        adminClient,
                        GROUP,
                        Arrays.asList(TOPIC, OTHER_TOPIC),
                        new HashSet<>(Arrays.asList(firstQueue, secondQueue)));

        Map<MessageQueue, Long> expected = new HashMap<>();
        expected.put(firstQueue, 42L);
        expected.put(secondQueue, 7L);
        Assertions.assertEquals(expected, offsets);
    }

    private static ConsumeStats consumeStatsFor(MessageQueue messageQueue, long committedOffset) {
        OffsetWrapper offsetWrapper = new OffsetWrapper();
        offsetWrapper.setConsumerOffset(committedOffset);
        HashMap<MessageQueue, OffsetWrapper> offsetTable = new HashMap<>();
        offsetTable.put(messageQueue, offsetWrapper);

        ConsumeStats consumeStats = new ConsumeStats();
        consumeStats.setOffsetTable(offsetTable);
        return consumeStats;
    }
}
