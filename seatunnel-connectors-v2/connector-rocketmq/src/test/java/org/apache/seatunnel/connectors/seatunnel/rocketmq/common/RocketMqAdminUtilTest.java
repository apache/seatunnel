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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class RocketMqAdminUtilTest {

    private static final String GROUP = "test-group";
    private static final String TOPIC = "test-topic";

    @Test
    void testCurrentOffsets_retryTopicMissingWhileRouteHealthyReturnsEmpty() throws Exception {
        RocketMqAdminUtil.RETRY_BACKOFF_MILLIS = 0L;
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
        // Verify no retries occurred since the route was healthy!
        Mockito.verify(adminClient, Mockito.times(1)).examineConsumeStats(GROUP, TOPIC);
    }

    @Test
    void testCurrentOffsets_routeOutageSurfacesAsFailureWithRetries() throws Exception {
        RocketMqAdminUtil.RETRY_BACKOFF_MILLIS = 0L;
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

        // Verify retries occurred since the business topic was also unresolvable
        Mockito.verify(adminClient, Mockito.times(4)).examineConsumeStats(GROUP, TOPIC);
    }

    @Test
    void testCurrentOffsets_otherResponseCodeSurfacesEvenWhenRouteHealthy() throws Exception {
        RocketMqAdminUtil.RETRY_BACKOFF_MILLIS = 0L;
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
        // No retries for CONSUMER_NOT_ONLINE
        Mockito.verify(adminClient, Mockito.times(1)).examineConsumeStats(GROUP, TOPIC);
    }

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
}
