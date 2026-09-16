package org.apache.seatunnel.connectors.seatunnel.rocketmq.common;

import org.apache.seatunnel.connectors.seatunnel.rocketmq.exception.RocketMqConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.exception.RocketMqConnectorException;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class RocketMqAdminUtilTest {

    @Test
    public void testCurrentOffsetsThrowsWhenRouteInfoNotFound() throws Exception {
        RocketMqAdminUtil.RETRY_BACKOFF_MILLIS = 0L;
        RocketMqBaseConfiguration config = new RocketMqBaseConfiguration();
        config.setGroupId("test-group");
        config.setNamesrvAddr("127.0.0.1:9876");

        String topic = "test-topic";
        Set<MessageQueue> queues = new HashSet<>();
        queues.add(new MessageQueue(topic, "broker-a", 0));

        DefaultMQAdminExt adminExt = Mockito.mock(DefaultMQAdminExt.class);
        Mockito.when(adminExt.examineConsumeStats(config.getGroupId(), topic))
                .thenThrow(
                        new MQClientException(ResponseCode.TOPIC_NOT_EXIST, "No topic route info"));

        try (MockedStatic<RocketMqAdminUtil> mockedStatic =
                Mockito.mockStatic(RocketMqAdminUtil.class, Mockito.CALLS_REAL_METHODS)) {
            mockedStatic
                    .when(() -> RocketMqAdminUtil.startMQAdminTool(config))
                    .thenReturn(adminExt);

            RocketMqConnectorException exception =
                    Assertions.assertThrows(
                            RocketMqConnectorException.class,
                            () ->
                                    RocketMqAdminUtil.currentOffsets(
                                            config, Collections.singletonList(topic), queues));

            Assertions.assertEquals(
                    RocketMqConnectorErrorCode.GET_CONSUMER_GROUP_OFFSETS_ERROR,
                    exception.getErrorCode());
            Assertions.assertTrue(
                    exception
                            .getMessage()
                            .contains("because route info for the group could not be resolved"));

            // Verify it was retried maxRetries (3) times + 1 initial = 4 total attempts
            Mockito.verify(adminExt, Mockito.times(4))
                    .examineConsumeStats(config.getGroupId(), topic);
        }
    }
}
