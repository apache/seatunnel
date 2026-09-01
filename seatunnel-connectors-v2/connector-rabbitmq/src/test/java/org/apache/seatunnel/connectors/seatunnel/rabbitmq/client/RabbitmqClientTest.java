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

package org.apache.seatunnel.connectors.seatunnel.rabbitmq.client;

import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception.RabbitmqConnectorException;

import org.junit.jupiter.api.Test;

import com.rabbitmq.client.Channel;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RabbitmqClientTest {

    @Test
    void declaresExistingQueuePassivelyWhenConfigured() throws Exception {
        Channel channel = mock(Channel.class);
        RabbitmqConfig config = mock(RabbitmqConfig.class);
        when(config.isPassive()).thenReturn(true);

        RabbitmqClient.declareQueue(channel, config, "existing-queue");

        verify(channel).queueDeclarePassive("existing-queue");
        verify(channel, never()).queueDeclare("existing-queue", true, false, false, null);
    }

    @Test
    void declaresQueueWithConfiguredPropertiesByDefault() throws Exception {
        Channel channel = mock(Channel.class);
        RabbitmqConfig config = mock(RabbitmqConfig.class);
        when(config.isPassive()).thenReturn(false);
        when(config.getDurable()).thenReturn(true);
        when(config.getExclusive()).thenReturn(false);
        when(config.getAutoDelete()).thenReturn(false);

        RabbitmqClient.declareQueue(channel, config, "new-queue");

        verify(channel).queueDeclare("new-queue", true, false, false, null);
        verify(channel, never()).queueDeclarePassive("new-queue");
    }

    @Test
    void explainsPassiveQueueDeclarationFailure() throws Exception {
        Channel channel = mock(Channel.class);
        RabbitmqConfig config = mock(RabbitmqConfig.class);
        when(config.isPassive()).thenReturn(true);
        doThrow(new IOException("queue not found"))
                .when(channel)
                .queueDeclarePassive("missing-queue");

        RabbitmqConnectorException exception =
                assertThrows(
                        RabbitmqConnectorException.class,
                        () -> RabbitmqClient.declareQueue(channel, config, "missing-queue"));

        assertTrue(exception.getMessage().contains("missing-queue"));
        assertTrue(exception.getMessage().contains("passive=false"));
    }
}
