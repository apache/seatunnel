/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.cursor.start;

import org.apache.seatunnel.connectors.seatunnel.pulsar.exception.PulsarConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.pulsar.exception.PulsarConnectorException;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.topic.TopicPartition;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;

public class SubscriptionStartCursor implements StartCursor {
    private static final long serialVersionUID = 1L;

    private final CursorResetStrategy cursorResetStrategy;

    public SubscriptionStartCursor() {
        this.cursorResetStrategy = CursorResetStrategy.LATEST;
    }

    public SubscriptionStartCursor(CursorResetStrategy cursorResetStrategy) {
        this.cursorResetStrategy = cursorResetStrategy;
    }

    public void ensureSubscription(String subscription, TopicPartition partition, PulsarAdmin pulsarAdmin) {
        try {
            if (pulsarAdmin.topics()
                .getSubscriptions(partition.getFullTopicName())
                .contains(subscription)) {
                return;
            }
            pulsarAdmin.topics().createSubscription(partition.getFullTopicName(), subscription, CursorResetStrategy.EARLIEST == cursorResetStrategy ? MessageId.earliest : MessageId.latest);
        } catch (PulsarAdminException e) {
            throw new PulsarConnectorException(PulsarConnectorErrorCode.OPEN_PULSAR_ADMIN_FAILED, e);
        }
    }

    @Override
    public void seekPosition(Consumer<?> consumer) throws PulsarClientException {
        // nothing
    }

    public enum CursorResetStrategy {
        LATEST, EARLIEST
    }
}
