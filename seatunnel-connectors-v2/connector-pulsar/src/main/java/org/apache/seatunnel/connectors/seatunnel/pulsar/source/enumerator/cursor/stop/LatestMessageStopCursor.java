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

package org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.cursor.stop;

import org.apache.seatunnel.connectors.seatunnel.pulsar.exception.PulsarConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.pulsar.exception.PulsarConnectorException;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.topic.TopicPartition;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;

/**
 * A stop cursor that initialize the position to the latest message id. The offsets initialization
 * are taken care of by the {@code PulsarPartitionSplitReaderBase} instead of by the {@code
 * PulsarSourceEnumerator}.
 */
public class LatestMessageStopCursor implements StopCursor {
    private static final long serialVersionUID = 1L;

    private MessageId messageId;

    public void prepare(PulsarAdmin admin, TopicPartition partition) {
        if (messageId == null) {
            String topic = partition.getFullTopicName();
            try {
                messageId = admin.topics().getLastMessageId(topic);
            } catch (PulsarAdminException e) {
                throw new PulsarConnectorException(PulsarConnectorErrorCode.GET_LAST_CURSOR_FAILED, "Failed to get the last cursor", e);
            }
        }
    }

    @Override
    public boolean shouldStop(Message<?> message) {
        MessageId id = message.getMessageId();
        return id.compareTo(messageId) >= 0;
    }

    @Override
    public StopCursor copy() {
        return new LatestMessageStopCursor();
    }
}
