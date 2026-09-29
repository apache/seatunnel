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

package org.apache.seatunnel.connectors.seatunnel.azure.queue.source;

import org.apache.seatunnel.connectors.seatunnel.azure.queue.client.AzureQueueClientFactory;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.config.AzureQueueSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.exception.AzureQueueConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.exception.AzureQueueConnectorException;

import com.azure.core.util.Context;
import com.azure.storage.queue.QueueClient;
import com.azure.storage.queue.models.QueueMessageItem;
import com.azure.storage.queue.models.UpdateMessageResult;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

final class AzureQueueStorageReceiver implements AzureQueueReceiver {

    private final QueueClient queueClient;
    private final Duration visibilityTimeout;
    private final Duration operationTimeout;

    private AzureQueueStorageReceiver(
            QueueClient queueClient, Duration visibilityTimeout, Duration operationTimeout) {
        this.queueClient = queueClient;
        this.visibilityTimeout = visibilityTimeout;
        this.operationTimeout = operationTimeout;
    }

    static AzureQueueReceiver create(AzureQueueSourceConfig config) {
        try {
            return new AzureQueueStorageReceiver(
                    AzureQueueClientFactory.builder(config).buildClient(),
                    Duration.ofSeconds(config.getVisibilityTimeoutSeconds()),
                    Duration.ofMillis(config.getOperationTimeoutMillis()));
        } catch (Exception e) {
            throw new AzureQueueConnectorException(
                    AzureQueueConnectorErrorCode.CONNECTION_FAILED,
                    "Failed to create Azure Queue Storage client for queue "
                            + config.getQueueName(),
                    e);
        }
    }

    @Override
    public List<AzureQueueMessage> receive(int maxMessages) {
        List<AzureQueueMessage> messages = new ArrayList<>(maxMessages);
        for (QueueMessageItem message :
                queueClient.receiveMessages(
                        maxMessages, visibilityTimeout, operationTimeout, Context.NONE)) {
            messages.add(
                    new AzureQueueMessage(
                            message.getMessageId(),
                            message.getPopReceipt(),
                            message.getBody().toString(),
                            message.getBody().toBytes()));
        }
        return messages;
    }

    @Override
    public void renewVisibility(AzureQueueMessage message) {
        UpdateMessageResult result =
                queueClient
                        .updateMessageWithResponse(
                                message.getMessageId(),
                                message.getPopReceipt(),
                                message.getMessageText(),
                                visibilityTimeout,
                                operationTimeout,
                                Context.NONE)
                        .getValue();
        message.updatePopReceipt(result.getPopReceipt());
    }

    @Override
    public void delete(AzureQueueMessage message) {
        queueClient.deleteMessageWithResponse(
                message.getMessageId(), message.getPopReceipt(), operationTimeout, Context.NONE);
        message.markDeleted();
    }

    @Override
    public void release(AzureQueueMessage message) {
        UpdateMessageResult result =
                queueClient
                        .updateMessageWithResponse(
                                message.getMessageId(),
                                message.getPopReceipt(),
                                message.getMessageText(),
                                Duration.ZERO,
                                operationTimeout,
                                Context.NONE)
                        .getValue();
        message.updatePopReceipt(result.getPopReceipt());
    }
}
