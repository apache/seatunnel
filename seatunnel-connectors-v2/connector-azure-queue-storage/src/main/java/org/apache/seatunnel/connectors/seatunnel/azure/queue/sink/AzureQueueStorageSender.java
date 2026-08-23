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

package org.apache.seatunnel.connectors.seatunnel.azure.queue.sink;

import org.apache.seatunnel.connectors.seatunnel.azure.queue.config.AuthenticationType;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.config.AzureQueueSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.config.MessageEncoding;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.exception.AzureQueueConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.exception.AzureQueueConnectorException;

import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.queue.QueueAsyncClient;
import com.azure.storage.queue.QueueClientBuilder;
import com.azure.storage.queue.QueueMessageEncoding;

import java.util.concurrent.CompletableFuture;

class AzureQueueStorageSender implements AzureQueueSender {

    private final QueueAsyncClient queueClient;

    private AzureQueueStorageSender(QueueAsyncClient queueClient) {
        this.queueClient = queueClient;
    }

    static AzureQueueSender create(AzureQueueSinkConfig config) {
        try {
            QueueClientBuilder builder =
                    new QueueClientBuilder()
                            .queueName(config.getQueueName())
                            .messageEncoding(toAzureEncoding(config.getMessageEncoding()));

            AuthenticationType authenticationType = config.getAuthenticationType();
            switch (authenticationType) {
                case CONNECTION_STRING:
                    builder.connectionString(config.getConnectionString());
                    break;
                case SHARED_KEY:
                    builder.endpoint(config.getEndpoint())
                            .credential(
                                    new StorageSharedKeyCredential(
                                            config.getAccountName(), config.getAccountKey()));
                    break;
                case SAS_TOKEN:
                    builder.endpoint(config.getEndpoint())
                            .sasToken(normalizeSasToken(config.getSasToken()));
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Unsupported authentication type: " + authenticationType);
            }
            return new AzureQueueStorageSender(builder.buildAsyncClient());
        } catch (Exception e) {
            throw new AzureQueueConnectorException(
                    AzureQueueConnectorErrorCode.CONNECTION_FAILED,
                    "Failed to create Azure Queue Storage client for queue "
                            + config.getQueueName(),
                    e);
        }
    }

    @Override
    public CompletableFuture<Void> send(String message) {
        return queueClient.sendMessage(message).then().toFuture();
    }

    @Override
    public void close() {
        // QueueAsyncClient has no close contract; its Reactor resources are process-wide.
    }

    private static QueueMessageEncoding toAzureEncoding(MessageEncoding messageEncoding) {
        return messageEncoding == MessageEncoding.BASE64
                ? QueueMessageEncoding.BASE64
                : QueueMessageEncoding.NONE;
    }

    private static String normalizeSasToken(String sasToken) {
        return sasToken.startsWith("?") ? sasToken.substring(1) : sasToken;
    }
}
