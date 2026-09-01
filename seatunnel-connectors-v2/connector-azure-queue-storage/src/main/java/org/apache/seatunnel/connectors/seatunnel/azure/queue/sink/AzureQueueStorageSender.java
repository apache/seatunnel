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

import org.apache.seatunnel.connectors.seatunnel.azure.queue.client.AzureQueueClientFactory;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.config.AzureQueueSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.exception.AzureQueueConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.exception.AzureQueueConnectorException;

import com.azure.storage.queue.QueueAsyncClient;

import java.util.concurrent.CompletableFuture;

class AzureQueueStorageSender implements AzureQueueSender {

    private final QueueAsyncClient queueClient;

    private AzureQueueStorageSender(QueueAsyncClient queueClient) {
        this.queueClient = queueClient;
    }

    static AzureQueueSender create(AzureQueueSinkConfig config) {
        try {
            return new AzureQueueStorageSender(
                    AzureQueueClientFactory.builder(config).buildAsyncClient());
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
}
