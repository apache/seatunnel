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

package org.apache.seatunnel.connectors.seatunnel.azure.queue.client;

import org.apache.seatunnel.connectors.seatunnel.azure.queue.config.AuthenticationType;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.config.AzureQueueClientConfig;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.config.MessageEncoding;

import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.queue.QueueClientBuilder;
import com.azure.storage.queue.QueueMessageEncoding;

/** Builds Azure Queue clients with the connector's shared authentication contract. */
public final class AzureQueueClientFactory {

    private AzureQueueClientFactory() {}

    public static QueueClientBuilder builder(AzureQueueClientConfig config) {
        QueueClientBuilder builder =
                new QueueClientBuilder()
                        .queueName(config.getQueueName())
                        .messageEncoding(toAzureEncoding(config.getMessageEncoding()));

        AuthenticationType authenticationType = config.getAuthenticationType();
        switch (authenticationType) {
            case CONNECTION_STRING:
                return builder.connectionString(config.getConnectionString());
            case SHARED_KEY:
                return builder.endpoint(config.getEndpoint())
                        .credential(
                                new StorageSharedKeyCredential(
                                        config.getAccountName(), config.getAccountKey()));
            case SAS_TOKEN:
                return builder.endpoint(config.getEndpoint())
                        .sasToken(normalizeSasToken(config.getSasToken()));
            default:
                throw new IllegalArgumentException(
                        "Unsupported authentication type: " + authenticationType);
        }
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
