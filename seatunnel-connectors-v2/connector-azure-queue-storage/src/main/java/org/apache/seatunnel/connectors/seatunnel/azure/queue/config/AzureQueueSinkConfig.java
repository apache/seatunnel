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

package org.apache.seatunnel.connectors.seatunnel.azure.queue.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;

@Getter
@Builder(toBuilder = true)
public class AzureQueueSinkConfig implements AzureQueueClientConfig, Serializable {

    private final String queueName;
    private final AuthenticationType authenticationType;
    private final String connectionString;
    private final String endpoint;
    private final String accountName;
    private final String accountKey;
    private final String sasToken;
    private final MessageFormat format;
    private final String fieldDelimiter;
    private final MessageEncoding messageEncoding;
    private final int maxInFlight;
    private final long operationTimeoutMillis;

    public static AzureQueueSinkConfig from(ReadonlyConfig config) {
        AzureQueueSinkConfig sinkConfig =
                AzureQueueSinkConfig.builder()
                        .queueName(config.get(AzureQueueStorageSinkOptions.QUEUE_NAME))
                        .authenticationType(
                                config.get(AzureQueueStorageSinkOptions.AUTHENTICATION_TYPE))
                        .connectionString(
                                config.get(AzureQueueStorageSinkOptions.CONNECTION_STRING))
                        .endpoint(config.get(AzureQueueStorageSinkOptions.ENDPOINT))
                        .accountName(config.get(AzureQueueStorageSinkOptions.ACCOUNT_NAME))
                        .accountKey(config.get(AzureQueueStorageSinkOptions.ACCOUNT_KEY))
                        .sasToken(config.get(AzureQueueStorageSinkOptions.SAS_TOKEN))
                        .format(config.get(AzureQueueStorageSinkOptions.FORMAT))
                        .fieldDelimiter(config.get(AzureQueueStorageSinkOptions.FIELD_DELIMITER))
                        .messageEncoding(config.get(AzureQueueStorageSinkOptions.MESSAGE_ENCODING))
                        .maxInFlight(config.get(AzureQueueStorageSinkOptions.MAX_IN_FLIGHT))
                        .operationTimeoutMillis(
                                config.get(AzureQueueStorageSinkOptions.OPERATION_TIMEOUT_MS))
                        .build();
        sinkConfig.validate();
        return sinkConfig;
    }

    private void validate() {
        AzureQueueConfigValidator.validateClient(this);

        if (format == MessageFormat.TEXT && fieldDelimiter.isEmpty()) {
            throw new IllegalArgumentException("Option 'field_delimiter' cannot be empty");
        }
        if (maxInFlight <= 0) {
            throw new IllegalArgumentException("Option 'max_in_flight' must be greater than zero");
        }
        if (operationTimeoutMillis <= 0) {
            throw new IllegalArgumentException(
                    "Option 'operation_timeout_ms' must be greater than zero");
        }
    }
}
