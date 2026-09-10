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

/** Immutable runtime configuration for the Azure Queue Storage source. */
@Getter
@Builder
public class AzureQueueSourceConfig implements AzureQueueClientConfig, Serializable {

    static final int MAX_BATCH_SIZE = 32;
    static final int MAX_VISIBILITY_TIMEOUT_SECONDS = 7 * 24 * 60 * 60;

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
    private final int batchSize;
    private final int visibilityTimeoutSeconds;
    private final long pollIntervalMillis;
    private final int maxInFlightMessages;
    private final long operationTimeoutMillis;

    public static AzureQueueSourceConfig from(ReadonlyConfig config) {
        AzureQueueSourceConfig sourceConfig =
                AzureQueueSourceConfig.builder()
                        .queueName(config.get(AzureQueueStorageSourceOptions.QUEUE_NAME))
                        .authenticationType(
                                config.get(AzureQueueStorageSourceOptions.AUTHENTICATION_TYPE))
                        .connectionString(
                                config.get(AzureQueueStorageSourceOptions.CONNECTION_STRING))
                        .endpoint(config.get(AzureQueueStorageSourceOptions.ENDPOINT))
                        .accountName(config.get(AzureQueueStorageSourceOptions.ACCOUNT_NAME))
                        .accountKey(config.get(AzureQueueStorageSourceOptions.ACCOUNT_KEY))
                        .sasToken(config.get(AzureQueueStorageSourceOptions.SAS_TOKEN))
                        .format(config.get(AzureQueueStorageSourceOptions.FORMAT))
                        .fieldDelimiter(config.get(AzureQueueStorageSourceOptions.FIELD_DELIMITER))
                        .messageEncoding(
                                config.get(AzureQueueStorageSourceOptions.MESSAGE_ENCODING))
                        .batchSize(config.get(AzureQueueStorageSourceOptions.BATCH_SIZE))
                        .visibilityTimeoutSeconds(
                                config.get(
                                        AzureQueueStorageSourceOptions.VISIBILITY_TIMEOUT_SECONDS))
                        .pollIntervalMillis(
                                config.get(AzureQueueStorageSourceOptions.POLL_INTERVAL_MS))
                        .maxInFlightMessages(
                                config.get(AzureQueueStorageSourceOptions.MAX_IN_FLIGHT_MESSAGES))
                        .operationTimeoutMillis(
                                config.get(AzureQueueStorageSourceOptions.OPERATION_TIMEOUT_MS))
                        .build();
        sourceConfig.validate();
        return sourceConfig;
    }

    private void validate() {
        AzureQueueConfigValidator.validateClient(this);
        if (format == MessageFormat.TEXT && fieldDelimiter.isEmpty()) {
            throw new IllegalArgumentException("Option 'field_delimiter' cannot be empty");
        }
        if (batchSize < 1 || batchSize > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException("Option 'batch_size' must be between 1 and 32");
        }
        if (visibilityTimeoutSeconds < 1
                || visibilityTimeoutSeconds > MAX_VISIBILITY_TIMEOUT_SECONDS) {
            throw new IllegalArgumentException(
                    "Option 'visibility_timeout_seconds' must be between 1 and 604800");
        }
        if (pollIntervalMillis <= 0) {
            throw new IllegalArgumentException(
                    "Option 'poll_interval_ms' must be greater than zero");
        }
        if (maxInFlightMessages < batchSize) {
            throw new IllegalArgumentException(
                    "Option 'max_in_flight_messages' must be greater than or equal to batch_size");
        }
        if (operationTimeoutMillis <= 0) {
            throw new IllegalArgumentException(
                    "Option 'operation_timeout_ms' must be greater than zero");
        }
        long visibilityTimeoutMillis = visibilityTimeoutSeconds * 1_000L;
        if (operationTimeoutMillis >= visibilityTimeoutMillis / 2) {
            throw new IllegalArgumentException(
                    "Option 'operation_timeout_ms' must be less than half of visibility_timeout_seconds");
        }
    }
}
