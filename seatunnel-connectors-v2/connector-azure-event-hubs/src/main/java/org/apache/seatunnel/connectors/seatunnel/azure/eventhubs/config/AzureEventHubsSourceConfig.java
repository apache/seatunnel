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
package org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;

/** Immutable runtime configuration for the Azure Event Hubs source. */
@Getter
@Builder
public class AzureEventHubsSourceConfig implements Serializable {

    public static final long MAX_POLL_TIMEOUT_MS = 5_000L;

    private static final long serialVersionUID = 1L;

    private final String connectionString;
    private final String eventHubName;
    private final String consumerGroup;
    private final AzureEventHubsStartMode startMode;
    private final AzureEventHubsMessageFormat format;
    private final String fieldDelimiter;
    private final int maxBatchSize;
    private final long pollTimeoutMs;
    private final int prefetchCount;

    public static AzureEventHubsSourceConfig from(ReadonlyConfig config) {
        AzureEventHubsSourceConfig sourceConfig =
                AzureEventHubsSourceConfig.builder()
                        .connectionString(config.get(AzureEventHubsSourceOptions.CONNECTION_STRING))
                        .eventHubName(config.get(AzureEventHubsSourceOptions.EVENT_HUB_NAME))
                        .consumerGroup(config.get(AzureEventHubsSourceOptions.CONSUMER_GROUP))
                        .startMode(config.get(AzureEventHubsSourceOptions.START_MODE))
                        .format(config.get(AzureEventHubsSourceOptions.FORMAT))
                        .fieldDelimiter(config.get(AzureEventHubsSourceOptions.FIELD_DELIMITER))
                        .maxBatchSize(config.get(AzureEventHubsSourceOptions.MAX_BATCH_SIZE))
                        .pollTimeoutMs(config.get(AzureEventHubsSourceOptions.POLL_TIMEOUT_MS))
                        .prefetchCount(config.get(AzureEventHubsSourceOptions.PREFETCH_COUNT))
                        .build();
        sourceConfig.validate();
        return sourceConfig;
    }

    private void validate() {
        requireNonBlank(connectionString, AzureEventHubsSourceOptions.CONNECTION_STRING.key());
        requireNonBlank(eventHubName, AzureEventHubsSourceOptions.EVENT_HUB_NAME.key());
        requireNonBlank(consumerGroup, AzureEventHubsSourceOptions.CONSUMER_GROUP.key());
        if (connectionStringContainsEntityPath(connectionString)) {
            throw new IllegalArgumentException(
                    "Option 'connection_string' must not include EntityPath; configure 'event_hub_name' separately");
        }
        if (format == AzureEventHubsMessageFormat.TEXT && fieldDelimiter.isEmpty()) {
            throw new IllegalArgumentException("Option 'field_delimiter' cannot be empty");
        }
        if (maxBatchSize <= 0) {
            throw new IllegalArgumentException("Option 'max_batch_size' must be greater than zero");
        }
        if (pollTimeoutMs <= 0 || pollTimeoutMs > MAX_POLL_TIMEOUT_MS) {
            throw new IllegalArgumentException(
                    "Option 'poll_timeout_ms' must be between 1 and " + MAX_POLL_TIMEOUT_MS);
        }
        if (prefetchCount <= 0) {
            throw new IllegalArgumentException("Option 'prefetch_count' must be greater than zero");
        }
        if (prefetchCount < maxBatchSize) {
            throw new IllegalArgumentException(
                    "Option 'prefetch_count' must be greater than or equal to max_batch_size");
        }
    }

    private static boolean connectionStringContainsEntityPath(String connectionString) {
        for (String segment : connectionString.split(";")) {
            int separator = segment.indexOf('=');
            if (separator > 0
                    && "entitypath".equalsIgnoreCase(segment.substring(0, separator).trim())) {
                return true;
            }
        }
        return false;
    }

    private static void requireNonBlank(String value, String option) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Option '" + option + "' cannot be blank");
        }
    }
}
