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
import org.apache.seatunnel.api.configuration.util.ConditionExtension;
import org.apache.seatunnel.api.configuration.util.Conditions;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;

import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config.AzureEventHubsSourceOptions.CONNECTION_STRING;
import static org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config.AzureEventHubsSourceOptions.CONSUMER_GROUP;
import static org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config.AzureEventHubsSourceOptions.EVENT_HUB_NAME;
import static org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config.AzureEventHubsSourceOptions.FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config.AzureEventHubsSourceOptions.FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config.AzureEventHubsSourceOptions.MAX_BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config.AzureEventHubsSourceOptions.POLL_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config.AzureEventHubsSourceOptions.PREFETCH_COUNT;
import static org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config.AzureEventHubsSourceOptions.START_MODE;

/** Immutable runtime configuration for the Azure Event Hubs source. */
@Getter
@Builder
public class AzureEventHubsSourceConfig implements Serializable {

    public static final long MAX_POLL_TIMEOUT_MS = 5_000L;
    // Mirrors EventHubClientBuilder.prefetchCount's inclusive SDK 5.21.3 bound; recheck on upgrade.
    public static final int MAX_PREFETCH_COUNT = 8_000;

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
        // Optional cross-field Conditions require both keys, even when an option has a default.
        Map<String, Object> validationOptions = new HashMap<>(config.getSourceMap());
        validationOptions.put(MAX_BATCH_SIZE.key(), config.get(MAX_BATCH_SIZE));
        validationOptions.put(PREFETCH_COUNT.key(), config.get(PREFETCH_COUNT));
        ConfigValidator.of(ReadonlyConfig.fromMap(validationOptions))
                .validate(optionRuleBuilder().build());
        return AzureEventHubsSourceConfig.builder()
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
    }

    /** Shared connector rules; the factory additionally requires a schema. */
    public static OptionRule.Builder optionRuleBuilder() {
        return OptionRule.builder()
                .required(
                        CONNECTION_STRING,
                        Conditions.notBlank(CONNECTION_STRING)
                                .and(
                                        Conditions.extension(
                                                CONNECTION_STRING,
                                                new ConditionExtension<String>() {
                                                    @Override
                                                    public String description() {
                                                        return "must not include EntityPath; configure 'event_hub_name' separately";
                                                    }

                                                    @Override
                                                    public boolean evaluate(
                                                            ReadonlyConfig config, String value) {
                                                        return !connectionStringContainsEntityPath(
                                                                value);
                                                    }
                                                })))
                .required(EVENT_HUB_NAME, Conditions.notBlank(EVENT_HUB_NAME))
                .optional(CONSUMER_GROUP, Conditions.notBlank(CONSUMER_GROUP))
                .optional(START_MODE, FORMAT, FIELD_DELIMITER)
                .conditional(
                        FORMAT,
                        AzureEventHubsMessageFormat.TEXT,
                        Conditions.matches(FIELD_DELIMITER, "(?s).+"))
                .optional(MAX_BATCH_SIZE, Conditions.greaterThan(MAX_BATCH_SIZE, 0))
                .optional(
                        POLL_TIMEOUT_MS,
                        Conditions.greaterThan(POLL_TIMEOUT_MS, 0L)
                                .and(Conditions.lessOrEqual(POLL_TIMEOUT_MS, MAX_POLL_TIMEOUT_MS)))
                .optional(
                        PREFETCH_COUNT,
                        Conditions.greaterThan(PREFETCH_COUNT, 0)
                                .and(Conditions.lessOrEqual(PREFETCH_COUNT, MAX_PREFETCH_COUNT)),
                        Conditions.greaterOrEqualField(PREFETCH_COUNT, MAX_BATCH_SIZE));
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
}
