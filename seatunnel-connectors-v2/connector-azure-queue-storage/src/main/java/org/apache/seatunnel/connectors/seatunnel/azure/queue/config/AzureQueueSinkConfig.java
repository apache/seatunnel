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
import java.util.regex.Pattern;

@Getter
@Builder(toBuilder = true)
public class AzureQueueSinkConfig implements Serializable {

    private static final Pattern QUEUE_NAME_PATTERN =
            Pattern.compile("[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])?");

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
                        .queueName(config.get(AzureQueueSinkOptions.QUEUE_NAME))
                        .authenticationType(config.get(AzureQueueSinkOptions.AUTHENTICATION_TYPE))
                        .connectionString(config.get(AzureQueueSinkOptions.CONNECTION_STRING))
                        .endpoint(config.get(AzureQueueSinkOptions.ENDPOINT))
                        .accountName(config.get(AzureQueueSinkOptions.ACCOUNT_NAME))
                        .accountKey(config.get(AzureQueueSinkOptions.ACCOUNT_KEY))
                        .sasToken(config.get(AzureQueueSinkOptions.SAS_TOKEN))
                        .format(config.get(AzureQueueSinkOptions.FORMAT))
                        .fieldDelimiter(config.get(AzureQueueSinkOptions.FIELD_DELIMITER))
                        .messageEncoding(config.get(AzureQueueSinkOptions.MESSAGE_ENCODING))
                        .maxInFlight(config.get(AzureQueueSinkOptions.MAX_IN_FLIGHT))
                        .operationTimeoutMillis(
                                config.get(AzureQueueSinkOptions.OPERATION_TIMEOUT_MS))
                        .build();
        sinkConfig.validate();
        return sinkConfig;
    }

    private void validate() {
        requireNonBlank(queueName, AzureQueueSinkOptions.QUEUE_NAME.key());
        if (queueName.length() < 3
                || queueName.length() > 63
                || !QUEUE_NAME_PATTERN.matcher(queueName).matches()
                || queueName.contains("--")) {
            throw new IllegalArgumentException(
                    "Option 'queue_name' must contain 3-63 lowercase letters, numbers or single hyphens");
        }
        if (authenticationType == null) {
            throw new IllegalArgumentException("Option 'authentication_type' is required");
        }

        switch (authenticationType) {
            case CONNECTION_STRING:
                requireNonBlank(connectionString, AzureQueueSinkOptions.CONNECTION_STRING.key());
                rejectPresent(
                        endpoint,
                        AzureQueueSinkOptions.ENDPOINT.key(),
                        accountName,
                        AzureQueueSinkOptions.ACCOUNT_NAME.key(),
                        accountKey,
                        AzureQueueSinkOptions.ACCOUNT_KEY.key(),
                        sasToken,
                        AzureQueueSinkOptions.SAS_TOKEN.key());
                break;
            case SHARED_KEY:
                requireNonBlank(endpoint, AzureQueueSinkOptions.ENDPOINT.key());
                requireNonBlank(accountName, AzureQueueSinkOptions.ACCOUNT_NAME.key());
                requireNonBlank(accountKey, AzureQueueSinkOptions.ACCOUNT_KEY.key());
                rejectPresent(
                        connectionString,
                        AzureQueueSinkOptions.CONNECTION_STRING.key(),
                        sasToken,
                        AzureQueueSinkOptions.SAS_TOKEN.key());
                break;
            case SAS_TOKEN:
                requireNonBlank(endpoint, AzureQueueSinkOptions.ENDPOINT.key());
                requireNonBlank(sasToken, AzureQueueSinkOptions.SAS_TOKEN.key());
                rejectPresent(
                        connectionString,
                        AzureQueueSinkOptions.CONNECTION_STRING.key(),
                        accountName,
                        AzureQueueSinkOptions.ACCOUNT_NAME.key(),
                        accountKey,
                        AzureQueueSinkOptions.ACCOUNT_KEY.key());
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported authentication_type: " + authenticationType);
        }

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

    private static void requireNonBlank(String value, String option) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Option '" + option + "' cannot be blank");
        }
    }

    private static void rejectPresent(Object... valuesAndOptions) {
        for (int index = 0; index < valuesAndOptions.length; index += 2) {
            Object value = valuesAndOptions[index];
            if (value != null) {
                throw new IllegalArgumentException(
                        "Option '"
                                + valuesAndOptions[index + 1]
                                + "' is not valid for the selected authentication_type");
            }
        }
    }
}
