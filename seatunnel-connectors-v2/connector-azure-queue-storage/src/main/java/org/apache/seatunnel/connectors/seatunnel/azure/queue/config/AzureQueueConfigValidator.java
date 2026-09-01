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

import java.util.regex.Pattern;

final class AzureQueueConfigValidator {

    private static final Pattern QUEUE_NAME_PATTERN =
            Pattern.compile("[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])?");

    private AzureQueueConfigValidator() {}

    static void validateClient(AzureQueueClientConfig config) {
        requireNonBlank(config.getQueueName(), "queue_name");
        if (config.getQueueName().length() < 3
                || config.getQueueName().length() > 63
                || !QUEUE_NAME_PATTERN.matcher(config.getQueueName()).matches()
                || config.getQueueName().contains("--")) {
            throw new IllegalArgumentException(
                    "Option 'queue_name' must contain 3-63 lowercase letters, numbers or single hyphens");
        }
        if (config.getAuthenticationType() == null) {
            throw new IllegalArgumentException("Option 'authentication_type' is required");
        }

        switch (config.getAuthenticationType()) {
            case CONNECTION_STRING:
                requireNonBlank(config.getConnectionString(), "connection_string");
                rejectPresent(
                        config.getEndpoint(),
                        "endpoint",
                        config.getAccountName(),
                        "account_name",
                        config.getAccountKey(),
                        "account_key",
                        config.getSasToken(),
                        "sas_token");
                break;
            case SHARED_KEY:
                requireNonBlank(config.getEndpoint(), "endpoint");
                requireNonBlank(config.getAccountName(), "account_name");
                requireNonBlank(config.getAccountKey(), "account_key");
                rejectPresent(
                        config.getConnectionString(),
                        "connection_string",
                        config.getSasToken(),
                        "sas_token");
                break;
            case SAS_TOKEN:
                requireNonBlank(config.getEndpoint(), "endpoint");
                requireNonBlank(config.getSasToken(), "sas_token");
                rejectPresent(
                        config.getConnectionString(),
                        "connection_string",
                        config.getAccountName(),
                        "account_name",
                        config.getAccountKey(),
                        "account_key");
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported authentication_type: " + config.getAuthenticationType());
        }
    }

    static void requireNonBlank(String value, String option) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Option '" + option + "' cannot be blank");
        }
    }

    private static void rejectPresent(Object... valuesAndOptions) {
        for (int index = 0; index < valuesAndOptions.length; index += 2) {
            if (valuesAndOptions[index] != null) {
                throw new IllegalArgumentException(
                        "Option '"
                                + valuesAndOptions[index + 1]
                                + "' is not valid for the selected authentication_type");
            }
        }
    }
}
