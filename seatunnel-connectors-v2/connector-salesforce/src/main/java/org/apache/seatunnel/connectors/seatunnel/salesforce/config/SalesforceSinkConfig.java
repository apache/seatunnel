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

package org.apache.seatunnel.connectors.seatunnel.salesforce.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Getter;

import java.io.Serializable;
import java.net.URI;

@Getter
public final class SalesforceSinkConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    public static final String IDENTIFIER_PATTERN = "[A-Za-z][A-Za-z0-9_]*";

    private final SalesforceParameters parameters;
    private final String objectName;
    private final String externalIdField;
    private final int batchSize;
    private final int batchMaxBytes;
    private final int maxRetries;
    private final long retryIntervalMs;

    public SalesforceSinkConfig(ReadonlyConfig config) {
        parameters = new SalesforceParameters();
        parameters.buildWithConfig(config);
        requireText(parameters.getClientId(), "client_id");
        requireText(parameters.getClientSecret(), "client_secret");
        requireText(parameters.getUsername(), "username");
        requireText(parameters.getPassword(), "password");
        validateInstanceUrl(parameters.getInstanceUrl());
        if (!parameters.getApiVersion().matches("v[0-9]+\\.[0-9]+")) {
            throw new IllegalArgumentException("api_version must have the form v59.0");
        }
        if (parameters.getRequestTimeoutMs() <= 0) {
            throw new IllegalArgumentException("request_timeout_ms must be positive");
        }
        objectName = config.get(SalesforceSourceOptions.OBJECT_NAME);
        externalIdField = config.get(SalesforceSinkOptions.EXTERNAL_ID_FIELD);
        requireIdentifier(objectName, "object_name");
        requireIdentifier(externalIdField, "external_id_field");
        if ("Id".equalsIgnoreCase(externalIdField)) {
            throw new IllegalArgumentException(
                    "external_id_field must be an external ID, not the Salesforce Id");
        }
        batchSize = config.get(SalesforceSinkOptions.BATCH_SIZE);
        batchMaxBytes = config.get(SalesforceSinkOptions.BATCH_MAX_BYTES);
        maxRetries = config.get(SalesforceSinkOptions.MAX_RETRIES);
        retryIntervalMs = config.get(SalesforceSinkOptions.RETRY_INTERVAL_MS);
        requireRange(batchSize, 1, 200, "batch_size");
        requireRange(batchMaxBytes, 128, 8 * 1024 * 1024, "batch_max_bytes");
        requireRange(maxRetries, 0, 10, "max_retries");
        requireRange(retryIntervalMs, 0, 60000, "retry_interval_ms");
    }

    public static void requireIdentifier(String value, String option) {
        if (value == null || !value.matches(IDENTIFIER_PATTERN)) {
            throw new IllegalArgumentException(option + " must be a Salesforce API identifier");
        }
    }

    public static void validateInstanceUrl(String value) {
        try {
            URI uri = URI.create(value);
            if ((!"http".equalsIgnoreCase(uri.getScheme())
                            && !"https".equalsIgnoreCase(uri.getScheme()))
                    || uri.getHost() == null
                    || uri.getUserInfo() != null
                    || uri.getRawQuery() != null
                    || uri.getRawFragment() != null
                    || (uri.getPath() != null && !uri.getPath().isEmpty())) {
                throw new IllegalArgumentException();
            }
        } catch (RuntimeException invalid) {
            throw new IllegalArgumentException(
                    "instance_url must be an HTTP(S) origin without credentials, path, query or fragment");
        }
    }

    private static void requireText(String value, String option) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(option + " must not be blank");
        }
    }

    private static void requireRange(long value, long min, long max, String option) {
        if (value < min || value > max) {
            throw new IllegalArgumentException(option + " must be between " + min + " and " + max);
        }
    }
}
