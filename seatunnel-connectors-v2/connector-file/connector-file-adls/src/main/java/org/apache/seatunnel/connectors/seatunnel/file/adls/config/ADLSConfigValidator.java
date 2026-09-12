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

package org.apache.seatunnel.connectors.seatunnel.file.adls.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

final class ADLSConfigValidator {
    private static final Pattern ACCOUNT = Pattern.compile("[a-z0-9]{3,24}");
    private static final Pattern CONTAINER =
            Pattern.compile("[a-z0-9](?:[a-z0-9]|-(?!-)){1,61}[a-z0-9]");
    private static final Pattern ENDPOINT =
            Pattern.compile("[a-zA-Z0-9](?:[a-zA-Z0-9.-]*[a-zA-Z0-9])?");

    private ADLSConfigValidator() {}

    static void validate(ReadonlyConfig config) {
        String account = required(config, ADLSFileBaseOptions.ACCOUNT_NAME);
        String container = required(config, ADLSFileBaseOptions.CONTAINER);
        String endpoint = required(config, ADLSFileBaseOptions.ENDPOINT_SUFFIX);
        if (!ACCOUNT.matcher(account).matches()) {
            throw new IllegalArgumentException(
                    "'account_name' must contain 3-24 lowercase letters or digits");
        }
        if (!CONTAINER.matcher(container).matches()) {
            throw new IllegalArgumentException(
                    "'container' must be a valid 3-63 character Azure container name");
        }
        if (!ENDPOINT.matcher(endpoint).matches()) {
            throw new IllegalArgumentException("'endpoint_suffix' must be a DNS suffix");
        }

        ADLSFileBaseOptions.AuthType authType = config.get(ADLSFileBaseOptions.AUTH_TYPE);
        if (authType == ADLSFileBaseOptions.AuthType.SHARED_KEY) {
            required(config, ADLSFileBaseOptions.ACCOUNT_KEY);
            rejectPresent(config, ADLSFileBaseOptions.TENANT_ID);
            rejectPresent(config, ADLSFileBaseOptions.CLIENT_ID);
            rejectPresent(config, ADLSFileBaseOptions.CLIENT_SECRET);
        } else {
            required(config, ADLSFileBaseOptions.TENANT_ID);
            required(config, ADLSFileBaseOptions.CLIENT_ID);
            required(config, ADLSFileBaseOptions.CLIENT_SECRET);
            required(config, ADLSFileBaseOptions.AUTHORITY_HOST);
            rejectPresent(config, ADLSFileBaseOptions.ACCOUNT_KEY);
        }

        config.getOptional(ADLSFileBaseOptions.HADOOP_PROPERTIES)
                .ifPresent(ADLSConfigValidator::validateAdvancedProperties);
    }

    private static void validateAdvancedProperties(Map<String, String> properties) {
        properties.forEach(
                (key, value) -> {
                    if (key == null || key.trim().isEmpty() || value == null) {
                        throw new IllegalArgumentException(
                                "'hadoop_adls_properties' cannot contain blank keys or null values");
                    }
                    String normalized = key.toLowerCase(Locale.ROOT);
                    if (normalized.equals("fs.defaultfs")
                            || normalized.startsWith("fs.abfs")
                            || normalized.startsWith("fs.azure.account.auth.type")
                            || normalized.startsWith("fs.azure.account.key")
                            || normalized.startsWith("fs.azure.account.oauth")
                            || normalized.startsWith("fs.s3")) {
                        throw new IllegalArgumentException(
                                "'hadoop_adls_properties' cannot override connector-owned key '"
                                        + key
                                        + "'");
                    }
                });
    }

    private static String required(ReadonlyConfig config, Option<String> option) {
        String value = config.get(option);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("'" + option.key() + "' must not be blank");
        }
        return value.trim();
    }

    private static void rejectPresent(ReadonlyConfig config, Option<String> option) {
        if (config.getOptional(option).filter(value -> !value.trim().isEmpty()).isPresent()) {
            throw new IllegalArgumentException(
                    "'" + option.key() + "' is not valid for the selected 'auth_type'");
        }
    }
}
