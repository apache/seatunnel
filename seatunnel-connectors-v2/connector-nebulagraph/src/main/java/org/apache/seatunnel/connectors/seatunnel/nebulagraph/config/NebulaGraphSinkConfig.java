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

package org.apache.seatunnel.connectors.seatunnel.nebulagraph.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.nebulagraph.exception.NebulaGraphConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.nebulagraph.exception.NebulaGraphConnectorException;

import com.vesoft.nebula.client.graph.data.HostAddress;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public final class NebulaGraphSinkConfig implements Serializable {

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private final List<HostAddress> hosts;
    private final String username;
    private final String password;
    private final String space;
    private final String tag;
    private final String vidField;
    private final List<String> writeFields;
    private final NebulaGraphWriteMode writeMode;
    private final int batchSize;
    private final int timeoutMillis;
    private final int maxRetries;
    private final int retryIntervalMillis;

    private NebulaGraphSinkConfig(ReadonlyConfig config) {
        this.hosts = parseHosts(config.get(NebulaGraphSinkOptions.HOSTS));
        this.username = requireNotBlank(config.get(NebulaGraphSinkOptions.USERNAME), "username");
        this.password = requireNotBlank(config.get(NebulaGraphSinkOptions.PASSWORD), "password");
        this.space = requireIdentifier(config.get(NebulaGraphSinkOptions.SPACE), "space");
        this.tag = requireIdentifier(config.get(NebulaGraphSinkOptions.TAG), "tag");
        this.vidField = requireNotBlank(config.get(NebulaGraphSinkOptions.VID_FIELD), "vid_field");
        this.writeFields =
                parseWriteFields(config.getOptional(NebulaGraphSinkOptions.WRITE_FIELDS));
        this.writeMode = config.get(NebulaGraphSinkOptions.WRITE_MODE);
        this.batchSize =
                requirePositive(config.get(NebulaGraphSinkOptions.BATCH_SIZE), "batch_size");
        this.timeoutMillis =
                requirePositive(
                        config.get(NebulaGraphSinkOptions.TIMEOUT_MILLIS), "timeout_millis");
        this.maxRetries =
                requireNonNegative(config.get(NebulaGraphSinkOptions.MAX_RETRIES), "max_retries");
        this.retryIntervalMillis =
                requireNonNegative(
                        config.get(NebulaGraphSinkOptions.RETRY_INTERVAL_MILLIS),
                        "retry_interval_millis");
    }

    public static NebulaGraphSinkConfig of(ReadonlyConfig config) {
        return new NebulaGraphSinkConfig(config);
    }

    private static List<HostAddress> parseHosts(List<String> values) {
        if (values == null || values.isEmpty()) {
            throw invalid("Option 'hosts' must contain at least one graphd address.");
        }
        List<HostAddress> result = new ArrayList<>(values.size());
        for (String value : values) {
            result.add(parseHost(value));
        }
        return Collections.unmodifiableList(result);
    }

    static HostAddress parseHost(String value) {
        String address = requireNotBlank(value, "hosts").trim();
        String host;
        String portText;
        if (address.startsWith("[")) {
            int bracket = address.indexOf(']');
            if (bracket <= 1
                    || bracket + 1 >= address.length()
                    || address.charAt(bracket + 1) != ':') {
                throw invalid("Invalid graphd address '" + value + "'. Expected [ipv6]:port.");
            }
            host = address.substring(1, bracket);
            portText = address.substring(bracket + 2);
        } else {
            int separator = address.lastIndexOf(':');
            if (separator <= 0
                    || separator == address.length() - 1
                    || address.indexOf(':') != separator) {
                throw invalid("Invalid graphd address '" + value + "'. Expected host:port.");
            }
            host = address.substring(0, separator).trim();
            portText = address.substring(separator + 1).trim();
        }

        int port;
        try {
            port = Integer.parseInt(portText);
        } catch (NumberFormatException e) {
            throw invalid("Invalid port in graphd address '" + value + "'.", e);
        }
        if (host.isEmpty() || port < 1 || port > 65535) {
            throw invalid("Invalid graphd address '" + value + "'. Port must be in [1, 65535].");
        }
        return new HostAddress(host, port);
    }

    private static List<String> parseWriteFields(java.util.Optional<List<String>> optional) {
        if (!optional.isPresent()) {
            return Collections.emptyList();
        }
        List<String> values = optional.get();
        if (values.isEmpty()) {
            throw invalid("Option 'write_fields' must not be empty when it is configured.");
        }
        Set<String> seen = new HashSet<>();
        List<String> result = new ArrayList<>(values.size());
        for (String value : values) {
            String field = requireIdentifier(value, "write_fields");
            if (!seen.add(field)) {
                throw invalid("Option 'write_fields' contains duplicate field '" + field + "'.");
            }
            result.add(field);
        }
        return Collections.unmodifiableList(result);
    }

    private static String requireIdentifier(String value, String option) {
        String identifier = requireNotBlank(value, option);
        if (!IDENTIFIER.matcher(identifier).matches()) {
            throw invalid(
                    "Option '"
                            + option
                            + "' must use a simple NebulaGraph identifier containing letters, digits, or underscores: "
                            + identifier);
        }
        return identifier;
    }

    private static String requireNotBlank(String value, String option) {
        if (value == null || value.trim().isEmpty()) {
            throw invalid("Option '" + option + "' must not be blank.");
        }
        return value.trim();
    }

    private static int requirePositive(int value, String option) {
        if (value < 1) {
            throw invalid("Option '" + option + "' must be greater than 0, but was " + value + ".");
        }
        return value;
    }

    private static int requireNonNegative(int value, String option) {
        if (value < 0) {
            throw invalid("Option '" + option + "' must be at least 0, but was " + value + ".");
        }
        return value;
    }

    private static NebulaGraphConnectorException invalid(String message) {
        return new NebulaGraphConnectorException(
                NebulaGraphConnectorErrorCode.INVALID_CONFIG, message);
    }

    private static NebulaGraphConnectorException invalid(String message, Throwable cause) {
        return new NebulaGraphConnectorException(
                NebulaGraphConnectorErrorCode.INVALID_CONFIG, message, cause);
    }

    public List<HostAddress> getHosts() {
        return hosts;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getSpace() {
        return space;
    }

    public String getTag() {
        return tag;
    }

    public String getVidField() {
        return vidField;
    }

    public List<String> getWriteFields() {
        return writeFields;
    }

    public NebulaGraphWriteMode getWriteMode() {
        return writeMode;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getTimeoutMillis() {
        return timeoutMillis;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public int getRetryIntervalMillis() {
        return retryIntervalMillis;
    }
}
