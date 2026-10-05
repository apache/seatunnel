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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;

import lombok.Data;

import java.io.Serializable;

@Data
public class HugeGraphConnectionConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private String host;
    private int port;
    private String protocol;
    private String graphName;
    private String graphSpace;
    private String username;
    private String password;
    private int maxRetries;
    private int retryBackoffMs;
    private int retryBackoffMaxMs;

    public static HugeGraphConnectionConfig of(ReadonlyConfig config) {
        HugeGraphConnectionConfig connectionConfig = new HugeGraphConnectionConfig();
        connectionConfig.setHost(config.get(HugeGraphOptions.HOST));
        connectionConfig.setPort(config.get(HugeGraphOptions.PORT));
        connectionConfig.setProtocol(
                config.getOptional(HugeGraphOptions.PROTOCOL)
                        .orElse(HugeGraphOptions.PROTOCOL.defaultValue()));
        connectionConfig.setGraphName(config.get(HugeGraphOptions.GRAPH_NAME));
        connectionConfig.setGraphSpace(
                config.getOptional(HugeGraphOptions.GRAPH_SPACE)
                        .filter(graphSpace -> !graphSpace.isEmpty())
                        .orElse(HugeGraphOptions.GRAPH_SPACE.defaultValue()));
        config.getOptional(HugeGraphOptions.USERNAME).ifPresent(connectionConfig::setUsername);
        config.getOptional(HugeGraphOptions.PASSWORD).ifPresent(connectionConfig::setPassword);
        connectionConfig.setMaxRetries(
                config.getOptional(HugeGraphOptions.MAX_RETRIES)
                        .orElse(HugeGraphOptions.MAX_RETRIES.defaultValue()));
        connectionConfig.setRetryBackoffMs(
                config.getOptional(HugeGraphOptions.RETRY_BACKOFF_MS)
                        .orElse(HugeGraphOptions.RETRY_BACKOFF_MS.defaultValue()));
        connectionConfig.setRetryBackoffMaxMs(
                config.getOptional(HugeGraphOptions.RETRY_BACKOFF_MAX_MS)
                        .orElse(HugeGraphOptions.RETRY_BACKOFF_MAX_MS.defaultValue()));
        validate(connectionConfig);
        return connectionConfig;
    }

    private static void validate(HugeGraphConnectionConfig config) {
        // Fail fast at config-load with the offending option name, so the job stops before opening
        // a client that would otherwise surface a generic connection error much later.
        if (isBlank(config.getHost())) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    "Option 'host' must not be empty");
        }
        if (isBlank(config.getGraphName())) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    "Option 'graph_name' must not be empty");
        }
        // graph_space is not validated for emptiness: it carries a non-empty default ("DEFAULT"),
        // HugeGraphConnectionConfig.of() coalesces blank values to that default, and
        // HugeGraphClient additionally falls back to "DEFAULT" for any null — so it can never be
        // empty here.
        if (config.getPort() < 1 || config.getPort() > 65535) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    String.format(
                            "Option 'port' must be in range [1, 65535], but got %s",
                            config.getPort()));
        }
        if (!"http".equalsIgnoreCase(config.getProtocol())
                && !"https".equalsIgnoreCase(config.getProtocol())) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    String.format(
                            "Option 'protocol' must be 'http' or 'https', but got '%s'",
                            config.getProtocol()));
        }
        // Credentials must be paired — a lone username or lone password almost always indicates a
        // config typo and produces a confusing 401 downstream.
        boolean userSet = !isBlank(config.getUsername());
        boolean passwordSet = !isBlank(config.getPassword());
        if (userSet != passwordSet) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    "Options 'username' and 'password' must be set together");
        }
        if (config.getMaxRetries() < 0) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    "Option 'max_retries' must be greater than or equal to 0");
        }
        if (config.getRetryBackoffMs() < 0) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    "Option 'retry_backoff_ms' must be greater than or equal to 0");
        }
        if (config.getRetryBackoffMaxMs() < 0) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    "Option 'retry_backoff_max_ms' must be greater than or equal to 0");
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
