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

package org.apache.seatunnel.connectors.seatunnel.splunk.config;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.splunk.exception.SplunkConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.splunk.exception.SplunkConnectorException;

import lombok.Getter;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Resolved and validated configuration of the Splunk HEC sink.
 *
 * <p>Presence of the required options is enforced declaratively by the factory {@code OptionRule}.
 * This class carries the semantic validation that a rule cannot express, so a misconfigured job
 * fails during sink construction rather than on the first flush.
 */
@Getter
public class SplunkSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Path of the HEC endpoint that accepts events in the JSON event envelope format. */
    static final String EVENT_ENDPOINT_PATH = "/services/collector/event";

    /** Marker used to detect a URL that already points at a collector endpoint. */
    private static final String COLLECTOR_PATH_MARKER = "/services/collector";

    private final String endpoint;
    private final String token;
    private final String index;
    private final String source;
    private final String sourceType;
    private final String host;
    private final String hostField;
    private final String timeField;
    private final int maxBatchSize;
    private final int maxRetryCount;
    private final int retryBackoffMs;
    private final int connectTimeoutMs;
    private final int socketTimeoutMs;
    private final boolean tlsVerifyCertificate;
    private final boolean tlsVerifyHostname;

    public SplunkSinkConfig(ReadonlyConfig config) {
        this.endpoint = resolveEndpoint(config.get(SplunkSinkOptions.URL));
        this.token = requireToken(config.get(SplunkSinkOptions.TOKEN));
        this.index = config.get(SplunkSinkOptions.INDEX);
        this.source = config.get(SplunkSinkOptions.SOURCE);
        this.sourceType = config.get(SplunkSinkOptions.SOURCE_TYPE);
        this.host = config.get(SplunkSinkOptions.HOST);
        this.hostField = config.get(SplunkSinkOptions.HOST_FIELD);
        this.timeField = config.get(SplunkSinkOptions.TIME_FIELD);
        this.maxBatchSize =
                requirePositive(
                        config.get(SplunkSinkOptions.MAX_BATCH_SIZE),
                        SplunkSinkOptions.MAX_BATCH_SIZE.key());
        this.maxRetryCount =
                requirePositive(
                        config.get(SplunkSinkOptions.MAX_RETRY_COUNT),
                        SplunkSinkOptions.MAX_RETRY_COUNT.key());
        this.retryBackoffMs =
                requireNonNegative(
                        config.get(SplunkSinkOptions.RETRY_BACKOFF_MS),
                        SplunkSinkOptions.RETRY_BACKOFF_MS.key());
        this.connectTimeoutMs =
                requirePositive(
                        config.get(SplunkSinkOptions.CONNECT_TIMEOUT_MS),
                        SplunkSinkOptions.CONNECT_TIMEOUT_MS.key());
        this.socketTimeoutMs =
                requirePositive(
                        config.get(SplunkSinkOptions.SOCKET_TIMEOUT_MS),
                        SplunkSinkOptions.SOCKET_TIMEOUT_MS.key());
        this.tlsVerifyCertificate = config.get(SplunkSinkOptions.TLS_VERIFY_CERTIFICATE);
        this.tlsVerifyHostname = config.get(SplunkSinkOptions.TLS_VERIFY_HOSTNAME);
    }

    /**
     * Normalizes the configured address into a full HEC event endpoint.
     *
     * <p>Accepts both the collector base address and a full endpoint address so that operators can
     * paste either form out of the Splunk UI.
     */
    private static String resolveEndpoint(String url) {
        if (StringUtils.isBlank(url)) {
            throw new SplunkConnectorException(
                    SplunkConnectorErrorCode.INVALID_CONFIG,
                    String.format(
                            "Option '%s' is required and must not be blank. Configure the Splunk HTTP "
                                    + "Event Collector address, for example 'https://splunk-host:8088'.",
                            SplunkSinkOptions.URL.key()));
        }

        String trimmed = StringUtils.stripEnd(url.trim(), "/");
        URI uri;
        try {
            uri = new URI(trimmed);
        } catch (URISyntaxException e) {
            throw new SplunkConnectorException(
                    SplunkConnectorErrorCode.INVALID_CONFIG,
                    String.format(
                            "Option '%s' is not a valid URL: '%s'.",
                            SplunkSinkOptions.URL.key(), url),
                    e);
        }

        String scheme = uri.getScheme();
        if (scheme == null
                || !("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme))
                || uri.getHost() == null) {
            throw new SplunkConnectorException(
                    SplunkConnectorErrorCode.INVALID_CONFIG,
                    String.format(
                            "Option '%s' must be an absolute http or https URL including a host, but was '%s'. "
                                    + "For example 'https://splunk-host:8088'.",
                            SplunkSinkOptions.URL.key(), url));
        }

        if (trimmed.contains(COLLECTOR_PATH_MARKER)) {
            return trimmed;
        }
        return trimmed + EVENT_ENDPOINT_PATH;
    }

    private static String requireToken(String token) {
        if (StringUtils.isBlank(token)) {
            throw new SplunkConnectorException(
                    SplunkConnectorErrorCode.INVALID_CONFIG,
                    String.format(
                            "Option '%s' is required and must not be blank. Configure the Splunk HTTP "
                                    + "Event Collector token of the target collector.",
                            SplunkSinkOptions.TOKEN.key()));
        }
        return token.trim();
    }

    private static int requirePositive(int value, String key) {
        if (value < 1) {
            throw new SplunkConnectorException(
                    SplunkConnectorErrorCode.INVALID_CONFIG,
                    String.format("Option '%s' must be greater than 0, but was %d.", key, value));
        }
        return value;
    }

    private static int requireNonNegative(int value, String key) {
        if (value < 0) {
            throw new SplunkConnectorException(
                    SplunkConnectorErrorCode.INVALID_CONFIG,
                    String.format("Option '%s' must not be negative, but was %d.", key, value));
        }
        return value;
    }
}
