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

package org.apache.seatunnel.connectors.seatunnel.websocket.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.websocket.exception.WebSocketConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.websocket.exception.WebSocketConnectorException;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Runtime view of the WebSocket source configuration, shared between the source and the reader. */
@Data
public class WebSocketSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Replaces the value of every query parameter, the names are kept. */
    private static final String MASKED_VALUE = "***";

    private static final String SCHEME_SEPARATOR = "://";
    private static final String PARAMETER_SEPARATOR = "&";
    private static final char QUERY_SEPARATOR = '?';
    private static final char VALUE_SEPARATOR = '=';

    // the url, the headers and the open messages routinely carry credentials, keep them out of the
    // generated toString so that logging the config object can never leak them
    @ToString.Exclude private String url;

    /**
     * The only form of the url that may be logged: userinfo is removed and every query parameter
     * keeps its name but loses its value, because endpoints of this kind commonly authenticate
     * through a token in the query string.
     */
    private String maskedUrl;

    @ToString.Exclude private Map<String, String> headers;
    @ToString.Exclude private List<String> openMessages;
    private WebSocketMessageFormat format;
    private String fieldDelimiter;
    private int connectTimeoutMs;
    private int pingIntervalMs;
    private boolean enableReconnect;
    private int maxReconnectTimes;
    private int reconnectIntervalMs;
    private int queueCapacity;
    private int pollTimeoutMs;
    private long maxRecords;
    private int readTimeoutMs;

    public WebSocketSourceConfig(ReadonlyConfig config) {
        this.url = config.get(WebSocketSourceOptions.URL);
        // derived once instead of per log statement, the url never changes afterwards
        this.maskedUrl = maskUrl(this.url);
        this.headers =
                config.getOptional(WebSocketSourceOptions.HEADERS).orElse(Collections.emptyMap());
        this.openMessages =
                config.getOptional(WebSocketSourceOptions.OPEN_MESSAGES)
                        .orElse(Collections.emptyList());
        this.format = config.get(WebSocketSourceOptions.FORMAT);
        this.fieldDelimiter = config.get(WebSocketSourceOptions.FIELD_DELIMITER);
        this.connectTimeoutMs = config.get(WebSocketSourceOptions.CONNECT_TIMEOUT_MS);
        this.pingIntervalMs = config.get(WebSocketSourceOptions.PING_INTERVAL_MS);
        this.enableReconnect = config.get(WebSocketSourceOptions.ENABLE_RECONNECT);
        this.maxReconnectTimes = config.get(WebSocketSourceOptions.MAX_RECONNECT_TIMES);
        this.reconnectIntervalMs = config.get(WebSocketSourceOptions.RECONNECT_INTERVAL_MS);
        this.queueCapacity = config.get(WebSocketSourceOptions.QUEUE_CAPACITY);
        this.pollTimeoutMs = config.get(WebSocketSourceOptions.POLL_TIMEOUT_MS);
        this.maxRecords = config.get(WebSocketSourceOptions.MAX_RECORDS);
        this.readTimeoutMs = config.get(WebSocketSourceOptions.READ_TIMEOUT_MS);
        validate();
    }

    /** Fails fast on the client side so users do not have to wait for a task to be scheduled. */
    private void validate() {
        if (url == null
                || !(url.startsWith(WebSocketCommonOptions.WS_SCHEME)
                        || url.startsWith(WebSocketCommonOptions.WSS_SCHEME))) {
            throw new WebSocketConnectorException(
                    WebSocketConnectorErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "Option [%s] must start with \"%s\" or \"%s\", but was [%s]",
                            WebSocketSourceOptions.URL.key(),
                            WebSocketCommonOptions.WS_SCHEME,
                            WebSocketCommonOptions.WSS_SCHEME,
                            maskedUrl));
        }
        checkPositive(WebSocketSourceOptions.CONNECT_TIMEOUT_MS, connectTimeoutMs);
        checkPositive(WebSocketSourceOptions.QUEUE_CAPACITY, queueCapacity);
        checkPositive(WebSocketSourceOptions.POLL_TIMEOUT_MS, pollTimeoutMs);
        checkPositive(WebSocketSourceOptions.RECONNECT_INTERVAL_MS, reconnectIntervalMs);
        if (pingIntervalMs < 0) {
            throw new WebSocketConnectorException(
                    WebSocketConnectorErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "Option [%s] must not be negative, but was [%s]",
                            WebSocketSourceOptions.PING_INTERVAL_MS.key(), pingIntervalMs));
        }
        if (maxReconnectTimes < 0) {
            throw new WebSocketConnectorException(
                    WebSocketConnectorErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "Option [%s] must not be negative, but was [%s]",
                            WebSocketSourceOptions.MAX_RECONNECT_TIMES.key(), maxReconnectTimes));
        }
    }

    /**
     * Reduces the url to the part that is safe to print: the scheme, host, port and path are kept,
     * userinfo is dropped, and every query parameter keeps its name but loses its value.
     *
     * <p>Values are masked regardless of the parameter name rather than by a deny list of
     * credential-like names, because these endpoints name their credential parameters in too many
     * ways for such a list to be trustworthy. Keeping the names still lets a log tell one
     * connection apart from another, and makes it obvious the value was masked rather than
     * misconfigured.
     *
     * <p>Note that a credential placed in the path cannot be masked without losing the identity of
     * the endpoint, so the documentation asks for credentials to be passed through {@code headers}
     * or {@code open_messages} instead.
     *
     * <p>Never throws: masking a url must not be able to fail a job.
     */
    private static String maskUrl(String url) {
        if (url == null || url.isEmpty()) {
            return url;
        }
        try {
            URI uri = new URI(url);
            if (uri.getScheme() == null || uri.getHost() == null) {
                // uncommon shapes, such as a host name containing an underscore, parse without a
                // host, so fall back instead of rebuilding from incomplete parts
                return maskQueryOnly(url);
            }
            StringBuilder masked =
                    new StringBuilder(uri.getScheme())
                            .append(SCHEME_SEPARATOR)
                            .append(uri.getHost());
            if (uri.getPort() > 0) {
                masked.append(':').append(uri.getPort());
            }
            if (uri.getRawPath() != null) {
                masked.append(uri.getRawPath());
            }
            if (uri.getRawQuery() != null) {
                masked.append(QUERY_SEPARATOR).append(maskQueryValues(uri.getRawQuery()));
            }
            return masked.toString();
        } catch (URISyntaxException e) {
            return maskQueryOnly(url);
        }
    }

    /** Fallback for a url that cannot be parsed: leave it alone apart from the query values. */
    private static String maskQueryOnly(String url) {
        int queryStart = url.indexOf(QUERY_SEPARATOR);
        if (queryStart < 0) {
            return url;
        }
        return url.substring(0, queryStart + 1) + maskQueryValues(url.substring(queryStart + 1));
    }

    private static String maskQueryValues(String rawQuery) {
        StringBuilder masked = new StringBuilder();
        for (String parameter : rawQuery.split(PARAMETER_SEPARATOR, -1)) {
            if (masked.length() > 0) {
                masked.append(PARAMETER_SEPARATOR);
            }
            int valueStart = parameter.indexOf(VALUE_SEPARATOR);
            // a parameter without a value carries nothing to hide, keep it as it is
            masked.append(
                    valueStart < 0
                            ? parameter
                            : parameter.substring(0, valueStart + 1) + MASKED_VALUE);
        }
        return masked.toString();
    }

    private static void checkPositive(Option<Integer> option, int value) {
        if (value <= 0) {
            throw new WebSocketConnectorException(
                    WebSocketConnectorErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "Option [%s] must be greater than 0, but was [%s]",
                            option.key(), value));
        }
    }
}
