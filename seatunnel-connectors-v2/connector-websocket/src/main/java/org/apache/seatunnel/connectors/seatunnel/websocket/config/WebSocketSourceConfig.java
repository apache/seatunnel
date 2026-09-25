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

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Runtime view of the WebSocket source configuration, shared between the source and the reader. */
@Data
public class WebSocketSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private String url;
    private Map<String, String> headers;
    private List<String> openMessages;
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
                            url));
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
