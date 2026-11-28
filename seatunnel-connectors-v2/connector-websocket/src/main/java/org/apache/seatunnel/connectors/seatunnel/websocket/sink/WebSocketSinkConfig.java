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

package org.apache.seatunnel.connectors.seatunnel.websocket.sink;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;

import static org.apache.seatunnel.connectors.seatunnel.websocket.sink.WebSocketSinkOptions.CLOSE_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.websocket.sink.WebSocketSinkOptions.CONNECT_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.websocket.sink.WebSocketSinkOptions.FIELD;
import static org.apache.seatunnel.connectors.seatunnel.websocket.sink.WebSocketSinkOptions.FLUSH_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.websocket.sink.WebSocketSinkOptions.FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.websocket.sink.WebSocketSinkOptions.MAX_FRAME_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.websocket.sink.WebSocketSinkOptions.MAX_QUEUE_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.websocket.sink.WebSocketSinkOptions.TOKEN;
import static org.apache.seatunnel.connectors.seatunnel.websocket.sink.WebSocketSinkOptions.URL;
import static org.apache.seatunnel.connectors.seatunnel.websocket.sink.WebSocketSinkOptions.WRITE_TIMEOUT;

final class WebSocketSinkConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final int MAX_PENDING_BYTES = 16 * 1024 * 1024;

    enum Format {
        JSON,
        TEXT,
        BINARY
    }

    final URI endpoint;
    final Format format;
    final int fieldIndex;
    final int connectTimeout;
    final int writeTimeout;
    final int flushTimeout;
    final int closeTimeout;
    final int frameBytes;
    final int queueBytes;
    private final String token;

    WebSocketSinkConfig(ReadonlyConfig options, SeaTunnelRowType type) {
        endpoint = validateEndpoint(options.get(URL));
        token = options.get(TOKEN);
        if (token != null
                && (!"wss".equalsIgnoreCase(endpoint.getScheme())
                        || token.isEmpty()
                        || token.length() > 4096
                        || token.chars().anyMatch(c -> c <= 32 || c >= 127))) {
            throw invalid("token must be a nonempty bearer credential and requires wss");
        }
        format = parseFormat(options.get(FORMAT));
        fieldIndex = findField(options.get(FIELD), type);
        connectTimeout = bounded(options, CONNECT_TIMEOUT, 300000);
        writeTimeout = bounded(options, WRITE_TIMEOUT, 300000);
        flushTimeout = bounded(options, FLUSH_TIMEOUT, 300000);
        closeTimeout = bounded(options, CLOSE_TIMEOUT, 300000);
        queueBytes = bounded(options, MAX_QUEUE_BYTES, MAX_PENDING_BYTES);
        frameBytes = bounded(options, MAX_FRAME_BYTES, queueBytes);
    }

    String authorization() {
        return token == null ? null : "Bearer " + token;
    }

    private int findField(String field, SeaTunnelRowType type) {
        if (format == Format.JSON) {
            if (field != null) {
                throw invalid("field applies only to TEXT and BINARY");
            }
            return -1;
        }
        if (field == null || field.isEmpty()) {
            throw invalid("field is required for TEXT and BINARY");
        }
        int index = type.indexOf(field, false);
        SqlType expected = format == Format.TEXT ? SqlType.STRING : SqlType.BYTES;
        if (index < 0 || type.getFieldType(index).getSqlType() != expected) {
            throw invalid(
                    "field must identify a STRING column for TEXT or a BYTES column for BINARY");
        }
        return index;
    }

    private static URI validateEndpoint(String url) {
        if (url == null || url.length() > 2048) {
            throw invalid("url must identify a ws or wss endpoint");
        }
        try {
            URI endpoint = new URI(url);
            if ((!"ws".equalsIgnoreCase(endpoint.getScheme())
                            && !"wss".equalsIgnoreCase(endpoint.getScheme()))
                    || endpoint.getHost() == null
                    || endpoint.getPort() == 0
                    || endpoint.getPort() > 65535
                    || endpoint.getRawUserInfo() != null
                    || endpoint.getRawQuery() != null
                    || endpoint.getRawFragment() != null) {
                throw invalid(
                        "url must identify a ws or wss endpoint without user information, query, or fragment");
            }
            return endpoint;
        } catch (URISyntaxException e) {
            throw invalid("url must identify a ws or wss endpoint");
        }
    }

    private static Format parseFormat(String format) {
        try {
            return Format.valueOf(format.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw invalid("format must be JSON, TEXT, or BINARY");
        }
    }

    private static int bounded(ReadonlyConfig options, Option<Integer> option, int upper) {
        int value = options.get(option);
        if (value < 1 || value > upper) {
            throw invalid(option.key() + " must be within its supported range");
        }
        return value;
    }

    private static IllegalArgumentException invalid(String message) {
        return new IllegalArgumentException("WebSocket sink: " + message);
    }
}
