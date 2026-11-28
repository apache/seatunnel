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

package org.apache.seatunnel.connectors.seatunnel.azureiothub;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.azureiothub.AzureIoTHubSinkOptions.APPLICATION_PROPERTIES;
import static org.apache.seatunnel.connectors.seatunnel.azureiothub.AzureIoTHubSinkOptions.CONNECT_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.azureiothub.AzureIoTHubSinkOptions.DEVICE_ID;
import static org.apache.seatunnel.connectors.seatunnel.azureiothub.AzureIoTHubSinkOptions.FIELD;
import static org.apache.seatunnel.connectors.seatunnel.azureiothub.AzureIoTHubSinkOptions.FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.azureiothub.AzureIoTHubSinkOptions.HUB_URL;
import static org.apache.seatunnel.connectors.seatunnel.azureiothub.AzureIoTHubSinkOptions.MAX_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.azureiothub.AzureIoTHubSinkOptions.MAX_PAYLOAD;
import static org.apache.seatunnel.connectors.seatunnel.azureiothub.AzureIoTHubSinkOptions.READ_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.azureiothub.AzureIoTHubSinkOptions.ROW_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.azureiothub.AzureIoTHubSinkOptions.SAS_TTL;
import static org.apache.seatunnel.connectors.seatunnel.azureiothub.AzureIoTHubSinkOptions.SECRET_KEY;

final class AzureIoTHubConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final int HUB_MESSAGE_BYTES = 256 * 1024;
    // Leave room for the system properties supplied by IoT Hub.
    private static final int SYSTEM_PROPERTY_RESERVE = 1024;

    enum Format {
        JSON("application/json", "utf-8"),
        TEXT("text/plain", "utf-8"),
        BINARY("application/octet-stream", null);

        final String type;
        final String encoding;

        Format(String type, String encoding) {
            this.type = type;
            this.encoding = encoding;
        }
    }

    final URI origin;
    final String target;
    final String scope;
    final byte[] key;
    final Format format;
    final int fieldIndex;
    final Map<String, String> properties;
    final int maxBodyBytes;
    final int sasTtl;
    final int connectTimeout;
    final int readTimeout;
    final int rowTimeout;
    final int attempts;

    AzureIoTHubConfig(ReadonlyConfig options, SeaTunnelRowType type) {
        origin = origin(required(options, HUB_URL, 512));
        String device = required(options, DEVICE_ID, 128);
        if (!device.matches("[A-Za-z0-9%.!()*,:=@_$'?-]+")
                || ".".equals(device)
                || "..".equals(device)) {
            throw invalid("device_id contains unsupported characters or is an HTTP dot segment");
        }
        target = "/devices/" + encode(device, false) + "/messages/events?api-version=2021-04-12";
        scope = encode(origin.getHost() + "/devices/" + device, true);
        key = key(required(options, SECRET_KEY, 256));
        try {
            format = Format.valueOf(options.get(FORMAT).toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw invalid("format must be JSON, TEXT, or BINARY");
        }
        fieldIndex = fieldIndex(type, options.get(FIELD));
        properties = properties(options.get(APPLICATION_PROPERTIES));
        int metadata =
                SYSTEM_PROPERTY_RESERVE
                        + format.type.length()
                        + (format.encoding == null ? 0 : format.encoding.length());
        for (Map.Entry<String, String> property : properties.entrySet()) {
            metadata += property.getKey().length() + property.getValue().length();
        }
        maxBodyBytes =
                Math.min(
                        bounded(options, MAX_PAYLOAD, 1, HUB_MESSAGE_BYTES),
                        HUB_MESSAGE_BYTES - metadata);
        sasTtl = bounded(options, SAS_TTL, 30, 86400);
        connectTimeout = bounded(options, CONNECT_TIMEOUT, 1, 300000);
        readTimeout = bounded(options, READ_TIMEOUT, 1, 300000);
        rowTimeout = bounded(options, ROW_TIMEOUT, 1, 300000);
        attempts = bounded(options, MAX_ATTEMPTS, 1, 10);
    }

    int port() {
        return origin.getPort() == -1 ? 443 : origin.getPort();
    }

    private int fieldIndex(SeaTunnelRowType type, String field) {
        if (format == Format.JSON) {
            if (field != null) {
                throw invalid("field applies only to TEXT and BINARY");
            }
            return -1;
        }
        int index = field == null ? -1 : type.indexOf(field, false);
        SqlType expected = format == Format.TEXT ? SqlType.STRING : SqlType.BYTES;
        if (index < 0 || type.getFieldType(index).getSqlType() != expected) {
            throw invalid(
                    "field must select a STRING column for TEXT or a BYTES column for BINARY");
        }
        return index;
    }

    private static Map<String, String> properties(Map<String, String> input) {
        Map<String, String> result = new LinkedHashMap<>();
        int total = 0;
        if (input.size() > 32) {
            throw invalid("at most 32 application properties may be supplied");
        }
        for (Map.Entry<String, String> entry : input.entrySet()) {
            String name = entry.getKey();
            String value = entry.getValue();
            if (name == null
                    || !name.matches("[A-Za-z0-9_.-]{1,64}")
                    || value == null
                    || value.length() > 1024
                    || value.chars().anyMatch(c -> c < 32 || c > 126)) {
                throw invalid("invalid application property name or ASCII value");
            }
            if (result.put(name.toLowerCase(Locale.ROOT), value) != null) {
                throw invalid("application property names must be unique, ignoring case");
            }
            total += name.length() + value.length();
            if (total > 4096) {
                throw invalid("application property names and values exceed 4096 bytes");
            }
        }
        return Collections.unmodifiableMap(result);
    }

    private static URI origin(String value) {
        try {
            URI uri = new URI(value);
            if (!"https".equalsIgnoreCase(uri.getScheme())
                    || uri.getHost() == null
                    || uri.getHost().length() > 253
                    || uri.getHost().contains(":")
                    || uri.getRawUserInfo() != null
                    || uri.getRawQuery() != null
                    || uri.getRawFragment() != null
                    || uri.getPort() == 0
                    || uri.getPort() > 65535
                    || (!uri.getRawPath().isEmpty() && !"/".equals(uri.getRawPath()))) {
                throw invalid("hub_url must be a trusted HTTPS origin");
            }
            return new URI(
                    "https",
                    null,
                    uri.getHost().toLowerCase(Locale.ROOT),
                    uri.getPort(),
                    null,
                    null,
                    null);
        } catch (URISyntaxException e) {
            throw invalid("hub_url must be a trusted HTTPS origin");
        }
    }

    private static byte[] key(String encoded) {
        byte[] decoded;
        try {
            decoded = Base64.getDecoder().decode(encoded);
        } catch (IllegalArgumentException e) {
            throw invalid("secret_key must be a valid Base64 device key");
        }
        if (decoded.length < 16 || decoded.length > 128) {
            throw invalid("secret_key must contain 16 to 128 Base64-encoded key bytes");
        }
        return decoded;
    }

    static String encode(String value, boolean lowerHex) {
        String hex = lowerHex ? "0123456789abcdef" : "0123456789ABCDEF";
        StringBuilder result = new StringBuilder(value.length());
        for (byte item : value.getBytes(StandardCharsets.UTF_8)) {
            int c = item & 255;
            if ((c >= 'a' && c <= 'z')
                    || (c >= 'A' && c <= 'Z')
                    || (c >= '0' && c <= '9')
                    || c == '-'
                    || c == '_'
                    || c == '.'
                    || c == '~') {
                result.append((char) c);
            } else {
                result.append('%').append(hex.charAt(c >>> 4)).append(hex.charAt(c & 15));
            }
        }
        return result.toString();
    }

    private static int bounded(
            ReadonlyConfig options, Option<Integer> option, int lower, int upper) {
        int value = options.get(option);
        if (value < lower || value > upper) {
            throw invalid(option.key() + " is outside the supported range");
        }
        return value;
    }

    private static String required(ReadonlyConfig options, Option<String> option, int maxLength) {
        String value = options.get(option);
        if (value == null || value.isEmpty() || value.length() > maxLength) {
            throw invalid(option.key() + " is missing or too long");
        }
        return value;
    }

    private static IllegalArgumentException invalid(String detail) {
        return new IllegalArgumentException("Azure IoT Hub sink: " + detail);
    }
}
