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

package org.apache.seatunnel.connectors.seatunnel.alphavantage;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * Defines each query function and its response envelope. The selected function determines which
 * query parameters apply and which dates are emitted.
 */
enum AlphaVantageResource {
    TIME_SERIES_INTRADAY("Time Series (%s)", true),
    TIME_SERIES_DAILY("Time Series (Daily)", true),
    TIME_SERIES_DAILY_ADJUSTED("Time Series (Daily)", true),
    TIME_SERIES_WEEKLY("Weekly Time Series", false),
    TIME_SERIES_WEEKLY_ADJUSTED("Weekly Adjusted Time Series", false),
    TIME_SERIES_MONTHLY("Monthly Time Series", false),
    TIME_SERIES_MONTHLY_ADJUSTED("Monthly Adjusted Time Series", false),
    QUOTE("Global Quote", false);

    private static final DateTimeFormatter INTRADAY =
            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss", Locale.ROOT)
                    .withResolverStyle(ResolverStyle.STRICT);
    private static final String HEX = "0123456789ABCDEF";
    private final String envelope;
    private final boolean supportsSize;

    AlphaVantageResource(String envelope, boolean supportsSize) {
        this.envelope = envelope;
        this.supportsSize = supportsSize;
    }

    static List<String> names() {
        List<String> names = new ArrayList<>();
        for (AlphaVantageResource resource : values()) {
            names.add(resource.key());
        }
        return Collections.unmodifiableList(names);
    }

    String key() {
        return name().toLowerCase(Locale.ROOT);
    }

    /**
     * Builds one GET target from validated options. The target contains the API key and stays
     * inside the request executor.
     */
    String requestTarget(AlphaVantageConfig config) {
        String function = this == QUOTE ? "GLOBAL_QUOTE" : name();
        StringBuilder target =
                new StringBuilder("/query?function=")
                        .append(function)
                        .append("&symbol=")
                        .append(encode(config.symbol))
                        .append("&datatype=json&apikey=")
                        .append(encode(config.token));
        if (supportsSize) {
            target.append("&outputsize=").append(config.size);
        }
        if (this == TIME_SERIES_INTRADAY) {
            target.append("&interval=")
                    .append(config.interval)
                    .append("&adjusted=")
                    .append(config.adjusted);
        }
        return target.toString();
    }

    /**
     * Validates the full response before any row is visible. Reordering JSON fields does not change
     * its identity. Changed timestamps, data or output metadata do.
     */
    Snapshot snapshot(JsonNode root, AlphaVantageConfig config) throws IOException {
        String field =
                this == TIME_SERIES_INTRADAY
                        ? String.format(Locale.ROOT, envelope, config.interval)
                        : envelope;
        JsonNode body = root.path(field);
        if (!body.isObject()) {
            throw failure("expected a " + field + " object");
        }
        if (this == QUOTE) {
            return quote(body, config);
        }
        JsonNode metadata = root.path("Meta Data");
        if (!metadata.isObject()) {
            throw failure("expected a Meta Data object");
        }
        verifySymbol(metadata.get("2. Symbol"), config);
        String timeZone = timezone(metadata);
        if (body.size() > config.maxRecords) {
            throw failure("response exceeds max_records");
        }
        Map<String, JsonNode> sorted = fields(body);
        List<Point> points = new ArrayList<>(sorted.size());
        MessageDigest digest = AlphaVantageConfig.digest();
        component(digest, timeZone);
        for (Map.Entry<String, JsonNode> point : sorted.entrySet()) {
            String time = point.getKey();
            JsonNode value = point.getValue();
            if (!value.isObject() || value.isEmpty()) {
                throw failure("invalid time series observation");
            }
            String timestamp = null;
            String date;
            if (this == TIME_SERIES_INTRADAY) {
                try {
                    if (time.length() != 19) {
                        throw failure("invalid intraday timestamp key");
                    }
                    date = LocalDateTime.parse(time, INTRADAY).toLocalDate().toString();
                } catch (DateTimeParseException e) {
                    throw failure("invalid intraday timestamp key");
                }
                timestamp = time;
            } else {
                validateDate(time);
                date = time;
            }
            String canonical = canonical(value).toString();
            component(digest, time);
            component(digest, canonical);
            points.add(new Point(date, timestamp, canonical));
        }
        return new Snapshot(timeZone, digest(digest), points);
    }

    private Snapshot quote(JsonNode body, AlphaVantageConfig config) throws IOException {
        if (body.isEmpty()) {
            return new Snapshot(
                    null, AlphaVantageConfig.snapshotId(new byte[0]), Collections.emptyList());
        }
        verifySymbol(body.get("01. symbol"), config);
        String date = text(body.get("07. latest trading day"), "invalid quote trading date");
        validateDate(date);
        String canonical = canonical(body).toString();
        MessageDigest digest = AlphaVantageConfig.digest();
        component(digest, canonical);
        return new Snapshot(
                null, digest(digest), Collections.singletonList(new Point(date, null, canonical)));
    }

    private void verifySymbol(JsonNode provider, AlphaVantageConfig config) throws IOException {
        if (!config.symbol.equalsIgnoreCase(text(provider, "response has no symbol"))) {
            throw failure("response symbol differs from the request");
        }
    }

    private String timezone(JsonNode meta) throws IOException {
        String zone = null;
        for (Iterator<Map.Entry<String, JsonNode>> i = meta.fields(); i.hasNext(); ) {
            Map.Entry<String, JsonNode> field = i.next();
            if (field.getKey().endsWith(". Time Zone")) {
                if (zone != null) {
                    throw failure("ambiguous time zone metadata");
                }
                zone = text(field.getValue(), "invalid time zone metadata");
            }
        }
        return zone;
    }

    private String text(JsonNode node, String error) throws IOException {
        if (node == null
                || !node.isTextual()
                || node.textValue().isEmpty()
                || node.textValue().length() > 128
                || node.textValue().chars().anyMatch(ch -> ch < 32 || ch == 127)) {
            throw failure(error);
        }
        return node.textValue();
    }

    private void validateDate(String date) throws IOException {
        try {
            if (date.length() != 10 || !LocalDate.parse(date).toString().equals(date)) {
                throw failure("invalid trading date");
            }
        } catch (DateTimeParseException e) {
            throw failure("invalid trading date");
        }
    }

    private static String encode(String value) {
        StringBuilder result = new StringBuilder();
        for (byte raw : value.getBytes(StandardCharsets.UTF_8)) {
            int ch = raw & 255;
            if ((ch >= 'a' && ch <= 'z')
                    || (ch >= 'A' && ch <= 'Z')
                    || (ch >= '0' && ch <= '9')
                    || ch == '_'
                    || ch == '-'
                    || ch == '.'
                    || ch == '~') {
                result.append((char) ch);
            } else {
                result.append('%').append(HEX.charAt(ch >>> 4)).append(HEX.charAt(ch & 15));
            }
        }
        return result.toString();
    }

    private static Map<String, JsonNode> fields(JsonNode node) {
        Map<String, JsonNode> result = new TreeMap<>();
        node.fields().forEachRemaining(entry -> result.put(entry.getKey(), entry.getValue()));
        return result;
    }

    private static JsonNode canonical(JsonNode node) {
        if (node.isObject()) {
            ObjectNode object = JsonNodeFactory.instance.objectNode();
            fields(node).forEach((key, value) -> object.set(key, canonical(value)));
            return object;
        }
        if (node.isArray()) {
            ArrayNode array = JsonNodeFactory.instance.arrayNode();
            node.forEach(value -> array.add(canonical(value)));
            return array;
        }
        return node;
    }

    private static void component(MessageDigest digest, String value) {
        if (value != null) {
            digest.update(value.getBytes(StandardCharsets.UTF_8));
        }
        digest.update((byte) 0);
    }

    private static String digest(MessageDigest digest) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(digest.digest());
    }

    IOException failure(String detail) {
        return new IOException("Alpha Vantage source " + key() + ": " + detail);
    }

    /**
     * Holds the observations for one verified resource and its stable identity, which is used only
     * if the reader checkpoints a partial response.
     */
    static final class Snapshot {
        final String timeZone;
        final String identity;
        final List<Point> points;

        Snapshot(String timeZone, String identity, List<Point> points) {
            this.timeZone = timeZone;
            this.identity = identity;
            this.points = points;
        }
    }

    /**
     * Holds one trading date or market-local timestamp with the corresponding provider observation.
     * Numeric strings retain their original formatting.
     */
    static final class Point {
        final String date;
        final String timestamp;
        final String data;

        Point(String date, String timestamp, String data) {
            this.date = date;
            this.timestamp = timestamp;
            this.data = data;
        }
    }
}
