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

package org.apache.seatunnel.connectors.seatunnel.opendatadc;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

class CrashesApi {
    static final String ENDPOINT =
            "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/"
                    + "Public_Safety_WebMercator/MapServer/24/query";
    private static final String FIELDS =
            "OBJECTID,CCN,REPORTDATE,WARD,TOTAL_VEHICLES,LATITUDE,LONGITUDE";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int MAX_EMPTY_WINDOWS = 32;
    private static final int MAX_THROTTLE_ATTEMPTS = 3;
    private static final long MAX_RETRY_AFTER_MS = 5000;

    private final String endpoint;

    CrashesApi() {
        this(ENDPOINT);
    }

    CrashesApi(String endpoint) {
        this.endpoint = endpoint;
    }

    long readUpperBound() throws IOException {
        JsonNode features = query("1=1", "OBJECTID", "OBJECTID DESC", 1).get("features");
        if (features.size() > 1) {
            throw new IOException("Open Data DC returned more rows than requested");
        }
        return features.isEmpty() ? 0L : objectId(attributes(features.get(0)));
    }

    JsonNode readPage(long lastId, long upperBound, int size) throws IOException {
        return query(
                "OBJECTID > " + lastId + " AND OBJECTID <= " + upperBound,
                FIELDS,
                "OBJECTID ASC",
                size);
    }

    private JsonNode query(String filter, String fields, String sort, int size) throws IOException {
        // MapServer may report an empty candidate window even though more results remain.
        // Keep the exact predicate, order and width while moving one complete window at a time.
        for (int window = 0; window <= MAX_EMPTY_WINDOWS; window++) {
            URL url =
                    new URL(
                            endpoint
                                    + "?f=json&returnGeometry=false"
                                    + "&where="
                                    + URLEncoder.encode(filter, StandardCharsets.UTF_8.name())
                                    + "&outFields="
                                    + URLEncoder.encode(fields, StandardCharsets.UTF_8.name())
                                    + "&orderByFields="
                                    + URLEncoder.encode(sort, StandardCharsets.UTF_8.name())
                                    + "&resultRecordCount="
                                    + size
                                    + "&resultOffset="
                                    + (window * size));
            JsonNode result = get(url);
            if (!result.get("features").isEmpty()
                    || !result.path("exceededTransferLimit").asBoolean()) {
                return result;
            }
        }
        throw new IOException("Open Data DC MapServer returned too many consecutive empty windows");
    }

    private JsonNode get(URL url) throws IOException {
        for (int attempt = 1; attempt <= MAX_THROTTLE_ATTEMPTS; attempt++) {
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(10000);
            connection.setReadTimeout(30000);
            connection.setInstanceFollowRedirects(false);
            long retryDelay;
            try {
                int code = connection.getResponseCode();
                if (code == HttpURLConnection.HTTP_OK) {
                    try (InputStream stream = connection.getInputStream()) {
                        return parse(stream);
                    }
                }
                if (code != 429 || attempt == MAX_THROTTLE_ATTEMPTS) {
                    throw new IOException("Open Data DC returned HTTP " + code);
                }
                retryDelay =
                        retryDelayMillis(
                                connection.getHeaderField("Retry-After"),
                                Instant.now(),
                                500L * attempt);
            } finally {
                connection.disconnect();
            }
            try {
                Thread.sleep(retryDelay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(
                        "Interrupted while waiting to retry Open Data DC HTTP 429", e);
            }
        }
        throw new IOException("Open Data DC returned HTTP 429");
    }

    static long retryDelayMillis(String header, Instant now, long fallback) throws IOException {
        if (header == null) {
            return fallback;
        }
        String value = header.trim();
        long delay;
        if (value.matches("[0-9]+")) {
            try {
                delay = Math.multiplyExact(Long.parseLong(value), 1000L);
            } catch (NumberFormatException | ArithmeticException e) {
                delay = Long.MAX_VALUE;
            }
        } else {
            try {
                Instant retryAt =
                        ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME)
                                .toInstant();
                delay = Math.max(0L, Duration.between(now, retryAt).toMillis());
            } catch (DateTimeParseException e) {
                return fallback;
            }
        }
        if (delay > MAX_RETRY_AFTER_MS) {
            throw new IOException(
                    "Open Data DC HTTP 429 Retry-After exceeds the local retry budget");
        }
        return delay;
    }

    static JsonNode parse(InputStream payload) throws IOException {
        JsonNode result = MAPPER.readTree(payload);
        if (result == null || !result.isObject()) {
            throw new IOException("Open Data DC returned an invalid JSON object");
        }
        if (result.has("error")) {
            throw new IOException(
                    "Open Data DC ArcGIS error: "
                            + result.path("error").path("code").asText("unknown"));
        }
        if (!result.path("features").isArray()) {
            throw new IOException("Open Data DC response is missing its features array");
        }
        if (result.has("exceededTransferLimit")
                && !result.get("exceededTransferLimit").isBoolean()) {
            throw new IOException("Open Data DC returned an invalid exceededTransferLimit flag");
        }
        return result;
    }

    static JsonNode attributes(JsonNode feature) throws IOException {
        JsonNode value = feature.path("attributes");
        if (!value.isObject()) {
            throw new IOException("Open Data DC feature is missing its attributes object");
        }
        return value;
    }

    static long objectId(JsonNode attributes) throws IOException {
        JsonNode id = attributes.path("OBJECTID");
        if (!id.isIntegralNumber() || !id.canConvertToLong() || id.longValue() < 1) {
            throw new IOException("Open Data DC returned an invalid OBJECTID");
        }
        return id.longValue();
    }
}
