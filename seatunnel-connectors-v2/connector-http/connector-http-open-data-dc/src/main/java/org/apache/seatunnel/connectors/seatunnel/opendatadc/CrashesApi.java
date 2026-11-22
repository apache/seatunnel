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

import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.NoConnectionReuseStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.SystemDefaultRoutePlanner;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.ProxySelector;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

class CrashesApi implements Closeable {
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
    private final CloseableHttpClient client;
    private final AtomicReference<HttpGet> inFlight = new AtomicReference<>();
    // The latch is both the sticky cancellation flag and the wake-up for Retry-After.
    private final CountDownLatch cancelled = new CountDownLatch(1);
    private final AtomicBoolean closed = new AtomicBoolean();

    CrashesApi() {
        this(ENDPOINT);
    }

    CrashesApi(String endpoint) {
        this(endpoint, ProxySelector.getDefault());
    }

    CrashesApi(String endpoint, ProxySelector proxySelector) {
        this.endpoint = endpoint;
        this.client =
                HttpClients.custom()
                        .useSystemProperties()
                        .setRoutePlanner(new SystemDefaultRoutePlanner(proxySelector))
                        .setDefaultRequestConfig(
                                RequestConfig.custom()
                                        .setConnectTimeout(10000)
                                        .setSocketTimeout(30000)
                                        .setConnectionRequestTimeout(10000)
                                        .build())
                        // Each GET is independent; the server or a proxy can close its connection
                        // after a page or a retryable response.
                        .setConnectionReuseStrategy(NoConnectionReuseStrategy.INSTANCE)
                        .disableRedirectHandling()
                        .disableAutomaticRetries()
                        .disableCookieManagement()
                        .disableAuthCaching()
                        .disableContentCompression()
                        .build();
    }

    void cancel() {
        cancelled.countDown();
        HttpGet request = inFlight.get();
        if (request != null) {
            request.abort();
        }
    }

    @Override
    public void close() throws IOException {
        cancel();
        if (closed.compareAndSet(false, true)) {
            client.close();
        }
    }

    boolean isCancelled() {
        return cancelled.getCount() == 0;
    }

    private void checkNotCancelled() throws IOException {
        if (isCancelled()) {
            throw cancelledRequest();
        }
    }

    private static IOException cancelledRequest() {
        return new IOException("Open Data DC source was cancelled");
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
            String url =
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
                            + (window * size);
            JsonNode result = get(url);
            if (!result.get("features").isEmpty()
                    || !result.path("exceededTransferLimit").asBoolean()) {
                return result;
            }
        }
        throw new IOException("Open Data DC MapServer returned too many consecutive empty windows");
    }

    private JsonNode get(String url) throws IOException {
        for (int attempt = 1; attempt <= MAX_THROTTLE_ATTEMPTS; attempt++) {
            HttpGet request = new HttpGet(url);
            // Publish before checking cancellation: cancel() either aborts this request, or the
            // sticky flag prevents it from ever being executed.
            inFlight.set(request);
            long retryDelay;
            try {
                checkNotCancelled();
                try (CloseableHttpResponse response = client.execute(request)) {
                    int code = response.getStatusLine().getStatusCode();
                    if (code == HttpStatus.SC_OK) {
                        if (response.getEntity() == null) {
                            throw new IOException("Open Data DC returned HTTP 200 without a body");
                        }
                        return parse(response.getEntity().getContent());
                    }
                    if (code != 429 || attempt == MAX_THROTTLE_ATTEMPTS) {
                        throw new IOException("Open Data DC returned HTTP " + code);
                    }
                    Header retryAfter = response.getFirstHeader("Retry-After");
                    retryDelay =
                            retryDelayMillis(
                                    retryAfter == null ? null : retryAfter.getValue(),
                                    Instant.now(),
                                    500L * attempt);
                }
            } catch (IllegalStateException e) {
                // HttpClient checks its connection pool before it checks whether the request was
                // aborted. A concurrent close can therefore surface a pool-shutdown error.
                if (closed.get() && request.isAborted()) {
                    throw new IOException("Open Data DC source was cancelled", e);
                }
                throw e;
            } finally {
                inFlight.compareAndSet(request, null);
            }
            awaitRetry(retryDelay);
        }
        throw new IOException("Open Data DC returned HTTP 429");
    }

    void awaitRetry(long retryDelay) throws IOException {
        try {
            if (cancelled.await(retryDelay, TimeUnit.MILLISECONDS)) {
                throw cancelledRequest();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting to retry Open Data DC HTTP 429", e);
        }
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
