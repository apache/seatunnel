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

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactoryBuilder;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonToken;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.MessageConstraints;
import org.apache.http.impl.NoConnectionReuseStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.SystemDefaultRoutePlanner;

import java.io.ByteArrayOutputStream;
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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Owns the crash-layer HTTP transport, response budgets, empty-window traversal and HTTP 429
 * retries. Per-GET deadlines abort transport I/O; bounded decoding also checks elapsed time before
 * return. Cancellation wakes retry waits and aborts the published request before transport
 * shutdown.
 */
class CrashesApi implements Closeable {
    static final String ENDPOINT =
            "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/"
                    + "Public_Safety_WebMercator/MapServer/24/query";
    private static final String FIELDS =
            "OBJECTID,CCN,REPORTDATE,WARD,TOTAL_VEHICLES,LATITUDE,LONGITUDE";
    private static final int MAX_RESPONSE_BYTES = 4 * 1024 * 1024;
    private static final int MAX_JSON_TOKENS = 200000;
    private static final int REQUEST_TIMEOUT_MS = 30000;
    // Settings template only: response-local copies own parser names, while interning stays off.
    private static final ObjectMapper MAPPER =
            new ObjectMapper(
                            new JsonFactoryBuilder()
                                    .disable(JsonFactory.Feature.INTERN_FIELD_NAMES)
                                    .streamReadConstraints(
                                            StreamReadConstraints.builder()
                                                    .maxNestingDepth(64)
                                                    .build())
                                    .build())
                    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private static final int MAX_EMPTY_WINDOWS = 32;
    private static final int MAX_THROTTLE_ATTEMPTS = 3;
    private static final long MAX_RETRY_AFTER_MS = 5000;

    private final String endpoint;
    private final CloseableHttpClient client;
    private final int maxResponseBytes;
    private final int requestTimeoutMillis;
    // Owned by this client; workers start lazily and canceled GET deadlines are removed.
    private final ScheduledThreadPoolExecutor deadlines;
    /*
     * Published before checking cancellation so cancel either aborts or prevents dispatch.
     */
    private final AtomicReference<HttpGet> inFlight = new AtomicReference<>();
    // The latch is both the sticky cancellation flag and the wake-up for Retry-After.
    private final CountDownLatch cancelled = new CountDownLatch(1);
    /*
     * Transfers client-close ownership once, independently of the sticky cancellation latch.
     */
    private final AtomicBoolean closed = new AtomicBoolean();

    CrashesApi() {
        this(ENDPOINT);
    }

    CrashesApi(String endpoint) {
        this(endpoint, ProxySelector.getDefault());
    }

    CrashesApi(String endpoint, ProxySelector proxySelector) {
        this(endpoint, proxySelector, MAX_RESPONSE_BYTES, REQUEST_TIMEOUT_MS);
    }

    // Package-private test budgets keep the production limits fixed without public options.
    CrashesApi(
            String endpoint,
            ProxySelector proxySelector,
            int maxResponseBytes,
            int requestTimeoutMillis) {
        if (maxResponseBytes <= 0
                || maxResponseBytes > MAX_RESPONSE_BYTES
                || requestTimeoutMillis <= 0
                || requestTimeoutMillis > REQUEST_TIMEOUT_MS) {
            throw new IllegalArgumentException(
                    "Open Data DC test budgets must fit the local limits");
        }
        this.endpoint = endpoint;
        this.maxResponseBytes = maxResponseBytes;
        this.requestTimeoutMillis = requestTimeoutMillis;
        this.client =
                HttpClients.custom()
                        .useSystemProperties()
                        .setRoutePlanner(new SystemDefaultRoutePlanner(proxySelector))
                        // Headers are parsed before application code can inspect the response body.
                        .setDefaultConnectionConfig(
                                ConnectionConfig.custom()
                                        .setMessageConstraints(
                                                MessageConstraints.custom()
                                                        .setMaxLineLength(8192)
                                                        .setMaxHeaderCount(64)
                                                        .build())
                                        .build())
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
        deadlines =
                new ScheduledThreadPoolExecutor(
                        1,
                        runnable -> {
                            Thread worker = new Thread(runnable, "open-data-dc-request-deadline");
                            worker.setDaemon(true);
                            return worker;
                        });
        deadlines.setRemoveOnCancelPolicy(true);
    }

    /*
     * Permanently wakes retry waits and aborts the currently published request.
     */
    void cancel() {
        cancelled.countDown();
        HttpGet request = inFlight.get();
        if (request != null) {
            request.abort();
        }
    }

    /*
     * Cancels before shutting down owned deadlines and transport, with one close owner across calls.
     */
    @Override
    public void close() throws IOException {
        cancel();
        if (closed.compareAndSet(false, true)) {
            deadlines.shutdownNow();
            client.close();
        }
    }

    boolean isCancelled() {
        return cancelled.getCount() == 0;
    }

    /**
     * Reads at most the highest OBJECTID; an empty layer has upper bound zero. This fixes the ID
     * ceiling, not a transactionally consistent snapshot of mutable records.
     */
    long readUpperBound() throws IOException {
        JsonNode features = query("1=1", "OBJECTID", "OBJECTID DESC", 1).get("features");
        if (features.size() > 1) {
            throw new IOException("Open Data DC returned more rows than requested");
        }
        return features.isEmpty() ? 0L : objectId(attributes(features.get(0)));
    }

    /*
     * Projects the fixed crash fields in ascending key order within the inclusive scan ceiling.
     */
    JsonNode readPage(long lastId, long upperBound, int size) throws IOException {
        return query(
                "OBJECTID > " + lastId + " AND OBJECTID <= " + upperBound,
                FIELDS,
                "OBJECTID ASC",
                size);
    }

    /**
     * Waits on the cancellation latch so cancel wakes a Retry-After delay immediately. Thread
     * interruption is preserved and propagated as an I/O failure.
     */
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

    /**
     * Supports delta-seconds and HTTP-date Retry-After values with a five-second ceiling. Missing
     * or malformed headers use the caller fallback; excessive server delays fail.
     */
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

    /**
     * Rejects ArcGIS error envelopes, missing features and invalid transfer-limit flags before
     * paging.
     */
    static JsonNode parse(InputStream payload) throws IOException {
        return parse(payload, MAX_RESPONSE_BYTES);
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

    private void checkNotCancelled() throws IOException {
        if (isCancelled()) {
            throw cancelledRequest();
        }
    }

    private static IOException cancelledRequest() {
        return new IOException("Open Data DC source was cancelled");
    }

    /**
     * Advances offsets only across empty candidate windows reporting more results. Each call starts
     * at offset zero with its key predicate; nonempty pages resume by OBJECTID. Fails after the
     * initial window plus at most 32 empty-window advances.
     */
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

    /**
     * Attempts each GET at most three times, retrying only HTTP 429 within the local delay budget.
     * Detaches and aborts the active request on every outcome. Each attempt owns an elapsed abort
     * deadline spanning headers/body reads, and verifies elapsed time after bounded decoding.
     * Deadline futures are cancelled before retry waits and every exit. Aborting transport cannot
     * forcibly interrupt arbitrary ProxySelector/DNS code or CPU decoding.
     */
    private JsonNode get(String url) throws IOException {
        for (int attempt = 1; attempt <= MAX_THROTTLE_ATTEMPTS; attempt++) {
            HttpGet request = new HttpGet(url);
            // Publish before checking cancellation: cancel() either aborts this request, or the
            // sticky flag prevents it from ever being executed.
            inFlight.set(request);
            long retryDelay;
            long started = System.nanoTime();
            ScheduledFuture<?> deadline = null;
            try {
                checkNotCancelled();
                deadline =
                        deadlines.schedule(
                                request::abort, requestTimeoutMillis, TimeUnit.MILLISECONDS);
                checkNotCancelled();
                try (CloseableHttpResponse response = client.execute(request)) {
                    int code = response.getStatusLine().getStatusCode();
                    if (code == HttpStatus.SC_OK) {
                        if (response.getEntity() == null) {
                            throw new IOException("Open Data DC returned HTTP 200 without a body");
                        }
                        if (response.getEntity().getContentLength() > maxResponseBytes) {
                            throw byteBudgetExceeded();
                        }
                        JsonNode result =
                                parse(response.getEntity().getContent(), maxResponseBytes);
                        checkNotCancelled();
                        checkElapsed(started);
                        return result;
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
                checkElapsed(started);
            } catch (IOException e) {
                checkNotCancelled();
                checkElapsed(started);
                throw e;
            } catch (RejectedExecutionException e) {
                // close() can win between the sticky cancellation check and timer registration.
                checkNotCancelled();
                throw new IOException("Open Data DC request deadline scheduler is unavailable");
            } catch (IllegalStateException e) {
                // HttpClient checks its connection pool before it checks whether the request was
                // aborted. A concurrent close can therefore surface a pool-shutdown error.
                if (closed.get() && request.isAborted()) {
                    throw new IOException("Open Data DC source was cancelled", e);
                }
                throw e;
            } finally {
                // Success, parse failure, cancellation and retries all release their own timer.
                if (deadline != null) {
                    deadline.cancel(false);
                }
                request.abort();
                inFlight.compareAndSet(request, null);
            }
            awaitRetry(retryDelay);
        }
        throw new IOException("Open Data DC returned HTTP 429");
    }

    // Aborting a completed request cannot interrupt JSON decoding; reject late decoded results too.
    private void checkElapsed(long started) throws IOException {
        if (System.nanoTime() - started >= TimeUnit.MILLISECONDS.toNanos(requestTimeoutMillis)) {
            throw new IOException("Open Data DC GET exceeded its elapsed request deadline");
        }
    }

    /**
     * Bounds bytes before parsing, then counts streaming tokens before allocating a JSON tree. Both
     * passes use the same local depth/duplicate rules; the tree pass also rejects trailing roots.
     * No shared mapper defaults are changed.
     */
    private static JsonNode parse(InputStream payload, int maxBytes) throws IOException {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        int count;
        while ((count = payload.read(buffer)) != -1) {
            if (body.size() > maxBytes - count) {
                throw byteBudgetExceeded();
            }
            body.write(buffer, 0, count);
        }
        byte[] bytes = body.toByteArray();
        // Both passes read the same bounded bytes; tree allocation cannot bypass the preflight.
        // Keep the strict byte parser and its encoding detection without retaining names between
        // responses. Disabling canonicalization would select a replacement-decoding Reader path.
        ObjectMapper responseMapper = MAPPER.copy();
        JsonNode result;
        try {
            try (JsonParser parser = responseMapper.getFactory().createParser(bytes)) {
                int tokens = 0;
                JsonToken token;
                while ((token = parser.nextToken()) != null) {
                    if (++tokens > MAX_JSON_TOKENS) {
                        throw new IOException("Open Data DC JSON token budget exceeded");
                    }
                    if (token == JsonToken.VALUE_STRING) {
                        // Complete lazy strings during the bounded streaming pass.
                        parser.getTextLength();
                    }
                }
            }
            result = responseMapper.readTree(bytes);
        } catch (JsonProcessingException e) {
            throw new IOException(
                    "Open Data DC returned invalid or oversized JSON ("
                            + e.getClass().getSimpleName()
                            + ")");
        }
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

    private static IOException byteBudgetExceeded() {
        return new IOException("Open Data DC response exceeds the local byte budget");
    }
}
