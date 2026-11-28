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
package org.apache.seatunnel.connectors.seatunnel.appstoreconnect;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.cfg.JsonNodeFeature;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.config.MessageConstraints;
import org.apache.http.impl.DefaultBHttpClientConnection;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.http.protocol.HttpRequestExecutor;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.net.URI;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

/**
 * Quiet HTTPS transport for Apple API calls and separate signed downloads.
 *
 * <p>Direct per-attempt sockets avoid redirect, cookie, wire-logging and cross-origin header reuse.
 * A watchdog bounds trickle responses and closes an active connection during cancellation.
 */
final class AppStoreConnectHttp implements AutoCloseable {
    private static final MessageConstraints HEADERS =
            MessageConstraints.custom().setMaxLineLength(16384).setMaxHeaderCount(64).build();
    static final String JSON = "application/json";
    static final String GZIP = "application/a-gzip";
    private static final Set<Integer> TRANSIENT = Set.of(408, 429, 500, 502, 503, 504);

    private final AppStoreConnectConfig config;
    private final AppStoreConnectJwt jwt;
    private final ScheduledThreadPoolExecutor watchdog;
    final ObjectMapper json;

    /**
     * Only the reader starts HTTP requests; the engine cancellation thread may close its socket.
     */
    private volatile boolean closed;

    private Socket connected;
    private long previousAttempt;
    private boolean attempted;

    AppStoreConnectHttp(AppStoreConnectConfig config) {
        this.config = config;
        jwt = new AppStoreConnectJwt(config);
        JsonFactory factory =
                new JsonFactory()
                        .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                        .setStreamReadConstraints(
                                StreamReadConstraints.builder()
                                        .maxNestingDepth(80)
                                        .maxNumberLength(512)
                                        .maxStringLength(config.jsonLimit)
                                        .build());
        json =
                new ObjectMapper(factory)
                        .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
                        .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                        .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                        .configure(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES, false);
        watchdog =
                new ScheduledThreadPoolExecutor(
                        1,
                        task -> {
                            Thread thread = new Thread(task, "app-store-connect-https-deadline");
                            thread.setDaemon(true);
                            return thread;
                        });
        watchdog.setRemoveOnCancelPolicy(true);
    }

    long deadline() {
        return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.operationTimeout);
    }

    JsonNode metadata(AppStoreConnectResource resource, URI uri, long deadline)
            throws InterruptedException {
        return api(
                resource,
                uri,
                JSON,
                deadline,
                (status, reply, expires) -> {
                    if (status != 200) {
                        throw resource.failure("metadata request returned HTTP " + status);
                    }
                    JsonNode result = readJson(resource, reply, config.jsonLimit, expires);
                    JsonNode errors = result.path("errors");
                    if (!(errors.isMissingNode() || errors.isArray() && errors.isEmpty())) {
                        throw resource.failure("metadata contained provider errors");
                    }
                    return result;
                });
    }

    AppStoreConnectReport dated(AppStoreConnectResource resource, URI uri, long deadline)
            throws InterruptedException {
        return api(
                resource,
                uri,
                GZIP,
                deadline,
                (status, reply, expires) -> {
                    if (status == 200) {
                        return AppStoreConnectReport.prepare(
                                this,
                                config,
                                resource,
                                entity(resource, reply),
                                encoding(resource, reply),
                                null,
                                expires);
                    }
                    if (status == 404 || status == 410) {
                        JsonNode error =
                                readJson(
                                        resource,
                                        reply,
                                        Math.min(config.jsonLimit, 65536),
                                        expires);
                        if (confirmedNoReport(error.path("errors"), status)) {
                            return null;
                        }
                    }
                    throw resource.failure("dated report request returned HTTP " + status);
                });
    }

    /**
     * Bounds compatibility to known missing/expired report meanings rather than ignoring every
     * error status. Provider error details are never propagated.
     */
    private static boolean confirmedNoReport(JsonNode errors, int status) {
        if (!errors.isArray() || errors.isEmpty() || errors.size() > 16) {
            return false;
        }
        for (JsonNode value : errors) {
            String code = value.path("code").asText("");
            String detail = value.path("detail").asText("").toLowerCase(Locale.ROOT);
            if (status == 404) {
                if (!"NOT_FOUND".equals(code)
                        || !(detail.startsWith("there were no sales for the date specified")
                                || detail.startsWith("report is not available yet")
                                || detail.startsWith("there is no report available for the date")
                                || detail.startsWith(
                                        "the report you requested is not available"))) {
                    return false;
                }
            } else if (!"GONE".equals(code)
                    || !detail.contains("report")
                    || !(detail.contains("expired") || detail.contains("no longer available"))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Signed download requests carry no bearer token and use only allowlisted HTTPS authorities. An
     * expired/rejected signature is refreshed by another segment metadata lookup.
     */
    AppStoreConnectReport download(
            AppStoreConnectResource resource, AppStoreConnectApi.Segment segment, long deadline)
            throws InterruptedException {
        URI uri = segment.download;
        return request(
                resource,
                uri,
                "application/octet-stream",
                null,
                deadline,
                (status, reply, expires) -> {
                    if (status == 401 || status == 403) {
                        return null;
                    }
                    if (status != 200) {
                        throw resource.failure("signed download returned HTTP " + status);
                    }
                    return AppStoreConnectReport.prepare(
                            this,
                            config,
                            resource,
                            entity(resource, reply),
                            encoding(resource, reply),
                            segment,
                            expires);
                });
    }

    private <T> T api(
            AppStoreConnectResource resource, URI uri, String media, long deadline, Body<T> body)
            throws InterruptedException {
        // Wrap the decoder so neither a report body nor an error body is read on HTTP 401.
        for (int renewal = 0; renewal < 2; renewal++) {
            String bearer = jwt.bearer(resource, Instant.now());
            Result<T> result =
                    request(
                            resource,
                            uri,
                            media,
                            bearer,
                            deadline,
                            (status, reply, expires) ->
                                    status == 401
                                            ? new Result<>(true, null)
                                            : new Result<>(
                                                    false, body.read(status, reply, expires)));
            if (!result.unauthorized) {
                return result.value;
            }
            jwt.invalidate();
        }
        throw resource.failure("HTTP 401 after re-signing the API token");
    }

    /**
     * Retries only transient statuses or safe GET I/O errors under the same operation budget.
     *
     * <p>The retry hint cannot extend the original deadline or trigger a redirect.
     */
    private <T> T request(
            AppStoreConnectResource resource,
            URI uri,
            String media,
            String bearer,
            long deadline,
            Body<T> decoder)
            throws InterruptedException {
        for (int attempt = 0; ; attempt++) {
            if (attempted) {
                pause(
                        resource,
                        TimeUnit.MILLISECONDS.toNanos(config.interval)
                                - (System.nanoTime() - previousAttempt),
                        deadline);
            }
            long retryAfter;
            String reason;
            try {
                return exchange(resource, uri, media, bearer, deadline, decoder);
            } catch (Retry ex) {
                retryAfter = ex.delay;
                reason = "HTTP " + ex.status;
            } catch (SSLException ex) {
                remaining(resource, deadline);
                throw resource.failure("HTTPS certificate verification or negotiation failed");
            } catch (JsonProcessingException ex) {
                throw resource.failure("invalid JSON response");
            } catch (HttpException ex) {
                throw resource.failure("malformed HTTP response");
            } catch (IOException ex) {
                remaining(resource, deadline);
                retryAfter = 0;
                reason = "HTTPS I/O failure or attempt timeout";
            }
            if (attempt >= config.retries) {
                throw resource.failure(reason + " after " + (attempt + 1) + " attempts");
            }
            long wait = Math.max(retryAfter, Math.min(5000, 200L << attempt));
            pause(resource, TimeUnit.MILLISECONDS.toNanos(wait), deadline);
        }
    }

    private <T> T exchange(
            AppStoreConnectResource resource,
            URI uri,
            String media,
            String bearer,
            long operationDeadline,
            Body<T> decoder)
            throws IOException, HttpException, InterruptedException, Retry {
        long attemptNanos =
                Math.min(
                        remaining(resource, operationDeadline),
                        TimeUnit.MILLISECONDS.toNanos(config.timeout));
        long expires = System.nanoTime() + attemptNanos;
        int port = uri.getPort() < 0 ? 443 : uri.getPort();
        Socket raw = new Socket(Proxy.NO_PROXY);
        ScheduledFuture<?> abort = null;
        try {
            synchronized (this) {
                remaining(resource, expires);
                connected = raw;
                abort =
                        watchdog.schedule(
                                () -> closeSocket(raw), attemptNanos, TimeUnit.NANOSECONDS);
            }
            previousAttempt = System.nanoTime();
            attempted = true;
            int timeout = (int) Math.max(1, TimeUnit.NANOSECONDS.toMillis(attemptNanos));
            raw.connect(new InetSocketAddress(uri.getHost(), port), timeout);
            raw.setSoTimeout(timeout);
            SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory.getDefault();
            try (SSLSocket socket =
                            (SSLSocket) factory.createSocket(raw, uri.getHost(), port, true);
                    DefaultBHttpClientConnection wire =
                            new DefaultBHttpClientConnection(8192, null, null, HEADERS)) {
                SSLParameters ssl = socket.getSSLParameters();
                ssl.setEndpointIdentificationAlgorithm("HTTPS");
                socket.setSSLParameters(ssl);
                socket.startHandshake();
                String target = uri.getRawPath();
                if (uri.getRawQuery() != null) {
                    target += "?" + uri.getRawQuery();
                }
                BasicHttpRequest request = new BasicHttpRequest("GET", target);
                request.addHeader("Host", uri.getRawAuthority());
                request.addHeader("Connection", "close");
                request.addHeader("Accept", media);
                request.addHeader("Accept-Encoding", "gzip");
                if (bearer != null) {
                    request.addHeader("Authorization", bearer);
                }
                wire.bind(socket);
                HttpResponse response =
                        new HttpRequestExecutor().execute(request, wire, HttpCoreContext.create());
                int status = response.getStatusLine().getStatusCode();
                if (TRANSIENT.contains(status)) {
                    throw new Retry(status, retryAfter(response));
                }
                remaining(resource, expires);
                T result = decoder.read(status, response, expires);
                try {
                    remaining(resource, operationDeadline);
                    return result;
                } catch (InterruptedException | RuntimeException error) {
                    if (result instanceof AppStoreConnectReport) {
                        ((AppStoreConnectReport) result).close();
                    } else if (result instanceof Result
                            && ((Result<?>) result).value instanceof AppStoreConnectReport) {
                        ((AppStoreConnectReport) ((Result<?>) result).value).close();
                    }
                    throw error;
                }
            }
        } finally {
            closeSocket(raw);
            if (abort != null) {
                abort.cancel(false);
            }
            synchronized (this) {
                connected = null;
            }
        }
    }

    private JsonNode readJson(
            AppStoreConnectResource resource, HttpResponse response, int limit, long expires)
            throws IOException, InterruptedException {
        InputStream wire =
                new AppStoreConnectReport.LimitedStream(
                        entity(resource, response), limit, resource, "JSON transfer bytes");
        InputStream body = encoding(resource, response) ? new GZIPInputStream(wire) : wire;
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        try (InputStream input =
                new AppStoreConnectReport.LimitedStream(
                        body, limit, resource, "decoded JSON bytes")) {
            for (int n; (n = input.read(buffer)) != -1; ) {
                remaining(resource, expires);
                bytes.write(buffer, 0, n);
            }
        }
        byte[] payload = bytes.toByteArray();
        try (JsonParser parser = json.getFactory().createParser(payload)) {
            int tokens = 0;
            while (parser.nextToken() != null) {
                if (++tokens > 250000) {
                    throw resource.failure("JSON token limit exceeded");
                }
                if ((tokens & 1023) == 0) {
                    remaining(resource, expires);
                }
            }
        }
        JsonNode parsed = json.readTree(payload);
        remaining(resource, expires);
        if (parsed == null || !parsed.isObject()) {
            throw resource.failure("expected a JSON object response");
        }
        return parsed;
    }

    private static InputStream entity(AppStoreConnectResource resource, HttpResponse response)
            throws IOException {
        HttpEntity body = response.getEntity();
        if (body == null) {
            throw resource.failure("missing HTTPS response body");
        }
        // Do not let an Apache entity drain an untrusted body when an upper limit rejects it.
        return new AppStoreConnectReport.NonClosingStream(body.getContent());
    }

    private static boolean encoding(AppStoreConnectResource resource, HttpResponse response) {
        Header[] headers = response.getHeaders("Content-Encoding");
        if (headers.length == 0
                || headers.length == 1
                        && "identity".equalsIgnoreCase(headers[0].getValue().trim())) {
            return false;
        }
        if (headers.length != 1 || !"gzip".equalsIgnoreCase(headers[0].getValue().trim())) {
            throw resource.failure("unsupported HTTPS content encoding");
        }
        return true;
    }

    private static long retryAfter(HttpResponse response) {
        long highest = 0;
        for (Header header : response.getHeaders("Retry-After")) {
            String value = header.getValue().trim();
            try {
                long delay =
                        value.matches("[0-9]+")
                                ? Math.multiplyExact(Long.parseLong(value), 1000L)
                                : ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME)
                                                .toInstant()
                                                .toEpochMilli()
                                        - System.currentTimeMillis();
                highest = Math.max(highest, delay);
            } catch (ArithmeticException | NumberFormatException overflow) {
                return Long.MAX_VALUE;
            } catch (DateTimeException invalid) {
                // Ignore malformed hints and retain the local retry schedule.
            }
        }
        return highest;
    }

    private void pause(AppStoreConnectResource resource, long nanos, long deadline)
            throws InterruptedException {
        if (nanos >= remaining(resource, deadline)) {
            throw resource.failure("retry or request pacing exceeds the operation deadline");
        }
        while (nanos > 0) {
            long start = System.nanoTime();
            TimeUnit.NANOSECONDS.sleep(Math.min(nanos, TimeUnit.MILLISECONDS.toNanos(100)));
            remaining(resource, deadline);
            nanos -= System.nanoTime() - start;
        }
    }

    long remaining(AppStoreConnectResource resource, long deadline) throws InterruptedException {
        if (closed || Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("AppStoreConnect reader closed");
        }
        long nanos = deadline - System.nanoTime();
        if (nanos <= 0) {
            throw resource.failure("HTTPS operation deadline exceeded");
        }
        return nanos;
    }

    @Override
    public synchronized void close() {
        closed = true;
        closeSocket(connected);
        watchdog.shutdownNow();
    }

    private static void closeSocket(Socket socket) {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // No further work is allowed on the socket.
            }
        }
    }

    /**
     * Parses only the body appropriate for the current request phase.
     *
     * <p>The attempt deadline also covers decoding and validated report staging.
     */
    private interface Body<T> {
        T read(int status, HttpResponse response, long expires)
                throws IOException, InterruptedException;
    }

    /**
     * Returns an authenticated response or asks the caller to re-sign one time.
     *
     * <p>A null value is also a successful empty report, so the flag is explicit.
     */
    private static final class Result<T> {
        final boolean unauthorized;
        final T value;

        Result(boolean unauthorized, T value) {
            this.unauthorized = unauthorized;
            this.value = value;
        }
    }

    /**
     * Carries an HTTP retry hint without exposing provider headers or message text.
     *
     * <p>The caller applies the bound shared by all attempts.
     */
    private static final class Retry extends Exception {
        private static final long serialVersionUID = 1L;
        final int status;
        final long delay;

        Retry(int status, long delay) {
            this.status = status;
            this.delay = delay;
        }
    }
}
