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

package org.apache.seatunnel.connectors.seatunnel.datadog;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;

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
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;

/**
 * Owns one quiet HTTP/1.1 exchange at a time. HTTP Core's plain connection has no wire or header
 * logging. The fixed TLS origin and bounded parser keep provider data private.
 */
final class DatadogHttp implements DatadogReader.PageClient {
    // Bound HTTP headers independently of the decompressed body and JSON limits.
    private static final MessageConstraints HEADER_LIMITS =
            MessageConstraints.custom().setMaxLineLength(16384).setMaxHeaderCount(64).build();
    private static final int MAX_JSON_TOKENS = 250000;
    private final DatadogConfig config;
    // The timer aborts the raw socket so trickling bodies cannot outlive the elapsed page budget.
    private final ScheduledThreadPoolExecutor deadlineTimer;
    // Preserve integer/decimal values while bounding tree complexity before materializing records.
    private final ObjectMapper mapper;
    // Guarded with current by this monitor; cancellation remains effective before connect starts.
    private boolean closed;
    // Raw socket remains closable while its TLS wrapper or entity stream is blocked.
    private Socket current;

    DatadogHttp(DatadogConfig config) {
        this.config = config;
        deadlineTimer =
                new ScheduledThreadPoolExecutor(1, task -> daemon(task, "datadog-deadline"));
        deadlineTimer.setRemoveOnCancelPolicy(true);
        JsonFactory factory =
                new JsonFactory()
                        .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                        .setStreamReadConstraints(
                                StreamReadConstraints.builder()
                                        .maxNestingDepth(100)
                                        .maxStringLength(config.maxBytes)
                                        .maxNumberLength(1000)
                                        .build());
        mapper =
                new ObjectMapper(factory)
                        .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                        .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
                        .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                        .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    }

    static Thread daemon(Runnable task, String name) {
        Thread thread = new Thread(task, name);
        thread.setDaemon(true);
        return thread;
    }

    /**
     * Retries the same read-only request within both the attempt and elapsed time budgets. A
     * provider delay can reject further retries when the remaining page budget is insufficient.
     */
    @Override
    public JsonNode fetch(String target, long expires) throws InterruptedException {
        for (int attempt = 1; ; attempt++) {
            ensureActive(expires);
            Retry retry;
            try {
                return exchange(target, expires);
            } catch (Retry e) {
                retry = e;
            } catch (SSLException e) {
                ensureActive(expires);
                throw failure("TLS peer verification or negotiation failed");
            } catch (ZipException e) {
                throw failure("invalid gzip response");
            } catch (HttpException e) {
                throw failure("invalid HTTP response");
            } catch (IOException e) {
                retry = new Retry("I/O failure", 0);
            }
            ensureActive(expires);
            if (attempt >= config.attempts) {
                throw failure(retry.reason + " after " + attempt + " attempts");
            }
            long interval = Math.min(5000, 200L << (attempt - 1));
            long delay =
                    Math.max(
                            retry.minimumMs,
                            interval + ThreadLocalRandom.current().nextLong(interval / 2 + 1));
            if (delay >= TimeUnit.NANOSECONDS.toMillis(expires - System.nanoTime())) {
                throw failure("retry delay exceeds the remaining page deadline");
            }
            Thread.sleep(delay);
        }
    }

    /**
     * Cancellation is sticky even if DNS, TLS or a retry wakeup has not started. Closing the raw
     * socket and timer never needs the reader checkpoint lock.
     */
    @Override
    public synchronized void close() {
        closed = true;
        closeSocket(current);
        deadlineTimer.shutdownNow();
    }

    /**
     * One fixed-origin TLS exchange; no redirects or response-controlled target can receive keys.
     * The idle timeout and socket-closing timer cover different network-stall conditions.
     */
    private JsonNode exchange(String target, long expires)
            throws IOException, HttpException, Retry, InterruptedException {
        Socket raw = new Socket(Proxy.NO_PROXY);
        ScheduledFuture<?> abort = null;
        try {
            synchronized (this) {
                ensureActive(expires);
                current = raw;
                abort =
                        deadlineTimer.schedule(
                                () -> closeSocket(raw),
                                Math.max(0, expires - System.nanoTime()),
                                TimeUnit.NANOSECONDS);
            }
            int timeout =
                    (int)
                            Math.min(
                                    config.timeout,
                                    Math.max(
                                            1,
                                            TimeUnit.NANOSECONDS.toMillis(
                                                    expires - System.nanoTime())));
            raw.connect(new InetSocketAddress(config.host, 443), timeout);
            raw.setSoTimeout(timeout);
            SSLSocketFactory tls = (SSLSocketFactory) SSLSocketFactory.getDefault();
            try (SSLSocket socket = (SSLSocket) tls.createSocket(raw, config.host, 443, true);
                    DefaultBHttpClientConnection connection =
                            new DefaultBHttpClientConnection(8192, null, null, HEADER_LIMITS)) {
                SSLParameters parameters = socket.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                socket.setSSLParameters(parameters);
                socket.startHandshake();
                connection.bind(socket);
                BasicHttpRequest request = new BasicHttpRequest("GET", target);
                request.addHeader("Host", config.host);
                request.addHeader("DD-API-KEY", config.accessKey);
                request.addHeader("DD-APPLICATION-KEY", config.secretKey);
                request.addHeader("Accept", "application/json");
                request.addHeader("Accept-Encoding", "gzip");
                request.addHeader("Connection", "close");
                HttpResponse response =
                        new HttpRequestExecutor()
                                .execute(request, connection, HttpCoreContext.create());
                int code = response.getStatusLine().getStatusCode();
                if (code == 408
                        || code == 429
                        || code == 500
                        || code == 502
                        || code == 503
                        || code == 504) {
                    throw new Retry("HTTP " + code, retryFloor(response, code));
                }
                if (code != 200) {
                    // Provider reasons and error bodies can contain credentials or tenant data.
                    throw failure("HTTP " + code);
                }
                return decode(response, raw);
            }
        } finally {
            closeSocket(raw);
            if (abort != null) {
                abort.cancel(false);
            }
            synchronized (this) {
                if (current == raw) {
                    current = null;
                }
            }
        }
    }

    /**
     * Reject oversized, malformed and incomplete results before returning any records to the
     * reader. Abort the socket before closing an incomplete entity because entity close may drain
     * its body.
     */
    private JsonNode decode(HttpResponse response, Socket socket) throws IOException {
        HttpEntity entity = response.getEntity();
        if (entity == null) {
            throw failure("missing response body");
        }
        Header contentEncoding = response.getFirstHeader("Content-Encoding");
        String encoding = contentEncoding == null ? null : contentEncoding.getValue();
        if (encoding != null
                && !encoding.equalsIgnoreCase("identity")
                && !encoding.equalsIgnoreCase("gzip")) {
            throw failure("unsupported response compression");
        }
        byte[] bytes;
        InputStream wire = entity.getContent();
        InputStream body = wire;
        try {
            if ("gzip".equalsIgnoreCase(encoding)) {
                body = new GZIPInputStream(wire);
            }
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            byte[] chunk = new byte[8192];
            int length;
            while ((length = body.read(chunk)) != -1) {
                if (length > config.maxBytes - buffer.size()) {
                    throw failure("decompressed response exceeds max_response_bytes");
                }
                buffer.write(chunk, 0, length);
            }
            bytes = buffer.toByteArray();
        } catch (IOException | RuntimeException e) {
            // HTTP Core entity streams may drain on close. Abort before discarding an incomplete
            // body.
            closeSocket(socket);
            throw e;
        } finally {
            try {
                body.close();
            } catch (IOException ignored) {
                // The connection is already closed.
            }
        }
        try {
            // Tiny tokens can occupy substantial tree memory even when the byte limit is met.
            try (JsonParser parser = mapper.getFactory().createParser(bytes)) {
                int tokens = 0;
                while (parser.nextToken() != null) {
                    if (++tokens > MAX_JSON_TOKENS) {
                        throw failure("JSON response has too many tokens");
                    }
                }
            }
            JsonNode root = mapper.readTree(bytes);
            if (root == null || !(root.isObject() || root.isArray())) {
                throw failure("missing or malformed JSON response");
            }
            JsonNode status = root.at("/meta/status");
            if (present(root.path("error"))
                    || present(root.path("errors"))
                    || present(root.at("/meta/warnings"))
                    || present(root.path("warnings"))
                    || (!status.isMissingNode()
                            && !status.isNull()
                            && !"done".equals(status.asText()))) {
                throw failure("provider returned errors or incomplete results");
            }
            return root;
        } catch (JsonProcessingException e) {
            throw failure("malformed or unsupported JSON response");
        }
    }

    private boolean present(JsonNode node) {
        return !node.isMissingNode()
                && !node.isNull()
                && !(node.isContainerNode() && node.isEmpty())
                && !(node.isTextual() && node.textValue().isEmpty());
    }

    /**
     * Treat valid provider delay hints as lower bounds; the page deadline can reject that wait.
     * Datadog X-RateLimit-Reset is seconds until reset, not an epoch timestamp.
     */
    private long retryFloor(HttpResponse response, int code) {
        long retryAfter = 0;
        for (Header header : response.getHeaders("Retry-After")) {
            String value = header.getValue();
            long numeric = secondsDelay(value);
            if (numeric >= 0) {
                retryAfter = Math.max(retryAfter, numeric);
            } else if (value != null) {
                try {
                    Instant next =
                            ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME)
                                    .toInstant();
                    retryAfter =
                            Math.max(retryAfter, Duration.between(Instant.now(), next).toMillis());
                } catch (DateTimeException | ArithmeticException ignored) {
                    // Invalid hints leave the bounded jittered policy in effect.
                }
            }
        }
        if (code == 429) {
            for (Header reset : response.getHeaders("X-RateLimit-Reset")) {
                retryAfter = Math.max(retryAfter, secondsDelay(reset.getValue()));
            }
        }
        return retryAfter;
    }

    private long secondsDelay(String value) {
        if (value == null || !value.trim().matches("[0-9]+")) {
            return -1;
        }
        try {
            return Math.multiplyExact(Long.parseLong(value.trim()), 1000);
        } catch (ArithmeticException | NumberFormatException e) {
            return Long.MAX_VALUE;
        }
    }

    private synchronized void ensureActive(long expires) throws InterruptedException {
        if (closed || Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Datadog reader closed");
        }
        if (expires - System.nanoTime() <= 0) {
            throw failure("page deadline exceeded");
        }
    }

    private IllegalStateException failure(String reason) {
        return config.resource.invalid(reason);
    }

    private static void closeSocket(Socket socket) {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // The request is already being discarded.
            }
        }
    }

    /**
     * Sanitized retry instruction; provider response text is deliberately never retained. Only
     * status categories and a minimum delay cross this internal boundary.
     */
    private static final class Retry extends Exception {
        private final String reason;
        private final long minimumMs;

        private Retry(String reason, long minimumMs) {
            this.reason = reason;
            this.minimumMs = minimumMs;
        }
    }
}
