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
package org.apache.seatunnel.connectors.seatunnel.calcom;

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
import org.apache.http.HttpRequest;
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
import java.io.FilterInputStream;
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
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;

/**
 * Quiet HTTPS reads against the canonical Cal.com API authority.
 *
 * <p>Redirects are errors. The connector closes the active socket on timeout or cancellation
 * without logging request credentials.
 */
final class CalComHttp implements AutoCloseable {
    private static final MessageConstraints HEADERS =
            MessageConstraints.custom().setMaxLineLength(16384).setMaxHeaderCount(64).build();
    private static final Set<Integer> TRANSIENT = Set.of(408, 429, 500, 502, 503, 504);

    private final CalComConfig config;
    private final ObjectMapper json;
    private final ScheduledThreadPoolExecutor watchdog;

    // Only the reader worker issues requests. Cancellation may close the active TLS socket.
    private volatile boolean closed;
    private Socket active;
    private long previousAttempt;
    private boolean attempted;

    // Connector-private trust configuration applies only to this reader's socket factory.
    private final SSLSocketFactory tlsFactory;

    CalComHttp(CalComConfig config) {
        this.config = config;
        tlsFactory = config.socketFactory();
        JsonFactory factory =
                new JsonFactory()
                        .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                        .setStreamReadConstraints(
                                StreamReadConstraints.builder()
                                        .maxNestingDepth(90)
                                        .maxNumberLength(500)
                                        .maxStringLength(config.bodyLimit)
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
                            Thread thread = new Thread(task, "calcom-https-deadline");
                            thread.setDaemon(true);
                            return thread;
                        });
        watchdog.setRemoveOnCancelPolicy(true);
    }

    long deadline() {
        return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.operationTimeout);
    }

    /**
     * Bounds a logical exchange across retries, throttling and attempt deadlines.
     *
     * <p>Read retries are limited to eligible transient HTTP statuses and I/O failures.
     */
    JsonNode request(CalComResource resource, String target, String apiVersion, long deadline)
            throws InterruptedException {
        for (int attempt = 0; ; attempt++) {
            if (attempted) {
                delay(
                        resource,
                        TimeUnit.MILLISECONDS.toNanos(config.interval)
                                - (System.nanoTime() - previousAttempt),
                        deadline);
            }
            long retryHint;
            String reason;
            try {
                return exchange(resource, target, apiVersion, deadline);
            } catch (Retry ex) {
                retryHint = ex.delay;
                reason = "HTTP " + ex.status;
            } catch (SSLException ex) {
                remaining(resource, deadline);
                throw failure(resource, "HTTPS certificate verification or negotiation failed");
            } catch (ZipException ex) {
                throw failure(resource, "invalid gzip response");
            } catch (JsonProcessingException ex) {
                throw failure(resource, "invalid JSON response");
            } catch (HttpException ex) {
                throw failure(resource, "malformed HTTP response");
            } catch (IOException ex) {
                remaining(resource, deadline);
                retryHint = 0;
                reason = "HTTPS I/O failure or attempt timeout";
            }
            if (attempt >= config.retries) {
                throw failure(resource, reason + " after " + (attempt + 1) + " attempts");
            }
            long backoff = Math.max(retryHint, Math.min(5000, 250L << attempt));
            if (backoff > config.maxRetryDelay) {
                throw failure(resource, "Retry-After exceeds max_retry_delay_ms");
            }
            delay(resource, TimeUnit.MILLISECONDS.toNanos(backoff), deadline);
        }
    }

    private JsonNode exchange(
            CalComResource resource, String target, String apiVersion, long deadline)
            throws IOException, HttpException, InterruptedException, Retry {
        long attemptTime =
                Math.min(
                        remaining(resource, deadline),
                        TimeUnit.MILLISECONDS.toNanos(config.requestTimeout));
        long expires = System.nanoTime() + attemptTime;
        Socket raw = new Socket(Proxy.NO_PROXY);
        ScheduledFuture<?> abort = null;
        try {
            synchronized (this) {
                remaining(resource, expires);
                active = raw;
                abort =
                        watchdog.schedule(
                                () -> closeSocket(raw), attemptTime, TimeUnit.NANOSECONDS);
            }
            previousAttempt = System.nanoTime();
            attempted = true;
            int timeout = (int) Math.max(1, TimeUnit.NANOSECONDS.toMillis(attemptTime));
            String host = CalComConfig.API_HOST;
            int port = 443;
            raw.connect(
                    new InetSocketAddress(host, port), Math.min(timeout, config.connectTimeout));
            raw.setSoTimeout(timeout);
            try (SSLSocket socket = (SSLSocket) tlsFactory.createSocket(raw, host, port, true);
                    DefaultBHttpClientConnection wire =
                            new DefaultBHttpClientConnection(8192, null, null, HEADERS)) {
                SSLParameters parameters = socket.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                socket.setSSLParameters(parameters);
                socket.startHandshake();
                HttpRequest request = new BasicHttpRequest("GET", target);
                request.addHeader("Authorization", "Bearer " + config.token);
                request.addHeader("Host", host);
                request.addHeader("User-Agent", "Apache-SeaTunnel-CalCom");
                request.addHeader("Accept", "application/json");
                if (apiVersion != null) {
                    request.addHeader("cal-api-version", apiVersion);
                }
                request.addHeader("Accept-Encoding", "gzip");
                request.addHeader("Connection", "close");
                wire.bind(socket);
                HttpResponse response =
                        new HttpRequestExecutor().execute(request, wire, HttpCoreContext.create());
                int status = response.getStatusLine().getStatusCode();
                if (TRANSIENT.contains(status)) {
                    throw new Retry(status, retryAfter(response));
                }
                if (status != 200) {
                    throw failure(resource, "HTTP " + status);
                }
                return body(resource, response, expires, config.bodyLimit);
            }
        } finally {
            closeSocket(raw);
            if (abort != null) {
                abort.cancel(false);
            }
            synchronized (this) {
                active = null;
            }
        }
    }

    private JsonNode body(CalComResource resource, HttpResponse response, long deadline, int limit)
            throws IOException, InterruptedException {
        HttpEntity entity = response.getEntity();
        if (entity == null) {
            throw failure(resource, "missing HTTPS response body");
        }
        Header[] encodings = response.getHeaders("Content-Encoding");
        boolean gzip =
                encodings.length == 1 && "gzip".equalsIgnoreCase(encodings[0].getValue().trim());
        if (encodings.length > 0
                && !(gzip
                        || encodings.length == 1
                                && "identity".equalsIgnoreCase(encodings[0].getValue().trim()))) {
            throw failure(resource, "unsupported HTTPS content encoding");
        }
        InputStream wire = new WireLimit(entity.getContent(), limit, resource);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] block = new byte[8192];
        try (InputStream input = gzip ? new GZIPInputStream(wire) : wire) {
            for (int n; (n = input.read(block)) != -1; ) {
                remaining(resource, deadline);
                if (n > limit - output.size()) {
                    throw failure(resource, "decoded response exceeds max_response_bytes");
                }
                output.write(block, 0, n);
            }
        }
        byte[] bytes = output.toByteArray();
        try (JsonParser parser = json.getFactory().createParser(bytes)) {
            int tokens = 0;
            while (parser.nextToken() != null) {
                if (++tokens > 500000) {
                    throw failure(resource, "JSON token limit exceeded");
                }
                if ((tokens & 1023) == 0) {
                    remaining(resource, deadline);
                }
            }
        }
        JsonNode root = json.readTree(bytes);
        remaining(resource, deadline);
        if (root == null || !(root.isObject() || root.isArray())) {
            throw failure(resource, "expected a JSON object or array");
        }
        return root;
    }

    private static long retryAfter(HttpResponse response) {
        long maximum = 0;
        for (Header header : response.getHeaders("Retry-After")) {
            String value = header.getValue().trim();
            try {
                if (value.matches("[0-9]+")) {
                    maximum = Math.max(maximum, Math.multiplyExact(Long.parseLong(value), 1000));
                } else {
                    long delay =
                            Duration.between(
                                            Instant.now(),
                                            ZonedDateTime.parse(
                                                            value,
                                                            DateTimeFormatter.RFC_1123_DATE_TIME)
                                                    .toInstant())
                                    .toMillis();
                    maximum = Math.max(maximum, delay);
                }
            } catch (NumberFormatException | ArithmeticException ex) {
                return Long.MAX_VALUE;
            } catch (DateTimeException ex) {
                // A malformed HTTP-date does not replace the bounded exponential backoff.
            }
        }
        return maximum;
    }

    private void delay(CalComResource resource, long nanos, long deadline)
            throws InterruptedException {
        if (nanos >= remaining(resource, deadline)) {
            throw failure(resource, "retry or pacing delay exceeds the operation deadline");
        }
        while (nanos > 0) {
            long started = System.nanoTime();
            TimeUnit.NANOSECONDS.sleep(Math.min(nanos, TimeUnit.MILLISECONDS.toNanos(100)));
            remaining(resource, deadline);
            nanos -= System.nanoTime() - started;
        }
    }

    long remaining(CalComResource resource, long deadline) throws InterruptedException {
        if (closed || Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("CalCom source closed");
        }
        long nanos = deadline - System.nanoTime();
        if (nanos <= 0) {
            throw failure(resource, "HTTPS operation deadline exceeded");
        }
        return nanos;
    }

    @Override
    public synchronized void close() {
        closed = true;
        closeSocket(active);
        watchdog.shutdownNow();
    }

    private static void closeSocket(Socket raw) {
        if (raw != null) {
            try {
                raw.close();
            } catch (IOException ignored) {
                // The closed socket will never be reused.
            }
        }
    }

    private static CalComResource.Failure failure(CalComResource resource, String reason) {
        return resource.failure(reason);
    }

    /**
     * Carries only the status and bounded retry information.
     *
     * <p>No provider headers or response strings reach caller diagnostics.
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

    /**
     * Bounds the compressed wire bytes independently of decoded JSON.
     *
     * <p>The enclosing HTTPS call owns the socket and closes it without consuming extra entity
     * bytes after a rejected response.
     */
    private static final class WireLimit extends FilterInputStream {
        private final int limit;
        private final CalComResource resource;
        private int count;

        WireLimit(InputStream body, int limit, CalComResource resource) {
            super(body);
            this.limit = limit;
            this.resource = resource;
        }

        @Override
        public int read() throws IOException {
            int value = in.read();
            if (value != -1) {
                check(1);
            }
            return value;
        }

        @Override
        public int read(byte[] data, int offset, int length) throws IOException {
            int read = in.read(data, offset, length);
            if (read > 0) {
                check(read);
            }
            return read;
        }

        private void check(int read) {
            count += read;
            if (count > limit) {
                throw failure(resource, "wire response exceeds max_response_bytes");
            }
        }

        @Override
        public void close() {
            // The HTTPS call closes instead of draining the entity.
        }
    }
}
