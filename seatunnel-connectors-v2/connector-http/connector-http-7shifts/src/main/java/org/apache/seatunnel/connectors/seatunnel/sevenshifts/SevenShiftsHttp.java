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

package org.apache.seatunnel.connectors.seatunnel.sevenshifts;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.entity.DeflateInputStream;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.MessageConstraints;
import org.apache.http.impl.DefaultBHttpClientConnectionFactory;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.http.protocol.HttpRequestExecutor;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

/**
 * Owns one asynchronous GET with a deadline covering retries, transfer and response decoding. Quiet
 * socket connections avoid credential/body logging; decoded JSON symbol tables belong to one page.
 */
final class SevenShiftsHttp implements AutoCloseable {
    private static final int MAX_BODY = 8 * 1024 * 1024;
    private static final ObjectMapper JSON =
            new ObjectMapper(
                            JsonFactory.builder()
                                    .disable(JsonFactory.Feature.INTERN_FIELD_NAMES)
                                    .streamReadConstraints(
                                            StreamReadConstraints.builder()
                                                    .maxNestingDepth(64)
                                                    .maxNumberLength(512)
                                                    .build())
                                    .build())
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true))
                    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private static final DefaultBHttpClientConnectionFactory CONNECTIONS =
            new DefaultBHttpClientConnectionFactory(
                    ConnectionConfig.custom()
                            .setMessageConstraints(
                                    MessageConstraints.custom()
                                            .setMaxHeaderCount(64)
                                            .setMaxLineLength(16384)
                                            .build())
                            .build());

    private final SevenShiftsConfig config;
    private final ExecutorService worker =
            Executors.newSingleThreadExecutor(
                    action -> {
                        Thread thread = new Thread(action, "seatunnel-7shifts-source");
                        thread.setDaemon(true);
                        return thread;
                    });

    private boolean closed;
    private Socket socket;
    private Pending pending;

    SevenShiftsHttp(SevenShiftsConfig config) {
        this.config = config;
    }

    /**
     * Submits one operation to the owned worker so connection setup, backoff and response parsing
     * do not occupy the engine thread that must process checkpoints and cancellation.
     */
    synchronized void start(SevenShiftsResource resource, String route) {
        if (closed) {
            return;
        }
        if (pending != null) {
            throw new IllegalStateException("7shifts Source already has a running request");
        }
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.timeoutMs);
        pending =
                new Pending(
                        resource, deadline, worker.submit(() -> fetch(resource, route, deadline)));
    }

    /**
     * Returns only a complete validated JSON response. A null result yields the engine thread while
     * the operation is pending; the watchdog closes an exchange that outlives its request deadline.
     */
    synchronized JsonNode poll() throws IOException {
        if (closed || pending == null) {
            return null;
        }
        Pending current = pending;
        if (!current.result.isDone()) {
            if (current.deadline - System.nanoTime() <= 0) {
                close();
                throw failure(current.resource, "request deadline exceeded");
            }
            return null;
        }
        pending = null;
        try {
            return current.result.get();
        } catch (InterruptedException e) {
            close();
            Thread.currentThread().interrupt();
            throw failure(current.resource, "request interrupted");
        } catch (CancellationException e) {
            throw failure(current.resource, "request cancelled");
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RequestFailure) {
                throw (RequestFailure) e.getCause();
            }
            throw failure(current.resource, "request failed");
        }
    }

    /**
     * Prevents new requests and interrupts worker backoff while closing the published socket
     * without waiting for the worker to finish. Repeated cancellation and close calls share this
     * ownership.
     */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (pending != null) {
            pending.result.cancel(true);
        }
        worker.shutdownNow();
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // The connection also closes the socket when the worker unwinds.
            }
        }
    }

    static RequestFailure failure(SevenShiftsResource resource, String reason) {
        return new RequestFailure("7shifts Source " + resource.apiName + ": " + reason);
    }

    private JsonNode fetch(SevenShiftsResource resource, String route, long deadline)
            throws IOException, InterruptedException {
        for (int attempt = 1; ; attempt++) {
            try {
                JsonNode page = connect(resource, route, deadline);
                remainingMillis(resource, deadline);
                return page;
            } catch (RetryableStatus e) {
                if (attempt >= config.maxAttempts) {
                    throw failure(
                            resource, "HTTP " + e.status + " exhausted the configured attempts");
                }
                long delay = Math.max(100L << (attempt - 1), e.retryAfterMillis);
                if (delay >= remainingMillis(resource, deadline)) {
                    throw failure(
                            resource,
                            "HTTP " + e.status + " retry delay exceeds the request deadline");
                }
                Thread.sleep(delay);
            }
        }
    }

    private JsonNode connect(SevenShiftsResource resource, String route, long deadline)
            throws IOException {
        try (Socket connection = new Socket(Proxy.NO_PROXY)) {
            synchronized (this) {
                if (closed || Thread.currentThread().isInterrupted()) {
                    throw failure(resource, "request cancelled");
                }
                socket = connection;
            }
            boolean secure = "https".equals(config.origin.getScheme());
            int port =
                    config.origin.getPort() == -1 ? (secure ? 443 : 80) : config.origin.getPort();
            String host = config.origin.getHost();
            if (host.startsWith("[")) {
                host = host.substring(1, host.length() - 1);
            }
            connection.connect(
                    new InetSocketAddress(host, port),
                    Math.min(10000, remainingMillis(resource, deadline)));
            connection.setSoTimeout(remainingMillis(resource, deadline));
            if (!secure) {
                return exchange(connection, resource, route);
            }
            SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory.getDefault();
            try (SSLSocket tls = (SSLSocket) factory.createSocket(connection, host, port, true)) {
                SSLParameters parameters = tls.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                tls.setSSLParameters(parameters);
                tls.startHandshake();
                return exchange(tls, resource, route);
            }
        } catch (RequestFailure | RetryableStatus e) {
            throw e;
        } catch (SSLException e) {
            throw failure(resource, "TLS transport failed");
        } catch (SocketTimeoutException e) {
            throw failure(resource, "request deadline exceeded");
        } catch (IOException | HttpException e) {
            throw failure(resource, "transport failed or returned invalid HTTP framing");
        } finally {
            synchronized (this) {
                socket = null;
            }
        }
    }

    private JsonNode exchange(Socket connection, SevenShiftsResource resource, String route)
            throws IOException, HttpException {
        BasicHttpRequest request = new BasicHttpRequest("GET", route, HttpVersion.HTTP_1_1);
        request.setHeader("Host", config.origin.getRawAuthority());
        request.setHeader("Connection", "close");
        request.setHeader("Accept", "application/json");
        request.setHeader("Accept-Encoding", "gzip, deflate");
        request.setHeader("Authorization", "Bearer " + config.token);
        request.setHeader("x-api-version", config.apiVersion);

        HttpResponse response =
                new HttpRequestExecutor()
                        .execute(
                                request,
                                CONNECTIONS.createConnection(connection),
                                HttpCoreContext.create());
        int status = response.getStatusLine().getStatusCode();
        if (status == 429 || status == 500 || status == 502 || status == 503 || status == 504) {
            throw new RetryableStatus(
                    status, retryAfter(header(response, "Retry-After", resource)));
        }
        if (status != 200) {
            throw failure(resource, "unexpected HTTP " + status);
        }
        byte[] decoded = decode(response, resource);
        try {
            // A fresh factory bounds symbol ownership to this response. Keep byte parsing and
            // canonicalization so malformed UTF-8 still fails instead of being replaced.
            JsonNode page = JSON.copy().readTree(decoded);
            if (page == null || !page.isObject()) {
                throw failure(resource, "invalid JSON page");
            }
            return page;
        } catch (IOException e) {
            throw failure(resource, "invalid JSON page");
        }
    }

    private static byte[] decode(HttpResponse response, SevenShiftsResource resource)
            throws IOException {
        String length = header(response, "Content-Length", resource);
        String transfer = header(response, "Transfer-Encoding", resource);
        if (length != null
                && (!length.matches("[0-9]{1,18}") || Long.parseLong(length) > MAX_BODY)) {
            throw failure(resource, "invalid or oversized response length");
        }
        if (transfer != null && (length != null || !"chunked".equalsIgnoreCase(transfer))) {
            throw failure(resource, "invalid response framing");
        }
        HttpEntity entity = response.getEntity();
        if (entity == null) {
            throw failure(resource, "missing response body");
        }
        byte[] wire = readBounded(entity.getContent(), resource);
        String encoding = header(response, "Content-Encoding", resource);
        if (encoding == null || "identity".equalsIgnoreCase(encoding)) {
            return wire;
        }
        InputStream input = new ByteArrayInputStream(wire);
        if ("gzip".equalsIgnoreCase(encoding)) {
            try (InputStream decoded = new GZIPInputStream(input)) {
                return readBounded(decoded, resource);
            }
        }
        if ("deflate".equalsIgnoreCase(encoding)) {
            try (InputStream decoded = new DeflateInputStream(input)) {
                return readBounded(decoded, resource);
            }
        }
        throw failure(resource, "unsupported response encoding");
    }

    private static byte[] readBounded(InputStream input, SevenShiftsResource resource)
            throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        int count;
        while ((count = input.read(buffer)) != -1) {
            if (count > MAX_BODY - bytes.size()) {
                throw failure(resource, "response exceeds 8 MiB");
            }
            bytes.write(buffer, 0, count);
        }
        return bytes.toByteArray();
    }

    private static String header(HttpResponse response, String name, SevenShiftsResource resource)
            throws IOException {
        Header[] values = response.getHeaders(name);
        if (values.length > 1) {
            throw failure(resource, "duplicate response header");
        }
        return values.length == 0 ? null : values[0].getValue().trim();
    }

    private static long retryAfter(String value) {
        if (value == null) {
            return 0;
        }
        if (value.matches("[0-9]+")) {
            try {
                return Math.multiplyExact(Long.parseLong(value), 1000);
            } catch (NumberFormatException | ArithmeticException e) {
                return Long.MAX_VALUE;
            }
        }
        try {
            Instant then =
                    ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant();
            Duration requested = Duration.between(Instant.now(), then);
            return requested.isNegative() ? 0 : requested.plusNanos(999999).toMillis();
        } catch (DateTimeParseException e) {
            return 0;
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    private static int remainingMillis(SevenShiftsResource resource, long deadline)
            throws RequestFailure {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
            throw failure(resource, "request deadline exceeded");
        }
        return (int) Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining));
    }

    /**
     * Public diagnostic boundary contains only resource and fixed failure reason. Request tokens,
     * opaque cursors, response bodies and underlying network/parser messages are never attached.
     */
    static final class RequestFailure extends IOException {
        private static final long serialVersionUID = 1L;

        private RequestFailure(String reason) {
            super(reason);
        }
    }

    /**
     * Worker-local retry instruction carries only HTTP status and bounded-deadline delay metadata.
     * It never exposes the provider error body or response headers to engine diagnostics.
     */
    private static final class RetryableStatus extends IOException {
        private static final long serialVersionUID = 1L;
        private final int status;
        private final long retryAfterMillis;

        private RetryableStatus(int status, long retryAfterMillis) {
            this.status = status;
            this.retryAfterMillis = retryAfterMillis;
        }
    }

    /**
     * Keeps the absolute operation deadline and future together while the engine polls. Closing the
     * owner cancels this future and its socket without serializing either resource into a
     * checkpoint.
     */
    private static final class Pending {
        private final SevenShiftsResource resource;
        private final long deadline;
        private final Future<JsonNode> result;

        private Pending(SevenShiftsResource resource, long deadline, Future<JsonNode> result) {
            this.resource = resource;
            this.deadline = deadline;
            this.result = result;
        }
    }
}
