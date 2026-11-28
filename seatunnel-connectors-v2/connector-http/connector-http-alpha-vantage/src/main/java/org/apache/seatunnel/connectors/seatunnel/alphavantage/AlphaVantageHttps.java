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
import org.apache.http.client.utils.DateUtils;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.MessageConstraints;
import org.apache.http.impl.DefaultBHttpClientConnection;
import org.apache.http.impl.DefaultBHttpClientConnectionFactory;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpRequestExecutor;

import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.Date;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPInputStream;

/**
 * Owns the single reader's attempt budget, pacing and active HTTPS socket. Failure messages use
 * fixed context and omit the query and its credentials.
 */
final class AlphaVantageHttps {
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .disable(JsonParser.Feature.AUTO_CLOSE_SOURCE)
                    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                    .enable(
                            DeserializationFeature.FAIL_ON_TRAILING_TOKENS,
                            DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    private static final DefaultBHttpClientConnectionFactory CONNECTIONS =
            new DefaultBHttpClientConnectionFactory(
                    ConnectionConfig.custom()
                            .setMessageConstraints(
                                    MessageConstraints.custom()
                                            .setMaxHeaderCount(64)
                                            .setMaxLineLength(16384)
                                            .build())
                            .build());

    static {
        JSON.getFactory()
                .setStreamReadConstraints(
                        StreamReadConstraints.builder()
                                .maxNestingDepth(64)
                                .maxStringLength(16777216)
                                .maxNumberLength(1024)
                                .build());
    }

    private final AlphaVantageConfig config;
    private final AtomicReference<Socket> socket = new AtomicReference<>();
    private volatile boolean closed;
    private int attemptsUsed;
    private long nextAllowed;

    AlphaVantageHttps(AlphaVantageConfig config) {
        this.config = config;
    }

    /**
     * Retries only recognized temporary responses. A single deadline covers pacing, TLS, the
     * response body, and all attempts.
     */
    AlphaVantageResource.Snapshot get(AlphaVantageResource resource, long deadline)
            throws IOException {
        long backoff = 0;
        for (int attempt = 1; attempt <= config.maxAttempts; attempt++) {
            if (attemptsUsed >= config.maxRequests) {
                throw resource.failure("request budget exhausted (max_requests)");
            }
            pace(resource, deadline, backoff);
            Reply reply = request(resource, deadline);
            if (reply.snapshot != null) {
                return reply.snapshot;
            }
            if (attempt == config.maxAttempts) {
                throw resource.failure("retry limit reached after " + reply.reason);
            }
            backoff = reply.delay;
        }
        throw resource.failure("retry limit reached");
    }

    private Reply request(AlphaVantageResource resource, long deadline) throws IOException {
        Socket raw = new Socket();
        socket.set(raw);
        try {
            if (closed) {
                throw new RequestFailure("request canceled");
            }
            attemptsUsed++;
            nextAllowed = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.requestInterval);
            int port = config.origin.getPort() < 0 ? 443 : config.origin.getPort();
            raw.connect(
                    new InetSocketAddress(config.origin.getHost(), port),
                    timeout(deadline, config.connectTimeout));
            raw.setSoTimeout(timeout(deadline, config.readTimeout));
            try (SSLSocket tls = tls(raw, port)) {
                tls.startHandshake();
                tls.setSoTimeout(timeout(deadline, config.readTimeout));
                DefaultBHttpClientConnection connection = CONNECTIONS.createConnection(tls);
                BasicHttpRequest request =
                        new BasicHttpRequest("GET", resource.requestTarget(config));
                request.setHeader("Host", config.origin.getRawAuthority());
                request.setHeader("Connection", "close");
                request.setHeader("Accept", "application/json");
                request.setHeader("Accept-Encoding", "gzip");
                request.setHeader("User-Agent", "Apache-SeaTunnel-AlphaVantage");
                HttpResponse response =
                        new HttpRequestExecutor()
                                .execute(request, connection, new BasicHttpContext());
                int status = response.getStatusLine().getStatusCode();
                if (status == 200) {
                    Reply body = parse(resource, response);
                    timeout(deadline, config.readTimeout);
                    return body;
                }
                if (status == 429
                        || status == 500
                        || status == 502
                        || status == 503
                        || status == 504) {
                    return Reply.retry(
                            "HTTP " + status,
                            Math.max(retryAfter(response), status == 429 ? 5000 : 1000));
                }
                throw new RequestFailure("HTTP " + status);
            }
        } catch (RequestFailure e) {
            if (System.nanoTime() - deadline >= 0) {
                throw resource.failure("request deadline exceeded");
            }
            throw resource.failure(e.getMessage());
        } catch (IOException | HttpException e) {
            if (System.nanoTime() - deadline >= 0) {
                throw resource.failure("request deadline exceeded");
            }
            throw resource.failure(closed ? "request canceled" : "HTTPS or TLS exchange failed");
        } finally {
            socket.compareAndSet(raw, null);
            closeSocket(raw);
        }
    }

    private Reply parse(AlphaVantageResource resource, HttpResponse response)
            throws RequestFailure {
        HttpEntity entity = response.getEntity();
        Header[] lengths = response.getHeaders("Content-Length");
        Header[] transfers = response.getHeaders("Transfer-Encoding");
        Header[] encodings = response.getHeaders("Content-Encoding");
        if (entity == null
                || lengths.length > 1
                || transfers.length > 1
                || encodings.length > 1
                || (lengths.length != 0 && transfers.length != 0)
                || (transfers.length != 0
                        && !"chunked".equalsIgnoreCase(transfers[0].getValue()))) {
            throw new RequestFailure("invalid response framing");
        }
        if (entity.getContentLength() > config.maxResponse) {
            throw new RequestFailure("response exceeds max_response_bytes");
        }
        JsonNode root;
        try {
            InputStream wire = new BoundedInput(entity.getContent(), config.maxResponse, false);
            String encoding = encodings.length == 0 ? "identity" : encodings[0].getValue().trim();
            InputStream body;
            if ("gzip".equalsIgnoreCase(encoding)) {
                body = new BoundedInput(new GZIPInputStream(wire), config.maxResponse, true);
            } else if ("identity".equalsIgnoreCase(encoding)) {
                body = wire;
            } else {
                throw new RequestFailure("unsupported response encoding");
            }
            try (InputStream input = body) {
                root = JSON.readTree(input);
                if (body != wire && wire.read() != -1) {
                    throw new RequestFailure("unexpected trailing compressed response bytes");
                }
            }
        } catch (BodyLimit e) {
            throw new RequestFailure("response exceeds max_response_bytes");
        } catch (RequestFailure e) {
            throw e;
        } catch (IOException e) {
            throw new RequestFailure("invalid, incomplete or unreadable JSON response");
        }
        if (root == null || !root.isObject()) {
            throw new RequestFailure("expected a JSON response object");
        }
        if (root.has("Error Message")) {
            throw new RequestFailure("provider rejected the query");
        }
        if (root.has("Note") || root.has("Information")) {
            String info =
                    (root.path("Note").asText("") + " " + root.path("Information").asText(""))
                            .toLowerCase(Locale.ROOT);
            boolean rateLimited =
                    info.contains("rate limit")
                            || info.contains("call frequency")
                            || info.contains("quota")
                            || info.contains("requests per")
                            || info.contains("calls per");
            if (rateLimited
                    && (info.contains("per day")
                            || info.contains("daily")
                            || info.contains("per-day")
                            || info.contains("/day"))) {
                throw new RequestFailure("provider daily request quota reached");
            }
            if (info.contains("premium")
                    || info.contains("subscription")
                    || info.contains("entitlement")) {
                throw new RequestFailure(
                        "provider reports a subscription or entitlement requirement");
            }
            if (rateLimited) {
                return Reply.retry("provider rate limit", 5000);
            }
            throw new RequestFailure("provider returned information without data");
        }
        try {
            return Reply.success(resource.snapshot(root, config));
        } catch (IOException e) {
            // Snapshot validation emits safe field and resource names only.
            throw new RequestFailure(e.getMessage());
        }
    }

    private void pace(AlphaVantageResource resource, long deadline, long delay) throws IOException {
        long now = System.nanoTime();
        long minSpacing = attemptsUsed == 0 ? 0 : Math.max(0, nextAllowed - now);
        long wait = Math.max(minSpacing, TimeUnit.MILLISECONDS.toNanos(delay));
        if (deadline - now <= wait) {
            throw resource.failure("pacing or retry delay exceeds request deadline");
        }
        try {
            TimeUnit.NANOSECONDS.sleep(wait);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw resource.failure("request canceled");
        }
        if (closed) {
            throw resource.failure("request canceled");
        }
    }

    private SSLSocket tls(Socket raw, int port) throws IOException {
        SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory.getDefault();
        SSLSocket tls = (SSLSocket) factory.createSocket(raw, config.origin.getHost(), port, true);
        SSLParameters ssl = tls.getSSLParameters();
        ssl.setEndpointIdentificationAlgorithm("HTTPS");
        ssl.setProtocols(
                Arrays.stream(tls.getEnabledProtocols())
                        .filter(
                                protocol ->
                                        "TLSv1.2".equals(protocol) || "TLSv1.3".equals(protocol))
                        .toArray(String[]::new));
        tls.setSSLParameters(ssl);
        return tls;
    }

    private static int timeout(long deadline, int upper) throws RequestFailure {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
            throw new RequestFailure("request deadline exceeded");
        }
        return (int) Math.min(upper, TimeUnit.NANOSECONDS.toMillis(remaining - 1) + 1);
    }

    private static long retryAfter(HttpResponse response) {
        Header header = response.getFirstHeader("Retry-After");
        if (header == null) {
            return 0;
        }
        String delay = header.getValue().trim();
        if (delay.matches("[0-9]+")) {
            try {
                return TimeUnit.SECONDS.toMillis(Long.parseLong(delay));
            } catch (NumberFormatException e) {
                return Long.MAX_VALUE;
            }
        }
        Date date = DateUtils.parseDate(delay);
        return date == null ? 0 : Math.max(0, date.getTime() - System.currentTimeMillis());
    }

    /**
     * Close the active TCP connection before the reader cancels its worker so a TLS handshake or
     * body read can return promptly.
     */
    void close() {
        closed = true;
        closeSocket(socket.getAndSet(null));
    }

    private static void closeSocket(Socket socket) {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // A canceled operation never reuses its connection.
            }
        }
    }

    /**
     * Carries a validated resource snapshot or a bounded retry instruction whose reason comes from
     * a fixed set of sanitized response categories.
     */
    private static final class Reply {
        private final AlphaVantageResource.Snapshot snapshot;
        private final String reason;
        private final long delay;

        private Reply(AlphaVantageResource.Snapshot snapshot, String reason, long delay) {
            this.snapshot = snapshot;
            this.reason = reason;
            this.delay = delay;
        }

        private static Reply success(AlphaVantageResource.Snapshot snapshot) {
            return new Reply(snapshot, null, 0);
        }

        private static Reply retry(String reason, long delay) {
            return new Reply(null, reason, delay);
        }
    }

    /**
     * Carries a safe validation or protocol error to the caller. The caller adds the resource name
     * without including the HTTP request or response.
     */
    private static final class RequestFailure extends IOException {
        private RequestFailure(String message) {
            super(message);
        }
    }

    /**
     * Signals that a wire or decoded input stream exceeded its own byte limit, allowing both paths
     * to use the same predictable source error.
     */
    private static final class BodyLimit extends IOException {}

    /**
     * Applies a byte limit and keeps the JSON parser from draining the HTTP body on close. The
     * socket remains owned by the request.
     */
    private static final class BoundedInput extends FilterInputStream {
        private int remaining;
        private final boolean closeDelegate;

        private BoundedInput(InputStream source, int limit, boolean closeDelegate) {
            super(source);
            this.remaining = limit;
            this.closeDelegate = closeDelegate;
        }

        @Override
        public int read() throws IOException {
            int value = in.read();
            if (value >= 0 && remaining-- == 0) {
                throw new BodyLimit();
            }
            return value;
        }

        @Override
        public int read(byte[] bytes, int offset, int count) throws IOException {
            int size = in.read(bytes, offset, Math.min(count, remaining + 1));
            remaining -= Math.max(0, size);
            if (remaining < 0) {
                throw new BodyLimit();
            }
            return size;
        }

        @Override
        public void close() throws IOException {
            if (closeDelegate) {
                super.close();
            }
        }
    }
}
