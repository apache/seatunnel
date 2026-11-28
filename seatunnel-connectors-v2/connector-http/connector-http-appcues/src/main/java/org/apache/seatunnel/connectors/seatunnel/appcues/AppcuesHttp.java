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

package org.apache.seatunnel.connectors.seatunnel.appcues;

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
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

/**
 * Reads Appcues list responses within a single wall clock budget including any retries. It owns the
 * active socket so cancellation works even during an HTTPS body read.
 */
final class AppcuesHttp {
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                    .disable(JsonParser.Feature.AUTO_CLOSE_SOURCE)
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
    private static final Pattern NEXT_LINK =
            Pattern.compile(
                    "(?:^|;)\\s*rel\\s*=\\s*(?:\"(?:[^\"]*\\s)?next(?:\\s[^\"]*)?\"|next(?=\\s|;|,|$))",
                    Pattern.CASE_INSENSITIVE);

    static {
        JSON.getFactory()
                .setStreamReadConstraints(
                        StreamReadConstraints.builder()
                                .maxNestingDepth(64)
                                .maxStringLength(33554432)
                                .maxNumberLength(1024)
                                .build());
    }

    private final AppcuesConfig config;
    private final AtomicReference<Socket> current = new AtomicReference<>();
    private volatile boolean closed;

    AppcuesHttp(AppcuesConfig config) {
        this.config = config;
    }

    /**
     * Retries explicit temporary statuses within the original deadline and attempt limit. Responses
     * and exception causes stay out of the logged failure.
     */
    AppcuesResource.Snapshot fetch(AppcuesResource resource, Instant end, long deadline)
            throws IOException {
        for (int attempt = 1; ; attempt++) {
            try {
                return request(resource, end, deadline);
            } catch (Retryable e) {
                if (attempt == config.maxAttempts) {
                    throw failure(resource, "retry limit reached after HTTP " + e.status);
                }
                long baseline = 500L << Math.min(attempt - 1, 5);
                long delay =
                        Math.max(
                                e.retryAfter,
                                ThreadLocalRandom.current().nextLong(baseline, baseline * 2));
                sleep(resource, deadline, delay);
            }
        }
    }

    private AppcuesResource.Snapshot request(AppcuesResource resource, Instant end, long deadline)
            throws IOException {
        Socket raw = new Socket();
        current.set(raw);
        try {
            if (closed) {
                throw failure(resource, "request canceled");
            }
            String host = config.origin.getHost();
            int port = config.origin.getPort() < 0 ? 443 : config.origin.getPort();
            raw.connect(
                    new InetSocketAddress(host, port), timeout(deadline, config.connectTimeout));
            raw.setSoTimeout(timeout(deadline, config.readTimeout));
            SSLSocketFactory sockets = (SSLSocketFactory) SSLSocketFactory.getDefault();
            try (SSLSocket ssl = (SSLSocket) sockets.createSocket(raw, host, port, true)) {
                SSLParameters parameters = ssl.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                ssl.setSSLParameters(parameters);
                ssl.setEnabledProtocols(
                        Arrays.stream(ssl.getEnabledProtocols())
                                .filter(
                                        version ->
                                                "TLSv1.2".equals(version)
                                                        || "TLSv1.3".equals(version))
                                .toArray(String[]::new));
                ssl.startHandshake();
                ssl.setSoTimeout(timeout(deadline, config.readTimeout));
                DefaultBHttpClientConnection connection = CONNECTIONS.createConnection(ssl);
                BasicHttpRequest request = new BasicHttpRequest("GET", config.target(resource));
                request.setHeader("Host", config.origin.getRawAuthority());
                request.setHeader("Authorization", config.authorization);
                request.setHeader("Accept", "application/json");
                request.setHeader("Accept-Encoding", "gzip");
                request.setHeader("Connection", "close");
                request.setHeader("User-Agent", "Apache-SeaTunnel-Appcues");
                HttpResponse response =
                        new HttpRequestExecutor()
                                .execute(request, connection, new BasicHttpContext());
                int status = response.getStatusLine().getStatusCode();
                if (status == 429
                        || status == 500
                        || status == 502
                        || status == 503
                        || status == 504) {
                    throw new Retryable(status, retryAfter(response));
                }
                if (status == 206) {
                    throw failure(resource, "unexpected continuation for an unpaginated resource");
                }
                if (status != 200) {
                    throw failure(resource, "HTTP " + status);
                }
                if (hasContinuation(response)) {
                    throw failure(resource, "unexpected continuation for an unpaginated resource");
                }
                try {
                    AppcuesResource.Snapshot snapshot =
                            resource.snapshot(
                                    decode(response), config.start, end, config.maxRecords);
                    if (System.nanoTime() - deadline >= 0) {
                        throw new SafeFailure("request deadline exceeded");
                    }
                    return snapshot;
                } catch (IOException safe) {
                    throw failure(
                            resource,
                            System.nanoTime() - deadline >= 0
                                    ? "request deadline exceeded"
                                    : safe.getMessage());
                }
            }
        } catch (Retryable | SafeFailure safe) {
            throw safe;
        } catch (IOException | HttpException | IllegalArgumentException ignored) {
            String reason =
                    System.nanoTime() - deadline >= 0
                            ? "request deadline exceeded"
                            : (closed ? "request canceled" : "HTTPS or TLS exchange failed");
            throw failure(resource, reason);
        } finally {
            current.compareAndSet(raw, null);
            closeQuietly(raw);
        }
    }

    private static boolean hasContinuation(HttpResponse response) {
        if (response.containsHeader("Content-Range")) {
            return true;
        }
        for (Header link : response.getHeaders("Link")) {
            if (NEXT_LINK.matcher(link.getValue()).find()) {
                return true;
            }
        }
        for (String name :
                new String[] {
                    "X-Next-Page", "X-Next-Page-Token", "X-Next-Cursor", "Next-Page-Token"
                }) {
            for (Header header : response.getHeaders(name)) {
                if (!header.getValue().trim().isEmpty()) {
                    return true;
                }
            }
        }
        return false;
    }

    private JsonNode decode(HttpResponse response) throws IOException {
        Header[] lengths = response.getHeaders("Content-Length");
        Header[] transfers = response.getHeaders("Transfer-Encoding");
        Header[] encodings = response.getHeaders("Content-Encoding");
        HttpEntity entity = response.getEntity();
        if (entity == null
                || lengths.length > 1
                || transfers.length > 1
                || encodings.length > 1
                || (lengths.length != 0 && transfers.length != 0)
                || (transfers.length != 0
                        && !"chunked".equalsIgnoreCase(transfers[0].getValue()))) {
            throw new SafeFailure("invalid response framing");
        }
        if (entity.getContentLength() > config.maxResponse) {
            throw new SafeFailure("response exceeds max_response_bytes");
        }
        String encoding = encodings.length == 0 ? "identity" : encodings[0].getValue().trim();
        if (!"identity".equalsIgnoreCase(encoding) && !"gzip".equalsIgnoreCase(encoding)) {
            throw new SafeFailure("unsupported response encoding");
        }
        try {
            InputStream wire = new BoundedBody(entity.getContent(), config.maxResponse);
            if ("identity".equalsIgnoreCase(encoding)) {
                return JSON.readTree(wire);
            }
            try (GZIPInputStream gzip = new GZIPInputStream(wire)) {
                JsonNode root = JSON.readTree(new BoundedBody(gzip, config.maxResponse));
                if (wire.read() >= 0) {
                    throw new SafeFailure("unexpected trailing compressed response bytes");
                }
                return root;
            }
        } catch (BodyLimit ignored) {
            throw new SafeFailure("response exceeds max_response_bytes");
        } catch (SafeFailure safe) {
            throw safe;
        } catch (IOException ignored) {
            throw new SafeFailure("invalid, incomplete or unreadable JSON response");
        }
    }

    private static long retryAfter(HttpResponse response) {
        Header header = response.getFirstHeader("Retry-After");
        if (header == null) {
            return 0;
        }
        String value = header.getValue().trim();
        try {
            return TimeUnit.SECONDS.toMillis(Math.max(0, Long.parseLong(value)));
        } catch (NumberFormatException ignored) {
            Date time = DateUtils.parseDate(value);
            return time == null ? 0 : Math.max(0, time.getTime() - System.currentTimeMillis());
        }
    }

    private static void sleep(AppcuesResource resource, long deadline, long delay)
            throws IOException {
        long wait = TimeUnit.MILLISECONDS.toNanos(delay);
        if (deadline - System.nanoTime() <= wait) {
            throw failure(resource, "retry delay exceeds request deadline");
        }
        try {
            TimeUnit.NANOSECONDS.sleep(wait);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw failure(resource, "request canceled");
        }
    }

    private static int timeout(long deadline, int ceiling) throws IOException {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
            throw new IOException("deadline");
        }
        return (int) Math.min(ceiling, Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)));
    }

    /**
     * Closes the socket to interrupt any pending connect, handshake or read. A racing socket
     * creation checks the closed flag before connecting.
     */
    void close() {
        closed = true;
        closeQuietly(current.getAndSet(null));
    }

    private static void closeQuietly(Socket socket) {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // This is already the final cleanup of the request.
            }
        }
    }

    static SafeFailure failure(AppcuesResource resource, String reason) {
        return new SafeFailure("Appcues source " + resource.key() + ": " + reason);
    }

    /**
     * Carries a temporary status and its suggested retry delay without the provider response body.
     * The caller still applies the total deadline and attempt budget.
     */
    private static final class Retryable extends IOException {
        private final int status;
        private final long retryAfter;

        private Retryable(int status, long retryAfter) {
            this.status = status;
            this.retryAfter = retryAfter;
        }
    }

    /**
     * Carries an error message constructed from a resource name and fixed reason. Request targets,
     * sensitive headers and provider responses stay out of the exception chain.
     */
    static final class SafeFailure extends IOException {
        private SafeFailure(String message) {
            super(message);
        }
    }

    /**
     * Signals a byte-limit violation at the wire or decoded layer of the HTTP response. The decoder
     * reports both cases as a configured resource bound.
     */
    private static final class BodyLimit extends IOException {}

    /**
     * Bounds the number of encoded or decoded bytes accepted from the provider. The socket stays
     * owned by the request, so closing a decoder does not drain the entity.
     */
    private static final class BoundedBody extends FilterInputStream {
        private int remaining;

        private BoundedBody(InputStream input, int remaining) {
            super(input);
            this.remaining = remaining;
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
        public int read(byte[] bytes, int offset, int length) throws IOException {
            int count = in.read(bytes, offset, Math.min(length, remaining + 1));
            remaining -= Math.max(0, count);
            if (remaining < 0) {
                throw new BodyLimit();
            }
            return count;
        }

        @Override
        public void close() {
            // Final socket disposal is handled in request's finally block.
        }
    }
}
