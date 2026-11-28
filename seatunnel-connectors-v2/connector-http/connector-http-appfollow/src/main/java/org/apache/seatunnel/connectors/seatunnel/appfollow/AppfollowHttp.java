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

package org.apache.seatunnel.connectors.seatunnel.appfollow;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;

import org.apache.seatunnel.connectors.seatunnel.appfollow.AppfollowData.Batch;
import org.apache.seatunnel.connectors.seatunnel.appfollow.AppfollowData.Request;

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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

/**
 * Executes AppFollow GETs with bounded socket lifetime, payload, and billed attempt budgets. Error
 * messages omit request details, headers and provider bodies.
 */
final class AppfollowHttp {
    private static final Pattern NEXT =
            Pattern.compile(
                    "(?:^|;)\\s*rel\\s*=\\s*(?:\"(?:[^\"]*\\s)?next(?:\\s[^\"]*)?\"|next(?=\\s|;|,|$))",
                    Pattern.CASE_INSENSITIVE);
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

    static {
        JSON.getFactory()
                .setStreamReadConstraints(
                        StreamReadConstraints.builder()
                                .maxNestingDepth(64)
                                .maxStringLength(33554432)
                                .maxNumberLength(1024)
                                .build());
    }

    private final AppfollowConfig config;
    private final AtomicReference<Socket> active = new AtomicReference<>();
    private volatile boolean closed;
    private int attemptsUsed;

    AppfollowHttp(AppfollowConfig config) {
        this.config = config;
    }

    /**
     * Shares the attempt count across discovery, selected endpoints, and transient retries.
     * Payment, permission and dependency errors cannot safely be retried automatically.
     */
    Batch fetch(Request query, long deadline) throws IOException {
        long retry = 0;
        for (int attempt = 1; ; attempt++) {
            if (attemptsUsed >= config.maxRequests) {
                throw failure(query, "request budget exhausted (max_requests)");
            }
            waitForRetry(query, deadline, retry);
            attemptsUsed++;
            try {
                return exchange(query, deadline);
            } catch (Retryable e) {
                if (attempt == config.maxAttempts) {
                    throw failure(query, "retry limit reached after HTTP " + e.status);
                }
                long base = 500L << Math.min(5, attempt - 1);
                retry = Math.max(e.delay, ThreadLocalRandom.current().nextLong(base, 2 * base));
            }
        }
    }

    private Batch exchange(Request query, long deadline) throws IOException {
        Socket raw = new Socket();
        active.set(raw);
        try {
            if (closed) {
                throw failure(query, "request canceled");
            }
            String host = config.origin.getHost();
            int port = config.origin.getPort() < 0 ? 443 : config.origin.getPort();
            raw.connect(
                    new InetSocketAddress(host, port), timeout(deadline, config.connectTimeout));
            raw.setSoTimeout(timeout(deadline, config.readTimeout));
            SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory.getDefault();
            try (SSLSocket socket = (SSLSocket) factory.createSocket(raw, host, port, true)) {
                SSLParameters parameters = socket.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                socket.setSSLParameters(parameters);
                socket.setEnabledProtocols(
                        Arrays.stream(socket.getEnabledProtocols())
                                .filter(tls -> "TLSv1.2".equals(tls) || "TLSv1.3".equals(tls))
                                .toArray(String[]::new));
                socket.startHandshake();
                socket.setSoTimeout(timeout(deadline, config.readTimeout));
                DefaultBHttpClientConnection connection = CONNECTIONS.createConnection(socket);
                BasicHttpRequest request = new BasicHttpRequest("GET", query.target());
                request.setHeader("Host", config.origin.getRawAuthority());
                request.setHeader("X-AppFollow-API-Token", config.token);
                request.setHeader("Accept", "application/json");
                request.setHeader("Accept-Encoding", "gzip");
                request.setHeader("Connection", "close");
                request.setHeader("User-Agent", "Apache-SeaTunnel-Appfollow");
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
                if (status == 206 || (status == 200 && continuation(response))) {
                    throw failure(query, "unexpected continuation for an unpaginated resource");
                }
                if (status != 200) {
                    throw failure(query, statusMessage(status));
                }
                try {
                    Batch batch = AppfollowData.parse(query, decode(response), config);
                    if (System.nanoTime() - deadline >= 0) {
                        throw new SafeFailure("request deadline exceeded");
                    }
                    return batch;
                } catch (IOException safe) {
                    throw failure(
                            query,
                            System.nanoTime() - deadline >= 0
                                    ? "request deadline exceeded"
                                    : safe.getMessage());
                }
            }
        } catch (SafeFailure | Retryable safe) {
            throw safe;
        } catch (IOException | HttpException | IllegalArgumentException ignored) {
            throw failure(
                    query,
                    System.nanoTime() - deadline >= 0
                            ? "request deadline exceeded"
                            : (closed ? "request canceled" : "HTTPS or TLS exchange failed"));
        } finally {
            active.compareAndSet(raw, null);
            closeSocket(raw);
        }
    }

    private static String statusMessage(int status) {
        switch (status) {
            case 401:
            case 403:
                return "HTTP " + status + " (authentication or permission failure)";
            case 402:
                return "HTTP 402 (API credit or subscription requirement)";
            case 400:
            case 422:
                return "HTTP " + status + " (invalid AppFollow request)";
            case 424:
                return "HTTP 424 (required AppFollow integration or dependency failed)";
            default:
                return "HTTP " + status;
        }
    }

    private static boolean continuation(HttpResponse response) {
        if (response.containsHeader("Content-Range")) {
            return true;
        }
        for (Header link : response.getHeaders("Link")) {
            if (NEXT.matcher(link.getValue()).find()) {
                return true;
            }
        }
        for (String name : new String[] {"X-Next-Page", "X-Next-Cursor", "X-Next-Page-Token"}) {
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
        Header[] transfer = response.getHeaders("Transfer-Encoding");
        Header[] encoding = response.getHeaders("Content-Encoding");
        HttpEntity entity = response.getEntity();
        if (entity == null
                || lengths.length > 1
                || transfer.length > 1
                || encoding.length > 1
                || (lengths.length != 0 && transfer.length != 0)
                || (transfer.length != 0 && !"chunked".equalsIgnoreCase(transfer[0].getValue()))) {
            throw new SafeFailure("invalid response framing");
        }
        if (entity.getContentLength() > config.maxResponse) {
            throw new SafeFailure("response exceeds max_response_bytes");
        }
        String coding = encoding.length == 0 ? "identity" : encoding[0].getValue().trim();
        if (!"identity".equalsIgnoreCase(coding) && !"gzip".equalsIgnoreCase(coding)) {
            throw new SafeFailure("unsupported response encoding");
        }
        try {
            InputStream raw = new BoundedBody(entity.getContent(), config.maxResponse);
            if ("identity".equalsIgnoreCase(coding)) {
                return JSON.readTree(raw);
            }
            try (GZIPInputStream gzip = new GZIPInputStream(raw)) {
                JsonNode parsed = JSON.readTree(new BoundedBody(gzip, config.maxResponse));
                if (raw.read() >= 0) {
                    throw new SafeFailure("unexpected trailing compressed response bytes");
                }
                return parsed;
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
            Date date = DateUtils.parseDate(value);
            return date == null ? 0 : Math.max(0, date.getTime() - System.currentTimeMillis());
        }
    }

    private static void waitForRetry(Request query, long deadline, long delay) throws IOException {
        long nanos = TimeUnit.MILLISECONDS.toNanos(delay);
        if (deadline - System.nanoTime() <= nanos) {
            throw failure(
                    query,
                    delay == 0
                            ? "request deadline exceeded"
                            : "retry delay exceeds request deadline");
        }
        try {
            TimeUnit.NANOSECONDS.sleep(nanos);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw failure(query, "request canceled");
        }
    }

    private static int timeout(long deadline, int max) throws IOException {
        long left = deadline - System.nanoTime();
        if (left <= 0) {
            throw new IOException("deadline");
        }
        return (int) Math.min(max, Math.max(1, TimeUnit.NANOSECONDS.toMillis(left)));
    }

    static SafeFailure failure(Request query, String reason) {
        return new SafeFailure("AppFollow source " + query.resource.key() + ": " + reason);
    }

    /**
     * Cancels network work by closing the active socket from the source lifecycle thread. A request
     * racing with close checks the same closed flag before dialing.
     */
    void close() {
        closed = true;
        closeSocket(active.getAndSet(null));
    }

    private static void closeSocket(Socket socket) {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // The socket is already being discarded.
            }
        }
    }

    /**
     * Carries an explicit remote retry decision while dropping all potentially sensitive content. A
     * retry remains subject to each deadline, attempt ceiling and the shared budget.
     */
    private static final class Retryable extends IOException {
        private final int status;
        private final long delay;

        private Retryable(int status, long delay) {
            this.status = status;
            this.delay = delay;
        }
    }

    /**
     * Carries source-generated and sanitized failures to the reader workflow. Provider response
     * text and request values never become a cause.
     */
    static final class SafeFailure extends IOException {
        private SafeFailure(String detail) {
            super(detail);
        }
    }

    /**
     * Indicates that an encoded or decoded input consumed more than its response allowance. Both
     * cases use the same fixed message without quoting any provider bytes.
     */
    private static final class BodyLimit extends IOException {}

    /**
     * Reads a bounded amount from one response layer without draining it on close. Request cleanup
     * owns the socket and compressed decoding closes its inflater explicitly.
     */
    private static final class BoundedBody extends FilterInputStream {
        private int remaining;

        private BoundedBody(InputStream in, int remaining) {
            super(in);
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
        public int read(byte[] bytes, int offset, int count) throws IOException {
            int used = in.read(bytes, offset, Math.min(count, remaining + 1));
            remaining -= Math.max(0, used);
            if (remaining < 0) {
                throw new BodyLimit();
            }
            return used;
        }

        @Override
        public void close() {
            // The enclosing request closes its socket.
        }
    }
}
