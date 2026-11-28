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

package org.apache.seatunnel.connectors.seatunnel.awscloudtrail;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.awscloudtrail.AwsCloudTrailConfig.Window;
import org.apache.seatunnel.connectors.seatunnel.awscloudtrail.AwsCloudTrailJson.Failure;
import org.apache.seatunnel.connectors.seatunnel.awscloudtrail.AwsCloudTrailJson.Page;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.client.utils.DateUtils;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.MessageConstraints;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.DefaultBHttpClientConnection;
import org.apache.http.impl.DefaultBHttpClientConnectionFactory;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpRequestExecutor;

import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPInputStream;

/**
 * Issues only signed LookupEvents requests to the configured regional AWS hostname. Pacing includes
 * every retry, and cancellation closes the socket independently of the reader lock.
 */
final class AwsCloudTrailHttp implements AutoCloseable {
    private static final long INITIAL_RETRY_MS = 500;
    private static final long REQUEST_GAP_NANOS = TimeUnit.MILLISECONDS.toNanos(510);
    private static final int ERROR_BYTES = 8192;
    private static final Set<String> RETRY_CODES =
            Set.of(
                    "Throttling",
                    "ThrottlingException",
                    "RequestLimitExceeded",
                    "TooManyRequestsException",
                    "RequestTimeoutException");
    private static final Set<String> PUBLIC_CODES =
            Set.of(
                    "AccessDenied",
                    "AccessDeniedException",
                    "ExpiredTokenException",
                    "InvalidClientTokenId",
                    "InvalidLookupAttributesException",
                    "InvalidMaxResultsException",
                    "InvalidNextTokenException",
                    "InvalidTimeRangeException",
                    "InvalidEventCategoryException",
                    "InvalidSignatureException",
                    "UnrecognizedClientException",
                    "IncompleteSignature",
                    "SignatureDoesNotMatch",
                    "ValidationError",
                    "RequestExpired",
                    "ThrottlingException");
    private static final DefaultBHttpClientConnectionFactory CONNECTIONS =
            new DefaultBHttpClientConnectionFactory(
                    ConnectionConfig.custom()
                            .setMessageConstraints(
                                    MessageConstraints.custom()
                                            .setMaxHeaderCount(64)
                                            .setMaxLineLength(8192)
                                            .build())
                            .build());

    private final AwsCloudTrailConfig config;
    private final AwsCloudTrailSigner signer;
    private final AtomicReference<Socket> connection = new AtomicReference<>();
    private volatile boolean closed;
    private long lastSend;
    private boolean hasSent;

    AwsCloudTrailHttp(AwsCloudTrailConfig config) {
        this.config = config;
        this.signer = new AwsCloudTrailSigner(config);
    }

    /**
     * Reuses the same encoded parameters across attempts while recomputing the timestamp signature.
     * Only documented throttling and temporary server statuses can retry within the deadline.
     */
    Page fetch(Window window, String token, BigDecimal preceding, long deadline)
            throws IOException {
        byte[] body = AwsCloudTrailJson.request(config, window, token);
        long backoff = 0;
        for (int attempt = 1; ; attempt++) {
            await(deadline, TimeUnit.MILLISECONDS.toNanos(backoff));
            if (!AwsCloudTrailConfig.isRetained(window, Instant.now())) {
                throw AwsCloudTrailJson.failure(
                        "the frozen window is outside the current 90-day history");
            }
            Reply reply = exchange(body, deadline);
            if (reply.events != null) {
                Page page = AwsCloudTrailJson.page(reply.events, config, window, preceding);
                if (System.nanoTime() - deadline >= 0) {
                    throw AwsCloudTrailJson.failure("request deadline exceeded");
                }
                return page;
            }
            boolean retry =
                    reply.status == 408
                            || reply.status == 429
                            || reply.status >= 500 && reply.status < 600
                            || reply.status == 400 && RETRY_CODES.contains(reply.code);
            if (!retry || attempt == config.maxAttempts) {
                String code = PUBLIC_CODES.contains(reply.code) ? " " + reply.code : "";
                throw AwsCloudTrailJson.failure(
                        (retry ? "retry limit reached after " : "")
                                + "HTTP "
                                + reply.status
                                + code);
            }
            long base = INITIAL_RETRY_MS << Math.min(attempt - 1, 5);
            backoff =
                    Math.max(
                            reply.retryAfter, ThreadLocalRandom.current().nextLong(base, base * 2));
        }
    }

    private Reply exchange(byte[] body, long deadline) throws IOException {
        Socket raw = new Socket();
        connection.set(raw);
        try {
            if (closed || Thread.currentThread().isInterrupted()) {
                throw AwsCloudTrailJson.failure("request canceled");
            }
            raw.connect(
                    new InetSocketAddress(config.host, 443),
                    timeout(deadline, config.connectTimeout));
            raw.setSoTimeout(timeout(deadline, config.readTimeout));
            SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory.getDefault();
            try (SSLSocket tls = (SSLSocket) factory.createSocket(raw, config.host, 443, true)) {
                SSLParameters parameters = tls.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                tls.setSSLParameters(parameters);
                tls.setEnabledProtocols(
                        Arrays.stream(tls.getEnabledProtocols())
                                .filter(
                                        protocol ->
                                                "TLSv1.2".equals(protocol)
                                                        || "TLSv1.3".equals(protocol))
                                .toArray(String[]::new));
                tls.startHandshake();
                long sinceLast = hasSent ? System.nanoTime() - lastSend : REQUEST_GAP_NANOS;
                // Measure actual request dispatch, since DNS and TLS handshakes take variable time.
                await(deadline, Math.max(0, REQUEST_GAP_NANOS - sinceLast));
                tls.setSoTimeout(timeout(deadline, config.readTimeout));
                DefaultBHttpClientConnection protocol = CONNECTIONS.createConnection(tls);
                BasicHttpEntityEnclosingRequest request =
                        new BasicHttpEntityEnclosingRequest("POST", "/");
                request.setEntity(new ByteArrayEntity(body));
                Map<String, String> headers = signer.sign(body, Instant.now());
                headers.forEach(request::setHeader);
                request.setHeader("Content-Length", Integer.toString(body.length));
                request.setHeader("Accept", "application/json");
                request.setHeader("Accept-Encoding", "gzip");
                request.setHeader("Connection", "close");
                request.setHeader("User-Agent", "Apache-SeaTunnel-AwsCloudTrail");
                lastSend = System.nanoTime();
                hasSent = true;
                HttpResponse result =
                        new HttpRequestExecutor()
                                .execute(request, protocol, new BasicHttpContext());
                int status = result.getStatusLine().getStatusCode();
                if (status != 200) {
                    return new Reply(null, status, errorCode(result), retryAfter(result));
                }
                ObjectNode events = AwsCloudTrailJson.parse(readBody(result, config.maxResponse));
                if (System.nanoTime() - deadline >= 0) {
                    throw new SafeFailure("request deadline exceeded");
                }
                return new Reply(events, status, "", 0);
            }
        } catch (SafeFailure e) {
            throw AwsCloudTrailJson.failure(e.getMessage());
        } catch (Failure e) {
            throw e;
        } catch (IOException | HttpException | IllegalArgumentException ignored) {
            if (System.nanoTime() - deadline >= 0) {
                throw AwsCloudTrailJson.failure("request deadline exceeded");
            }
            throw AwsCloudTrailJson.failure(
                    closed ? "request canceled" : "HTTPS or TLS exchange failed");
        } finally {
            connection.compareAndSet(raw, null);
            closeSocket(raw);
        }
    }

    private static byte[] readBody(HttpResponse response, int limit) throws IOException {
        HttpEntity entity = response.getEntity();
        Header[] lengths = response.getHeaders("Content-Length");
        Header[] transfer = response.getHeaders("Transfer-Encoding");
        Header[] encoding = response.getHeaders("Content-Encoding");
        if (entity == null
                || lengths.length > 1
                || transfer.length > 1
                || encoding.length > 1
                || response.containsHeader("Content-Range")
                || (transfer.length > 0
                        && (lengths.length > 0
                                || !"chunked".equalsIgnoreCase(transfer[0].getValue())))) {
            throw new SafeFailure("malformed or partial response framing");
        }
        if (entity.getContentLength() > limit) {
            throw new SafeFailure("response exceeds max_response_bytes");
        }
        InputStream wire = new Bounded(entity.getContent(), limit);
        String coding = encoding.length == 0 ? "identity" : encoding[0].getValue().trim();
        if ("identity".equalsIgnoreCase(coding)) {
            return wire.readAllBytes();
        }
        if (!"gzip".equalsIgnoreCase(coding)) {
            throw new SafeFailure("unsupported Content-Encoding");
        }
        try (InputStream gzip = new GZIPInputStream(wire)) {
            byte[] result = new Bounded(gzip, limit).readAllBytes();
            if (wire.read() != -1) {
                throw new SafeFailure("unexpected trailing compressed response bytes");
            }
            return result;
        }
    }

    private static String errorCode(HttpResponse response) {
        Header header = response.getFirstHeader("x-amzn-errortype");
        if (header != null) {
            return shortCode(header.getValue());
        }
        HttpEntity entity = response.getEntity();
        if (entity == null || entity.getContentLength() > ERROR_BYTES) {
            return "";
        }
        try {
            byte[] raw = readBody(response, ERROR_BYTES);
            ObjectNode body = AwsCloudTrailJson.parse(raw);
            String type = body.path("__type").asText("");
            return shortCode(type.isEmpty() ? body.path("code").asText("") : type);
        } catch (IOException ignored) {
            return "";
        }
    }

    private static String shortCode(String type) {
        String unqualified = type.substring(type.lastIndexOf('#') + 1);
        int delimiter = unqualified.indexOf(':');
        return delimiter < 0 ? unqualified : unqualified.substring(0, delimiter);
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

    private void await(long deadline, long delay) throws IOException {
        if (closed || Thread.currentThread().isInterrupted()) {
            throw AwsCloudTrailJson.failure("request canceled");
        }
        if (deadline - System.nanoTime() <= delay) {
            throw AwsCloudTrailJson.failure(
                    delay == 0
                            ? "request deadline exceeded"
                            : "pacing or retry delay exceeds request deadline");
        }
        try {
            TimeUnit.NANOSECONDS.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw AwsCloudTrailJson.failure("request canceled");
        }
    }

    private static int timeout(long deadline, int cap) throws IOException {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
            throw new SafeFailure("request deadline exceeded");
        }
        return (int) Math.min(cap, Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)));
    }

    /**
     * Stops requests without acquiring checkpoint progress or blocking on network cleanup. Closing
     * the same active socket more than once is harmless.
     */
    @Override
    public void close() {
        closed = true;
        closeSocket(connection.getAndSet(null));
    }

    private static void closeSocket(Socket raw) {
        if (raw != null) {
            try {
                raw.close();
            } catch (IOException ignored) {
                // The fixed request failure or cancellation is reported by the reader.
            }
        }
    }

    /**
     * Carries only validated success data or one sanitized status classification. AWS error
     * response content is never passed to the source reader.
     */
    private static final class Reply {
        final ObjectNode events;
        final int status;
        final String code;
        final long retryAfter;

        Reply(ObjectNode events, int status, String code, long retryAfter) {
            this.events = events;
            this.status = status;
            this.code = code;
            this.retryAfter = retryAfter;
        }
    }

    /**
     * Signals a fixed size, framing or deadline reason safe for job logs. The transport closes the
     * socket after every response.
     */
    private static final class SafeFailure extends IOException {
        SafeFailure(String reason) {
            super(reason);
        }
    }

    /**
     * Limits wire or decoded bytes without draining unfinished HTTP response entities. Underlying
     * streams are owned by the socket lifecycle.
     */
    private static final class Bounded extends FilterInputStream {
        private int remaining;

        Bounded(InputStream input, int limit) {
            super(input);
            remaining = limit;
        }

        @Override
        public int read() throws IOException {
            int value = in.read();
            if (value >= 0 && remaining-- == 0) {
                throw new SafeFailure("response exceeds max_response_bytes");
            }
            return value;
        }

        @Override
        public int read(byte[] values, int offset, int count) throws IOException {
            int size = in.read(values, offset, Math.min(count, remaining + 1));
            if (size > remaining) {
                throw new SafeFailure("response exceeds max_response_bytes");
            }
            if (size > 0) {
                remaining -= size;
            }
            return size;
        }

        @Override
        public void close() {}
    }
}
