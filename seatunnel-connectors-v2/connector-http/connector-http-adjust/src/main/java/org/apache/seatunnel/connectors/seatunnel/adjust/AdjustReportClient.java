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

package org.apache.seatunnel.connectors.seatunnel.adjust;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

/** Executes a report GET without redirect, URL, header or body logging. */
final class AdjustReportClient implements AutoCloseable {
    private static final int BODY_LIMIT = 8 * 1024 * 1024;
    private static final ObjectMapper REPORT_JSON =
            new ObjectMapper(
                            JsonFactory.builder()
                                    .streamReadConstraints(
                                            StreamReadConstraints.builder()
                                                    .maxNestingDepth(64)
                                                    .maxNumberLength(512)
                                                    .maxStringLength(2 * 1024 * 1024)
                                                    .build())
                                    .build())
                    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                    .enable(
                            DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS,
                            DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private static final DefaultBHttpClientConnectionFactory CONNECTIONS =
            new DefaultBHttpClientConnectionFactory(
                    ConnectionConfig.custom()
                            .setMessageConstraints(
                                    MessageConstraints.custom()
                                            .setMaxHeaderCount(48)
                                            .setMaxLineLength(12288)
                                            .build())
                            .build());

    private final AdjustReportPlan plan;
    private Socket connected;
    private boolean closed;

    AdjustReportClient(AdjustReportPlan plan) {
        this.plan = plan;
    }

    /** Retries only expected transient statuses of this read-only request. */
    JsonNode fetch(LocalDate day, long deadline) throws IOException, InterruptedException {
        String target = plan.target(day);
        for (int attempt = 1; attempt <= plan.maxAttempts; attempt++) {
            try {
                return roundTrip(target, deadline);
            } catch (RetryableStatus status) {
                if (attempt == plan.maxAttempts) {
                    throw problem("HTTP " + status.code + " exhausted the configured attempts");
                }
                long exponential = 100L << (attempt - 1);
                long delay =
                        Math.max(
                                status.delay,
                                exponential + ThreadLocalRandom.current().nextLong(exponential));
                if (delay >= remainingMillis(deadline)) {
                    throw problem(
                            "HTTP " + status.code + " retry delay exceeds the report day deadline");
                }
                Thread.sleep(delay);
            }
        }
        throw new IllegalStateException("No Adjust HTTP attempt was made");
    }

    private JsonNode roundTrip(String target, long deadline) throws IOException {
        try (Socket connection = new Socket(Proxy.NO_PROXY)) {
            remember(connection);
            boolean tls = "https".equals(plan.origin.getScheme());
            String host = plan.origin.getHost();
            if (host.startsWith("[")) {
                host = host.substring(1, host.length() - 1);
            }
            int port = plan.origin.getPort() < 0 ? (tls ? 443 : 80) : plan.origin.getPort();
            InetSocketAddress address = new InetSocketAddress(host, port);
            connection.connect(address, Math.min(remainingMillis(deadline), 10000));
            connection.setSoTimeout(remainingMillis(deadline));
            if (tls) {
                SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory.getDefault();
                try (SSLSocket secured =
                        (SSLSocket) factory.createSocket(connection, host, port, true)) {
                    SSLParameters parameters = secured.getSSLParameters();
                    parameters.setEndpointIdentificationAlgorithm("HTTPS");
                    secured.setSSLParameters(parameters);
                    secured.startHandshake();
                    return exchange(secured, target);
                }
            }
            return exchange(connection, target);
        } catch (ReportFailure | RetryableStatus e) {
            throw e;
        } catch (SSLException e) {
            throw problem("TLS transport failed");
        } catch (SocketTimeoutException e) {
            throw problem("report day deadline exceeded");
        } catch (IOException | HttpException e) {
            throw problem("transport failed or returned invalid HTTP framing");
        } finally {
            synchronized (this) {
                connected = null;
            }
        }
    }

    private synchronized void remember(Socket connection) throws ReportFailure {
        if (closed || Thread.currentThread().isInterrupted()) {
            throw problem("request cancelled");
        }
        connected = connection;
    }

    private JsonNode exchange(Socket connection, String target) throws IOException, HttpException {
        BasicHttpRequest request = new BasicHttpRequest("GET", target, HttpVersion.HTTP_1_1);
        request.setHeader("Host", plan.origin.getRawAuthority());
        request.setHeader("Connection", "close");
        request.setHeader("Accept", "application/json");
        request.setHeader("Accept-Encoding", "gzip, deflate");
        request.setHeader("Authorization", "Bearer " + plan.token);

        HttpResponse response =
                new HttpRequestExecutor()
                        .execute(
                                request,
                                CONNECTIONS.createConnection(connection),
                                HttpCoreContext.create());
        int status = response.getStatusLine().getStatusCode();
        switch (status) {
            case 204:
                return null;
            case 200:
                return responseBody(response);
            case 429:
            case 500:
            case 502:
            case 503:
            case 504:
                throw new RetryableStatus(status, retryAfter(oneHeader(response, "Retry-After")));
            default:
                throw problem("unexpected HTTP " + status);
        }
    }

    private static JsonNode responseBody(HttpResponse response) throws IOException {
        String length = oneHeader(response, "Content-Length");
        String transfer = oneHeader(response, "Transfer-Encoding");
        if (length != null
                && (!length.matches("[0-9]{1,18}") || Long.parseLong(length) > BODY_LIMIT)) {
            throw problem("invalid or oversized response length");
        }
        if (transfer != null && (length != null || !"chunked".equalsIgnoreCase(transfer))) {
            throw problem("invalid response framing");
        }
        HttpEntity entity = response.getEntity();
        if (entity == null) {
            throw problem("missing report body");
        }
        InputStream wire = new LimitedBody(entity.getContent());
        try (InputStream content = decoded(wire, oneHeader(response, "Content-Encoding"))) {
            JsonNode root = REPORT_JSON.readTree(new LimitedBody(content));
            if (root == null || !root.isObject()) {
                throw problem("report must be a JSON object");
            }
            return root;
        } catch (ReportFailure | SocketTimeoutException e) {
            throw e;
        } catch (IOException e) {
            throw problem("invalid JSON report or compression");
        }
    }

    private static InputStream decoded(InputStream body, String encoding) throws IOException {
        if (encoding == null || encoding.equalsIgnoreCase("identity")) {
            return body;
        }
        if (encoding.equalsIgnoreCase("gzip")) {
            return new GZIPInputStream(body);
        }
        if (encoding.equalsIgnoreCase("deflate")) {
            return new DeflateInputStream(body);
        }
        throw problem("unsupported response encoding");
    }

    private static String oneHeader(HttpResponse response, String key) throws ReportFailure {
        Header[] headers = response.getHeaders(key);
        if (headers.length > 1) {
            throw problem("response contains a duplicate framing or Retry-After header");
        }
        return headers.length == 0 ? null : headers[0].getValue().trim();
    }

    private static long retryAfter(String value) {
        if (value == null) {
            return 0;
        }
        try {
            if (value.matches("[0-9]+")) {
                return Math.multiplyExact(Long.parseLong(value), 1000L);
            }
            Instant due =
                    ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant();
            Duration wait = Duration.between(Instant.now(), due);
            return wait.isNegative() ? 0 : wait.plusNanos(999999).toMillis();
        } catch (DateTimeParseException e) {
            return 0;
        } catch (NumberFormatException | ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    static int remainingMillis(long deadline) throws ReportFailure {
        long nanos = deadline - System.nanoTime();
        if (nanos <= 0) {
            throw problem("report day deadline exceeded");
        }
        return (int) Math.max(1L, TimeUnit.NANOSECONDS.toMillis(nanos));
    }

    @Override
    public synchronized void close() {
        closed = true;
        if (connected != null) {
            try {
                connected.close();
            } catch (IOException ignored) {
                // Closing the same connection again on worker exit is safe.
            }
        }
    }

    static ReportFailure problem(String text) {
        return new ReportFailure("Adjust Source: " + text);
    }

    static final class ReportFailure extends IOException {
        private static final long serialVersionUID = 1L;

        private ReportFailure(String text) {
            super(text);
        }
    }

    private static final class RetryableStatus extends IOException {
        private static final long serialVersionUID = 1L;
        private final int code;
        private final long delay;

        private RetryableStatus(int code, long delay) {
            this.code = code;
            this.delay = delay;
        }
    }

    private static final class LimitedBody extends FilterInputStream {
        private int remaining = BODY_LIMIT;

        private LimitedBody(InputStream body) {
            super(body);
        }

        @Override
        public int read() throws IOException {
            int value = in.read();
            if (value != -1) {
                consumed(1);
            }
            return value;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            int count = in.read(bytes, offset, Math.min(length, remaining + 1));
            if (count > 0) {
                consumed(count);
            }
            return count;
        }

        private void consumed(int count) throws ReportFailure {
            remaining -= count;
            if (remaining < 0) {
                throw problem("report body exceeds 8 MiB");
            }
        }

        @Override
        public void close() {
            // The exchange owns the socket. Do not drain a rejected HTTP response.
        }
    }
}
