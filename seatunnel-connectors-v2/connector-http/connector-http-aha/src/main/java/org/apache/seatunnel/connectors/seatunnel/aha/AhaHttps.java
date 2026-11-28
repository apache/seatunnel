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

package org.apache.seatunnel.connectors.seatunnel.aha;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
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
import java.util.zip.GZIPInputStream;

/** Owns each HTTPS request and its deadline without exposing provider data in failures. */
final class AhaHttps {
    private static final DefaultBHttpClientConnectionFactory CONNECTIONS =
            new DefaultBHttpClientConnectionFactory(
                    ConnectionConfig.custom()
                            .setMessageConstraints(
                                    MessageConstraints.custom()
                                            .setMaxHeaderCount(64)
                                            .setMaxLineLength(16384)
                                            .build())
                            .build());
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                    .enable(
                            DeserializationFeature.FAIL_ON_TRAILING_TOKENS,
                            DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));

    static {
        JSON.getFactory()
                .setStreamReadConstraints(
                        StreamReadConstraints.builder()
                                .maxNestingDepth(100)
                                .maxNumberLength(1000)
                                .maxStringLength(16777216)
                                .build());
    }

    private final AhaConfig config;
    private final AtomicReference<Socket> active = new AtomicReference<>();
    private volatile boolean closed;

    AhaHttps(AhaConfig config) {
        this.config = config;
    }

    /** Runs a read-only request with a single budget for connection, TLS, body, and retries. */
    AhaResource.Page get(AhaResource endpoint, String target, long deadline) throws IOException {
        for (int attempt = 1; attempt <= config.maxAttempts; attempt++) {
            long delay =
                    ThreadLocalRandom.current().nextLong(100, 201)
                            * (1L << Math.min(attempt - 1, 4));
            int status;
            Socket raw = new Socket();
            active.set(raw);
            try {
                status = 0;
                if (closed) {
                    throw endpoint.problem("HTTPS request canceled");
                }
                raw.connect(
                        new InetSocketAddress(
                                config.origin.getHost(),
                                config.origin.getPort() == -1 ? 443 : config.origin.getPort()),
                        timeout(endpoint, deadline, config.connectTimeout));
                raw.setSoTimeout(timeout(endpoint, deadline, config.readTimeout));
                try (SSLSocket tls = tls(raw)) {
                    tls.startHandshake();
                    tls.setSoTimeout(timeout(endpoint, deadline, config.readTimeout));
                    DefaultBHttpClientConnection connection = CONNECTIONS.createConnection(tls);
                    BasicHttpRequest request = new BasicHttpRequest("GET", target);
                    request.setHeader("Host", config.origin.getRawAuthority());
                    request.setHeader("Authorization", "Bearer " + config.token);
                    request.setHeader("Accept", "application/json");
                    request.setHeader("Accept-Encoding", "gzip");
                    request.setHeader(
                            "User-Agent", "Apache-SeaTunnel-Aha (dev@seatunnel.apache.org)");
                    request.setHeader("Connection", "close");
                    HttpResponse response =
                            new HttpRequestExecutor()
                                    .execute(request, connection, new BasicHttpContext());
                    status = response.getStatusLine().getStatusCode();
                    if (status == 200) {
                        AhaResource.Page page = parse(endpoint, response);
                        timeout(endpoint, deadline, config.readTimeout);
                        return page;
                    }
                    if (status == 429
                            || status == 500
                            || status == 502
                            || status == 503
                            || status == 504) {
                        delay = Math.max(delay, retryDelay(response, status));
                    }
                }
            } catch (IOException | org.apache.http.HttpException e) {
                timeout(endpoint, deadline, config.readTimeout);
                if (e instanceof AhaFailure) {
                    throw (AhaFailure) e;
                }
                throw endpoint.problem(
                        closed ? "HTTPS request canceled" : "HTTPS or TLS exchange failed");
            } finally {
                active.compareAndSet(raw, null);
                closeSocket(raw);
            }
            if (status != 429 && status != 500 && status != 502 && status != 503 && status != 504) {
                throw endpoint.problem("HTTP " + status);
            }
            if (attempt == config.maxAttempts) {
                throw endpoint.problem("retry limit reached after HTTP " + status);
            }
            if (delay >= TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())) {
                throw endpoint.problem("retry delay exceeds request deadline after HTTP " + status);
            }
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw endpoint.problem("HTTPS request canceled");
            }
        }
        throw endpoint.problem("retry limit reached");
    }

    private SSLSocket tls(Socket raw) throws IOException {
        SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory.getDefault();
        SSLSocket tls =
                (SSLSocket) factory.createSocket(raw, config.origin.getHost(), raw.getPort(), true);
        SSLParameters settings = tls.getSSLParameters();
        settings.setEndpointIdentificationAlgorithm("HTTPS");
        settings.setProtocols(
                Arrays.stream(tls.getEnabledProtocols())
                        .filter(p -> "TLSv1.2".equals(p) || "TLSv1.3".equals(p))
                        .toArray(String[]::new));
        tls.setSSLParameters(settings);
        return tls;
    }

    private AhaResource.Page parse(AhaResource endpoint, HttpResponse response) throws IOException {
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
            throw new AhaFailure(endpoint, "invalid response framing");
        }
        if (entity.getContentLength() > config.maxBytes) {
            throw new AhaFailure(endpoint, "response exceeds max_response_bytes");
        }
        try {
            InputStream wire = new BoundedInput(entity.getContent(), config.maxBytes, false);
            String encoding = encodings.length == 0 ? "identity" : encodings[0].getValue().trim();
            InputStream body = wire;
            if ("gzip".equalsIgnoreCase(encoding)) {
                body = new BoundedInput(new GZIPInputStream(wire), config.maxBytes, true);
            } else if (!"identity".equalsIgnoreCase(encoding)) {
                throw new AhaFailure(endpoint, "unsupported response encoding");
            }
            try (InputStream stream = body) {
                JsonNode root = JSON.readTree(stream);
                if (root == null || !root.isObject()) {
                    throw new AhaFailure(endpoint, "expected a JSON response object");
                }
                try {
                    return endpoint.parse(root, config.pageSize);
                } catch (IOException e) {
                    throw new AhaFailure(e.getMessage());
                }
            }
        } catch (BodyLimit e) {
            throw new AhaFailure(endpoint, "response exceeds max_response_bytes");
        } catch (AhaFailure e) {
            throw e;
        } catch (IOException e) {
            throw new AhaFailure(endpoint, "invalid, incomplete, or unreadable JSON response");
        }
    }

    private static long retryDelay(HttpResponse response, int status) {
        long now = System.currentTimeMillis();
        Header retry = response.getFirstHeader("Retry-After");
        long delay = 0;
        if (retry != null) {
            String value = retry.getValue().trim();
            if (value.matches("[0-9]+")) {
                delay = TimeUnit.SECONDS.toMillis(seconds(value));
            } else {
                Date time = DateUtils.parseDate(value);
                if (time != null && time.getTime() > now) {
                    delay = time.getTime() - now;
                }
            }
        }
        Header reset = response.getFirstHeader("X-Ratelimit-Reset");
        if (status == 429 && reset != null && reset.getValue().trim().matches("[0-9]+")) {
            long resetMillis = TimeUnit.SECONDS.toMillis(seconds(reset.getValue().trim()));
            delay = Math.max(delay, resetMillis - now);
        }
        return delay;
    }

    private static long seconds(String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return Long.MAX_VALUE;
        }
    }

    private static int timeout(AhaResource endpoint, long deadline, int requested)
            throws AhaFailure {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
            throw new AhaFailure(endpoint, "request deadline exceeded");
        }
        long millis = TimeUnit.NANOSECONDS.toMillis(remaining - 1) + 1;
        return (int) Math.min(requested, millis);
    }

    void close() {
        closed = true;
        closeSocket(active.getAndSet(null));
    }

    private static void closeSocket(Socket socket) {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // Cancellation has no subsequent use for this connection.
            }
        }
    }

    private static final class AhaFailure extends IOException {
        private AhaFailure(AhaResource endpoint, String message) {
            this(endpoint.problem(message).getMessage());
        }

        private AhaFailure(String message) {
            super(message);
        }
    }

    private static final class BodyLimit extends IOException {}

    private static final class BoundedInput extends FilterInputStream {
        private final boolean closeDelegate;
        private int remaining;

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
        public int read(byte[] bytes, int offset, int length) throws IOException {
            int count = in.read(bytes, offset, Math.min(length, remaining + 1));
            remaining -= Math.max(0, count);
            if (remaining < 0) {
                throw new BodyLimit();
            }
            return count;
        }

        @Override
        public void close() throws IOException {
            if (closeDelegate) {
                super.close();
            }
            // The owning socket closes the wire stream without draining the remaining entity.
        }
    }
}
