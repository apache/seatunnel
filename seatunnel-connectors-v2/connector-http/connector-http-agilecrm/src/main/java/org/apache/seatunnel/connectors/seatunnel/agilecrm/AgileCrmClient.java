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

package org.apache.seatunnel.connectors.seatunnel.agilecrm;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;

import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.entity.DeflateInputStream;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.MessageConstraints;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.DefaultBHttpClientConnectionFactory;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

/** Covers only the fixed Agile CRM read routes with bounded, cancellable exchanges. */
final class AgileCrmClient implements AutoCloseable {
    private static final int MAX_BYTES = 8 * 1024 * 1024;
    private static final ObjectMapper JSON =
            new ObjectMapper(
                            JsonFactory.builder()
                                    .streamReadConstraints(
                                            StreamReadConstraints.builder()
                                                    .maxNestingDepth(64)
                                                    .maxNumberLength(512)
                                                    .maxStringLength(2 * 1024 * 1024)
                                                    .build())
                                    .build())
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true))
                    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                    .enable(
                            DeserializationFeature.FAIL_ON_TRAILING_TOKENS,
                            DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private static final DefaultBHttpClientConnectionFactory RAW_HTTP =
            new DefaultBHttpClientConnectionFactory(
                    ConnectionConfig.custom()
                            .setMessageConstraints(
                                    MessageConstraints.custom()
                                            .setMaxHeaderCount(64)
                                            .setMaxLineLength(16384)
                                            .build())
                            .build());

    private final AgileCrmSettings config;
    private boolean closed;
    private Socket active;

    AgileCrmClient(AgileCrmSettings config) {
        this.config = config;
    }

    JsonNode retrieve(AgileCrmResource.Request page, long deadline)
            throws IOException, InterruptedException {
        for (int retry = 0; retry < config.attempts; retry++) {
            try {
                return exchange(page, deadline);
            } catch (StatusRetry e) {
                if (retry + 1 == config.attempts) {
                    throw fail(
                            page.resource,
                            "HTTP " + e.status + " exhausted the configured attempts");
                }
                long minimum = 150L << retry;
                long delay =
                        Math.max(
                                e.retryAfter,
                                minimum + ThreadLocalRandom.current().nextLong(minimum));
                if (delay >= timeRemaining(page.resource, deadline)) {
                    throw fail(
                            page.resource, "Retry-After or backoff exceeds the request deadline");
                }
                Thread.sleep(delay);
            }
        }
        throw new IllegalStateException("No Agile CRM read attempt was made");
    }

    private JsonNode exchange(AgileCrmResource.Request page, long deadline) throws IOException {
        try (Socket plain = new Socket(Proxy.NO_PROXY)) {
            setActive(plain, page.resource);
            boolean tls = "https".equals(config.origin.getScheme());
            String host = config.origin.getHost();
            if (host.startsWith("[")) {
                host = host.substring(1, host.length() - 1);
            }
            int port = config.origin.getPort();
            if (port < 0) {
                port = tls ? 443 : 80;
            }
            InetSocketAddress address = new InetSocketAddress(host, port);
            plain.connect(address, Math.min(timeRemaining(page.resource, deadline), 10000));
            plain.setSoTimeout(timeRemaining(page.resource, deadline));
            if (tls) {
                SSLSocketFactory sockets = (SSLSocketFactory) SSLSocketFactory.getDefault();
                try (SSLSocket secure = (SSLSocket) sockets.createSocket(plain, host, port, true)) {
                    SSLParameters checks = secure.getSSLParameters();
                    checks.setEndpointIdentificationAlgorithm("HTTPS");
                    secure.setSSLParameters(checks);
                    secure.startHandshake();
                    return response(secure, page);
                }
            }
            return response(plain, page);
        } catch (ReadFailure | StatusRetry e) {
            throw e;
        } catch (SocketTimeoutException e) {
            throw fail(page.resource, "request deadline exceeded");
        } catch (SSLException e) {
            throw fail(page.resource, "TLS transport failed");
        } catch (HttpException | IOException e) {
            throw fail(page.resource, "transport failed or returned invalid HTTP framing");
        } finally {
            synchronized (this) {
                active = null;
            }
        }
    }

    private synchronized void setActive(Socket socket, AgileCrmResource resource)
            throws ReadFailure {
        if (closed || Thread.currentThread().isInterrupted()) {
            throw fail(resource, "request cancelled");
        }
        active = socket;
    }

    private JsonNode response(Socket socket, AgileCrmResource.Request page)
            throws HttpException, IOException {
        HttpRequest request;
        if (page.form == null) {
            request = new BasicHttpRequest("GET", page.target, HttpVersion.HTTP_1_1);
        } else {
            byte[] form = page.form.getBytes(StandardCharsets.UTF_8);
            BasicHttpEntityEnclosingRequest post =
                    new BasicHttpEntityEnclosingRequest("POST", page.target, HttpVersion.HTTP_1_1);
            post.setEntity(new ByteArrayEntity(form));
            post.setHeader("Content-Type", "application/x-www-form-urlencoded");
            post.setHeader("Content-Length", Integer.toString(form.length));
            request = post;
        }
        request.setHeader("Host", config.origin.getRawAuthority());
        request.setHeader("Authorization", config.authorization);
        request.setHeader("Accept", "application/json");
        request.setHeader("Accept-Encoding", "gzip, deflate");
        request.setHeader("Connection", "close");
        HttpResponse reply =
                new HttpRequestExecutor()
                        .execute(
                                request,
                                RAW_HTTP.createConnection(socket),
                                HttpCoreContext.create());
        int code = reply.getStatusLine().getStatusCode();
        if (code == 204) {
            return JSON.createArrayNode();
        }
        if (code == 429 || code == 500 || code == 502 || code == 503 || code == 504) {
            throw new StatusRetry(code, retryTime(header(reply, "Retry-After", page.resource)));
        }
        if (code != 200) {
            throw fail(page.resource, "unexpected HTTP " + code);
        }
        return parse(reply, page.resource);
    }

    private static JsonNode parse(HttpResponse reply, AgileCrmResource resource)
            throws IOException {
        String len = header(reply, "Content-Length", resource);
        String transfer = header(reply, "Transfer-Encoding", resource);
        if (len != null && (!len.matches("[0-9]{1,18}") || Long.parseLong(len) > MAX_BYTES)) {
            throw fail(resource, "invalid or oversized response length");
        }
        if (transfer != null && (len != null || !"chunked".equalsIgnoreCase(transfer))) {
            throw fail(resource, "invalid response framing");
        }
        if (reply.getEntity() == null) {
            throw fail(resource, "missing response body");
        }
        byte[] bytes = bounded(reply.getEntity().getContent(), resource);
        String encoding = header(reply, "Content-Encoding", resource);
        if (encoding != null && !"identity".equalsIgnoreCase(encoding)) {
            try (InputStream compressed = decompress(bytes, encoding, resource)) {
                bytes = bounded(compressed, resource);
            } catch (ReadFailure e) {
                throw e;
            } catch (IOException e) {
                throw fail(resource, "invalid compressed response");
            }
        }
        try {
            return JSON.readTree(bytes);
        } catch (IOException e) {
            throw fail(resource, "invalid JSON response");
        }
    }

    private static InputStream decompress(byte[] bytes, String encoding, AgileCrmResource resource)
            throws IOException {
        ByteArrayInputStream input = new ByteArrayInputStream(bytes);
        if ("gzip".equalsIgnoreCase(encoding)) {
            return new GZIPInputStream(input);
        }
        if ("deflate".equalsIgnoreCase(encoding)) {
            return new DeflateInputStream(input);
        }
        throw fail(resource, "unsupported response encoding");
    }

    private static byte[] bounded(InputStream source, AgileCrmResource resource)
            throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        int n;
        while ((n = source.read(buffer)) != -1) {
            if (n > MAX_BYTES - output.size()) {
                throw fail(resource, "response exceeds 8 MiB");
            }
            output.write(buffer, 0, n);
        }
        return output.toByteArray();
    }

    private static String header(HttpResponse response, String key, AgileCrmResource scope)
            throws ReadFailure {
        Header[] values = response.getHeaders(key);
        if (values.length > 1) {
            throw fail(scope, "duplicate framing or Retry-After header");
        }
        return values.length == 0 ? null : values[0].getValue().trim();
    }

    private static long retryTime(String value) {
        if (value == null) {
            return 0L;
        }
        try {
            if (value.matches("[0-9]+")) {
                return Math.multiplyExact(1000L, Long.parseLong(value));
            }
            Instant limit =
                    ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant();
            Duration wait = Duration.between(Instant.now(), limit);
            return wait.isNegative() ? 0L : wait.plusNanos(999999).toMillis();
        } catch (DateTimeParseException e) {
            return 0L;
        } catch (NumberFormatException | ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    static int timeRemaining(AgileCrmResource resource, long deadline) throws ReadFailure {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0L) {
            throw fail(resource, "request deadline exceeded");
        }
        return (int) Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remaining));
    }

    @Override
    public synchronized void close() {
        closed = true;
        if (active != null) {
            try {
                active.close();
            } catch (IOException ignored) {
                // The worker also closes its socket on exit.
            }
        }
    }

    static ReadFailure fail(AgileCrmResource resource, String message) {
        return new ReadFailure("AgileCRM Source " + resource.name + ": " + message);
    }

    static final class ReadFailure extends IOException {
        private static final long serialVersionUID = 1L;

        private ReadFailure(String message) {
            super(message);
        }
    }

    private static final class StatusRetry extends IOException {
        private static final long serialVersionUID = 1L;
        private final int status;
        private final long retryAfter;

        private StatusRetry(int status, long retryAfter) {
            this.status = status;
            this.retryAfter = retryAfter;
        }
    }
}
