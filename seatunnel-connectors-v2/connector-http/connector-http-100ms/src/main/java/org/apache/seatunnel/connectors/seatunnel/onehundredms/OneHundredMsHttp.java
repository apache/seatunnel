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

package org.apache.seatunnel.connectors.seatunnel.onehundredms;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.utils.DateUtils;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.MessageConstraints;
import org.apache.http.impl.DefaultBHttpClientConnection;
import org.apache.http.impl.DefaultBHttpClientConnectionFactory;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.http.protocol.HttpRequestExecutor;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPInputStream;

/**
 * Owns each HTTPS socket and management-token cache for one reader. The poller can close the
 * published socket independently; retries, TLS, decoding and waits share one absolute deadline.
 * Each response copies the constrained parser factory, so remote symbols cannot cross responses.
 */
final class OneHundredMsHttp implements AutoCloseable {
    private static final int TOKEN_LIFETIME_SECONDS = 86400;
    private static final Base64.Encoder JWT_ENCODING = Base64.getUrlEncoder().withoutPadding();
    private static final String JWT_HEADER =
            JWT_ENCODING.encodeToString(
                    "{\"alg\":\"HS256\",\"typ\":\"JWT\"}".getBytes(StandardCharsets.US_ASCII));
    private static final DefaultBHttpClientConnectionFactory CONNECTIONS =
            new DefaultBHttpClientConnectionFactory(
                    ConnectionConfig.custom()
                            .setMessageConstraints(
                                    MessageConstraints.custom()
                                            .setMaxLineLength(8192)
                                            .setMaxHeaderCount(64)
                                            .build())
                            .build());
    private static final ObjectMapper JSON =
            new ObjectMapper(
                            JsonFactory.builder()
                                    .disable(JsonFactory.Feature.INTERN_FIELD_NAMES)
                                    .streamReadConstraints(
                                            StreamReadConstraints.builder()
                                                    .maxNestingDepth(100)
                                                    .maxNumberLength(1000)
                                                    .maxStringLength(16777216)
                                                    .build())
                                    .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                                    .build())
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));

    private final OneHundredMsConfig config;
    // The polling thread can abort a stuck socket without entering the request executor.
    private final AtomicReference<Socket> socket = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private String mintedToken;
    private long tokenExpiry;

    OneHundredMsHttp(OneHundredMsConfig config) {
        this.config = config;
    }

    /**
     * GET retries are limited to explicitly retryable statuses and a single app-token refresh.
     * Every attempt shares the caller's absolute deadline and rejects exhausted continuation
     * snapshots.
     */
    JsonNode get(
            OneHundredMsResource resource,
            String target,
            boolean snapshotContinuation,
            long deadline)
            throws IOException {
        boolean renewed = false;
        for (int attempt = 1; attempt <= config.attempts; attempt++) {
            ensureTime(resource, deadline);
            Result result = request(resource, target, deadline);
            if (result.status == 200) {
                return result.body;
            }
            if (result.status == 404 && resource == OneHundredMsResource.ACTIVE_ROOM_PEERS) {
                return null;
            }
            if (snapshotContinuation
                    && (result.status == 400 || result.status == 404 || result.status == 410)) {
                throw resource.invalid(
                        "analytics continuation rejected or expired (HTTP " + result.status + ")");
            }
            if (result.status == 401
                    && config.token == null
                    && !renewed
                    && attempt < config.attempts) {
                renewed = true;
                mintedToken = null;
                continue;
            }
            if (!retryable(result.status) || attempt == config.attempts) {
                throw resource.invalid("HTTP " + result.status + " (response details withheld)");
            }
            long delay = Math.max(result.retryAfterMillis, Math.min(5000L, 200L << (attempt - 1)));
            if (delay >= TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())) {
                throw resource.invalid("retry delay exceeds the request deadline");
            }
            try {
                TimeUnit.MILLISECONDS.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw resource.invalid("request canceled");
            }
        }
        throw resource.invalid("request attempt limit reached");
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            Socket active = socket.getAndSet(null);
            if (active != null) {
                try {
                    active.close();
                } catch (IOException ignored) {
                    // The network task also owns and closes the socket.
                }
            }
        }
    }

    private Result request(OneHundredMsResource resource, String target, long deadline)
            throws IOException {
        Socket raw = new Socket();
        socket.set(raw);
        try (Socket connection = raw) {
            ensureTime(resource, deadline);
            String host = config.origin.getHost();
            int port = config.origin.getPort() < 0 ? 443 : config.origin.getPort();
            connection.connect(
                    new InetSocketAddress(host, port),
                    timeout(resource, deadline, config.connectTimeout));
            connection.setSoTimeout(timeout(resource, deadline, config.readTimeout));
            SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory.getDefault();
            try (SSLSocket tls = (SSLSocket) factory.createSocket(connection, host, port, true)) {
                SSLParameters parameters = tls.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                parameters.setProtocols(
                        Arrays.stream(tls.getEnabledProtocols())
                                .filter(
                                        version ->
                                                "TLSv1.2".equals(version)
                                                        || "TLSv1.3".equals(version))
                                .toArray(String[]::new));
                tls.setSSLParameters(parameters);
                tls.startHandshake();
                tls.setSoTimeout(timeout(resource, deadline, config.readTimeout));
                return exchange(resource, target, deadline, tls);
            }
        } catch (HttpFailure e) {
            throw e;
        } catch (SSLException e) {
            throw resource.invalid("TLS connection or certificate validation failed");
        } catch (HttpException e) {
            throw resource.invalid("invalid HTTP response");
        } catch (IOException e) {
            throw resource.invalid("HTTPS request did not complete");
        } finally {
            socket.compareAndSet(raw, null);
        }
    }

    private Result exchange(OneHundredMsResource resource, String target, long deadline, Socket tls)
            throws IOException, HttpException {
        DefaultBHttpClientConnection connection = CONNECTIONS.createConnection(tls);
        try {
            BasicHttpRequest request = new BasicHttpRequest("GET", target, HttpVersion.HTTP_1_1);
            request.setHeader("Host", config.origin.getRawAuthority());
            request.setHeader("Accept", "application/json");
            request.setHeader("Accept-Encoding", "gzip");
            request.setHeader("Connection", "close");
            request.setHeader("Authorization", "Bearer " + authorization(resource));
            HttpResponse response =
                    new HttpRequestExecutor()
                            .execute(request, connection, HttpCoreContext.create());
            int status = response.getStatusLine().getStatusCode();
            if (status != 200) {
                return new Result(
                        status, null, retryable(status) ? retryAfter(resource, response) : 0L);
            }
            if (response.getHeaders("Content-Length").length > 1
                    || response.getHeaders("Transfer-Encoding").length > 1
                    || (response.containsHeader("Content-Length")
                            && response.containsHeader("Transfer-Encoding"))) {
                throw failure(resource, "ambiguous HTTP body framing");
            }
            JsonNode body = readBody(resource, response.getEntity());
            ensureTime(resource, deadline);
            return new Result(status, body, 0);
        } finally {
            // No response/error body is drained after a failure or used in diagnostic logs.
            try {
                connection.shutdown();
            } catch (IOException ignored) {
                // The try-with-resources transport also closes the TLS socket.
            }
        }
    }

    /**
     * Separate limits count both compressed and decoded bytes. The JSON mapper rejects duplicate
     * keys, trailing messages, deep nesting and long numbers before data reaches the reader.
     */
    private JsonNode readBody(OneHundredMsResource resource, HttpEntity entity) throws HttpFailure {
        if (entity == null || entity.getContentLength() > config.maxResponseBytes) {
            throw failure(resource, "missing or oversized response body");
        }
        Header encoding = entity.getContentEncoding();
        String coding = encoding == null ? "identity" : encoding.getValue().trim();
        if (!"identity".equalsIgnoreCase(coding) && !"gzip".equalsIgnoreCase(coding)) {
            throw failure(resource, "unsupported response content encoding");
        }
        try {
            InputStream wire = new LimitedInput(entity.getContent(), config.maxResponseBytes);
            try (InputStream decoded =
                    "gzip".equalsIgnoreCase(coding) ? new GZIPInputStream(wire) : wire) {
                JsonNode node =
                        // A fresh factory keeps the strict native byte decoder and owns symbols
                        // only for this response. Disabling canonicalization would change its
                        // decoder.
                        JSON.copy()
                                .reader()
                                .without(JsonParser.Feature.AUTO_CLOSE_SOURCE)
                                .readTree(new LimitedInput(decoded, config.maxResponseBytes));
                if (node == null || !node.isObject()) {
                    throw failure(resource, "expected a JSON response object");
                }
                return node;
            }
        } catch (HttpFailure e) {
            throw e;
        } catch (IOException | RuntimeException e) {
            throw failure(resource, "invalid, incomplete or oversized JSON response");
        }
    }

    private String authorization(OneHundredMsResource resource) throws HttpFailure {
        if (config.token != null) {
            return config.token;
        }
        long now = Instant.now().getEpochSecond();
        if (mintedToken != null && now < tokenExpiry - 60) {
            return mintedToken;
        }
        tokenExpiry = now + TOKEN_LIFETIME_SECONDS;
        ObjectNode claims = JSON.createObjectNode();
        claims.put("access_key", config.accessKey);
        claims.put("type", "management");
        claims.put("version", 2);
        claims.put("iat", now);
        claims.put("nbf", now);
        claims.put("exp", tokenExpiry);
        claims.put("jti", UUID.randomUUID().toString());
        String value =
                JWT_HEADER
                        + "."
                        + JWT_ENCODING.encodeToString(
                                claims.toString().getBytes(StandardCharsets.UTF_8));
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(
                    new SecretKeySpec(
                            config.secretKey.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            mintedToken =
                    value
                            + "."
                            + JWT_ENCODING.encodeToString(
                                    mac.doFinal(value.getBytes(StandardCharsets.US_ASCII)));
            return mintedToken;
        } catch (GeneralSecurityException e) {
            throw failure(resource, "unable to sign management token");
        }
    }

    private static boolean retryable(int status) {
        return status == 429 || status == 500 || status == 502 || status == 503 || status == 504;
    }

    private static long retryAfter(OneHundredMsResource resource, HttpResponse response)
            throws HttpFailure {
        Header[] hints = response.getHeaders("Retry-After");
        if (hints.length > 1) {
            throw failure(resource, "ambiguous Retry-After");
        }
        if (hints.length == 0) {
            return 0;
        }
        String value = hints[0].getValue().trim();
        if (!value.isEmpty() && value.chars().allMatch(ch -> ch >= '0' && ch <= '9')) {
            int seconds = 0;
            for (int i = 0; i < value.length(); i++) {
                seconds = seconds * 10 + value.charAt(i) - '0';
                if (seconds > 300) {
                    return Long.MAX_VALUE;
                }
            }
            return seconds * 1000L;
        }
        Date date = DateUtils.parseDate(value);
        return date == null ? 0 : Math.max(0, date.getTime() - System.currentTimeMillis());
    }

    private void ensureTime(OneHundredMsResource resource, long deadline) throws HttpFailure {
        if (closed.get() || Thread.currentThread().isInterrupted()) {
            throw failure(resource, "request canceled");
        }
        if (deadline - System.nanoTime() <= 0) {
            throw failure(resource, "request deadline exceeded");
        }
    }

    private int timeout(OneHundredMsResource resource, long deadline, int cap) throws HttpFailure {
        ensureTime(resource, deadline);
        return (int)
                Math.min(
                        cap,
                        Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())));
    }

    private static HttpFailure failure(OneHundredMsResource resource, String reason) {
        return new HttpFailure("100ms source " + resource.tableName() + ": " + reason);
    }

    /**
     * Internal transport validation error whose message already has safe resource context. Raw
     * upstream bodies, credentials and nested transport causes are excluded from this diagnostic.
     */
    private static final class HttpFailure extends IOException {
        private static final long serialVersionUID = 1L;

        HttpFailure(String message) {
            super(message);
        }
    }

    /**
     * Exposes successful JSON and retry metadata without retaining any provider error body. A
     * retryable status carries only its bounded delay while decoded successful data remains
     * request-local.
     */
    private static final class Result {
        private final int status;
        private final JsonNode body;
        private final long retryAfterMillis;

        Result(int status, JsonNode body, long retryAfterMillis) {
            this.status = status;
            this.body = body;
            this.retryAfterMillis = retryAfterMillis;
        }
    }

    /**
     * Takes one sentinel byte past the limit to distinguish exact-size responses from truncation.
     */
    private static final class LimitedInput extends FilterInputStream {
        private int remaining;

        LimitedInput(InputStream source, int limit) {
            super(source);
            remaining = limit;
        }

        @Override
        public int read() throws IOException {
            int value = in.read();
            if (value >= 0) {
                consume(1);
            }
            return value;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            int read = in.read(bytes, offset, Math.min(length, remaining + 1));
            if (read > 0) {
                consume(read);
            }
            return read;
        }

        @Override
        public void close() {
            // Release the gzip inflater without asking the HTTP entity stream to drain.
        }

        private void consume(int count) throws IOException {
            remaining -= count;
            if (remaining < 0) {
                throw new IOException("response exceeds the configured limit");
            }
        }
    }
}
