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
package org.apache.seatunnel.connectors.seatunnel.akeneo;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.cfg.JsonNodeFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.config.MessageConstraints;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.DefaultBHttpClientConnection;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.http.protocol.HttpRequestExecutor;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import java.io.ByteArrayOutputStream;
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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;

/** One bounded direct HTTPS connection per attempt without wire or header loggers. */
final class AkeneoHttpTransport implements AutoCloseable {
    private static final MessageConstraints HEADERS =
            MessageConstraints.custom().setMaxLineLength(16384).setMaxHeaderCount(64).build();
    private static final int MAX_JSON_TOKENS = 250000;
    private static final int MAX_TOKEN_RESPONSE = 65536;
    private static final String TOKEN_PATH = "/api/oauth/v1/token";
    private final AkeneoConfig config;
    private final ObjectMapper json;
    private final ScheduledThreadPoolExecutor timeouts;
    private volatile boolean closed;
    private Socket connected;
    private long previousAttempt;
    private boolean attempted;
    private Token token;

    AkeneoHttpTransport(AkeneoConfig config) {
        this.config = config;
        JsonFactory factory =
                new JsonFactory()
                        .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                        .setStreamReadConstraints(
                                StreamReadConstraints.builder()
                                        .maxNestingDepth(100)
                                        .maxStringLength(config.maxBytes)
                                        .maxNumberLength(1000)
                                        .build());
        json =
                new ObjectMapper(factory)
                        .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
                        .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                        .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                        .configure(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES, false);
        timeouts = new ScheduledThreadPoolExecutor(1, task -> daemon(task, "akeneo-http-deadline"));
        timeouts.setRemoveOnCancelPolicy(true);
    }

    AkeneoPage page(
            AkeneoResource resource, String family, AkeneoSplit.Position position, long deadline)
            throws InterruptedException {
        if (token == null || token.renewAt - System.nanoTime() <= 0) {
            authorize(resource, deadline);
        }
        String stage = resource.optionName();
        String target = resource.target(config, family, position);
        JsonNode payload = request(stage, target, token.header, null, deadline);
        if (payload == null) {
            authorize(resource, deadline);
            payload = request(stage, target, token.header, null, deadline);
            if (payload == null) {
                throw resource.failure("HTTP 401 after one token renewal");
            }
        }
        return AkeneoPage.read(config, resource, family, position, payload, this, deadline);
    }

    private void authorize(AkeneoResource resource, long deadline) throws InterruptedException {
        String stage = resource.optionName() + " authorization";
        ObjectNode body = json.createObjectNode();
        if (token == null) {
            body.put("grant_type", "password");
            body.put("username", config.username);
            body.put("password", config.password);
        } else {
            body.put("grant_type", "refresh_token");
            body.put("refresh_token", token.refresh);
        }
        byte[] bytes;
        try {
            bytes = json.writeValueAsBytes(body);
        } catch (JsonProcessingException ex) {
            throw fail(stage, "could not construct the token request");
        }
        JsonNode payload = request(stage, TOKEN_PATH, config.clientAuthorization, bytes, deadline);
        String access = payload.path("access_token").asText("");
        String refresh = payload.path("refresh_token").asText("");
        JsonNode expires = payload.path("expires_in");
        if (!payload.path("access_token").isTextual()
                || access.length() > 4096
                || !access.matches("[A-Za-z0-9._~+/-]+=*")
                || !payload.path("refresh_token").isTextual()
                || refresh.isBlank()
                || refresh.length() > 4096
                || refresh.codePoints().anyMatch(Character::isISOControl)
                || !"Bearer".equalsIgnoreCase(payload.path("token_type").asText(""))
                || !expires.isIntegralNumber()
                || !expires.canConvertToLong()
                || expires.longValue() < 1
                || expires.longValue() > 31536000) {
            throw fail(stage, "invalid token response");
        }
        long lifetime = TimeUnit.SECONDS.toNanos(expires.longValue());
        long safety = Math.min(TimeUnit.SECONDS.toNanos(30), lifetime / 10);
        token = new Token("Bearer " + access, refresh, System.nanoTime() + lifetime - safety);
    }

    private JsonNode request(
            String stage, String target, String authorization, byte[] body, long deadline)
            throws InterruptedException {
        for (int attempt = 1; ; attempt++) {
            remaining(stage, deadline);
            if (attempted) {
                pause(
                        stage,
                        TimeUnit.MILLISECONDS.toNanos(config.interval)
                                - (System.nanoTime() - previousAttempt),
                        deadline);
            }
            Backoff retry;
            try {
                return execute(stage, target, authorization, body, deadline);
            } catch (Backoff ex) {
                retry = ex;
            } catch (SSLException ex) {
                remaining(stage, deadline);
                throw fail(stage, "TLS peer verification or negotiation failed");
            } catch (JsonProcessingException ex) {
                throw fail(stage, "invalid JSON response");
            } catch (ZipException ex) {
                throw fail(stage, "invalid gzip response");
            } catch (HttpException ex) {
                throw fail(stage, "malformed HTTP response");
            } catch (IOException ex) {
                remaining(stage, deadline);
                if (body != null) {
                    // A lost response could hide a successful refresh and invalidate the old
                    // refresh token.
                    throw fail(stage, "token request I/O failure");
                }
                retry = new Backoff("I/O failure", 0);
            }
            long left = remaining(stage, deadline);
            if (attempt >= config.attempts) {
                throw fail(stage, retry.reason + " after " + attempt + " attempts");
            }
            long millis = Math.min(5000, 250L << (attempt - 1));
            long delay =
                    TimeUnit.MILLISECONDS.toNanos(
                            Math.max(
                                    retry.minimum,
                                    millis + ThreadLocalRandom.current().nextLong(millis / 2 + 1)));
            if (delay >= left) {
                throw fail(stage, "retry delay exceeds the remaining deadline");
            }
            pause(stage, delay, deadline);
        }
    }

    private JsonNode execute(
            String stage, String target, String authorization, byte[] body, long deadline)
            throws IOException, HttpException, InterruptedException, Backoff {
        Socket raw = new Socket(Proxy.NO_PROXY);
        ScheduledFuture<?> abort = null;
        try {
            synchronized (this) {
                long left = remaining(stage, deadline);
                connected = raw;
                abort = timeouts.schedule(() -> quietClose(raw), left, TimeUnit.NANOSECONDS);
            }
            previousAttempt = System.nanoTime();
            attempted = true;
            int timeout =
                    (int)
                            Math.min(
                                    config.socketTimeout,
                                    Math.max(
                                            1,
                                            TimeUnit.NANOSECONDS.toMillis(
                                                    remaining(stage, deadline))));
            raw.connect(new InetSocketAddress(config.host, config.port), timeout);
            raw.setSoTimeout(timeout);
            SSLSocketFactory tls = (SSLSocketFactory) SSLSocketFactory.getDefault();
            try (SSLSocket socket =
                            (SSLSocket) tls.createSocket(raw, config.host, config.port, true);
                    DefaultBHttpClientConnection wire =
                            new DefaultBHttpClientConnection(8192, null, null, HEADERS)) {
                SSLParameters parameters = socket.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                socket.setSSLParameters(parameters);
                socket.startHandshake();

                HttpRequest message;
                if (body == null) {
                    message = new BasicHttpRequest("GET", target);
                } else {
                    BasicHttpEntityEnclosingRequest post =
                            new BasicHttpEntityEnclosingRequest("POST", target);
                    post.setEntity(new ByteArrayEntity(body));
                    post.addHeader("Content-Type", "application/json");
                    post.addHeader("Content-Length", Integer.toString(body.length));
                    message = post;
                }
                message.addHeader("Host", config.authority);
                message.addHeader("Authorization", authorization);
                message.addHeader("Accept", "application/json");
                message.addHeader("Accept-Encoding", "gzip");
                message.addHeader("Connection", "close");
                wire.bind(socket);
                HttpResponse response =
                        new HttpRequestExecutor().execute(message, wire, HttpCoreContext.create());
                int status = response.getStatusLine().getStatusCode();
                if (status == 200) {
                    int limit =
                            body == null
                                    ? config.maxBytes
                                    : Math.min(MAX_TOKEN_RESPONSE, config.maxBytes);
                    return decode(stage, entity(stage, response, raw, limit, deadline), deadline);
                }
                if (status == 401 && body == null) {
                    return null;
                }
                if (status == 429
                        || body == null
                                && (status == 408
                                        || status == 500
                                        || status == 502
                                        || status == 503
                                        || status == 504)) {
                    throw new Backoff("HTTP " + status, retryDelay(response));
                }
                // Provider response text, headers, credentials and opaque cursors are private.
                throw fail(stage, "HTTP " + status);
            }
        } finally {
            quietClose(raw);
            if (abort != null) {
                abort.cancel(false);
            }
            synchronized (this) {
                connected = null;
            }
        }
    }

    private byte[] entity(
            String stage, HttpResponse response, Socket raw, int maximum, long deadline)
            throws IOException, InterruptedException {
        HttpEntity entity = response.getEntity();
        if (entity == null) {
            throw fail(stage, "missing response body");
        }
        Header header = response.getFirstHeader("Content-Encoding");
        String encoding = header == null ? "identity" : header.getValue();
        if (!"identity".equalsIgnoreCase(encoding) && !"gzip".equalsIgnoreCase(encoding)) {
            throw fail(stage, "unsupported content encoding");
        }
        InputStream input = entity.getContent();
        try {
            if ("gzip".equalsIgnoreCase(encoding)) {
                input = new GZIPInputStream(input);
            }
            byte[] buffer = new byte[8192];
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            for (int count; (count = input.read(buffer)) != -1; ) {
                remaining(stage, deadline);
                if (count > maximum - bytes.size()) {
                    throw fail(stage, "response exceeds max_response_bytes");
                }
                bytes.write(buffer, 0, count);
            }
            return bytes.toByteArray();
        } catch (IOException | InterruptedException | RuntimeException ex) {
            // HTTP entity close may drain. Close the socket first on failure.
            quietClose(raw);
            throw ex;
        } finally {
            try {
                input.close();
            } catch (IOException ignored) {
                // The connection is being closed.
            }
        }
    }

    private JsonNode decode(String stage, byte[] bytes, long deadline)
            throws IOException, InterruptedException {
        try (JsonParser parser = json.getFactory().createParser(bytes)) {
            int count = 0;
            while (parser.nextToken() != null) {
                if (++count > MAX_JSON_TOKENS) {
                    throw fail(stage, "JSON token limit exceeded");
                }
                if ((count & 1023) == 0) {
                    remaining(stage, deadline);
                }
            }
        }
        JsonNode root = json.readTree(bytes);
        remaining(stage, deadline);
        if (root == null || (!root.isObject() && !root.isArray()) || root.hasNonNull("error")) {
            throw fail(stage, "invalid collection response");
        }
        JsonNode errors = root.get("errors");
        if (errors != null
                && !errors.isNull()
                && !(errors.isContainerNode() && errors.isEmpty())
                && !(errors.isTextual() && errors.textValue().isEmpty())) {
            throw fail(stage, "provider reported an error response");
        }
        return root;
    }

    private static long retryDelay(HttpResponse response) {
        long floor = 0;
        for (Header header : response.getHeaders("Retry-After")) {
            String value = header.getValue().trim();
            try {
                floor =
                        Math.max(
                                floor,
                                value.matches("[0-9]+")
                                        ? Math.multiplyExact(Long.parseLong(value), 1000)
                                        : Duration.between(
                                                        Instant.now(),
                                                        ZonedDateTime.parse(
                                                                        value,
                                                                        DateTimeFormatter
                                                                                .RFC_1123_DATE_TIME)
                                                                .toInstant())
                                                .toMillis());
            } catch (ArithmeticException | NumberFormatException overflow) {
                return Long.MAX_VALUE;
            } catch (DateTimeException malformed) {
                // Apply the local backoff when the hint is malformed.
            }
        }
        return floor;
    }

    private void pause(String stage, long nanos, long deadline) throws InterruptedException {
        if (nanos >= remaining(stage, deadline)) {
            throw fail(stage, "request interval exceeds the remaining deadline");
        }
        if (nanos > 0) {
            TimeUnit.NANOSECONDS.sleep(nanos);
        }
    }

    long remaining(String stage, long deadline) throws InterruptedException {
        if (closed || Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Akeneo request closed");
        }
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
            throw fail(stage, "page deadline exceeded");
        }
        return remaining;
    }

    static Thread daemon(Runnable task, String name) {
        Thread thread = new Thread(task, name);
        thread.setDaemon(true);
        return thread;
    }

    @Override
    public synchronized void close() {
        closed = true;
        quietClose(connected);
        timeouts.shutdownNow();
    }

    private static void quietClose(Socket socket) {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // Nothing remains useful on this connection.
            }
        }
    }

    private static AkeneoResource.Failure fail(String stage, String reason) {
        return new AkeneoResource.Failure("Akeneo " + stage + ": " + reason);
    }

    private static final class Token {
        private final String header;
        private final String refresh;
        private final long renewAt;

        private Token(String header, String refresh, long renewAt) {
            this.header = header;
            this.refresh = refresh;
            this.renewAt = renewAt;
        }
    }

    private static final class Backoff extends Exception {
        private static final long serialVersionUID = 1L;
        private final String reason;
        private final long minimum;

        private Backoff(String reason, long minimum) {
            this.reason = reason;
            this.minimum = minimum;
        }
    }
}
