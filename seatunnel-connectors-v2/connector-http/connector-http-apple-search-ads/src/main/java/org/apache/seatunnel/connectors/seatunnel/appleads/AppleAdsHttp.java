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
package org.apache.seatunnel.connectors.seatunnel.appleads;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.cfg.JsonNodeFeature;

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
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;

/**
 * Account-scoped, quiet HTTPS transport for Apple Ads queries and OAuth exchange.
 *
 * <p>Separate request phases prevent context or bearer credentials from reaching the token
 * authority. A direct TLS watchdog enforces the wall-clock attempt budget and cancellation.
 */
final class AppleAdsHttp implements AutoCloseable {
    private static final MessageConstraints HEADERS =
            MessageConstraints.custom().setMaxLineLength(16384).setMaxHeaderCount(64).build();
    private static final Set<Integer> TRANSIENT = Set.of(408, 429, 500, 502, 503, 504);

    private final AppleAdsConfig config;
    final ObjectMapper json;
    private final ScheduledThreadPoolExecutor watchdog;

    // Only the reader worker issues requests. Cancellation may close the active TLS socket.
    private volatile boolean closed;
    private Socket active;
    private long previousAttempt;
    private boolean attempted;

    // Use a monotonic expiration and renew early to leave room for server-side validation.
    private String accessToken;
    private long renewAt;

    AppleAdsHttp(AppleAdsConfig config) {
        this.config = config;
        JsonFactory factory =
                new JsonFactory()
                        .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                        .setStreamReadConstraints(
                                StreamReadConstraints.builder()
                                        .maxNestingDepth(90)
                                        .maxNumberLength(500)
                                        .maxStringLength(config.bodyLimit)
                                        .build());
        json =
                new ObjectMapper(factory)
                        .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
                        .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                        .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                        .configure(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES, false);
        watchdog =
                new ScheduledThreadPoolExecutor(
                        1,
                        task -> {
                            Thread thread = new Thread(task, "apple-ads-https-deadline");
                            thread.setDaemon(true);
                            return thread;
                        });
        watchdog.setRemoveOnCancelPolicy(true);
    }

    long deadline() {
        return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.operationTimeout);
    }

    /**
     * Executes a proven read-only API action, performing at most one reactive credential renewal.
     *
     * <p>The same deadline covers the exchange, all API attempts, and the optional new token.
     */
    JsonNode query(AppleAdsResource resource, String path, JsonNode body, long deadline)
            throws InterruptedException {
        byte[] payload;
        try {
            payload = body == null ? null : json.writeValueAsBytes(body);
        } catch (JsonProcessingException ex) {
            throw resource.fail("could not serialize the read request");
        }
        Call call = new Call(config.origin.resolve(path), payload, "application/json", true);
        for (int i = 0; i < 2; i++) {
            authorize(resource, deadline);
            Response answer = execute(resource, call, "Bearer " + accessToken, deadline);
            if (answer.status != 401) {
                return answer.body;
            }
            accessToken = null;
        }
        throw resource.fail("HTTP 401 after renewing the Apple access token");
    }

    private void authorize(AppleAdsResource resource, long deadline) throws InterruptedException {
        if (accessToken != null && renewAt - System.nanoTime() > 0) {
            return;
        }
        String form =
                "grant_type=client_credentials&scope=searchadsorg&client_id="
                        + encode(config.clientId)
                        + "&client_secret="
                        + encode(config.clientSecret);
        Call token =
                new Call(
                        config.oauth.resolve("/auth/oauth2/token"),
                        form.getBytes(StandardCharsets.US_ASCII),
                        "application/x-www-form-urlencoded",
                        false);
        JsonNode response = execute(resource, token, null, deadline).body;
        JsonNode lifetime = response.path("expires_in");
        String value = response.path("access_token").asText("");
        if (!response.path("access_token").isTextual()
                || value.isEmpty()
                || value.length() > 8192
                || !value.chars().allMatch(c -> c >= 33 && c <= 126)
                || !"Bearer".equalsIgnoreCase(response.path("token_type").asText(""))
                || !lifetime.isIntegralNumber()
                || !lifetime.canConvertToInt()
                || lifetime.intValue() < 1
                || lifetime.intValue() > 86400) {
            throw resource.fail("invalid Apple OAuth token response");
        }
        long nanos = TimeUnit.SECONDS.toNanos(lifetime.intValue());
        renewAt = System.nanoTime() + nanos - Math.min(nanos / 10, TimeUnit.SECONDS.toNanos(30));
        accessToken = value;
    }

    private static String encode(String text) {
        return URLEncoder.encode(text, StandardCharsets.UTF_8);
    }

    /**
     * Retries only explicit transient outcomes and repeatable remote reads.
     *
     * <p>A failed token POST can have an unknown outcome, so network failures on that phase
     * propagate without an automatic second exchange.
     */
    private Response execute(AppleAdsResource resource, Call call, String bearer, long deadline)
            throws InterruptedException {
        for (int attempt = 0; ; attempt++) {
            if (attempted) {
                delay(
                        resource,
                        TimeUnit.MILLISECONDS.toNanos(config.interval)
                                - (System.nanoTime() - previousAttempt),
                        deadline);
            }
            long retryHint;
            String reason;
            try {
                return exchange(resource, call, bearer, deadline);
            } catch (Retry ex) {
                retryHint = ex.delay;
                reason = "HTTP " + ex.status;
            } catch (SSLException ex) {
                remaining(resource, deadline);
                throw resource.fail("HTTPS certificate verification or negotiation failed");
            } catch (ZipException ex) {
                throw resource.fail("invalid gzip response");
            } catch (JsonProcessingException ex) {
                throw resource.fail("invalid JSON response");
            } catch (HttpException ex) {
                throw resource.fail("malformed HTTP response");
            } catch (IOException ex) {
                remaining(resource, deadline);
                if (!call.api) {
                    throw resource.fail("Apple OAuth exchange outcome is unknown after HTTPS I/O");
                }
                retryHint = 0;
                reason = "HTTPS I/O failure or attempt timeout";
            }
            if (attempt >= config.retries) {
                throw resource.fail(
                        call.phase() + " " + reason + " after " + (attempt + 1) + " attempts");
            }
            long backoff = Math.max(retryHint, Math.min(5000, 250L << attempt));
            delay(resource, TimeUnit.MILLISECONDS.toNanos(backoff), deadline);
        }
    }

    private Response exchange(AppleAdsResource resource, Call call, String bearer, long deadline)
            throws IOException, HttpException, InterruptedException, Retry {
        long attemptTime =
                Math.min(
                        remaining(resource, deadline),
                        TimeUnit.MILLISECONDS.toNanos(config.requestTimeout));
        long expires = System.nanoTime() + attemptTime;
        Socket raw = new Socket(Proxy.NO_PROXY);
        ScheduledFuture<?> abort = null;
        try {
            synchronized (this) {
                remaining(resource, expires);
                active = raw;
                abort =
                        watchdog.schedule(
                                () -> closeSocket(raw), attemptTime, TimeUnit.NANOSECONDS);
            }
            attempted = true;
            previousAttempt = System.nanoTime();
            int timeout = (int) Math.max(1, TimeUnit.NANOSECONDS.toMillis(attemptTime));
            String host = call.uri.getHost();
            int port = call.uri.getPort() == -1 ? 443 : call.uri.getPort();
            raw.connect(new InetSocketAddress(host, port), timeout);
            raw.setSoTimeout(timeout);
            SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory.getDefault();
            try (SSLSocket socket = (SSLSocket) factory.createSocket(raw, host, port, true);
                    DefaultBHttpClientConnection wire =
                            new DefaultBHttpClientConnection(8192, null, null, HEADERS)) {
                SSLParameters parameters = socket.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                socket.setSSLParameters(parameters);
                socket.startHandshake();
                wire.bind(socket);
                HttpResponse response =
                        new HttpRequestExecutor()
                                .execute(message(call, bearer), wire, HttpCoreContext.create());
                int status = response.getStatusLine().getStatusCode();
                if (call.api && TRANSIENT.contains(status)
                        || !call.api && (status == 429 || status == 503)) {
                    throw new Retry(status, retryAfter(response));
                }
                if (call.api && status == 401) {
                    return new Response(status, null);
                }
                if (status != 200) {
                    throw resource.fail(call.phase() + " returned HTTP " + status);
                }
                return new Response(
                        status,
                        body(
                                resource,
                                response,
                                expires,
                                call.api ? config.bodyLimit : Math.min(config.bodyLimit, 65536)));
            }
        } finally {
            closeSocket(raw);
            if (abort != null) {
                abort.cancel(false);
            }
            synchronized (this) {
                active = null;
            }
        }
    }

    private HttpRequest message(Call call, String bearer) {
        String target = call.uri.getRawPath();
        if (call.uri.getRawQuery() != null) {
            target += "?" + call.uri.getRawQuery();
        }
        HttpRequest request;
        if (call.bytes == null) {
            request = new BasicHttpRequest("GET", target);
        } else {
            BasicHttpEntityEnclosingRequest post =
                    new BasicHttpEntityEnclosingRequest("POST", target);
            post.setEntity(new ByteArrayEntity(call.bytes));
            post.addHeader("Content-Type", call.media);
            post.addHeader("Content-Length", Integer.toString(call.bytes.length));
            request = post;
        }
        request.addHeader("Host", call.uri.getRawAuthority());
        request.addHeader("Accept", "application/json");
        request.addHeader("Accept-Encoding", "gzip");
        request.addHeader("Connection", "close");
        if (call.api) {
            request.addHeader("Authorization", bearer);
            request.addHeader("X-AP-Context", config.mode.contextKey + "=" + config.accountId);
        }
        return request;
    }

    private JsonNode body(
            AppleAdsResource resource, HttpResponse response, long deadline, int limit)
            throws IOException, InterruptedException {
        HttpEntity entity = response.getEntity();
        if (entity == null) {
            throw resource.fail("missing HTTPS response body");
        }
        Header[] encodings = response.getHeaders("Content-Encoding");
        boolean gzip =
                encodings.length == 1 && "gzip".equalsIgnoreCase(encodings[0].getValue().trim());
        if (encodings.length > 0
                && !(gzip
                        || encodings.length == 1
                                && "identity".equalsIgnoreCase(encodings[0].getValue().trim()))) {
            throw resource.fail("unsupported HTTPS content encoding");
        }
        InputStream wire = new WireLimit(entity.getContent(), limit, resource);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] block = new byte[8192];
        try (InputStream input = gzip ? new GZIPInputStream(wire) : wire) {
            for (int n; (n = input.read(block)) != -1; ) {
                remaining(resource, deadline);
                if (n > limit - output.size()) {
                    throw resource.fail("decoded response exceeds max_response_bytes");
                }
                output.write(block, 0, n);
            }
        }
        byte[] bytes = output.toByteArray();
        try (JsonParser parser = json.getFactory().createParser(bytes)) {
            int tokens = 0;
            while (parser.nextToken() != null) {
                if (++tokens > 500000) {
                    throw resource.fail("JSON token limit exceeded");
                }
                if ((tokens & 1023) == 0) {
                    remaining(resource, deadline);
                }
            }
        }
        JsonNode root = json.readTree(bytes);
        remaining(resource, deadline);
        if (root == null || !root.isObject()) {
            throw resource.fail("expected a JSON object");
        }
        return root;
    }

    private static long retryAfter(HttpResponse response) {
        long wait = 0;
        for (Header header : response.getHeaders("Retry-After")) {
            String value = header.getValue().trim();
            try {
                long hinted =
                        value.matches("[0-9]+")
                                ? Math.multiplyExact(Long.parseLong(value), 1000L)
                                : ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME)
                                                .toInstant()
                                                .toEpochMilli()
                                        - System.currentTimeMillis();
                wait = Math.max(wait, hinted);
            } catch (ArithmeticException | NumberFormatException ex) {
                return Long.MAX_VALUE;
            } catch (DateTimeException ex) {
                // Malformed headers cannot extend the local backoff.
            }
        }
        return wait;
    }

    private void delay(AppleAdsResource resource, long nanos, long deadline)
            throws InterruptedException {
        if (nanos >= remaining(resource, deadline)) {
            throw resource.fail("retry or pacing delay exceeds the operation deadline");
        }
        while (nanos > 0) {
            long started = System.nanoTime();
            TimeUnit.NANOSECONDS.sleep(Math.min(nanos, TimeUnit.MILLISECONDS.toNanos(100)));
            remaining(resource, deadline);
            nanos -= System.nanoTime() - started;
        }
    }

    long remaining(AppleAdsResource resource, long deadline) throws InterruptedException {
        if (closed || Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("AppleAds source closed");
        }
        long nanos = deadline - System.nanoTime();
        if (nanos <= 0) {
            throw resource.fail("HTTPS operation deadline exceeded");
        }
        return nanos;
    }

    @Override
    public synchronized void close() {
        closed = true;
        closeSocket(active);
        watchdog.shutdownNow();
    }

    private static void closeSocket(Socket raw) {
        if (raw != null) {
            try {
                raw.close();
            } catch (IOException ignored) {
                // The closed socket will never be reused.
            }
        }
    }

    /**
     * Fixed endpoint information for one provider phase.
     *
     * <p>Only API calls may carry a bearer and account context or retry on an unknown outcome.
     */
    private static final class Call {
        final URI uri;
        final byte[] bytes;
        final String media;
        final boolean api;

        Call(URI uri, byte[] bytes, String media, boolean api) {
            this.uri = uri;
            this.bytes = bytes;
            this.media = media;
            this.api = api;
        }

        String phase() {
            return api ? "Apple Ads API" : "Apple OAuth exchange";
        }
    }

    /**
     * Returns parsed JSON or a dedicated API unauthorized signal.
     *
     * <p>Provider error messages are never retained by the transport.
     */
    private static final class Response {
        final int status;
        final JsonNode body;

        Response(int status, JsonNode body) {
            this.status = status;
            this.body = body;
        }
    }

    /**
     * Carries only the status and bounded retry information.
     *
     * <p>No provider headers or response strings reach caller diagnostics.
     */
    private static final class Retry extends Exception {
        private static final long serialVersionUID = 1L;
        final int status;
        final long delay;

        Retry(int status, long delay) {
            this.status = status;
            this.delay = delay;
        }
    }

    /**
     * Bounds the compressed wire bytes independently of decoded JSON.
     *
     * <p>The enclosing HTTPS call owns the socket and closes it without consuming extra entity
     * bytes after a rejected response.
     */
    private static final class WireLimit extends FilterInputStream {
        private final int limit;
        private final AppleAdsResource resource;
        private int count;

        WireLimit(InputStream body, int limit, AppleAdsResource resource) {
            super(body);
            this.limit = limit;
            this.resource = resource;
        }

        @Override
        public int read() throws IOException {
            int value = in.read();
            if (value != -1) {
                check(1);
            }
            return value;
        }

        @Override
        public int read(byte[] data, int offset, int length) throws IOException {
            int read = in.read(data, offset, length);
            if (read > 0) {
                check(read);
            }
            return read;
        }

        private void check(int read) {
            count += read;
            if (count > limit) {
                throw resource.fail("wire response exceeds max_response_bytes");
            }
        }

        @Override
        public void close() {
            // The HTTPS call closes instead of draining the entity.
        }
    }
}
