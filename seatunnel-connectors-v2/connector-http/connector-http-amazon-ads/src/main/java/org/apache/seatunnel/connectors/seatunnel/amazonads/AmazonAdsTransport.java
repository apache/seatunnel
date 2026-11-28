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
package org.apache.seatunnel.connectors.seatunnel.amazonads;

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
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;

/**
 * Bounded direct HTTPS for separate OAuth, Ads and signed download origins. Requests are made
 * without wire loggers or redirects and only carry their own phase's headers.
 */
final class AmazonAdsTransport implements AutoCloseable {
    private static final MessageConstraints HEADERS =
            MessageConstraints.custom().setMaxLineLength(16384).setMaxHeaderCount(64).build();
    private static final int MAX_JSON_TOKENS = 250000;
    private final AmazonAdsConfig config;
    private final ObjectMapper json;
    private final ScheduledThreadPoolExecutor timeouts;
    private volatile boolean closed;
    private Socket connected;
    private long previousAttempt;
    private boolean attempted;
    private String accessToken;
    private long refreshAt;
    private AmazonAdsReportSpool spool;

    AmazonAdsTransport(AmazonAdsConfig config) {
        this.config = config;
        JsonFactory factory =
                new JsonFactory()
                        .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                        .setStreamReadConstraints(
                                StreamReadConstraints.builder()
                                        .maxNestingDepth(100)
                                        .maxStringLength(config.maxResponse)
                                        .maxNumberLength(1000)
                                        .build());
        json =
                new ObjectMapper(factory)
                        .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
                        .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                        .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                        .configure(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES, false);
        timeouts =
                new ScheduledThreadPoolExecutor(1, task -> daemon(task, "amazonads-http-deadline"));
        timeouts.setRemoveOnCancelPolicy(true);
    }

    ObjectMapper mapper() {
        return json;
    }

    JsonNode read(
            AmazonAdsResource resource,
            String profile,
            String path,
            String media,
            ObjectNode body,
            long deadline)
            throws InterruptedException {
        return ads(resource, profile, path, media, media, body, false, Set.of(200), deadline)
                .payload;
    }

    Reply ads(
            AmazonAdsResource resource,
            String profile,
            String path,
            String mediaType,
            String acceptedType,
            ObjectNode body,
            boolean create,
            Set<Integer> permitted,
            long deadline)
            throws InterruptedException {
        byte[] bytes = body == null ? null : jsonBytes(resource, body);
        for (int renewal = 0; renewal < 2; renewal++) {
            ensureAccess(resource, deadline);
            Request req =
                    new Request(
                            config.api,
                            path,
                            mediaType,
                            acceptedType,
                            bytes,
                            "Bearer " + accessToken,
                            config.clientId,
                            profile,
                            !create);
            Reply reply = request(resource, req, permitted, deadline);
            if (reply.status != 401) {
                return reply;
            }
            accessToken = null;
            if (renewal == 1) {
                throw resource.fail("HTTP 401 after token renewal");
            }
        }
        throw resource.fail("token renewal could not be completed");
    }

    private void ensureAccess(AmazonAdsResource resource, long deadline)
            throws InterruptedException {
        if (accessToken != null && refreshAt - System.nanoTime() > 0) {
            return;
        }
        String form =
                "grant_type=refresh_token&refresh_token="
                        + AmazonAdsDirect.encode(config.refreshToken);
        Request token =
                new Request(
                        config.oauth,
                        "/auth/o2/token",
                        "application/x-www-form-urlencoded;charset=UTF-8",
                        "application/json",
                        form.getBytes(StandardCharsets.US_ASCII),
                        config.clientAuthorization,
                        null,
                        null,
                        false);
        Reply reply = request(resource, token, Set.of(200, 400), deadline);
        if (reply.status != 200) {
            throw resource.fail("Login with Amazon token request returned HTTP " + reply.status);
        }
        JsonNode payload = reply.payload;
        String value = payload.path("access_token").asText("");
        JsonNode ttl = payload.path("expires_in");
        if (!payload.path("access_token").isTextual()
                || value.isEmpty()
                || value.length() > 4096
                || !value.chars().allMatch(c -> c >= 33 && c <= 126)
                || !"bearer".equalsIgnoreCase(payload.path("token_type").asText(""))
                || !ttl.isIntegralNumber()
                || !ttl.canConvertToInt()
                || ttl.intValue() <= 0
                || ttl.intValue() > 86400) {
            throw resource.fail("Login with Amazon token response is invalid");
        }
        long duration = TimeUnit.SECONDS.toNanos(ttl.intValue());
        accessToken = value;
        refreshAt =
                System.nanoTime()
                        + duration
                        - Math.min(duration / 10, TimeUnit.SECONDS.toNanos(30));
    }

    private byte[] jsonBytes(AmazonAdsResource resource, JsonNode value) {
        try {
            return json.writeValueAsBytes(value);
        } catch (JsonProcessingException ex) {
            throw resource.fail("could not encode the request body");
        }
    }

    /**
     * Reads a provider-controlled signed URL without any Ads or OAuth headers. Returning null
     * signals an expired or rejected URL that must be obtained again from the Ads API.
     */
    AmazonAdsReportSpool download(
            AmazonAdsResource resource,
            String location,
            AmazonAdsPartition.Window window,
            String profile,
            long deadline)
            throws InterruptedException {
        final URI uri;
        try {
            uri = new URI(location);
        } catch (URISyntaxException ex) {
            throw resource.fail("report download URL is invalid");
        }
        String host = uri.getHost() == null ? "" : uri.getHost().toLowerCase(Locale.ROOT);
        boolean expectedS3 =
                (uri.getPort() == -1 || uri.getPort() == 443)
                        && host.matches(
                                "(?:[a-z0-9.-]+\\.)?s3(?:[.-][a-z0-9-]+)*\\.amazonaws\\.com");
        if (!AmazonAdsConfig.Endpoint.isHttps(uri)
                || !expectedS3 && !config.extraDownloadHosts.contains(host)
                || uri.getRawPath().isEmpty()
                || location.length() > 16384) {
            throw resource.fail("report download origin is outside the allowed HTTPS hosts");
        }
        String path = uri.getRawPath() + (uri.getRawQuery() == null ? "" : "?" + uri.getRawQuery());
        Request request =
                new Request(
                        new AmazonAdsConfig.Endpoint(uri),
                        path,
                        null,
                        "application/octet-stream",
                        null,
                        null,
                        null,
                        null,
                        true);
        return request(
                resource,
                request,
                Set.of(200, 401, 403, 404),
                deadline,
                (status, response, raw, expires) -> {
                    if (status != 200) {
                        return null;
                    }
                    HttpEntity body = response.getEntity();
                    if (request.body == null) {
                        throw resource.fail("missing report download body");
                    }
                    Header encoding = response.getFirstHeader("Content-Encoding");
                    try {
                        AmazonAdsReportSpool result =
                                AmazonAdsReportSpool.read(
                                        config,
                                        resource,
                                        window,
                                        profile,
                                        json,
                                        body.getContent(),
                                        encoding == null ? null : encoding.getValue(),
                                        () -> remaining(resource, expires));
                        synchronized (this) {
                            if (closed) {
                                result.close();
                                throw new InterruptedException("AmazonAds reader closed");
                            }
                            spool = result;
                        }
                        return result;
                    } catch (EOFException ex) {
                        quietClose(raw);
                        remaining(resource, expires);
                        throw resource.fail("truncated gzip report download");
                    } catch (IOException | InterruptedException | RuntimeException error) {
                        quietClose(raw);
                        throw error;
                    }
                });
    }

    synchronized void release(AmazonAdsReportSpool result) {
        if (result != null) {
            result.close();
            if (spool == result) {
                spool = null;
            }
        }
    }

    private Reply request(
            AmazonAdsResource resource, Request request, Set<Integer> permitted, long deadline)
            throws InterruptedException {
        return request(
                resource,
                request,
                permitted,
                deadline,
                (status, response, raw, expires) -> {
                    if (status != 200 && status != 425) {
                        return new Reply(status, null);
                    }
                    int max =
                            request.endpoint == config.oauth || status == 425
                                    ? Math.min(config.maxResponse, 65536)
                                    : config.maxResponse;
                    try {
                        return new Reply(
                                status,
                                decode(
                                        resource,
                                        entity(resource, response, raw, max, expires),
                                        expires));
                    } catch (IOException | AmazonAdsResource.Failure ex) {
                        if (status != 425) {
                            throw ex;
                        }
                        remaining(resource, expires);
                        return new Reply(status, null);
                    }
                });
    }

    private <T> T request(
            AmazonAdsResource resource,
            Request request,
            Set<Integer> permitted,
            long deadline,
            ResponseBody<T> decoder)
            throws InterruptedException {
        for (int attempt = 1; ; attempt++) {
            remaining(resource, deadline);
            if (attempted) {
                pause(
                        resource,
                        TimeUnit.MILLISECONDS.toNanos(config.intervalMs)
                                - (System.nanoTime() - previousAttempt),
                        deadline);
            }
            Backoff retry;
            try {
                return execute(resource, request, permitted, deadline, decoder);
            } catch (Backoff ex) {
                retry = ex;
            } catch (SSLException ex) {
                remaining(resource, deadline);
                throw resource.failure("TLS peer verification or negotiation failed");
            } catch (JsonProcessingException ex) {
                throw resource.failure("invalid JSON response");
            } catch (ZipException ex) {
                throw resource.failure("invalid gzip response");
            } catch (HttpException ex) {
                throw resource.failure("malformed HTTP response");
            } catch (IOException ex) {
                remaining(resource, deadline);
                if (!request.repeatable) {
                    throw resource.fail("request outcome is unknown after I/O failure");
                }
                retry = new Backoff("I/O failure", 0);
            }
            long left = remaining(resource, deadline);
            if (attempt >= config.attempts) {
                throw resource.failure(retry.reason + " after " + attempt + " attempts");
            }
            long millis = Math.min(5000, 250L << (attempt - 1));
            long delay =
                    TimeUnit.MILLISECONDS.toNanos(
                            Math.max(
                                    retry.minimum,
                                    millis + ThreadLocalRandom.current().nextLong(millis / 2 + 1)));
            if (delay >= left) {
                throw resource.failure("retry delay exceeds the remaining deadline");
            }
            pause(resource, delay, deadline);
        }
    }

    private <T> T execute(
            AmazonAdsResource resource,
            Request request,
            Set<Integer> permitted,
            long deadline,
            ResponseBody<T> decoder)
            throws IOException, HttpException, InterruptedException, Backoff {
        Socket raw = new Socket(Proxy.NO_PROXY);
        ScheduledFuture<?> abort = null;
        try {
            synchronized (this) {
                long left = remaining(resource, deadline);
                connected = raw;
                abort = timeouts.schedule(() -> quietClose(raw), left, TimeUnit.NANOSECONDS);
            }
            previousAttempt = System.nanoTime();
            attempted = true;
            int timeout =
                    (int)
                            Math.min(
                                    config.idleMs,
                                    Math.max(
                                            1,
                                            TimeUnit.NANOSECONDS.toMillis(
                                                    remaining(resource, deadline))));
            raw.connect(
                    new InetSocketAddress(request.endpoint.host, request.endpoint.port), timeout);
            raw.setSoTimeout(timeout);
            SSLSocketFactory tls = (SSLSocketFactory) SSLSocketFactory.getDefault();
            try (SSLSocket socket =
                            (SSLSocket)
                                    tls.createSocket(
                                            raw,
                                            request.endpoint.host,
                                            request.endpoint.port,
                                            true);
                    DefaultBHttpClientConnection wire =
                            new DefaultBHttpClientConnection(8192, null, null, HEADERS)) {
                SSLParameters parameters = socket.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                socket.setSSLParameters(parameters);
                socket.startHandshake();

                HttpRequest message;
                if (request.body == null) {
                    message = new BasicHttpRequest("GET", request.target);
                } else {
                    BasicHttpEntityEnclosingRequest post =
                            new BasicHttpEntityEnclosingRequest("POST", request.target);
                    post.setEntity(new ByteArrayEntity(request.body));
                    post.addHeader("Content-Type", request.mediaType);
                    post.addHeader("Content-Length", Integer.toString(request.body.length));
                    message = post;
                }
                message.addHeader("Host", request.endpoint.authority);
                message.addHeader("Accept", request.acceptedType);
                message.addHeader("Accept-Encoding", "gzip");
                message.addHeader("Connection", "close");
                if (request.authorization != null) {
                    message.addHeader("Authorization", request.authorization);
                }
                if (request.clientId != null) {
                    message.addHeader("Amazon-Advertising-API-ClientId", request.clientId);
                }
                if (request.profile != null) {
                    message.addHeader("Amazon-Advertising-API-Scope", request.profile);
                }
                wire.bind(socket);
                HttpResponse response =
                        new HttpRequestExecutor().execute(message, wire, HttpCoreContext.create());
                int status = response.getStatusLine().getStatusCode();
                if (permitted.contains(status) || status == 401 && request.endpoint == config.api) {
                    return decoder.read(status, response, raw, deadline);
                }
                if (status == 429
                        || request.repeatable
                                && (status == 408
                                        || status == 500
                                        || status == 502
                                        || status == 503
                                        || status == 504)) {
                    throw new Backoff("HTTP " + status, retryDelay(response));
                }
                // Provider response text, headers, credentials and query parameters are private.
                throw resource.failure("HTTP " + status);
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
            AmazonAdsResource resource,
            HttpResponse response,
            Socket raw,
            int maximum,
            long deadline)
            throws IOException, InterruptedException {
        HttpEntity entity = response.getEntity();
        if (entity == null) {
            throw resource.failure("missing response body");
        }
        Header header = response.getFirstHeader("Content-Encoding");
        String encoding = header == null ? "identity" : header.getValue();
        if (!"identity".equalsIgnoreCase(encoding) && !"gzip".equalsIgnoreCase(encoding)) {
            throw resource.failure("unsupported content encoding");
        }
        InputStream input = entity.getContent();
        try {
            if ("gzip".equalsIgnoreCase(encoding)) {
                input = new GZIPInputStream(input);
            }
            byte[] buffer = new byte[8192];
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            for (int count; (count = input.read(buffer)) != -1; ) {
                remaining(resource, deadline);
                if (count > maximum - bytes.size()) {
                    throw resource.failure("response exceeds max_response_bytes");
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

    private JsonNode decode(AmazonAdsResource resource, byte[] bytes, long deadline)
            throws IOException, InterruptedException {
        try (JsonParser parser = json.getFactory().createParser(bytes)) {
            int count = 0;
            while (parser.nextToken() != null) {
                if (++count > MAX_JSON_TOKENS) {
                    throw resource.failure("JSON token limit exceeded");
                }
                if ((count & 1023) == 0) {
                    remaining(resource, deadline);
                }
            }
        }
        JsonNode root = json.readTree(bytes);
        remaining(resource, deadline);
        if (root == null || !root.isObject() && !root.isArray()) {
            throw resource.failure("invalid collection response");
        }
        JsonNode errors = root.get("errors");
        if (errors != null
                && !errors.isNull()
                && !(errors.isContainerNode() && errors.isEmpty())
                && !(errors.isTextual() && errors.textValue().isEmpty())) {
            throw resource.failure("provider reported an error response");
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

    private void pause(AmazonAdsResource resource, long nanos, long deadline)
            throws InterruptedException {
        if (nanos >= remaining(resource, deadline)) {
            throw resource.failure("request interval exceeds the remaining deadline");
        }
        if (nanos > 0) {
            TimeUnit.NANOSECONDS.sleep(nanos);
        }
    }

    long remaining(AmazonAdsResource resource, long deadline) throws InterruptedException {
        if (closed || Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("AmazonAds request closed");
        }
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
            throw resource.failure("page deadline exceeded");
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
        release(spool);
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

    /**
     * Carries only the request details permitted for that origin and operation.
     *
     * <p>The repeatability flag controls whether an uncertain HTTP outcome may be retried.
     */
    private static final class Request {
        final AmazonAdsConfig.Endpoint endpoint;
        final String target;
        final String mediaType;
        final String acceptedType;
        final byte[] body;
        final String authorization;
        final String clientId;
        final String profile;
        final boolean repeatable;

        Request(
                AmazonAdsConfig.Endpoint endpoint,
                String target,
                String mediaType,
                String acceptedType,
                byte[] body,
                String authorization,
                String clientId,
                String profile,
                boolean repeatable) {
            this.endpoint = endpoint;
            this.target = target;
            this.mediaType = mediaType;
            this.acceptedType = acceptedType;
            this.body = body;
            this.authorization = authorization;
            this.clientId = clientId;
            this.profile = profile;
            this.repeatable = repeatable;
        }
    }

    /**
     * Returns an explicitly allowed Ads status with a bounded decoded body.
     *
     * <p>The report protocol can inspect a duplicate response without publishing the provider error
     * text.
     */
    static final class Reply {
        final int status;
        final JsonNode payload;

        Reply(int status, JsonNode payload) {
            this.status = status;
            this.payload = payload;
        }
    }

    /**
     * Decodes a response while the transport still owns the socket deadline.
     *
     * <p>The callback can consume a signed report before the connection is released.
     */
    private interface ResponseBody<T> {
        T read(int status, HttpResponse response, Socket raw, long deadline)
                throws IOException, InterruptedException;
    }

    /**
     * Retry classification with sanitized diagnostic text and a minimum wait.
     *
     * <p>The caller combines the provider delay with local pacing under the current deadline.
     */
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
