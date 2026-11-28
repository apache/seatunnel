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
package org.apache.seatunnel.connectors.seatunnel.aircall;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.cfg.JsonNodeFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.MissingNode;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.config.MessageConstraints;
import org.apache.http.impl.DefaultBHttpClientConnection;
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
import java.util.Locale;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;

/** One bounded direct HTTPS connection per attempt without wire or header loggers. */
final class AircallHttpTransport implements AutoCloseable {
    private static final MessageConstraints HEADERS =
            MessageConstraints.custom().setMaxLineLength(16384).setMaxHeaderCount(64).build();
    private static final int MAX_JSON_TOKENS = 250000;
    private final AircallConfig config;
    private final ObjectMapper json;
    private final ScheduledThreadPoolExecutor timeouts;
    private volatile boolean closed;
    private Socket connected;
    private long previousAttempt;
    private boolean attempted;

    AircallHttpTransport(AircallConfig config) {
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
        timeouts =
                new ScheduledThreadPoolExecutor(1, task -> daemon(task, "aircall-http-deadline"));
        timeouts.setRemoveOnCancelPolicy(true);
    }

    AircallPage page(
            AircallResource resource,
            AircallResource.Request request,
            AircallSplit state,
            AircallSplit.Cursor cursor,
            long deadline)
            throws InterruptedException {
        for (int attempt = 1; ; attempt++) {
            remaining(resource, deadline);
            if (attempted) {
                pause(
                        resource,
                        TimeUnit.MILLISECONDS.toNanos(config.minInterval)
                                - (System.nanoTime() - previousAttempt),
                        deadline);
            }
            Backoff retry;
            try {
                Answer result = execute(resource, request, deadline);
                if (result.mayBeTruncated) {
                    return AircallPage.saturated();
                }
                if (result.payload == null) {
                    return AircallPage.empty(resource, cursor);
                }
                return AircallPage.parse(
                        config, resource, request, state, cursor, result.payload, this, deadline);
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

    private Answer execute(AircallResource resource, AircallResource.Request request, long deadline)
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
                                    config.timeout,
                                    Math.max(
                                            1,
                                            TimeUnit.NANOSECONDS.toMillis(
                                                    remaining(resource, deadline))));
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

                BasicHttpRequest get = new BasicHttpRequest("GET", request.target());
                get.addHeader("Host", config.authority);
                get.addHeader("Authorization", config.authorization);
                get.addHeader("Accept", "application/json");
                get.addHeader("Accept-Encoding", "gzip");
                get.addHeader("Connection", "close");
                wire.bind(socket);
                HttpResponse response =
                        new HttpRequestExecutor().execute(get, wire, HttpCoreContext.create());
                int status = response.getStatusLine().getStatusCode();
                if (status == 200) {
                    byte[] content = entity(resource, response, raw, config.maxBytes, deadline);
                    return new Answer(decode(resource, content, deadline), false);
                }
                if (status == 408
                        || status == 429
                        || status == 500
                        || status == 502
                        || status == 503
                        || status == 504) {
                    throw new Backoff("HTTP " + status, retryDelay(response, status));
                }
                if ((status == 400 && resource.kind == AircallResource.Kind.PAGE)
                        || ((status == 403 || status == 404) && resource.parent != null)) {
                    JsonNode error = exceptionBody(resource, response, raw, deadline);
                    if (status == 400 && cap(error)) {
                        return new Answer(null, true);
                    }
                    if (resource.knownAbsent(status, error)) {
                        return new Answer(null, false);
                    }
                }
                // Do not include the response reason, response bytes, headers, path or URI.
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

    private JsonNode exceptionBody(
            AircallResource resource, HttpResponse response, Socket raw, long deadline)
            throws InterruptedException {
        try {
            byte[] bytes =
                    entity(resource, response, raw, Math.min(65536, config.maxBytes), deadline);
            JsonNode error = json.readTree(bytes);
            return error == null ? MissingNode.getInstance() : error;
        } catch (AircallResource.Failure | IOException ignored) {
            remaining(resource, deadline);
            return MissingNode.getInstance();
        }
    }

    private static boolean cap(JsonNode error) {
        if (!error.isObject()) {
            return false;
        }
        String trouble = error.path("troubleshoot").asText("").toLowerCase(Locale.ROOT);
        return trouble.contains("page * per_page must not exceed 10000")
                || error.path("errors")
                        .toString()
                        .toLowerCase(Locale.ROOT)
                        .contains("limited to 10k results");
    }

    private byte[] entity(
            AircallResource resource, HttpResponse response, Socket raw, int maximum, long deadline)
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

    private JsonNode decode(AircallResource resource, byte[] bytes, long deadline)
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
        if (root == null || !root.isObject() || root.hasNonNull("error")) {
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

    private static long retryDelay(HttpResponse response, int status) {
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
        if (status == 429) {
            for (Header header : response.getHeaders("X-AircallApi-Reset")) {
                String value = header.getValue().trim();
                if (value.matches("[0-9]+")) {
                    try {
                        long reset = Math.multiplyExact(Long.parseLong(value), 1000);
                        floor = Math.max(floor, reset - System.currentTimeMillis());
                    } catch (ArithmeticException | NumberFormatException overflow) {
                        return Long.MAX_VALUE;
                    }
                }
            }
        }
        return floor;
    }

    private void pause(AircallResource resource, long nanos, long deadline)
            throws InterruptedException {
        if (nanos >= remaining(resource, deadline)) {
            throw resource.failure("request interval exceeds the remaining deadline");
        }
        if (nanos > 0) {
            TimeUnit.NANOSECONDS.sleep(nanos);
        }
    }

    long remaining(AircallResource resource, long deadline) throws InterruptedException {
        if (closed || Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Aircall request closed");
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

    private static final class Answer {
        private final JsonNode payload;
        private final boolean mayBeTruncated;

        private Answer(JsonNode payload, boolean mayBeTruncated) {
            this.payload = payload;
            this.mayBeTruncated = mayBeTruncated;
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
