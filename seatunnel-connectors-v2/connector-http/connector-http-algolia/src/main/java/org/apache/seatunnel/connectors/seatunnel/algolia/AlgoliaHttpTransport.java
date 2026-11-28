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
package org.apache.seatunnel.connectors.seatunnel.algolia;

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
final class AlgoliaHttpTransport implements AutoCloseable {
    private static final MessageConstraints HEADERS =
            MessageConstraints.custom().setMaxLineLength(16384).setMaxHeaderCount(64).build();
    private static final int MAX_JSON_TOKENS = 250000;
    private final AlgoliaConfig config;
    private final ObjectMapper json;
    private final ScheduledThreadPoolExecutor timeouts;
    private volatile boolean closed;
    private Socket connected;
    private long previousAttempt;
    private boolean attempted;

    AlgoliaHttpTransport(AlgoliaConfig config) {
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
                new ScheduledThreadPoolExecutor(1, task -> daemon(task, "algolia-http-deadline"));
        timeouts.setRemoveOnCancelPolicy(true);
    }

    AlgoliaPage page(
            AlgoliaResource resource, String parent, AlgoliaSplit.Cursor cursor, long deadline)
            throws InterruptedException {
        byte[] body = null;
        if (resource == AlgoliaResource.INDEXES_QUERY) {
            try {
                body =
                        json.writeValueAsBytes(
                                json.createObjectNode()
                                        .put("params", config.query.params(cursor.page)));
            } catch (JsonProcessingException ex) {
                throw resource.failure("could not construct the search request");
            }
        }
        JsonNode payload =
                request(resource, resource.target(config, parent, cursor), body, deadline);
        return AlgoliaPage.read(config, resource, parent, cursor, payload, this, deadline);
    }

    private JsonNode request(AlgoliaResource resource, String target, byte[] body, long deadline)
            throws InterruptedException {
        for (int attempt = 1; ; attempt++) {
            remaining(resource, deadline);
            if (attempted) {
                pause(
                        resource,
                        TimeUnit.MILLISECONDS.toNanos(config.interval)
                                - (System.nanoTime() - previousAttempt),
                        deadline);
            }
            Backoff retry;
            try {
                return execute(resource, target, body, deadline);
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
                // Search POST requests are read-only operations.
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

    private JsonNode execute(AlgoliaResource resource, String target, byte[] body, long deadline)
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
                                    config.socketTimeout,
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
                message.addHeader("X-Algolia-Application-Id", config.applicationId);
                message.addHeader("X-Algolia-API-Key", config.token);
                message.addHeader("Accept", "application/json");
                message.addHeader("Accept-Encoding", "gzip");
                message.addHeader("Connection", "close");
                wire.bind(socket);
                HttpResponse response =
                        new HttpRequestExecutor().execute(message, wire, HttpCoreContext.create());
                int status = response.getStatusLine().getStatusCode();
                if (status == 200) {
                    return decode(
                            resource,
                            entity(resource, response, raw, config.maxBytes, deadline),
                            deadline);
                }
                if (status == 408
                        || status == 429
                        || status == 500
                        || status == 502
                        || status == 503
                        || status == 504) {
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
            AlgoliaResource resource, HttpResponse response, Socket raw, int maximum, long deadline)
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

    private JsonNode decode(AlgoliaResource resource, byte[] bytes, long deadline)
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

    private void pause(AlgoliaResource resource, long nanos, long deadline)
            throws InterruptedException {
        if (nanos >= remaining(resource, deadline)) {
            throw resource.failure("request interval exceeds the remaining deadline");
        }
        if (nanos > 0) {
            TimeUnit.NANOSECONDS.sleep(nanos);
        }
    }

    long remaining(AlgoliaResource resource, long deadline) throws InterruptedException {
        if (closed || Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Algolia request closed");
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

    /** Retry classification with only sanitized diagnostic text and a minimum wait. */
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
