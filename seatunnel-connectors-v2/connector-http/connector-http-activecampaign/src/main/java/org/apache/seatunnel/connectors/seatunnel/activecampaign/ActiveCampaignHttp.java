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

package org.apache.seatunnel.connectors.seatunnel.activecampaign;

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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;

/** Exposes a single cancellable HTTP exchange without header or wire logging. */
final class ActiveCampaignHttp implements AutoCloseable {
    private static final MessageConstraints HEADERS =
            MessageConstraints.custom().setMaxLineLength(16384).setMaxHeaderCount(64).build();
    private static final int MAX_JSON_TOKENS = 250000;
    private final ActiveCampaignSourceOptions.Settings settings;
    private final ScheduledThreadPoolExecutor timer;
    private final ObjectMapper mapper;
    private boolean closed;
    private Socket current;
    private long previousStart;
    private boolean started;

    ActiveCampaignHttp(ActiveCampaignSourceOptions.Settings settings) {
        this.settings = settings;
        timer = new ScheduledThreadPoolExecutor(1, task -> daemon(task, "activecampaign-deadline"));
        timer.setRemoveOnCancelPolicy(true);
        JsonFactory json =
                new JsonFactory()
                        .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                        .setStreamReadConstraints(
                                StreamReadConstraints.builder()
                                        .maxNestingDepth(100)
                                        .maxStringLength(settings.maxBytes)
                                        .maxNumberLength(1000)
                                        .build());
        mapper =
                new ObjectMapper(json)
                        .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                        .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
                        .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                        .configure(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES, false);
    }

    static Thread daemon(Runnable task, String name) {
        Thread thread = new Thread(task, name);
        thread.setDaemon(true);
        return thread;
    }

    /** All attempts are charged to the same deadline and account pacing. */
    JsonNode fetch(String resource, int offset, long deadline) throws InterruptedException {
        for (int attempt = 1; ; attempt++) {
            check(resource, deadline);
            if (started) {
                sleep(
                        resource,
                        TimeUnit.MILLISECONDS.toNanos(settings.interval)
                                - (System.nanoTime() - previousStart),
                        deadline);
            }
            Retry retry;
            try {
                return exchange(resource, offset, deadline);
            } catch (Retry ex) {
                retry = ex;
            } catch (SSLException ex) {
                check(resource, deadline);
                throw failure(resource, "TLS peer verification or negotiation failed");
            } catch (ZipException ex) {
                throw failure(resource, "invalid gzip response");
            } catch (HttpException ex) {
                throw failure(resource, "invalid HTTP response");
            } catch (IOException ex) {
                retry = new Retry("I/O failure", 0);
            }
            check(resource, deadline);
            if (attempt == settings.attempts) {
                throw failure(resource, retry.reason + " after " + attempt + " attempts");
            }
            long base = Math.min(5000, 200L << (attempt - 1));
            long delay =
                    Math.max(
                            retry.floorMs,
                            base + ThreadLocalRandom.current().nextLong(base / 2 + 1));
            sleep(resource, TimeUnit.MILLISECONDS.toNanos(delay), deadline);
        }
    }

    private JsonNode exchange(String resource, int offset, long deadline)
            throws IOException, HttpException, Retry, InterruptedException {
        Socket raw = new Socket(Proxy.NO_PROXY);
        ScheduledFuture<?> abort = null;
        try {
            synchronized (this) {
                check(resource, deadline);
                current = raw;
                abort =
                        timer.schedule(
                                () -> closeSocket(raw),
                                Math.max(0, deadline - System.nanoTime()),
                                TimeUnit.NANOSECONDS);
            }
            previousStart = System.nanoTime();
            started = true;
            int timeout =
                    (int)
                            Math.min(
                                    settings.socketTimeout,
                                    Math.max(
                                            1,
                                            TimeUnit.NANOSECONDS.toMillis(
                                                    deadline - previousStart)));
            raw.connect(new InetSocketAddress(settings.host, settings.port), timeout);
            raw.setSoTimeout(timeout);
            SSLSocketFactory tls = (SSLSocketFactory) SSLSocketFactory.getDefault();
            try (SSLSocket socket =
                            (SSLSocket) tls.createSocket(raw, settings.host, settings.port, true);
                    DefaultBHttpClientConnection connection =
                            new DefaultBHttpClientConnection(8192, null, null, HEADERS)) {
                SSLParameters parameters = socket.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                socket.setSSLParameters(parameters);
                socket.startHandshake();
                connection.bind(socket);
                BasicHttpRequest request =
                        new BasicHttpRequest(
                                "GET",
                                "/api/3/"
                                        + resource
                                        + "?limit="
                                        + settings.pageSize
                                        + "&offset="
                                        + offset);
                request.addHeader("Host", settings.authority);
                request.addHeader("Api-Token", settings.token);
                request.addHeader("Accept", "application/json");
                request.addHeader("Accept-Encoding", "gzip");
                request.addHeader("Connection", "close");
                HttpResponse response =
                        new HttpRequestExecutor()
                                .execute(request, connection, HttpCoreContext.create());
                int code = response.getStatusLine().getStatusCode();
                if (code == 408
                        || code == 429
                        || code == 500
                        || code == 502
                        || code == 503
                        || code == 504) {
                    throw new Retry("HTTP " + code, retryAfter(response));
                }
                if (code != 200) {
                    throw failure(resource, "HTTP " + code);
                }
                return decode(resource, response, raw, deadline);
            }
        } finally {
            closeSocket(raw);
            if (abort != null) {
                abort.cancel(false);
            }
            synchronized (this) {
                if (current == raw) {
                    current = null;
                }
            }
        }
    }

    private JsonNode decode(String resource, HttpResponse response, Socket socket, long deadline)
            throws IOException, InterruptedException {
        HttpEntity entity = response.getEntity();
        if (entity == null) {
            throw failure(resource, "missing response body");
        }
        Header encodingHeader = response.getFirstHeader("Content-Encoding");
        String encoding = encodingHeader == null ? "identity" : encodingHeader.getValue();
        if (!"identity".equalsIgnoreCase(encoding) && !"gzip".equalsIgnoreCase(encoding)) {
            throw failure(resource, "unsupported response compression");
        }
        byte[] bytes;
        InputStream wire = entity.getContent();
        InputStream body = wire;
        try {
            if ("gzip".equalsIgnoreCase(encoding)) {
                body = new GZIPInputStream(wire);
            }
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            byte[] chunk = new byte[8192];
            for (int count; (count = body.read(chunk)) != -1; ) {
                check(resource, deadline);
                if (count > settings.maxBytes - output.size()) {
                    throw failure(resource, "response exceeds max_response_bytes");
                }
                output.write(chunk, 0, count);
            }
            bytes = output.toByteArray();
        } catch (IOException | InterruptedException | RuntimeException ex) {
            // Discard a failed entity by closing the transport before closing its stream.
            closeSocket(socket);
            throw ex;
        } finally {
            try {
                body.close();
            } catch (IOException ignored) {
                // The transport is already being discarded.
            }
        }
        try (JsonParser parser = mapper.getFactory().createParser(bytes)) {
            int tokens = 0;
            while (parser.nextToken() != null) {
                if (++tokens > MAX_JSON_TOKENS) {
                    throw failure(resource, "JSON response exceeds the token limit");
                }
                if ((tokens & 1023) == 0) {
                    check(resource, deadline);
                }
            }
            JsonNode root = mapper.readTree(bytes);
            if (root == null
                    || !root.isObject()
                    || present(root.get("errors"))
                    || present(root.get("error"))) {
                throw failure(resource, "invalid collection response");
            }
            check(resource, deadline);
            return root;
        } catch (JsonProcessingException ex) {
            throw failure(resource, "malformed or unsupported JSON response");
        }
    }

    private static boolean present(JsonNode node) {
        return node != null
                && !node.isNull()
                && !(node.isContainerNode() && node.isEmpty())
                && !(node.isTextual() && node.textValue().isEmpty());
    }

    private static long retryAfter(HttpResponse response) {
        long floor = 0;
        for (Header header : response.getHeaders("Retry-After")) {
            String value = header.getValue().trim();
            if (value.matches("[0-9]+")) {
                try {
                    floor = Math.max(floor, Math.multiplyExact(Long.parseLong(value), 1000));
                } catch (NumberFormatException | ArithmeticException ex) {
                    return Long.MAX_VALUE;
                }
            } else {
                try {
                    Instant instant =
                            ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME)
                                    .toInstant();
                    floor = Math.max(floor, Duration.between(Instant.now(), instant).toMillis());
                } catch (DateTimeException | ArithmeticException ignored) {
                    // A malformed hint leaves the bounded backoff active.
                }
            }
        }
        return floor;
    }

    private static void sleep(String resource, long delay, long deadline)
            throws InterruptedException {
        if (delay >= deadline - System.nanoTime()) {
            throw failure(resource, "request delay exceeds the remaining page deadline");
        }
        if (delay > 0) {
            TimeUnit.NANOSECONDS.sleep(delay);
        }
    }

    synchronized void check(String resource, long deadline) throws InterruptedException {
        if (closed || Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("ActiveCampaign request closed");
        }
        if (deadline - System.nanoTime() <= 0) {
            throw failure(resource, "page deadline exceeded");
        }
    }

    static Failure failure(String resource, String reason) {
        return new Failure("ActiveCampaign " + resource + " " + reason);
    }

    @Override
    public synchronized void close() {
        closed = true;
        closeSocket(current);
        timer.shutdownNow();
    }

    private static void closeSocket(Socket socket) {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // The request is being discarded.
            }
        }
    }

    static final class Failure extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        private Failure(String message) {
            super(message);
        }
    }

    private static final class Retry extends Exception {
        private static final long serialVersionUID = 1L;
        private final String reason;
        private final long floorMs;

        Retry(String reason, long floorMs) {
            this.reason = reason;
            this.floorMs = floorMs;
        }
    }
}
