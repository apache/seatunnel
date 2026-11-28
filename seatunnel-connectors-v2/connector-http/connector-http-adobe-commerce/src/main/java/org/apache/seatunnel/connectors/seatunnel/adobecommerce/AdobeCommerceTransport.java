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
package org.apache.seatunnel.connectors.seatunnel.adobecommerce;

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

/** HTTP Core's base connection sends only constructed requests, without a logging wrapper. */
final class AdobeCommerceTransport implements AutoCloseable {
    private static final int MAX_TOKENS = 250000;
    private static final MessageConstraints HEADERS =
            MessageConstraints.custom().setMaxHeaderCount(64).setMaxLineLength(16384).build();

    private final AdobeCommerceSettings settings;
    private final ScheduledThreadPoolExecutor aborter;
    private final ObjectMapper json;
    private volatile boolean closed;
    private Socket active;

    AdobeCommerceTransport(AdobeCommerceSettings settings) {
        this.settings = settings;
        aborter =
                new ScheduledThreadPoolExecutor(1, task -> thread(task, "adobe-commerce-deadline"));
        aborter.setRemoveOnCancelPolicy(true);
        JsonFactory factory =
                new JsonFactory()
                        .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                        .setStreamReadConstraints(
                                StreamReadConstraints.builder()
                                        .maxNestingDepth(100)
                                        .maxStringLength(settings.maxBytes)
                                        .maxNumberLength(1000)
                                        .build());
        json =
                new ObjectMapper(factory)
                        .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                        .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
                        .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                        .configure(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES, false);
    }

    static Thread thread(Runnable task, String name) {
        Thread thread = new Thread(task, name);
        thread.setDaemon(true);
        return thread;
    }

    JsonNode fetch(AdobeCommerceResource resource, String target, long deadline)
            throws InterruptedException {
        for (int attempt = 1; ; attempt++) {
            needTime(resource, deadline);
            Temporary failure;
            try {
                return get(resource, target, deadline);
            } catch (Temporary retry) {
                failure = retry;
            } catch (SSLException tls) {
                needTime(resource, deadline);
                throw resource.error("TLS peer verification or negotiation failed");
            } catch (ZipException zip) {
                throw resource.error("invalid gzip response");
            } catch (JsonProcessingException badJson) {
                throw resource.error("invalid JSON response");
            } catch (HttpException badHttp) {
                throw resource.error("invalid HTTP response");
            } catch (IOException io) {
                failure = new Temporary("transport I/O failure", 0);
            }

            long remaining = needTime(resource, deadline);
            if (attempt == settings.attempts) {
                throw resource.error(failure.reason + " after " + attempt + " attempts");
            }
            long base = 200L << (attempt - 1);
            long delay =
                    TimeUnit.MILLISECONDS.toNanos(
                            Math.max(
                                    failure.minimumWait,
                                    base + ThreadLocalRandom.current().nextLong(base / 2 + 1)));
            if (delay >= remaining) {
                throw resource.error("retry wait exceeds the remaining page deadline");
            }
            TimeUnit.NANOSECONDS.sleep(delay);
        }
    }

    /** The whole exchange has an absolute deadline in addition to per-read inactivity. */
    private JsonNode get(AdobeCommerceResource resource, String target, long deadline)
            throws IOException, HttpException, InterruptedException, Temporary {
        Socket raw = new Socket(Proxy.NO_PROXY);
        ScheduledFuture<?> abort = null;
        try {
            synchronized (this) {
                long remaining = needTime(resource, deadline);
                active = raw;
                abort = aborter.schedule(() -> closeSocket(raw), remaining, TimeUnit.NANOSECONDS);
            }
            int timeout =
                    (int)
                            Math.min(
                                    settings.socketTimeout,
                                    Math.max(
                                            1,
                                            TimeUnit.NANOSECONDS.toMillis(
                                                    needTime(resource, deadline))));
            raw.connect(new InetSocketAddress(settings.host, settings.port), timeout);
            raw.setSoTimeout(timeout);
            SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory.getDefault();
            try (SSLSocket socket =
                            (SSLSocket)
                                    factory.createSocket(raw, settings.host, settings.port, true);
                    DefaultBHttpClientConnection http =
                            new DefaultBHttpClientConnection(8192, null, null, HEADERS)) {
                SSLParameters tls = socket.getSSLParameters();
                tls.setEndpointIdentificationAlgorithm("HTTPS");
                socket.setSSLParameters(tls);
                socket.startHandshake();

                BasicHttpRequest request = new BasicHttpRequest("GET", target);
                request.addHeader("Host", settings.authority);
                request.addHeader("Authorization", "Bearer " + settings.token);
                request.addHeader("Accept", "application/json");
                request.addHeader("Accept-Encoding", "gzip");
                request.addHeader("Connection", "close");

                http.bind(socket);
                HttpResponse response =
                        new HttpRequestExecutor().execute(request, http, HttpCoreContext.create());
                int code = response.getStatusLine().getStatusCode();
                switch (code) {
                    case 200:
                        byte[] bytes = body(resource, response, raw, deadline);
                        return decode(resource, bytes, deadline);
                    case 408:
                    case 429:
                    case 500:
                    case 502:
                    case 503:
                    case 504:
                        throw new Temporary("HTTP " + code, minimumWait(response));
                    default:
                        // Error bodies, reasons, and redirect targets are provider-controlled.
                        throw resource.error("HTTP " + code);
                }
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

    private byte[] body(
            AdobeCommerceResource resource, HttpResponse response, Socket raw, long deadline)
            throws IOException, InterruptedException {
        HttpEntity entity = response.getEntity();
        if (entity == null) {
            throw resource.error("missing response body");
        }
        Header contentEncoding = response.getFirstHeader("Content-Encoding");
        String encoding = contentEncoding == null ? "identity" : contentEncoding.getValue();
        if (!"identity".equalsIgnoreCase(encoding) && !"gzip".equalsIgnoreCase(encoding)) {
            throw resource.error("unsupported content encoding");
        }
        InputStream input = entity.getContent();
        try {
            if ("gzip".equalsIgnoreCase(encoding)) {
                input = new GZIPInputStream(input);
            }
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            byte[] buffer = new byte[8192];
            int read;
            while ((read = input.read(buffer)) != -1) {
                needTime(resource, deadline);
                if (read > settings.maxBytes - bytes.size()) {
                    throw resource.error("response exceeds max_response_bytes");
                }
                bytes.write(buffer, 0, read);
            }
            return bytes.toByteArray();
        } catch (IOException | InterruptedException | RuntimeException ex) {
            // A failed entity can drain on close. Disconnect before closing its stream.
            closeSocket(raw);
            throw ex;
        } finally {
            try {
                input.close();
            } catch (IOException ignored) {
                // The response was already discarded.
            }
        }
    }

    private JsonNode decode(AdobeCommerceResource resource, byte[] bytes, long deadline)
            throws IOException, InterruptedException {
        try (JsonParser parser = json.getFactory().createParser(bytes)) {
            int count = 0;
            while (parser.nextToken() != null) {
                if (++count > MAX_TOKENS) {
                    throw resource.error("JSON token limit exceeded");
                }
                if ((count & 1023) == 0) {
                    needTime(resource, deadline);
                }
            }
        }
        JsonNode root = json.readTree(bytes);
        needTime(resource, deadline);
        if (root == null || !(root.isObject() || root.isArray())) {
            throw resource.error("invalid JSON collection response");
        }
        if (root.has("errors")
                && !root.get("errors").isNull()
                && !(root.get("errors").isContainerNode() && root.get("errors").isEmpty())) {
            throw resource.error("provider reported an error response");
        }
        return root;
    }

    long needTime(AdobeCommerceResource resource, long deadline) throws InterruptedException {
        if (closed || Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("AdobeCommerce request closed");
        }
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
            throw resource.error("page deadline exceeded");
        }
        return remaining;
    }

    private static long minimumWait(HttpResponse response) {
        long wait = 0;
        for (Header head : response.getHeaders("Retry-After")) {
            String value = head.getValue().trim();
            if (value.matches("[0-9]+")) {
                try {
                    wait = Math.max(wait, Math.multiplyExact(Long.parseLong(value), 1000L));
                } catch (NumberFormatException | ArithmeticException overflow) {
                    return Long.MAX_VALUE;
                }
            } else {
                try {
                    Instant time =
                            ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME)
                                    .toInstant();
                    wait = Math.max(wait, Duration.between(Instant.now(), time).toMillis());
                } catch (DateTimeException | ArithmeticException ignored) {
                    // Ignore malformed hints and apply the bounded retry policy.
                }
            }
        }
        return wait;
    }

    /** Break blocking TLS or HTTP reads and reject any retry after cancellation. */
    @Override
    public synchronized void close() {
        closed = true;
        closeSocket(active);
        aborter.shutdownNow();
    }

    private static void closeSocket(Socket socket) {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // The underlying connection is being discarded.
            }
        }
    }

    private static final class Temporary extends Exception {
        private static final long serialVersionUID = 1L;
        private final String reason;
        private final long minimumWait;

        Temporary(String reason, long minimumWait) {
            this.reason = reason;
            this.minimumWait = minimumWait;
        }
    }
}
