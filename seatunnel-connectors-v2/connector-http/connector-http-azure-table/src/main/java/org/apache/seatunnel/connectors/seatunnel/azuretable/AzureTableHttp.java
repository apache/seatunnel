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
package org.apache.seatunnel.connectors.seatunnel.azuretable;

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
import org.apache.http.impl.DefaultBHttpClientConnection;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.http.protocol.HttpRequestExecutor;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
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
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;

/**
 * Quiet HTTPS transport for fixed-origin Azure Table Shared Key reads.
 *
 * <p>Redirects are errors. The connector closes the active socket on timeout or cancellation
 * without logging request credentials.
 */
final class AzureTableHttp implements AutoCloseable {
    private static final MessageConstraints HEADERS =
            MessageConstraints.custom().setMaxLineLength(16384).setMaxHeaderCount(64).build();
    private static final Set<Integer> TRANSIENT = Set.of(408, 429, 500, 502, 503, 504);

    private static final DateTimeFormatter HTTP_DATE =
            DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.US)
                    .withZone(ZoneOffset.UTC);

    private final AzureTableConfig config;
    final ObjectMapper json;
    private final ScheduledThreadPoolExecutor watchdog;

    // Only the reader worker issues requests. Cancellation may close the active TLS socket.
    private volatile boolean closed;
    private Socket active;
    private long previousAttempt;
    private boolean attempted;

    // Connector-private trust configuration applies only to this reader's socket factory.
    private final SSLSocketFactory tlsFactory;

    AzureTableHttp(AzureTableConfig config) {
        this.config = config;
        tlsFactory = config.socketFactory();
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
                            Thread thread = new Thread(task, "azuretable-https-deadline");
                            thread.setDaemon(true);
                            return thread;
                        });
        watchdog.setRemoveOnCancelPolicy(true);
    }

    long deadline() {
        return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.operationTimeout);
    }

    /**
     * Reads one page within finite per-attempt and total retry deadlines.
     *
     * <p>Each retry gets a fresh signed date. Status and bounded retry hints may be exposed to the
     * reader, while request paths, signatures and error bodies stay private.
     */
    Reply request(String collection, String path, String query, long deadline)
            throws InterruptedException {
        for (int attempt = 0; ; attempt++) {
            if (attempted) {
                delay(
                        collection,
                        TimeUnit.MILLISECONDS.toNanos(config.interval)
                                - (System.nanoTime() - previousAttempt),
                        deadline);
            }
            long retryHint;
            String reason;
            try {
                return exchange(collection, path, query, deadline);
            } catch (Retry ex) {
                retryHint = ex.delay;
                reason = "HTTP " + ex.status;
            } catch (SSLException ex) {
                remaining(collection, deadline);
                throw failure(collection, "HTTPS certificate verification or negotiation failed");
            } catch (ZipException ex) {
                throw failure(collection, "invalid gzip response");
            } catch (JsonProcessingException ex) {
                throw failure(collection, "invalid JSON response");
            } catch (HttpException ex) {
                throw failure(collection, "malformed HTTP response");
            } catch (IOException ex) {
                remaining(collection, deadline);
                retryHint = 0;
                reason = "HTTPS I/O failure or attempt timeout";
            }
            if (attempt >= config.retries) {
                throw failure(collection, reason + " after " + (attempt + 1) + " attempts");
            }
            long backoff = Math.max(retryHint, Math.min(5000, 250L << attempt));
            delay(collection, TimeUnit.MILLISECONDS.toNanos(backoff), deadline);
        }
    }

    private Reply exchange(String collection, String path, String query, long deadline)
            throws IOException, HttpException, InterruptedException, Retry {
        long attemptTime =
                Math.min(
                        remaining(collection, deadline),
                        TimeUnit.MILLISECONDS.toNanos(config.requestTimeout));
        long expires = System.nanoTime() + attemptTime;
        Socket raw = new Socket(Proxy.NO_PROXY);
        ScheduledFuture<?> abort = null;
        try {
            synchronized (this) {
                remaining(collection, expires);
                active = raw;
                abort =
                        watchdog.schedule(
                                () -> closeSocket(raw), attemptTime, TimeUnit.NANOSECONDS);
            }
            previousAttempt = System.nanoTime();
            attempted = true;
            int timeout = (int) Math.max(1, TimeUnit.NANOSECONDS.toMillis(attemptTime));
            String host = config.endpoint.getHost();
            int port = 443;
            raw.connect(
                    new InetSocketAddress(host, port), Math.min(timeout, config.connectTimeout));
            raw.setSoTimeout(timeout);
            try (SSLSocket socket = (SSLSocket) tlsFactory.createSocket(raw, host, port, true);
                    DefaultBHttpClientConnection wire =
                            new DefaultBHttpClientConnection(8192, null, null, HEADERS)) {
                SSLParameters parameters = socket.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                socket.setSSLParameters(parameters);
                socket.startHandshake();
                String date = HTTP_DATE.format(Instant.now());
                HttpRequest request = new BasicHttpRequest("GET", path + query);
                request.addHeader("Host", host);
                request.addHeader("x-ms-date", date);
                request.addHeader("Authorization", authorization(config, path, date));
                request.addHeader("x-ms-version", "2019-02-02");
                request.addHeader("DataServiceVersion", "3.0");
                request.addHeader("MaxDataServiceVersion", "3.0");
                request.addHeader("Accept", "application/json;odata=fullmetadata");
                request.addHeader("Accept-Encoding", "gzip");
                request.addHeader("Connection", "close");
                wire.bind(socket);
                HttpResponse response =
                        new HttpRequestExecutor().execute(request, wire, HttpCoreContext.create());
                int status = response.getStatusLine().getStatusCode();
                if (TRANSIENT.contains(status)) {
                    throw new Retry(status, retryAfter(response));
                }
                if (status != 200) {
                    throw failure(collection, "HTTP " + status);
                }
                return new Reply(
                        body(collection, response, expires, config.bodyLimit),
                        token(collection, response, "NextTableName"),
                        token(collection, response, "NextPartitionKey"),
                        token(collection, response, "NextRowKey"));
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

    private JsonNode body(String collection, HttpResponse response, long deadline, int limit)
            throws IOException, InterruptedException {
        HttpEntity entity = response.getEntity();
        if (entity == null) {
            throw failure(collection, "missing HTTPS response body");
        }
        Header[] encodings = response.getHeaders("Content-Encoding");
        boolean gzip =
                encodings.length == 1 && "gzip".equalsIgnoreCase(encodings[0].getValue().trim());
        if (encodings.length > 0
                && !(gzip
                        || encodings.length == 1
                                && "identity".equalsIgnoreCase(encodings[0].getValue().trim()))) {
            throw failure(collection, "unsupported HTTPS content encoding");
        }
        InputStream wire = new WireLimit(entity.getContent(), limit, collection);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] block = new byte[8192];
        try (InputStream input = gzip ? new GZIPInputStream(wire) : wire) {
            for (int n; (n = input.read(block)) != -1; ) {
                remaining(collection, deadline);
                if (n > limit - output.size()) {
                    throw failure(collection, "decoded response exceeds max_response_bytes");
                }
                output.write(block, 0, n);
            }
        }
        byte[] bytes = output.toByteArray();
        try (JsonParser parser = json.getFactory().createParser(bytes)) {
            int tokens = 0;
            while (parser.nextToken() != null) {
                if (++tokens > 500000) {
                    throw failure(collection, "JSON token limit exceeded");
                }
                if ((tokens & 1023) == 0) {
                    remaining(collection, deadline);
                }
            }
        }
        JsonNode root = json.readTree(bytes);
        remaining(collection, deadline);
        if (root == null || !root.isObject()) {
            throw failure(collection, "expected a JSON object");
        }
        return root;
    }

    /**
     * Signs the Table-specific canonical resource, which excludes OData and continuation queries.
     *
     * <p>These GETs have no content headers or comp parameter. Unlike Blob signing, the date line
     * is populated even when the request sends only x-ms-date.
     */
    static String authorization(AzureTableConfig config, String rawPath, String date) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(config.secret, "HmacSHA256"));
            String message = "GET\n\n\n" + date + "\n/" + config.account + rawPath;
            String signature =
                    Base64.getEncoder()
                            .encodeToString(mac.doFinal(message.getBytes(StandardCharsets.UTF_8)));
            return "SharedKey " + config.account + ":" + signature;
        } catch (GeneralSecurityException ex) {
            throw failure("read", "cannot initialize Shared Key signing");
        }
    }

    private static String token(String collection, HttpResponse response, String name) {
        Header[] fields = response.getHeaders("x-ms-continuation-" + name);
        if (fields.length > 1) {
            throw failure(collection, "repeated continuation header");
        }
        if (fields.length == 0 || fields[0].getValue().isEmpty()) {
            return null;
        }
        String current = fields[0].getValue();
        if (current.length() > 4096 || current.chars().anyMatch(c -> c < 32 || c > 126)) {
            throw failure(collection, "invalid opaque continuation header");
        }
        return current;
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
        Header hint = response.getFirstHeader("x-ms-retry-after-ms");
        if (hint != null && hint.getValue().trim().matches("[0-9]+")) {
            try {
                wait = Math.max(wait, Long.parseLong(hint.getValue().trim()));
            } catch (NumberFormatException ex) {
                return Long.MAX_VALUE;
            }
        }
        return wait;
    }

    private void delay(String collection, long nanos, long deadline) throws InterruptedException {
        if (nanos >= remaining(collection, deadline)) {
            throw failure(collection, "retry or pacing delay exceeds the operation deadline");
        }
        while (nanos > 0) {
            long started = System.nanoTime();
            TimeUnit.NANOSECONDS.sleep(Math.min(nanos, TimeUnit.MILLISECONDS.toNanos(100)));
            remaining(collection, deadline);
            nanos -= System.nanoTime() - started;
        }
    }

    long remaining(String collection, long deadline) throws InterruptedException {
        if (closed || Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("AzureTableStorage source closed");
        }
        long nanos = deadline - System.nanoTime();
        if (nanos <= 0) {
            throw failure(collection, "HTTPS operation deadline exceeded");
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

    static AzureTableApi.Failure failure(String collection, String reason) {
        return new AzureTableApi.Failure(collection, reason);
    }

    /**
     * Holds a fully validated response body and its protocol continuation headers.
     *
     * <p>The caller interprets these markers according to the requested collection.
     */
    static final class Reply {
        final JsonNode json;
        final String nextTable;
        final String nextPartition;
        final String nextRow;

        Reply(JsonNode json, String nextTable, String nextPartition, String nextRow) {
            this.json = json;
            this.nextTable = nextTable;
            this.nextPartition = nextPartition;
            this.nextRow = nextRow;
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
        private final String collection;
        private int count;

        WireLimit(InputStream body, int limit, String collection) {
            super(body);
            this.limit = limit;
            this.collection = collection;
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
                throw failure(collection, "wire response exceeds max_response_bytes");
            }
        }

        @Override
        public void close() {
            // The HTTPS call closes instead of draining the entity.
        }
    }
}
