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

package org.apache.seatunnel.connectors.seatunnel.acuityscheduling;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonToken;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;

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

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPInputStream;

/**
 * HTTPS list transport with no credential, body or request-target logging. A single reader owns the
 * active socket and can close it from a different thread.
 */
final class AcuityHttp implements AutoCloseable {
    private static final ObjectMapper JSON =
            new ObjectMapper(
                            JsonFactory.builder()
                                    .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                                    .disable(StreamReadFeature.AUTO_CLOSE_SOURCE)
                                    .streamReadConstraints(
                                            StreamReadConstraints.builder()
                                                    .maxNestingDepth(100)
                                                    .maxNumberLength(1000)
                                                    .maxStringLength(16777216)
                                                    .build())
                                    .build())
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true))
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private static final DefaultBHttpClientConnectionFactory FACTORY =
            new DefaultBHttpClientConnectionFactory(
                    ConnectionConfig.custom()
                            .setMessageConstraints(
                                    MessageConstraints.custom()
                                            .setMaxHeaderCount(64)
                                            .setMaxLineLength(8192)
                                            .build())
                            .build());

    private final AcuityConfig config;
    private final AtomicReference<Socket> socket = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    AcuityHttp(AcuityConfig config) {
        this.config = config;
    }

    /** The same absolute monotonic deadline covers all HTTP retries and Retry-After waits. */
    JsonNode list(AcuityResource resource, String target, long deadline) throws IOException {
        for (int attempt = 1; attempt <= config.maxAttempts; attempt++) {
            Attempt result = execute(resource, target, deadline);
            if (result.status == 200) {
                return result.json;
            }
            if (!canRetry(result.status) || attempt == config.maxAttempts) {
                throw resource.failure("HTTP " + result.status + " (response details withheld)");
            }
            long base = Math.min(5000L, 200L << (attempt - 1));
            long wait =
                    Math.max(result.retryAfter, base + ThreadLocalRandom.current().nextLong(base));
            if (wait >= TimeUnit.NANOSECONDS.toMillis(remaining(resource, deadline))) {
                throw resource.failure("Retry-After or backoff exceeds request deadline");
            }
            try {
                TimeUnit.MILLISECONDS.sleep(wait);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw resource.failure("request canceled");
            }
        }
        throw resource.failure("request attempt limit reached");
    }

    private Attempt execute(AcuityResource resource, String target, long deadline)
            throws IOException {
        Socket raw = new Socket();
        socket.set(raw);
        try (Socket connection = raw) {
            remaining(resource, deadline);
            String host = config.origin.getHost();
            int port = config.origin.getPort() < 0 ? 443 : config.origin.getPort();
            connection.connect(
                    new InetSocketAddress(host, port),
                    timeout(resource, deadline, config.connectTimeout));
            connection.setSoTimeout(timeout(resource, deadline, config.readTimeout));
            SSLSocketFactory ssl = (SSLSocketFactory) SSLSocketFactory.getDefault();
            try (SSLSocket tls = (SSLSocket) ssl.createSocket(connection, host, port, true)) {
                SSLParameters parameters = tls.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                parameters.setProtocols(
                        Arrays.stream(tls.getEnabledProtocols())
                                .filter(v -> "TLSv1.2".equals(v) || "TLSv1.3".equals(v))
                                .toArray(String[]::new));
                tls.setSSLParameters(parameters);
                tls.startHandshake();
                tls.setSoTimeout(timeout(resource, deadline, config.readTimeout));
                return exchange(resource, target, deadline, tls);
            }
        } catch (HttpFailure e) {
            throw e;
        } catch (SSLException e) {
            throw resource.failure("TLS connection or certificate validation failed");
        } catch (HttpException e) {
            throw resource.failure("invalid HTTP response");
        } catch (IOException e) {
            throw resource.failure("HTTPS request did not complete");
        } finally {
            socket.compareAndSet(raw, null);
        }
    }

    private Attempt exchange(AcuityResource resource, String target, long deadline, Socket tls)
            throws IOException, HttpException {
        DefaultBHttpClientConnection connection = FACTORY.createConnection(tls);
        try {
            BasicHttpRequest request = new BasicHttpRequest("GET", target, HttpVersion.HTTP_1_1);
            request.setHeader("Host", config.origin.getRawAuthority());
            request.setHeader("Authorization", config.authorization);
            request.setHeader("Accept", "application/json");
            request.setHeader("Accept-Encoding", "gzip");
            request.setHeader("Connection", "close");
            HttpResponse response =
                    new HttpRequestExecutor()
                            .execute(request, connection, HttpCoreContext.create());
            int status = response.getStatusLine().getStatusCode();
            if (status != 200) {
                return new Attempt(
                        status, null, canRetry(status) ? retryAfter(resource, response) : 0);
            }
            Header[] lengths = response.getHeaders("Content-Length");
            Header[] encodings = response.getHeaders("Transfer-Encoding");
            if (lengths.length > 1
                    || encodings.length > 1
                    || (lengths.length == 1 && encodings.length == 1)) {
                throw safe(resource, "ambiguous HTTP body framing");
            }
            JsonNode data = json(resource, response.getEntity());
            remaining(resource, deadline);
            return new Attempt(status, data, 0);
        } finally {
            // HTTP entities may drain on close. Close the transport without reading the tail.
            try {
                connection.shutdown();
            } catch (IOException ignored) {
                // The raw and TLS sockets are also closed by the owning request.
            }
        }
    }

    private JsonNode json(AcuityResource resource, HttpEntity entity) throws HttpFailure {
        if (entity == null || entity.getContentLength() > config.maxBytes) {
            throw safe(resource, "empty or oversized HTTP body");
        }
        Header header = entity.getContentEncoding();
        String coding = header == null ? "identity" : header.getValue().trim();
        boolean compressed = "gzip".equalsIgnoreCase(coding);
        if (!compressed && !"identity".equalsIgnoreCase(coding)) {
            throw safe(resource, "unsupported HTTP content encoding");
        }
        try {
            InputStream source = new LimitedStream(entity.getContent(), config.maxBytes);
            try (InputStream decoded = compressed ? new GZIPInputStream(source) : source) {
                return readRecords(resource, new LimitedStream(decoded, config.maxBytes));
            }
        } catch (HttpFailure e) {
            throw e;
        } catch (IOException | RuntimeException e) {
            throw safe(resource, "invalid, incomplete or oversized JSON body");
        }
    }

    /**
     * Applies the row limit before building a second object beyond it. A payload must contain one
     * complete JSON array of objects, including when the wire entity is chunked.
     */
    private JsonNode readRecords(AcuityResource resource, InputStream input) throws IOException {
        int max = resource.dated ? config.dateLimit : config.maxRows;
        try (JsonParser parser = JSON.getFactory().createParser(input)) {
            if (parser.nextToken() != JsonToken.START_ARRAY) {
                throw safe(resource, "expected a JSON list response");
            }
            ArrayNode records = JSON.createArrayNode();
            while (parser.nextToken() != JsonToken.END_ARRAY) {
                if (parser.currentToken() != JsonToken.START_OBJECT) {
                    throw safe(resource, "expected a response array of JSON objects");
                }
                if (records.size() == max) {
                    throw safe(
                            resource,
                            resource.dated
                                    ? "response exceeded the requested max_date_results"
                                    : "list exceeds max_records_per_response");
                }
                JsonNode record = JSON.readTree(parser);
                records.add(record);
            }
            if (parser.nextToken() != null) {
                throw safe(resource, "invalid, incomplete or oversized JSON body");
            }
            return records;
        }
    }

    private static boolean canRetry(int status) {
        return status == 429 || status == 500 || status == 502 || status == 503 || status == 504;
    }

    private static long retryAfter(AcuityResource resource, HttpResponse response)
            throws HttpFailure {
        Header[] headers = response.getHeaders("Retry-After");
        if (headers.length > 1) {
            throw safe(resource, "ambiguous Retry-After");
        }
        if (headers.length == 0) {
            return 0;
        }
        String value = headers[0].getValue().trim();
        if (!value.isEmpty() && value.chars().allMatch(c -> c >= '0' && c <= '9')) {
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

    private long remaining(AcuityResource resource, long deadline) throws HttpFailure {
        if (closed.get() || Thread.currentThread().isInterrupted()) {
            throw safe(resource, "request canceled");
        }
        long time = deadline - System.nanoTime();
        if (time <= 0) {
            throw safe(resource, "request deadline exceeded");
        }
        return time;
    }

    private int timeout(AcuityResource resource, long deadline, int limit) throws HttpFailure {
        return (int)
                Math.max(
                        1,
                        Math.min(
                                limit,
                                TimeUnit.NANOSECONDS.toMillis(remaining(resource, deadline))));
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            Socket active = socket.getAndSet(null);
            if (active != null) {
                try {
                    active.close();
                } catch (IOException ignored) {
                    // The request's resource scope also closes the socket.
                }
            }
        }
    }

    private static HttpFailure safe(AcuityResource resource, String message) {
        return new HttpFailure("Acuity Scheduling source " + resource.name + ": " + message);
    }

    /** Distinguishes diagnostic-safe parser or policy errors from raw transport exceptions. */
    private static final class HttpFailure extends IOException {
        private static final long serialVersionUID = 1L;

        HttpFailure(String message) {
            super(message);
        }
    }

    /** Captures successful JSON or safe retry metadata without exposing error bodies. */
    private static final class Attempt {
        final int status;
        final JsonNode json;
        final long retryAfter;

        Attempt(int status, JsonNode json, long retryAfter) {
            this.status = status;
            this.json = json;
            this.retryAfter = retryAfter;
        }
    }

    /** Enforces a byte budget while shielding the socket from draining on stream close. */
    private static final class LimitedStream extends FilterInputStream {
        private int remaining;

        LimitedStream(InputStream source, int limit) {
            super(source);
            remaining = limit;
        }

        @Override
        public int read() throws IOException {
            int value = in.read();
            if (value >= 0) {
                count(1);
            }
            return value;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            int read = in.read(bytes, offset, Math.min(length, remaining + 1));
            if (read > 0) {
                count(read);
            }
            return read;
        }

        @Override
        public void close() {
            // The enclosing socket closes the raw stream without waiting on a response tail.
        }

        private void count(int bytes) throws IOException {
            remaining -= bytes;
            if (remaining < 0) {
                throw new IOException("response exceeds the configured byte limit");
            }
        }
    }
}
