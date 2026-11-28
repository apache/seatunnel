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

package org.apache.seatunnel.connectors.seatunnel.apify;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;

import org.apache.seatunnel.connectors.seatunnel.apify.ApifyDatasetConfig.Resource;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.client.utils.DateUtils;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.MessageConstraints;
import org.apache.http.impl.DefaultBHttpClientConnection;
import org.apache.http.impl.DefaultBHttpClientConnectionFactory;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpRequestExecutor;

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
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPInputStream;

/**
 * Executes one bounded bearer-authenticated page request at a time without exposing provider URLs,
 * payloads, or credentials in source failures. The reader can close the active socket.
 */
final class ApifyHttp {
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                    .disable(JsonParser.Feature.AUTO_CLOSE_SOURCE)
                    .enable(
                            DeserializationFeature.FAIL_ON_TRAILING_TOKENS,
                            DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    private static final DefaultBHttpClientConnectionFactory CONNECTIONS =
            new DefaultBHttpClientConnectionFactory(
                    ConnectionConfig.custom()
                            .setMessageConstraints(
                                    MessageConstraints.custom()
                                            .setMaxHeaderCount(64)
                                            .setMaxLineLength(16384)
                                            .build())
                            .build());

    static {
        JSON.getFactory()
                .setStreamReadConstraints(
                        StreamReadConstraints.builder()
                                .maxNestingDepth(64)
                                .maxStringLength(33554432)
                                .maxNumberLength(1024)
                                .build());
    }

    private final ApifyDatasetConfig config;
    private final AtomicReference<Socket> current = new AtomicReference<>();
    private volatile boolean closed;

    ApifyHttp(ApifyDatasetConfig config) {
        this.config = config;
    }

    /**
     * Retries temporary status codes inside the original page deadline. A retry reissues the exact
     * same idempotent GET and preserves the computed scan boundary.
     */
    ApifyPages.Page fetch(Resource resource, long offset, int limit, long end, long deadline)
            throws IOException {
        for (int attempt = 1; ; attempt++) {
            try {
                return exchange(resource, offset, limit, end, deadline);
            } catch (Retryable e) {
                if (attempt == config.maxAttempts) {
                    throw failure(resource, offset, "retry limit reached after HTTP " + e.status);
                }
                long baseline = 500L << Math.min(attempt - 1, 5);
                long delay =
                        Math.max(
                                e.retryAfter,
                                ThreadLocalRandom.current().nextLong(baseline, 2 * baseline));
                await(resource, offset, deadline, delay);
            }
        }
    }

    private ApifyPages.Page exchange(
            Resource resource, long offset, int limit, long end, long deadline) throws IOException {
        Socket raw = new Socket();
        current.set(raw);
        try {
            if (closed) {
                throw failure(resource, offset, "request canceled");
            }
            int port = config.origin.getPort() < 0 ? 443 : config.origin.getPort();
            String host = config.origin.getHost();
            raw.connect(
                    new InetSocketAddress(host, port), timeout(deadline, config.connectTimeout));
            raw.setSoTimeout(timeout(deadline, config.readTimeout));
            SSLSocketFactory sockets = (SSLSocketFactory) SSLSocketFactory.getDefault();
            try (SSLSocket ssl = (SSLSocket) sockets.createSocket(raw, host, port, true)) {
                SSLParameters parameters = ssl.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                ssl.setSSLParameters(parameters);
                String[] versions =
                        Arrays.stream(ssl.getEnabledProtocols())
                                .filter(s -> "TLSv1.2".equals(s) || "TLSv1.3".equals(s))
                                .toArray(String[]::new);
                ssl.setEnabledProtocols(versions);
                ssl.startHandshake();
                ssl.setSoTimeout(timeout(deadline, config.readTimeout));
                DefaultBHttpClientConnection connection = CONNECTIONS.createConnection(ssl);
                BasicHttpRequest request =
                        new BasicHttpRequest("GET", config.target(resource, offset, limit));
                request.setHeader("Host", config.origin.getRawAuthority());
                request.setHeader("Authorization", "Bearer " + config.token);
                request.setHeader("Connection", "close");
                request.setHeader("Accept", "application/json");
                request.setHeader("Accept-Encoding", "gzip");
                request.setHeader("User-Agent", "Apache-SeaTunnel-ApifyDataset");
                HttpResponse response =
                        new HttpRequestExecutor()
                                .execute(request, connection, new BasicHttpContext());
                int status = response.getStatusLine().getStatusCode();
                if (status == 429
                        || status == 500
                        || status == 502
                        || status == 503
                        || status == 504) {
                    throw new Retryable(status, retryAfter(response));
                }
                if (status != 200) {
                    throw failure(resource, offset, "HTTP " + status);
                }
                try {
                    JsonNode root = decode(response);
                    ApifyPages.Page page =
                            ApifyPages.parse(
                                    resource, response, root, config.datasetId, offset, limit, end);
                    if (System.nanoTime() - deadline >= 0) {
                        throw new PageFailure("request deadline exceeded");
                    }
                    return page;
                } catch (IOException safe) {
                    // The decoder and envelope parser emit fixed reason strings with no provider
                    // data.
                    throw failure(
                            resource,
                            offset,
                            System.nanoTime() - deadline >= 0
                                    ? "request deadline exceeded"
                                    : safe.getMessage());
                }
            }
        } catch (Retryable | PageFailure safe) {
            throw safe;
        } catch (IOException | HttpException | IllegalArgumentException ignored) {
            String message =
                    System.nanoTime() - deadline >= 0
                            ? "request deadline exceeded"
                            : (closed ? "request canceled" : "HTTPS or TLS exchange failed");
            throw failure(resource, offset, message);
        } finally {
            current.compareAndSet(raw, null);
            closeQuietly(raw);
        }
    }

    private JsonNode decode(HttpResponse response) throws IOException {
        Header[] lengths = response.getHeaders("Content-Length");
        Header[] transfer = response.getHeaders("Transfer-Encoding");
        Header[] encoding = response.getHeaders("Content-Encoding");
        HttpEntity entity = response.getEntity();
        if (entity == null
                || lengths.length > 1
                || transfer.length > 1
                || encoding.length > 1
                || (lengths.length != 0 && transfer.length != 0)
                || (transfer.length != 0 && !"chunked".equalsIgnoreCase(transfer[0].getValue()))) {
            throw new PageFailure("invalid response framing");
        }
        if (entity.getContentLength() > config.maxResponse) {
            throw new PageFailure("response exceeds max_response_bytes");
        }
        String contentEncoding = encoding.length == 0 ? "identity" : encoding[0].getValue().trim();
        if (!"identity".equalsIgnoreCase(contentEncoding)
                && !"gzip".equalsIgnoreCase(contentEncoding)) {
            throw new PageFailure("unsupported response encoding");
        }
        try {
            InputStream wire = new BoundedBody(entity.getContent(), config.maxResponse);
            if ("identity".equalsIgnoreCase(contentEncoding)) {
                return JSON.readTree(wire);
            }
            try (GZIPInputStream uncompressed = new GZIPInputStream(wire)) {
                JsonNode root = JSON.readTree(new BoundedBody(uncompressed, config.maxResponse));
                if (wire.read() >= 0) {
                    throw new PageFailure("unexpected trailing compressed response bytes");
                }
                return root;
            }
        } catch (BodyLimit ignored) {
            throw new PageFailure("response exceeds max_response_bytes");
        } catch (PageFailure safe) {
            throw safe;
        } catch (IOException ignored) {
            throw new PageFailure("invalid, incomplete or unreadable JSON response");
        }
    }

    private static long retryAfter(HttpResponse response) {
        Header retryAfter = response.getFirstHeader("Retry-After");
        if (retryAfter == null) {
            return 0;
        }
        String value = retryAfter.getValue().trim();
        try {
            return TimeUnit.SECONDS.toMillis(Math.max(0, Long.parseLong(value)));
        } catch (NumberFormatException ignored) {
            Date time = DateUtils.parseDate(value);
            return time == null ? 0 : Math.max(0, time.getTime() - System.currentTimeMillis());
        }
    }

    private static int timeout(long deadline, int maxMillis) throws IOException {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
            throw new IOException("deadline");
        }
        return (int) Math.min(maxMillis, Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)));
    }

    private void await(Resource resource, long offset, long deadline, long delay)
            throws IOException {
        long wait = TimeUnit.MILLISECONDS.toNanos(delay);
        if (deadline - System.nanoTime() <= wait) {
            throw failure(resource, offset, "retry delay exceeds request deadline");
        }
        try {
            TimeUnit.NANOSECONDS.sleep(wait);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw failure(resource, offset, "request canceled");
        }
    }

    /**
     * Releases the active connection to unblock TLS and body reads during job cancellation. The
     * reader cancels the retrieval future separately to interrupt its retry wait.
     */
    void close() {
        closed = true;
        closeQuietly(current.getAndSet(null));
    }

    private static void closeQuietly(Socket socket) {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // The request no longer owns a socket after any close failure.
            }
        }
    }

    static PageFailure failure(Resource resource, long offset, String reason) {
        return new PageFailure(
                "Apify Dataset source " + resource.key() + " at offset " + offset + ": " + reason);
    }

    /**
     * Stores a recognized temporary status and any provider retry delay without retaining its body.
     * The caller enforces the maximum attempts and wall clock budget.
     */
    private static final class Retryable extends IOException {
        private final int status;
        private final long retryAfter;

        private Retryable(int status, long retryAfter) {
            this.status = status;
            this.retryAfter = retryAfter;
        }
    }

    /**
     * Represents a source-generated failure safe to report without logging remote response content
     * or exposing the authorization header in the exception chain.
     */
    static final class PageFailure extends IOException {
        private PageFailure(String message) {
            super(message);
        }
    }

    /**
     * Indicates an encoded or decoded body exceeded the configured payload byte limit. The
     * transport converts it to the same sanitized error for either layer.
     */
    private static final class BodyLimit extends IOException {}

    /**
     * Streams the response with a fixed byte budget and permits a probe at the boundary. Remaining
     * budgets are tracked separately for the encoded and decoded streams.
     */
    private static final class BoundedBody extends FilterInputStream {
        private int remaining;

        private BoundedBody(InputStream input, int remaining) {
            super(input);
            this.remaining = remaining;
        }

        @Override
        public int read() throws IOException {
            int result = in.read();
            if (result >= 0 && remaining-- == 0) {
                throw new BodyLimit();
            }
            return result;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            int count = in.read(buffer, offset, Math.min(length, remaining + 1));
            remaining -= Math.max(0, count);
            if (remaining < 0) {
                throw new BodyLimit();
            }
            return count;
        }

        @Override
        public void close() {
            // The transport owns and closes the socket directly without draining an HTTP entity.
        }
    }
}
