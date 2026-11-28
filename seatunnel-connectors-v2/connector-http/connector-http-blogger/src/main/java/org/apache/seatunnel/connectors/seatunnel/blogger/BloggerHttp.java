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

package org.apache.seatunnel.connectors.seatunnel.blogger;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.connectors.seatunnel.blogger.BloggerJson.Failure;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.utils.DateUtils;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.MessageConstraints;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.DefaultBHttpClientConnection;
import org.apache.http.impl.DefaultBHttpClientConnectionFactory;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
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
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Read-only HTTPS transport with one bounded operation at a time. HTTP framing is handled without
 * installing request, URI, or wire loggers.
 */
final class BloggerHttp implements AutoCloseable {
    private static final DefaultBHttpClientConnectionFactory CONNECTIONS =
            new DefaultBHttpClientConnectionFactory(
                    ConnectionConfig.custom()
                            .setMessageConstraints(
                                    MessageConstraints.custom()
                                            .setMaxHeaderCount(64)
                                            .setMaxLineLength(8192)
                                            .build())
                            .build());

    private final BloggerConfig config;
    private static final int OAUTH_BYTES = 65536;
    private static final int ERROR_BYTES = 8192;
    private BloggerJson.Access access;
    private long refreshAt;

    private final ExecutorService worker =
            Executors.newSingleThreadExecutor(
                    job -> {
                        Thread thread = new Thread(job, "seatunnel-blogger-https");
                        thread.setDaemon(true);
                        return thread;
                    });
    private final AtomicReference<Socket> connection = new AtomicReference<>();
    private final AtomicReference<Future<Reply>> inFlight = new AtomicReference<>();
    private final Object waitLock = new Object();
    private volatile boolean closed;

    BloggerHttp(BloggerConfig config) {
        this.config = config;
    }

    /**
     * Fetches one API page within a deadline shared with token renewal and all retries.
     *
     * <p>One rejected bearer can be replaced during a page read. A second 401 is permanent.
     */
    JsonNode fetch(String target) throws IOException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.deadlineMs);
        boolean renewedAfterUnauthorized = false;
        for (int attempt = 1; ; attempt++) {
            if (access == null || System.nanoTime() - refreshAt >= 0) {
                refresh(deadline);
            }
            Reply reply = exchange(false, target, access.token, deadline);
            if (reply.body != null) {
                return reply.body;
            }
            if (reply.status == 401 && !renewedAfterUnauthorized && attempt < config.maxAttempts) {
                renewedAfterUnauthorized = true;
                access = null;
            } else {
                retry(reply, attempt, deadline, "Blogger API");
            }
        }
    }

    private void refresh(long deadline) throws IOException {
        for (int attempt = 1; ; attempt++) {
            long started = System.nanoTime();
            Reply reply = exchange(true, "/token", null, deadline);
            if (reply.body != null) {
                access = BloggerJson.access(reply.body);
                long seconds = access.lifetimeSeconds;
                long safety = Math.min(60, seconds / 10);
                refreshAt = started + TimeUnit.SECONDS.toNanos(seconds - safety);
                return;
            }
            retry(reply, attempt, deadline, "OAuth refresh");
        }
    }

    private void retry(Reply reply, int attempt, long deadline, String operation)
            throws IOException {
        boolean transientStatus =
                reply.status == 408
                        || reply.status == 429
                        || reply.status >= 500 && reply.status < 600
                        || reply.throttled;
        if (!transientStatus || attempt >= config.maxAttempts) {
            throw BloggerJson.failure(
                    operation
                            + (transientStatus ? " retry limit reached after " : " returned ")
                            + "HTTP "
                            + reply.status);
        }
        long base = 200L << Math.min(attempt - 1, 5);
        await(
                deadline,
                Math.max(reply.retryAfter, ThreadLocalRandom.current().nextLong(base, base * 2)));
    }

    private Reply exchange(boolean oauth, String target, String bearer, long deadline)
            throws IOException {
        if (closed || Thread.currentThread().isInterrupted()) {
            throw BloggerJson.failure("request canceled");
        }
        Future<Reply> future;
        try {
            future = worker.submit(() -> request(oauth, target, bearer, deadline));
        } catch (RejectedExecutionException e) {
            throw BloggerJson.failure("request canceled");
        }
        inFlight.set(future);
        try {
            if (closed) {
                throw BloggerJson.failure("request canceled");
            }
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                throw BloggerJson.failure("request deadline exceeded");
            }
            return future.get(remaining, TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            throw BloggerJson.failure("request deadline exceeded");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw BloggerJson.failure("request canceled");
        } catch (CancellationException e) {
            throw BloggerJson.failure("request canceled");
        } catch (ExecutionException e) {
            if (e.getCause() instanceof Failure) {
                throw (Failure) e.getCause();
            }
            throw BloggerJson.failure("HTTPS or TLS exchange failed");
        } finally {
            inFlight.compareAndSet(future, null);
            if (!future.isDone()) {
                future.cancel(true);
                closeSocket(connection.getAndSet(null));
            }
        }
    }

    private Reply request(boolean oauth, String target, String bearer, long deadline)
            throws IOException {
        String host = oauth ? BloggerConfig.OAUTH_HOST : BloggerConfig.API_HOST;
        Socket raw = new Socket();
        connection.set(raw);
        try {
            if (closed || Thread.currentThread().isInterrupted()) {
                throw BloggerJson.failure("request canceled");
            }
            raw.connect(new InetSocketAddress(host, 443), timeout(deadline, config.connectTimeout));
            raw.setSoTimeout(timeout(deadline, config.readTimeout));
            SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory.getDefault();
            try (SSLSocket tls = (SSLSocket) factory.createSocket(raw, host, 443, true)) {
                SSLParameters parameters = tls.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                tls.setSSLParameters(parameters);
                tls.setEnabledProtocols(
                        Arrays.stream(tls.getEnabledProtocols())
                                .filter(value -> "TLSv1.2".equals(value) || "TLSv1.3".equals(value))
                                .toArray(String[]::new));
                tls.startHandshake();
                tls.setSoTimeout(timeout(deadline, config.readTimeout));
                DefaultBHttpClientConnection protocol = CONNECTIONS.createConnection(tls);
                HttpRequest request;
                if (oauth) {
                    byte[] form = config.refreshBody();
                    BasicHttpEntityEnclosingRequest grant =
                            new BasicHttpEntityEnclosingRequest("POST", "/token");
                    grant.setEntity(new ByteArrayEntity(form));
                    grant.setHeader("Content-Length", Integer.toString(form.length));
                    grant.setHeader("Content-Type", "application/x-www-form-urlencoded");
                    request = grant;
                } else {
                    request = new BasicHttpRequest("GET", target);
                    request.setHeader("Authorization", "Bearer " + bearer);
                }
                request.setHeader("Host", host);
                request.setHeader("Accept", "application/json");
                request.setHeader("Accept-Encoding", "gzip");
                request.setHeader("Connection", "close");
                request.setHeader("User-Agent", "Apache-SeaTunnel-Blogger");
                HttpResponse response =
                        new HttpRequestExecutor()
                                .execute(request, protocol, new BasicHttpContext());
                int status = response.getStatusLine().getStatusCode();
                if (status == 200) {
                    int limit = oauth ? Math.min(config.maxBytes, OAUTH_BYTES) : config.maxBytes;
                    return new Reply(
                            BloggerJson.parse(readBody(response, limit)), status, 0, false);
                }
                return new Reply(
                        null,
                        status,
                        retryAfter(response),
                        !oauth && status == 403 && throttled(response));
            }
        } catch (SafeFailure e) {
            throw BloggerJson.failure(e.getMessage());
        } catch (Failure e) {
            throw e;
        } catch (IOException | HttpException | IllegalArgumentException ignored) {
            if (deadline - System.nanoTime() <= 0) {
                throw BloggerJson.failure("request deadline exceeded");
            }
            throw BloggerJson.failure(closed ? "request canceled" : "HTTPS or TLS exchange failed");
        } finally {
            connection.compareAndSet(raw, null);
            closeSocket(raw);
        }
    }

    private static boolean throttled(HttpResponse response) {
        try {
            return BloggerJson.throttled(BloggerJson.parse(readBody(response, ERROR_BYTES)));
        } catch (IOException ignored) {
            // A payload that cannot prove transient throttling remains a permanent HTTP 403.
            return false;
        }
    }

    private void await(long deadline, long delayMs) throws IOException {
        long nanos = TimeUnit.MILLISECONDS.toNanos(delayMs);
        long now = System.nanoTime();
        if (deadline - now <= nanos) {
            throw BloggerJson.failure(
                    delayMs == 0
                            ? "request deadline exceeded"
                            : "retry delay exceeds request deadline");
        }
        long wake = now + nanos;
        synchronized (waitLock) {
            while (!closed && wake - System.nanoTime() > 0) {
                try {
                    TimeUnit.NANOSECONDS.timedWait(waitLock, Math.max(1, wake - System.nanoTime()));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw BloggerJson.failure("request canceled");
                }
            }
        }
    }

    private static int timeout(long deadline, int cap) throws IOException {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
            throw new SafeFailure("request deadline exceeded");
        }
        return (int) Math.min(cap, Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)));
    }

    /**
     * Cancels a pending exchange and retry wait without acquiring reader progress locks.
     *
     * <p>Closing the active socket also interrupts a reader blocked in TLS or the response body.
     */
    @Override
    public void close() {
        closed = true;
        Future<Reply> future = inFlight.getAndSet(null);
        if (future != null) {
            future.cancel(true);
        }
        closeSocket(connection.getAndSet(null));
        worker.shutdownNow();
        synchronized (waitLock) {
            waitLock.notifyAll();
        }
    }

    private static void closeSocket(Socket raw) {
        if (raw != null) {
            try {
                raw.close();
            } catch (IOException ignored) {
                // The safe request failure or cancellation is reported by the reader.
            }
        }
    }

    /**
     * Carries a validated success body or a status code and sanitized retry delay.
     *
     * <p>The caller can evaluate retry policy without decoding or logging a service error body.
     */
    private static final class Reply {
        final JsonNode body;
        final int status;
        final long retryAfter;
        final boolean throttled;

        Reply(JsonNode body, int status, long retryAfter, boolean throttled) {
            this.body = body;
            this.status = status;
            this.retryAfter = retryAfter;
            this.throttled = throttled;
        }
    }

    /**
     * A bounded-I/O or framing error with a fixed message suitable for the job log.
     *
     * <p>Only locally defined bounds and framing messages may be carried by this error.
     */
    private static final class SafeFailure extends IOException {
        SafeFailure(String reason) {
            super(reason);
        }
    }

    private static byte[] readBody(HttpResponse response, int limit) throws IOException {
        HttpEntity entity = response.getEntity();
        Header[] lengths = response.getHeaders("Content-Length");
        Header[] transfer = response.getHeaders("Transfer-Encoding");
        Header[] encoding = response.getHeaders("Content-Encoding");
        if (entity == null
                || lengths.length > 1
                || transfer.length > 1
                || encoding.length > 1
                || response.containsHeader("Content-Range")
                || (transfer.length > 0
                        && (lengths.length > 0
                                || !"chunked".equalsIgnoreCase(transfer[0].getValue())))) {
            throw new SafeFailure("malformed or partial response framing");
        }
        if (entity.getContentLength() > limit) {
            throw new SafeFailure("response exceeds max_response_bytes");
        }
        InputStream wire = new Bounded(entity.getContent(), limit);
        String coding = encoding.length == 0 ? "identity" : encoding[0].getValue().trim();
        if ("identity".equalsIgnoreCase(coding)) {
            return wire.readAllBytes();
        }
        if (!"gzip".equalsIgnoreCase(coding)) {
            throw new SafeFailure("unsupported Content-Encoding");
        }
        try (InputStream gzip = new GZIPInputStream(wire)) {
            byte[] result = new Bounded(gzip, limit).readAllBytes();
            if (wire.read() != -1) {
                throw new SafeFailure("unexpected trailing compressed response bytes");
            }
            return result;
        }
    }

    private static long retryAfter(HttpResponse response) {
        Header header = response.getFirstHeader("Retry-After");
        if (header == null) {
            return 0;
        }
        String value = header.getValue().trim();
        if (!value.isEmpty() && value.chars().allMatch(ch -> ch >= '0' && ch <= '9')) {
            try {
                return TimeUnit.SECONDS.toMillis(Long.parseLong(value));
            } catch (NumberFormatException ignored) {
                return Long.MAX_VALUE;
            }
        }
        Date date = DateUtils.parseDate(value);
        return date == null ? 0 : Math.max(0, date.getTime() - System.currentTimeMillis());
    }

    /**
     * Limits wire or decoded bytes without draining unfinished HTTP response entities. Underlying
     * streams are owned by the socket lifecycle.
     */
    private static final class Bounded extends FilterInputStream {
        private int remaining;

        Bounded(InputStream input, int limit) {
            super(input);
            remaining = limit;
        }

        @Override
        public int read() throws IOException {
            int value = in.read();
            if (value >= 0 && remaining-- == 0) {
                throw new SafeFailure("response exceeds max_response_bytes");
            }
            return value;
        }

        @Override
        public int read(byte[] values, int offset, int count) throws IOException {
            int size = in.read(values, offset, Math.min(count, remaining + 1));
            if (size > remaining) {
                throw new SafeFailure("response exceeds max_response_bytes");
            }
            if (size > 0) {
                remaining -= size;
            }
            return size;
        }

        @Override
        public void close() {}
    }
}
