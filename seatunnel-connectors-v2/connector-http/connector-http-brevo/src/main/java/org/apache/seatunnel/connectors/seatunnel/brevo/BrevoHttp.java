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

package org.apache.seatunnel.connectors.seatunnel.brevo;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.connectors.seatunnel.brevo.BrevoJson.Failure;

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

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
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
import java.util.zip.GZIPInputStream;

/**
 * Read-only HTTPS transport with one bounded operation at a time. HTTP framing is handled without
 * installing request, URI, or wire loggers.
 */
final class BrevoHttp implements AutoCloseable {
    private static final DefaultBHttpClientConnectionFactory CONNECTIONS =
            new DefaultBHttpClientConnectionFactory(
                    ConnectionConfig.custom()
                            .setMessageConstraints(
                                    MessageConstraints.custom()
                                            .setMaxHeaderCount(64)
                                            .setMaxLineLength(8192)
                                            .build())
                            .build());

    private final BrevoConfig config;
    private final ExecutorService worker =
            Executors.newSingleThreadExecutor(
                    job -> {
                        Thread thread = new Thread(job, "seatunnel-brevo-https");
                        thread.setDaemon(true);
                        return thread;
                    });
    private final AtomicReference<Socket> connection = new AtomicReference<>();
    private final AtomicReference<Future<Reply>> inFlight = new AtomicReference<>();
    private final Object waitLock = new Object();
    private volatile boolean closed;
    private SSLSocketFactory sockets;

    BrevoHttp(BrevoConfig config) {
        this.config = config;
    }

    /**
     * Fetches a fixed-host export with GET and a bounded total deadline.
     *
     * <p>The bounded retry budget applies to documented throttling and temporary server failures.
     */
    JsonNode fetch(String target) throws IOException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.deadlineMs);
        for (int attempt = 1; ; attempt++) {
            if (closed || Thread.currentThread().isInterrupted()) {
                throw BrevoJson.failure("request canceled");
            }
            Reply reply = exchange(target, deadline);
            if (reply.body != null) {
                if (deadline - System.nanoTime() <= 0) {
                    throw BrevoJson.failure("request deadline exceeded");
                }
                return reply.body;
            }
            boolean retry =
                    reply.status == 408
                            || reply.status == 429
                            || (reply.status >= 500 && reply.status < 600);
            if (!retry || attempt == config.maxAttempts) {
                throw BrevoJson.failure(
                        (retry ? "retry limit reached after " : "") + "HTTP " + reply.status);
            }
            long baseMs = 200L << Math.min(attempt - 1, 5);
            long delay =
                    Math.max(
                            reply.retryAfter,
                            ThreadLocalRandom.current().nextLong(baseMs, baseMs * 2));
            await(deadline, delay);
        }
    }

    private Reply exchange(String target, long deadline) throws IOException {
        Future<Reply> future;
        try {
            future = worker.submit(() -> request(target, deadline));
        } catch (RejectedExecutionException e) {
            throw BrevoJson.failure("request canceled");
        }
        inFlight.set(future);
        try {
            if (closed) {
                throw BrevoJson.failure("request canceled");
            }
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                throw BrevoJson.failure("request deadline exceeded");
            }
            return future.get(remaining, TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            throw BrevoJson.failure("request deadline exceeded");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw BrevoJson.failure("request canceled");
        } catch (CancellationException e) {
            throw BrevoJson.failure("request canceled");
        } catch (ExecutionException e) {
            if (e.getCause() instanceof Failure) {
                throw (Failure) e.getCause();
            }
            throw BrevoJson.failure("HTTPS or TLS exchange failed");
        } finally {
            inFlight.compareAndSet(future, null);
            if (!future.isDone()) {
                future.cancel(true);
                closeSocket(connection.getAndSet(null));
            }
        }
    }

    private Reply request(String target, long deadline) throws IOException {
        Socket raw = new Socket();
        connection.set(raw);
        try {
            if (closed || Thread.currentThread().isInterrupted()) {
                throw BrevoJson.failure("request canceled");
            }
            raw.connect(
                    new InetSocketAddress(BrevoConfig.HOST, 443),
                    timeout(deadline, config.connectTimeout));
            raw.setSoTimeout(timeout(deadline, config.readTimeout));
            SSLSocketFactory factory = socketFactory();
            try (SSLSocket tls =
                    (SSLSocket) factory.createSocket(raw, BrevoConfig.HOST, 443, true)) {
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
                BasicHttpRequest request = new BasicHttpRequest("GET", target);
                request.setHeader("Host", BrevoConfig.HOST);
                request.setHeader("api-key", config.token);
                request.setHeader("Accept", "application/json");
                request.setHeader("Accept-Encoding", "gzip");
                request.setHeader("Connection", "close");
                request.setHeader("User-Agent", "Apache-SeaTunnel-Brevo");
                HttpResponse response =
                        new HttpRequestExecutor()
                                .execute(request, protocol, new BasicHttpContext());
                int status = response.getStatusLine().getStatusCode();
                return status == 200
                        ? new Reply(BrevoJson.parse(readBody(response, config.maxBytes)), status, 0)
                        : new Reply(null, status, retryAfter(response, status));
            }
        } catch (SafeFailure e) {
            throw BrevoJson.failure(e.getMessage());
        } catch (Failure e) {
            throw e;
        } catch (IOException | HttpException | IllegalArgumentException ignored) {
            if (deadline - System.nanoTime() <= 0) {
                throw BrevoJson.failure("request deadline exceeded");
            }
            throw BrevoJson.failure(closed ? "request canceled" : "HTTPS or TLS exchange failed");
        } finally {
            connection.compareAndSet(raw, null);
            closeSocket(raw);
        }
    }

    /**
     * Loads an optional engine-local trust anchor with a finite read size.
     *
     * <p>This factory is confined to this connector instance. The request always verifies the Brevo
     * hostname and uses the default JVM trust anchors when no file is configured.
     */
    private SSLSocketFactory socketFactory() throws IOException {
        if (sockets != null) {
            return sockets;
        }
        if (config.trustCertificate == null) {
            sockets = (SSLSocketFactory) SSLSocketFactory.getDefault();
            return sockets;
        }
        int limit = 1024 * 1024;
        try {
            Path path = Path.of(config.trustCertificate);
            if (!Files.isRegularFile(path) || Files.size(path) > limit) {
                throw new SafeFailure("tls_trust_certificate must be a regular file up to 1 MiB");
            }
            byte[] bytes;
            try (InputStream input = Files.newInputStream(path)) {
                bytes = input.readNBytes(limit + 1);
            }
            if (bytes.length == 0 || bytes.length > limit) {
                throw new SafeFailure(
                        "tls_trust_certificate must contain a PEM or DER certificate");
            }
            KeyStore trust = KeyStore.getInstance(KeyStore.getDefaultType());
            trust.load(null, null);
            int number = 0;
            for (Certificate entry :
                    CertificateFactory.getInstance("X.509")
                            .generateCertificates(new ByteArrayInputStream(bytes))) {
                trust.setCertificateEntry("brevo-" + number++, entry);
            }
            if (number == 0) {
                throw new SafeFailure(
                        "tls_trust_certificate must contain a PEM or DER certificate");
            }
            TrustManagerFactory managers =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            managers.init(trust);
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, managers.getTrustManagers(), null);
            sockets = context.getSocketFactory();
            return sockets;
        } catch (SafeFailure known) {
            throw known;
        } catch (IOException | GeneralSecurityException | IllegalArgumentException ignored) {
            throw new SafeFailure("could not read tls_trust_certificate");
        }
    }

    private void await(long deadline, long delayMs) throws IOException {
        long nanos = TimeUnit.MILLISECONDS.toNanos(delayMs);
        long now = System.nanoTime();
        if (deadline - now <= nanos) {
            throw BrevoJson.failure(
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
                    throw BrevoJson.failure("request canceled");
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

        Reply(JsonNode body, int status, long retryAfter) {
            this.body = body;
            this.status = status;
            this.retryAfter = retryAfter;
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

    /**
     * Uses Brevo's relative reset delay for HTTP 429 and the standard header when present.
     *
     * <p>The request-level deadline still limits the combined backoff.
     */
    private static long retryAfter(HttpResponse response, int status) {
        long providerDelay = 0;
        if (status == 429) {
            Long seconds = unsigned(response.getFirstHeader("x-sib-ratelimit-reset"));
            if (seconds != null) {
                providerDelay = TimeUnit.SECONDS.toMillis(seconds);
            }
        }
        Header header = response.getFirstHeader("Retry-After");
        if (header == null) {
            return providerDelay;
        }
        Long seconds = unsigned(header);
        if (seconds != null) {
            return Math.max(providerDelay, TimeUnit.SECONDS.toMillis(seconds));
        }
        Date date = DateUtils.parseDate(header.getValue().trim());
        return date == null
                ? providerDelay
                : Math.max(providerDelay, Math.max(0, date.getTime() - System.currentTimeMillis()));
    }

    private static Long unsigned(Header header) {
        if (header == null) {
            return null;
        }
        String value = header.getValue().trim();
        if (value.isEmpty() || !value.chars().allMatch(ch -> ch >= '0' && ch <= '9')) {
            return null;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException ignored) {
            return Long.MAX_VALUE;
        }
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
