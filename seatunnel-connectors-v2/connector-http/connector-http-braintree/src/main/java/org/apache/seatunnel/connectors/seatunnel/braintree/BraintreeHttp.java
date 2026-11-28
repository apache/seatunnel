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
package org.apache.seatunnel.connectors.seatunnel.braintree;

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
import javax.xml.stream.XMLStreamException;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;

/**
 * Quiet, deadline-bounded HTTPS for read-only Braintree XML operations.
 *
 * <p>Credentials are only sent to the configured production or sandbox host. The source closes a
 * blocked socket on deadline or cancellation and never follows redirects.
 */
final class BraintreeHttp implements AutoCloseable {
    private static final MessageConstraints HEADERS =
            MessageConstraints.custom().setMaxLineLength(16384).setMaxHeaderCount(64).build();
    private static final Set<Integer> RETRYABLE = Set.of(408, 429, 500, 502, 503, 504);

    private final BraintreeConfig config;
    private final SSLSocketFactory tls;
    private final ScheduledThreadPoolExecutor watchdog;
    private volatile boolean closed;
    private Socket active;

    BraintreeHttp(BraintreeConfig config) {
        this.config = config;
        tls = config.socketFactory();
        watchdog =
                new ScheduledThreadPoolExecutor(
                        1,
                        task -> {
                            Thread thread = new Thread(task, "braintree-https-deadline");
                            thread.setDaemon(true);
                            return thread;
                        });
        watchdog.setRemoveOnCancelPolicy(true);
    }

    long deadline() {
        return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.operationTimeout);
    }

    /**
     * Executes a gateway read with bounded retries and one absolute operation deadline.
     *
     * <p>The only POST endpoints here perform searches, so replay cannot write gateway data.
     */
    BraintreeXml.Element read(
            BraintreeResource resource, String target, String requestXml, long deadline)
            throws InterruptedException {
        byte[] body = requestXml == null ? null : requestXml.getBytes(StandardCharsets.UTF_8);
        for (int attempt = 0; ; ++attempt) {
            long hint = 0;
            String reason;
            try {
                return exchange(resource, target, body, deadline);
            } catch (Retry ex) {
                hint = ex.after;
                reason = "HTTP " + ex.status;
            } catch (SSLException ex) {
                check(resource, deadline);
                throw resource.failure("HTTPS certificate verification or negotiation failed");
            } catch (ZipException ex) {
                throw resource.failure("invalid gzip XML response");
            } catch (XMLStreamException ex) {
                check(resource, deadline);
                Throwable underlying = ex.getNestedException();
                if (underlying instanceof BraintreeFailure) {
                    throw (BraintreeFailure) underlying;
                }
                if (underlying instanceof IOException) {
                    reason = "HTTPS I/O failure or timeout while reading XML";
                } else {
                    throw resource.failure("invalid or forbidden XML response");
                }
            } catch (HttpException ex) {
                throw resource.failure("invalid HTTP response");
            } catch (IOException ex) {
                check(resource, deadline);
                reason = "HTTPS I/O failure or timeout";
            }
            if (attempt == config.retries) {
                throw resource.failure(reason + " after " + (attempt + 1) + " attempts");
            }
            if (hint > config.retryDelay) {
                throw resource.failure("Retry-After exceeds max_retry_delay_ms");
            }
            long delay =
                    TimeUnit.MILLISECONDS.toNanos(
                            Math.max(hint, Math.min(config.retryDelay, 250L << attempt)));
            if (delay >= check(resource, deadline)) {
                throw resource.failure("retry delay exceeds HTTPS operation deadline");
            }
            TimeUnit.NANOSECONDS.sleep(delay);
        }
    }

    private BraintreeXml.Element exchange(
            BraintreeResource resource, String target, byte[] body, long deadline)
            throws IOException, HttpException, InterruptedException, Retry, XMLStreamException {
        long attempt =
                Math.min(
                        check(resource, deadline),
                        TimeUnit.MILLISECONDS.toNanos(config.requestTimeout));
        long expires = System.nanoTime() + attempt;
        Socket raw = new Socket(Proxy.NO_PROXY);
        ScheduledFuture<?> abort = null;
        try {
            synchronized (this) {
                check(resource, expires);
                active = raw;
                abort = watchdog.schedule(() -> closeSocket(raw), attempt, TimeUnit.NANOSECONDS);
            }
            int timeout = (int) Math.max(1, TimeUnit.NANOSECONDS.toMillis(attempt));
            raw.connect(
                    new InetSocketAddress(config.host, 443),
                    Math.min(timeout, config.connectTimeout));
            raw.setSoTimeout(timeout);
            try (SSLSocket socket = (SSLSocket) tls.createSocket(raw, config.host, 443, true);
                    DefaultBHttpClientConnection wire =
                            new DefaultBHttpClientConnection(8192, null, null, HEADERS)) {
                SSLParameters parameters = socket.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                socket.setSSLParameters(parameters);
                socket.startHandshake();
                wire.bind(socket);
                HttpResponse response =
                        new HttpRequestExecutor()
                                .execute(request(target, body), wire, HttpCoreContext.create());
                int status = response.getStatusLine().getStatusCode();
                if (RETRYABLE.contains(status)) {
                    throw new Retry(status, retryAfter(response));
                }
                if (status != 200) {
                    throw resource.failure("gateway returned HTTP " + status);
                }
                return xml(resource, response, expires);
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

    private HttpRequest request(String target, byte[] body) {
        String path = config.merchantPath + target;
        HttpRequest request;
        if (body == null) {
            request = new BasicHttpRequest("GET", path);
        } else {
            BasicHttpEntityEnclosingRequest post =
                    new BasicHttpEntityEnclosingRequest("POST", path);
            post.setEntity(new ByteArrayEntity(body));
            post.addHeader("Content-Type", "application/xml; charset=UTF-8");
            post.addHeader("Content-Length", Integer.toString(body.length));
            request = post;
        }
        request.addHeader("Host", config.host);
        request.addHeader("Accept", "application/xml");
        request.addHeader("Accept-Encoding", "gzip");
        request.addHeader("User-Agent", "Apache SeaTunnel Braintree");
        request.addHeader("X-ApiVersion", "6");
        request.addHeader("Authorization", config.authorization);
        request.addHeader("Connection", "close");
        return request;
    }

    private BraintreeXml.Element xml(
            BraintreeResource resource, HttpResponse response, long deadline)
            throws IOException, InterruptedException, XMLStreamException {
        HttpEntity entity = response.getEntity();
        if (entity == null || entity.getContentLength() > config.bytes) {
            throw resource.failure("empty response or max_response_bytes exceeded");
        }
        Header[] encoding = response.getHeaders("Content-Encoding");
        boolean gzip =
                encoding.length == 1 && "gzip".equalsIgnoreCase(encoding[0].getValue().trim());
        if (encoding.length > 0
                && !gzip
                && !(encoding.length == 1
                        && "identity".equalsIgnoreCase(encoding[0].getValue().trim()))) {
            throw resource.failure("unsupported HTTPS content encoding");
        }
        InputStream boundedWire = new LimitedInput(entity.getContent(), config.bytes, resource);
        try (InputStream stream = gzip ? new GZIPInputStream(boundedWire) : boundedWire) {
            InputStream xml = gzip ? new LimitedInput(stream, config.bytes, resource) : stream;
            BraintreeXml.Element root =
                    BraintreeXml.read(xml, config, resource, () -> check(resource, deadline));
            // Reach the gzip trailer before accepting a row. This also accounts for trailing XML
            // bytes.
            byte[] tail = new byte[8192];
            while (xml.read(tail) >= 0) {
                check(resource, deadline);
            }
            return root;
        }
    }

    private static long retryAfter(HttpResponse response) {
        long result = 0;
        for (Header header : response.getHeaders("Retry-After")) {
            String value = header.getValue().trim();
            try {
                long wait =
                        value.matches("[0-9]+")
                                ? Math.multiplyExact(Long.parseLong(value), 1000L)
                                : ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME)
                                                .toInstant()
                                                .toEpochMilli()
                                        - System.currentTimeMillis();
                result = Math.max(result, wait);
            } catch (ArithmeticException | NumberFormatException ex) {
                return Long.MAX_VALUE;
            } catch (DateTimeException ex) {
                // Malformed values cannot extend local retry delays.
            }
        }
        return result;
    }

    private long check(BraintreeResource resource, long deadline) throws InterruptedException {
        if (closed || Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Braintree source closed");
        }
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
            throw resource.failure("HTTPS operation deadline exceeded");
        }
        return remaining;
    }

    /**
     * Aborts the current socket and cancels the only deadline worker.
     *
     * <p>The source reader makes this cancellation sticky across open callbacks.
     */
    @Override
    public synchronized void close() {
        closed = true;
        closeSocket(active);
        watchdog.shutdownNow();
    }

    private static void closeSocket(Socket socket) {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // This connection will not be reused.
            }
        }
    }

    /**
     * A transient HTTP rejection with its numeric retry hint.
     *
     * <p>It carries no request values or response body.
     */
    private static final class Retry extends Exception {
        private static final long serialVersionUID = 1L;
        final int status;
        final long after;

        Retry(int status, long after) {
            this.status = status;
            this.after = after;
        }
    }

    /**
     * Counts bytes without exposing the underlying stream for an unbounded drain.
     *
     * <p>One wrapper covers wire bytes and another can cover post-gzip XML bytes.
     */
    private static final class LimitedInput extends FilterInputStream {
        private final int limit;
        private final BraintreeResource resource;
        private int consumed;

        LimitedInput(InputStream input, int limit, BraintreeResource resource) {
            super(input);
            this.limit = limit;
            this.resource = resource;
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
        public int read(byte[] buffer, int offset, int length) throws IOException {
            int size = in.read(buffer, offset, length);
            if (size > 0) {
                count(size);
            }
            return size;
        }

        private void count(int size) {
            consumed += size;
            if (consumed > limit) {
                throw resource.failure("XML response exceeds max_response_bytes");
            }
        }

        @Override
        public void close() {
            // Socket ownership stays with the enclosing attempt.
        }
    }
}
