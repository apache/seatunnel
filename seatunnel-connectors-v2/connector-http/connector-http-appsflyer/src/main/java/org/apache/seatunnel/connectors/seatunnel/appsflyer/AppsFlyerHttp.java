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

package org.apache.seatunnel.connectors.seatunnel.appsflyer;

import org.apache.seatunnel.connectors.seatunnel.appsflyer.AppsFlyerRecords.InvalidReport;
import org.apache.seatunnel.connectors.seatunnel.appsflyer.AppsFlyerRecords.Saturated;
import org.apache.seatunnel.connectors.seatunnel.appsflyer.AppsFlyerRecords.Snapshot;

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
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Date;
import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPInputStream;

/**
 * Retrieves one complete AppsFlyer report within a bounded attempt and time budget. Delegated
 * report downloads receive no API credentials and errors omit signed URLs and bodies.
 */
final class AppsFlyerHttp {
    private static final int MAX_REDIRECTS = 3;
    private static final int MAX_ERROR_BYTES = 8192;
    private static final DefaultBHttpClientConnectionFactory CONNECTIONS =
            new DefaultBHttpClientConnectionFactory(
                    ConnectionConfig.custom()
                            .setMessageConstraints(
                                    MessageConstraints.custom()
                                            .setMaxHeaderCount(64)
                                            .setMaxLineLength(16384)
                                            .build())
                            .build());

    private final AppsFlyerConfig config;
    private final String apiOrigin;
    private final AtomicReference<Socket> current = new AtomicReference<>();
    private volatile boolean closed;
    private int attemptsUsed;

    AppsFlyerHttp(AppsFlyerConfig config) {
        this.config = config;
        this.apiOrigin = AppsFlyerConfig.originKey(config.origin);
    }

    /**
     * Follows the reporting redirects within the original deadline and origin policy. Temporary
     * errors retry the initial request only while all budgets allow it.
     */
    Snapshot fetch(AppsFlyerWindow window, long deadline) throws IOException {
        long retryDelay = 0;
        for (int attempt = 1; ; attempt++) {
            await(window, deadline, retryDelay);
            try {
                URI target = config.origin.resolve(window.report.target(config, window));
                boolean authorize = true;
                for (int hops = 0; ; hops++) {
                    if (attemptsUsed >= config.maxRequests) {
                        throw failure(window, "request budget exhausted (max_requests)");
                    }
                    attemptsUsed++;
                    Reply reply = exchange(window, target, authorize, deadline);
                    if (reply.snapshot != null) {
                        return reply.snapshot;
                    }
                    if (hops == MAX_REDIRECTS) {
                        throw failure(window, "report redirect limit exceeded");
                    }
                    target = redirect(window, target, reply.location);
                    authorize = authorize && apiOrigin.equals(AppsFlyerConfig.originKey(target));
                }
            } catch (Retryable e) {
                if (attempt == config.maxAttempts) {
                    throw failure(window, "retry limit reached after " + e.reason);
                }
                long base = 500L << Math.min(attempt - 1, 5);
                retryDelay =
                        Math.max(
                                e.waitMillis, ThreadLocalRandom.current().nextLong(base, 2 * base));
            }
        }
    }

    private URI redirect(AppsFlyerWindow window, URI previous, String location) throws SafeFailure {
        try {
            URI resolved = previous.resolve(location);
            if (AppsFlyerConfig.validHttps(resolved)
                    && resolved.getRawPath() != null
                    && resolved.getRawPath().startsWith("/")
                    && !resolved.getRawPath().startsWith("//")) {
                String key = AppsFlyerConfig.originKey(resolved);
                boolean sameOrigin = apiOrigin.equals(key);
                if (sameOrigin
                        || (config.downloadOrigins.contains(key) && !containsToken(resolved))) {
                    return resolved;
                }
            }
        } catch (IllegalArgumentException ignored) {
            // Signed locations can contain user data and credential material.
        }
        throw failure(window, "report redirect is outside the configured HTTPS origins");
    }

    private boolean containsToken(URI target) {
        String query = target.getQuery();
        return target.toASCIIString().contains(config.token)
                || target.getPath().contains(config.token)
                || (query != null && query.contains(config.token));
    }

    private Reply exchange(AppsFlyerWindow window, URI uri, boolean authorize, long deadline)
            throws IOException {
        Socket raw = new Socket();
        current.set(raw);
        try {
            if (closed) {
                throw failure(window, "request canceled");
            }
            String host = uri.getHost();
            int port = uri.getPort() < 0 ? 443 : uri.getPort();
            raw.connect(
                    new InetSocketAddress(host, port), timeout(deadline, config.connectTimeout));
            raw.setSoTimeout(timeout(deadline, config.readTimeout));
            SSLSocketFactory sockets = (SSLSocketFactory) SSLSocketFactory.getDefault();
            try (SSLSocket tls = (SSLSocket) sockets.createSocket(raw, host, port, true)) {
                SSLParameters parameters = tls.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                tls.setSSLParameters(parameters);
                tls.setEnabledProtocols(
                        Arrays.stream(tls.getEnabledProtocols())
                                .filter(name -> "TLSv1.2".equals(name) || "TLSv1.3".equals(name))
                                .toArray(String[]::new));
                tls.startHandshake();
                tls.setSoTimeout(timeout(deadline, config.readTimeout));
                DefaultBHttpClientConnection connection = CONNECTIONS.createConnection(tls);
                String path = uri.getRawPath();
                String query = uri.getRawQuery();
                BasicHttpRequest request =
                        new BasicHttpRequest("GET", query == null ? path : path + "?" + query);
                request.setHeader("Host", uri.getRawAuthority());
                if (authorize) {
                    request.setHeader("Authorization", "Bearer " + config.token);
                }
                request.setHeader("Accept", "text/csv");
                request.setHeader("Accept-Encoding", "gzip");
                request.setHeader("Connection", "close");
                request.setHeader("User-Agent", "Apache-SeaTunnel-AppsFlyer");
                HttpResponse response =
                        new HttpRequestExecutor()
                                .execute(request, connection, new BasicHttpContext());
                int status = response.getStatusLine().getStatusCode();
                if (status == 301
                        || status == 302
                        || status == 303
                        || status == 307
                        || status == 308) {
                    Header[] locations = response.getHeaders("Location");
                    if (locations.length != 1 || locations[0].getValue().isEmpty()) {
                        throw failure(window, "invalid report redirect");
                    }
                    return new Reply(null, locations[0].getValue());
                }
                if (status != 200) {
                    classifyError(window, response, status, authorize);
                }
                try {
                    Snapshot parsed = decode(window, response);
                    if (System.nanoTime() - deadline >= 0) {
                        throw new SafeFailure("request deadline exceeded");
                    }
                    return new Reply(parsed, null);
                } catch (Saturated e) {
                    if (System.nanoTime() - deadline >= 0) {
                        throw failure(window, "request deadline exceeded");
                    }
                    throw e;
                } catch (InvalidReport | SafeFailure e) {
                    throw failure(
                            window,
                            System.nanoTime() - deadline >= 0
                                    ? "request deadline exceeded"
                                    : e.getMessage());
                } catch (IOException ignored) {
                    throw failure(
                            window,
                            System.nanoTime() - deadline >= 0
                                    ? "request deadline exceeded"
                                    : "invalid, incomplete or unreadable CSV response");
                }
            }
        } catch (SafeFailure | Retryable | Saturated safe) {
            throw safe;
        } catch (IOException | HttpException | IllegalArgumentException ignored) {
            throw failure(
                    window,
                    System.nanoTime() - deadline >= 0
                            ? "request deadline exceeded"
                            : (closed ? "request canceled" : "HTTPS or TLS exchange failed"));
        } finally {
            current.compareAndSet(raw, null);
            closeSocket(raw);
        }
    }

    private void classifyError(
            AppsFlyerWindow window, HttpResponse response, int status, boolean authorized)
            throws IOException {
        if (status == 429 || (status >= 500 && status < 600)) {
            throw new Retryable("HTTP " + status, retryAfter(response));
        }
        if (authorized && (status == 400 || status == 403)) {
            String body = errorPrefix(response);
            if (status == 403 && !window.report.raw && body.contains("limit reached for ")) {
                throw new Retryable(
                        "aggregate report rate limit", Math.max(60000, retryAfter(response)));
            }
            if (status == 400
                    && (body.contains("your api calls limit has been reached for report type")
                            || body.contains("you've reached your maximum number of"))) {
                throw failure(window, "daily report quota exhausted");
            }
        }
        if (status == 206) {
            throw failure(window, "unexpected partial report response");
        }
        throw failure(window, "HTTP " + status);
    }

    private static String errorPrefix(HttpResponse response) throws IOException {
        HttpEntity body = response.getEntity();
        if (body == null) {
            return "";
        }
        InputStream input = body.getContent();
        Header encoding = response.getFirstHeader("Content-Encoding");
        if (encoding != null && "gzip".equalsIgnoreCase(encoding.getValue())) {
            try (GZIPInputStream gzip =
                    new GZIPInputStream(new BoundedBody(input, MAX_ERROR_BYTES))) {
                return new String(gzip.readNBytes(MAX_ERROR_BYTES), StandardCharsets.UTF_8)
                        .toLowerCase(Locale.ROOT);
            } catch (Saturated ignored) {
                return "";
            }
        }
        return new String(input.readNBytes(MAX_ERROR_BYTES), StandardCharsets.UTF_8)
                .toLowerCase(Locale.ROOT);
    }

    private Snapshot decode(AppsFlyerWindow window, HttpResponse response) throws IOException {
        Header[] lengths = response.getHeaders("Content-Length");
        Header[] transfer = response.getHeaders("Transfer-Encoding");
        Header[] encoding = response.getHeaders("Content-Encoding");
        HttpEntity entity = response.getEntity();
        if (entity == null
                || lengths.length > 1
                || transfer.length > 1
                || encoding.length > 1
                || response.containsHeader("Content-Range")
                || (lengths.length != 0 && transfer.length != 0)
                || (transfer.length != 0 && !"chunked".equalsIgnoreCase(transfer[0].getValue()))) {
            throw new SafeFailure("invalid or partial report framing");
        }
        Header contentType = response.getFirstHeader("Content-Type");
        if (contentType != null) {
            String mediaType = contentType.getValue().toLowerCase(Locale.ROOT);
            if (mediaType.contains("json") || mediaType.contains("text/html")) {
                throw new SafeFailure("provider returned a non-CSV report");
            }
        }
        if (entity.getContentLength() > config.maxResponse) {
            throw new Saturated("report exceeds max_response_bytes");
        }
        String coding = encoding.length == 0 ? "identity" : encoding[0].getValue().trim();
        if (!"identity".equalsIgnoreCase(coding) && !"gzip".equalsIgnoreCase(coding)) {
            throw new SafeFailure("unsupported response encoding");
        }
        InputStream wire = new BoundedBody(entity.getContent(), config.maxResponse);
        if ("identity".equalsIgnoreCase(coding)) {
            return AppsFlyerRecords.parse(wire, config, window);
        }
        try (GZIPInputStream gzip = new GZIPInputStream(wire)) {
            Snapshot report =
                    AppsFlyerRecords.parse(
                            new BoundedBody(gzip, config.maxResponse), config, window);
            if (wire.read() >= 0) {
                throw new SafeFailure("unexpected trailing compressed response bytes");
            }
            return report;
        }
    }

    private static long retryAfter(HttpResponse response) {
        Header header = response.getFirstHeader("Retry-After");
        if (header == null) {
            return 0;
        }
        String value = header.getValue().trim();
        try {
            return TimeUnit.SECONDS.toMillis(Math.max(0, Long.parseLong(value)));
        } catch (NumberFormatException ignored) {
            Date date = DateUtils.parseDate(value);
            return date == null ? 0 : Math.max(0, date.getTime() - System.currentTimeMillis());
        }
    }

    private static void await(AppsFlyerWindow window, long deadline, long delay)
            throws IOException {
        long nanos = TimeUnit.MILLISECONDS.toNanos(delay);
        if (deadline - System.nanoTime() <= nanos) {
            throw failure(
                    window,
                    delay == 0
                            ? "request deadline exceeded"
                            : "retry delay exceeds request deadline");
        }
        try {
            TimeUnit.NANOSECONDS.sleep(nanos);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw failure(window, "request canceled");
        }
    }

    private static int timeout(long deadline, int max) throws IOException {
        long left = deadline - System.nanoTime();
        if (left <= 0) {
            throw new IOException("deadline");
        }
        return (int) Math.min(max, Math.max(1, TimeUnit.NANOSECONDS.toMillis(left)));
    }

    /**
     * Cancels in-flight TLS and body I/O without holding the engine checkpoint lock. A racing new
     * socket also observes the closed flag before connecting.
     */
    void close() {
        closed = true;
        closeSocket(current.getAndSet(null));
    }

    private static void closeSocket(Socket raw) {
        if (raw != null) {
            try {
                raw.close();
            } catch (IOException ignored) {
                // The socket is already undergoing final cleanup.
            }
        }
    }

    static SafeFailure failure(AppsFlyerWindow window, String reason) {
        return new SafeFailure("AppsFlyer source " + window.report.endpoint + ": " + reason);
    }

    /**
     * Returns either a validated report or a redirect location held only in memory. Only the outer
     * request loop can approve a subsequent origin.
     */
    private static final class Reply {
        final Snapshot snapshot;
        final String location;

        Reply(Snapshot snapshot, String location) {
            this.snapshot = snapshot;
            this.location = location;
        }
    }

    /**
     * Represents a retryable provider status and delay without retaining provider error body. The
     * caller retries the initial request only under the same total deadline.
     */
    private static final class Retryable extends IOException {
        final String reason;
        final long waitMillis;

        Retryable(String reason, long waitMillis) {
            this.reason = reason;
            this.waitMillis = waitMillis;
        }
    }

    /**
     * Carries errors constructed without connection or URI details. Credentials, signed downloads
     * and raw help text remain private.
     */
    static final class SafeFailure extends IOException {
        SafeFailure(String reason) {
            super(reason);
        }
    }

    /**
     * Enforces a byte cap at one selected layer while the request owns socket disposal. Limits are
     * converted into a split decision before any row is emitted.
     */
    private static final class BoundedBody extends FilterInputStream {
        private int remaining;

        BoundedBody(InputStream in, int remaining) {
            super(in);
            this.remaining = remaining;
        }

        @Override
        public int read() throws IOException {
            int value = in.read();
            if (value >= 0 && remaining-- == 0) {
                throw new Saturated("report exceeds max_response_bytes");
            }
            return value;
        }

        @Override
        public int read(byte[] values, int offset, int count) throws IOException {
            int used = in.read(values, offset, Math.min(count, remaining + 1));
            remaining -= Math.max(0, used);
            if (remaining < 0) {
                throw new Saturated("report exceeds max_response_bytes");
            }
            return used;
        }

        @Override
        public void close() {
            // Final socket cleanup is owned by the enclosing request.
        }
    }
}
