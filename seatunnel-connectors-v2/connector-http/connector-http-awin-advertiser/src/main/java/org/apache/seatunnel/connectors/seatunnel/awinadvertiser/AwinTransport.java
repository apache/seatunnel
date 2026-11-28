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
package org.apache.seatunnel.connectors.seatunnel.awinadvertiser;

import okhttp3.Call;
import okhttp3.ConnectionPool;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Reader-owned HTTPS client for paced Awin legacy requests.
 *
 * <p>The rolling allowance includes failed attempts. This reader follows Retry-After for a
 * provider-wide limit, which may also count requests from other jobs using the same Awin user.
 */
final class AwinTransport implements AutoCloseable {
    private static final long MINUTE = TimeUnit.MINUTES.toNanos(1L);
    private static final int MAX_PEM_BYTES = 65536;
    private static final Pattern NEXT_LINK =
            Pattern.compile("(?i)\\brel\\s*=\\s*\"?next(?:\"|;|,|\\s|$)");
    private final AwinSettings options;
    private final OkHttpClient http;
    private final HttpUrl origin;

    // Confined to the single acquisition worker. An attempt is counted before it can send.
    private final Deque<Long> starts = new ArrayDeque<>();
    // These are guarded by the transport monitor while job stop may abort a call.
    private Call active;
    private boolean stopped;

    AwinTransport(AwinSettings options, AwinSlice slice) throws AwinReadException {
        this.options = options;
        origin = HttpUrl.get(options.origin);
        OkHttpClient.Builder builder =
                new OkHttpClient.Builder()
                        .proxy(Proxy.NO_PROXY)
                        .followRedirects(false)
                        .followSslRedirects(false)
                        .retryOnConnectionFailure(false)
                        .connectionPool(new ConnectionPool(1, 20, TimeUnit.SECONDS))
                        .connectTimeout(options.timeout, TimeUnit.MILLISECONDS)
                        .readTimeout(options.timeout, TimeUnit.MILLISECONDS)
                        .callTimeout(options.timeout, TimeUnit.MILLISECONDS)
                        .addNetworkInterceptor(
                                chain -> {
                                    Response response = chain.proceed(chain.request());
                                    String retry = response.header("Retry-After");
                                    // Disable OkHttp's automatic zero-delay 503 retry. Every
                                    // attempt must pass
                                    // this connector's rolling rate, deadline and request-attempt
                                    // accounting.
                                    if (response.code() == 503
                                            && retry != null
                                            && retry.matches("0+")) {
                                        return response.newBuilder()
                                                .removeHeader("Retry-After")
                                                .build();
                                    }
                                    return response;
                                });
        if (options.trustCertificate != null) {
            customTrust(builder, slice);
        }
        http = builder.build();
    }

    /**
     * Acquires one bounded response with the Awin token only in its Authorization header.
     *
     * <p>Any transport diagnostic is rebuilt using safe context because exceptions and response
     * bodies can expose sensitive reporting data.
     */
    byte[] get(AwinSlice slice, long deadline) throws AwinReadException, InterruptedException {
        HttpUrl.Builder destination = origin.newBuilder().encodedPath(slice.path(options));
        slice.query().forEach(destination::addQueryParameter);
        Request request =
                new Request.Builder()
                        .url(destination.build())
                        .get()
                        .header("Accept", "application/json")
                        .header("Authorization", "Bearer " + options.token)
                        .header("User-Agent", "Apache-SeaTunnel-AwinAdvertiser/1.0")
                        .build();
        String exhausted = "transport retry budget exhausted";
        for (int index = 0; index < options.attempts; index++) {
            checkDeadline(slice, deadline);
            admit(slice, deadline);
            checkDeadline(slice, deadline);
            Call requestCall = begin(slice, request, deadline);
            long delay = 200L << index;
            try (Response reply = requestCall.execute()) {
                int status = reply.code();
                if (status == 206
                        || reply.header("Content-Range") != null
                        || reply.headers("Link").stream()
                                .anyMatch(h -> NEXT_LINK.matcher(h).find())) {
                    throw new AwinReadException(
                            slice, "provider returned a partial response or continuation");
                }
                if (status == 200) {
                    return body(slice, reply);
                }
                if (status != 408 && status != 429 && status < 500) {
                    String problem =
                            status == 401 || status == 403
                                    ? "HTTP "
                                            + status
                                            + " requires a token authorized for this advertiser resource"
                                    : "provider rejected the request (HTTP " + status + ")";
                    throw new AwinReadException(slice, problem);
                }
                exhausted = "HTTP " + status + " retry budget exhausted";
                delay = Math.max(delay, retryAfter(reply.header("Retry-After")));
            } catch (AwinReadException safe) {
                throw safe;
            } catch (SSLException rejected) {
                throw new AwinReadException(slice, "TLS certificate or hostname validation failed");
            } catch (IOException transport) {
                checkDeadline(slice, deadline);
                if (requestCall.isCanceled() || Thread.currentThread().isInterrupted()) {
                    throw new AwinReadException(slice, "request was cancelled");
                }
                exhausted = "transport retry budget exhausted";
            } finally {
                end(requestCall);
            }
            if (index + 1 < options.attempts) {
                waitMillis(slice, delay, deadline, "retry delay exceeds request deadline");
            }
        }
        throw new AwinReadException(slice, exhausted);
    }

    private void admit(AwinSlice slice, long deadline)
            throws AwinReadException, InterruptedException {
        while (true) {
            long now = System.nanoTime();
            while (!starts.isEmpty() && now - starts.peekFirst() >= MINUTE) {
                starts.removeFirst();
            }
            if (starts.size() < options.requestsPerMinute) {
                starts.addLast(now);
                return;
            }
            long remaining = MINUTE - (now - starts.peekFirst());
            waitMillis(
                    slice,
                    TimeUnit.NANOSECONDS.toMillis(remaining) + 1L,
                    deadline,
                    "legacy rate pacing exceeds request deadline");
        }
    }

    private static void waitMillis(AwinSlice slice, long delay, long deadline, String failure)
            throws AwinReadException, InterruptedException {
        if (delay >= TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())) {
            throw new AwinReadException(slice, failure);
        }
        Thread.sleep(delay);
    }

    private byte[] body(AwinSlice slice, Response reply) throws IOException {
        ResponseBody payload = reply.body();
        if (payload == null) {
            throw new AwinReadException(slice, "missing response body");
        }
        String encoding = reply.header("Content-Encoding");
        if (encoding != null && !"identity".equalsIgnoreCase(encoding)) {
            throw new AwinReadException(slice, "unsupported response compression");
        }
        // OkHttp's transparent gzip handler strips both stale wire headers before this method.
        if (payload.contentLength() > options.maxBytes) {
            throw new AwinReadException(slice, "response exceeds max_response_bytes");
        }
        try (InputStream input = payload.byteStream();
                ByteArrayOutputStream decoded = new ByteArrayOutputStream()) {
            byte[] block = new byte[8192];
            for (int length; (length = input.read(block)) != -1; ) {
                if (length > options.maxBytes - decoded.size()) {
                    throw new AwinReadException(slice, "response exceeds max_response_bytes");
                }
                decoded.write(block, 0, length);
            }
            return decoded.toByteArray();
        }
    }

    private synchronized Call begin(AwinSlice slice, Request request, long deadline)
            throws AwinReadException {
        if (stopped) {
            throw new AwinReadException(slice, "reader was cancelled");
        }
        active = http.newCall(request);
        active.timeout().deadlineNanoTime(deadline);
        return active;
    }

    private synchronized void end(Call call) {
        if (active == call) {
            active = null;
        }
    }

    static void checkDeadline(AwinSlice slice, long deadline) throws AwinReadException {
        if (System.nanoTime() - deadline >= 0L) {
            throw new AwinReadException(slice, "request deadline exceeded");
        }
    }

    /**
     * Stops this reader's connection and releases its client resources.
     *
     * <p>The JVM-wide trust and network state remain outside the reader's ownership.
     */
    @Override
    public synchronized void close() {
        if (stopped) {
            return;
        }
        stopped = true;
        if (active != null) {
            active.cancel();
        }
        http.connectionPool().evictAll();
        http.dispatcher().executorService().shutdownNow();
    }

    private void customTrust(OkHttpClient.Builder builder, AwinSlice slice)
            throws AwinReadException {
        try {
            Path location = Path.of(options.trustCertificate);
            if (!Files.isRegularFile(location)) {
                throw new AwinReadException(slice, "trust_certificate is not a regular file");
            }
            byte[] data;
            try (InputStream input = Files.newInputStream(location)) {
                data = input.readNBytes(MAX_PEM_BYTES + 1);
            }
            if (data.length == 0 || data.length > MAX_PEM_BYTES) {
                throw new AwinReadException(
                        slice, "trust_certificate is outside the supported size");
            }
            Collection<? extends Certificate> anchors =
                    CertificateFactory.getInstance("X.509")
                            .generateCertificates(new ByteArrayInputStream(data));
            if (anchors.isEmpty() || anchors.size() > 16) {
                throw new AwinReadException(
                        slice, "trust_certificate has an invalid certificate count");
            }
            KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());
            store.load(null);
            int index = 0;
            for (Certificate anchor : anchors) {
                store.setCertificateEntry("awin-seed-" + index++, anchor);
            }
            TrustManagerFactory managers =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            managers.init(store);
            for (TrustManager manager : managers.getTrustManagers()) {
                if (manager instanceof X509TrustManager) {
                    SSLContext context = SSLContext.getInstance("TLS");
                    context.init(null, new TrustManager[] {manager}, null);
                    builder.sslSocketFactory(
                            context.getSocketFactory(), (X509TrustManager) manager);
                    return;
                }
            }
        } catch (AwinReadException safe) {
            throw safe;
        } catch (IOException | GeneralSecurityException | RuntimeException invalid) {
            throw new AwinReadException(slice, "could not load trust_certificate");
        }
        throw new AwinReadException(slice, "could not establish X.509 trust");
    }

    private static long retryAfter(String header) {
        if (header == null || header.length() > 96) {
            return 0L;
        }
        try {
            if (header.matches("[0-9]+")) {
                return Math.multiplyExact(Long.parseLong(header), 1000L);
            }
            Instant end =
                    ZonedDateTime.parse(header, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant();
            return Math.max(0L, Duration.between(Instant.now(), end).toMillis());
        } catch (ArithmeticException | NumberFormatException overflow) {
            return Long.MAX_VALUE;
        } catch (DateTimeException invalid) {
            return 0L;
        }
    }
}
