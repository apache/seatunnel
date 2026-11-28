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
package org.apache.seatunnel.connectors.seatunnel.bugsnag;

import okhttp3.Call;
import okhttp3.ConnectionPool;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
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
import java.nio.charset.StandardCharsets;
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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Restricted BugSnag HTTPS client for one worker.
 *
 * <p>Every request is a GET to the fixed origin. The page deadline includes retry waits, transfers
 * and parsing. A private trust store is used only for an explicitly configured custom origin.
 */
final class BugSnagTransport implements AutoCloseable {
    private static final int MAX_PEM_BYTES = 65536;
    private final BugSnagConfig options;
    private final HttpUrl origin;
    private final OkHttpClient client;

    // The same monitor binds close to the active call.
    private Call running;
    private boolean closed;

    BugSnagTransport(BugSnagConfig options) throws BugSnagReadException {
        this.options = options;
        origin = HttpUrl.get(options.origin);
        OkHttpClient.Builder builder =
                new OkHttpClient.Builder()
                        .proxy(Proxy.NO_PROXY)
                        .followRedirects(false)
                        .followSslRedirects(false)
                        .retryOnConnectionFailure(false)
                        .connectionPool(new ConnectionPool(1, 20L, TimeUnit.SECONDS))
                        .connectTimeout(options.timeout, TimeUnit.MILLISECONDS)
                        .readTimeout(options.timeout, TimeUnit.MILLISECONDS)
                        .callTimeout(options.timeout, TimeUnit.MILLISECONDS)
                        .addNetworkInterceptor(
                                chain -> {
                                    Response response = chain.proceed(chain.request());
                                    String retry = response.header("Retry-After");
                                    // The library reissues 503/zero even with connection retries
                                    // disabled.
                                    if (response.code() == 503
                                            && retry != null
                                            && retry.matches("0+")) {
                                        List<String> values = response.headers("Retry-After");
                                        Response.Builder bounded =
                                                response.newBuilder().removeHeader("Retry-After");
                                        if (values.size() > 1) {
                                            // Preserve malformed timing while preventing the
                                            // implicit follow-up.
                                            bounded.header(
                                                    "Retry-After", String.join(", ", values));
                                        }
                                        return bounded.build();
                                    }
                                    return response;
                                });
        if (options.trustCertificate != null) {
            loadTrust(builder);
        }
        client = builder.build();
    }

    /**
     * Reads one approved Data Access request within its deadline.
     *
     * <p>The route and query are rebuilt from validated state. Server continuation links are
     * inspected separately before they can affect a later request.
     */
    ResponsePage get(BugSnagWork work, Instant scanEnd, long deadline)
            throws BugSnagReadException, InterruptedException {
        HttpUrl url = url(work, scanEnd);
        Request request =
                new Request.Builder()
                        .url(url)
                        .get()
                        .header("Authorization", "token " + options.token)
                        .header("X-Version", "2")
                        .header("Accept", "application/json")
                        .header("User-Agent", "Apache-SeaTunnel-BugSnag/1.0")
                        .build();
        String exhausted = "HTTPS retry budget exhausted";
        for (int attempt = 0; attempt < options.attempts; attempt++) {
            checkDeadline(work, deadline);
            Call call = begin(request, work, deadline);
            long delay = 200L << attempt;
            try (Response response = call.execute()) {
                int status = response.code();
                if (status == 200) {
                    return new ResponsePage(
                            url, readBody(response, work), response.headers("Link"));
                }
                if (!retryable(status)) {
                    if (status == 401 || status == 403) {
                        throw work.failure(
                                "access denied (HTTP "
                                        + status
                                        + "); verify organization membership and resource permissions");
                    }
                    throw work.failure("provider rejected the request (HTTP " + status + ")");
                }
                delay = Math.max(delay, serverDelay(response, work));
                exhausted = "HTTP " + status + " retry budget exhausted";
            } catch (BugSnagReadException failure) {
                throw failure;
            } catch (SSLException failure) {
                throw work.failure("TLS certificate or hostname validation failed");
            } catch (IOException failure) {
                checkDeadline(work, deadline);
                if (call.isCanceled() || Thread.currentThread().isInterrupted()) {
                    throw work.failure("HTTPS request was cancelled");
                }
                exhausted = "HTTPS retry budget exhausted";
            } finally {
                released(call);
            }
            if (attempt + 1 < options.attempts) {
                if (delay >= TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())) {
                    throw work.failure("retry delay exceeds request deadline");
                }
                Thread.sleep(delay);
            }
        }
        throw work.failure(exhausted);
    }

    private HttpUrl url(BugSnagWork work, Instant scanEnd) {
        HttpUrl.Builder url = origin.newBuilder();
        BugSnagResource resource = work.resource;
        resource.path(work).forEach(url::addPathSegment);
        int requested = resource.pageSize(options.pageSize);
        if (requested != 0) {
            url.addQueryParameter("per_page", Integer.toString(requested));
        }
        if (resource.sort != null && resource != BugSnagResource.ORGANIZATIONS) {
            url.addQueryParameter("sort", resource.sort);
            if (resource != BugSnagResource.RELEASES) {
                url.addQueryParameter("direction", "desc");
            }
        }
        if (resource == BugSnagResource.EVENTS) {
            url.addQueryParameter("full_reports", "true");
        }
        if (resource == BugSnagResource.RELEASES) {
            url.addQueryParameter("base", scanEnd.toString());
        } else if (work.base != null) {
            url.addQueryParameter("base", work.base);
        }
        if (work.offset != null) {
            url.addQueryParameter("offset", work.offset);
        }
        return url.build();
    }

    private static boolean retryable(int code) {
        return code == 408
                || code == 429
                || code == 500
                || code == 502
                || code == 503
                || code == 504;
    }

    private static long serverDelay(Response response, BugSnagWork work)
            throws BugSnagReadException {
        List<String> timing = response.headers("Retry-After");
        if (timing.isEmpty()) {
            if (response.code() == 429) {
                throw work.failure("HTTP 429 without a confirmed Retry-After delay");
            }
            return 0L;
        }
        if (timing.size() != 1 || timing.get(0).isEmpty() || timing.get(0).length() > 96) {
            throw work.failure("provider sent invalid retry timing");
        }
        String value = timing.get(0);
        try {
            if (value.matches("[0-9]+")) {
                return Math.multiplyExact(Long.parseLong(value), 1000L);
            }
            return until(
                    Instant.now(),
                    ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant());
        } catch (ArithmeticException | NumberFormatException overflow) {
            return Long.MAX_VALUE;
        } catch (DateTimeException invalid) {
            throw work.failure("provider sent invalid retry timing");
        }
    }

    private static long until(Instant now, Instant target) {
        Duration remaining = Duration.between(now, target);
        if (remaining.isZero() || remaining.isNegative()) {
            return 0L;
        }
        // Round up: never retry just before the advertised boundary.
        return Math.addExact(remaining.toMillis(), remaining.getNano() % 1000000 == 0 ? 0L : 1L);
    }

    private byte[] readBody(Response response, BugSnagWork work) throws IOException {
        // OkHttp unwraps negotiated gzip and exposes decoded bytes to the limit below.
        String encoding = response.header("Content-Encoding");
        if (encoding != null && !"identity".equalsIgnoreCase(encoding)) {
            throw work.failure("unsupported response compression");
        }
        ResponseBody body = response.body();
        MediaType type = body == null ? null : body.contentType();
        if (type == null
                || !"application".equals(type.type())
                || !"json".equals(type.subtype())
                || (type.parameter("charset") != null
                        && !StandardCharsets.UTF_8.equals(type.charset()))) {
            throw work.failure("expected Content-Type application/json with UTF-8");
        }
        if (body.contentLength() > options.maxBytes) {
            throw work.failure("response exceeds max_response_bytes");
        }
        try (InputStream source = body.byteStream();
                ByteArrayOutputStream captured = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[8192];
            for (int count; (count = source.read(buffer)) >= 0; ) {
                if (count > options.maxBytes - captured.size()) {
                    throw work.failure("response exceeds max_response_bytes");
                }
                captured.write(buffer, 0, count);
            }
            return captured.toByteArray();
        }
    }

    private synchronized Call begin(Request request, BugSnagWork work, long deadline)
            throws BugSnagReadException {
        if (closed) {
            throw work.failure("HTTPS request was cancelled");
        }
        running = client.newCall(request);
        running.timeout().deadlineNanoTime(deadline);
        return running;
    }

    private synchronized void released(Call call) {
        if (running == call) {
            running = null;
        }
    }

    static void checkDeadline(BugSnagWork work, long deadline) throws BugSnagReadException {
        if (System.nanoTime() - deadline >= 0L) {
            throw work.failure("request deadline exceeded");
        }
    }

    /**
     * Cancels in-flight I/O and releases this connector's private connection pool.
     *
     * <p>The JVM trust configuration and other clients are untouched.
     */
    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;
            if (running != null) {
                running.cancel();
            }
            client.connectionPool().evictAll();
            client.dispatcher().executorService().shutdownNow();
        }
    }

    private void loadTrust(OkHttpClient.Builder builder) throws BugSnagReadException {
        try {
            Path path = Path.of(options.trustCertificate);
            if (!Files.isRegularFile(path)) {
                throw new BugSnagReadException(
                        "transport", "trust_certificate is not a regular file");
            }
            byte[] pem;
            try (InputStream source = Files.newInputStream(path)) {
                pem = source.readNBytes(MAX_PEM_BYTES + 1);
            }
            if (pem.length == 0 || pem.length > MAX_PEM_BYTES) {
                throw new BugSnagReadException(
                        "transport", "trust_certificate has an invalid size");
            }
            Collection<? extends Certificate> certificates =
                    CertificateFactory.getInstance("X.509")
                            .generateCertificates(new ByteArrayInputStream(pem));
            if (certificates.isEmpty() || certificates.size() > 16) {
                throw new BugSnagReadException(
                        "transport", "trust_certificate must contain a bounded X.509 chain");
            }
            KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());
            store.load(null);
            int index = 0;
            for (Certificate certificate : certificates) {
                store.setCertificateEntry("ca-" + index++, certificate);
            }
            TrustManagerFactory factory =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            factory.init(store);
            for (TrustManager trust : factory.getTrustManagers()) {
                if (trust instanceof X509TrustManager) {
                    X509TrustManager manager = (X509TrustManager) trust;
                    SSLContext tls = SSLContext.getInstance("TLS");
                    tls.init(null, new TrustManager[] {manager}, null);
                    builder.sslSocketFactory(tls.getSocketFactory(), manager);
                    return;
                }
            }
        } catch (BugSnagReadException failure) {
            throw failure;
        } catch (IOException | GeneralSecurityException | RuntimeException failure) {
            throw new BugSnagReadException("transport", "could not load trust_certificate");
        }
        throw new BugSnagReadException("transport", "could not establish X.509 trust");
    }

    /**
     * An ephemeral successful HTTPS response and its continuation headers.
     *
     * <p>This value is never serialized with source progress.
     */
    static final class ResponsePage {
        final HttpUrl request;
        final byte[] body;
        final List<String> links;

        ResponsePage(HttpUrl request, byte[] body, List<String> links) {
            this.request = request;
            this.body = body;
            this.links = links;
        }
    }
}
