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

package org.apache.seatunnel.connectors.seatunnel.appfigures;

import okhttp3.Call;
import okhttp3.ConnectionPool;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import java.io.ByteArrayInputStream;
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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Connector-local HTTPS transport with cancellable, bounded requests.
 *
 * <p>Authorization stays on the selected origin. Daily quota exhaustion and retryable request
 * throttling are treated separately.
 */
final class AppfiguresHttp implements AutoCloseable {
    private static final int PEM_LIMIT = 65536;
    private static final Set<Integer> RETRYABLE = Set.of(408, 429, 500, 502, 503, 504);
    private final AppfiguresSettings settings;
    private final OkHttpClient http;
    private Call active;
    private boolean closed;

    AppfiguresHttp(AppfiguresSettings settings) throws AppfiguresFailure {
        this.settings = settings;
        OkHttpClient.Builder builder =
                new OkHttpClient.Builder()
                        .protocols(List.of(Protocol.HTTP_1_1))
                        .proxy(Proxy.NO_PROXY)
                        .followRedirects(false)
                        .followSslRedirects(false)
                        .retryOnConnectionFailure(false)
                        .connectionPool(new ConnectionPool(1, 30, TimeUnit.SECONDS))
                        .connectTimeout(settings.timeout, TimeUnit.MILLISECONDS)
                        .readTimeout(settings.timeout, TimeUnit.MILLISECONDS)
                        .callTimeout(settings.timeout, TimeUnit.MILLISECONDS)
                        .addNetworkInterceptor(
                                chain -> {
                                    Response response = chain.proceed(chain.request());
                                    if (response.code() == 503) {
                                        // Prevent a hidden OkHttp follow-up on Retry-After: 0.
                                        long delay = retryAfter(response.header("Retry-After"));
                                        response.close();
                                        throw new ServiceUnavailable(delay);
                                    }
                                    return response;
                                });
        if (settings.trustCertificate != null) {
            configureTrust(builder);
        }
        http = builder.build();
    }

    byte[] get(AppfiguresResource target, Map<String, String> args, long deadline)
            throws IOException, InterruptedException {
        HttpUrl.Builder url = HttpUrl.get(settings.origin + "/v2/" + target.route).newBuilder();
        args.forEach(url::addQueryParameter);
        Request request =
                new Request.Builder()
                        .url(url.build())
                        .get()
                        .header("Accept", "application/json")
                        .header("Authorization", "Bearer " + settings.token)
                        .header("User-Agent", "Apache-SeaTunnel-Appfigures/1.0")
                        .build();

        int status = 0;
        for (int attempt = 0; attempt < settings.maxAttempts; attempt++) {
            requireTime(deadline);
            long delay = 200L << attempt;
            Call call = begin(request, deadline);
            try (Response response = call.execute()) {
                status = response.code();
                if (status == 200) {
                    return body(response);
                }
                if (status == 420) {
                    throw failure("HTTP 420 daily API quota exhausted");
                }
                if (!RETRYABLE.contains(status)) {
                    if (status == 401 || status == 403 || status == 402) {
                        throw failure("HTTP " + status + " denied access to the selected resource");
                    }
                    throw failure("unexpected HTTP " + status);
                }
                delay = Math.max(delay, retryAfter(response.header("Retry-After")));
            } catch (ServiceUnavailable e) {
                status = 503;
                delay = Math.max(delay, e.retryAfter);
            } catch (AppfiguresFailure e) {
                throw e;
            } catch (SSLException e) {
                throw failure("HTTPS certificate or hostname validation failed");
            } catch (IOException e) {
                if (call.isCanceled() || Thread.currentThread().isInterrupted()) {
                    requireTime(deadline);
                    throw failure("HTTPS request was cancelled");
                }
                status = 0;
            } finally {
                release(call);
            }
            if (attempt + 1 < settings.maxAttempts) {
                if (delay >= TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())) {
                    throw failure("retry delay exceeds the request deadline");
                }
                Thread.sleep(delay);
            }
        }
        throw failure(
                status == 0
                        ? "transport retry budget exhausted"
                        : "HTTP " + status + " retry budget exhausted");
    }

    private byte[] body(Response response) throws IOException {
        ResponseBody body = response.body();
        if (body == null) {
            throw failure("missing response body");
        }
        if (body.contentLength() > settings.maxBytes) {
            throw failure("response exceeds max_response_bytes");
        }
        String encoding = response.header("Content-Encoding");
        if (encoding != null && !"identity".equalsIgnoreCase(encoding)) {
            throw failure("unsupported response content encoding");
        }
        try (InputStream data = body.byteStream()) {
            byte[] bytes = data.readNBytes(settings.maxBytes + 1);
            if (bytes.length > settings.maxBytes) {
                throw failure("response exceeds max_response_bytes");
            }
            return bytes;
        }
    }

    private synchronized Call begin(Request request, long deadline) throws AppfiguresFailure {
        if (closed) {
            throw failure("HTTPS request was cancelled");
        }
        active = http.newCall(request);
        active.timeout().deadlineNanoTime(deadline);
        return active;
    }

    private synchronized void release(Call call) {
        if (active == call) {
            active = null;
        }
    }

    void requireTime(long deadline) throws AppfiguresFailure {
        if (System.nanoTime() - deadline >= 0L) {
            throw failure("request deadline exceeded");
        }
    }

    /**
     * Closes the active transfer before shutting down the isolated HTTP resources.
     *
     * <p>The reader can cancel a blocked body read without acquiring its checkpoint lock.
     */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (active != null) {
            active.cancel();
        }
        http.dispatcher().executorService().shutdownNow();
        http.connectionPool().evictAll();
    }

    private void configureTrust(OkHttpClient.Builder client) throws AppfiguresFailure {
        try {
            Path path = Path.of(settings.trustCertificate);
            if (!Files.isRegularFile(path)) {
                throw failure("trust_certificate must refer to a regular PEM file");
            }
            byte[] pem;
            try (InputStream file = Files.newInputStream(path)) {
                pem = file.readNBytes(PEM_LIMIT + 1);
            }
            if (pem.length == 0 || pem.length > PEM_LIMIT) {
                throw failure("trust_certificate is empty or exceeds its byte limit");
            }
            Collection<? extends Certificate> anchors =
                    CertificateFactory.getInstance("X.509")
                            .generateCertificates(new ByteArrayInputStream(pem));
            if (anchors.isEmpty() || anchors.size() > 16) {
                throw failure("invalid number of trust anchors");
            }
            KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());
            store.load(null, null);
            int certificate = 0;
            for (Certificate anchor : anchors) {
                store.setCertificateEntry("anchor-" + certificate++, anchor);
            }
            TrustManagerFactory factory =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            factory.init(store);
            for (TrustManager manager : factory.getTrustManagers()) {
                if (manager instanceof X509TrustManager) {
                    X509TrustManager trust = (X509TrustManager) manager;
                    SSLContext ssl = SSLContext.getInstance("TLS");
                    ssl.init(null, new TrustManager[] {trust}, null);
                    client.sslSocketFactory(ssl.getSocketFactory(), trust);
                    return;
                }
            }
            throw failure("no X.509 trust manager is available");
        } catch (AppfiguresFailure e) {
            throw e;
        } catch (IOException | GeneralSecurityException | IllegalArgumentException e) {
            throw failure("could not load trust_certificate");
        }
    }

    private static long retryAfter(String value) {
        if (value == null || value.isEmpty()) {
            return 0L;
        }
        if (value.chars().allMatch(c -> c >= '0' && c <= '9')) {
            try {
                return TimeUnit.SECONDS.toMillis(Long.parseLong(value));
            } catch (NumberFormatException e) {
                return Long.MAX_VALUE;
            }
        }
        if (value.length() > 100) {
            return 0L;
        }
        try {
            Instant after =
                    ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant();
            return Math.max(Duration.between(Instant.now(), after).toMillis(), 0L);
        } catch (DateTimeException | ArithmeticException ignored) {
            return 0L;
        }
    }

    private AppfiguresFailure failure(String reason) {
        return new AppfiguresFailure(settings.resource, reason);
    }

    /**
     * Signals a 503 intercepted before OkHttp can perform its own retry.
     *
     * <p>Only the parsed provider delay is carried across the HTTP boundary.
     */
    private static final class ServiceUnavailable extends IOException {
        private static final long serialVersionUID = 1L;
        private final long retryAfter;

        private ServiceUnavailable(long retryAfter) {
            this.retryAfter = retryAfter;
        }
    }
}
