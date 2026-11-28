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
package org.apache.seatunnel.connectors.seatunnel.box;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import okhttp3.Call;
import okhttp3.ConnectionPool;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
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
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Quiet worker-local HTTPS transport for authorized Box API and content requests.
 *
 * <p>Credentials stay within the configured API origin or published Box content origins. Externally
 * provided URLs are accepted only by the representation download path.
 */
final class BoxHttp implements AutoCloseable {
    private static final MediaType JSON = MediaType.get("application/json");
    private static final Set<String> CONTENT_HOSTS =
            Set.of("dl.boxcloud.com", "public.boxcloud.com");
    private static final int MAX_CERTIFICATE_BYTES = 65536;

    final HttpUrl api;
    private final HttpUrl customContent;
    private final BoxConfig config;
    private final OkHttpClient client;
    private final BoxAuth auth;

    // Guarded by this instance while cancellation can run independently of acquisition.
    private Call active;
    private boolean closed;

    BoxHttp(BoxConfig options) throws BoxReadException {
        config = options;
        api = HttpUrl.get(options.origin);
        customContent = options.downloadOrigin == null ? null : HttpUrl.get(options.downloadOrigin);
        auth = new BoxAuth(options);
        OkHttpClient.Builder builder =
                new OkHttpClient.Builder()
                        .proxy(Proxy.NO_PROXY)
                        .followRedirects(false)
                        .followSslRedirects(false)
                        .retryOnConnectionFailure(false)
                        .connectionPool(new ConnectionPool(2, 20L, TimeUnit.SECONDS))
                        .connectTimeout(config.timeout, TimeUnit.MILLISECONDS)
                        .readTimeout(config.timeout, TimeUnit.MILLISECONDS)
                        .callTimeout(config.timeout, TimeUnit.MILLISECONDS)
                        .addNetworkInterceptor(
                                chain -> {
                                    Response response = chain.proceed(chain.request());
                                    String retry = response.header("Retry-After");
                                    // Suppress OkHttp's special transparent replay for a zero-delay
                                    // 503.
                                    if (response.code() == 503
                                            && retry != null
                                            && retry.matches("0+")) {
                                        return response.newBuilder()
                                                .removeHeader("Retry-After")
                                                .build();
                                    }
                                    return response;
                                });
        if (config.trustCertificate != null) {
            configureTrust(builder, config.trustCertificate);
        }
        client = builder.build();
    }

    Acquisition start(BoxSplit state, long deadline) {
        return new Acquisition(deadline, config.maxRequests - state.requests);
    }

    /**
     * Resolves one CCG form request without applying the API's Authorization header.
     *
     * <p>The grant cannot be sent outside the configured API origin.
     */
    JsonNode grant(Request request, long deadline) throws BoxReadException, InterruptedException {
        if (!safe(request.url()) || !sameOrigin(api, request.url())) {
            throw new BoxReadException("token grant", "invalid token endpoint");
        }
        Result result = exchange(request, "token grant", deadline, false, false);
        if (result.status == 401) {
            throw new BoxReadException("token grant", "provider rejected the request (HTTP 401)");
        }
        return asJson(result, "token grant");
    }

    boolean allowedContent(HttpUrl url) {
        return safe(url)
                && (customContent == null
                        ? config.trustCertificate == null
                                && url.port() == 443
                                && CONTENT_HOSTS.contains(url.host())
                        : sameOrigin(url, customContent));
    }

    boolean allowedApi(HttpUrl url) {
        return safe(url) && sameOrigin(api, url);
    }

    private static boolean safe(HttpUrl url) {
        return url != null
                && "https".equals(url.scheme())
                && url.username().isEmpty()
                && url.password().isEmpty()
                && url.fragment() == null;
    }

    private static boolean sameOrigin(HttpUrl first, HttpUrl second) {
        return first.scheme().equals(second.scheme())
                && first.host().equals(second.host())
                && first.port() == second.port();
    }

    /**
     * Keeps a shared absolute deadline and logical request count for one prepared response.
     *
     * <p>Retries and CCG renewal are bounded independently. Only committed results advance the
     * outer split's logical request count.
     */
    final class Acquisition {
        final long deadline;
        private final int budget;
        private int used;

        private Acquisition(long deadline, int budget) {
            this.deadline = deadline;
            this.budget = budget;
        }

        int used() {
            return used;
        }

        JsonNode get(HttpUrl url, String operation) throws BoxReadException, InterruptedException {
            return asJson(request(url, null, operation, null, false, false), operation);
        }

        JsonNode hint(HttpUrl url, String operation) throws BoxReadException, InterruptedException {
            return asJson(
                    request(url, null, operation, "[extracted_text]", false, false), operation);
        }

        JsonNode post(HttpUrl url, String body, String operation)
                throws BoxReadException, InterruptedException {
            return asJson(request(url, body, operation, null, false, false), operation);
        }

        Result poll(HttpUrl url, String operation) throws BoxReadException, InterruptedException {
            return request(url, null, operation, null, true, false);
        }

        Result content(HttpUrl url, String operation)
                throws BoxReadException, InterruptedException {
            return request(url, null, operation, null, false, true);
        }

        private Result request(
                HttpUrl url,
                String json,
                String operation,
                String hint,
                boolean allowPending,
                boolean content)
                throws BoxReadException, InterruptedException {
            if (!(content ? allowedContent(url) : allowedApi(url))) {
                throw new BoxReadException(
                        operation, "request origin is outside the Box allowlist");
            }
            if (used == budget) {
                throw new BoxReadException(
                        operation, "max_requests exhausted before the scan completed");
            }
            used++;
            Request.Builder builder =
                    new Request.Builder()
                            .url(url)
                            .header(
                                    "Accept",
                                    content
                                            ? "text/plain, application/octet-stream"
                                            : "application/json");
            if (hint != null) {
                builder.header("X-Rep-Hints", hint);
            }
            if (json == null) {
                builder.get();
            } else {
                builder.post(RequestBody.create(json, JSON));
            }

            Result result = authorized(builder, operation, json == null, allowPending);
            if (result.status == 401 && config.clientCredentials) {
                auth.invalidate();
                result = authorized(builder, operation, json == null, allowPending);
            }
            if (result.status == 401) {
                throw new BoxReadException(operation, "provider rejected the request (HTTP 401)");
            }
            return result;
        }

        private Result authorized(
                Request.Builder builder, String operation, boolean safeRead, boolean allowPending)
                throws BoxReadException, InterruptedException {
            String token = auth.token(BoxHttp.this, deadline);
            Request request = builder.header("Authorization", "Bearer " + token).build();
            return exchange(request, operation, deadline, safeRead, allowPending);
        }
    }

    /**
     * Raw response from a bounded successful request or an explicit authentication rejection.
     *
     * <p>Only a representation status poll may return HTTP 202 as a successful intermediate.
     */
    static final class Result {
        final int status;
        final byte[] bytes;
        final MediaType type;
        final long retryDelay;

        private Result(int status, byte[] bytes, MediaType type, long retryDelay) {
            this.status = status;
            this.bytes = bytes;
            this.type = type;
            this.retryDelay = retryDelay;
        }
    }

    static JsonNode asJson(Result result, String operation) throws BoxReadException {
        if (result.type == null
                || !"application".equals(result.type.type())
                || !"json".equals(result.type.subtype())) {
            throw new BoxReadException(operation, "expected Content-Type application/json");
        }
        return BoxJson.object(BoxJson.read(result.bytes, operation), operation);
    }

    private Result exchange(
            Request request, String operation, long expires, boolean safeRead, boolean allowPending)
            throws BoxReadException, InterruptedException {
        String exhausted = "HTTPS retry budget exhausted";
        for (int attempt = 0; attempt < config.attempts; attempt++) {
            deadline(operation, expires);
            Call call = begin(request, operation, expires);
            long delay = 200L << attempt;
            try (Response response = call.execute()) {
                int status = response.code();
                if (status == 401) {
                    return new Result(status, new byte[0], null, 0L);
                }
                long serverDelay = retryAfter(response.header("Retry-After"));
                if (status == 200 || (status == 202 && allowPending)) {
                    return capture(response, operation, serverDelay);
                }

                if (status == 429) {
                    if (serverDelay < 0L) {
                        throw new BoxReadException(
                                operation, "HTTP 429 without a confirmed retry delay");
                    }
                } else if (isTransient(status)) {
                    if (!safeRead) {
                        throw new BoxReadException(
                                operation, "POST result is unknown after HTTP " + status);
                    }
                } else {
                    throw new BoxReadException(
                            operation, "provider rejected the request (HTTP " + status + ")");
                }
                delay = Math.max(delay, serverDelay);
                exhausted = "HTTP " + status + " retry budget exhausted";
            } catch (BoxReadException failure) {
                throw failure;
            } catch (SSLException failure) {
                throw new BoxReadException(
                        operation, "TLS certificate or hostname validation failed");
            } catch (IOException failure) {
                if (!safeRead) {
                    throw new BoxReadException(
                            operation, "POST result is unknown after an HTTPS failure");
                }
                deadline(operation, expires);
                if (call.isCanceled() || Thread.currentThread().isInterrupted()) {
                    throw new BoxReadException(operation, "HTTPS request was cancelled");
                }
                exhausted = "HTTPS retry budget exhausted";
            } finally {
                end(call);
            }
            if (attempt + 1 < config.attempts) {
                pause(operation, expires, delay);
            }
        }
        throw new BoxReadException(operation, exhausted);
    }

    private static boolean isTransient(int status) {
        return status == 408 || status == 500 || status == 502 || status == 503 || status == 504;
    }

    static void deadline(String operation, long expires) throws BoxReadException {
        if (System.nanoTime() - expires >= 0L) {
            throw new BoxReadException(operation, "request deadline exceeded");
        }
    }

    static void pause(String operation, long expires, long delay)
            throws BoxReadException, InterruptedException {
        if (delay >= TimeUnit.NANOSECONDS.toMillis(expires - System.nanoTime())) {
            throw new BoxReadException(operation, "retry delay exceeds request deadline");
        }
        Thread.sleep(delay);
    }

    private Result capture(Response response, String operation, long retryDelay)
            throws IOException {
        ResponseBody body = response.body();
        if (body == null) {
            return new Result(response.code(), new byte[0], null, retryDelay);
        }
        String encoding = response.header("Content-Encoding");
        if (encoding != null && !"identity".equalsIgnoreCase(encoding)) {
            throw new BoxReadException(operation, "unsupported response compression");
        }
        if (body.contentLength() > config.maxBytes) {
            throw new BoxReadException(operation, "response exceeds max_response_bytes");
        }
        try (InputStream input = body.byteStream();
                ByteArrayOutputStream contents = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[8192];
            for (int count; (count = input.read(buffer)) != -1; ) {
                if (count > config.maxBytes - contents.size()) {
                    throw new BoxReadException(operation, "response exceeds max_response_bytes");
                }
                contents.write(buffer, 0, count);
            }
            return new Result(
                    response.code(), contents.toByteArray(), body.contentType(), retryDelay);
        }
    }

    private synchronized Call begin(Request request, String operation, long expires)
            throws BoxReadException {
        if (closed) {
            throw new BoxReadException(operation, "HTTPS request was cancelled");
        }
        active = client.newCall(request);
        active.timeout().deadlineNanoTime(expires);
        return active;
    }

    private synchronized void end(Call call) {
        if (active == call) {
            active = null;
        }
    }

    /**
     * Cancels the current connection and releases owned HTTP resources.
     *
     * <p>This monitor never holds the engine collector lock.
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
        client.connectionPool().evictAll();
        client.dispatcher().executorService().shutdownNow();
    }

    private static void configureTrust(OkHttpClient.Builder builder, String pem)
            throws BoxReadException {
        try {
            Path path = Path.of(pem);
            if (!Files.isRegularFile(path)) {
                throw new BoxReadException("TLS", "trust_certificate is not a regular file");
            }
            byte[] bytes;
            try (InputStream file = Files.newInputStream(path)) {
                bytes = file.readNBytes(MAX_CERTIFICATE_BYTES + 1);
            }
            Collection<? extends Certificate> anchors =
                    CertificateFactory.getInstance("X.509")
                            .generateCertificates(new ByteArrayInputStream(bytes));
            if (bytes.length == 0
                    || bytes.length > MAX_CERTIFICATE_BYTES
                    || anchors.isEmpty()
                    || anchors.size() > 16) {
                throw new BoxReadException(
                        "TLS", "trust_certificate must contain a bounded X.509 chain");
            }
            KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());
            store.load(null);
            int index = 0;
            for (Certificate anchor : anchors) {
                store.setCertificateEntry("cert-" + index++, anchor);
            }
            TrustManagerFactory factory =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            factory.init(store);
            for (TrustManager trust : factory.getTrustManagers()) {
                if (trust instanceof X509TrustManager) {
                    X509TrustManager manager = (X509TrustManager) trust;
                    SSLContext ssl = SSLContext.getInstance("TLS");
                    ssl.init(null, new TrustManager[] {manager}, null);
                    builder.sslSocketFactory(ssl.getSocketFactory(), manager);
                    return;
                }
            }
        } catch (BoxReadException failure) {
            throw failure;
        } catch (IOException | GeneralSecurityException | RuntimeException failure) {
            throw new BoxReadException("TLS", "could not load trust_certificate");
        }
        throw new BoxReadException("TLS", "could not establish X.509 trust");
    }

    private static long retryAfter(String value) {
        if (value == null || value.length() > 96) {
            return -1L;
        }
        try {
            if (value.matches("[0-9]+")) {
                return Math.multiplyExact(Long.parseLong(value), 1000L);
            }
            return Math.max(
                    0L,
                    Duration.between(
                                    Instant.now(),
                                    ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME)
                                            .toInstant())
                            .toMillis());
        } catch (NumberFormatException | ArithmeticException overflow) {
            return Long.MAX_VALUE;
        } catch (RuntimeException invalid) {
            return -1L;
        }
    }
}
