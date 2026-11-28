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

package org.apache.seatunnel.connectors.seatunnel.ashby;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import okhttp3.Call;
import okhttp3.ConnectionPool;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
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
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.connectors.seatunnel.ashby.AshbyResource.APPLICATION_CRITERIA_EVALUATIONS;
import static org.apache.seatunnel.connectors.seatunnel.ashby.AshbyResource.APPLICATION_HISTORY;

/**
 * Owns Basic authentication, HTTPS verification and cancellable read-only RPC calls.
 *
 * <p>Retries can only reach the connector's enumerated read methods. A shared request deadline
 * covers pacing, bounded reads and all retry attempts.
 */
final class AshbyHttp implements AutoCloseable {
    private static final MediaType JSON = MediaType.get("application/json");
    private static final int ERROR_BYTES = 16384;
    private static final int PEM_BYTES = 65536;
    private final AshbySettings settings;
    private final OkHttpClient client;
    private final String authorization;
    // The synchronized lifecycle covers acquisition before an engine cancellation can intervene.
    private Call active;
    private boolean closed;
    // The single acquisition thread owns the pacing timestamp across attempts and RPCs.
    private long nextApplicationChild;

    AshbyHttp(AshbySettings settings) throws AshbyFailure {
        this.settings = settings;
        authorization =
                "Basic "
                        + Base64.getEncoder()
                                .encodeToString(
                                        (settings.token + ":").getBytes(StandardCharsets.UTF_8));
        OkHttpClient.Builder builder =
                new OkHttpClient.Builder()
                        .proxy(Proxy.NO_PROXY)
                        .protocols(List.of(Protocol.HTTP_1_1))
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
                                        long retry = retryAfter(response.header("Retry-After"));
                                        response.close();
                                        // A 503 with Retry-After: 0 would otherwise bypass this
                                        // attempt budget.
                                        throw new TemporarilyUnavailable(retry);
                                    }
                                    return response;
                                });
        if (settings.trustCertificate != null) {
            configureTrust(builder);
        }
        client = builder.build();
    }

    /**
     * Reads a provider page with a shared deadline for response consumption and every retry.
     *
     * <p>The caller supplies that deadline so pacing and decoding stay in the same budget.
     */
    JsonNode post(AshbyResource target, ObjectNode arguments, long deadline)
            throws IOException, InterruptedException {
        Request request =
                new Request.Builder()
                        .url(settings.origin + "/" + target.rpc)
                        .header("Accept", "application/json")
                        .header("User-Agent", "Apache-SeaTunnel-Ashby/1.0")
                        .header("Authorization", authorization)
                        .post(RequestBody.create(AshbyJson.encode(arguments, target), JSON))
                        .build();
        int status = 0;
        for (int attempt = 0; attempt < settings.maxAttempts; attempt++) {
            time(target, deadline);
            pace(target, deadline);
            Call call = begin(target, request, deadline);
            long delay = 200L << attempt;
            try (Response response = call.execute()) {
                status = response.code();
                if (status == 200) {
                    return AshbyJson.envelope(
                            bytes(target, response, settings.maxBytes, true), status, target);
                }
                if (status != 408 && status != 429 && status < 500) {
                    return AshbyJson.envelope(
                            bytes(target, response, ERROR_BYTES, false), status, target);
                }
                delay = Math.max(delay, retryAfter(response.header("Retry-After")));
            } catch (TemporarilyUnavailable e) {
                status = 503;
                delay = Math.max(delay, e.delay);
            } catch (AshbyFailure e) {
                throw e;
            } catch (SSLException e) {
                throw new AshbyFailure(target, "HTTPS certificate or hostname validation failed");
            } catch (IOException e) {
                if (call.isCanceled() || Thread.currentThread().isInterrupted()) {
                    time(target, deadline);
                    throw new AshbyFailure(target, "HTTPS request was cancelled");
                }
                status = 0;
            } finally {
                finish(call);
            }
            if (attempt + 1 < settings.maxAttempts) {
                pause(target, delay, deadline);
            }
        }
        throw new AshbyFailure(
                target,
                status == 0
                        ? "transport retry budget exhausted"
                        : "HTTP " + status + " retry budget exhausted");
    }

    private void pace(AshbyResource target, long deadline)
            throws AshbyFailure, InterruptedException {
        if (target != APPLICATION_HISTORY && target != APPLICATION_CRITERIA_EVALUATIONS) {
            return;
        }
        long remaining = nextApplicationChild - System.nanoTime();
        if (nextApplicationChild != 0L && remaining > 0L) {
            pause(target, TimeUnit.NANOSECONDS.toMillis(remaining) + 1L, deadline);
        }
        nextApplicationChild =
                System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(settings.childInterval);
    }

    private static void pause(AshbyResource target, long delay, long deadline)
            throws AshbyFailure, InterruptedException {
        if (delay >= TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())) {
            throw new AshbyFailure(target, "wait exceeds the request deadline");
        }
        Thread.sleep(delay);
    }

    private static byte[] bytes(
            AshbyResource target, Response response, int limit, boolean required)
            throws IOException {
        ResponseBody body = response.body();
        if (body == null || body.contentLength() > limit) {
            if (required) {
                throw new AshbyFailure(target, "missing or oversized response body");
            }
            return new byte[0];
        }
        String encoding = response.header("Content-Encoding");
        if (required && encoding != null && !"identity".equalsIgnoreCase(encoding)) {
            throw new AshbyFailure(target, "unsupported response encoding");
        }
        try (InputStream stream = body.byteStream()) {
            byte[] data = stream.readNBytes(limit + 1);
            if (data.length <= limit) {
                return data;
            }
            if (required) {
                throw new AshbyFailure(target, "response exceeds max_response_bytes");
            }
            return new byte[0];
        }
    }

    private synchronized Call begin(AshbyResource target, Request request, long deadline)
            throws AshbyFailure {
        if (closed) {
            throw new AshbyFailure(target, "HTTPS request was cancelled");
        }
        active = client.newCall(request);
        active.timeout().deadlineNanoTime(deadline);
        return active;
    }

    private synchronized void finish(Call call) {
        if (active == call) {
            active = null;
        }
    }

    static void time(AshbyResource target, long deadline) throws AshbyFailure {
        if (System.nanoTime() - deadline >= 0L) {
            throw new AshbyFailure(target, "request deadline exceeded");
        }
    }

    /**
     * Aborts active I/O and releases this reader's client without altering JVM-wide trust.
     *
     * <p>Close can run while the acquisition worker is blocked in a response read.
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
        client.dispatcher().executorService().shutdownNow();
        client.connectionPool().evictAll();
    }

    private void configureTrust(OkHttpClient.Builder builder) throws AshbyFailure {
        try {
            Path path = Path.of(settings.trustCertificate);
            if (!Files.isRegularFile(path)) {
                throw new AshbyFailure(
                        settings.resource, "trust_certificate must be a regular file");
            }
            byte[] pem;
            try (InputStream input = Files.newInputStream(path)) {
                pem = input.readNBytes(PEM_BYTES + 1);
            }
            if (pem.length == 0 || pem.length > PEM_BYTES) {
                throw new AshbyFailure(settings.resource, "trust_certificate has an invalid size");
            }
            Collection<? extends Certificate> anchors =
                    CertificateFactory.getInstance("X.509")
                            .generateCertificates(new ByteArrayInputStream(pem));
            if (anchors.isEmpty() || anchors.size() > 16) {
                throw new AshbyFailure(settings.resource, "invalid number of trust anchors");
            }
            KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());
            store.load(null, null);
            int index = 0;
            for (Certificate anchor : anchors) {
                store.setCertificateEntry("anchor-" + index++, anchor);
            }
            TrustManagerFactory factory =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            factory.init(store);
            for (TrustManager manager : factory.getTrustManagers()) {
                if (manager instanceof X509TrustManager) {
                    X509TrustManager trust = (X509TrustManager) manager;
                    SSLContext ssl = SSLContext.getInstance("TLS");
                    ssl.init(null, new TrustManager[] {trust}, null);
                    builder.sslSocketFactory(ssl.getSocketFactory(), trust);
                    return;
                }
            }
        } catch (AshbyFailure e) {
            throw e;
        } catch (IOException | GeneralSecurityException | IllegalArgumentException e) {
            throw new AshbyFailure(settings.resource, "could not load trust_certificate");
        }
        throw new AshbyFailure(settings.resource, "no X.509 trust manager is available");
    }

    private static long retryAfter(String value) {
        if (value == null || value.isEmpty()) {
            return 0L;
        }
        if (value.chars().allMatch(Character::isDigit)) {
            try {
                return TimeUnit.SECONDS.toMillis(Long.parseLong(value));
            } catch (NumberFormatException e) {
                return Long.MAX_VALUE;
            }
        }
        if (value.length() <= 100) {
            try {
                long delay =
                        ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME)
                                        .toInstant()
                                        .toEpochMilli()
                                - Instant.now().toEpochMilli();
                return Math.max(delay, 0L);
            } catch (DateTimeException | ArithmeticException ignored) {
                // Keep the connector's local backoff when the provider hint is unusable.
            }
        }
        return 0L;
    }

    /**
     * Carries the retry delay for a response intercepted before automatic follow-up.
     *
     * <p>The originating response and authorization header stay private to the client.
     */
    private static final class TemporarilyUnavailable extends IOException {
        private static final long serialVersionUID = 1L;
        private final long delay;

        private TemporarilyUnavailable(long delay) {
            this.delay = delay;
        }
    }
}
