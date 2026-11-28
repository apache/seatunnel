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
package org.apache.seatunnel.connectors.seatunnel.bluetally;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

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
import java.util.concurrent.TimeUnit;

/**
 * Client-local TLS, authentication and retry handling for the configured BlueTally origin.
 *
 * <p>Only source-built GETs are sent. Cancellation releases the active request and excludes later
 * acquisitions without waiting for a response to be read.
 */
final class BlueTallyTransport implements AutoCloseable {
    private static final int MAX_PEM_BYTES = 65536;
    private final BlueTallyConfig options;
    private final HttpUrl origin;
    private final OkHttpClient client;

    // This monitor links cancellation to the current OkHttp call.
    private Call running;
    private boolean closed;

    BlueTallyTransport(BlueTallyConfig options, BlueTallyResource resource, int offset)
            throws BlueTallyReadException {
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
                                    // OkHttp automatically reissues this particular response even
                                    // with retries off.
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
            loadTrust(builder, resource, offset);
        }
        client = builder.build();
    }

    /**
     * Requests one absolute offset in the selected BlueTally resource.
     *
     * <p>Both list APIs accept identical page and sort parameters. The tenant stays fixed across
     * all retries and checkpoints.
     */
    JsonNode get(BlueTallyResource resource, int offset, long expires)
            throws BlueTallyReadException, InterruptedException {
        HttpUrl.Builder target =
                origin.newBuilder()
                        .addPathSegment("api")
                        .addPathSegment("v1")
                        .addPathSegment(resource.path)
                        .addQueryParameter("limit", Integer.toString(options.pageSize))
                        .addQueryParameter("offset", Integer.toString(offset))
                        .addQueryParameter("sort", "updated_at")
                        .addQueryParameter("order", "desc");
        if (options.tenant != null) {
            target.addQueryParameter("tenant_id", options.tenant.toString());
        }
        HttpUrl url = target.build();
        Request request =
                new Request.Builder()
                        .get()
                        .url(url)
                        .header("Authorization", "Bearer " + options.token)
                        .header("Accept", "application/json")
                        .header("User-Agent", "Apache-SeaTunnel-BlueTally/1.0")
                        .build();

        String exhausted = "transient HTTPS retry budget exhausted";
        for (int attempt = 0; attempt < options.attempts; attempt++) {
            deadline(resource, offset, expires);
            Call call = begin(request, resource, offset, expires);
            long delay = 200L << attempt;
            try (Response response = call.execute()) {
                int status = response.code();
                if (status == 200) {
                    return responseJson(response, resource, offset);
                }

                long requestedDelay = retryAfter(response.header("Retry-After"));
                if (status == 429 && requestedDelay <= 0L) {
                    throw new BlueTallyReadException(
                            resource, offset, "HTTP 429 without a confirmed retry delay");
                }
                if (status != 429
                        && status != 408
                        && status != 500
                        && status != 502
                        && status != 503
                        && status != 504) {
                    throw new BlueTallyReadException(
                            resource,
                            offset,
                            "provider rejected the request (HTTP " + status + ")");
                }
                delay = Math.max(delay, requestedDelay);
                exhausted = "HTTP " + status + " retry budget exhausted";
            } catch (BlueTallyReadException failure) {
                throw failure;
            } catch (SSLException failure) {
                throw new BlueTallyReadException(
                        resource, offset, "TLS certificate or hostname validation failed");
            } catch (IOException failure) {
                deadline(resource, offset, expires);
                if (call.isCanceled() || Thread.currentThread().isInterrupted()) {
                    throw new BlueTallyReadException(
                            resource, offset, "HTTPS request was cancelled");
                }
                exhausted = "transient HTTPS retry budget exhausted";
            } finally {
                released(call);
            }
            if (attempt + 1 < options.attempts) {
                if (delay >= TimeUnit.NANOSECONDS.toMillis(expires - System.nanoTime())) {
                    throw new BlueTallyReadException(
                            resource, offset, "retry delay exceeds request deadline");
                }
                Thread.sleep(delay);
            }
        }
        throw new BlueTallyReadException(resource, offset, exhausted);
    }

    private JsonNode responseJson(Response response, BlueTallyResource resource, int offset)
            throws IOException {
        String encoding = response.header("Content-Encoding");
        if (encoding != null && !"identity".equalsIgnoreCase(encoding)) {
            throw new BlueTallyReadException(resource, offset, "unsupported response compression");
        }
        ResponseBody body = response.body();
        MediaType type = body == null ? null : body.contentType();
        if (type == null || !"application".equals(type.type()) || !"json".equals(type.subtype())) {
            throw new BlueTallyReadException(
                    resource, offset, "expected Content-Type application/json");
        }
        return BlueTallyPage.parse(
                readBody(body, options.maxBytes, resource, offset), resource, offset);
    }

    private static byte[] readBody(
            ResponseBody body, int maximum, BlueTallyResource resource, int offset)
            throws IOException {
        if (body == null) {
            throw new BlueTallyReadException(resource, offset, "provider response has no body");
        }
        if (body.contentLength() > maximum) {
            throw new BlueTallyReadException(
                    resource, offset, "response exceeds max_response_bytes");
        }
        try (InputStream bytes = body.byteStream();
                ByteArrayOutputStream captured = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[8192];
            for (int n; (n = bytes.read(buffer)) >= 0; ) {
                if (n > maximum - captured.size()) {
                    throw new BlueTallyReadException(
                            resource, offset, "response exceeds max_response_bytes");
                }
                captured.write(buffer, 0, n);
            }
            return captured.toByteArray();
        }
    }

    private synchronized Call begin(
            Request request, BlueTallyResource resource, int offset, long expires)
            throws BlueTallyReadException {
        if (closed) {
            throw new BlueTallyReadException(resource, offset, "HTTPS request was cancelled");
        }
        running = client.newCall(request);
        running.timeout().deadlineNanoTime(expires);
        return running;
    }

    private synchronized void released(Call call) {
        if (running == call) {
            running = null;
        }
    }

    static void deadline(BlueTallyResource resource, int offset, long expires)
            throws BlueTallyReadException {
        if (System.nanoTime() - expires >= 0L) {
            throw new BlueTallyReadException(resource, offset, "request deadline exceeded");
        }
    }

    /**
     * Closes the active transfer and this reader's dedicated connection pool.
     *
     * <p>The connector leaves the JVM trust configuration and other clients alone.
     */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (running != null) {
            running.cancel();
        }
        client.connectionPool().evictAll();
        client.dispatcher().executorService().shutdownNow();
    }

    private void loadTrust(OkHttpClient.Builder builder, BlueTallyResource resource, int offset)
            throws BlueTallyReadException {
        try {
            Path path = Path.of(options.trustCertificate);
            if (!Files.isRegularFile(path)) {
                throw new BlueTallyReadException(
                        resource, offset, "trust_certificate is not a regular file");
            }
            byte[] pem;
            try (InputStream source = Files.newInputStream(path)) {
                pem = source.readNBytes(MAX_PEM_BYTES + 1);
            }
            if (pem.length == 0 || pem.length > MAX_PEM_BYTES) {
                throw new BlueTallyReadException(
                        resource, offset, "trust_certificate has an invalid size");
            }
            Collection<? extends Certificate> certificates =
                    CertificateFactory.getInstance("X.509")
                            .generateCertificates(new ByteArrayInputStream(pem));
            if (certificates.isEmpty() || certificates.size() > 16) {
                throw new BlueTallyReadException(
                        resource, offset, "trust_certificate must contain a bounded X.509 chain");
            }
            KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());
            store.load(null);
            int index = 0;
            for (Certificate cert : certificates) {
                store.setCertificateEntry("ca-" + index++, cert);
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
        } catch (BlueTallyReadException failure) {
            throw failure;
        } catch (IOException | GeneralSecurityException | RuntimeException failure) {
            throw new BlueTallyReadException(resource, offset, "could not load trust_certificate");
        }
        throw new BlueTallyReadException(resource, offset, "could not establish X.509 trust");
    }

    private static long retryAfter(String value) {
        if (value == null || value.length() > 96) {
            return 0L;
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
        } catch (ArithmeticException | NumberFormatException overflow) {
            return Long.MAX_VALUE;
        } catch (RuntimeException invalid) {
            return 0L;
        }
    }
}
