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

package org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner;

import okhttp3.Call;
import okhttp3.ConnectionPool;
import okhttp3.Dns;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.ProtocolException;
import java.net.Proxy;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Reader-scoped HTTPS client with bounded attempts, transfer size and deadline.
 *
 * <p>Document requests use a separate client. Redirects, implicit retries, cookies and ambient
 * proxies cannot carry OAuth or report download URLs to a different service.
 */
final class SpTransport implements AutoCloseable {
    private static final Set<Integer> TRANSIENT = Set.of(408, 429, 500, 502, 503, 504);
    private static final int MAX_ERROR_BYTES = 32768;
    private static final int MAX_PEM_BYTES = 65536;
    private final SpSettings settings;
    private final OkHttpClient api;
    private final OkHttpClient downloads;
    private boolean closed;
    private Call active;

    /**
     * Specifies whether an unsuccessful call is safe to repeat.
     *
     * <p>Report submission has an external side effect. A transport failure or server error could
     * occur after Amazon accepted it.
     */
    enum Safety {
        READ,
        TOKEN,
        CREATE_REPORT
    }

    SpTransport(SpSettings settings) throws IOException {
        this.settings = settings;
        api = builder().build();
        OkHttpClient.Builder noAuth = builder();
        if (settings.customOrigin == null) {
            noAuth.dns(SpTransport::publicAddresses);
        }
        downloads = noAuth.build();
    }

    private OkHttpClient.Builder builder() throws IOException {
        OkHttpClient.Builder builder =
                new OkHttpClient.Builder()
                        .protocols(List.of(Protocol.HTTP_1_1))
                        .proxy(Proxy.NO_PROXY)
                        .followRedirects(false)
                        .followSslRedirects(false)
                        .retryOnConnectionFailure(false)
                        .connectionPool(new ConnectionPool(1, 1L, TimeUnit.MINUTES))
                        .connectTimeout(settings.requestTimeout, TimeUnit.MILLISECONDS)
                        .readTimeout(settings.requestTimeout, TimeUnit.MILLISECONDS)
                        .callTimeout(settings.requestTimeout, TimeUnit.MILLISECONDS)
                        .addNetworkInterceptor(
                                chain -> {
                                    Response response = chain.proceed(chain.request());
                                    if (response.code() == 503) {
                                        // OkHttp otherwise performs an automatic 503 follow-up for
                                        // Retry-After: 0.
                                        long delay = retryAfter(response.header("Retry-After"));
                                        response.close();
                                        throw new ServiceUnavailable(delay);
                                    }
                                    return response;
                                });
        if (settings.trustCertificate != null) {
            pinnedTls(builder, settings.trustCertificate);
        }
        return builder;
    }

    Reply exchange(
            String operation, Request request, Safety safety, boolean document, long deadline)
            throws IOException, InterruptedException {
        int retries429 = 0;
        int lastStatus = 0;
        for (int attempt = 1; attempt <= settings.maxAttempts; attempt++) {
            requireTime(operation, deadline);
            long delay = 200L << (attempt - 1);
            Call call = begin(request, document, deadline);
            try (Response response = call.execute()) {
                int status = response.code();
                lastStatus = status;
                if (status >= 200 && status < 300) {
                    return new Reply(
                            status,
                            read(response, settings.maxResponseBytes, operation, true),
                            response.header("Content-Type"));
                }
                if (safety == Safety.CREATE_REPORT && status >= 500) {
                    throw new SpFailure(operation, "report creation outcome is unknown");
                }
                if (!TRANSIENT.contains(status)
                        || (safety == Safety.CREATE_REPORT && status != 429)) {
                    byte[] body =
                            document
                                    ? new byte[0]
                                    : read(response, MAX_ERROR_BYTES, operation, false);
                    return new Reply(status, body, null);
                }
                if (status == 429
                        && ((settings.stopThrottled && settings.resource.isReport())
                                || retries429++ >= settings.rateRetries)) {
                    throw new SpFailure(
                            operation, "HTTP 429 exhausted the configured rate limit budget");
                }
                delay = Math.max(delay, retryAfter(response.header("Retry-After")));
            } catch (ServiceUnavailable e) {
                lastStatus = 503;
                if (safety == Safety.CREATE_REPORT) {
                    throw new SpFailure(operation, "report creation outcome is unknown");
                }
                delay = Math.max(delay, e.delay);
            } catch (SpFailure e) {
                throw e;
            } catch (SSLException | ProtocolException e) {
                if (safety == Safety.CREATE_REPORT) {
                    throw new SpFailure(
                            operation, "TLS or protocol failure during report creation");
                }
                throw new SpFailure(operation, "TLS trust or HTTP protocol validation failed");
            } catch (IOException e) {
                if (safety == Safety.CREATE_REPORT) {
                    throw new SpFailure(operation, "report creation outcome is unknown");
                }
                if (call.isCanceled() || Thread.currentThread().isInterrupted()) {
                    requireTime(operation, deadline);
                    throw new SpFailure(operation, "request was cancelled");
                }
                lastStatus = 0;
            } finally {
                done(call);
            }
            if (attempt < settings.maxAttempts) {
                if (delay >= TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())) {
                    throw new SpFailure(operation, "retry delay exceeds the request deadline");
                }
                Thread.sleep(delay);
            }
        }
        throw new SpFailure(operation, lastStatus == 0 ? "transport failed" : "HTTP " + lastStatus);
    }

    static void requireTime(String operation, long deadline) throws SpFailure {
        if (System.nanoTime() - deadline >= 0L) {
            throw new SpFailure(operation, "request deadline exceeded");
        }
    }

    private static byte[] read(Response response, int limit, String operation, boolean strict)
            throws IOException {
        ResponseBody body = response.body();
        if (body == null) {
            if (strict) {
                throw new SpFailure(operation, "missing response body");
            }
            return new byte[0];
        }
        String encoding = response.header("Content-Encoding");
        if (strict && encoding != null && !"identity".equalsIgnoreCase(encoding)) {
            throw new SpFailure(operation, "unsupported HTTP content encoding");
        }
        if (body.contentLength() > limit) {
            if (strict) {
                throw new SpFailure(operation, "response exceeds its byte limit");
            }
            return new byte[0];
        }
        try (InputStream input = body.byteStream()) {
            byte[] bytes = input.readNBytes(limit + 1);
            if (bytes.length <= limit) {
                return bytes;
            }
            if (strict) {
                throw new SpFailure(operation, "response exceeds its byte limit");
            }
            return new byte[0];
        }
    }

    private synchronized Call begin(Request request, boolean document, long deadline)
            throws IOException {
        if (closed) {
            throw new SpFailure("HTTPS", "reader was stopped");
        }
        active = (document ? downloads : api).newCall(request);
        active.timeout().deadlineNanoTime(deadline);
        return active;
    }

    private synchronized void done(Call call) {
        if (active == call) {
            active = null;
        }
    }

    static HttpUrl endpoint(String origin, String path) {
        HttpUrl.Builder address = HttpUrl.get(origin).newBuilder();
        for (String segment : path.split("/")) {
            address.addPathSegment(segment);
        }
        return address.build();
    }

    private static long retryAfter(String header) {
        if (header == null) {
            return 0L;
        }
        try {
            long seconds = Long.parseLong(header.trim());
            return seconds <= 0L ? 0L : seconds <= 300L ? seconds * 1000L : Long.MAX_VALUE;
        } catch (NumberFormatException ignored) {
            try {
                Duration wait =
                        Duration.between(
                                Instant.now(),
                                ZonedDateTime.parse(
                                                header.trim(), DateTimeFormatter.RFC_1123_DATE_TIME)
                                        .toInstant());
                if (wait.isNegative()) {
                    return 0L;
                }
                return wait.getSeconds() > 300L ? Long.MAX_VALUE : wait.toMillis() + 1L;
            } catch (DateTimeException e) {
                return 0L;
            }
        }
    }

    private static List<InetAddress> publicAddresses(String host) throws UnknownHostException {
        List<InetAddress> addresses = Dns.SYSTEM.lookup(host);
        for (InetAddress address : addresses) {
            byte[] bytes = address.getAddress();
            int first = bytes[0] & 255;
            int second = bytes[1] & 255;
            boolean blockedRange =
                    address instanceof Inet6Address
                            ? (first & 0xe0) != 0x20
                            : first == 0
                                    || first >= 224
                                    || (first == 100 && second >= 64 && second <= 127)
                                    || (first == 198 && (second == 18 || second == 19));
            if (blockedRange
                    || address.isAnyLocalAddress()
                    || address.isLoopbackAddress()
                    || address.isLinkLocalAddress()
                    || address.isSiteLocalAddress()
                    || address.isMulticastAddress()) {
                throw new UnknownHostException("report document resolved to a non-public address");
            }
        }
        return addresses;
    }

    private static void pinnedTls(OkHttpClient.Builder builder, String file) throws IOException {
        try {
            Path path = Path.of(file);
            if (!Files.isRegularFile(path)) {
                throw new SpFailure("TLS", "trust_certificate must be a regular file");
            }
            byte[] pem;
            try (InputStream in = Files.newInputStream(path)) {
                pem = in.readNBytes(MAX_PEM_BYTES + 1);
            }
            if (pem.length > MAX_PEM_BYTES) {
                throw new SpFailure("TLS", "trust_certificate exceeds its byte limit");
            }
            Collection<? extends Certificate> parsed =
                    CertificateFactory.getInstance("X.509")
                            .generateCertificates(new ByteArrayInputStream(pem));
            if (parsed.size() != 1 || !(parsed.iterator().next() instanceof X509Certificate)) {
                throw new SpFailure("TLS", "one X.509 trust certificate is required");
            }
            X509Certificate anchor = (X509Certificate) parsed.iterator().next();
            anchor.checkValidity();
            KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());
            store.load(null, null);
            store.setCertificateEntry("sandbox", anchor);
            TrustManagerFactory trust =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trust.init(store);
            X509TrustManager manager = (X509TrustManager) trust.getTrustManagers()[0];
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, new X509TrustManager[] {manager}, null);
            builder.sslSocketFactory(context.getSocketFactory(), manager);
        } catch (SpFailure e) {
            throw e;
        } catch (Exception e) {
            throw new SpFailure("TLS", "could not load or validate the sandbox trust certificate");
        }
    }

    /**
     * Aborts any active transfer and prevents another connection for this reader.
     *
     * <p>The engine cancellation path calls this without holding the checkpoint lock.
     */
    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;
            if (active != null) {
                active.cancel();
            }
            api.connectionPool().evictAll();
            downloads.connectionPool().evictAll();
        }
    }

    /**
     * A bounded response at the HTTP boundary.
     *
     * <p>The caller interprets status and response shape before a page can reach the reader.
     */
    static final class Reply {
        final int status;
        final byte[] body;
        final String contentType;

        private Reply(int status, byte[] body, String contentType) {
            this.status = status;
            this.body = body;
            this.contentType = contentType;
        }
    }

    /**
     * Carries a provider 503 past the HTTP client's automatic follow-up handling.
     *
     * <p>This keeps attempt counting and ambiguous report creation handling in one place.
     */
    private static final class ServiceUnavailable extends IOException {
        private static final long serialVersionUID = 1L;
        private final long delay;

        private ServiceUnavailable(long delay) {
            this.delay = delay;
        }
    }
}
