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

package org.apache.seatunnel.connectors.seatunnel.azureiothub;

import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.utils.DateUtils;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.MessageConstraints;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.DefaultBHttpClientConnection;
import org.apache.http.impl.DefaultBHttpClientConnectionFactory;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.http.protocol.HttpRequestExecutor;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Issues synchronous device events using one owned, daemon network executor. Only HTTP 204 confirms
 * delivery; a transport failure leaves the mutation outcome unknown and must not trigger replay.
 */
final class AzureIoTHubClient implements AutoCloseable {
    // Bound response line/header admission before accepting a peer-controlled HTTP response.
    private static final DefaultBHttpClientConnectionFactory CONNECTIONS =
            new DefaultBHttpClientConnectionFactory(
                    ConnectionConfig.custom()
                            .setMessageConstraints(
                                    MessageConstraints.custom()
                                            .setMaxHeaderCount(64)
                                            .setMaxLineLength(8192)
                                            .build())
                            .build());

    private final AzureIoTHubConfig config;
    // The writer admits one send at a time; this executor never accumulates a row queue.
    private final ExecutorService network =
            Executors.newSingleThreadExecutor(
                    task -> {
                        Thread thread = new Thread(task, "azure-iot-hub-telemetry");
                        thread.setDaemon(true);
                        return thread;
                    });
    // Sticky close wins races with submit/socket publication and forbids later attempts.
    private final AtomicBoolean closed = new AtomicBoolean();
    // Publish the raw socket before connect so deadline/close can interrupt every transport phase.
    private final AtomicReference<Socket> socket = new AtomicReference<>();
    // The caller owns this pending operation until completion; close cancels it without a monitor.
    private final AtomicReference<Future<?>> operation = new AtomicReference<>();

    AzureIoTHubClient(AzureIoTHubConfig config) {
        this.config = config;
    }

    /**
     * Waits within one total I/O deadline. A successful return means HTTP 204 acknowledged the row;
     * timeout/interruption closes transport without attempting to replay an unknown mutation.
     */
    void send(byte[] payload) throws IOException {
        checkOpen();
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.rowTimeout);
        Future<?> future;
        try {
            future =
                    network.submit(
                            () -> {
                                sendWithThrottleRetry(payload, deadline);
                                return null;
                            });
        } catch (RejectedExecutionException e) {
            throw failure("telemetry writer is closed");
        }
        operation.set(future);
        if (closed.get()) {
            future.cancel(true);
        }
        try {
            future.get(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            close();
            throw failure("row I/O deadline exceeded; delivery outcome can be unknown");
        } catch (InterruptedException e) {
            close();
            Thread.currentThread().interrupt();
            throw failure("telemetry send interrupted; delivery outcome can be unknown");
        } catch (CancellationException e) {
            throw failure("telemetry send canceled; delivery outcome can be unknown");
        } catch (ExecutionException e) {
            if (e.getCause() instanceof Failure) {
                throw (Failure) e.getCause();
            }
            throw failure("telemetry send failed; delivery outcome can be unknown");
        } finally {
            operation.compareAndSet(future, null);
        }
    }

    static Failure failure(String detail) {
        return new Failure("Azure IoT Hub sink: " + detail);
    }

    /*
     * Only a completed 429 explicitly rejects this attempt and permits a bounded later attempt.
     * Missing/unusable timing uses finite jittered backoff; a valid hint is a minimum delay.
     */
    /**
     * Idempotently aborts the published transport and operation without waiting on the network
     * executor itself. The socket owner also closes its resources on every exit path.
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            Socket current = socket.getAndSet(null);
            if (current != null) {
                try {
                    current.close();
                } catch (IOException ignored) {
                    // The owning network task also closes the socket.
                }
            }
            Future<?> pending = operation.getAndSet(null);
            if (pending != null) {
                pending.cancel(true);
            }
            network.shutdownNow();
        }
    }

    private void sendWithThrottleRetry(byte[] payload, long deadline) throws IOException {
        for (int attempt = 1; attempt <= config.attempts; attempt++) {
            checkOpen();
            long minimumDelay = request(payload, deadline);
            if (minimumDelay < 0) {
                return;
            }
            if (attempt == config.attempts) {
                throw failure("HTTP 429 throttle attempt limit exhausted");
            }
            long base = Math.min(10000L, 250L << (attempt - 1));
            long delay =
                    Math.max(minimumDelay, base + ThreadLocalRandom.current().nextLong(base + 1));
            if (delay >= TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())) {
                throw failure("HTTP 429 retry delay exceeds the remaining row I/O budget");
            }
            try {
                TimeUnit.MILLISECONDS.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw failure("telemetry retry canceled");
            }
        }
    }

    /**
     * Owns one TCP/TLS attempt under the caller deadline. Returns -1 only for a valid HTTP 204;
     * otherwise returns the minimum wait explicitly authorized by HTTP 429.
     */
    private long request(byte[] payload, long deadline) throws IOException {
        Socket raw = new Socket();
        socket.set(raw);
        try (Socket connection = raw) {
            checkOpen();
            String host = config.origin.getHost();
            connection.connect(
                    new InetSocketAddress(host, config.port()),
                    remainingTimeout(deadline, config.connectTimeout));
            connection.setSoTimeout(remainingTimeout(deadline, config.readTimeout));
            SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory.getDefault();
            try (SSLSocket tls =
                    (SSLSocket) factory.createSocket(connection, host, config.port(), true)) {
                SSLParameters parameters = tls.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                parameters.setProtocols(
                        Arrays.stream(tls.getEnabledProtocols())
                                .filter(
                                        protocol ->
                                                "TLSv1.2".equals(protocol)
                                                        || "TLSv1.3".equals(protocol))
                                .toArray(String[]::new));
                tls.setSSLParameters(parameters);
                tls.startHandshake();
                checkOpen();
                tls.setSoTimeout(remainingTimeout(deadline, config.readTimeout));
                return sendRequest(tls, payload);
            }
        } catch (Failure e) {
            throw e;
        } catch (SSLException e) {
            throw failure("TLS transport failed; no confirmed HTTP 204");
        } catch (HttpException e) {
            throw failure("malformed HTTP response; no confirmed HTTP 204");
        } catch (IOException e) {
            throw failure("transport ended without HTTP 204; delivery outcome can be unknown");
        } finally {
            socket.compareAndSet(raw, null);
        }
    }

    /*
     * The low-level executor has no automatic retry, redirect or wire-logging interceptor. Response
     * bodies are never retained; 204 framing and 429 timing are validated before acknowledging.
     */
    private long sendRequest(Socket tls, byte[] payload) throws IOException, HttpException {
        DefaultBHttpClientConnection connection = CONNECTIONS.createConnection(tls);
        try {
            BasicHttpEntityEnclosingRequest message =
                    new BasicHttpEntityEnclosingRequest(
                            "POST", config.target, HttpVersion.HTTP_1_1);
            message.setHeader("Host", config.origin.getRawAuthority());
            message.setHeader("Authorization", authorization());
            message.setHeader("Connection", "close");
            message.setHeader(
                    "Content-Type",
                    config.format.type
                            + (config.format.encoding == null
                                    ? ""
                                    : ";charset=" + config.format.encoding));
            message.setHeader("Content-Length", Integer.toString(payload.length));
            message.setHeader("iothub-contenttype", config.format.type);
            if (config.format.encoding != null) {
                message.setHeader("iothub-contentencoding", config.format.encoding);
            }
            config.properties.forEach(
                    (name, value) -> message.setHeader("iothub-app-" + name, value));
            message.setEntity(new ByteArrayEntity(payload));
            HttpResponse response =
                    new HttpRequestExecutor()
                            .execute(message, connection, HttpCoreContext.create());
            int status = response.getStatusLine().getStatusCode();
            if (status == 204) {
                Header[] lengths = response.getHeaders("Content-Length");
                if (response.containsHeader("Transfer-Encoding")
                        || lengths.length > 1
                        || (lengths.length == 1 && !"0".equals(lengths[0].getValue().trim()))) {
                    throw failure("HTTP 204 has contradictory body framing");
                }
                return -1;
            }
            if (status == 429) {
                Header[] hints = response.getHeaders("Retry-After");
                if (hints.length > 1) {
                    throw failure("HTTP 429 has ambiguous retry timing");
                }
                return hints.length == 0 ? 0 : retryDelay(hints[0].getValue().trim());
            }
            throw failure("HTTP " + status + "; expected HTTP 204 (response details withheld)");
        } finally {
            // Error responses may stream a body indefinitely. Release the socket without reading
            // it.
            try {
                connection.shutdown();
            } catch (IOException ignored) {
                // The owning TLS socket also closes when the attempt exits.
            }
        }
    }

    /*
     * Sign the encoded device resource and fresh expiry with the registered-device symmetric key.
     * Neither the key, signed token nor endpoint response text is attached to a diagnostic cause.
     */
    private String authorization() throws IOException {
        long expires = Instant.now().getEpochSecond() + config.sasTtl;
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(config.key, "HmacSHA256"));
            byte[] hmac =
                    mac.doFinal((config.scope + "\n" + expires).getBytes(StandardCharsets.UTF_8));
            String signature =
                    AzureIoTHubConfig.encode(Base64.getEncoder().encodeToString(hmac), false);
            return "SharedAccessSignature sr="
                    + config.scope
                    + "&sig="
                    + signature
                    + "&se="
                    + expires;
        } catch (GeneralSecurityException e) {
            throw failure("could not sign device credentials");
        }
    }

    // Reject a numeric delay beyond every supported row budget without overflowing its parser.
    private static long retryDelay(String value) {
        if (!value.isEmpty() && value.chars().allMatch(c -> c >= '0' && c <= '9')) {
            int seconds = 0;
            for (int i = 0; i < value.length(); i++) {
                seconds = seconds * 10 + value.charAt(i) - '0';
                if (seconds > 300) {
                    return Long.MAX_VALUE;
                }
            }
            return seconds * 1000L;
        }
        Date retryAt = DateUtils.parseDate(value);
        return retryAt == null ? 0 : Math.max(0L, retryAt.getTime() - System.currentTimeMillis());
    }

    private static int remainingTimeout(long deadline, int limit) throws IOException {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
            throw failure("row I/O deadline exceeded; delivery outcome can be unknown");
        }
        return (int) Math.min(limit, Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)));
    }

    private void checkOpen() throws IOException {
        if (closed.get() || Thread.currentThread().isInterrupted()) {
            throw failure("telemetry writer was closed or interrupted");
        }
    }

    /**
     * Carries only connector-authored safe diagnostics; third-party causes and response text are
     * deliberately excluded because they can contain authentication material.
     */
    static final class Failure extends IOException {
        private static final long serialVersionUID = 1L;

        private Failure(String message) {
            super(message);
        }
    }
}
