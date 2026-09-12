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

package org.apache.seatunnel.connectors.seatunnel.syslog.sink;

import org.apache.seatunnel.connectors.seatunnel.syslog.config.SyslogSinkConfig;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/** One synchronous in-flight operation; errors permanently poison the connection. */
final class SyslogTlsClient implements AutoCloseable {
    private final Socket rawSocket;
    private final SSLSocket tlsSocket;
    private final OutputStream output;
    private final int timeout;
    private final ScheduledThreadPoolExecutor watchdog;
    private final ReentrantLock operationLock = new ReentrantLock();
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile IOException failure;

    static SyslogTlsClient connect(SyslogSinkConfig config) throws IOException {
        SSLContext context = sslContext(config);
        Socket raw = new Socket();
        SyslogTlsClient client = null;
        try {
            // JVM DNS resolution has its own platform limits, outside Socket.connect's deadline.
            raw.connect(
                    new InetSocketAddress(config.getHost(), config.getPort()),
                    config.getConnectTimeout());
            raw.setTcpNoDelay(true);
            raw.setSoTimeout(config.getWriteTimeout());
            SSLSocket tls =
                    (SSLSocket)
                            context.getSocketFactory()
                                    .createSocket(raw, config.getHost(), config.getPort(), true);
            SSLParameters parameters = tls.getSSLParameters();
            parameters.setEndpointIdentificationAlgorithm("HTTPS");
            parameters.setProtocols(
                    Arrays.stream(tls.getSupportedProtocols())
                            .filter(p -> "TLSv1.2".equals(p) || "TLSv1.3".equals(p))
                            .toArray(String[]::new));
            tls.setSSLParameters(parameters);
            client = new SyslogTlsClient(raw, tls, tls.getOutputStream(), config.getWriteTimeout());
            client.execute("TLS handshake", tls::startHandshake);
            return client;
        } catch (IOException | RuntimeException e) {
            if (client != null) {
                client.abort();
            } else {
                try {
                    raw.close();
                } catch (IOException closeError) {
                    e.addSuppressed(closeError);
                }
            }
            throw e;
        }
    }

    // Package-private transport boundary also permits deterministic partial-write/timeout tests.
    SyslogTlsClient(Socket rawSocket, SSLSocket tlsSocket, OutputStream output, int timeout) {
        this.rawSocket = rawSocket;
        this.tlsSocket = tlsSocket;
        this.output = output;
        this.timeout = timeout;
        watchdog =
                new ScheduledThreadPoolExecutor(
                        1,
                        runnable -> {
                            Thread thread = new Thread(runnable, "syslog-sink-write-deadline");
                            thread.setDaemon(true);
                            return thread;
                        });
        watchdog.setRemoveOnCancelPolicy(true);
    }

    void write(byte[] frame) throws IOException {
        execute(
                "write",
                () -> {
                    output.write(frame);
                    output.flush();
                });
    }

    void flush() throws IOException {
        execute("flush", output::flush);
    }

    private void execute(String phase, IoAction action) throws IOException {
        if (!operationLock.tryLock()) {
            throw new IOException("Syslog concurrent transport operations are not supported");
        }
        try {
            checkOpen();
            runBounded(phase, action);
            checkOpen();
        } finally {
            operationLock.unlock();
        }
    }

    private void checkOpen() throws IOException {
        if (failure != null) {
            throw new IOException(
                    "Syslog writer previously failed; delivery is uncertain", failure);
        }
        if (closed.get()) {
            throw new IOException("Syslog writer is closed");
        }
        if (Thread.currentThread().isInterrupted()) {
            failure = new IOException("Syslog operation interrupted");
            abort();
            throw failure;
        }
    }

    private void runBounded(String phase, IoAction action) throws IOException {
        // SO_TIMEOUT only bounds reads. Close the underlying TCP socket, not SSLSocket (which
        // may wait for the blocked TLS write lock), to interrupt a stalled output operation.
        AtomicInteger status = new AtomicInteger(0);
        ScheduledFuture<?> deadline = null;
        try {
            deadline =
                    watchdog.schedule(
                            () -> {
                                if (status.compareAndSet(0, 2)) {
                                    closeRaw();
                                }
                            },
                            timeout,
                            TimeUnit.MILLISECONDS);
            action.run();
            if (!status.compareAndSet(0, 1)) {
                throw new SocketTimeoutException(
                        "Syslog " + phase + " timed out; delivery is uncertain");
            }
            if (Thread.currentThread().isInterrupted()) {
                throw new IOException("Syslog " + phase + " interrupted; delivery is uncertain");
            }
        } catch (IOException | RuntimeException e) {
            boolean timedOut = status.get() == 2 || e instanceof SocketTimeoutException;
            IOException error =
                    timedOut
                            ? new SocketTimeoutException(
                                    "Syslog " + phase + " timed out; delivery is uncertain")
                            : new IOException(
                                    "Syslog " + phase + " failed; delivery is uncertain; no retry",
                                    e);
            if (timedOut) {
                error.initCause(e);
            }
            failure = error;
            abort();
            throw error;
        } finally {
            status.compareAndSet(0, 1);
            if (deadline != null) {
                deadline.cancel(false);
            }
        }
    }

    /** A concurrent close aborts an active write immediately instead of waiting for its lock. */
    @Override
    public void close() throws IOException {
        if (!closed.compareAndSet(false, true)) {
            if (failure != null) {
                throw new IOException(
                        "Syslog writer previously failed; delivery is uncertain", failure);
            }
            return;
        }
        if (!operationLock.tryLock()) {
            failure =
                    new IOException(
                            "Syslog closed during an active operation; delivery is uncertain");
            abort();
            throw failure;
        }
        try {
            if (failure != null) {
                throw new IOException(
                        "Syslog writer previously failed; delivery is uncertain", failure);
            }
            runBounded(
                    "close",
                    () -> {
                        output.flush();
                        tlsSocket.close();
                    });
        } finally {
            abort();
            operationLock.unlock();
        }
        if (failure != null) {
            throw new IOException("Syslog TCP close failed", failure);
        }
    }

    private void abort() {
        closed.set(true);
        closeRaw();
        watchdog.shutdownNow();
    }

    private void closeRaw() {
        try {
            rawSocket.close();
        } catch (IOException e) {
            if (failure == null) {
                failure = new IOException("Syslog TCP close failed", e);
            }
        }
    }

    private static SSLContext sslContext(SyslogSinkConfig config) throws IOException {
        try {
            KeyStore trustStore = null;
            if (config.getCaCert() != null) {
                trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
                trustStore.load(null, null);
                try (InputStream input = Files.newInputStream(Paths.get(config.getCaCert()))) {
                    Collection<? extends Certificate> certificates =
                            CertificateFactory.getInstance("X.509").generateCertificates(input);
                    if (certificates.isEmpty()) {
                        throw new IOException("Syslog tls.ca_cert_path contains no certificates");
                    }
                    int index = 0;
                    for (Certificate certificate : certificates) {
                        trustStore.setCertificateEntry("ca-" + index++, certificate);
                    }
                }
            }
            TrustManagerFactory trust =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trust.init(trustStore);
            KeyManager[] keys = null;
            if (config.getKeyStore() != null) {
                char[] password = config.getKeyStorePassword().toCharArray();
                try (InputStream input = Files.newInputStream(Paths.get(config.getKeyStore()))) {
                    KeyStore store = KeyStore.getInstance(config.getKeyStoreType());
                    store.load(input, password);
                    KeyManagerFactory keyFactory =
                            KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                    keyFactory.init(store, password);
                    keys = keyFactory.getKeyManagers();
                } finally {
                    Arrays.fill(password, '\0');
                }
            }
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(keys, trust.getTrustManagers(), null);
            return context;
        } catch (GeneralSecurityException e) {
            throw new IOException("Syslog TLS material could not be loaded", e);
        }
    }

    @FunctionalInterface
    private interface IoAction {
        void run() throws IOException;
    }
}
