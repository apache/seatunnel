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

package org.apache.seatunnel.connectors.seatunnel.mongodb.internal;

import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.mongodb.MongoTimeoutException;
import com.mongodb.client.MongoClient;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Proves the {@code tls_ca_file} trust behavior against real TLS endpoints. Two in-process TLS
 * servers present certificates signed by the test CA: one for {@code DNS:localhost}, and one for
 * {@code DNS:unrelated-host.invalid} only, which is used for hostname mismatches without relying on
 * DNS or reverse lookups. The servers do not speak the MongoDB protocol, so every client call ends
 * with a server selection timeout; the outcome of the TLS handshake is taken from the server side
 * and from the client error.
 */
class MongodbTlsCaFileHandshakeTest {

    private static final String CLIENT_OPTIONS =
            "/?directConnection=true&serverSelectionTimeoutMS=2000&connectTimeoutMS=2000";

    @TempDir static Path tempDir;

    private static TlsServer server;
    private static TlsServer wrongHostServer;
    private static String trustedCa;
    private static String untrustedCa;

    @BeforeAll
    static void startServer() throws Exception {
        server = new TlsServer(TlsTestMaterial.localhostServerContext());
        wrongHostServer = new TlsServer(TlsTestMaterial.wrongHostServerContext());
        trustedCa =
                TlsTestMaterial.writePem(tempDir, "ca.pem", TlsTestMaterial.testCa()).toString();
        untrustedCa =
                TlsTestMaterial.writePem(tempDir, "other.pem", TlsTestMaterial.untrustedCa())
                        .toString();
    }

    @AfterAll
    static void stopServer() throws IOException {
        server.close();
        wrongHostServer.close();
    }

    @BeforeEach
    void resetCounters() {
        server.reset();
        wrongHostServer.reset();
    }

    @Test
    void configuredCaIsTrusted() {
        ping(server, "", trustedCa);

        Assertions.assertTrue(
                server.completedHandshakes() > 0, "TLS handshake with the configured CA");
    }

    @Test
    void untrustedCaIsRejected() {
        String error = ping(server, "", untrustedCa);

        Assertions.assertEquals(0, server.completedHandshakes());
        Assertions.assertTrue(error.contains("PKIX path building failed"), error);
    }

    @Test
    void withoutCaFileTheJvmDefaultTrustIsUsedAsBefore() {
        // The test CA is not in the JDK trust store, so the previous behavior rejects it.
        String error = ping(server, "&tls=true", null);

        Assertions.assertEquals(0, server.completedHandshakes());
        Assertions.assertTrue(error.contains("PKIX path building failed"), error);
    }

    @Test
    void hostnameMismatchIsRejectedEvenWithTrustedCa() {
        // Trusted CA, but the certificate is for unrelated-host.invalid, not localhost.
        String error = ping(wrongHostServer, "", trustedCa);

        Assertions.assertEquals(0, wrongHostServer.completedHandshakes());
        Assertions.assertTrue(error.contains("No subject alternative DNS name matching"), error);
    }

    @Test
    void hostnameVerificationIsStillControlledByTheConnectionString() {
        ping(wrongHostServer, "&tlsAllowInvalidHostnames=true", trustedCa);

        Assertions.assertTrue(wrongHostServer.completedHandshakes() > 0);
    }

    /** Runs a ping and returns the client error (the server never answers MongoDB commands). */
    private static String ping(TlsServer target, String extraOptions, String caFile) {
        String uri = "mongodb://localhost:" + target.port() + CLIENT_OPTIONS + extraOptions;
        try (MongoClient client = MongodbClientSettingsFactory.createClient(uri, caFile)) {
            MongoTimeoutException e =
                    Assertions.assertThrows(
                            MongoTimeoutException.class,
                            () -> client.getDatabase("admin").runCommand(new Document("ping", 1)));
            return e.getMessage();
        }
    }

    /** Accepts TLS connections and records whether each handshake completed. */
    private static final class TlsServer implements AutoCloseable {
        private final SSLServerSocket socket;
        private final AtomicInteger completedHandshakes = new AtomicInteger();
        private final Thread acceptor;

        TlsServer(SSLContext context) throws Exception {
            socket =
                    (SSLServerSocket)
                            context.getServerSocketFactory()
                                    .createServerSocket(0, 50, InetAddress.getLoopbackAddress());
            acceptor = new Thread(this::acceptLoop, "mongodb-tls-test-server");
            acceptor.setDaemon(true);
            acceptor.start();
        }

        int port() {
            return socket.getLocalPort();
        }

        int completedHandshakes() {
            return completedHandshakes.get();
        }

        void reset() {
            completedHandshakes.set(0);
        }

        private void acceptLoop() {
            while (!socket.isClosed()) {
                try (Socket accepted = socket.accept()) {
                    accepted.setSoTimeout(5000);
                    try {
                        ((SSLSocket) accepted).startHandshake();
                        // With TLS 1.3 the server side can finish before the client has
                        // validated the certificate. Only a client that accepted the
                        // certificate sends application data (its MongoDB hello).
                        if (accepted.getInputStream().read() >= 0) {
                            completedHandshakes.incrementAndGet();
                        }
                    } catch (IOException handshakeFailure) {
                        // rejected by the client: untrusted CA or hostname mismatch
                    }
                } catch (SocketException closed) {
                    return;
                } catch (IOException ignored) {
                    // keep accepting
                }
            }
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }
}
