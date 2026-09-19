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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.connectors.seatunnel.syslog.config.SyslogSinkConfig;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Local TLS transport integration. Interoperability with syslog-ng lives in the E2E module. */
@Timeout(40)
class SyslogTlsClientTest {
    @TempDir static Path directory;
    private static SSLContext serverContext;

    @BeforeAll
    static void certificates() throws Exception {
        keytool(
                "-genkeypair",
                "-alias",
                "receiver",
                "-keyalg",
                "RSA",
                "-keysize",
                "2048",
                "-sigalg",
                "SHA256withRSA",
                "-dname",
                "CN=localhost",
                "-ext",
                "SAN=dns:localhost",
                "-ext",
                "EKU=serverAuth,clientAuth",
                "-startdate",
                "2020/01/01 00:00:00",
                "-validity",
                "36500",
                "-storetype",
                "JKS",
                "-keystore",
                directory.resolve("receiver.jks").toString(),
                "-storepass",
                "test-password",
                "-keypass",
                "test-password",
                "-noprompt");
        keytool(
                "-exportcert",
                "-rfc",
                "-alias",
                "receiver",
                "-keystore",
                directory.resolve("receiver.jks").toString(),
                "-storepass",
                "test-password",
                "-file",
                directory.resolve("ca.pem").toString());
        KeyStore keys = KeyStore.getInstance("JKS");
        try (InputStream input = Files.newInputStream(directory.resolve("receiver.jks"))) {
            keys.load(input, "test-password".toCharArray());
        }
        KeyManagerFactory keyFactory =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyFactory.init(keys, "test-password".toCharArray());
        TrustManagerFactory trust =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trust.init(keys);
        serverContext = SSLContext.getInstance("TLS");
        serverContext.init(keyFactory.getKeyManagers(), trust.getTrustManagers(), null);
    }

    @Test
    void serializedSinkDeliversBulkUtf8FramesToTlsReceiver() throws Exception {
        SSLContext global = SSLContext.getDefault();
        try (Receiver receiver = new Receiver(false)) {
            int count = 1000;
            Future<List<String>> received = receiver.read(count);
            SyslogSink original =
                    new SyslogSink(config(receiver.port()), SyslogSinkFactoryTest.table());
            SyslogSink restored =
                    SerializationUtils.deserialize(SerializationUtils.serialize(original));
            SyslogSinkWriter writer = (SyslogSinkWriter) restored.createWriter(null);
            long started = System.nanoTime();
            try {
                for (int i = 0; i < count; i++) {
                    writer.write(
                            new SeaTunnelRow(
                                    new Object[] {"event-" + i + " \u4e16\u754c\nsecond"}));
                }
                writer.prepareCommit();
                writer.prepareCommit(1);
                writer.snapshotState(1);
            } finally {
                writer.close();
            }
            List<String> messages = received.get(10, TimeUnit.SECONDS);
            assertEquals(count, messages.size());
            for (int i = 0; i < count; i++) {
                assertEquals(
                        "<14>1 - - - - - - \uFEFFevent-" + i + " \u4e16\u754c\nsecond",
                        messages.get(i));
            }
            System.out.println(
                    "Syslog local TLS receiver: "
                            + count
                            + " frames in "
                            + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)
                            + " ms (not a benchmark)");
        }
        assertSame(global, SSLContext.getDefault());
    }

    @Test
    void supportsMutualTlsWithClientKeyStore() throws Exception {
        try (Receiver receiver = new Receiver(true)) {
            Future<List<String>> received = receiver.read(1);
            Map<String, Object> values = values(receiver.port());
            values.put("tls.key_store.path", directory.resolve("receiver.jks").toString());
            values.put("password", "test-password");
            values.put("tls.key_store.type", "JKS");
            try (SyslogTlsClient client =
                    SyslogTlsClient.connect(new SyslogSinkConfig(ReadonlyConfig.fromMap(values)))) {
                client.write(
                        new SyslogMessageEncoder(SyslogMessageEncoderTest.TYPE, 8192)
                                .encode(SyslogMessageEncoderTest.row()));
            }
            assertEquals(1, received.get(10, TimeUnit.SECONDS).size());
        }
    }

    @Test
    void rejectsUntrustedReceiverAndWrongHostname() throws Exception {
        for (boolean wrongHost : new boolean[] {false, true}) {
            try (Receiver receiver = new Receiver(false)) {
                Future<Boolean> rejected = receiver.expectRejectedHandshake();
                Map<String, Object> values = values(receiver.port());
                if (wrongHost) {
                    values.put("host", "127.0.0.1");
                } else {
                    values.remove("tls.ca_cert_path");
                }
                IOException error =
                        assertThrows(
                                IOException.class,
                                () ->
                                        SyslogTlsClient.connect(
                                                new SyslogSinkConfig(
                                                        ReadonlyConfig.fromMap(values))));
                assertTrue(error.getMessage().contains("handshake"));
                assertTrue(rejected.get(10, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    void rejectsMissingClientCertificate() throws Exception {
        try (Receiver receiver = new Receiver(true)) {
            Future<Boolean> rejected = receiver.expectRejectedHandshake();
            assertThrows(
                    IOException.class,
                    () -> {
                        try (SyslogTlsClient client =
                                SyslogTlsClient.connect(config(receiver.port()))) {
                            // TLS 1.3 may report the peer's rejection only on a subsequent
                            // operation.
                            for (int i = 0; i < 100; i++) {
                                client.write(new byte[65536]);
                            }
                        }
                    });
            assertTrue(rejected.get(10, TimeUnit.SECONDS));
        }
    }

    @Test
    void boundsHandshakeWhenTcpPeerDoesNotSpeakTls() throws Exception {
        ExecutorService worker = Executors.newSingleThreadExecutor();
        CountDownLatch release = new CountDownLatch(1);
        try (ServerSocket server = new ServerSocket(0)) {
            Future<?> accepted =
                    worker.submit(
                            () -> {
                                try (Socket socket = server.accept()) {
                                    assertTrue(release.await(10, TimeUnit.SECONDS));
                                }
                                return null;
                            });
            Map<String, Object> values = values(server.getLocalPort());
            values.put("write_timeout_ms", 500);
            assertThrows(
                    SocketTimeoutException.class,
                    () ->
                            SyslogTlsClient.connect(
                                    new SyslogSinkConfig(ReadonlyConfig.fromMap(values))));
            release.countDown();
            accepted.get(5, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            worker.shutdownNow();
            assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void boundsActualTlsWriteWhenReceiverStopsReading() throws Exception {
        CountDownLatch handshaken = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (Receiver receiver = new Receiver(false)) {
            receiver.server.setReceiveBufferSize(1024);
            Future<?> accepted =
                    receiver.worker.submit(
                            () -> {
                                try (SSLSocket socket = receiver.accept()) {
                                    socket.startHandshake();
                                    handshaken.countDown();
                                    assertTrue(release.await(20, TimeUnit.SECONDS));
                                }
                                return null;
                            });
            Map<String, Object> values = values(receiver.port());
            values.put("write_timeout_ms", 2000);
            SyslogTlsClient client =
                    SyslogTlsClient.connect(new SyslogSinkConfig(ReadonlyConfig.fromMap(values)));
            try {
                assertTrue(handshaken.await(5, TimeUnit.SECONDS));
                assertThrows(
                        SocketTimeoutException.class,
                        () -> {
                            byte[] frame = new byte[65536];
                            for (int i = 0; i < 2048; i++) {
                                client.write(frame);
                            }
                        });
                assertThrows(IOException.class, client::flush);
            } finally {
                release.countDown();
                assertThrows(IOException.class, client::close);
            }
            accepted.get(5, TimeUnit.SECONDS);
        } finally {
            release.countDown();
        }
    }

    private static SyslogSinkConfig config(int port) {
        return new SyslogSinkConfig(ReadonlyConfig.fromMap(values(port)));
    }

    private static Map<String, Object> values(int port) {
        Map<String, Object> values = SyslogSinkFactoryTest.config();
        values.put("port", port);
        values.put("tls.ca_cert_path", directory.resolve("ca.pem").toString());
        values.put("write_timeout_ms", 10000);
        return values;
    }

    private static void keytool(String... arguments) throws Exception {
        List<String> command = new ArrayList<>();
        command.add(Paths.get(System.getProperty("java.home"), "bin", "keytool").toString());
        java.util.Collections.addAll(command, arguments);
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        try {
            assertTrue(process.waitFor(30, TimeUnit.SECONDS), "keytool timed out");
            assertEquals(0, process.exitValue(), "keytool fixture generation failed");
        } finally {
            process.destroyForcibly();
        }
    }

    private static final class Receiver implements AutoCloseable {
        private final SSLServerSocket server;
        private final ExecutorService worker = Executors.newSingleThreadExecutor();
        private volatile SSLSocket socket;

        Receiver(boolean clientAuth) throws IOException {
            server = (SSLServerSocket) serverContext.getServerSocketFactory().createServerSocket(0);
            server.setSoTimeout(10000);
            server.setNeedClientAuth(clientAuth);
        }

        int port() {
            return server.getLocalPort();
        }

        SSLSocket accept() throws IOException {
            socket = (SSLSocket) server.accept();
            socket.setSoTimeout(10000);
            return socket;
        }

        Future<List<String>> read(int count) {
            return worker.submit(
                    () -> {
                        try (SSLSocket connection = accept();
                                DataInputStream input =
                                        new DataInputStream(connection.getInputStream())) {
                            List<String> messages = new ArrayList<>();
                            for (int i = 0; i < count; i++) {
                                int length = 0;
                                int digit;
                                int digits = 0;
                                while ((digit = input.read()) != ' ') {
                                    assertTrue(
                                            digit >= '0' && digit <= '9' && ++digits <= 7,
                                            "invalid octet prefix");
                                    assertTrue(digits > 1 || digit != '0', "leading zero");
                                    length = length * 10 + digit - '0';
                                }
                                assertTrue(length > 0 && length <= 1048576);
                                byte[] message = new byte[length];
                                input.readFully(message);
                                messages.add(new String(message, StandardCharsets.UTF_8));
                            }
                            return messages;
                        }
                    });
        }

        Future<Boolean> expectRejectedHandshake() {
            return worker.submit(
                    () -> {
                        try (SSLSocket connection = accept()) {
                            connection.startHandshake();
                            return false;
                        } catch (IOException expected) {
                            return true;
                        }
                    });
        }

        @Override
        public void close() throws Exception {
            try {
                server.close();
                if (socket != null) {
                    socket.close();
                }
            } finally {
                worker.shutdownNow();
                assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }
}
