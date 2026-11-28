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

package org.apache.seatunnel.e2e.connector.websocket.server;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;
import org.apache.seatunnel.e2e.common.util.DependencyJar;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.postgresql.Driver;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Drives a streaming job through a separate RFC 6455 client and reads committed JDBC rows. */
class WebSocketServerIT extends TestSuiteBase {
    private static final String HOST = "ws-fixture";
    private static final String ORIGIN = "https://sender.example";
    private static final int PORT = 18898;
    private static final String STORE_PASSWORD = "fixture-store";
    private static final String STORE_PATH = "/tmp/websocket-server.p12";
    private static final String RAW = "/raw.conf";
    private static final String JSON = "/json_tls.conf";
    private static final String TEXT = "/text.conf";
    private static final AtomicLong IDS = new AtomicLong(System.currentTimeMillis());

    @TempDir private static Path temporary;

    private final String token = UUID.randomUUID().toString();
    private PostgreSQLContainer<?> postgres;
    private Engine engine;
    private SSLContext clientTls;

    @BeforeAll
    void start() throws Exception {
        Path keyStore = prepareTls();
        postgres =
                new PostgreSQLContainer<>(DockerImageName.parse("postgres:14-alpine"))
                        .withDatabaseName("test")
                        .withUsername("test")
                        .withPassword("test")
                        .withNetwork(NETWORK)
                        .withNetworkAliases("websocket-postgres");
        postgres.start();
        try (Connection sql = sql();
                Statement statement = sql.createStatement()) {
            statement.execute("CREATE TABLE raw_output (message_type TEXT, payload BYTEA)");
            statement.execute("CREATE TABLE json_output (id BIGINT, value TEXT)");
            statement.execute("CREATE TABLE text_output (id BIGINT, value TEXT)");
        }
        engine = new Engine(keyStore, DependencyJar.of(Driver.class).path());
        engine.startUp();
    }

    @BeforeEach
    void truncate() throws Exception {
        try (Connection sql = sql();
                Statement statement = sql.createStatement()) {
            statement.execute("TRUNCATE raw_output, json_output, text_output");
        }
    }

    @AfterAll
    void stop() throws Exception {
        try {
            if (engine != null) {
                engine.tearDown();
            }
        } finally {
            if (postgres != null) {
                postgres.stop();
            }
        }
    }

    @Test
    void validatesUpgradeAndFramesAndEmitsExactRowsAcrossClients() throws Exception {
        try (Job job = new Job(RAW, 1024, 8, 20000, 4)) {
            assertResponse("401", request("/ingest", HOST, ORIGIN, "wrong", "13", Peer.KEY));
            assertResponse(
                    "403",
                    request("/ingest", HOST, "https://other.example", token, "13", Peer.KEY));
            assertResponse(
                    "400", request("/ingest", "wrong.example", ORIGIN, token, "13", Peer.KEY));
            assertResponse("404", request("/other", HOST, ORIGIN, token, "13", Peer.KEY));
            assertResponse(
                    "404", request("/ingest?token=untrusted", HOST, ORIGIN, token, "13", Peer.KEY));
            assertResponse("426", request("/ingest", HOST, ORIGIN, token, "12", Peer.KEY));
            assertResponse("400", request("/ingest", HOST, ORIGIN, token, "13", "invalid"));
            assertResponse(
                    "400",
                    authorized()
                            .replace("\r\n\r\n", "\r\nX-Fill: " + "x".repeat(17000) + "\r\n\r\n"));

            rejectFrame(0x81, false, bytes("unmasked"), 1002);
            rejectFrame(0xc1, true, bytes("reserved bit"), 1002);
            rejectFrame(0x83, true, bytes("reserved opcode"), 1002);
            rejectFrame(0x09, true, bytes("fragmented ping"), 1002);
            rejectFrame(0x80, true, bytes("unstarted continuation"), 1002);
            rejectFrame(0x88, true, new byte[] {0x03, (byte) 0xee}, 1002);
            rejectFrame(0x81, true, new byte[] {(byte) 0xc3, 0x28}, 1007);
            rejectFrame(0x82, true, new byte[1025], 1009);
            try (Peer malformed = peer(null)) {
                malformed.upgrade(authorized());
                malformed.write(new byte[] {(byte) 0x82, (byte) 0xfe, 0, 1});
                malformed.expectClose(1002);
            }
            try (Peer malformed = peer(null)) {
                malformed.upgrade(authorized());
                malformed.write(new byte[] {(byte) 0x82, (byte) 0xff, 0x7f, 0, 0, 0, 0, 0, 0, 0});
                malformed.expectClose(1009);
            }
            try (Peer malformed = peer(null)) {
                malformed.upgrade(authorized());
                malformed.send(0x02, new byte[600]);
                malformed.send(0x80, new byte[425]);
                malformed.expectClose(1009);
            }
            try (Peer first = peer(null);
                    Peer second = peer(null)) {
                first.upgrade(authorized());
                second.upgrade(request("/ingest", HOST, null, token, "13", Peer.KEY));
                byte[] unicode = bytes("snow 雪");
                first.send(0x01, Arrays.copyOfRange(unicode, 0, unicode.length - 1));
                first.send(0x89, new byte[] {0, 1, (byte) 255});
                first.expectControl(0x8a, new byte[] {0, 1, (byte) 255});
                second.send(0x82, new byte[] {0, 1, (byte) 255});
                first.send(0x80, Arrays.copyOfRange(unicode, unicode.length - 1, unicode.length));
                first.send(0x88, new byte[] {3, (byte) 232, 'b', 'y', 'e'});
                first.expectControl(0x88, new byte[] {3, (byte) 232, 'b', 'y', 'e'});
                first.expectDisconnected();
                second.send(0x81, new byte[0]);
                assertRawRows("binary:0001ff", "text:", "text:736e6f7720e99baa");
                assertEquals("RUNNING", engine.getJobStatus(job.id));
                assertFalse(
                        engine.getServerLogs().contains(token),
                        "Bearer token appeared in worker logs");
            }
        }
    }

    @Test
    void tlsRejectsWrongHostnameAndDeliversExactJson() throws Exception {
        try (Job job = new Job(JSON, 2048, 8, 20000, 2)) {
            assertThrows(
                    SSLHandshakeException.class,
                    () -> {
                        try (Peer rejected = peer("wrong.example")) {
                            rejected.upgrade(authorized());
                        }
                    });
            try (Peer sender = peer(HOST)) {
                sender.upgrade(authorized());
                sender.send(0x81, bytes("{\"id\":70000000001,\"value\":\"snow 雪\"}"));
                sender.send(0x82, bytes("{\"id\":70000000002,\"value\":\"binary JSON\"}"));
                await().atMost(30, TimeUnit.SECONDS)
                        .untilAsserted(
                                () ->
                                        assertEquals(
                                                Arrays.asList(
                                                        "70000000001:snow 雪",
                                                        "70000000002:binary JSON"),
                                                rows(
                                                        "SELECT id::text || ':' || value FROM json_output ORDER BY id")));
                assertEquals("RUNNING", engine.getJobStatus(job.id));
            }
        }
    }

    @Test
    void emitsTypedUtf8TextWithTheConfiguredSeparator() throws Exception {
        try (Job job = new Job(TEXT, 2048, 8, 20000, 4);
                Peer sender = peer(null)) {
            sender.upgrade(authorized());
            sender.send(0x81, bytes("90000000001|typed 雪"));
            sender.send(0x82, bytes("90000000002|typed binary"));
            await().atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    assertEquals(
                                            Arrays.asList(
                                                    "90000000001:typed 雪",
                                                    "90000000002:typed binary"),
                                            rows(
                                                    "SELECT id::text || ':' || value FROM text_output ORDER BY id")));
            assertEquals("RUNNING", engine.getJobStatus(job.id));
        }
    }

    @Test
    void enforcesDeadlinesConnectionLimitAndCancellation() throws Exception {
        try (Job job = new Job(RAW, 1024, 8, 1500, 2)) {
            try (Peer slow = peer(null)) {
                slow.write(bytes("GET /ingest HTTP/1.1\r\n"));
                slow.expectDisconnected();
            }
            try (Peer slow = peer(null);
                    Peer sender = peer(null)) {
                slow.upgrade(authorized());
                sender.upgrade(authorized());
                try (Peer extra = peer(null)) {
                    extra.expectDisconnected();
                }
                slow.send(0x01, bytes("never completed"));
                slow.send(0x89, bytes("still alive"));
                slow.expectControl(0x8a, bytes("still alive"));
                slow.expectDisconnected();
                sender.send(0x81, bytes("ok"));
                assertRawRows("text:6f6b");
                job.cancel();
                sender.expectDisconnected();
            }
        }
    }

    @Test
    void restoresEndpointWithoutReplayingAnIncompleteMessage() throws Exception {
        try (Job job = new Job(RAW, 1024, 8, 60000, 4);
                Peer before = peer(null)) {
            before.upgrade(authorized());
            before.send(0x81, bytes("before"));
            before.send(0x01, bytes("partial"));
            before.send(0x89, bytes("seen"));
            before.expectControl(0x8a, bytes("seen"));
            assertRawRows("text:6265666f7265");
            long prior = engine.getCompletedCheckpointCount(job.id);
            await().atMost(30, TimeUnit.SECONDS)
                    .until(() -> engine.getCompletedCheckpointCount(job.id) > prior);
            job.saveAndRestore();
            before.expectDisconnected();
            rejectFrame(0x80, true, bytes("fragment from prior connection"), 1002);
            try (Peer after = peer(null)) {
                after.upgrade(authorized());
                after.send(0x81, bytes("after"));
                assertRawRows("text:6166746572", "text:6265666f7265");
            }
        }
    }

    @Test
    void rejectsNewMessagesAtTheQueueLimitWhileDownstreamIsHeld() throws Exception {
        try (Job job = new Job(RAW, 1024, 1, 60000, 4);
                Connection blocked = sql();
                Statement locking = blocked.createStatement();
                Peer sender = peer(null)) {
            blocked.setAutoCommit(false);
            locking.execute("LOCK TABLE raw_output IN ACCESS EXCLUSIVE MODE");
            sender.upgrade(authorized());
            byte[] packet = new byte[1000];
            Arrays.fill(packet, (byte) 'x');
            CompletableFuture<Void> burst =
                    CompletableFuture.runAsync(
                            () -> {
                                try {
                                    for (int i = 0; i < 8192 && !sender.socket.isClosed(); i++) {
                                        sender.send(0x82, packet);
                                    }
                                } catch (IOException ended) {
                                    // The peer closes after sending its overload frame.
                                }
                            });
            try {
                sender.expectClose(1013);
            } finally {
                blocked.rollback();
                sender.close();
            }
            burst.get(10, TimeUnit.SECONDS);
            try (Peer next = peer(null)) {
                next.upgrade(authorized());
                next.send(0x81, bytes("recovered"));
                await().atMost(35, TimeUnit.SECONDS)
                        .untilAsserted(
                                () ->
                                        assertEquals(
                                                Collections.singletonList("1"),
                                                rows(
                                                        "SELECT count(*)::text FROM raw_output WHERE message_type='text' "
                                                                + "AND payload=decode('7265636f7665726564', 'hex')")));
            }
        }
    }

    private void assertResponse(String status, String request) throws Exception {
        try (Peer peer = peer(null)) {
            assertTrue(peer.exchange(request).startsWith("HTTP/1.1 " + status + " "));
        }
    }

    private void rejectFrame(int first, boolean mask, byte[] data, int code) throws Exception {
        try (Peer peer = peer(null)) {
            peer.upgrade(authorized());
            peer.write(Peer.frame(first, mask, data));
            peer.expectClose(code);
        }
    }

    private String authorized() {
        return request("/ingest", HOST, ORIGIN, token, "13", Peer.KEY);
    }

    private static String request(
            String path, String host, String origin, String token, String version, String key) {
        return "GET "
                + path
                + " HTTP/1.1\r\nHost: "
                + host
                + "\r\nConnection: keep-alive, Upgrade\r\nUpgrade: websocket\r\n"
                + "Sec-WebSocket-Version: "
                + version
                + "\r\nSec-WebSocket-Key: "
                + key
                + "\r\n"
                + (origin == null ? "" : "Origin: " + origin + "\r\n")
                + "Authorization: Bearer "
                + token
                + "\r\n\r\n";
    }

    private Connection sql() throws Exception {
        return DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    private List<String> rows(String query) throws Exception {
        try (Connection sql = sql();
                Statement statement = sql.createStatement();
                ResultSet result = statement.executeQuery(query)) {
            List<String> found = new ArrayList<>();
            while (result.next()) {
                found.add(result.getString(1));
            }
            return found;
        }
    }

    private void assertRawRows(String... expected) {
        await().atMost(35, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                assertEquals(
                                        Arrays.asList(expected),
                                        rows(
                                                "SELECT message_type || ':' || encode(payload,'hex') FROM raw_output ORDER BY 1")));
    }

    private Socket connect() throws IOException {
        Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(engine.host(), engine.port()), 1000);
            socket.setSoTimeout(20000);
            return socket;
        } catch (IOException error) {
            socket.close();
            throw error;
        }
    }

    private Peer peer(String tlsHost) throws IOException {
        Socket raw = connect();
        if (tlsHost == null) {
            return new Peer(raw);
        }
        try {
            SSLSocket socket =
                    (SSLSocket)
                            clientTls
                                    .getSocketFactory()
                                    .createSocket(raw, tlsHost, engine.port(), true);
            SSLParameters options = socket.getSSLParameters();
            options.setEndpointIdentificationAlgorithm("HTTPS");
            socket.setSSLParameters(options);
            socket.startHandshake();
            return new Peer(socket);
        } catch (IOException error) {
            raw.close();
            throw error;
        }
    }

    private Path prepareTls() throws Exception {
        Path file = temporary.resolve("fixture.p12");
        Process keytool =
                new ProcessBuilder(
                                Paths.get(System.getProperty("java.home"), "bin", "keytool")
                                        .toString(),
                                "-genkeypair",
                                "-noprompt",
                                "-alias",
                                "fixture",
                                "-keyalg",
                                "RSA",
                                "-keysize",
                                "2048",
                                "-validity",
                                "2",
                                "-dname",
                                "CN=" + HOST,
                                "-ext",
                                "SAN=dns:" + HOST,
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                file.toString(),
                                "-storepass",
                                STORE_PASSWORD)
                        .redirectErrorStream(true)
                        .redirectOutput(temporary.resolve("keytool.log").toFile())
                        .start();
        if (!keytool.waitFor(30, TimeUnit.SECONDS)) {
            keytool.destroyForcibly();
            throw new IllegalStateException("Fixture certificate generation timed out");
        }
        assertEquals(0, keytool.exitValue());
        KeyStore privateStore = KeyStore.getInstance("PKCS12");
        try (InputStream data = Files.newInputStream(file)) {
            privateStore.load(data, STORE_PASSWORD.toCharArray());
        }
        KeyStore trust = KeyStore.getInstance("JKS");
        trust.load(null, null);
        trust.setCertificateEntry("fixture", privateStore.getCertificate("fixture"));
        TrustManagerFactory managers =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        managers.init(trust);
        clientTls = SSLContext.getInstance("TLS");
        clientTls.init(null, managers.getTrustManagers(), null);
        return file;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private final class Job implements AutoCloseable {
        private final String id = Long.toString(IDS.incrementAndGet());
        private final String config;
        private final String[] args;
        private CompletableFuture<Container.ExecResult> active;

        Job(String config, int messageBytes, int queueCount, int messageTimeout, int connections)
                throws Exception {
            this.config = config;
            this.args =
                    new String[] {
                        "test_token=" + token,
                        "test_message_bytes=" + messageBytes,
                        "test_queue_count=" + queueCount,
                        "test_message_timeout=" + messageTimeout,
                        "test_connections=" + connections,
                        "test_port=" + PORT,
                        "test_tls_password=" + STORE_PASSWORD,
                        "test_tls_path=" + STORE_PATH
                    };
            start(false);
        }

        private void start(boolean restore) throws Exception {
            active =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return restore
                                            ? engine.restoreJob(config, id, args)
                                            : engine.executeJob(config, id, args);
                                } catch (Exception error) {
                                    throw new IllegalStateException(
                                            "Engine invocation failed", error);
                                }
                            });
            try {
                await().atMost(55, TimeUnit.SECONDS)
                        .until(
                                () -> {
                                    if (active.isDone()) {
                                        throw new IllegalStateException(
                                                "Streaming job exited: "
                                                        + active.get().getExitCode());
                                    }
                                    if (!"RUNNING".equals(engine.getJobStatus(id))) {
                                        return false;
                                    }
                                    try (Socket probe = connect()) {
                                        return true;
                                    } catch (IOException notBoundYet) {
                                        return false;
                                    }
                                });
            } catch (Exception | AssertionError error) {
                close();
                throw error;
            }
        }

        void saveAndRestore() throws Exception {
            assertEquals(0, engine.savepointJob(id).getExitCode());
            assertEquals(0, active.get(45, TimeUnit.SECONDS).getExitCode());
            start(true);
        }

        void cancel() throws Exception {
            assertEquals(0, engine.cancelJob(id).getExitCode());
            await().atMost(35, TimeUnit.SECONDS)
                    .until(() -> "CANCELED".equals(engine.getJobStatus(id)));
            active.get(45, TimeUnit.SECONDS);
        }

        @Override
        public void close() throws Exception {
            if (!active.isDone() && "RUNNING".equals(engine.getJobStatus(id))) {
                cancel();
            }
        }
    }

    private static final class Engine extends SeaTunnelContainer {
        private final Path store;
        private final Path jdbc;

        Engine(Path store, Path jdbc) {
            this.store = store;
            this.jdbc = jdbc;
        }

        @Override
        protected void executeExtraCommands(GenericContainer<?> runtime) {
            runtime.withExposedPorts(5801, 8080, PORT);
            runtime.withCopyFileToContainer(MountableFile.forHostPath(store), STORE_PATH);
            runtime.withCopyFileToContainer(
                    MountableFile.forHostPath(jdbc),
                    "/tmp/seatunnel/plugins/Jdbc/lib/postgresql.jar");
        }

        String host() {
            return server.getHost();
        }

        int port() {
            return server.getMappedPort(PORT);
        }
    }

    /** Independent client framing, without the connector's protocol implementation. */
    private static final class Peer implements AutoCloseable {
        static final String KEY = "dGhlIHNhbXBsZSBub25jZQ==";
        private static final byte[] MASK = new byte[] {0x12, 0x34, 0x56, 0x78};
        private final Socket socket;
        private final DataInputStream input;

        Peer(Socket socket) throws IOException {
            this.socket = socket;
            input = new DataInputStream(socket.getInputStream());
        }

        void upgrade(String request) throws IOException {
            String answer = exchange(request);
            assertTrue(answer.startsWith("HTTP/1.1 101 "));
            assertTrue(answer.contains("Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo="));
        }

        String exchange(String request) throws IOException {
            write(bytes(request));
            ByteArrayOutputStream answer = new ByteArrayOutputStream();
            byte[] ending = new byte[] {'\r', '\n', '\r', '\n'};
            int matched = 0;
            while (answer.size() < 4096) {
                int next = input.readUnsignedByte();
                answer.write(next);
                matched = next == ending[matched] ? matched + 1 : next == '\r' ? 1 : 0;
                if (matched == ending.length) {
                    return answer.toString("US-ASCII");
                }
            }
            throw new IOException("Oversized upgrade response");
        }

        void write(byte[] value) throws IOException {
            socket.getOutputStream().write(value);
            socket.getOutputStream().flush();
        }

        void send(int first, byte[] payload) throws IOException {
            write(frame(first, true, payload));
        }

        static byte[] frame(int first, boolean mask, byte[] payload) {
            ByteArrayOutputStream wire = new ByteArrayOutputStream();
            wire.write(first);
            if (payload.length < 126) {
                wire.write((mask ? 128 : 0) | payload.length);
            } else {
                wire.write((mask ? 128 : 0) | 126);
                wire.write(payload.length >>> 8);
                wire.write(payload.length & 255);
            }
            if (mask) {
                wire.write(MASK, 0, MASK.length);
            }
            for (int index = 0; index < payload.length; index++) {
                wire.write(mask ? payload[index] ^ MASK[index % 4] : payload[index]);
            }
            return wire.toByteArray();
        }

        void expectControl(int first, byte[] data) throws IOException {
            assertEquals(first, input.readUnsignedByte());
            assertEquals(data.length, input.readUnsignedByte());
            byte[] actual = new byte[data.length];
            input.readFully(actual);
            assertArrayEquals(data, actual);
        }

        void expectClose(int status) throws IOException {
            expectControl(0x88, new byte[] {(byte) (status >>> 8), (byte) status});
        }

        void expectDisconnected() throws IOException {
            try {
                assertEquals(-1, input.read());
            } catch (SocketException | EOFException disconnected) {
                // TCP reset and EOF are equivalent when the listener aborts a peer.
            }
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }
}
