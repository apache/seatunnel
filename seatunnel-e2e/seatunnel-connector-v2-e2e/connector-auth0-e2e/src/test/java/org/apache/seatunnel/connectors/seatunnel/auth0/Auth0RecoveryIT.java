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

package org.apache.seatunnel.connectors.seatunnel.auth0;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.e2e.connector.auth0.Auth0Peer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises serialized reader state against an independent HTTPS peer at exact poll boundaries.
 *
 * <p>These component cases supplement the real-engine integration tests where a page-internal
 * checkpoint cannot be timed deterministically by the engine scheduler.
 */
class Auth0RecoveryIT {
    private static final String TOKEN = "synthetic-auth0-management-token";
    private static String trust;
    private static final String PASSWORD = "fixture-store-password";
    private static final ExecutorService PEER_EXECUTOR = Executors.newSingleThreadExecutor();
    @TempDir static Path directory;
    private static Future<?> peer;

    @BeforeAll
    static void startIndependentPeer() throws Exception {
        identity("trusted");
        identity("untrusted");
        peer =
                PEER_EXECUTOR.submit(
                        () -> {
                            Auth0Peer.main(new String[] {directory.toString()});
                            return null;
                        });
        Awaitility.await()
                .atMost(15, TimeUnit.SECONDS)
                .until(() -> Files.exists(directory.resolve("ready")));
    }

    @AfterAll
    static void stopIndependentPeer() throws Exception {
        try {
            if (peer != null) {
                Files.writeString(directory.resolve("stop"), "stop");
                peer.get(20, TimeUnit.SECONDS);
            }
        } finally {
            PEER_EXECUTOR.shutdownNow();
        }
    }

    @Test
    void restoresTheSerializedClientRowOffsetWithoutRepeatingCommittedOutput() throws Exception {
        program("row-offset", page(false), page(false));
        Auth0Config config = settings();
        Rows rows = new Rows();
        Auth0Split saved = prefix(config, rows);
        ReaderContext context = new ReaderContext();
        try (Auth0Reader restored = reader(context, config)) {
            restored.addSplits(List.of(saved));
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                restored.pollNext(rows);
                                return context.done;
                            });
            assertTrue(restored.snapshotState(2L).get(0).finished());
        }
        assertEquals(2, rows.values.size());
        Set<String> ids = new HashSet<>();
        for (SeaTunnelRow row : rows.values) {
            assertEquals("clients", row.getField(0));
            assertTrue(ids.add((String) row.getField(1)));
            assertNull(row.getField(2));
            assertNull(row.getField(3));
        }
        assertEquals(Set.of("client-1", "client-2"), ids);
        assertEquals(2, requests("row-offset"));
    }

    @Test
    void rejectsAChangedClientPageBeforeEmittingAnUnverifiedSuffix() throws Exception {
        program("changed-page", page(false), page(true));
        Auth0Config config = settings();
        Auth0Split saved = prefix(config, new Rows());
        Rows suffix = new Rows();
        try (Auth0Reader restored = reader(new ReaderContext(), config)) {
            restored.addSplits(List.of(saved));
            Auth0Resource.Failure failure =
                    assertThrows(
                            Auth0Resource.Failure.class,
                            () -> {
                                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                                while (System.nanoTime() - deadline < 0) {
                                    restored.pollNext(suffix);
                                }
                            });
            assertTrue(failure.getMessage().contains("page changed since checkpoint"));
            assertTrue(suffix.values.isEmpty());
        }
        assertEquals(2, requests("changed-page"));
    }

    @Test
    void preservesEmptyAssignmentSnapshotAndStickyCancellationAndTerminalState() throws Exception {
        program("zero-traffic", page(false));
        Auth0Config config = settings();
        Rows rows = new Rows();
        try (Auth0Reader cancelled = new Auth0Reader(config, new ReaderContext())) {
            assertTrue(cancelled.snapshotState(0L).isEmpty());
            cancelled.cancel();
            cancelled.cancel();
            cancelled.open();
            cancelled.addSplits(List.of(config.newSplit(Instant.parse("2024-01-03T00:00:00Z"))));
            cancelled.pollNext(rows);
            assertTrue(rows.values.isEmpty());
        }
        ReaderContext context = new ReaderContext();
        Auth0Split terminal = config.newSplit(Instant.parse("2024-01-03T00:00:00Z"));
        terminal.phase = null;
        try (Auth0Reader restored = reader(context, config)) {
            restored.addSplits(List.of(serialized(terminal)));
            restored.pollNext(rows);
            restored.pollNext(rows);
            assertTrue(context.done);
            assertTrue(restored.snapshotState(1L).get(0).finished());
            assertTrue(rows.values.isEmpty());
        }
        assertEquals(0, requests("zero-traffic"));
    }

    @Test
    void refusesToRequestARoleOffsetBeyondTheProviderLimit() throws Exception {
        String name = "role-page-limit";
        String org =
                reply(
                        "/api/v2/organizations",
                        "take=1",
                        "{\"organizations\":[{\"id\":\"org_1\"}]}");
        String members =
                reply(
                        "/api/v2/organizations/org_1/members",
                        "take=1",
                        "{\"members\":[{\"user_id\":\"auth0|a\"}]}");
        String roles =
                reply(
                        "/api/v2/organizations/org_1/members/auth0%7Ca/roles",
                        "include_totals=false&page=1000&per_page=1", "[{\"id\":\"rol_last\"}]");
        writeProgram(name, List.of(org, members, org, members, roles));
        Auth0Config config = settings("organization_member_roles", 1);
        Rows rows = new Rows();
        Auth0Split saved;
        try (Auth0Reader original = reader(new ReaderContext(), config)) {
            original.addSplits(List.of(config.newSplit(Instant.parse("2024-01-03T00:00:00Z"))));
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                original.pollNext(rows);
                                return original.snapshotState(1L).get(0).userId != null;
                            });
            saved = original.snapshotState(2L).get(0);
        }
        // Only the offset and charged request count are needed to represent the preceding pages.
        saved.roles.number = 1000;
        saved.requests.put(Auth0Resource.ORGANIZATION_MEMBER_ROLES, 1000);
        try (Auth0Reader restored = reader(new ReaderContext(), config)) {
            restored.addSplits(List.of(serialized(saved)));
            Auth0Resource.Failure failure =
                    assertThrows(
                            Auth0Resource.Failure.class,
                            () -> {
                                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                                while (System.nanoTime() - deadline < 0) {
                                    restored.pollNext(rows);
                                }
                            });
            assertTrue(
                    failure.getMessage()
                            .contains("member role pagination reached the provider page limit"));
        }
        assertEquals(1, rows.values.size());
        assertEquals("rol_last", rows.values.get(0).getField(1));
        assertEquals("org_1", rows.values.get(0).getField(2));
        assertEquals("auth0|a", rows.values.get(0).getField(3));
        assertEquals(5, requests(name));
    }

    private static Auth0Split prefix(Auth0Config config, Rows rows) throws Exception {
        try (Auth0Reader first = reader(new ReaderContext(), config)) {
            assertTrue(first.snapshotState(0L).isEmpty());
            first.addSplits(List.of(config.newSplit(Instant.parse("2024-01-03T00:00:00Z"))));
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                first.pollNext(rows);
                                return rows.values.size() == 1;
                            });
            Auth0Split checkpoint = first.snapshotState(1L).get(0);
            assertEquals(1, checkpoint.top.row);
            assertNotNull(checkpoint.top.digest);
            Auth0Split saved = serialized(checkpoint);
            checkpoint.top.row = 0;
            assertEquals(1, first.snapshotState(2L).get(0).top.row);
            return saved;
        }
    }

    private static Auth0Reader reader(ReaderContext context, Auth0Config config) {
        Auth0Reader reader = new Auth0Reader(config, context);
        reader.open();
        return reader;
    }

    private static Auth0Config settings() {
        return settings("clients", 3);
    }

    private static Auth0Config settings(String resource, int size) {
        return new Auth0Config(
                ReadonlyConfig.fromMap(
                        Map.of(
                                "resources",
                                List.of(resource),
                                "token",
                                TOKEN,
                                "tenant_url",
                                "https://localhost:21443",
                                "allow_custom_domain",
                                true,
                                "tls_trust_certificate",
                                trust,
                                "page_size",
                                size)));
    }

    private static void program(String name, String... bodies) throws Exception {
        List<String> lines = new ArrayList<>();
        for (String body : bodies) {
            lines.add(reply("/api/v2/clients", "include_totals=false&page=0&per_page=3", body));
        }
        writeProgram(name, lines);
    }

    private static String reply(String path, String query, String body) {
        return path
                + "\t"
                + query
                + "\t200\tfixed\t\t"
                + Base64.getEncoder().encodeToString(body.getBytes(StandardCharsets.UTF_8))
                + "\tinitial";
    }

    private static void writeProgram(String name, List<String> lines) throws IOException {
        Files.write(directory.resolve(name + ".program"), lines, StandardCharsets.UTF_8);
        Files.writeString(directory.resolve("active"), name);
    }

    private static int requests(String name) throws Exception {
        assertFalse(Files.exists(directory.resolve(name + ".invalid")));
        Path journal = directory.resolve(name + ".requests");
        return Files.exists(journal) ? Files.readAllLines(journal).size() : 0;
    }

    private static String page(boolean changed) {
        return "[{\"client_id\":\"client-1\",\"name\":\"private-client-name-a\"},"
                + "{\"client_id\":\"client-2\",\"name\":\""
                + (changed ? "changed-private-client-name" : "private-client-name-b")
                + "\"}]";
    }

    private static Auth0Split serialized(Auth0Split split) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(split);
        }
        assertFalse(bytes.toString(StandardCharsets.ISO_8859_1).contains(TOKEN));

        assertFalse(bytes.toString(StandardCharsets.ISO_8859_1).contains("private-client-name"));
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (Auth0Split) input.readObject();
        }
    }

    private static void identity(String name) throws Exception {
        Path file = directory.resolve(name + ".p12");
        Process process =
                new ProcessBuilder(
                                Path.of(System.getProperty("java.home"), "bin", "keytool")
                                        .toString(),
                                "-genkeypair",
                                "-alias",
                                "api",
                                "-keyalg",
                                "RSA",
                                "-keysize",
                                "2048",
                                "-validity",
                                "7",
                                "-dname",
                                "CN=localhost",
                                "-ext",
                                "SAN=dns:localhost",
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                file.toString(),
                                "-storepass",
                                PASSWORD,
                                "-keypass",
                                PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, process.waitFor(), output);
        if ("trusted".equals(name)) {
            KeyStore store = KeyStore.getInstance("PKCS12");
            try (InputStream input = Files.newInputStream(file)) {
                store.load(input, PASSWORD.toCharArray());
            }
            trust = Base64.getEncoder().encodeToString(store.getCertificate("api").getEncoded());
            String certificate =
                    Base64.getMimeEncoder(64, new byte[] {'\n'})
                            .encodeToString(store.getCertificate("api").getEncoded());
            Files.writeString(
                    directory.resolve("trusted.pem"),
                    "-----BEGIN CERTIFICATE-----\n"
                            + certificate
                            + "\n-----END CERTIFICATE-----\n");
        }
    }

    /**
     * Captures actual reader output using the same lock exposed to checkpointing.
     *
     * <p>Rows are inspected across the two reader instances rather than inferred from HTTP calls.
     */
    private static final class Rows implements Collector<SeaTunnelRow> {
        private final Object lock = new Object();
        private final List<SeaTunnelRow> values = new ArrayList<>();

        @Override
        public void collect(SeaTunnelRow row) {
            values.add(row);
        }

        @Override
        public Object getCheckpointLock() {
            return lock;
        }
    }

    /**
     * Records the bounded reader's real terminal signal without an engine scheduler.
     *
     * <p>No transport behavior or connector response is mocked by this context.
     */
    private static final class ReaderContext implements SourceReader.Context {
        private boolean done;

        @Override
        public int getIndexOfSubtask() {
            return 0;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.BOUNDED;
        }

        @Override
        public void signalNoMoreElement() {
            done = true;
        }

        @Override
        public void sendSplitRequest() {}

        @Override
        public void sendSourceEventToEnumerator(SourceEvent event) {}

        @Override
        public MetricsContext getMetricsContext() {
            return null;
        }

        @Override
        public EventListener getEventListener() {
            return null;
        }
    }
}
