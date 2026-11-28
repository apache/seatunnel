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

package org.apache.seatunnel.connectors.seatunnel.avni;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.e2e.connector.avni.AvniPeer;

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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises serialized reader state against an independent HTTPS peer at exact poll boundaries.
 *
 * <p>These component cases supplement the real-engine integration tests where a page-internal
 * checkpoint cannot be timed deterministically by the engine scheduler.
 */
class AvniRecoveryIT {
    private static final String TOKEN = "synthetic-avni-management-token";
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
                            AvniPeer.main(new String[] {directory.toString()});
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
    void restoresTheSerializedSubjectRowOffsetWithoutRepeatingCommittedOutput() throws Exception {
        program("row-offset", page(false), page(false));
        AvniConfig config = settings();
        Rows rows = new Rows();
        AvniSplit saved = prefix(config, rows);
        ReaderContext context = new ReaderContext();
        try (AvniReader restored = reader(context, config)) {
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
            assertEquals("subjects", row.getField(0));
            assertTrue(ids.add((String) row.getField(1)));
            assertEquals("2024-01-01T00:00:00.001Z", row.getField(2));
            assertTrue(((String) row.getField(3)).contains("private-subject-name"));
        }
        assertEquals(Set.of("subject-1", "subject-2"), ids);
        assertEquals(6, requests("row-offset"));
    }

    @Test
    void rejectsAChangedSubjectPageBeforeEmittingAnUnverifiedSuffix() throws Exception {
        program("changed-page", page(false), page(true));
        AvniConfig config = settings();
        AvniSplit saved = prefix(config, new Rows());
        Rows suffix = new Rows();
        try (AvniReader restored = reader(new ReaderContext(), config)) {
            restored.addSplits(List.of(saved));
            AvniResource.Failure failure =
                    assertThrows(
                            AvniResource.Failure.class,
                            () -> {
                                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                                while (System.nanoTime() - deadline < 0) {
                                    restored.pollNext(suffix);
                                }
                            });
            assertTrue(failure.getMessage().contains("page changed since checkpoint"));
            assertTrue(suffix.values.isEmpty());
        }
        assertEquals(6, requests("changed-page"));
    }

    @Test
    void preservesEmptyAssignmentSnapshotAndStickyCancellationAndTerminalState() throws Exception {
        program("zero-traffic", page(false));
        AvniConfig config = settings();
        Rows rows = new Rows();
        try (AvniReader cancelled = new AvniReader(config, new ReaderContext())) {
            assertTrue(cancelled.snapshotState(0L).isEmpty());
            cancelled.cancel();
            cancelled.cancel();
            cancelled.open();
            cancelled.addSplits(List.of(config.newSplit(Instant.parse("2024-01-03T00:00:00Z"))));
            cancelled.pollNext(rows);
            assertTrue(rows.values.isEmpty());
        }
        ReaderContext context = new ReaderContext();
        AvniSplit terminal = config.newSplit(Instant.parse("2024-01-03T00:00:00Z"));
        terminal.finished = true;
        try (AvniReader restored = reader(context, config)) {
            restored.addSplits(List.of(serialized(terminal)));
            restored.pollNext(rows);
            restored.pollNext(rows);
            assertTrue(context.done);
            assertTrue(restored.snapshotState(1L).get(0).finished());
            assertTrue(rows.values.isEmpty());
        }
        assertEquals(0, requests("zero-traffic"));
    }

    private static AvniSplit prefix(AvniConfig config, Rows rows) throws Exception {
        try (AvniReader first = reader(new ReaderContext(), config)) {
            assertTrue(first.snapshotState(0L).isEmpty());
            first.addSplits(List.of(config.newSplit(Instant.parse("2024-01-03T00:00:00Z"))));
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                first.pollNext(rows);
                                return rows.values.size() == 1;
                            });
            AvniSplit checkpoint = first.snapshotState(1L).get(0);
            assertEquals(1, checkpoint.row);
            assertNotNull(checkpoint.digest);
            AvniSplit saved = serialized(checkpoint);
            checkpoint.row = 0;
            assertEquals(1, first.snapshotState(2L).get(0).row);
            return saved;
        }
    }

    private static AvniReader reader(ReaderContext context, AvniConfig config) {
        AvniReader reader = new AvniReader(config, context);
        reader.open();
        return reader;
    }

    private static AvniConfig settings() {
        return new AvniConfig(
                ReadonlyConfig.fromMap(
                        Map.ofEntries(
                                Map.entry("resources", List.of("subjects")),
                                Map.entry("username", "synthetic-user+reserved@example.invalid"),
                                Map.entry("password", "synthetic-password-+&=%"),
                                Map.entry("start_date", "2024-01-01T00:00:00.000Z"),
                                Map.entry("end_date", "2024-01-01T00:00:01.000Z"),
                                Map.entry("api_origin", "https://localhost:22443"),
                                Map.entry("identity_origin", "https://localhost:22444"),
                                Map.entry("allow_custom_endpoints", true),
                                Map.entry("tls_trust_certificate", trust),
                                Map.entry("page_size", 3))));
    }

    private static void program(String name, String... bodies) throws Exception {
        List<String> lines = new ArrayList<>();
        for (String body : bodies) {
            lines.add(
                    mode(
                            reply(
                                    "/idp-details",
                                    "",
                                    "{\"cognito\":{\"clientId\":\"synthetic_avni_client\"}}"),
                            "none"));
            lines.add(
                    mode(
                            reply(
                                    "/",
                                    "",
                                    "{\"AuthenticationResult\":{\"IdToken\":\""
                                            + TOKEN
                                            + "\",\"ExpiresIn\":3600}}"),
                            "login"));
            lines.add(
                    reply(
                            "/api/subjects",
                            "page=0&size=3&lastModifiedDateTime=2024-01-01T00:00:00.000Z&now=2024-01-01T00:00:01.000Z",
                            body));
        }
        writeProgram(name, lines);
    }

    private static String mode(String step, String marker) {
        return step.substring(0, step.lastIndexOf('\t') + 1) + marker;
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
        return "{\"content\":[{\"ID\":\"subject-1\",\"name\":\"private-subject-name-a\",\"audit\":{\"Last modified at\":\"2024-01-01T00:00:00.001Z\"}},"
                + "{\"ID\":\"subject-2\",\"name\":\""
                + (changed ? "changed-private-subject-name" : "private-subject-name-b")
                + "\",\"audit\":{\"Last modified at\":\"2024-01-01T00:00:00.001Z\"}}],\"totalElements\":2,\"totalPages\":1,\"pageSize\":3}";
    }

    private static AvniSplit serialized(AvniSplit split) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(split);
        }
        assertFalse(bytes.toString(StandardCharsets.ISO_8859_1).contains(TOKEN));
        assertFalse(bytes.toString(StandardCharsets.ISO_8859_1).contains("synthetic-password"));
        assertFalse(bytes.toString(StandardCharsets.ISO_8859_1).contains("synthetic-user"));

        assertFalse(bytes.toString(StandardCharsets.ISO_8859_1).contains("private-subject-name"));
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (AvniSplit) input.readObject();
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
