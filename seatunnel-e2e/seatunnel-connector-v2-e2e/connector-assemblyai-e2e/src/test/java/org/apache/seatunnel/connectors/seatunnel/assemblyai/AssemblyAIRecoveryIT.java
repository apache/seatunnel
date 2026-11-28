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

package org.apache.seatunnel.connectors.seatunnel.assemblyai;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.e2e.connector.assemblyai.AssemblyAIPeer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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
class AssemblyAIRecoveryIT {
    private static final String TOKEN = "synthetic-assemblyai-api-key";
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
                            AssemblyAIPeer.main(new String[] {directory.toString()});
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
    void restoresSerializedChildOffsetWithoutRepeatingCommittedPrefix() throws Exception {
        program("child-offset", list(false), sentences(false), list(false), sentences(false));
        AssemblyAIConfig config = settings();
        Rows rows = new Rows();
        AssemblyAISplit saved = prefix(config, rows);
        ReaderContext context = new ReaderContext();
        try (AssemblyAIReader restored = reader(context, config)) {
            restored.addSplits(List.of(saved));
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                restored.pollNext(rows);
                                return context.done;
                            });
            assertTrue(restored.snapshotState(2L).get(0).finished);
        }
        assertEquals(2, rows.values.size());
        Set<String> ids = new HashSet<>();
        for (SeaTunnelRow row : rows.values) {
            assertEquals("transcript_sentences", row.getField(0));
            assertEquals("parent-1", row.getField(2));
            assertTrue(ids.add((String) row.getField(1)));
        }
        assertEquals(Set.of("parent-1:1", "parent-1:2"), ids);
        assertEquals(4, requests("child-offset"));
    }

    @Test
    void rejectsChangedChildContentBeforePublishingItsUnverifiedSuffix() throws Exception {
        program("changed-child", list(false), sentences(false), list(false), sentences(true));
        AssemblyAIConfig config = settings();
        AssemblyAISplit saved = prefix(config, new Rows());
        Rows suffix = new Rows();
        try (AssemblyAIReader restored = reader(new ReaderContext(), config)) {
            restored.addSplits(List.of(saved));
            AssemblyAIResource.Failure failure =
                    assertThrows(
                            AssemblyAIResource.Failure.class,
                            () -> {
                                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                                while (System.nanoTime() - deadline < 0) {
                                    restored.pollNext(suffix);
                                }
                            });
            assertTrue(failure.getMessage().contains("child rows changed since checkpoint"));
            assertTrue(suffix.values.isEmpty());
        }
        assertEquals(4, requests("changed-child"));
    }

    @Test
    void rejectsChangedParentPageBeforeQueryingAnyRestoredChild() throws Exception {
        program("changed-parent", list(false), sentences(false), list(true));
        AssemblyAIConfig config = settings();
        AssemblyAISplit saved = prefix(config, new Rows());
        Rows suffix = new Rows();
        try (AssemblyAIReader restored = reader(new ReaderContext(), config)) {
            restored.addSplits(List.of(saved));
            AssemblyAIResource.Failure failure =
                    assertThrows(
                            AssemblyAIResource.Failure.class,
                            () -> {
                                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                                while (System.nanoTime() - deadline < 0) {
                                    restored.pollNext(suffix);
                                }
                            });
            assertTrue(failure.getMessage().contains("transcripts page changed since checkpoint"));
            assertTrue(suffix.values.isEmpty());
        }
        assertEquals(3, requests("changed-parent"));
    }

    @Test
    void snapshotsBeforeAssignmentAndPreservesStickyCancellationAndTerminalState()
            throws Exception {
        program("zero-traffic", list(false), sentences(false));
        AssemblyAIConfig config = settings();
        Rows rows = new Rows();
        try (AssemblyAIReader cancelled = new AssemblyAIReader(config, new ReaderContext())) {
            assertTrue(cancelled.snapshotState(0L).isEmpty());
            cancelled.cancel();
            cancelled.cancel();
            cancelled.open();
            cancelled.addSplits(List.of(config.newSplit(Instant.parse("2024-01-03T00:00:00Z"))));
            cancelled.pollNext(rows);
            assertTrue(rows.values.isEmpty());
        }
        ReaderContext context = new ReaderContext();
        AssemblyAISplit terminal = config.newSplit(Instant.parse("2024-01-03T00:00:00Z"));
        terminal.finished = true;
        try (AssemblyAIReader restored = reader(context, config)) {
            restored.addSplits(List.of(serialized(terminal)));
            restored.pollNext(rows);
            restored.pollNext(rows);
            assertTrue(context.done);
            assertTrue(restored.snapshotState(1L).get(0).finished);
            assertTrue(rows.values.isEmpty());
        }
        assertEquals(0, requests("zero-traffic"));
    }

    private static AssemblyAISplit prefix(AssemblyAIConfig config, Rows rows) throws Exception {
        try (AssemblyAIReader first = reader(new ReaderContext(), config)) {
            assertTrue(first.snapshotState(0L).isEmpty());
            first.addSplits(List.of(config.newSplit(Instant.parse("2024-01-03T00:00:00Z"))));
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                first.pollNext(rows);
                                return rows.values.size() == 1;
                            });
            AssemblyAISplit checkpoint = first.snapshotState(1L).get(0);
            assertEquals(1, checkpoint.childRow);
            assertEquals("parent-1", checkpoint.parentId);
            assertNotNull(checkpoint.childDigest);
            assertNotNull(checkpoint.pageDigest);
            AssemblyAISplit saved = serialized(checkpoint);
            checkpoint.childRow = 0;
            assertEquals(1, first.snapshotState(2L).get(0).childRow);
            return saved;
        }
    }

    private static AssemblyAIReader reader(ReaderContext context, AssemblyAIConfig settings) {
        AssemblyAIReader reader = new AssemblyAIReader(settings, context);
        reader.open();
        return reader;
    }

    private static AssemblyAIConfig settings() {
        return new AssemblyAIConfig(
                ReadonlyConfig.fromMap(
                        Map.of(
                                "resources",
                                List.of("transcript_sentences"),
                                "token",
                                TOKEN,
                                "api_origin",
                                "https://127.0.0.1:20443",
                                "tls_trust_certificate",
                                trust,
                                "page_size",
                                2,
                                "start_date",
                                "2024-01-01T00:00:00Z",
                                "end_date",
                                "2024-01-02T00:00:00Z")));
    }

    private static void program(String name, String... bodies) throws Exception {
        List<String> lines = new ArrayList<>();
        for (int i = 0; i < bodies.length; i++) {
            boolean list = (i & 1) == 0;
            String route = list ? "/v2/transcript" : "/v2/transcript/parent-1/sentences";
            lines.add(
                    route
                            + "\t"
                            + (list ? "limit=2" : "")
                            + "\t200\tfixed\t\t"
                            + Base64.getEncoder()
                                    .encodeToString(bodies[i].getBytes(StandardCharsets.UTF_8)));
        }
        Files.write(directory.resolve(name + ".program"), lines, StandardCharsets.UTF_8);
        Files.writeString(directory.resolve("active"), name);
    }

    private static int requests(String name) throws Exception {
        assertFalse(Files.exists(directory.resolve(name + ".invalid")));
        Path journal = directory.resolve(name + ".requests");
        return Files.exists(journal) ? Files.readAllLines(journal).size() : 0;
    }

    private static String list(boolean changed) {
        return "{\"page_details\":{\"limit\":2,\"result_count\":1,\"prev_url\":null},"
                + "\"transcripts\":[{\"id\":\""
                + (changed ? "parent-2" : "parent-1")
                + "\",\"status\":\"completed\",\"created\":\"2024-01-01T12:00:00Z\","
                + "\"audio_url\":\"https://audio.invalid/not-fetched\"}]}";
    }

    private static String sentences(boolean changed) {
        return "{\"id\":\"parent-1\",\"sentences\":[{\"text\":\"private-child-value-a\"},"
                + "{\"text\":\""
                + (changed ? "changed-private-child-value" : "private-child-value-b")
                + "\"}]}";
    }

    private static AssemblyAISplit serialized(AssemblyAISplit split) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(split);
        }
        assertFalse(bytes.toString(StandardCharsets.ISO_8859_1).contains(TOKEN));
        assertFalse(bytes.toString(StandardCharsets.ISO_8859_1).contains("private-child-value"));
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (AssemblyAISplit) input.readObject();
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
                                "CN=127.0.0.1",
                                "-ext",
                                "SAN=ip:127.0.0.1",
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
