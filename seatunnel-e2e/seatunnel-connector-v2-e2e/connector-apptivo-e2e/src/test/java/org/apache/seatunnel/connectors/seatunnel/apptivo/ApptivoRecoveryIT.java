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

package org.apache.seatunnel.connectors.seatunnel.apptivo;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.e2e.connector.apptivo.ApptivoPeer;

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
class ApptivoRecoveryIT {
    private static final String TOKEN = "synthetic-apptivo-api-key+/=&?#";
    private static final String ACCESS_KEY = "synthetic-apptivo-access-key+/=&?#";
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
                            ApptivoPeer.main(new String[] {directory.toString()});
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
    void resumesSerializedOffsetWithoutRepeatingTheCommittedPrefix() throws Exception {
        program("partial-offset", page(false), page(false));
        ApptivoConfig settings = settings();
        Rows output = new Rows();
        ApptivoSplit saved;
        try (ApptivoReader first = reader(new ReaderContext(), settings)) {
            assertTrue(first.snapshotState(0L).isEmpty());
            first.addSplits(List.of(new ApptivoSplit(settings)));
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                first.pollNext(output);
                                return output.values.size() >= 1;
                            });
            assertEquals(1, output.values.size());
            ApptivoSplit copy = first.snapshotState(1L).get(0);
            assertEquals(1, copy.row);
            assertNotNull(copy.digest);
            saved = serialized(copy);
            copy.row = 0;
            assertEquals(1, first.snapshotState(2L).get(0).row);
        }
        ReaderContext resumed = new ReaderContext();
        try (ApptivoReader second = reader(resumed, settings)) {
            second.addSplits(List.of(saved));
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                second.pollNext(output);
                                return resumed.done;
                            });
            assertTrue(second.snapshotState(3L).get(0).finished(settings));
        }
        assertEquals(2, output.values.size());
        Set<String> ids = new HashSet<>();
        for (SeaTunnelRow row : output.values) {
            assertEquals("customers", row.getField(0));
            assertTrue(ids.add((String) row.getField(1)), "a restored row was repeated");
        }
        for (int id = 1; id <= 2; id++) {
            assertTrue(ids.contains(Integer.toString(id)));
        }
        assertEquals(2, requests("partial-offset"));
    }

    @Test
    void rejectsChangedPageBeforePublishingAnUnverifiedSuffix() throws Exception {
        program("changed-page", page(false), page(true));
        ApptivoConfig settings = settings();
        ApptivoSplit saved;
        try (ApptivoReader first = reader(new ReaderContext(), settings)) {
            first.addSplits(List.of(new ApptivoSplit(settings)));
            Rows prefix = new Rows();
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                first.pollNext(prefix);
                                return prefix.values.size() == 1;
                            });
            saved = serialized(first.snapshotState(1L).get(0));
        }
        Rows suffix = new Rows();
        try (ApptivoReader second = reader(new ReaderContext(), settings)) {
            second.addSplits(List.of(saved));
            ApptivoResource.Failure failure =
                    assertThrows(
                            ApptivoResource.Failure.class,
                            () -> {
                                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                                while (System.nanoTime() - deadline < 0) {
                                    second.pollNext(suffix);
                                }
                            });
            assertTrue(failure.getMessage().contains("saved page changed since checkpoint"));
            assertTrue(suffix.values.isEmpty());
        }
        assertEquals(2, requests("changed-page"));
    }

    @Test
    void cancellationBeforeAssignmentAndTerminalRestoreNeverOpenTheTransport() throws Exception {
        program("no-traffic", page(false));
        ApptivoConfig settings = settings();
        Rows output = new Rows();
        try (ApptivoReader cancelled = new ApptivoReader(settings, new ReaderContext())) {
            assertTrue(cancelled.snapshotState(0L).isEmpty());
            cancelled.cancel();
            cancelled.cancel();
            cancelled.open();
            cancelled.addSplits(List.of(new ApptivoSplit(settings)));
            cancelled.pollNext(output);
            assertTrue(output.values.isEmpty());
        }
        ReaderContext terminal = new ReaderContext();
        ApptivoSplit done = new ApptivoSplit(settings);
        done.resource = settings.resources.size();
        try (ApptivoReader restored = reader(terminal, settings)) {
            restored.addSplits(List.of(serialized(done)));
            restored.pollNext(output);
            restored.pollNext(output);
            assertTrue(terminal.done);
            assertTrue(restored.snapshotState(1L).get(0).finished(settings));
            assertTrue(output.values.isEmpty());
        }
        assertEquals(0, requests("no-traffic"));
    }

    private static ApptivoReader reader(ReaderContext context, ApptivoConfig settings) {
        ApptivoReader reader = new ApptivoReader(settings, context);
        reader.open();
        return reader;
    }

    private static ApptivoConfig settings() {
        return new ApptivoConfig(
                ReadonlyConfig.fromMap(
                        Map.of(
                                "resources",
                                List.of("customers"),
                                "token",
                                TOKEN,
                                "access_key",
                                ACCESS_KEY,
                                "api_origin",
                                "https://127.0.0.1:19443",
                                "tls_trust_certificate",
                                trust,
                                "page_size",
                                2)));
    }

    private static ApptivoSplit serialized(ApptivoSplit split) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(split);
        }
        assertFalse(bytes.toString(StandardCharsets.ISO_8859_1).contains(TOKEN));
        assertFalse(bytes.toString(StandardCharsets.ISO_8859_1).contains(ACCESS_KEY));
        assertFalse(bytes.toString(StandardCharsets.ISO_8859_1).contains("customerId"));
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (ApptivoSplit) input.readObject();
        }
    }

    private static void program(String name, String... bodies) throws Exception {
        List<String> lines = new ArrayList<>();
        for (String body : bodies) {
            lines.add(
                    "/app/dao/v6/customers\ta=getAll&numRecords=2&startIndex=0\t200\tfixed\t\t"
                            + Base64.getEncoder()
                                    .encodeToString(body.getBytes(StandardCharsets.UTF_8)));
        }
        Files.write(directory.resolve(name + ".program"), lines, StandardCharsets.UTF_8);
        Files.writeString(directory.resolve("active"), name);
    }

    private static int requests(String name) throws Exception {
        assertFalse(Files.exists(directory.resolve(name + ".invalid")));
        Path journal = directory.resolve(name + ".requests");
        return Files.exists(journal) ? Files.readAllLines(journal).size() : 0;
    }

    private static String page(boolean changed) {
        List<String> rows = new ArrayList<>();
        for (int id = 1; id <= 2; id++) {
            rows.add(
                    "{\"customerId\":"
                            + id
                            + ",\"firmId\":101,\"value\":"
                            + (changed && id == 2 ? 9 : 8)
                            + "}");
        }
        return "{\"status\":\"SUCCESS\",\"startIndex\":0,\"totalRecords\":2,\"firmId\":101,\"data\":["
                + String.join(",", rows)
                + "]}";
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
