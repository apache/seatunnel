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
package org.apache.seatunnel.connectors.seatunnel.microsoftadvertising;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Checks exact file-row and terminal checkpoints against the packaged connector and real HTTPS.
 *
 * <p>These container-only reader cases supplement engine jobs where a row-internal snapshot cannot
 * be scheduled deterministically. They never substitute a mock transport for the provider exchange.
 */
public final class MicrosoftAdvertisingRecoveryProbe {
    private static final ObjectMapper JSON = new ObjectMapper();

    private MicrosoftAdvertisingRecoveryProbe() {}

    /**
     * Restores a serialized real reader position and verifies duplicate-free or fail-closed replay.
     */
    public static void main(String[] args) throws Exception {
        MicrosoftAdvertisingConfig config = config(args[0], args[2]);
        if (args[1].equals("zero")) {
            zero(config);
            return;
        }
        Rows before = new Rows();
        MicrosoftAdvertisingSplit saved;
        try (MicrosoftAdvertisingReader reader = opened(config, new Context())) {
            require(
                    reader.snapshotState(0L).isEmpty(),
                    "unassigned reader checkpoint must be empty");
            reader.addSplits(List.of(config.newSplit(Instant.now())));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (before.values.isEmpty() && System.nanoTime() - deadline < 0) {
                reader.pollNext(before);
                Thread.sleep(5L);
            }
            require(before.values.size() == 1, "exactly one file row must precede this boundary");
            saved = serialized(reader.snapshotState(1L).get(0));
            require(
                    saved.file.row == 1 && saved.file.digest != null,
                    "checkpoint must retain consumed CSV position");
            require(
                    saved.jobId != null && saved.jobReady,
                    "file restore must retain the confirmed job");
        }
        byte[] encoded = encode(saved);
        String serialized = new String(encoded, StandardCharsets.ISO_8859_1);
        for (String secret :
                List.of(
                        "synthetic-advertising-access-token",
                        "synthetic-advertising-developer-token",
                        "synthetic-advertising-refresh+&=%",
                        "synthetic-advertising-secret+&=%",
                        "signed-download.example",
                        "sig=opaque")) {
            require(
                    !serialized.contains(secret),
                    "checkpoint must exclude credentials and signed URLs");
        }
        Rows after = new Rows();
        Context context = new Context();
        boolean rejected = false;
        try (MicrosoftAdvertisingReader reader = opened(config, context)) {
            reader.addSplits(List.of(saved));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (!context.done && System.nanoTime() - deadline < 0) {
                reader.pollNext(after);
                Thread.sleep(5L);
            }
            if (args[1].equals("resume")) {
                require(context.done, "restored reader must finish");
                require(
                        after.values.size() == 1,
                        "restore must emit only the uncommitted file suffix");
                require(
                        !before.values.get(0).getField(4).equals(after.values.get(0).getField(4)),
                        "distinct CSV rows must survive");
                MicrosoftAdvertisingSplit terminal = serialized(reader.snapshotState(2L).get(0));
                require(terminal.finished(), "terminal position must survive serialization");
                Context restoredContext = new Context();
                try (MicrosoftAdvertisingReader restored = opened(config, restoredContext)) {
                    restored.addSplits(List.of(terminal));
                    restored.pollNext(new Rows());
                    require(
                            restoredContext.done,
                            "terminal restore must finish without provider traffic");
                }
            }
        } catch (MicrosoftAdvertisingFailure failure) {
            require(!args[1].equals("resume"), "normal file resume must not fail");
            String expected = args[1].equals("user-change") ? "user" : "changed";
            require(
                    failure.getMessage().contains(expected),
                    "restore must identify the actual incompatible state");
            rejected = true;
        }
        if (!args[1].equals("resume")) {
            require(rejected, "changed response or OAuth user must fail explicitly");
            require(after.values.isEmpty(), "rejected replay must publish no suffix row");
        }
    }

    private static MicrosoftAdvertisingConfig config(String certificate, String day)
            throws Exception {
        Map<String, Object> values = new HashMap<>();
        values.put("developer_token", "synthetic-advertising-developer-token");
        values.put("client_id", "synthetic-advertising-client");
        values.put("client_secret", "synthetic-advertising-secret+&=%");
        values.put("refresh_token", "synthetic-advertising-refresh+&=%");
        values.put("tls_trust_certificate", Files.readString(Path.of(certificate)));
        values.put("reports_start_date", day);
        values.put("reports_end_date", day);
        values.put("resources", List.of("probe"));
        values.put(
                "custom_reports",
                List.of(
                        JSON.readTree(
                                "{\"name\":\"probe\",\"reporting_object\":\"AccountPerformanceReportRequest\",\"report_columns\":[\"AccountId\",\"Clicks\"],\"report_aggregation\":\"Daily\"}")));
        values.put("max_retries", 0);
        values.put("poll_interval_ms", 10);
        return new MicrosoftAdvertisingConfig(ReadonlyConfig.fromMap(values));
    }

    private static void zero(MicrosoftAdvertisingConfig config) throws Exception {
        MicrosoftAdvertisingSplit terminal = config.newSplit(Instant.now());
        terminal.userId = "9007199254740997";
        terminal.discovering = false;
        terminal.complete = true;
        Context context = new Context();
        try (MicrosoftAdvertisingReader reader = opened(config, context)) {
            require(reader.snapshotState(0L).isEmpty(), "no implicit split may be manufactured");
            reader.addSplits(List.of(serialized(terminal)));
            reader.pollNext(new Rows());
            require(context.done, "terminal state must signal bounded completion without network");
        }
        MicrosoftAdvertisingSplit invalid = config.newSplit(Instant.now());
        invalid.file.row = -1;
        boolean rejected = false;
        try (MicrosoftAdvertisingReader reader = opened(config, new Context())) {
            reader.addSplits(List.of(invalid));
            reader.pollNext(new Rows());
        } catch (MicrosoftAdvertisingFailure failure) {
            rejected = true;
        }
        require(rejected, "invalid saved row must fail before HTTP");
        Context empty = new Context();
        try (MicrosoftAdvertisingReader reader = opened(config, empty)) {
            reader.handleNoMoreSplits();
            reader.pollNext(new Rows());
            require(
                    empty.done && reader.snapshotState(0L).isEmpty(),
                    "an unassigned reader must finish without creating a split");
        }
        MicrosoftAdvertisingSplit wrongScope =
                new MicrosoftAdvertisingSplit(
                        "different-scope",
                        terminal.asOf,
                        terminal.start,
                        terminal.end,
                        terminal.bulkFrom);
        rejectedBeforeNetwork(config, wrongScope, "scope");
        MicrosoftAdvertisingSplit uncertain = active(config);
        uncertain.submitPending = true;
        uncertain.jobCreatedAt = Instant.now();
        rejectedBeforeNetwork(config, uncertain, "unknown outcome");
        MicrosoftAdvertisingSplit expired = active(config);
        expired.jobId = "expired confirmed job";
        expired.jobCreatedAt = Instant.now().minusSeconds(172800L);
        rejectedBeforeNetwork(config, expired, "expired");
    }

    private static MicrosoftAdvertisingSplit active(MicrosoftAdvertisingConfig config) {
        MicrosoftAdvertisingSplit split = config.newSplit(Instant.now());
        split.userId = "9007199254740997";
        split.discovering = false;
        split.resourceIndex = 0;
        split.accounts.add(
                new MicrosoftAdvertisingSplit.Account("9007199254740993", "9007199254740995"));
        return split;
    }

    private static void rejectedBeforeNetwork(
            MicrosoftAdvertisingConfig config, MicrosoftAdvertisingSplit split, String reason)
            throws Exception {
        boolean rejected = false;
        try (MicrosoftAdvertisingReader reader = opened(config, new Context())) {
            reader.addSplits(List.of(serialized(split)));
            reader.pollNext(new Rows());
        } catch (MicrosoftAdvertisingFailure failure) {
            require(
                    failure.getMessage().contains(reason),
                    "saved state must fail for its actual invalid boundary");
            rejected = true;
        }
        require(rejected, "invalid restored state must fail before network access");
    }

    private static MicrosoftAdvertisingReader opened(
            MicrosoftAdvertisingConfig config, Context context) throws Exception {
        MicrosoftAdvertisingReader reader = new MicrosoftAdvertisingReader(config, context);
        reader.open();
        return reader;
    }

    private static byte[] encode(MicrosoftAdvertisingSplit split) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(split);
        }
        return bytes.toByteArray();
    }

    private static MicrosoftAdvertisingSplit serialized(MicrosoftAdvertisingSplit split)
            throws Exception {
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(encode(split)))) {
            return (MicrosoftAdvertisingSplit) input.readObject();
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    /**
     * Collects actual connector rows while providing the same checkpoint lock as an engine task.
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
     * Records bounded completion from the real packaged reader without mocking its state.
     *
     * <p>The probe supplies no metrics service or enumerator-side behavior.
     */
    private static final class Context implements SourceReader.Context {
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
