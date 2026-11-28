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
package org.apache.seatunnel.connectors.seatunnel.calendly;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.calendly.CalendlySource.Position;
import org.apache.seatunnel.connectors.seatunnel.calendly.CalendlySource.Split;
import org.apache.seatunnel.connectors.seatunnel.calendly.CalendlySource.State;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Restores an exact raw-page publication chunk with the packaged source and real HTTPS.
 *
 * <p>This deterministic 32/33-row boundary supplements real Zeta parent, cursor, OAuth and terminal
 * savepoints. No provider acquisition, projection or digest is replaced by a mock.
 */
public final class CalendlyRecoveryProbe {
    private CalendlyRecoveryProbe() {}

    /**
     * Checks unchanged replay, changed selected or filtered rows, or quiet lifecycle completion.
     *
     * <p>The owning integration test supplies independent provider responses and request
     * assertions.
     */
    public static void main(String[] args) throws Exception {
        CalendlyConfig config = config(args[0], "synthetic-calendly-token");
        if (args[1].equals("zero")) {
            zero(config, args[0]);
            return;
        }
        Rows before = new Rows();
        Split saved;
        try (CalendlyReader first = opened(config, new Context())) {
            require(
                    first.snapshotState(0L).isEmpty(),
                    "fresh checkpoint must contain no invented split");
            first.addSplits(List.of(new Split(config.scope)));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (before.rows.isEmpty() && System.nanoTime() - deadline < 0) {
                first.pollNext(before);
                Thread.sleep(5L);
            }
            require(before.rows.size() == 32, "one poll must publish exactly the first32 rows");
            saved = copy(first.snapshotState(1L).get(0));
            require(
                    saved.leaf.offset == 32 && saved.leaf.digest != null && saved.subject != null,
                    "serialized state must retain raw row offset, full-page fingerprint and authenticated subject");
        }
        String encoded = new String(encode(saved), StandardCharsets.ISO_8859_1);
        for (String secret :
                List.of(
                        "synthetic-calendly-token",
                        "synthetic-calendly-secret",
                        "refresh-1+&=%",
                        "updated_at")) {
            require(
                    !encoded.contains(secret),
                    "checkpoint must omit credentials and raw provider records");
        }
        Context context = new Context();
        Rows after = new Rows();
        boolean rejected = false;
        try (CalendlyReader restored = opened(config, context)) {
            restored.addSplits(List.of(saved));
            await(restored, after, context);
            require(args[1].equals("resume"), "changed pages must fail before completion");
            require(
                    after.rows.size() == 1
                            && "00000000-0000-4000-8000-000000001032"
                                    .equals(after.rows.get(0).getField(0)),
                    "restore must publish only the final uncommitted selected row");
            Set<String> ids = new HashSet<>();
            for (SeaTunnelRow row : before.rows) {
                require(ids.add((String) row.getField(0)), "duplicate first-chunk identity");
            }
            require(
                    ids.add((String) after.rows.get(0).getField(0)) && ids.size() == 33,
                    "all33 selected identities must appear exactly once");
            Split terminal = copy(restored.snapshotState(2L).get(0));
            require(terminal.finished, "final progress must be terminal");
            Context finished = new Context();
            try (CalendlyReader reader = opened(config, finished)) {
                reader.addSplits(List.of(terminal));
                reader.pollNext(new Rows());
                require(
                        finished.done,
                        "terminal replay must finish without identity or token traffic");
            }
        } catch (IOException failure) {
            require(
                    args[1].startsWith("changed") && failure.getMessage().contains("page changed"),
                    "only a precise raw-page replay mismatch is expected");
            rejected = true;
        }
        if (args[1].startsWith("changed")) {
            require(
                    rejected && after.rows.isEmpty(),
                    "changed selected or filtered records must reject before suffix publication");
        }
    }

    private static CalendlyConfig config(String certificate, String token) {
        Map<String, Object> values = new HashMap<>();
        values.put("token", token);
        values.put("resource", "event_types");
        values.put("start_date", "2024-01-01T00:00:00Z");
        values.put("end_date", "2024-01-02T00:00:00Z");
        values.put("incremental_start", "2024-01-01T00:00:00Z");
        values.put("page_size", 34);
        values.put("max_attempts", 1);
        values.put("tls_trust_certificate", certificate);
        return new CalendlyConfig(ReadonlyConfig.fromMap(values));
    }

    private static void zero(CalendlyConfig config, String certificate) throws Exception {
        Context unassigned = new Context();
        try (CalendlyReader reader = opened(config, unassigned)) {
            reader.handleNoMoreSplits();
            reader.pollNext(new Rows());
            require(
                    unassigned.done && reader.snapshotState(0L).isEmpty(),
                    "unassigned EOF must not manufacture a scan");
        }
        Context terminal = new Context();
        try (CalendlyReader reader = opened(config, terminal)) {
            reader.addSplits(
                    List.of(
                            copy(
                                    new Split(
                                            config.scope,
                                            null,
                                            Instant.parse("2024-01-02T00:00:00Z"),
                                            0,
                                            new Position(),
                                            new Position(),
                                            true))));
            reader.pollNext(new Rows());
            require(terminal.done, "terminal saved progress must finish quietly");
        }
        boolean rejected = false;
        try (CalendlyReader wrong =
                opened(config(certificate, "different-calendly-key"), new Context())) {
            wrong.addSplits(List.of(new Split(config.scope)));
        } catch (IllegalArgumentException expected) {
            rejected = true;
        }
        require(rejected, "changed credential scope must fail before either origin is contacted");
        CalendlyReader cancelled = new CalendlyReader(config, new Context(), "calendly_probe");
        cancelled.cancel();
        cancelled.open();
        cancelled.pollNext(new Rows());
        cancelled.close();
        require(cancelled.snapshotState(0L).isEmpty(), "cancel-before-open must remain sticky");
        Set<Integer> readers = new HashSet<>();
        List<Split> assignments = new ArrayList<>();
        SourceSplitEnumerator.Context<Split> context = enumeratorContext(readers, assignments);
        CalendlySource source = new CalendlySource(config);
        State saved;
        try (SourceSplitEnumerator<Split, State> first = source.createEnumerator(context)) {
            first.open();
            first.run();
            saved = copyState(first.snapshotState(1L));
            require(
                    saved.pending != null
                            && first.currentUnassignedSplitSize() == 1
                            && assignments.isEmpty(),
                    "unassigned coordinator checkpoint must retain its traversal");
        }
        readers.add(0);
        try (SourceSplitEnumerator<Split, State> restored =
                source.restoreEnumerator(context, saved)) {
            restored.open();
            restored.run();
            restored.registerReader(0);
            require(
                    assignments.size() == 1 && restored.currentUnassignedSplitSize() == 0,
                    "restored pending traversal must be assigned only once");
        }
    }

    @SuppressWarnings("unchecked")
    private static SourceSplitEnumerator.Context<Split> enumeratorContext(
            Set<Integer> readers, List<Split> assignments) {
        return (SourceSplitEnumerator.Context<Split>)
                Proxy.newProxyInstance(
                        SourceSplitEnumerator.Context.class.getClassLoader(),
                        new Class<?>[] {SourceSplitEnumerator.Context.class},
                        (proxy, method, args) -> {
                            switch (method.getName()) {
                                case "currentParallelism":
                                    return 1;
                                case "registeredReaders":
                                    return readers;
                                case "assignSplit":
                                    assignments.add((Split) args[1]);
                                    return null;
                                case "signalNoMoreSplits":
                                    return null;
                                default:
                                    throw new AssertionError(
                                            "Unexpected coordinator operation " + method.getName());
                            }
                        });
    }

    private static CalendlyReader opened(CalendlyConfig config, Context context) {
        CalendlyReader reader = new CalendlyReader(config, context, "calendly_probe");
        reader.open();
        return reader;
    }

    private static void await(CalendlyReader reader, Rows rows, Context context) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!context.done && System.nanoTime() - deadline < 0) {
            reader.pollNext(rows);
            Thread.sleep(5L);
        }
        require(context.done, "bounded reader failed to finish");
    }

    private static byte[] encode(Object value) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(value);
        }
        return bytes.toByteArray();
    }

    private static Split copy(Split split) throws Exception {
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(encode(split)))) {
            return (Split) input.readObject();
        }
    }

    private static State copyState(State state) throws Exception {
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(encode(state)))) {
            return (State) input.readObject();
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    /**
     * Captures rows published by the actual reader under its normal checkpoint lock.
     *
     * <p>The HTTPS process remains independent from this collector and saved progress.
     */
    private static final class Rows implements Collector<SeaTunnelRow> {
        private final Object lock = new Object();
        private final List<SeaTunnelRow> rows = new ArrayList<>();

        @Override
        public void collect(SeaTunnelRow row) {
            rows.add(row);
        }

        @Override
        public Object getCheckpointLock() {
            return lock;
        }
    }

    /**
     * Observes bounded completion without replacing source, split or transport behavior.
     *
     * <p>All provider traffic is still performed by the packaged connector in the CI container.
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
