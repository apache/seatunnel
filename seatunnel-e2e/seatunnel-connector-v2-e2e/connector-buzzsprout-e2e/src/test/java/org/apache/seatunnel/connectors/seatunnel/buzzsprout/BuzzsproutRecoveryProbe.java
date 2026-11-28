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
package org.apache.seatunnel.connectors.seatunnel.buzzsprout;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.buzzsprout.BuzzsproutEnumerator.Snapshot;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Restores a complete-list chunk through the packaged reader and independent HTTPS peer.
 *
 * <p>The 32/33-row boundary supplements real Zeta savepoints. Replay checks include even filtered
 * provider rows, so a changed full list cannot silently resume its former output offset.
 */
public final class BuzzsproutRecoveryProbe {
    private BuzzsproutRecoveryProbe() {}

    /**
     * Checks exact serialized replay, changed raw arrays and quiet assignment lifecycle.
     *
     * <p>This entry point runs only in the CI container with its owned explicit trust anchor.
     */
    public static void main(String[] args) throws Exception {
        BuzzsproutConfig config = config(args[0], "synthetic-buzzsprout-token");
        if (args[1].equals("zero")) {
            zero(config, args[0]);
            return;
        }
        Rows before = new Rows();
        BuzzsproutSplit saved;
        try (BuzzsproutReader first = opened(config, new Context())) {
            require(first.snapshotState(0L).isEmpty(), "unassigned checkpoint must remain empty");
            first.addSplits(List.of(new BuzzsproutSplit(config)));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (before.rows.isEmpty() && System.nanoTime() - deadline < 0) {
                first.pollNext(before);
                Thread.sleep(5L);
            }
            require(before.rows.size() == 32, "one poll must publish exactly one 32-row chunk");
            saved = copy(first.snapshotState(1L).get(0));
            require(
                    saved.published == 32 && saved.fingerprint != null,
                    "row offset and full-array fingerprint must survive serialization");
        }
        String encoded = new String(encode(saved), StandardCharsets.ISO_8859_1);
        require(
                !encoded.contains("synthetic-buzzsprout-token")
                        && !encoded.contains("published_at"),
                "state must omit credentials and provider bodies");
        Rows after = new Rows();
        Context context = new Context();
        boolean rejected = false;
        try (BuzzsproutReader restored = opened(config, context)) {
            restored.addSplits(List.of(saved));
            await(restored, after, context);
            require(args[1].equals("resume"), "changed raw array must not complete successfully");
            require(
                    after.rows.size() == 1 && "1032".equals(after.rows.get(0).getField(1)),
                    "only the last unpublished selected record may be replayed");
            Set<String> ids = new HashSet<>();
            for (SeaTunnelRow row : before.rows) {
                require(ids.add((String) row.getField(1)), "duplicate initial row");
            }
            require(
                    ids.add((String) after.rows.get(0).getField(1)) && ids.size() == 33,
                    "all 33 selected identities must appear exactly once");
            BuzzsproutSplit terminal = copy(restored.snapshotState(2L).get(0));
            require(
                    terminal.resourceIndex == config.resources.size(),
                    "completed state must be terminal");
            Context finished = new Context();
            try (BuzzsproutReader reader = opened(config, finished)) {
                reader.addSplits(List.of(terminal));
                reader.pollNext(new Rows());
                require(finished.done, "terminal restore must not reopen the API");
            }
        } catch (BuzzsproutReadException failure) {
            require(
                    args[1].startsWith("changed")
                            && failure.getMessage().contains("provider list changed"),
                    "rejection must identify an actual replay change");
            rejected = true;
        }
        if (args[1].startsWith("changed")) {
            require(
                    rejected && after.rows.isEmpty(),
                    "changed raw arrays must fail before publishing any suffix");
        }
    }

    private static BuzzsproutConfig config(String certificate, String token) {
        Map<String, Object> values = new HashMap<>();
        values.put("token", token);
        values.put("resources", List.of("episodes"));
        values.put("podcast_id", "9007199254740993");
        values.put("start_date", "2024-01-01T00:00:00Z");
        values.put("end_date", "2024-01-02T00:00:00Z");
        values.put("endpoint", "https://www.buzzsprout.com");
        values.put("trust_certificate", certificate);
        values.put("max_attempts", 1);
        return new BuzzsproutConfig(ReadonlyConfig.fromMap(values));
    }

    private static void zero(BuzzsproutConfig config, String certificate) throws Exception {
        Context empty = new Context();
        try (BuzzsproutReader reader = opened(config, empty)) {
            reader.handleNoMoreSplits();
            reader.pollNext(new Rows());
            require(
                    empty.done && reader.snapshotState(0L).isEmpty(),
                    "unassigned EOF must not manufacture work");
        }
        BuzzsproutSplit terminal = new BuzzsproutSplit(config);
        while (terminal.resourceIndex < config.resources.size()) {
            terminal = terminal.next();
        }
        Context done = new Context();
        try (BuzzsproutReader reader = opened(config, done)) {
            reader.addSplits(List.of(copy(terminal)));
            reader.pollNext(new Rows());
            require(done.done, "terminal state must finish without HTTPS");
        }
        boolean rejected = false;
        try (BuzzsproutReader reader =
                opened(config(certificate, "different-buzzsprout-key"), new Context())) {
            reader.addSplits(List.of(new BuzzsproutSplit(config)));
        } catch (IllegalArgumentException expected) {
            rejected = true;
        }
        require(rejected, "credential scope change must fail before network");
        BuzzsproutReader cancelled =
                new BuzzsproutReader(new Context(), config, "buzzsprout_probe");
        cancelled.cancel();
        cancelled.open();
        cancelled.pollNext(new Rows());
        cancelled.close();
        require(
                cancelled.snapshotState(0L).isEmpty(),
                "sticky cancel-before-open must retain empty progress");
        List<BuzzsproutSplit> assignments = new ArrayList<>();
        Set<Integer> readers = new HashSet<>();
        SourceSplitEnumerator.Context<BuzzsproutSplit> context =
                enumeratorContext(readers, assignments);
        Snapshot pending;
        try (BuzzsproutEnumerator first = new BuzzsproutEnumerator(context, config, null)) {
            first.open();
            first.run();
            pending = copySnapshot(first.snapshotState(0L));
            require(
                    first.currentUnassignedSplitSize() == 1 && assignments.isEmpty(),
                    "coordinator must retain unassigned work");
        }
        readers.add(0);
        try (BuzzsproutEnumerator restored = new BuzzsproutEnumerator(context, config, pending)) {
            restored.open();
            restored.run();
            restored.registerReader(0);
            require(
                    assignments.size() == 1 && restored.currentUnassignedSplitSize() == 0,
                    "restored pending work must be assigned only once");
        }
    }

    @SuppressWarnings("unchecked")
    private static SourceSplitEnumerator.Context<BuzzsproutSplit> enumeratorContext(
            Set<Integer> readers, List<BuzzsproutSplit> assignments) {
        return (SourceSplitEnumerator.Context<BuzzsproutSplit>)
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
                                    assignments.add((BuzzsproutSplit) args[1]);
                                    return null;
                                case "signalNoMoreSplits":
                                    return null;
                                default:
                                    throw new AssertionError(
                                            "Unexpected coordinator operation " + method.getName());
                            }
                        });
    }

    private static BuzzsproutReader opened(BuzzsproutConfig config, Context context) {
        BuzzsproutReader reader = new BuzzsproutReader(context, config, "buzzsprout_probe");
        reader.open();
        return reader;
    }

    private static void await(BuzzsproutReader reader, Rows rows, Context context)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!context.done && System.nanoTime() - deadline < 0) {
            reader.pollNext(rows);
            Thread.sleep(5L);
        }
        require(context.done, "bounded reader did not finish");
    }

    private static byte[] encode(Object value) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(value);
        }
        return bytes.toByteArray();
    }

    private static BuzzsproutSplit copy(BuzzsproutSplit split) throws Exception {
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(encode(split)))) {
            return (BuzzsproutSplit) input.readObject();
        }
    }

    private static Snapshot copySnapshot(Snapshot state) throws Exception {
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(encode(state)))) {
            return (Snapshot) input.readObject();
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
