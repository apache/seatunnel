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
package org.apache.seatunnel.connectors.seatunnel.buildkite;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.buildkite.BuildkiteResource.Failure;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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
 * Restores the packaged reader immediately after collection and before advancing its row.
 *
 * <p>Real HTTPS and Java serialization supplement the integration test's real Zeta savepoints. No
 * connector resource, serializer or fingerprint supplies expected provider responses.
 */
public final class BuildkiteRecoveryProbe {
    private static final Instant END = Instant.parse("2024-01-02T00:00:00Z");

    private BuildkiteRecoveryProbe() {}

    /**
     * Verifies one-row publication replay, changed-page rejection or quiet lifecycle transitions.
     *
     * <p>The caller supplies a container-private explicit certificate and an independent peer.
     */
    public static void main(String[] args) throws Exception {
        BuildkiteConfig config = config(args[0], "synthetic-buildkite-token");
        if (args[1].equals("zero")) {
            zero(config, args[0]);
            return;
        }
        Rows before = new Rows();
        BuildkiteSplit saved;
        try (BuildkiteReader first = opened(config, new Context())) {
            require(first.snapshotState(0L).isEmpty(), "unassigned checkpoint must stay empty");
            first.addSplits(List.of(config.newSplit(END)));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (before.rows.isEmpty() && System.nanoTime() - deadline < 0) {
                first.pollNext(before);
                Thread.sleep(5L);
            }
            require(
                    before.rows.size() == 1 && "probe-1".equals(before.rows.get(0).getField(1)),
                    "first publication must contain exactly the first provider row");
            saved = copy(first.snapshotState(1L).get(0));
            require(
                    saved.frames.size() == 1
                            && saved.frames.get(0).row == 0
                            && saved.frames.get(0).emitted
                            && saved.frames.get(0).digest != null,
                    "checkpoint must retain emitted=true before row advancement");
        }
        String encoded = new String(encode(saved), StandardCharsets.ISO_8859_1);
        require(
                !encoded.contains("synthetic-buildkite-token") && !encoded.contains("probe-1"),
                "checkpoint must omit provider rows and credentials");
        Rows after = new Rows();
        Context context = new Context();
        boolean rejected = false;
        try (BuildkiteReader restored = opened(config, context)) {
            restored.addSplits(List.of(saved));
            await(restored, after, context);
            require(args[1].equals("resume"), "changed replay must fail");
            require(
                    after.rows.size() == 1 && "probe-2".equals(after.rows.get(0).getField(1)),
                    "only the unpublished second row may survive restore");
            BuildkiteSplit terminal = copy(restored.snapshotState(2L).get(0));
            require(terminal.finished(), "completed traversal must serialize terminal progress");
            Context finished = new Context();
            try (BuildkiteReader reader = opened(config, finished)) {
                reader.addSplits(List.of(terminal));
                reader.pollNext(new Rows());
                require(finished.done, "terminal restore must finish without another HTTPS call");
            }
        } catch (Failure failure) {
            require(
                    args[1].equals("changed")
                            && failure.getMessage().contains("checkpoint page changed"),
                    "only a precise replay-content failure is expected");
            rejected = true;
        }
        if (args[1].equals("changed")) {
            require(
                    rejected && after.rows.isEmpty(),
                    "changed response must fail before replay emits any suffix");
        }
    }

    private static BuildkiteConfig config(String certificate, String token) {
        Map<String, Object> values = new HashMap<>();
        values.put("token", token);
        values.put("resources", List.of("builds"));
        values.put("start_date", "2024-01-01T00:00:00Z");
        values.put("end_date", END.toString());
        values.put("tls_trust_certificate", certificate);
        values.put("max_retries", 0);
        values.put("request_interval_ms", 0);
        return new BuildkiteConfig(ReadonlyConfig.fromMap(values));
    }

    private static void zero(BuildkiteConfig config, String certificate) throws Exception {
        Context empty = new Context();
        try (BuildkiteReader reader = opened(config, empty)) {
            reader.handleNoMoreSplits();
            reader.pollNext(new Rows());
            require(
                    empty.done && reader.snapshotState(0L).isEmpty(),
                    "unassigned EOF must not manufacture a scan");
        }
        BuildkiteSplit terminal = config.newSplit(END);
        while (!terminal.finished()) {
            terminal.nextRoot(config);
        }
        Context done = new Context();
        try (BuildkiteReader reader = opened(config, done)) {
            reader.addSplits(List.of(copy(terminal)));
            reader.pollNext(new Rows());
            require(done.done, "terminal assigned reader must finish quietly");
        }
        boolean rejected = false;
        try (BuildkiteReader reader =
                opened(config(certificate, "changed-buildkite-key"), new Context())) {
            reader.addSplits(List.of(config.newSplit(END)));
        } catch (Failure expected) {
            rejected = true;
        }
        require(rejected, "changed credentials must reject restore before network");
        BuildkiteReader cancelled = new BuildkiteReader(config, new Context());
        cancelled.cancel();
        cancelled.open();
        cancelled.pollNext(new Rows());
        cancelled.close();
        require(
                cancelled.snapshotState(0L).isEmpty(),
                "sticky cancellation before open must not create work");
        List<BuildkiteSplit> assigned = new ArrayList<>();
        Set<Integer> readers = new HashSet<>();
        SourceSplitEnumerator.Context<BuildkiteSplit> context =
                enumeratorContext(readers, assigned);
        BuildkiteSplit saved;
        try (SourceSplitEnumerator<BuildkiteSplit, BuildkiteSplit> first =
                new BuildkiteSource.Enumerator(context, config, config.newSplit(END))) {
            first.open();
            first.run();
            saved = copy(first.snapshotState(0L));
            require(
                    first.currentUnassignedSplitSize() == 1 && assigned.isEmpty(),
                    "unregistered coordinator must retain an assignable scan");
        }
        readers.add(0);
        try (SourceSplitEnumerator<BuildkiteSplit, BuildkiteSplit> restored =
                new BuildkiteSource.Enumerator(context, config, saved)) {
            restored.open();
            restored.run();
            restored.registerReader(0);
            require(
                    assigned.size() == 1 && restored.currentUnassignedSplitSize() == 0,
                    "restored pending scan must be assigned only once");
        }
    }

    @SuppressWarnings("unchecked")
    private static SourceSplitEnumerator.Context<BuildkiteSplit> enumeratorContext(
            Set<Integer> readers, List<BuildkiteSplit> assigned) {
        return (SourceSplitEnumerator.Context<BuildkiteSplit>)
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
                                    assigned.add((BuildkiteSplit) args[1]);
                                    return null;
                                case "signalNoMoreSplits":
                                    return null;
                                default:
                                    throw new AssertionError(
                                            "Unexpected coordinator operation " + method.getName());
                            }
                        });
    }

    private static BuildkiteReader opened(BuildkiteConfig config, Context context) {
        BuildkiteReader reader = new BuildkiteReader(config, context);
        reader.open();
        return reader;
    }

    private static void await(BuildkiteReader reader, Rows rows, Context context) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!context.done && System.nanoTime() - deadline < 0) {
            reader.pollNext(rows);
            Thread.sleep(5L);
        }
        require(context.done, "bounded traversal did not finish");
    }

    private static byte[] encode(BuildkiteSplit split) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(split);
        }
        return bytes.toByteArray();
    }

    private static BuildkiteSplit copy(BuildkiteSplit split) throws Exception {
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(encode(split)))) {
            return (BuildkiteSplit) input.readObject();
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
