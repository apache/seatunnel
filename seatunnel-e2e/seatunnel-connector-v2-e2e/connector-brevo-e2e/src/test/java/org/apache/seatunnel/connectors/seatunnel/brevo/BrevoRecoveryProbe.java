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
package org.apache.seatunnel.connectors.seatunnel.brevo;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.brevo.BrevoSource.Cursor;
import org.apache.seatunnel.connectors.seatunnel.brevo.BrevoSource.Split;
import org.apache.seatunnel.connectors.seatunnel.brevo.BrevoSource.State;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
 * Supplements real Zeta savepoints with a deterministic 16/17-row publication boundary.
 *
 * <p>This probe runs only inside the CI worker with the packaged connector and real HTTPS. It does
 * not manufacture response fingerprints or replace the transport with an in-process mock.
 */
public final class BrevoRecoveryProbe {
    private BrevoRecoveryProbe() {}

    /**
     * Restores serialized progress, rejects changed pages, or checks quiet lifecycle completion.
     *
     * <p>The owning integration test controls the independent peer and validates every request.
     */
    public static void main(String[] args) throws Exception {
        BrevoConfig config = config(args[0], "synthetic-brevo-token");
        if (args[1].equals("zero")) {
            zero(config, args[0]);
            return;
        }
        Rows before = new Rows();
        Split saved;
        try (BrevoReader first = opened(config, new Context())) {
            require(first.snapshotState(0L).isEmpty(), "unassigned checkpoint must remain empty");
            first.addSplits(List.of(initial(config)));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (before.rows.isEmpty() && System.nanoTime() - deadline < 0) {
                first.pollNext(before);
                Thread.sleep(5L);
            }
            require(before.rows.size() == 16, "first poll must publish exactly one 16-row chunk");
            saved = copy(first.snapshotState(1L).get(0));
            require(
                    saved.row == 16 && saved.rowPage != null,
                    "serialized split must retain row and response digest");
        }
        String encoded = new String(encode(saved), StandardCharsets.ISO_8859_1);
        require(
                !encoded.contains("synthetic-brevo-token") && !encoded.contains("\"id\""),
                "saved state must omit credentials and provider payloads");
        Context context = new Context();
        Rows after = new Rows();
        boolean rejected = false;
        try (BrevoReader restored = opened(config, context)) {
            restored.addSplits(List.of(saved));
            await(restored, after, context);
            require(args[1].equals("resume"), "changed response must not finish successfully");
            require(after.rows.size() == 1, "only the uncommitted suffix may be emitted");
            Set<String> ids = new HashSet<>();
            for (SeaTunnelRow row : before.rows) {
                require(ids.add((String) row.getField(0)), "duplicate initial publication");
            }
            require(
                    ids.add((String) after.rows.get(0).getField(0)) && ids.size() == 17,
                    "restore must preserve all 17 distinct identities exactly once");
            Split terminal = copy(restored.snapshotState(2L).get(0));
            require(terminal.finished, "final checkpoint must be terminal");
            Context finished = new Context();
            try (BrevoReader terminalReader = opened(config, finished)) {
                terminalReader.addSplits(List.of(terminal));
                terminalReader.pollNext(new Rows());
                require(finished.done, "terminal restore must finish without provider traffic");
            }
        } catch (IOException failure) {
            require(
                    args[1].equals("changed") && failure.getMessage().contains("changed"),
                    "only an actual changed-page rejection is expected");
            rejected = true;
        }
        if (args[1].equals("changed")) {
            require(
                    rejected && after.rows.isEmpty(),
                    "changed page must fail before emitting its suffix");
        }
    }

    private static BrevoConfig config(String certificate, String token) {
        Map<String, Object> options = new HashMap<>();
        options.put("token", token);
        options.put("resource", "processes");
        options.put("tls_trust_certificate", certificate);
        options.put("page_size", 17);
        options.put("max_attempts", 1);
        return new BrevoConfig(ReadonlyConfig.fromMap(options));
    }

    private static Split initial(BrevoConfig config) {
        return new Split(
                config.scope,
                null,
                null,
                Cursor.first(config.resource),
                null,
                0,
                null,
                0,
                null,
                false);
    }

    private static void zero(BrevoConfig config, String certificate) throws Exception {
        Context unassigned = new Context();
        try (BrevoReader reader = opened(config, unassigned)) {
            require(
                    reader.snapshotState(0L).isEmpty(),
                    "fresh checkpoint must not manufacture work");
            reader.handleNoMoreSplits();
            reader.pollNext(new Rows());
            require(
                    unassigned.done && reader.snapshotState(1L).isEmpty(),
                    "unassigned EOF must terminate quietly");
        }
        Context terminal = new Context();
        try (BrevoReader reader = opened(config, terminal)) {
            reader.addSplits(
                    List.of(
                            copy(
                                    new Split(
                                            config.scope,
                                            null,
                                            null,
                                            null,
                                            null,
                                            0,
                                            null,
                                            0,
                                            null,
                                            true))));
            reader.pollNext(new Rows());
            require(terminal.done, "terminal split must not start HTTPS");
        }
        boolean rejected = false;
        try (BrevoReader wrong =
                opened(config(certificate, "different-brevo-key"), new Context())) {
            wrong.addSplits(List.of(initial(config)));
        } catch (IllegalArgumentException expected) {
            rejected = true;
        }
        require(rejected, "changed credential scope must fail before networking");
        BrevoReader cancelled = new BrevoReader(config, new Context(), "brevo_probe");
        cancelled.cancel();
        cancelled.open();
        cancelled.pollNext(new Rows());
        cancelled.close();
        require(
                cancelled.snapshotState(0L).isEmpty(),
                "cancel before open must not manufacture work");
        List<Split> assignments = new ArrayList<>();
        Set<Integer> readers = new HashSet<>();
        SourceSplitEnumerator.Context<Split> context = enumeratorContext(readers, assignments);
        BrevoSource source = new BrevoSource(config);
        State pending;
        try (SourceSplitEnumerator<Split, State> first = source.createEnumerator(context)) {
            first.open();
            first.run();
            pending = copyState(first.snapshotState(1L));
            require(
                    pending.pending != null && assignments.isEmpty(),
                    "unassigned coordinator progress must survive before reader registration");
        }
        readers.add(0);
        try (SourceSplitEnumerator<Split, State> restored =
                source.restoreEnumerator(context, pending)) {
            restored.open();
            restored.run();
            require(
                    assignments.size() == 1 && restored.currentUnassignedSplitSize() == 0,
                    "restored coordinator must assign exactly one retained traversal");
            restored.registerReader(0);
            require(assignments.size() == 1, "repeated registration must not duplicate assignment");
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

    private static BrevoReader opened(BrevoConfig config, Context context) {
        BrevoReader reader = new BrevoReader(config, context, "brevo_probe");
        reader.open();
        return reader;
    }

    private static void await(BrevoReader reader, Rows rows, Context context) throws Exception {
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
