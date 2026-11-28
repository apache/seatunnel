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
package org.apache.seatunnel.connectors.seatunnel.box;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Restores an exact published response chunk with the packaged reader and real HTTPS.
 *
 * <p>The container-only probe supplements real Zeta savepoint scenarios for deterministic 32/33 row
 * boundaries. It does not replace provider acquisition with mocks or call production serializers as
 * the fixture's oracle.
 */
public final class BoxRecoveryProbe {
    private BoxRecoveryProbe() {}

    /**
     * Checks exact serialized replay, changed-page rejection, or quiet lifecycle completion.
     *
     * <p>The owning integration test supplies the network script and verifies every request.
     */
    public static void main(String[] args) throws Exception {
        BoxConfig config = config(args[0], "synthetic-box-access-token");
        if (args[1].equals("zero")) {
            zero(config, args[0]);
            return;
        }
        Rows before = new Rows();
        BoxSplit saved;
        try (BoxReader first = opened(config, new Context())) {
            require(first.snapshotState(0L).isEmpty(), "unassigned checkpoint must stay empty");
            first.addSplits(List.of(new BoxSplit(config)));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (before.rows.isEmpty() && System.nanoTime() - deadline < 0) {
                first.pollNext(before);
                Thread.sleep(5L);
            }
            require(
                    before.rows.size() == 32,
                    "one complete publication chunk must precede restore");
            saved = copy(first.snapshotState(1L).get(0));
            require(
                    saved.published == 32 && saved.partialFingerprint != null,
                    "published chunk position and digest must survive serialization");
        }
        String encoded = new String(encode(saved), StandardCharsets.ISO_8859_1);
        for (String secret :
                List.of(
                        "synthetic-box-access-token",
                        "synthetic-box-secret+&=%",
                        "box-content.test",
                        "exact metadata")) {
            require(
                    !encoded.contains(secret),
                    "split must omit credentials, content origins and provider payloads");
        }
        Context context = new Context();
        Rows after = new Rows();
        boolean rejected = false;
        try (BoxReader restored = opened(config, context)) {
            restored.addSplits(List.of(saved));
            await(restored, after, context);
            if (args[1].equals("resume")) {
                require(
                        after.rows.size() == 1,
                        "only the uncommitted response suffix may be published");
                Set<String> ids = new HashSet<>();
                for (SeaTunnelRow row : before.rows) {
                    require(
                            ids.add((String) row.getField(1)),
                            "no duplicate in first publication chunk");
                }
                require(
                        ids.add((String) after.rows.get(0).getField(1)),
                        "restore must not repeat a committed identity");
                require(ids.size() == 33, "all provider records must survive checkpoint restore");
                BoxSplit terminal = copy(restored.snapshotState(2L).get(0));
                require(terminal.work.isEmpty(), "the final saved position must be terminal");
                Context terminalContext = new Context();
                try (BoxReader terminalReader = opened(config, terminalContext)) {
                    terminalReader.addSplits(List.of(terminal));
                    terminalReader.pollNext(new Rows());
                    require(
                            terminalContext.done,
                            "terminal restore must finish without provider traffic");
                }
            }
        } catch (BoxReadException failure) {
            require(args[1].equals("changed"), "unchanged response restore must not fail");
            require(
                    failure.getMessage().contains("changed"),
                    "changed response must fail for its actual replay mismatch");
            rejected = true;
        }
        if (args[1].equals("changed")) {
            require(
                    rejected && after.rows.isEmpty(),
                    "changed response must fail before publishing a suffix");
        }
    }

    private static BoxConfig config(String certificate, String token) throws Exception {
        Map<String, Object> values = new HashMap<>();
        values.put("endpoint", "https://box-api.test");
        values.put("download_endpoint", "https://box-content.test");
        values.put("trust_certificate", Files.readString(Path.of(certificate)));
        values.put("token", token);
        values.put("resources", List.of("sign_templates"));
        values.put("page_size", 33);
        values.put("max_attempts", 1);
        return new BoxConfig(ReadonlyConfig.fromMap(values));
    }

    private static void zero(BoxConfig config, String certificate) throws Exception {
        Context empty = new Context();
        try (BoxReader unassigned = opened(config, empty)) {
            require(unassigned.snapshotState(0L).isEmpty(), "a fresh reader has no split");
            unassigned.handleNoMoreSplits();
            unassigned.pollNext(new Rows());
            require(
                    empty.done && unassigned.snapshotState(1L).isEmpty(),
                    "an unassigned reader must finish without manufacturing a split");
        }
        BoxSplit terminal = new BoxSplit(config);
        while (!terminal.work.isEmpty()) {
            terminal = terminal.advance(List.of(), 0, 0, Set.of());
        }
        Context context = new Context();
        try (BoxReader reader = opened(config, context)) {
            reader.addSplits(List.of(copy(terminal)));
            reader.pollNext(new Rows());
            require(context.done, "terminal scan must complete quietly");
        }
        boolean rejected = false;
        try (BoxReader wrongScope = opened(config(certificate, "different-token"), new Context())) {
            wrongScope.addSplits(List.of(new BoxSplit(config)));
        } catch (IllegalArgumentException failure) {
            rejected = true;
        }
        require(rejected, "changed credential scope must fail before HTTPS");
        BoxReader cancelled = new BoxReader(new Context(), config, TablePath.DEFAULT.getFullName());
        cancelled.cancel();
        cancelled.open();
        cancelled.pollNext(new Rows());
        cancelled.close();
        require(cancelled.snapshotState(0L).isEmpty(), "cancel before open must not create work");
    }

    private static BoxReader opened(BoxConfig config, Context context) {
        BoxReader reader = new BoxReader(context, config, TablePath.DEFAULT.getFullName());
        reader.open();
        return reader;
    }

    private static void await(BoxReader reader, Rows rows, Context context) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!context.done && System.nanoTime() - deadline < 0) {
            reader.pollNext(rows);
            Thread.sleep(5L);
        }
        require(context.done, "bounded reader did not finish");
    }

    private static byte[] encode(BoxSplit split) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(split);
        }
        return bytes.toByteArray();
    }

    private static BoxSplit copy(BoxSplit split) throws Exception {
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(encode(split)))) {
            return (BoxSplit) input.readObject();
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    /**
     * Captures only rows published by the actual source reader.
     *
     * <p>The same lock guards publication and checkpoint transitions in the probe.
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
     * Observes bounded completion without simulating the source's state machine.
     *
     * <p>All provider behavior comes from the independent container HTTPS process.
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
