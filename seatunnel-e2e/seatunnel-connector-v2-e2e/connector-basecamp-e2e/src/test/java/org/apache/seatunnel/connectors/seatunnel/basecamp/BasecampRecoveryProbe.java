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

package org.apache.seatunnel.connectors.seatunnel.basecamp;

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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Runs exact reader checkpoint boundaries inside the isolated HTTPS engine container.
 *
 * <p>The main process uses the packaged connector and real TLS without modifying host DNS. It
 * supplements the real Zeta cases for row-internal positions that cannot be scheduled reliably.
 */
public final class BasecampRecoveryProbe {
    private BasecampRecoveryProbe() {}

    /**
     * Checks row resume, changed-page rejection, or zero-traffic lifecycle against the test peer.
     *
     * <p>No test framework or production parser is used to produce the peer's expected responses.
     */
    public static void main(String[] args) throws Exception {
        BasecampConfig config =
                new BasecampConfig(
                        ReadonlyConfig.fromMap(
                                Map.of(
                                        "account_id",
                                        "123456",
                                        "token",
                                        args[1],
                                        "user_agent",
                                        "SeaTunnel-Test (https://example.invalid/integration)",
                                        "tls_trust_certificate",
                                        Files.readString(Path.of(args[0])),
                                        "resources",
                                        List.of("projects"),
                                        "start_date",
                                        "2024-01-01T00:00:00.000Z",
                                        "end_date",
                                        "2024-01-01T00:00:00.003Z")));
        if ("zero".equals(args[2])) {
            zero(config, args[1]);
            return;
        }
        Rows rows = new Rows();
        BasecampSplit saved;
        try (BasecampReader first = opened(config, new Context())) {
            require(first.snapshotState(0L).isEmpty(), "empty assignment must remain empty");
            first.addSplits(List.of(config.newSplit(Instant.parse("2024-01-03T00:00:00Z"))));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (rows.values.size() != 1 && System.nanoTime() - deadline < 0) {
                first.pollNext(rows);
                Thread.sleep(5L);
            }
            require(rows.values.size() == 1, "first row was not emitted");
            BasecampSplit checkpoint = first.snapshotState(1L).get(0);
            require(
                    checkpoint.top.row == 1 && checkpoint.top.digest != null,
                    "missing row fingerprint");
            saved = serialized(checkpoint, args[1]);
            checkpoint.top.row = 0;
            checkpoint.entries.row = 9;
            require(
                    first.snapshotState(2L).get(0).top.row == 1
                            && first.snapshotState(2L).get(0).entries.row == 0,
                    "snapshot aliases live state");
        }
        Context context = new Context();
        try (BasecampReader restored = opened(config, context)) {
            restored.addSplits(List.of(saved));
            if ("changed".equals(args[2])) {
                boolean rejected = false;
                try {
                    drain(restored, context, rows);
                } catch (BasecampResource.Failure error) {
                    require(
                            error.getMessage().contains("page changed since checkpoint"),
                            "wrong changed-page failure");
                    rejected = true;
                }
                require(rejected && rows.values.size() == 1, "unverified page suffix escaped");
            } else if ("row".equals(args[2])) {
                drain(restored, context, rows);
                require(
                        rows.values.size() == 2 && context.done,
                        "restored row count or terminal signal");
                Set<String> ids = new HashSet<>();
                for (SeaTunnelRow row : rows.values) {
                    require(
                            "projects".equals(row.getField(0))
                                    && row.getField(2) == null
                                    && row.getField(3) == null,
                            "changed identity context");
                    require(ids.add((String) row.getField(1)), "duplicate accepted row");
                    require(
                            ((String) row.getField(5)).contains("private-project-value"),
                            "raw payload missing");
                }
                require(ids.equals(Set.of("101", "102")), "lost row identity");
                require(
                        restored.snapshotState(3L).get(0).finished(),
                        "terminal state was not retained");
            } else {
                throw new AssertionError("unsupported probe scenario");
            }
        }
    }

    private static void zero(BasecampConfig config, String key) throws Exception {
        Rows rows = new Rows();
        try (BasecampReader cancelled = new BasecampReader(config, new Context())) {
            require(cancelled.snapshotState(0L).isEmpty(), "empty assignment snapshot");
            cancelled.cancel();
            cancelled.cancel();
            cancelled.open();
            cancelled.addSplits(List.of(config.newSplit(Instant.parse("2024-01-03T00:00:00Z"))));
            cancelled.pollNext(rows);
            require(rows.values.isEmpty(), "cancel-before-open emitted rows");
        }
        BasecampSplit terminal = config.newSplit(Instant.parse("2024-01-03T00:00:00Z"));
        terminal.phase = null;
        Context context = new Context();
        try (BasecampReader restored = opened(config, context)) {
            restored.addSplits(List.of(serialized(terminal, key)));
            restored.pollNext(rows);
            restored.pollNext(rows);
            require(context.done && rows.values.isEmpty(), "terminal scan resumed provider work");
            require(restored.snapshotState(1L).get(0).finished(), "terminal snapshot regressed");
        }
    }

    private static BasecampReader opened(BasecampConfig config, Context context) {
        BasecampReader reader = new BasecampReader(config, context);
        reader.open();
        return reader;
    }

    private static void drain(BasecampReader reader, Context context, Rows rows) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (!context.done && System.nanoTime() - deadline < 0) {
            reader.pollNext(rows);
            Thread.sleep(5L);
        }
        require(context.done, "reader did not terminate");
    }

    private static BasecampSplit serialized(BasecampSplit split, String key) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(split);
        }
        String view = bytes.toString(StandardCharsets.ISO_8859_1);
        require(
                !view.contains(key)
                        && !view.contains("private-project-value")
                        && !view.contains("Bearer "),
                "sensitive state persisted");
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (BasecampSplit) input.readObject();
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    /**
     * Captures accepted rows with the same lock exposed to reader checkpoint commits.
     *
     * <p>The restored instance appends to this list so repeated output is observable.
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
     * Records the real bounded reader's lifecycle signals without simulating provider behavior.
     *
     * <p>The context runs in a standalone JVM using the packaged connector artifacts.
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
