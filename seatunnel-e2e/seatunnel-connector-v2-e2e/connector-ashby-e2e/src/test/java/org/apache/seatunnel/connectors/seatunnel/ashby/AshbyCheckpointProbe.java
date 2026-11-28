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
package org.apache.seatunnel.connectors.seatunnel.ashby;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Deterministically pauses the production reader at an exact position within its HTTP response.
 *
 * <p>This supplements the engine savepoint cases. An external engine task cannot reliably stop
 * between two publication calls on the same decoded response.
 */
public final class AshbyCheckpointProbe {
    private final AshbySettings settings;

    public AshbyCheckpointProbe(String origin, String certificate, String token) {
        settings =
                new AshbySettings(
                        ReadonlyConfig.fromMap(
                                Map.of(
                                        "resource", "archive_reasons",
                                        "endpoint", origin,
                                        "trust_certificate", certificate,
                                        "token", token)));
    }

    /**
     * Saves a serialized split after one nonempty publication call.
     *
     * <p>The wire response stays fixed while the test records the collector's exact offset.
     */
    public byte[] saveFirstChunk(List<String> emitted) throws Exception {
        Reads collector = new Reads();
        Context context = new Context();
        try (AshbyReader reader = new AshbyReader(context, settings, "test-ashby")) {
            reader.open();
            reader.addSplits(List.of(new AshbySplit(settings)));
            poll(reader, collector, context, true);
            emitted.addAll(collector.ids());
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
                output.writeObject(reader.snapshotState(1L).get(0));
            }
            return bytes.toByteArray();
        }
    }

    /**
     * Restores the exact saved response position and reads through the terminal signal.
     *
     * <p>The caller also receives any rows emitted before a failure so it can check for leakage.
     */
    public void resume(byte[] state, List<String> emitted) throws Exception {
        Context context = new Context();
        Reads collector = new Reads();
        try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(state));
                AshbyReader reader = new AshbyReader(context, settings, "test-ashby")) {
            reader.open();
            reader.addSplits(List.of((AshbySplit) input.readObject()));
            try {
                poll(reader, collector, context, false);
            } finally {
                emitted.addAll(collector.ids());
            }
            if (!context.done) {
                throw new AssertionError("Restored reader did not finish");
            }
        }
    }

    /**
     * Cancels the reader before assignment and keeps the cancellation across assignment/open.
     *
     * <p>The fixture trace lets the caller verify that the subsequent polls never start HTTPS.
     */
    public void cancelBeforeAssignment() throws Exception {
        Context context = new Context();
        Reads collector = new Reads();
        try (AshbyReader reader = new AshbyReader(context, settings, "test-ashby")) {
            reader.cancel();
            reader.open();
            reader.addSplits(List.of(new AshbySplit(settings)));
            for (int i = 0; i < 3; i++) {
                reader.pollNext(collector);
            }
        }
        if (!collector.rows.isEmpty() || context.done) {
            throw new AssertionError("A stopped reader still published data or completion");
        }
    }

    private static void poll(
            AshbyReader reader, Reads collector, Context context, boolean stopAfterFirst)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        do {
            reader.pollNext(collector);
            if (context.done || (stopAfterFirst && !collector.rows.isEmpty())) {
                return;
            }
        } while (System.nanoTime() - deadline < 0L);
        throw new AssertionError("Reader did not publish within the test deadline");
    }

    /**
     * Collector with a distinct checkpoint lock and exact output IDs.
     *
     * <p>Collect checks lock ownership so an incorrect publication path cannot satisfy the test.
     */
    private static final class Reads implements Collector<SeaTunnelRow> {
        private final List<SeaTunnelRow> rows = new ArrayList<>();
        private final Object lock = new Object();

        @Override
        public void collect(SeaTunnelRow row) {
            if (!Thread.holdsLock(lock)) {
                throw new AssertionError("The output is outside the checkpoint lock");
            }
            rows.add(row);
        }

        @Override
        public Object getCheckpointLock() {
            return lock;
        }

        private List<String> ids() {
            return rows.stream().map(row -> (String) row.getField(1)).collect(Collectors.toList());
        }
    }

    /**
     * Context for a bounded reader with exactly one assigned split.
     *
     * <p>Completion is observable without coordinating any other task.
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
        public void sendSourceEventToEnumerator(SourceEvent sourceEvent) {}

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
