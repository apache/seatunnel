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
package org.apache.seatunnel.connectors.seatunnel.aviationstack;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Exercises serialized catalog positions at deterministic reader and coordinator boundaries.
 *
 * <p>The engine suite separately covers actual savepoint commands. Here each reader poll allows the
 * test to stop precisely inside a decoded response or before registration.
 */
public final class AviationstackRecoveryProbe {
    private final AviationstackConfig config;

    public AviationstackRecoveryProbe(
            String endpoint, String cert, String key, String catalog, String lowerBound) {
        Map<String, Object> options = new HashMap<>();
        options.put("endpoint", endpoint);
        options.put("trust_certificate", cert);
        options.put("access_key", key);
        options.put("resources", List.of(catalog));
        if (lowerBound != null) {
            options.put("start_date", lowerBound);
        }
        config = new AviationstackConfig(ReadonlyConfig.fromMap(options));
    }

    /**
     * Serializes the first partially delivered page.
     *
     * <p>The supplied list captures exactly the records emitted before the snapshot.
     */
    public byte[] firstChunk(List<String> ids) throws Exception {
        Output draft = new Output(ids);
        try (AviationstackReader reader = new AviationstackReader(draft, config, "catalog")) {
            reader.open();
            reader.addSplits(List.of(new AviationstackSplit(config)));
            poll(reader, draft, true);
            return serialize(reader.snapshotState(1L).get(0));
        }
    }

    /**
     * Reads from the exact position in the saved split.
     *
     * <p>On a digest mismatch the caller can confirm that no further records were published.
     */
    public void consume(byte[] bytes, List<String> output) throws Exception {
        Output collector = new Output(output);
        try (AviationstackReader reader = new AviationstackReader(collector, config, "catalog")) {
            reader.open();
            reader.addSplits(List.of(AviationstackSplit.class.cast(deserialize(bytes))));
            poll(reader, collector, false);
        }
    }

    /**
     * Checks serialized coordinator ownership before a registered subtask has received its split.
     *
     * <p>Returning the assigned split lets the test verify that the original time bound survived
     * restoring and assigning the coordinator state.
     */
    public byte[] recoverUnassigned() throws Exception {
        Owner absent = new Owner();
        AviationstackEnumerator original = new AviationstackEnumerator(absent, config, null);
        original.run();
        if (absent.split != null) {
            throw new AssertionError("Split assigned before registration");
        }
        AviationstackEnumerator.Snapshot saved =
                AviationstackEnumerator.Snapshot.class.cast(
                        deserialize(serialize(original.snapshotState(1))));
        original.close();
        Owner ready = new Owner();
        AviationstackEnumerator restore = new AviationstackEnumerator(ready, config, saved);
        restore.run();
        ready.registered = true;
        restore.registerReader(0);
        restore.close();
        if (ready.split == null || !ready.noMore) {
            throw new AssertionError("Restored coordinator did not offer its pending split");
        }
        return serialize(ready.split);
    }

    /**
     * Returns the persisted implicit upper bound used by subsequent provider pages.
     *
     * <p>The caller can program an exact nanosecond boundary using the returned timestamp.
     */
    public static Instant frozenEnd(byte[] split) throws Exception {
        return AviationstackSplit.class.cast(deserialize(split)).windowEnd;
    }

    /**
     * Exercises sticky cancellation before the first split assignment.
     *
     * <p>The fixture trace and empty output prove that later polls cannot reopen transport.
     */
    public void stopBeforeAssignment(List<String> ids) throws Exception {
        Output output = new Output(ids);
        try (AviationstackReader reader = new AviationstackReader(output, config, "catalog")) {
            reader.cancel();
            reader.open();
            reader.addSplits(List.of(new AviationstackSplit(config)));
            reader.pollNext(output);
            reader.pollNext(output);
        }
    }

    private static void poll(AviationstackReader reader, Output output, boolean partial)
            throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        do {
            reader.pollNext(output);
            if (output.done || (partial && !output.ids.isEmpty())) {
                return;
            }
        } while (System.nanoTime() < until);
        throw new AssertionError("Reader did not reach the requested state");
    }

    private static byte[] serialize(Serializable state) throws Exception {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (ObjectOutputStream stream = new ObjectOutputStream(buffer)) {
            stream.writeObject(state);
        }
        return buffer.toByteArray();
    }

    private static Object deserialize(byte[] state) throws Exception {
        try (ObjectInputStream stream = new ObjectInputStream(new ByteArrayInputStream(state))) {
            return stream.readObject();
        }
    }

    /**
     * Observes publication under a distinct collector lock and tracks end of input.
     *
     * <p>The list contains exact provider IDs rather than an inferred offset.
     */
    private static final class Output implements SourceReader.Context, Collector<SeaTunnelRow> {
        private final Object lock = new Object();
        private final List<String> ids;
        private boolean done;

        private Output(List<String> ids) {
            this.ids = ids;
        }

        @Override
        public void collect(SeaTunnelRow record) {
            if (!Thread.holdsLock(lock)) {
                throw new AssertionError("Catalog rows must be emitted under the checkpoint lock");
            }
            ids.add((String) record.getField(1));
        }

        @Override
        public Object getCheckpointLock() {
            return lock;
        }

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

    /**
     * Records the single handoff from coordinator to reader.
     *
     * <p>The test controls registration so it can snapshot before the coordinator may assign.
     */
    private static final class Owner implements SourceSplitEnumerator.Context<AviationstackSplit> {
        private boolean registered;
        private boolean noMore;
        private AviationstackSplit split;

        @Override
        public int currentParallelism() {
            return 1;
        }

        @Override
        public Set<Integer> registeredReaders() {
            return registered ? Set.of(0) : Set.of();
        }

        @Override
        public void assignSplit(int subtask, List<AviationstackSplit> splits) {
            if (subtask != 0 || splits.size() != 1 || split != null) {
                throw new AssertionError("Duplicate or incorrect split handoff");
            }
            split = splits.get(0);
        }

        @Override
        public void signalNoMoreSplits(int subtask) {
            noMore = subtask == 0;
        }

        @Override
        public void sendEventToSourceReader(int subtask, SourceEvent event) {}

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
