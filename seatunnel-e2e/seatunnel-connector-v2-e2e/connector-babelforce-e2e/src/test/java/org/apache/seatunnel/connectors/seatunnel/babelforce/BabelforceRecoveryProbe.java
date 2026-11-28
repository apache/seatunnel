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
package org.apache.seatunnel.connectors.seatunnel.babelforce;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Supplements real Zeta savepoints with deterministic partial-response and assignment boundaries.
 *
 * <p>The probe uses the same external TLS peer as engine tests and records only delivered call IDs.
 */
public final class BabelforceRecoveryProbe {
    private final BabelforceOptions options;

    public BabelforceRecoveryProbe(
            String endpoint, String pem, String id, String token, long lower, Long upper) {
        Map<String, Object> config = new HashMap<>();
        config.put("endpoint", endpoint);
        config.put("trust_certificate", pem);
        config.put("access_key", id);
        config.put("token", token);
        config.put("date_created_from", lower);
        if (upper != null) {
            config.put("date_created_to", upper);
        }
        options = new BabelforceOptions(ReadonlyConfig.fromMap(config));
    }

    /**
     * Gets one production publication batch and serializes the resulting position.
     *
     * <p>This reaches the partial offset through an actual provider response.
     */
    public byte[] firstChunk(List<String> observed) throws Exception {
        Capture output = new Capture(observed);
        try (BabelforceReader reader = new BabelforceReader(output, options, "calls")) {
            reader.open();
            reader.addSplits(List.of(new BabelforceCheckpoint(options)));
            drive(reader, output, true);
            return serialize(reader.snapshotState(1L).get(0));
        }
    }

    /**
     * Restores the supplied position and reads through the terminal signal.
     *
     * <p>The caller can assert that a changed response failed before any further publication.
     */
    public void restore(byte[] snapshot, List<String> observed) throws Exception {
        Capture output = new Capture(observed);
        try (BabelforceReader reader = new BabelforceReader(output, options, "calls")) {
            reader.open();
            reader.addSplits(List.of(BabelforceCheckpoint.class.cast(deserialize(snapshot))));
            drive(reader, output, false);
        }
    }

    /**
     * Serializes the unassigned coordinator, restores it, then lets the worker register.
     *
     * <p>The assigned split exposes the original implicit upper bound across this handoff.
     */
    public byte[] restoredAssignment() throws Exception {
        Assignment current = new Assignment();
        BabelforceEnumerator coordinator = new BabelforceEnumerator(current, options, null);
        coordinator.run();
        if (coordinator.currentUnassignedSplitSize() != 1 || current.received != null) {
            throw new AssertionError("The initial scan must remain unassigned before registration");
        }
        BabelforceEnumerator.State state =
                BabelforceEnumerator.State.class.cast(
                        deserialize(serialize(coordinator.snapshotState(1L))));
        coordinator.close();
        Assignment worker = new Assignment();
        BabelforceEnumerator restored = new BabelforceEnumerator(worker, options, state);
        restored.run();
        worker.registered = true;
        restored.registerReader(0);
        restored.close();
        if (worker.received == null || !worker.noMore) {
            throw new AssertionError("Expected one restored scan assignment");
        }
        return serialize(worker.received);
    }

    /**
     * Reads the frozen upper bound from checkpoint bytes for an exact outbound query assertion.
     *
     * <p>Only the timestamp, rather than any provider request data, is returned.
     */
    public static long cutoff(byte[] snapshot) throws Exception {
        return BabelforceCheckpoint.class.cast(deserialize(snapshot)).until;
    }

    /**
     * Resolves a production region preset without opening a production connection.
     *
     * <p>Separating this check keeps every call in CI on the fixture network.
     */
    public static String origin(String region, String id, String token) {
        return new BabelforceOptions(
                        ReadonlyConfig.fromMap(
                                Map.of("region", region, "access_key", id, "token", token)))
                .origin;
    }

    /**
     * Exercises cancellation before the reader receives a split.
     *
     * <p>Later open and assignment callbacks must leave the provider trace empty.
     */
    public void cancelFirst(List<String> observed) throws Exception {
        Capture output = new Capture(observed);
        try (BabelforceReader reader = new BabelforceReader(output, options, "calls")) {
            reader.cancel();
            reader.open();
            reader.addSplits(List.of(new BabelforceCheckpoint(options)));
            reader.pollNext(output);
            reader.pollNext(output);
        }
    }

    private static void drive(BabelforceReader reader, Capture output, boolean first)
            throws Exception {
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(20L);
        do {
            reader.pollNext(output);
            if (output.finished || (first && !output.ids.isEmpty())) {
                return;
            }
        } while (System.nanoTime() - limit < 0L);
        throw new AssertionError("Call reader did not reach the expected state");
    }

    private static byte[] serialize(Serializable position) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream stream = new ObjectOutputStream(bytes)) {
            stream.writeObject(position);
        }
        return bytes.toByteArray();
    }

    private static Object deserialize(byte[] snapshot) throws Exception {
        try (ObjectInputStream stream = new ObjectInputStream(new ByteArrayInputStream(snapshot))) {
            return stream.readObject();
        }
    }

    /**
     * Captures exactly the IDs published under the production reader's checkpoint lock.
     *
     * <p>Completion is observed separately from a response delivering zero rows.
     */
    private static final class Capture implements SourceReader.Context, Collector<SeaTunnelRow> {
        private final List<String> ids;
        private final Object lock = new Object();
        private boolean finished;

        private Capture(List<String> ids) {
            this.ids = ids;
        }

        @Override
        public void collect(SeaTunnelRow row) {
            if (!Thread.holdsLock(lock)) {
                throw new AssertionError("Call record publication bypassed the checkpoint lock");
            }
            ids.add((String) row.getField(0));
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
            finished = true;
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
     * Records the coordinator's assignment of a single call scan.
     *
     * <p>The test deliberately holds back registration until the saved coordinator is restored.
     */
    private static final class Assignment
            implements SourceSplitEnumerator.Context<BabelforceCheckpoint> {
        private boolean registered;
        private boolean noMore;
        private BabelforceCheckpoint received;

        @Override
        public int currentParallelism() {
            return 1;
        }

        @Override
        public Set<Integer> registeredReaders() {
            return registered ? Set.of(0) : Set.of();
        }

        @Override
        public void assignSplit(int subtaskId, List<BabelforceCheckpoint> splits) {
            if (subtaskId != 0 || splits.size() != 1 || received != null) {
                throw new AssertionError("Unexpected call scan handoff");
            }
            received = splits.get(0);
        }

        @Override
        public void signalNoMoreSplits(int subtaskId) {
            noMore = subtaskId == 0;
        }

        @Override
        public void sendEventToSourceReader(int subtaskId, SourceEvent event) {}

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
