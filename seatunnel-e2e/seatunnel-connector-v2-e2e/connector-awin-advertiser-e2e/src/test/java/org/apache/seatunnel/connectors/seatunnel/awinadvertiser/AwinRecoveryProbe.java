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
package org.apache.seatunnel.connectors.seatunnel.awinadvertiser;

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
 * Drives the production Awin reader at deterministic checkpoint boundaries in the HTTPS suite.
 *
 * <p>Actual Zeta jobs cover job and region recovery. These checks target the smaller boundaries
 * that an external engine submitter cannot schedule precisely.
 */
public final class AwinRecoveryProbe {
    private final AwinSettings settings;

    public AwinRecoveryProbe(
            String endpoint,
            String trust,
            String token,
            long advertiser,
            String resource,
            String start) {
        Map<String, Object> config = new HashMap<>();
        config.put("endpoint", endpoint);
        config.put("trust_certificate", trust);
        config.put("token", token);
        config.put("advertiser_id", advertiser);
        config.put("resources", List.of(resource));
        if (start != null) {
            config.put("start_date", start);
        }
        settings = new AwinSettings(ReadonlyConfig.fromMap(config));
    }

    /**
     * Captures the first publication batch in serialized form and makes its delivered IDs visible.
     *
     * <p>This observes the original provider response, not a pre-filled checkpoint offset.
     */
    public byte[] firstBatch(List<String> ids) throws Exception {
        Consumer output = new Consumer(ids);
        try (AwinReader reader = new AwinReader(output, settings, "awin-test")) {
            reader.open();
            reader.addSplits(List.of(new AwinCursor(settings)));
            drive(reader, output, true);
            return bytes(reader.snapshotState(1L).get(0));
        }
    }

    /**
     * Resumes the supplied serialized cursor against the same external test endpoint.
     *
     * <p>A caller can distinguish rows emitted before a failing response from a clean rejection.
     */
    public void resume(byte[] state, List<String> ids) throws Exception {
        Consumer collect = new Consumer(ids);
        try (AwinReader reader = new AwinReader(collect, settings, "awin-test")) {
            reader.open();
            reader.addSplits(List.of(AwinCursor.class.cast(decode(state))));
            drive(reader, collect, false);
        }
    }

    /**
     * Starts a coordinator with no registered reader, restores it, then registers its reader.
     *
     * <p>The cutoff in the returned assigned split allows an exact independent wire assertion.
     */
    public byte[] assignAfterRestore() throws Exception {
        Assignment unregistered = new Assignment();
        AwinEnumerator owner = new AwinEnumerator(unregistered, settings, null);
        owner.run();
        if (unregistered.offered != null || owner.currentUnassignedSplitSize() != 1) {
            throw new AssertionError(
                    "The initial cursor must stay with the unregistered coordinator");
        }
        AwinEnumerator.State checkpoint =
                AwinEnumerator.State.class.cast(decode(bytes(owner.snapshotState(1L))));
        owner.close();
        Assignment restored = new Assignment();
        AwinEnumerator ownerAgain = new AwinEnumerator(restored, settings, checkpoint);
        restored.registered = true;
        ownerAgain.registerReader(0);
        ownerAgain.run();
        ownerAgain.close();
        if (restored.offered == null || !restored.endOfAssignments) {
            throw new AssertionError("The restored coordinator did not hand off the sole split");
        }
        return bytes(restored.offered);
    }

    /**
     * Returns the checkpoint's original UTC cutoff.
     *
     * <p>Responses and token state stay outside the serialized position.
     */
    public static Instant cutoff(byte[] state) throws Exception {
        return AwinCursor.class.cast(decode(state)).cutoff;
    }

    /**
     * Verifies that an early cancellation applies across subsequent open and addSplits callbacks.
     *
     * <p>The external trace confirms that the cancelled reader never begins a request.
     */
    public void cancelBeforeAssignment(List<String> ids) throws Exception {
        Consumer collect = new Consumer(ids);
        try (AwinReader reader = new AwinReader(collect, settings, "awin-test")) {
            reader.cancel();
            reader.open();
            reader.addSplits(List.of(new AwinCursor(settings)));
            reader.pollNext(collect);
            reader.pollNext(collect);
        }
    }

    private static void drive(AwinReader reader, Consumer collect, boolean firstBatch)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20L);
        do {
            reader.pollNext(collect);
            if (collect.finished || (firstBatch && !collect.ids.isEmpty())) {
                return;
            }
        } while (System.nanoTime() - deadline < 0L);
        throw new AssertionError("Report reader did not reach the expected publication point");
    }

    private static byte[] bytes(Serializable snapshot) throws Exception {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        try (ObjectOutputStream stream = new ObjectOutputStream(result)) {
            stream.writeObject(snapshot);
        }
        return result.toByteArray();
    }

    private static Object decode(byte[] state) throws Exception {
        try (ObjectInputStream stream = new ObjectInputStream(new ByteArrayInputStream(state))) {
            return stream.readObject();
        }
    }

    /**
     * Observes publication and end of data under a test-owned checkpoint lock.
     *
     * <p>An ID is recorded only when the production reader calls its collector.
     */
    private static final class Consumer implements SourceReader.Context, Collector<SeaTunnelRow> {
        private final Object lock = new Object();
        private final List<String> ids;
        private boolean finished;

        private Consumer(List<String> ids) {
            this.ids = ids;
        }

        @Override
        public void collect(SeaTunnelRow row) {
            if (!Thread.holdsLock(lock)) {
                throw new AssertionError("Awin rows must use the checkpoint lock");
            }
            ids.add((String) row.getField(1));
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
     * Records the single transition from coordinator ownership to the chosen subtask.
     *
     * <p>The test controls exactly when the reader registers.
     */
    private static final class Assignment implements SourceSplitEnumerator.Context<AwinCursor> {
        private boolean registered;
        private boolean endOfAssignments;
        private AwinCursor offered;

        @Override
        public int currentParallelism() {
            return 1;
        }

        @Override
        public Set<Integer> registeredReaders() {
            return registered ? Set.of(0) : Set.of();
        }

        @Override
        public void assignSplit(int subtask, List<AwinCursor> splits) {
            if (subtask != 0 || splits.size() != 1 || offered != null) {
                throw new AssertionError("Unexpected Awin split assignment");
            }
            offered = splits.get(0);
        }

        @Override
        public void signalNoMoreSplits(int subtask) {
            endOfAssignments = subtask == 0;
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
