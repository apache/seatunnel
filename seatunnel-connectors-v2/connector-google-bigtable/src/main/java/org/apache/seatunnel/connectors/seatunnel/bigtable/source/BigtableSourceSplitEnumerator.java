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

package org.apache.seatunnel.connectors.seatunnel.bigtable.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.common.utils.HashUtils;
import org.apache.seatunnel.connectors.seatunnel.bigtable.client.BigtableClient;
import org.apache.seatunnel.connectors.seatunnel.bigtable.config.BigtableParameters;

import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Enumerates {@link BigtableSourceSplit}s for parallel reading.
 *
 * <p>Main workflow:
 *
 * <ol>
 *   <li>Use {@link BigtableClient#sampleRowKeys()} to split the key range into approximately
 *       equal-size tablet boundaries.
 *   <li>Intersect each tablet range with the user-configured {@code start_rowkey}/{@code
 *       end_rowkey}.
 *   <li>Assign splits to registered readers by hashing the split ID modulo parallelism.
 * </ol>
 *
 * <p>If split generation fails (sampling exception, empty samples, or empty intersection), falls
 * back to a single split covering the full user range so the job can still proceed. Checkpoint
 * persists both {@code assignedSplits} and {@code pendingSplits} (#11144).
 */
@Slf4j
public class BigtableSourceSplitEnumerator
        implements SourceSplitEnumerator<BigtableSourceSplit, BigtableSourceState> {

    private final Context<BigtableSourceSplit> context;
    private final BigtableParameters parameters;
    private final Set<BigtableSourceSplit> assignedSplits;
    private Set<BigtableSourceSplit> pendingSplits;
    private boolean initialized = false;

    /**
     * Data API client used only for {@code sampleRowKeys}; injectable in unit tests. Lazily created
     * when split discovery first runs. The field itself is only published under {@link #stateLock};
     * construction may run outside the lock, but {@link #getBigtableClient()} never publishes a
     * client after {@link #closed} is set (see close/create race handling there).
     */
    private BigtableClient bigtableClient;

    /**
     * Set under {@link #stateLock} by {@link #close()}. After this flag is set, discovery must not
     * publish a client, commit {@code pendingSplits}/{@code initialized}, or fall back to a
     * fabricated single split — shutdown wins over in-flight open().
     */
    private boolean closed = false;

    /**
     * Guards the shared assignment state ({@code assignedSplits}, {@code pendingSplits}, {@code
     * initialized}, {@code bigtableClient}, {@code closed}) against concurrent enumerator
     * callbacks. {@link #assignSplit(int)} must only run while this lock is held. Blocking I/O
     * ({@code sampleRowKeys()} and client construction) must run <em>outside</em> this lock so it
     * cannot stall {@link #snapshotState(long)} / checkpoint-barrier processing.
     */
    private final Object stateLock = new Object();

    public BigtableSourceSplitEnumerator(
            Context<BigtableSourceSplit> context, BigtableParameters parameters) {
        this(context, parameters, null, null);
    }

    public BigtableSourceSplitEnumerator(
            Context<BigtableSourceSplit> context,
            BigtableParameters parameters,
            BigtableSourceState sourceState) {
        this(context, parameters, sourceState, null);
    }

    /**
     * Package-private constructor for injecting a {@link BigtableClient} in unit tests to avoid
     * real cloud connections.
     *
     * @param context engine split-assignment context
     * @param parameters connection and scan parameters
     * @param sourceState checkpoint recovery state; {@code null} on first start
     * @param bigtableClient pre-built client; lazily created on first split if {@code null}
     */
    BigtableSourceSplitEnumerator(
            Context<BigtableSourceSplit> context,
            BigtableParameters parameters,
            BigtableSourceState sourceState,
            BigtableClient bigtableClient) {
        this.context = context;
        this.parameters = parameters;
        this.bigtableClient = bigtableClient;
        if (sourceState == null) {
            this.assignedSplits = new HashSet<>();
            this.pendingSplits = new HashSet<>();
            this.initialized = false;
        } else {
            this.assignedSplits = new HashSet<>(sourceState.getAssignedSplits());
            // Restore pending splits first so a returned-but-not-yet-reassigned split survives
            // recovery even when its ID is already present in assignedSplits.
            this.pendingSplits = new HashSet<>(sourceState.getPendingSplits());
            // Only skip table-split discovery when the checkpoint captured real enumerator
            // progress.
            // An empty-empty checkpoint (before any reader registered) must still discover splits.
            this.initialized = !this.assignedSplits.isEmpty() || !this.pendingSplits.isEmpty();
        }
    }

    @Override
    public void open() {
        // Discover splits before any reader registers. sampleRowKeys() is a blocking RPC
        // (plus lazy client construction); running it here avoids holding the engine's
        // enumeratorContext monitor used by triggerBarrier() during receivedReader().
        // Across Zeta/Flink/Spark, open() always completes before registerReader() is called,
        // so discovery is guaranteed to have finished by the time any reader registers.
        initializePendingSplits();
    }

    @Override
    public void run() throws Exception {
        // Splits are assigned lazily when readers register.
    }

    @Override
    public void close() throws IOException {
        synchronized (stateLock) {
            closed = true;
            if (bigtableClient != null) {
                bigtableClient.close();
                bigtableClient = null;
            }
        }
    }

    @Override
    public void addSplitsBack(List<BigtableSourceSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            synchronized (stateLock) {
                pendingSplits.addAll(splits);
                if (context.registeredReaders().contains(subtaskId)) {
                    assignSplit(subtaskId);
                }
            }
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        synchronized (stateLock) {
            return pendingSplits.size();
        }
    }

    @Override
    public void registerReader(int subtaskId) {
        // Discovery already ran in open(); assign under stateLock only.
        synchronized (stateLock) {
            assignSplit(subtaskId);
        }
    }

    @Override
    public BigtableSourceState snapshotState(long checkpointId) throws Exception {
        synchronized (stateLock) {
            return new BigtableSourceState(assignedSplits, pendingSplits);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}

    @Override
    public void handleSplitRequest(int subtaskId) {}

    /**
     * Discovers tablet splits once per job start. The blocking {@code sampleRowKeys} RPC runs
     * outside {@link #stateLock}; the lock is re-acquired only to mutate {@code pendingSplits}.
     *
     * <p>If {@link #close()} wins a race against discovery, this method returns without committing
     * {@code pendingSplits} or {@code initialized}, so a fabricated fallback split cannot be
     * checkpointed after shutdown.
     */
    private void initializePendingSplits() {
        synchronized (stateLock) {
            if (initialized || closed) {
                return;
            }
        }
        Set<BigtableSourceSplit> tableSplits = buildSplits();
        synchronized (stateLock) {
            // closed may have flipped while buildSplits() ran unlocked; do not mutate after close.
            if (initialized || closed) {
                return;
            }
            Set<String> existingIds =
                    pendingSplits.stream()
                            .map(BigtableSourceSplit::splitId)
                            .collect(Collectors.toSet());
            existingIds.addAll(
                    assignedSplits.stream()
                            .map(BigtableSourceSplit::splitId)
                            .collect(Collectors.toSet()));
            tableSplits.stream()
                    .filter(s -> !existingIds.contains(s.splitId()))
                    .forEach(pendingSplits::add);
            initialized = true;
        }
    }

    /**
     * Generates multiple splits via {@link BigtableClient#sampleRowKeys()}; falls back to a single
     * split on failure.
     *
     * <p>Sample keys are turned into half-open intervals {@code [prev, current)} in lexicographic
     * order, then intersected with the user range. If the last sample key is non-empty (i.e. not
     * the table-end sentinel), an extra segment {@code [lastSample, "")} is appended to cover the
     * table tail.
     *
     * <p>Performs blocking I/O (client construction and the sampleRowKeys RPC). Callers must not
     * hold {@link #stateLock} while invoking this method.
     *
     * @return discovered splits; a single split covering the user range when sampling fails; an
     *     empty set when the enumerator was closed during discovery (caller must not commit that)
     */
    Set<BigtableSourceSplit> buildSplits() {
        String userStart = parameters.getStartRowkey() != null ? parameters.getStartRowkey() : "";
        String userEnd = parameters.getEndRowkey() != null ? parameters.getEndRowkey() : "";

        List<KeyOffset> samples;
        try {
            samples = getBigtableClient().sampleRowKeys();
        } catch (EnumeratorClosedException e) {
            // close() raced discovery — not a Bigtable API failure; do not log as sampleRowKeys
            // failed and do not fabricate a whole-range fallback split.
            log.info(
                    "Enumerator closed during split discovery for table [{}]; skipping discovery",
                    parameters.getTable());
            return Collections.emptySet();
        } catch (Exception e) {
            if (isClosed()) {
                log.info(
                        "Enumerator closed during split discovery for table [{}]; skipping discovery",
                        parameters.getTable());
                return Collections.emptySet();
            }
            log.warn(
                    "sampleRowKeys failed for table [{}], fallback to single split",
                    parameters.getTable(),
                    e);
            return Collections.singleton(new BigtableSourceSplit(0, userStart, userEnd));
        }

        if (isClosed()) {
            return Collections.emptySet();
        }

        if (samples == null || samples.isEmpty()) {
            return Collections.singleton(new BigtableSourceSplit(0, userStart, userEnd));
        }

        Set<BigtableSourceSplit> splits = new LinkedHashSet<>();
        String rangeStart = "";
        int index = 0;
        for (KeyOffset sample : samples) {
            String rangeEnd = keyToUtf8(sample);
            index = addIntersectedSplit(splits, index, rangeStart, rangeEnd, userStart, userEnd);
            rangeStart = rangeEnd;
        }

        // If the last sample key is not empty the API did not emit a table-end sentinel;
        // append [lastSample, "") to avoid missing the table tail.
        if (!rangeStart.isEmpty()) {
            addIntersectedSplit(splits, index, rangeStart, "", userStart, userEnd);
        }

        if (splits.isEmpty()) {
            return Collections.singleton(new BigtableSourceSplit(0, userStart, userEnd));
        }
        log.info(
                "Enumerated {} Bigtable splits for table [{}]",
                splits.size(),
                parameters.getTable());
        return splits;
    }

    /**
     * Intersects a tablet range with the user range and adds the result to the split set.
     *
     * @return {@code index + 1} if a split was added, otherwise the original {@code index}
     */
    private static int addIntersectedSplit(
            Set<BigtableSourceSplit> splits,
            int index,
            String rangeStart,
            String rangeEnd,
            String userStart,
            String userEnd) {
        String splitStart = maxStart(rangeStart, userStart);
        String splitEnd = minEnd(rangeEnd, userEnd);
        if (isValidRange(splitStart, splitEnd)) {
            splits.add(new BigtableSourceSplit(index, splitStart, splitEnd));
            return index + 1;
        }
        return index;
    }

    /**
     * Converts a sample key to a UTF-8 string; null or empty ByteString represents the table end.
     */
    private static String keyToUtf8(KeyOffset sample) {
        if (sample == null || sample.getKey() == null) {
            return "";
        }
        return sample.getKey().toStringUtf8();
    }

    /** Returns the lexicographically larger start key; empty string means table-begin (minimum). */
    private static String maxStart(String a, String b) {
        if (a.isEmpty()) {
            return b;
        }
        if (b.isEmpty()) {
            return a;
        }
        return a.compareTo(b) >= 0 ? a : b;
    }

    /** Returns the lexicographically smaller end key; empty string means table-end (maximum). */
    private static String minEnd(String a, String b) {
        if (a.isEmpty()) {
            return b;
        }
        if (b.isEmpty()) {
            return a;
        }
        return a.compareTo(b) <= 0 ? a : b;
    }

    /** Returns true if {@code [start, end)} is non-empty; empty end means until table-end. */
    private static boolean isValidRange(String start, String end) {
        if (end.isEmpty()) {
            return true;
        }
        if (start.isEmpty()) {
            return true;
        }
        return start.compareTo(end) < 0;
    }

    private boolean isClosed() {
        synchronized (stateLock) {
            return closed;
        }
    }

    private void assignSplit(int taskId) {
        List<BigtableSourceSplit> toAssign = new ArrayList<>();
        if (context.currentParallelism() == 1) {
            toAssign.addAll(pendingSplits);
        } else {
            for (BigtableSourceSplit split : pendingSplits) {
                int owner =
                        HashUtils.bucketIndex(
                                split.splitId().hashCode(), context.currentParallelism());
                if (owner == taskId) {
                    toAssign.add(split);
                }
            }
        }
        context.assignSplit(taskId, toAssign);
        assignedSplits.addAll(toAssign);
        toAssign.forEach(pendingSplits::remove);
        log.info(
                "SubTask {} assigned [{}]",
                taskId,
                toAssign.stream()
                        .map(BigtableSourceSplit::splitId)
                        .collect(Collectors.joining(",")));
        context.signalNoMoreSplits(taskId);
    }

    /**
     * Returns the shared client, creating it outside {@link #stateLock} when needed.
     *
     * <p>Construction is unlocked so a slow gRPC channel / credential load does not stall
     * checkpoint snapshotting. Before publishing the new instance, this method re-checks under the
     * lock: if another thread already published a client, or if {@link #close()} has already set
     * {@code closed}, the just-built instance is closed immediately and never leaked.
     *
     * @throws EnumeratorClosedException if the enumerator is already closed (or closes during
     *     construction), so callers can distinguish shutdown from a real sampleRowKeys failure
     */
    private BigtableClient getBigtableClient() {
        synchronized (stateLock) {
            if (bigtableClient != null) {
                return bigtableClient;
            }
            if (closed) {
                throw new EnumeratorClosedException(
                        "BigtableSourceSplitEnumerator already closed; cannot create client");
            }
        }
        BigtableClient created = BigtableClient.createInstance(parameters);
        synchronized (stateLock) {
            if (closed) {
                created.close();
                throw new EnumeratorClosedException(
                        "BigtableSourceSplitEnumerator closed during client creation");
            }
            if (bigtableClient == null) {
                bigtableClient = created;
                return created;
            }
            created.close();
            return bigtableClient;
        }
    }

    /**
     * Thrown when discovery/client creation observes that {@link #close()} has already run. Not a
     * Bigtable API failure — callers must not treat it as {@code sampleRowKeys} failure.
     */
    static final class EnumeratorClosedException extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        EnumeratorClosedException(String message) {
            super(message);
        }
    }
}
