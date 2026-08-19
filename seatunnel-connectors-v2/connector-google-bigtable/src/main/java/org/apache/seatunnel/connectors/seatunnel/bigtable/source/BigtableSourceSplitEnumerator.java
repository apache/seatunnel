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

    /** Lazily-initialized Data API client used only for sampleRowKeys; injectable in unit tests. */
    private BigtableClient bigtableClient;

    /**
     * Guards the shared assignment state ({@code assignedSplits}, {@code pendingSplits}, {@code
     * initialized}) against concurrent enumerator callbacks. {@link #initializePendingSplits()} and
     * {@link #assignSplit(int)} must only run while this lock is held.
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
        // State is fully initialized in the constructor; nothing to reset on open().
    }

    @Override
    public void run() throws Exception {
        // Splits are assigned lazily when readers register.
    }

    @Override
    public void close() throws IOException {
        if (bigtableClient != null) {
            bigtableClient.close();
            bigtableClient = null;
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
        synchronized (stateLock) {
            initializePendingSplits();
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

    private void initializePendingSplits() {
        if (initialized) {
            return;
        }
        Set<BigtableSourceSplit> tableSplits = buildSplits();
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

    /**
     * Generates multiple splits via {@link BigtableClient#sampleRowKeys()}; falls back to a single
     * split on failure.
     *
     * <p>Sample keys are turned into half-open intervals {@code [prev, current)} in lexicographic
     * order, then intersected with the user range. If the last sample key is non-empty (i.e. not
     * the table-end sentinel), an extra segment {@code [lastSample, "")} is appended to cover the
     * table tail.
     *
     * @return at least one split; a single split covering the user range when splitting fails
     */
    Set<BigtableSourceSplit> buildSplits() {
        String userStart = parameters.getStartRowkey() != null ? parameters.getStartRowkey() : "";
        String userEnd = parameters.getEndRowkey() != null ? parameters.getEndRowkey() : "";

        List<KeyOffset> samples;
        try {
            samples = getBigtableClient().sampleRowKeys();
        } catch (Exception e) {
            log.warn(
                    "sampleRowKeys failed for table [{}], fallback to single split",
                    parameters.getTable(),
                    e);
            return Collections.singleton(new BigtableSourceSplit(0, userStart, userEnd));
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

    /** Converts a sample key to a UTF-8 string; null or empty ByteString represents the table end. */
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

    private void assignSplit(int taskId) {
        List<BigtableSourceSplit> toAssign = new ArrayList<>();
        if (context.currentParallelism() == 1) {
            toAssign.addAll(pendingSplits);
        } else {
            for (BigtableSourceSplit split : pendingSplits) {
                int owner =
                        (split.splitId().hashCode() & Integer.MAX_VALUE)
                                % context.currentParallelism();
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

    private BigtableClient getBigtableClient() {
        if (bigtableClient == null) {
            bigtableClient = BigtableClient.createInstance(parameters);
        }
        return bigtableClient;
    }
}
