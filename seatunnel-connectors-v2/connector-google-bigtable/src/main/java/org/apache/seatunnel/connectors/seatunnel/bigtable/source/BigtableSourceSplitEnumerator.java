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
import org.apache.seatunnel.connectors.seatunnel.bigtable.config.BigtableParameters;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Enumerates {@link BigtableSourceSplit} instances for parallel reading.
 *
 * <p>Currently produces a single split covering the full table (or the user-defined row-key range).
 * The split is assigned to whichever reader registers first. Future work can partition by Bigtable
 * tablet boundaries using the Admin API.
 */
@Slf4j
public class BigtableSourceSplitEnumerator
        implements SourceSplitEnumerator<BigtableSourceSplit, BigtableSourceState> {

    private final Context<BigtableSourceSplit> context;
    private final BigtableParameters parameters;
    private final Set<BigtableSourceSplit> assignedSplits;
    private Set<BigtableSourceSplit> pendingSplits;
    private boolean initialized = false;

    public BigtableSourceSplitEnumerator(
            Context<BigtableSourceSplit> context, BigtableParameters parameters) {
        this(context, parameters, new HashSet<>());
    }

    public BigtableSourceSplitEnumerator(
            Context<BigtableSourceSplit> context,
            BigtableParameters parameters,
            BigtableSourceState sourceState) {
        this(context, parameters, sourceState.getAssignedSplits());
    }

    private BigtableSourceSplitEnumerator(
            Context<BigtableSourceSplit> context,
            BigtableParameters parameters,
            Set<BigtableSourceSplit> assignedSplits) {
        this.context = context;
        this.parameters = parameters;
        this.assignedSplits = new HashSet<>(assignedSplits);
    }

    @Override
    public void open() {
        this.pendingSplits = new HashSet<>();
        this.initialized = false;
    }

    @Override
    public void run() throws Exception {
        // Splits are assigned lazily when readers register.
    }

    @Override
    public void close() throws IOException {
        // Nothing to close – no persistent connection held here.
    }

    @Override
    public void addSplitsBack(List<BigtableSourceSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            pendingSplits.addAll(splits);
            if (context.registeredReaders().contains(subtaskId)) {
                assignSplit(subtaskId);
            }
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplits.size();
    }

    @Override
    public void registerReader(int subtaskId) {
        initializePendingSplits();
        assignSplit(subtaskId);
    }

    @Override
    public BigtableSourceState snapshotState(long checkpointId) throws Exception {
        return new BigtableSourceState(assignedSplits);
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
     * Builds the set of splits.
     *
     * <p>For now a single split spanning the requested row-key range is produced. This is
     * sufficient for bounded batch reads. Parallel multi-split support can be added later by
     * querying Bigtable tablet boundary information.
     */
    private Set<BigtableSourceSplit> buildSplits() {
        String startKey = parameters.getStartRowkey() != null ? parameters.getStartRowkey() : "";
        String endKey = parameters.getEndRowkey() != null ? parameters.getEndRowkey() : "";
        return Collections.singleton(new BigtableSourceSplit(0, startKey, endKey));
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
}
