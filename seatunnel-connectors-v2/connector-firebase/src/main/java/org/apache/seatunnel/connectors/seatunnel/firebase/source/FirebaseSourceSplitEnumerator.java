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

package org.apache.seatunnel.connectors.seatunnel.firebase.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.firebase.client.FirebaseHttpClient;
import org.apache.seatunnel.connectors.seatunnel.firebase.config.FirebaseSourceOptions;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
public class FirebaseSourceSplitEnumerator
        implements SourceSplitEnumerator<FirebaseSourceSplit, FirebaseSourceState> {
    private final Context<FirebaseSourceSplit> context;
    private final ReadonlyConfig config;
    private final FirebaseHttpClient httpClient;

    private final Set<FirebaseSourceSplit> pendingSplits = new HashSet<>();
    private final Set<String> assignedSplitIds = new HashSet<>();

    public FirebaseSourceSplitEnumerator(
            Context<FirebaseSourceSplit> context, ReadonlyConfig config) {
        this.context = context;
        this.config = config;
        this.httpClient = new FirebaseHttpClient(config);
    }

    public FirebaseSourceSplitEnumerator(
            Context<FirebaseSourceSplit> context,
            ReadonlyConfig config,
            FirebaseSourceState state) {
        this(context, config);
        if (state != null) {
            this.pendingSplits.addAll(state.getPendingSplits());
            this.assignedSplitIds.addAll(state.getAssignedSplitIds());
        }
    }

    FirebaseSourceSplitEnumerator(
            Context<FirebaseSourceSplit> context,
            ReadonlyConfig config,
            FirebaseHttpClient httpClient) {
        this.context = context;
        this.config = config;
        this.httpClient = httpClient;
    }

    @Override
    public void open() {
        // No explicit persistent connections needed for REST client
    }

    @Override
    public void run() throws Exception {
        log.info("FirebaseSourceSplitEnumerator run()");
        if (!pendingSplits.isEmpty() || !assignedSplitIds.isEmpty()) {
            // Already initialized or restored from state
            return;
        }
        String basePath = config.get(FirebaseSourceOptions.PATH);
        List<FirebaseSourceSplit> generatedSplits = new ArrayList<>();

        try {
            log.info("Attempting automatic key discovery (shallow scan) on path [{}]", basePath);
            List<String> keys = httpClient.fetchShallowKeys();
            if (!keys.isEmpty()) {
                generatedSplits = partitionKeysIntoSplits(basePath, keys);
                log.info(
                        "Key discovery succeeded. Partitioned path into {} splits.",
                        generatedSplits.size());
            } else {
                log.info("Shallow scan returned no child keys. Falling back to Single Path Split.");
                generatedSplits.add(createSinglePathSplit(basePath));
            }
        } catch (Exception e) {
            log.warn(
                    "Shallow key discovery failed on path [{}]. "
                            + "Falling back to Single Path Split. Reason: {}",
                    basePath,
                    e.getMessage());
            generatedSplits.add(createSinglePathSplit(basePath));
        }
        pendingSplits.addAll(generatedSplits);
        log.info("pending Splits : {}", pendingSplits.toString());
        assignSplits();
    }

    @Override
    public void registerReader(int subtaskId) {
        log.info("Reader subtask [{}] registered.", subtaskId);
        assignSplits();
    }

    @Override
    public FirebaseSourceState snapshotState(long checkpointId) throws Exception {
        return new FirebaseSourceState(
                new HashSet<>(pendingSplits), new HashSet<>(assignedSplitIds));
    }

    @Override
    public void close() throws IOException {
        // Cleanup resources if needed
    }

    @Override
    public void addSplitsBack(List<FirebaseSourceSplit> splits, int subtaskId) {}

    @Override
    public int currentUnassignedSplitSize() {
        return 0;
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        assignSplits();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}

    /** Splits top-level keys among available worker threads according to current parallelism. */
    private List<FirebaseSourceSplit> partitionKeysIntoSplits(String basePath, List<String> keys) {
        int currentParallelism = Math.max(1, context.currentParallelism());
        List<FirebaseSourceSplit> splits = new ArrayList<>();

        int totalKeys = keys.size();
        int chunkSize = (int) Math.ceil((double) totalKeys / currentParallelism);
        for (int i = 0; i < totalKeys; i += chunkSize) {
            int end = Math.min(i + chunkSize, totalKeys);
            List<String> keySubList = new ArrayList<>(keys.subList(i, end));
            String splitId = "split_key_range_" + i + "_" + (end - 1);
            splits.add(new FirebaseSourceSplit(splitId, basePath, keySubList));
        }
        return splits;
    }

    private FirebaseSourceSplit createSinglePathSplit(String basePath) {
        return new FirebaseSourceSplit("split_single_path_" + basePath.hashCode(), basePath);
    }

    /** Assigns pending splits to available reader subtasks. */
    private synchronized void assignSplits() {
        if (pendingSplits.isEmpty()) {
            return;
        }
        Set<Integer> readers = context.registeredReaders();
        if (readers.isEmpty()) {
            return;
        }
        Set<FirebaseSourceSplit> assignedInThisBatch = new HashSet<>();
        for (FirebaseSourceSplit split : pendingSplits) {
            // Distribute splits across registered reader subtasks using round-robin indexing
            int targetReader = Math.abs(split.splitId().hashCode()) % readers.size();
            Integer readerId = new ArrayList<>(readers).get(targetReader);

            context.assignSplit(readerId, split);
            context.signalNoMoreSplits(readerId);

            assignedSplitIds.add(split.splitId());
            assignedInThisBatch.add(split);
            log.info("Assigned split [{}] to reader subtask [{}]", split.splitId(), readerId);
        }
        pendingSplits.removeAll(assignedInThisBatch);
        for (Integer readerId : readers) {
            context.signalNoMoreSplits(readerId);
            log.info("Signaled NO_MORE_SPLITS to reader subtask [{}]", readerId);
        }
    }
}
