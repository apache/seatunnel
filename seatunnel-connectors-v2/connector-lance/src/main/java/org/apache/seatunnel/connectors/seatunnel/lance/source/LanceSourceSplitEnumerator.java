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

package org.apache.seatunnel.connectors.seatunnel.lance.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.lance.config.LanceSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.lance.exception.LanceConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.lance.exception.LanceConnectorException;

import com.lancedb.lance.Dataset;
import com.lancedb.lance.Fragment;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Splits a lance dataset into one split per fragment and assigns them round-robin, mirroring the
 * milvus source enumerator.
 */
@Slf4j
public class LanceSourceSplitEnumerator
        implements SourceSplitEnumerator<LanceSourceSplit, LanceSourceState> {

    private final Context<LanceSourceSplit> context;
    private final LanceSourceConfig config;

    private final ConcurrentLinkedQueue<Integer> pendingFragmentIds;
    private final Map<Integer, List<LanceSourceSplit>> pendingSplits;
    private final Object stateLock = new Object();
    // Keeps round-robin assignment global across fragments and restore.
    private final AtomicInteger assignCount = new AtomicInteger(0);

    public LanceSourceSplitEnumerator(
            Context<LanceSourceSplit> context,
            LanceSourceConfig config,
            LanceSourceState sourceState) {
        this.context = context;
        this.config = config;
        if (sourceState == null) {
            this.pendingFragmentIds = new ConcurrentLinkedQueue<>();
            this.pendingSplits = new HashMap<>();
        } else {
            this.pendingFragmentIds =
                    new ConcurrentLinkedQueue<>(sourceState.getPendingFragmentIds());
            this.pendingSplits = new HashMap<>(sourceState.getPendingSplits());
            this.assignCount.set(sourceState.getAssignCount());
        }
    }

    @Override
    public void open() {}

    @Override
    public void run() throws Exception {
        log.info("Starting lance split enumerator for dataset {}", config.getDatasetPath());
        Set<Integer> readers = context.registeredReaders();
        synchronized (stateLock) {
            if (pendingFragmentIds.isEmpty()) {
                pendingFragmentIds.addAll(discoverFragments());
            }
            while (!pendingFragmentIds.isEmpty()) {
                int fragmentId = pendingFragmentIds.poll();
                LanceSourceSplit split =
                        LanceSourceSplit.builder()
                                .fragmentId(fragmentId)
                                .splitId(String.valueOf(fragmentId))
                                .build();
                log.info("Generated split for fragment {}", fragmentId);
                addPendingSplit(Collections.singletonList(split));
            }
            assignSplit(readers);
        }

        log.info("No more splits to assign. Sending NoMoreSplitsEvent to reader {}.", readers);
        readers.forEach(context::signalNoMoreSplits);
    }

    /** Lists the fragment ids of the dataset at its latest version. */
    private Collection<Integer> discoverFragments() {
        try (Dataset dataset = Dataset.open(config.getDatasetPath(), config.toReadOptions())) {
            List<Integer> fragmentIds =
                    dataset.getFragments().stream()
                            .map(Fragment::getId)
                            .sorted()
                            .collect(Collectors.toList());
            log.info(
                    "Dataset {} has {} fragments: {}",
                    config.getDatasetPath(),
                    fragmentIds.size(),
                    fragmentIds);
            return fragmentIds;
        } catch (Exception e) {
            throw new LanceConnectorException(
                    LanceConnectorErrorCode.TABLE_DATASET_PATH_OPEN_EXCEPTION,
                    String.format(
                            "Failed to open dataset %s to enumerate fragments: %s",
                            config.getDatasetPath(), e.getMessage()),
                    e);
        }
    }

    private void addPendingSplit(Collection<LanceSourceSplit> splits) {
        int readerCount = context.currentParallelism();

        List<LanceSourceSplit> sortedSplits =
                splits.stream()
                        .sorted(Comparator.comparing(LanceSourceSplit::getSplitId))
                        .collect(Collectors.toList());

        for (LanceSourceSplit split : sortedSplits) {
            int ownerReader = getSplitOwner(assignCount.getAndIncrement(), readerCount);
            log.info("Assigning {} to {} reader.", split, ownerReader);

            pendingSplits.computeIfAbsent(ownerReader, r -> new ArrayList<>()).add(split);
        }
    }

    private static int getSplitOwner(int assignCount, int numReaders) {
        return assignCount % numReaders;
    }

    private void assignSplit(Collection<Integer> readers) {
        log.info("Assign pendingSplits to readers {}", readers);

        for (int reader : readers) {
            List<LanceSourceSplit> assignmentForReader = pendingSplits.remove(reader);
            if (assignmentForReader != null && !assignmentForReader.isEmpty()) {
                log.debug("Assign splits {} to reader {}", assignmentForReader, reader);
                context.assignSplit(reader, assignmentForReader);
            }
        }
    }

    @Override
    public void close() throws IOException {}

    @Override
    public void addSplitsBack(List<LanceSourceSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            synchronized (stateLock) {
                addPendingSplit(splits, subtaskId);
                if (context.registeredReaders().contains(subtaskId)) {
                    assignSplit(Collections.singletonList(subtaskId));
                } else {
                    log.warn(
                            "Reader {} is not registered. Pending splits {} are not assigned.",
                            subtaskId,
                            splits);
                }
            }
        }
        log.info("Add back splits {} to LanceSourceSplitEnumerator.", splits.size());
    }

    private void addPendingSplit(Collection<LanceSourceSplit> splits, int ownerReader) {
        pendingSplits.computeIfAbsent(ownerReader, r -> new ArrayList<>()).addAll(splits);
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingFragmentIds.isEmpty() && pendingSplits.isEmpty() ? 0 : 1;
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        log.warn("Lance source does not support split request from reader {}.", subtaskId);
    }

    @Override
    public void registerReader(int subtaskId) {
        log.info("Register reader {} to LanceSourceSplitEnumerator.", subtaskId);
        synchronized (stateLock) {
            if (!pendingSplits.isEmpty()) {
                assignSplit(Collections.singletonList(subtaskId));
            }
        }
    }

    @Override
    public LanceSourceState snapshotState(long checkpointId) throws Exception {
        synchronized (stateLock) {
            return new LanceSourceState(
                    new ArrayList<>(pendingFragmentIds),
                    new HashMap<>(pendingSplits),
                    assignCount.get());
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
