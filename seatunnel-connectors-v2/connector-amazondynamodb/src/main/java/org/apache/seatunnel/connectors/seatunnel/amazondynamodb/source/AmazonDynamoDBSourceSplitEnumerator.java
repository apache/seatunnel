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

package org.apache.seatunnel.connectors.seatunnel.amazondynamodb.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config.AmazonDynamoDBConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class AmazonDynamoDBSourceSplitEnumerator
        implements SourceSplitEnumerator<AmazonDynamoDBSourceSplit, AmazonDynamoDBSourceState> {

    private static final Logger log =
            LoggerFactory.getLogger(AmazonDynamoDBSourceSplitEnumerator.class);

    private final SourceSplitEnumerator.Context<AmazonDynamoDBSourceSplit> enumeratorContext;
    private final Map<Integer, List<AmazonDynamoDBSourceSplit>> pendingSplits;
    private final AmazonDynamoDBConfig amazonDynamoDBConfig;
    private final AtomicInteger assignCount = new AtomicInteger(0);

    private final Object stateLock = new Object();
    private volatile boolean shouldEnumerate;

    public AmazonDynamoDBSourceSplitEnumerator(
            Context<AmazonDynamoDBSourceSplit> enumeratorContext,
            AmazonDynamoDBConfig amazonDynamoDBConfig) {
        this(enumeratorContext, amazonDynamoDBConfig, null);
    }

    public AmazonDynamoDBSourceSplitEnumerator(
            Context<AmazonDynamoDBSourceSplit> enumeratorContext,
            AmazonDynamoDBConfig amazonDynamoDBConfig,
            AmazonDynamoDBSourceState sourceState) {
        this.enumeratorContext = enumeratorContext;
        this.amazonDynamoDBConfig = amazonDynamoDBConfig;
        this.pendingSplits = new HashMap<>();
        this.shouldEnumerate = sourceState == null;
        if (sourceState != null) {
            this.shouldEnumerate = sourceState.isShouldEnumerate();
            this.pendingSplits.putAll(sourceState.getPendingSplits());
            this.assignCount.set(sourceState.getAssignCount());
        }
    }

    @Override
    public void open() {}

    @Override
    public void run() throws Exception {
        Set<Integer> readers = enumeratorContext.registeredReaders();
        if (shouldEnumerate) {
            Set<AmazonDynamoDBSourceSplit> newSplits = discoverySplits();

            synchronized (stateLock) {
                addPendingSplit(newSplits);
                shouldEnumerate = false;
            }

            assignSplit(readers);
        }
    }

    private void assignSplit(Set<Integer> readers) {
        for (int reader : readers) {
            List<AmazonDynamoDBSourceSplit> assignmentForReader = pendingSplits.remove(reader);
            if (assignmentForReader != null && !assignmentForReader.isEmpty()) {
                log.info("Assign splits {} to reader {}", assignmentForReader, reader);
                try {
                    enumeratorContext.assignSplit(reader, assignmentForReader);
                } catch (Exception e) {
                    log.error(
                            "Failed to assign splits {} to reader {}",
                            assignmentForReader,
                            reader,
                            e);
                    pendingSplits.put(reader, assignmentForReader);
                }
            }
            enumeratorContext.signalNoMoreSplits(reader);
        }
    }

    private void addPendingSplit(Collection<AmazonDynamoDBSourceSplit> splits) {
        int readerCount = enumeratorContext.currentParallelism();
        splits.stream()
                .sorted(Comparator.comparing(AmazonDynamoDBSourceSplit::splitId))
                .forEach(
                        split -> {
                            int ownerReader =
                                    getSplitOwner(assignCount.getAndIncrement(), readerCount);
                            log.info("Assigning {} to {} reader.", split, ownerReader);
                            pendingSplits
                                    .computeIfAbsent(ownerReader, r -> new ArrayList<>())
                                    .add(split);
                        });
    }

    private void addPendingSplit(Collection<AmazonDynamoDBSourceSplit> splits, int ownerReader) {
        pendingSplits.computeIfAbsent(ownerReader, r -> new ArrayList<>()).addAll(splits);
    }

    private static int getSplitOwner(int assignCount, int numReaders) {
        return assignCount % numReaders;
    }

    private Set<AmazonDynamoDBSourceSplit> discoverySplits() {
        Set<AmazonDynamoDBSourceSplit> allSplit = new HashSet<>();
        int totalSegments = amazonDynamoDBConfig.parallelScanThreads;
        int itemLimit = amazonDynamoDBConfig.scanItemLimit;
        for (int i = 0; i < totalSegments; i++) {
            AmazonDynamoDBSourceSplit split =
                    new AmazonDynamoDBSourceSplit(i, totalSegments, itemLimit);

            allSplit.add(split);
        }
        return allSplit;
    }

    @Override
    public void close() throws IOException {}

    @Override
    public void addSplitsBack(List<AmazonDynamoDBSourceSplit> splits, int subtaskId) {
        log.debug("Add back splits {} to AmazonDynamoDBSourceSplitEnumerator.", splits);
        if (!splits.isEmpty()) {
            addPendingSplit(splits, subtaskId);
            if (enumeratorContext.registeredReaders().contains(subtaskId)) {
                assignSplit(Collections.singleton(subtaskId));
            } else {
                log.warn(
                        "Reader {} is not registered. Pending splits {} are not assigned.",
                        subtaskId,
                        splits);
            }
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplits.values().stream().mapToInt(List::size).sum();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {}

    @Override
    public void registerReader(int subtaskId) {
        log.debug("Register reader {} to AmazonDynamoDBSourceSplitEnumerator.", subtaskId);
        if (!pendingSplits.isEmpty()) {
            assignSplit(Collections.singleton(subtaskId));
        }
    }

    @Override
    public AmazonDynamoDBSourceState snapshotState(long checkpointId) throws Exception {
        synchronized (stateLock) {
            return new AmazonDynamoDBSourceState(shouldEnumerate, pendingSplits, assignCount.get());
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
