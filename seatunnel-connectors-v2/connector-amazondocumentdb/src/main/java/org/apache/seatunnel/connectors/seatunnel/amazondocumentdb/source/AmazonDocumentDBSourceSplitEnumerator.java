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

package org.apache.seatunnel.connectors.seatunnel.amazondocumentdb.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AmazonDocumentDBSourceSplitEnumerator
        implements SourceSplitEnumerator<AmazonDocumentDBSourceSplit, AmazonDocumentDBSourceState> {

    private static final Logger LOG =
            LoggerFactory.getLogger(AmazonDocumentDBSourceSplitEnumerator.class);

    private final Context<AmazonDocumentDBSourceSplit> enumeratorContext;
    private final String matchQuery;
    private final String projection;
    private final Map<Integer, List<AmazonDocumentDBSourceSplit>> pendingSplits = new HashMap<>();
    private final Object stateLock = new Object();

    private boolean shouldEnumerate;

    public AmazonDocumentDBSourceSplitEnumerator(
            Context<AmazonDocumentDBSourceSplit> enumeratorContext,
            AmazonDocumentDBSourceState sourceState,
            String matchQuery,
            String projection) {
        this.enumeratorContext = enumeratorContext;
        this.matchQuery = matchQuery;
        this.projection = projection;
        this.shouldEnumerate = sourceState == null || sourceState.isShouldEnumerate();
        if (sourceState != null) {
            this.pendingSplits.putAll(sourceState.getPendingSplits());
        }
    }

    @Override
    public void open() {
        // no-op
    }

    @Override
    public void run() {
        synchronized (stateLock) {
            if (shouldEnumerate) {
                addPendingSplits(
                        Collections.singletonList(
                                new AmazonDocumentDBSourceSplit(0, matchQuery, projection)));
                shouldEnumerate = false;
            }
            assignSplits(enumeratorContext.registeredReaders());
        }
    }

    @Override
    public void close() throws IOException {
        // no-op
    }

    @Override
    public void addSplitsBack(List<AmazonDocumentDBSourceSplit> splits, int subtaskId) {
        synchronized (stateLock) {
            addPendingSplits(splits);
            assignSplits(Collections.singleton(subtaskId));
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        synchronized (stateLock) {
            return pendingSplits.values().stream().mapToInt(List::size).sum();
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        // The bounded source enumerates its only split eagerly.
    }

    @Override
    public void registerReader(int subtaskId) {
        synchronized (stateLock) {
            assignSplits(Collections.singleton(subtaskId));
        }
    }

    @Override
    public AmazonDocumentDBSourceState snapshotState(long checkpointId) {
        synchronized (stateLock) {
            return new AmazonDocumentDBSourceState(shouldEnumerate, pendingSplits);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // no-op
    }

    private void addPendingSplits(Collection<AmazonDocumentDBSourceSplit> splits) {
        int readerCount = enumeratorContext.currentParallelism();
        for (AmazonDocumentDBSourceSplit split : splits) {
            int ownerReader = getSplitOwner(split.getSplitId(), readerCount);
            pendingSplits.computeIfAbsent(ownerReader, ignored -> new ArrayList<>()).add(split);
        }
    }

    private void assignSplits(Set<Integer> readers) {
        for (int reader : readers) {
            List<AmazonDocumentDBSourceSplit> assignment = pendingSplits.remove(reader);
            if (assignment != null && !assignment.isEmpty()) {
                LOG.info("Assign splits {} to reader {}", assignment, reader);
                try {
                    enumeratorContext.assignSplit(reader, assignment);
                } catch (Exception e) {
                    LOG.error("Failed to assign splits {} to reader {}", assignment, reader, e);
                    pendingSplits.put(reader, assignment);
                    continue;
                }
            }
            enumeratorContext.signalNoMoreSplits(reader);
        }
    }

    private static int getSplitOwner(Integer splitId, int readerCount) {
        return (splitId.hashCode() & Integer.MAX_VALUE) % readerCount;
    }
}
