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

package org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.source;

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

public class AzureCosmosDBSourceSplitEnumerator
        implements SourceSplitEnumerator<AzureCosmosDBSourceSplit, AzureCosmosDBSourceState> {

    private static final Logger LOG =
            LoggerFactory.getLogger(AzureCosmosDBSourceSplitEnumerator.class);

    private final SourceSplitEnumerator.Context<AzureCosmosDBSourceSplit> enumeratorContext;
    private final Map<Integer, List<AzureCosmosDBSourceSplit>> pendingSplits;
    private final Object stateLock = new Object();

    private volatile boolean shouldEnumerate;

    public AzureCosmosDBSourceSplitEnumerator(
            Context<AzureCosmosDBSourceSplit> enumeratorContext,
            AzureCosmosDBSourceState sourceState) {
        this.enumeratorContext = enumeratorContext;
        this.pendingSplits = new HashMap<>();
        this.shouldEnumerate = sourceState == null;
        if (sourceState != null) {
            this.shouldEnumerate = sourceState.isShouldEnumerate();
            this.pendingSplits.putAll(sourceState.getPendingSplits());
        }
    }

    @Override
    public void open() {
        // no-op
    }

    @Override
    public void run() throws Exception {
        Set<Integer> readers = enumeratorContext.registeredReaders();
        if (shouldEnumerate) {
            synchronized (stateLock) {
                addPendingSplits(Collections.singletonList(new AzureCosmosDBSourceSplit(0)));
                shouldEnumerate = false;
            }
            assignSplit(readers);
        }
    }

    @Override
    public void close() throws IOException {
        // no-op
    }

    @Override
    public void addSplitsBack(List<AzureCosmosDBSourceSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            addPendingSplits(splits);
            assignSplit(Collections.singleton(subtaskId));
            enumeratorContext.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplits.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        // no-op
    }

    @Override
    public void registerReader(int subtaskId) {
        if (!pendingSplits.isEmpty()) {
            assignSplit(Collections.singleton(subtaskId));
        }
    }

    @Override
    public AzureCosmosDBSourceState snapshotState(long checkpointId) throws Exception {
        synchronized (stateLock) {
            return new AzureCosmosDBSourceState(shouldEnumerate, pendingSplits);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // no-op
    }

    private void addPendingSplits(Collection<AzureCosmosDBSourceSplit> splits) {
        int readerCount = enumeratorContext.currentParallelism();
        for (AzureCosmosDBSourceSplit split : splits) {
            int ownerReader = getSplitOwner(split.getSplitId(), readerCount);
            pendingSplits.computeIfAbsent(ownerReader, id -> new ArrayList<>()).add(split);
        }
    }

    private void assignSplit(Set<Integer> readers) {
        for (int reader : readers) {
            List<AzureCosmosDBSourceSplit> assignment = pendingSplits.remove(reader);
            if (assignment != null && !assignment.isEmpty()) {
                LOG.info("Assign splits {} to reader {}", assignment, reader);
                try {
                    enumeratorContext.assignSplit(reader, assignment);
                } catch (Exception e) {
                    LOG.error("Failed to assign splits {} to reader {}", assignment, reader, e);
                    pendingSplits.put(reader, assignment);
                }
            }
            enumeratorContext.signalNoMoreSplits(reader);
        }
    }

    private static int getSplitOwner(Integer splitId, int numReaders) {
        return (splitId.hashCode() & Integer.MAX_VALUE) % numReaders;
    }
}
