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

package org.apache.seatunnel.connectors.seatunnel.typesense.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.typesense.client.TypesenseClient;
import org.apache.seatunnel.connectors.seatunnel.typesense.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.typesense.dto.SourceCollectionInfo;
import org.apache.seatunnel.connectors.seatunnel.typesense.exception.TypesenseConnectorException;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class TypesenseSourceSplitEnumerator
        implements SourceSplitEnumerator<TypesenseSourceSplit, TypesenseSourceState> {

    private final SourceSplitEnumerator.Context<TypesenseSourceSplit> context;

    private final ReadonlyConfig config;

    private TypesenseClient typesenseClient;

    private final Object stateLock = new Object();

    private Map<Integer, List<TypesenseSourceSplit>> pendingSplit;

    private volatile boolean shouldEnumerate;

    public TypesenseSourceSplitEnumerator(
            SourceSplitEnumerator.Context<TypesenseSourceSplit> context, ReadonlyConfig config) {
        this(context, null, config);
    }

    public TypesenseSourceSplitEnumerator(
            SourceSplitEnumerator.Context<TypesenseSourceSplit> context,
            TypesenseSourceState sourceState,
            ReadonlyConfig config) {
        this.context = context;
        this.config = config;
        this.pendingSplit = new HashMap<>();
        this.shouldEnumerate = sourceState == null;
        if (sourceState != null) {
            this.shouldEnumerate = sourceState.isShouldEnumerate();
            this.pendingSplit.putAll(sourceState.getPendingSplit());
        }
    }

    @Override
    public void open() {
        // Nothing
    }

    @Override
    public void run() throws Exception {
        Set<Integer> readers = context.registeredReaders();
        if (shouldEnumerate) {
            List<TypesenseSourceSplit> newSplits = getTypesenseSplit();

            synchronized (stateLock) {
                addPendingSplit(newSplits);
                shouldEnumerate = false;
            }

            assignSplit(readers);
        }

        log.debug(
                "No more splits to assign." + " Sending NoMoreSplitsEvent to reader {}.", readers);
        readers.forEach(context::signalNoMoreSplits);
    }

    private void addPendingSplit(Collection<TypesenseSourceSplit> splits) {
        int readerCount = context.currentParallelism();
        for (TypesenseSourceSplit split : splits) {
            int ownerReader = getSplitOwner(split.splitId(), readerCount);
            log.info("Assigning {} to {} reader.", split, ownerReader);
            pendingSplit.computeIfAbsent(ownerReader, r -> new ArrayList<>()).add(split);
        }
    }

    private void assignSplit(Collection<Integer> readers) {
        log.debug("Assign pendingSplits to readers {}", readers);

        for (int reader : readers) {
            List<TypesenseSourceSplit> assignmentForReader = pendingSplit.remove(reader);
            if (assignmentForReader != null && !assignmentForReader.isEmpty()) {
                log.info("Assign splits {} to reader {}", assignmentForReader, reader);
                try {
                    context.assignSplit(reader, assignmentForReader);
                } catch (Exception e) {
                    log.error(
                            "Failed to assign splits {} to reader {}",
                            assignmentForReader,
                            reader,
                            e);
                    pendingSplit.put(reader, assignmentForReader);
                }
            }
        }
    }

    private static int getSplitOwner(String tp, int numReaders) {
        return (tp.hashCode() & Integer.MAX_VALUE) % numReaders;
    }

    private List<TypesenseSourceSplit> getTypesenseSplit() {
        List<TypesenseSourceSplit> splits = new ArrayList<>();

        String collection = config.get(SourceConfig.COLLECTION);
        String query = config.get(SourceConfig.QUERY);
        int queryBatchSize = config.get(SourceConfig.QUERY_BATCH_SIZE);
        splits.add(
                new TypesenseSourceSplit(
                        collection,
                        new SourceCollectionInfo(collection, query, 0, 0, queryBatchSize)));
        return splits;
    }

    @Override
    public void close() throws IOException {
        // Nothing
    }

    @Override
    public void addSplitsBack(List<TypesenseSourceSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            addPendingSplit(splits);
            assignSplit(Collections.singletonList(subtaskId));
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplit.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        throw new TypesenseConnectorException(
                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                "Unsupported handleSplitRequest: " + subtaskId);
    }

    @Override
    public void registerReader(int subtaskId) {
        log.debug("Register reader {} to IoTDBSourceSplitEnumerator.", subtaskId);
        if (!pendingSplit.isEmpty()) {
            assignSplit(Collections.singletonList(subtaskId));
        }
    }

    @Override
    public TypesenseSourceState snapshotState(long checkpointId) throws Exception {
        synchronized (stateLock) {
            return new TypesenseSourceState(shouldEnumerate, pendingSplit);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
