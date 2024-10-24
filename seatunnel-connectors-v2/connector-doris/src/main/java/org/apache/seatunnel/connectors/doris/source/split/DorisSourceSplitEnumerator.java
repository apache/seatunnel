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

package org.apache.seatunnel.connectors.doris.source.split;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.doris.config.DorisSourceConfig;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorException;
import org.apache.seatunnel.connectors.doris.rest.PartitionDefinition;
import org.apache.seatunnel.connectors.doris.rest.RestService;
import org.apache.seatunnel.connectors.doris.source.DorisSourceState;
import org.apache.seatunnel.connectors.doris.source.DorisSourceTable;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class DorisSourceSplitEnumerator
        implements SourceSplitEnumerator<DorisSourceSplit, DorisSourceState> {

    private final Context<DorisSourceSplit> context;
    private final DorisSourceConfig dorisSourceConfig;

    private volatile boolean shouldEnumerate;

    private final Map<Integer, List<DorisSourceSplit>> pendingSplit;

    private final Map<TablePath, DorisSourceTable> dorisSourceTables;
    private final Object stateLock = new Object();

    public DorisSourceSplitEnumerator(
            Context<DorisSourceSplit> context,
            DorisSourceConfig dorisSourceConfig,
            Map<TablePath, DorisSourceTable> dorisSourceTables) {
        this(context, dorisSourceConfig, dorisSourceTables, null);
    }

    public DorisSourceSplitEnumerator(
            Context<DorisSourceSplit> context,
            DorisSourceConfig dorisSourceConfig,
            Map<TablePath, DorisSourceTable> dorisSourceTables,
            DorisSourceState dorisSourceState) {
        this.context = context;
        this.dorisSourceConfig = dorisSourceConfig;
        this.dorisSourceTables = dorisSourceTables;
        this.pendingSplit = new ConcurrentHashMap<>();
        this.shouldEnumerate = (dorisSourceState == null);
        if (dorisSourceState != null) {
            this.shouldEnumerate = dorisSourceState.isShouldEnumerate();
            this.pendingSplit.putAll(dorisSourceState.getPendingSplit());
        }
    }

    @Override
    public void open() {}

    @Override
    public void close() throws IOException {}

    @Override
    public void run() {
        Set<Integer> readers = context.registeredReaders();
        if (shouldEnumerate) {
            List<DorisSourceSplit> dorisSourceSplits = getDorisSourceSplit();
            synchronized (stateLock) {
                addPendingSplit(dorisSourceSplits);
                shouldEnumerate = false;
                assignSplit(readers);
            }
        }

        log.debug(
                "No more splits to assign." + " Sending NoMoreSplitsEvent to reader {}.", readers);
        readers.forEach(context::signalNoMoreSplits);
    }

    @Override
    public void addSplitsBack(List<DorisSourceSplit> splits, int subtaskId) {
        log.debug("Add back splits {} to DorisSourceSplitEnumerator.", splits);
        if (!splits.isEmpty()) {
            synchronized (stateLock) {
                addPendingSplit(splits);
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
    }

    @Override
    public int currentUnassignedSplitSize() {
        return this.pendingSplit.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        throw new DorisConnectorException(
                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                String.format("Unsupported handleSplitRequest: %d", subtaskId));
    }

    @Override
    public void registerReader(int subtaskId) {
        log.debug("Register reader {} to DorisSourceSplitEnumerator.", subtaskId);
        if (!pendingSplit.isEmpty()) {
            synchronized (stateLock) {
                assignSplit(Collections.singletonList(subtaskId));
            }
        }
    }

    @Override
    public DorisSourceState snapshotState(long checkpointId) {
        synchronized (stateLock) {
            return new DorisSourceState(shouldEnumerate, pendingSplit);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    private List<DorisSourceSplit> getDorisSourceSplit() {
        List<DorisSourceSplit> splits = new ArrayList<>();
        for (DorisSourceTable dorisSourceTable : dorisSourceTables.values()) {
            List<PartitionDefinition> partitions =
                    RestService.findPartitions(dorisSourceConfig, dorisSourceTable, log);
            for (PartitionDefinition partition : partitions) {
                splits.add(new DorisSourceSplit(partition, String.valueOf(partition.hashCode())));
            }
        }
        return splits;
    }

    private void addPendingSplit(Collection<DorisSourceSplit> splits) {
        int readerCount = context.currentParallelism();
        for (DorisSourceSplit split : splits) {
            int ownerReader = getSplitOwner(split.splitId(), readerCount);
            log.info("Assigning split {} to reader {} .", split.splitId(), ownerReader);
            pendingSplit.computeIfAbsent(ownerReader, f -> new ArrayList<>()).add(split);
        }
    }

    private static int getSplitOwner(String tp, int numReaders) {
        return (tp.hashCode() & Integer.MAX_VALUE) % numReaders;
    }

    private void assignSplit(Collection<Integer> readers) {
        for (Integer reader : readers) {
            final List<DorisSourceSplit> assignmentForReader = pendingSplit.remove(reader);
            if (assignmentForReader != null && !assignmentForReader.isEmpty()) {
                log.debug("Assign splits {} to reader {}", assignmentForReader, reader);
                context.assignSplit(reader, assignmentForReader);
            }
        }
    }
}
