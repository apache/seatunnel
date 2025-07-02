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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.source.split;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.source.ClickhouseSourceTable;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.ClickhouseSourceState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ClickhouseSourceSplitEnumerator
        implements SourceSplitEnumerator<ClickhouseSourceSplit, ClickhouseSourceState> {
    private static final Logger LOG =
            LoggerFactory.getLogger(ClickhouseSourceSplitEnumerator.class);

    private final ClickhouseSourceConfig clickhouseSourceConfig;
    private final Map<TablePath, ClickhouseSourceTable> clickhouseSourceTables;
    private volatile boolean shouldEnumerate;
    private final Map<Integer, List<ClickhouseSourceSplit>> pendingSplit;
    private final Splitter splitter;
    private final Context<ClickhouseSourceSplit> context;
    private final Object stateLock = new Object();

    public ClickhouseSourceSplitEnumerator(
            Context<ClickhouseSourceSplit> context,
            ClickhouseSourceConfig clickhouseSourceConfig,
            Map<TablePath, ClickhouseSourceTable> clickhouseSourceTables) {
        this(context, clickhouseSourceConfig, clickhouseSourceTables, null);
    }

    public ClickhouseSourceSplitEnumerator(
            Context<ClickhouseSourceSplit> context,
            ClickhouseSourceConfig clickhouseSourceConfig,
            Map<TablePath, ClickhouseSourceTable> clickhouseSourceTables,
            ClickhouseSourceState sourceState) {
        this.context = context;
        this.clickhouseSourceConfig = clickhouseSourceConfig;
        this.clickhouseSourceTables = clickhouseSourceTables;
        this.splitter = Splitter.createSplitter(clickhouseSourceConfig);
        this.pendingSplit = new ConcurrentHashMap<>();
        this.shouldEnumerate = (sourceState == null);
        if (sourceState != null) {
            this.shouldEnumerate = sourceState.isShouldEnumerate();
            this.pendingSplit.putAll(sourceState.getPendingSplit());
        }
    }

    @Override
    public void open() {}

    @Override
    public void run() throws Exception {
        LOG.info("Starting split enumerator.");

        Set<Integer> readers = context.registeredReaders();
        if (shouldEnumerate) {
            synchronized (stateLock) {
                if (shouldEnumerate) {
                    List<ClickhouseSourceSplit> clickhouseSourceSplits =
                            getClickhouseSourceSplits();
                    addPendingSplit(clickhouseSourceSplits);
                    shouldEnumerate = false;
                    assignSplit(readers);
                }
            }
        }

        LOG.info("No more splits to assign." + " Sending NoMoreSplitsEvent to reader {}.", readers);
        readers.forEach(context::signalNoMoreSplits);
    }

    @Override
    public void close() throws IOException {
        splitter.close();
    }

    @Override
    public void addSplitsBack(List<ClickhouseSourceSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            synchronized (stateLock) {
                addPendingSplit(splits, subtaskId);
                if (context.registeredReaders().contains(subtaskId)) {
                    assignSplit(Collections.singletonList(subtaskId));
                } else {
                    LOG.warn(
                            "Reader {} is not registered. Pending splits {} are not assigned.",
                            subtaskId,
                            splits);
                }
            }
        }
        LOG.info("Add back splits {} to JdbcSourceSplitEnumerator.", splits.size());
    }

    @Override
    public int currentUnassignedSplitSize() {
        return this.pendingSplit.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        throw new ClickhouseConnectorException(
                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                String.format("Unsupported handleSplitRequest: %d", subtaskId));
    }

    @Override
    public void registerReader(int subtaskId) {
        LOG.info("Register reader {} to ClickhouseSourceSplitEnumerator.", subtaskId);
        if (!pendingSplit.isEmpty()) {
            synchronized (stateLock) {
                assignSplit(Collections.singletonList(subtaskId));
            }
        }
    }

    @Override
    public ClickhouseSourceState snapshotState(long checkpointId) throws Exception {
        synchronized (stateLock) {
            return new ClickhouseSourceState(shouldEnumerate, pendingSplit);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}

    private List<ClickhouseSourceSplit> getClickhouseSourceSplits() {
        List<ClickhouseSourceSplit> splits = new ArrayList<>();
        for (ClickhouseSourceTable clickhouseSourceTable : clickhouseSourceTables.values()) {
            List<ClickhouseSourceSplit> sourceSplits =
                    splitter.generateSplits(clickhouseSourceTable);
            splits.addAll(sourceSplits);
        }

        return splits;
    }

    private void assignSplit(Collection<Integer> readers) {
        LOG.debug("Assign pendingSplits to readers {}", readers);

        for (int reader : readers) {
            List<ClickhouseSourceSplit> assignmentForReader = pendingSplit.remove(reader);
            if (assignmentForReader != null && !assignmentForReader.isEmpty()) {
                LOG.debug("Assign splits {} to reader {}", assignmentForReader, reader);
                context.assignSplit(reader, assignmentForReader);
            }
        }
    }

    private void addPendingSplit(Collection<ClickhouseSourceSplit> splits) {
        int readerCount = context.currentParallelism();
        for (ClickhouseSourceSplit split : splits) {
            int ownerReader = getSplitOwner(split.splitId(), readerCount);
            LOG.debug("Assigning {} to {} reader.", split, ownerReader);

            pendingSplit.computeIfAbsent(ownerReader, r -> new ArrayList<>()).add(split);
        }
    }

    private void addPendingSplit(Collection<ClickhouseSourceSplit> splits, int ownerReader) {
        pendingSplit.computeIfAbsent(ownerReader, r -> new ArrayList<>()).addAll(splits);
    }

    private static int getSplitOwner(String tp, int numReaders) {
        return (tp.hashCode() & Integer.MAX_VALUE) % numReaders;
    }
}
