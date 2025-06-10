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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.ClickhouseSourceState;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class ClickhouseSourceSplitEnumerator
        implements SourceSplitEnumerator<ClickHouseSourceSplit, ClickhouseSourceState> {

    private final Context<ClickHouseSourceSplit> context;
    private final ClickhouseSourceConfig sourceConfig;
    private final CatalogTable catalogTable;
    private final ClickhouseChunkSplitter splitter;
    private final Map<Integer, List<ClickHouseSourceSplit>> pendingSplits;
    private final Object stateLock = new Object();

    public ClickhouseSourceSplitEnumerator(
            Context<ClickHouseSourceSplit> context,
            ClickhouseSourceConfig sourceConfig,
            CatalogTable catalogTable) {
        this.context = context;
        this.sourceConfig = sourceConfig;
        this.catalogTable = catalogTable;
        this.splitter = new ClickhouseChunkSplitter();
        this.pendingSplits = new HashMap<>();
    }

    @Override
    public void open() {}

    @Override
    public void run() throws Exception {
        log.info("Starting split enumerator.");

        Set<Integer> readers = context.registeredReaders();

        synchronized (stateLock) {
            Collection<ClickHouseSourceSplit> splits =
                    splitter.generateSplits(sourceConfig, catalogTable);
            addPendingSplit(splits);
        }

        synchronized (stateLock) {
            assignSplit(readers);
        }

        log.info("No more splits to assign." + " Sending NoMoreSplitsEvent to reader {}.", readers);
        readers.forEach(context::signalNoMoreSplits);
    }

    private void assignSplit(Collection<Integer> readers) {
        log.info("Assign pendingSplits to readers {}", readers);

        for (int reader : readers) {
            List<ClickHouseSourceSplit> splits = pendingSplits.remove(reader);
            if (splits != null && !splits.isEmpty()) {
                String splitIds =
                        splits.stream()
                                .map(ClickHouseSourceSplit::getSplitId)
                                .collect(Collectors.joining(", "));
                log.info("Assign splits {} to reader {}", splitIds, reader);
                context.assignSplit(reader, splits);
            }
        }
    }

    @Override
    public void close() throws IOException {}

    @Override
    public void addSplitsBack(List<ClickHouseSourceSplit> splits, int subtaskId) {}

    @Override
    public int currentUnassignedSplitSize() {
        return 0;
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        throw new ClickhouseConnectorException(
                CommonErrorCode.UNSUPPORTED_METHOD,
                String.format("Unsupported handleSplitRequest: %d", subtaskId));
    }

    @Override
    public void registerReader(int subtaskId) {
        log.info("Register reader {} to ClickhouseSourceSplitEnumerator.", subtaskId);
    }

    @Override
    public ClickhouseSourceState snapshotState(long checkpointId) throws Exception {
        synchronized (stateLock) {
            return new ClickhouseSourceState();
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}

    private void addPendingSplit(Collection<ClickHouseSourceSplit> splits) {
        int readerCount = context.currentParallelism();
        for (ClickHouseSourceSplit split : splits) {
            int ownerReader = getSplitOwner(split.splitId(), readerCount);

            pendingSplits.computeIfAbsent(ownerReader, r -> new ArrayList<>()).add(split);
        }
    }

    private static int getSplitOwner(String splitId, int numReaders) {
        return splitId.hashCode() % numReaders;
    }
}
