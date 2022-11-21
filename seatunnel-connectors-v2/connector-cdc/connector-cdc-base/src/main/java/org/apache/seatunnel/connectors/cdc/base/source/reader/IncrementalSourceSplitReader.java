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

package org.apache.seatunnel.connectors.cdc.base.source.reader;

import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.Fetcher;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.IncrementalSourceScanFetcher;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.IncrementalSourceStreamFetcher;
import org.apache.seatunnel.connectors.cdc.base.source.split.ChangeEventRecords;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceRecords;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsWithSplitIds;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitsAddition;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitsChange;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;

@Slf4j
public class IncrementalSourceSplitReader<C extends SourceConfig>
        implements SplitReader<SourceRecords, SourceSplitBase> {
    private final Queue<SourceSplitBase> splits;
    private final int subtaskId;

    private Fetcher<SourceRecords, SourceSplitBase> currentFetcher;

    private String currentSplitId;
    private final DataSourceDialect<C> dataSourceDialect;
    private final C sourceConfig;

    public IncrementalSourceSplitReader(
        int subtaskId, DataSourceDialect<C> dataSourceDialect, C sourceConfig) {
        this.subtaskId = subtaskId;
        this.splits = new ArrayDeque<>();
        this.dataSourceDialect = dataSourceDialect;
        this.sourceConfig = sourceConfig;
    }

    @Override
    public RecordsWithSplitIds<SourceRecords> fetch() throws IOException {

        checkSplitOrStartNext();
        checkNeedStopBinlogReader();
        Iterator<SourceRecords> dataIt = null;
        try {
            dataIt = currentFetcher.pollSplitRecords();
        } catch (InterruptedException e) {
            log.warn("fetch data failed.", e);
            throw new IOException(e);
        }
        return dataIt == null ? finishedSnapshotSplit()
                : ChangeEventRecords.forRecords(currentSplitId, dataIt);
    }

    @Override
    public void handleSplitsChanges(SplitsChange<SourceSplitBase> splitsChanges) {
        if (!(splitsChanges instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChanges.getClass()));
        }

        log.debug("Handling split change {}", splitsChanges);
        splits.addAll(splitsChanges.splits());
    }

    @Override
    public void wakeUp() {}

    @Override
    public void close() throws Exception {
        if (currentFetcher != null) {
            log.info("Close current fetcher {}", currentFetcher.getClass().getCanonicalName());
            currentFetcher.close();
            currentSplitId = null;
        }
    }

    private void checkNeedStopBinlogReader() {
        // TODO Currently not supported
    }

    protected void checkSplitOrStartNext() throws IOException {
        // the stream fetcher should keep alive
        if (currentFetcher instanceof IncrementalSourceStreamFetcher) {
            return;
        }

        if (canAssignNextSplit()) {
            final SourceSplitBase nextSplit = splits.poll();
            if (nextSplit == null) {
                throw new IOException("Cannot fetch from another split - no split remaining.");
            }
            currentSplitId = nextSplit.splitId();

            if (nextSplit.isSnapshotSplit()) {
                if (currentFetcher == null) {
                    final FetchTask.Context taskContext =
                            dataSourceDialect.createFetchTaskContext(nextSplit, sourceConfig);
                    currentFetcher = new IncrementalSourceScanFetcher(taskContext, subtaskId);
                }
            } else {
                // point from snapshot split to incremental split
                if (currentFetcher != null) {
                    log.info("It's turn to read incremental split, close current snapshot fetcher.");
                    currentFetcher.close();
                }
                final FetchTask.Context taskContext =
                        dataSourceDialect.createFetchTaskContext(nextSplit, sourceConfig);
                currentFetcher = new IncrementalSourceStreamFetcher(taskContext, subtaskId);
                log.info("Stream fetcher is created.");
            }
            currentFetcher.submitTask(dataSourceDialect.createFetchTask(nextSplit));
        }
    }

    public boolean canAssignNextSplit() {
        return currentFetcher == null || currentFetcher.isFinished();
    }

    private ChangeEventRecords finishedSnapshotSplit() {
        final ChangeEventRecords finishedRecords =
                ChangeEventRecords.forFinishedSplit(currentSplitId);
        currentSplitId = null;
        return finishedRecords;
    }
}
