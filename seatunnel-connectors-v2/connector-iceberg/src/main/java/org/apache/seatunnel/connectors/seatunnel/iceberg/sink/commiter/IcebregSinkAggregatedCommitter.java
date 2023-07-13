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

package org.apache.seatunnel.connectors.seatunnel.iceberg.sink.commiter;

import org.apache.seatunnel.shade.com.google.common.base.Preconditions;

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergTableLoader;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.util.ThreadPools;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** @Author: Liuli @Date: 2023/7/12 18:33 */
@Slf4j
public class IcebregSinkAggregatedCommitter
        implements SinkAggregatedCommitter<IcebergCommitInfo, IcebergAggregatedCommitInfo> {

    private final Map<String, String> snapshotProperties;

    private final IcebergTableLoader tableLoader;
    private transient Table table;
    private transient ExecutorService workerPool;

    private transient boolean isOpen = false;

    public IcebregSinkAggregatedCommitter(
            Map<String, String> snapshotProperties, IcebergTableLoader tableLoader) {
        this.snapshotProperties = snapshotProperties;
        this.tableLoader = tableLoader;
    }

    @Override
    public List<IcebergAggregatedCommitInfo> commit(
            List<IcebergAggregatedCommitInfo> aggregatedCommitInfo) throws IOException {
        tryOpen();
        List<WriteResult> results =
                aggregatedCommitInfo.stream()
                        .flatMap(icebergCommitInfo -> icebergCommitInfo.getWriteResults().stream())
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
        commitPendingResult(results);
        return Collections.emptyList();
    }

    @Override
    public IcebergAggregatedCommitInfo combine(List<IcebergCommitInfo> commitInfos) {
        List<WriteResult> writeResults =
                commitInfos.stream()
                        .map(IcebergCommitInfo::getWriteResult)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
        return new IcebergAggregatedCommitInfo(writeResults);
    }

    @Override
    public void abort(List<IcebergAggregatedCommitInfo> aggregatedCommitInfo) throws Exception {}

    @Override
    public void close() throws IOException {
        if (tableLoader != null) {
            tableLoader.close();
        }

        if (workerPool != null) {
            workerPool.shutdown();
        }
    }

    private void tryOpen() {
        if (!isOpen) {
            tableLoader.open();
            table = tableLoader.loadTable();
            this.workerPool = ThreadPools.newWorkerPool("iceberg-worker-pool", 1);
            isOpen = true;
        }
    }

    private void commitPendingResult(List<WriteResult> pendingResults) {
        CommitSummary summary = new CommitSummary(pendingResults);
        long totalFiles = summary.dataFilesCount() + summary.deleteFilesCount();
        if (totalFiles != 0) {
            commitDeltaTxn(pendingResults, summary);
        } else {
            log.info("Skip commit due to no data files or delete files.");
        }
    }

    private void commitDeltaTxn(List<WriteResult> pendingResults, CommitSummary summary) {
        if (summary.deleteFilesCount() == 0) {
            // To be compatible with iceberg format V1.
            AppendFiles appendFiles = table.newAppend().scanManifestsWith(workerPool);
            for (WriteResult result : pendingResults) {
                Preconditions.checkState(
                        result.referencedDataFiles().length == 0,
                        "Should have no referenced data files for append.");
                Arrays.stream(result.dataFiles()).forEach(appendFiles::appendFile);
            }
            commitOperation(appendFiles, summary, "append");
        } else {
            // To be compatible with iceberg format V2.
            for (WriteResult result : pendingResults) {
                // We don't commit the merged result into a single transaction because for the
                // sequential
                // transaction txn1 and txn2, the equality-delete files of txn2 are required to be
                // applied
                // to data files from txn1. Committing the merged one will lead to the incorrect
                // delete
                // semantic.

                // Row delta validations are not needed for streaming changes that write equality
                // deletes.
                // Equality deletes are applied to data in all previous sequence numbers, so retries
                // may
                // push deletes further in the future, but do not affect correctness. Position
                // deletes
                // committed to the table in this path are used only to delete rows from data files
                // that are
                // being added in this commit. There is no way for data files added along with the
                // delete
                // files to be concurrently removed, so there is no need to validate the files
                // referenced by
                // the position delete files that are being committed.
                RowDelta rowDelta = table.newRowDelta().scanManifestsWith(workerPool);

                Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);
                Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);
                commitOperation(rowDelta, summary, "rowDelta");
            }
        }
    }

    private void commitOperation(
            SnapshotUpdate<?> operation, CommitSummary summary, String description) {
        log.info("Committing {} to table {} with summary: {}", description, table.name(), summary);
        snapshotProperties.forEach(operation::set);

        long startNano = System.nanoTime();
        operation.commit(); // abort is automatically called if this fails.
        long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano);
        log.info("Committed {} to table: {} in {} ms", description, table.name(), durationMs);
    }
}
