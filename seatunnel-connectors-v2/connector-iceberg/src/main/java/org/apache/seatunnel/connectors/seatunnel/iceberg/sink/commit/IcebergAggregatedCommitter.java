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

package org.apache.seatunnel.connectors.seatunnel.iceberg.sink.commit;

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergTableLoader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer.WriteResult;

import org.apache.iceberg.FileContent;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Iceberg aggregated committer */
@Slf4j
public class IcebergAggregatedCommitter
        implements SinkAggregatedCommitter<IcebergCommitInfo, IcebergAggregatedCommitInfo> {

    private IcebergTableLoader tableLoader;
    private IcebergFilesCommitter filesCommitter;
    private final IcebergSinkConfig config;
    private final CatalogTable catalogTable;

    public IcebergAggregatedCommitter(IcebergSinkConfig config, CatalogTable catalogTable) {
        this.config = config;
        this.catalogTable = catalogTable;
    }

    @Override
    public void init() {
        this.tableLoader = IcebergTableLoader.create(config, catalogTable);
        this.filesCommitter = IcebergFilesCommitter.of(config, tableLoader);
    }

    @Override
    public List<IcebergAggregatedCommitInfo> commit(
            List<IcebergAggregatedCommitInfo> aggregatedCommitInfo) throws IOException {
        for (IcebergAggregatedCommitInfo commitInfo : aggregatedCommitInfo) {
            commitFiles(commitInfo.getCommitInfos(), commitInfo.getCheckpointId(), false);
        }
        return Collections.emptyList();
    }

    @Override
    public List<IcebergAggregatedCommitInfo> restoreCommit(
            List<IcebergAggregatedCommitInfo> aggregatedCommitInfo) throws IOException {
        for (IcebergAggregatedCommitInfo commitInfo : aggregatedCommitInfo) {
            commitFiles(commitInfo.getCommitInfos(), commitInfo.getCheckpointId(), true);
        }
        return Collections.emptyList();
    }

    @Override
    public IcebergAggregatedCommitInfo combine(List<IcebergCommitInfo> commitInfos) {
        return new IcebergAggregatedCommitInfo(commitInfos);
    }

    @Override
    public void abort(List<IcebergAggregatedCommitInfo> aggregatedCommitInfo) throws Exception {}

    @Override
    public void close() throws IOException {
        this.tableLoader.close();
    }

    private void commitFiles(
            List<IcebergCommitInfo> commitInfos, long checkpointId, boolean restoring) {
        List<List<WriteResult>> groups = planCommits(commitInfos);
        if (groups.isEmpty()) {
            return;
        }
        if (groups.size() > 1) {
            log.info(
                    "Checkpoint {} has equality deletes after a writer rotation, committing it as"
                            + " {} ordered snapshots",
                    checkpointId,
                    groups.size());
            filesCommitter.doCommitInOrder(groups, checkpointId, restoring);
            return;
        }
        List<WriteResult> results = groups.get(0);
        if (restoring && filesCommitter.isAlreadyCommitted(checkpointId, results)) {
            log.info(
                    "Checkpoint {} already committed to Iceberg table, skipping restore commit",
                    checkpointId);
            return;
        }
        filesCommitter.doCommit(results, checkpointId);
    }

    /**
     * Splits the results of one checkpoint into groups that must be committed one after another.
     *
     * <p>Each writer reports one result per task writer it completed, in order. Iceberg applies an
     * equality delete only to data files with a smaller sequence number, so an equality delete
     * written after a rotation does not remove a row written before it if both land in the same
     * snapshot. The i-th results of all writers form round i, and a round that carries equality
     * deletes starts a new group. Without rotation, or without equality deletes, there is a single
     * group, which is the same single snapshot as before.
     */
    static List<List<WriteResult>> planCommits(List<IcebergCommitInfo> commitInfos) {
        List<List<WriteResult>> perWriter =
                commitInfos.stream()
                        .map(IcebergCommitInfo::getResults)
                        .filter(results -> results != null && !results.isEmpty())
                        .collect(Collectors.toList());
        int rounds = perWriter.stream().mapToInt(List::size).max().orElse(0);
        List<List<WriteResult>> groups = new ArrayList<>();
        for (int round = 0; round < rounds; round++) {
            List<WriteResult> roundResults = new ArrayList<>();
            for (List<WriteResult> results : perWriter) {
                if (round < results.size()) {
                    roundResults.add(results.get(round));
                }
            }
            // A group without records (for example from a flush right after a schema change)
            // commits no snapshot, so later rounds join it instead of opening another group.
            if (groups.isEmpty()
                    || (hasEqualityDeletes(roundResults)
                            && hasRecords(groups.get(groups.size() - 1)))) {
                groups.add(roundResults);
            } else {
                groups.get(groups.size() - 1).addAll(roundResults);
            }
        }
        return groups;
    }

    private static boolean hasRecords(List<WriteResult> results) {
        return results.stream()
                .flatMap(
                        result ->
                                Stream.concat(
                                        result.getDataFiles() == null
                                                ? Stream.empty()
                                                : result.getDataFiles().stream(),
                                        result.getDeleteFiles() == null
                                                ? Stream.empty()
                                                : result.getDeleteFiles().stream()))
                .anyMatch(file -> file.recordCount() > 0);
    }

    private static boolean hasEqualityDeletes(List<WriteResult> results) {
        return results.stream()
                .filter(result -> result.getDeleteFiles() != null)
                .flatMap(result -> result.getDeleteFiles().stream())
                .anyMatch(
                        file ->
                                file.content() == FileContent.EQUALITY_DELETES
                                        && file.recordCount() > 0);
    }
}
