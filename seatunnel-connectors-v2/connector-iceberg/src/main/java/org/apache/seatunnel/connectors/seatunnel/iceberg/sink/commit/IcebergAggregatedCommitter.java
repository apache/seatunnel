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

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

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
            commitFiles(commitInfo.getCommitInfos(), commitInfo.getCheckpointId());
        }
        return Collections.emptyList();
    }

    @Override
    public List<IcebergAggregatedCommitInfo> restoreCommit(
            List<IcebergAggregatedCommitInfo> aggregatedCommitInfo) throws IOException {
        for (IcebergAggregatedCommitInfo commitInfo : aggregatedCommitInfo) {
            long checkpointId = commitInfo.getCheckpointId();
            List<WriteResult> allResults = flattenResults(commitInfo.getCommitInfos());
            if (filesCommitter.isAlreadyCommitted(checkpointId, allResults)) {
                log.info(
                        "Checkpoint {} already committed to Iceberg table, skipping restore commit",
                        checkpointId);
                continue;
            }
            commitFiles(commitInfo.getCommitInfos(), checkpointId);
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

    private void commitFiles(List<IcebergCommitInfo> commitInfos, long checkpointId) {
        List<WriteResult> allResults = flattenResults(commitInfos);
        if (!allResults.isEmpty()) {
            filesCommitter.doCommit(allResults, checkpointId);
        }
    }

    private List<WriteResult> flattenResults(List<IcebergCommitInfo> commitInfos) {
        return commitInfos.stream()
                .filter(info -> info.getResults() != null && !info.getResults().isEmpty())
                .flatMap(info -> info.getResults().stream())
                .collect(Collectors.toList());
    }
}
