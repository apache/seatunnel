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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Iceberg aggregated committer */
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
            Map<Long, List<IcebergCommitInfo>> checkpoints = new LinkedHashMap<>();
            for (IcebergCommitInfo writer : commitInfo.getCommitInfos()) {
                checkpoints
                        .computeIfAbsent(writer.getCheckpointId(), ignored -> new ArrayList<>())
                        .add(writer);
            }
            for (Map.Entry<Long, List<IcebergCommitInfo>> checkpoint : checkpoints.entrySet()) {
                commitFiles(checkpoint.getValue(), checkpoint.getKey());
            }
        }
        return Collections.emptyList();
    }

    @Override
    public List<IcebergAggregatedCommitInfo> restoreCommit(
            List<IcebergAggregatedCommitInfo> aggregatedCommitInfo) throws IOException {
        // Flink also replays recovered committables through ordinary commit(). Both paths must
        // use the same publication check, rather than relying on engine-specific restore routing.
        return commit(aggregatedCommitInfo);
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
        List<List<WriteResult>> groups = orderedGroups(commitInfos);
        for (int group = 0; group < groups.size(); group++) {
            if (groups.size() == 1) {
                filesCommitter.doCommit(groups.get(group), checkpointId);
            } else {
                filesCommitter.doCommit(groups.get(group), checkpointId, group, groups.size());
            }
        }
    }

    /**
     * Combines writers by result-list index, which is each writer's chronological generation,
     * rather than by writer arrival order. Writers must retain key affinity within a checkpoint:
     * generations from independent writers do not establish an order for the same key. An equality
     * delete after accumulated data starts a new publication group so it can see that older data.
     */
    private List<List<WriteResult>> orderedGroups(List<IcebergCommitInfo> commitInfos) {
        int rounds = 0;
        for (IcebergCommitInfo writer : commitInfos) {
            if (writer.getResults() != null) {
                rounds = Math.max(rounds, writer.getResults().size());
            }
        }
        List<List<WriteResult>> groups = new ArrayList<>();
        List<WriteResult> pending = new ArrayList<>();
        for (int round = 0; round < rounds; round++) {
            List<WriteResult> results = new ArrayList<>();
            for (IcebergCommitInfo writer : commitInfos) {
                if (writer.getResults() != null && round < writer.getResults().size()) {
                    results.add(writer.getResults().get(round));
                }
            }
            // A reset writer cannot position-delete an earlier generation's rows. Its equality
            // deletes require an older data sequence number, hence a separate snapshot.
            boolean equalityDeletes =
                    results.stream()
                            .filter(result -> result.getDeleteFiles() != null)
                            .flatMap(result -> result.getDeleteFiles().stream())
                            .anyMatch(
                                    file ->
                                            file.recordCount() > 0
                                                    && file.content()
                                                            == FileContent.EQUALITY_DELETES);
            if (equalityDeletes && !pending.isEmpty()) {
                groups.add(pending);
                pending = new ArrayList<>();
            }
            pending.addAll(results);
        }
        if (!pending.isEmpty()) {
            groups.add(pending);
        }
        return groups;
    }
}
