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

package org.apache.seatunnel.connectors.seatunnel.hudi.sink.commiter;

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableConfig;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.client.HudiWriteClientProvider;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.state.HudiAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.state.HudiCommitInfo;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;

@Slf4j
public class HudiSinkAggregatedCommitter
        implements SinkAggregatedCommitter<HudiCommitInfo, HudiAggregatedCommitInfo> {

    private final HudiTableConfig tableConfig;

    private final HudiWriteClientProvider writeClientProvider;

    public HudiSinkAggregatedCommitter(
            HudiTableConfig tableConfig,
            HudiSinkConfig sinkConfig,
            SeaTunnelRowType seaTunnelRowType) {
        this.tableConfig = tableConfig;
        this.writeClientProvider =
                new HudiWriteClientProvider(
                        sinkConfig, tableConfig.getTableName(), seaTunnelRowType);
    }

    @Override
    public List<HudiAggregatedCommitInfo> commit(
            List<HudiAggregatedCommitInfo> aggregatedCommitInfo) throws IOException {
        aggregatedCommitInfo =
                aggregatedCommitInfo.stream()
                        .filter(
                                commit ->
                                        commit.getHudiCommitInfoList().stream()
                                                .anyMatch(
                                                        aggregateCommit ->
                                                                !aggregateCommit
                                                                                .getWriteStatusList()
                                                                                .isEmpty()
                                                                        && !writeClientProvider
                                                                                .getOrCreateClient()
                                                                                .commit(
                                                                                        aggregateCommit
                                                                                                .getWriteInstantTime(),
                                                                                        aggregateCommit
                                                                                                .getWriteStatusList())))
                        .collect(Collectors.toList());
        log.debug(
                "hudi records have been committed, error commit info are {}", aggregatedCommitInfo);
        return aggregatedCommitInfo;
    }

    @Override
    public HudiAggregatedCommitInfo combine(List<HudiCommitInfo> commitInfos) {
        return new HudiAggregatedCommitInfo(commitInfos);
    }

    @Override
    public void abort(List<HudiAggregatedCommitInfo> aggregatedCommitInfo) throws Exception {
        writeClientProvider.getOrCreateClient().rollbackFailedWrites();
        // rollback force commit
        for (HudiAggregatedCommitInfo hudiAggregatedCommitInfo : aggregatedCommitInfo) {
            for (HudiCommitInfo commitInfo : hudiAggregatedCommitInfo.getHudiCommitInfoList()) {
                Stack<String> forceCommitTime = commitInfo.getForceCommitTime();
                while (!forceCommitTime.isEmpty()) {
                    writeClientProvider.getOrCreateClient().rollback(forceCommitTime.pop());
                }
            }
        }
    }

    @Override
    public void close() {
        writeClientProvider.close();
    }
}
