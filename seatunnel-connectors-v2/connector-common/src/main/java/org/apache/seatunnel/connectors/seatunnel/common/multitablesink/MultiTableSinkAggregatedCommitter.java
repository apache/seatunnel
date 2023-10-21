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

package org.apache.seatunnel.connectors.seatunnel.common.multitablesink;

import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkAggregatedCommitter;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class MultiTableSinkAggregatedCommitter
        implements SinkAggregatedCommitter<MultiTableCommitInfo, MultiTableAggregatedCommitInfo> {

    private final Map<String, SinkAggregatedCommitter<?, ?>> aggCommitters;

    private transient MultiTableResourceManager resourceManager = null;

    public MultiTableSinkAggregatedCommitter(
            Map<String, SinkAggregatedCommitter<?, ?>> aggCommitters) {
        this.aggCommitters = aggCommitters;
    }

    @Override
    public void init() {
        initResourceManager();
    }

    private void initResourceManager() {
        for (String tableIdentifier : aggCommitters.keySet()) {
            SinkAggregatedCommitter<?, ?> aggCommitter = aggCommitters.get(tableIdentifier);
            if (!(aggCommitter instanceof SupportMultiTableSinkAggregatedCommitter)) {
                return;
            }
            resourceManager =
                    ((SupportMultiTableSinkAggregatedCommitter<?>) aggCommitter)
                            .initMultiTableResourceManager(aggCommitters.size(), 1)
                            .orElse(null);
            break;
        }
        if (resourceManager != null) {
            for (String tableIdentifier : aggCommitters.keySet()) {
                SinkAggregatedCommitter<?, ?> aggCommitter = aggCommitters.get(tableIdentifier);
                aggCommitter.init();
                ((SupportMultiTableSinkAggregatedCommitter<?>) aggCommitter)
                        .setMultiTableResourceManager(Optional.of(resourceManager), 1);
            }
        }
    }

    @Override
    public List<MultiTableAggregatedCommitInfo> commit(
            List<MultiTableAggregatedCommitInfo> aggregatedCommitInfo) throws IOException {
        for (String sinkIdentifier : aggCommitters.keySet()) {
            SinkAggregatedCommitter<?, ?> sinkCommitter = aggCommitters.get(sinkIdentifier);
            if (sinkCommitter != null) {
                List commitInfo =
                        aggregatedCommitInfo.stream()
                                .map(
                                        multiTableCommitInfo ->
                                                multiTableCommitInfo
                                                        .getCommitInfo()
                                                        .get(sinkIdentifier))
                                .filter(Objects::nonNull)
                                .collect(Collectors.toList());
                sinkCommitter.commit(commitInfo);
            }
        }
        return new ArrayList<>();
    }

    @Override
    public MultiTableAggregatedCommitInfo combine(List<MultiTableCommitInfo> commitInfos) {
        Map<String, Object> commitInfo = new HashMap<>();
        for (String sinkIdentifier : aggCommitters.keySet()) {
            SinkAggregatedCommitter<?, ?> sinkCommitter = aggCommitters.get(sinkIdentifier);
            if (sinkCommitter != null) {
                List commits =
                        commitInfos.stream()
                                .flatMap(
                                        multiTableCommitInfo ->
                                                multiTableCommitInfo.getCommitInfo().entrySet()
                                                        .stream()
                                                        .filter(
                                                                m ->
                                                                        m.getKey()
                                                                                .getTableIdentifier()
                                                                                .equals(
                                                                                        sinkIdentifier))
                                                        .map(Map.Entry::getValue))
                                .collect(Collectors.toList());
                commitInfo.put(sinkIdentifier, sinkCommitter.combine(commits));
            }
        }
        return new MultiTableAggregatedCommitInfo(commitInfo);
    }

    @Override
    public void abort(List<MultiTableAggregatedCommitInfo> aggregatedCommitInfo) throws Exception {
        Throwable firstE = null;
        for (String sinkIdentifier : aggCommitters.keySet()) {
            SinkAggregatedCommitter<?, ?> sinkCommitter = aggCommitters.get(sinkIdentifier);
            if (sinkCommitter != null) {
                List commitInfo =
                        aggregatedCommitInfo.stream()
                                .map(
                                        multiTableCommitInfo ->
                                                multiTableCommitInfo
                                                        .getCommitInfo()
                                                        .get(sinkIdentifier))
                                .filter(Objects::nonNull)
                                .collect(Collectors.toList());
                try {
                    sinkCommitter.abort(commitInfo);
                } catch (Throwable e) {
                    log.error("abort sink committer error", e);
                    if (firstE == null) {
                        firstE = e;
                    }
                }
            }
        }
        if (firstE != null) {
            throw new RuntimeException(firstE);
        }
    }

    @Override
    public void close() throws IOException {
        Throwable firstE = null;
        for (String sinkIdentifier : aggCommitters.keySet()) {
            SinkAggregatedCommitter<?, ?> sinkCommitter = aggCommitters.get(sinkIdentifier);
            if (sinkCommitter != null) {
                try {
                    sinkCommitter.close();
                } catch (Throwable e) {
                    log.error("close sink committer error", e);
                    if (firstE == null) {
                        firstE = e;
                    }
                }
            }
        }
        if (firstE != null) {
            throw new RuntimeException(firstE);
        }
        try {
            if (resourceManager != null) {
                resourceManager.close();
            }
        } catch (Throwable e) {
            log.error("close resourceManager error", e);
        }
    }
}
