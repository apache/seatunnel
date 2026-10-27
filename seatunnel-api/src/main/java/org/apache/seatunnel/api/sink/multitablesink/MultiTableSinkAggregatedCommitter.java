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

package org.apache.seatunnel.api.sink.multitablesink;

import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkAggregatedCommitter;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
        Map<SinkAggregatedCommitter<?, ?>, List<String>> groupedCommitters = groupByIdentity();
        for (SinkAggregatedCommitter<?, ?> aggCommitter : groupedCommitters.keySet()) {
            if (!(aggCommitter instanceof SupportMultiTableSinkAggregatedCommitter)) {
                break;
            }
            resourceManager =
                    ((SupportMultiTableSinkAggregatedCommitter<?>) aggCommitter)
                            .initMultiTableResourceManager(groupedCommitters.size(), 1);
            break;
        }
        for (SinkAggregatedCommitter<?, ?> aggCommitter : groupedCommitters.keySet()) {
            aggCommitter.init();
            if (resourceManager != null) {
                ((SupportMultiTableSinkAggregatedCommitter<?>) aggCommitter)
                        .setMultiTableResourceManager(resourceManager, 0);
            }
        }
    }

    @Override
    public List<MultiTableAggregatedCommitInfo> commit(
            List<MultiTableAggregatedCommitInfo> aggregatedCommitInfo) throws IOException {
        List<MultiTableAggregatedCommitInfo> errorList = new ArrayList<>();
        for (Map.Entry<SinkAggregatedCommitter<?, ?>, List<String>> entry :
                groupByIdentity().entrySet()) {
            List errCommitList =
                    entry.getKey()
                            .commit(
                                    getAggregatedCommitInfos(
                                            aggregatedCommitInfo, entry.getValue()));
            if (errCommitList.size() == 0) {
                continue;
            }

            String canonicalIdentifier = entry.getValue().get(0);
            for (int i = 0; i < errCommitList.size(); i++) {
                if (errorList.size() < i + 1) {
                    errorList.add(i, new MultiTableAggregatedCommitInfo(new HashMap<>()));
                }
                errorList.get(i).getCommitInfo().put(canonicalIdentifier, errCommitList.get(i));
            }
        }
        return errorList;
    }

    @Override
    public MultiTableAggregatedCommitInfo combine(List<MultiTableCommitInfo> commitInfos) {
        Map<String, Object> commitInfo = new HashMap<>();
        for (Map.Entry<SinkAggregatedCommitter<?, ?>, List<String>> entry :
                groupByIdentity().entrySet()) {
            commitInfo.put(
                    entry.getValue().get(0),
                    entry.getKey().combine(getCommitInfos(commitInfos, entry.getValue())));
        }
        return new MultiTableAggregatedCommitInfo(commitInfo);
    }

    @Override
    public void abort(List<MultiTableAggregatedCommitInfo> aggregatedCommitInfo) throws Exception {
        Throwable firstE = null;
        for (Map.Entry<SinkAggregatedCommitter<?, ?>, List<String>> entry :
                groupByIdentity().entrySet()) {
            try {
                entry.getKey()
                        .abort(getAggregatedCommitInfos(aggregatedCommitInfo, entry.getValue()));
            } catch (Throwable e) {
                log.error("abort sink committer error", e);
                if (firstE == null) {
                    firstE = e;
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
        for (SinkAggregatedCommitter<?, ?> sinkCommitter : groupByIdentity().keySet()) {
            try {
                sinkCommitter.close();
            } catch (Throwable e) {
                log.error("close sink committer error", e);
                if (firstE == null) {
                    firstE = e;
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

    /**
     * Groups aliases that intentionally reference the same physical destination committer.
     *
     * @return one aggregated committer with every source-table identifier that routes to it
     */
    private Map<SinkAggregatedCommitter<?, ?>, List<String>> groupByIdentity() {
        Map<SinkAggregatedCommitter<?, ?>, List<String>> groupedCommitters =
                new IdentityHashMap<>();
        for (Map.Entry<String, SinkAggregatedCommitter<?, ?>> entry : aggCommitters.entrySet()) {
            if (entry.getValue() != null) {
                groupedCommitters
                        .computeIfAbsent(entry.getValue(), ignored -> new ArrayList<>())
                        .add(entry.getKey());
            }
        }
        return groupedCommitters;
    }

    /**
     * Collects canonical and legacy per-alias commit payloads for one physical committer.
     *
     * @param commitInfos all multi-table commit records received by the engine
     * @param sinkIdentifiers source-table identifiers that share this committer
     * @return commit payloads to combine in one committer call
     */
    private List getCommitInfos(
            List<MultiTableCommitInfo> commitInfos, List<String> sinkIdentifiers) {
        return commitInfos.stream()
                .flatMap(
                        multiTableCommitInfo ->
                                multiTableCommitInfo.getCommitInfo().entrySet().stream()
                                        .filter(
                                                entry ->
                                                        sinkIdentifiers.contains(
                                                                entry.getKey()
                                                                        .getTableIdentifier()))
                                        .map(Map.Entry::getValue))
                .collect(Collectors.toList());
    }

    /**
     * Collects canonical and legacy per-alias aggregated commit payloads for one committer.
     *
     * @param commitInfos all aggregated commit records received by the engine
     * @param sinkIdentifiers source-table identifiers that share this committer
     * @return aggregated commit payloads to deliver in one committer call
     */
    private List getAggregatedCommitInfos(
            List<MultiTableAggregatedCommitInfo> commitInfos, List<String> sinkIdentifiers) {
        return commitInfos.stream()
                .flatMap(
                        multiTableCommitInfo ->
                                sinkIdentifiers.stream()
                                        .map(multiTableCommitInfo.getCommitInfo()::get)
                                        .filter(Objects::nonNull))
                .collect(Collectors.toList());
    }
}
