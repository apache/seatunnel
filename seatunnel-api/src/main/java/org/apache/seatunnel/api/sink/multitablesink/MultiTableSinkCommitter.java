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

import org.apache.seatunnel.api.sink.SinkCommitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MultiTableSinkCommitter implements SinkCommitter<MultiTableCommitInfo> {

    private final Map<String, SinkCommitter<?>> sinkCommitters;

    public MultiTableSinkCommitter(Map<String, SinkCommitter<?>> sinkCommitters) {
        this.sinkCommitters = sinkCommitters;
    }

    @Override
    public List<MultiTableCommitInfo> commit(List<MultiTableCommitInfo> commitInfos)
            throws IOException {
        for (Map.Entry<SinkCommitter<?>, List<String>> entry : groupByIdentity().entrySet()) {
            entry.getKey().commit(getCommitInfos(commitInfos, entry.getValue()));
        }
        return new ArrayList<>();
    }

    @Override
    public void abort(List<MultiTableCommitInfo> commitInfos) throws IOException {
        for (Map.Entry<SinkCommitter<?>, List<String>> entry : groupByIdentity().entrySet()) {
            entry.getKey().abort(getCommitInfos(commitInfos, entry.getValue()));
        }
    }

    /**
     * Groups aliases that intentionally reference the same physical destination committer.
     *
     * @return one committer with every source-table identifier that routes to it
     */
    private Map<SinkCommitter<?>, List<String>> groupByIdentity() {
        Map<SinkCommitter<?>, List<String>> groupedCommitters = new IdentityHashMap<>();
        for (Map.Entry<String, SinkCommitter<?>> entry : sinkCommitters.entrySet()) {
            if (entry.getValue() != null) {
                groupedCommitters
                        .computeIfAbsent(entry.getValue(), ignored -> new ArrayList<>())
                        .add(entry.getKey());
            }
        }
        return groupedCommitters;
    }

    /**
     * Collects the canonical and legacy per-alias commit records for one physical committer.
     *
     * @param commitInfos all multi-table commit records received by the engine
     * @param sinkIdentifiers source-table identifiers that share this committer
     * @return commit payloads to deliver in one committer call
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
}
