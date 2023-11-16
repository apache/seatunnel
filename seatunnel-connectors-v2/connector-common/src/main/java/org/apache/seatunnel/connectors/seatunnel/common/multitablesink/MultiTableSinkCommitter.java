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

import org.apache.seatunnel.api.sink.SinkCommitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class MultiTableSinkCommitter implements SinkCommitter<MultiTableCommitInfo> {

    private final Map<String, SinkCommitter<?>> sinkCommitters;

    public MultiTableSinkCommitter(Map<String, SinkCommitter<?>> sinkCommitters) {
        this.sinkCommitters = sinkCommitters;
    }

    @Override
    public List<MultiTableCommitInfo> commit(List<MultiTableCommitInfo> commitInfos)
            throws IOException {
        for (String sinkIdentifier : sinkCommitters.keySet()) {
            SinkCommitter<?> sinkCommitter = sinkCommitters.get(sinkIdentifier);
            if (sinkCommitter != null) {
                List commitInfo =
                        commitInfos.stream()
                                .flatMap(
                                        multiTableCommitInfo ->
                                                multiTableCommitInfo.getCommitInfo().entrySet()
                                                        .stream()
                                                        .filter(
                                                                entry ->
                                                                        entry.getKey()
                                                                                .getTableIdentifier()
                                                                                .equals(
                                                                                        sinkIdentifier)))
                                .map(Map.Entry::getValue)
                                .collect(Collectors.toList());
                sinkCommitter.commit(commitInfo);
            }
        }
        return new ArrayList<>();
    }

    @Override
    public void abort(List<MultiTableCommitInfo> commitInfos) throws IOException {
        for (String sinkIdentifier : sinkCommitters.keySet()) {
            SinkCommitter<?> sinkCommitter = sinkCommitters.get(sinkIdentifier);
            if (sinkCommitter != null) {
                List commitInfo =
                        commitInfos.stream()
                                .map(
                                        multiTableCommitInfo ->
                                                multiTableCommitInfo
                                                        .getCommitInfo()
                                                        .get(sinkIdentifier))
                                .filter(Objects::nonNull)
                                .collect(Collectors.toList());
                sinkCommitter.abort(commitInfo);
            }
        }
    }
}
