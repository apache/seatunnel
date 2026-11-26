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

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MultiTableSinkAggregatedCommitterTest {

    @Test
    void testInitBeInvoked() throws IOException {
        Map<String, SinkAggregatedCommitter<?, ?>> aggCommitters = new HashMap<>();
        List<String> methodInvoked = new ArrayList<>();
        aggCommitters.put(
                "table1",
                new SinkAggregatedCommitter<Object, Object>() {

                    @Override
                    public void init() {
                        methodInvoked.add("init");
                    }

                    @Override
                    public List<Object> commit(List<Object> aggregatedCommitInfo)
                            throws IOException {
                        return Collections.emptyList();
                    }

                    @Override
                    public Object combine(List<Object> commitInfos) {
                        return null;
                    }

                    @Override
                    public void abort(List<Object> aggregatedCommitInfo) throws Exception {}

                    @Override
                    public void close() throws IOException {
                        methodInvoked.add("close");
                    }
                });
        MultiTableSinkAggregatedCommitter committer =
                new MultiTableSinkAggregatedCommitter(aggCommitters);
        committer.init();
        committer.close();
        Assertions.assertIterableEquals(Arrays.asList("init", "close"), methodInvoked);
    }

    @Test
    void testRestoreCommitRoutesToTableRestoreCommit() throws IOException {
        List<String> calls = new ArrayList<>();
        Map<String, SinkAggregatedCommitter<?, ?>> aggCommitters = new HashMap<>();
        aggCommitters.put(
                "table1",
                new SinkAggregatedCommitter<Object, Object>() {
                    @Override
                    public List<Object> commit(List<Object> aggregatedCommitInfo) {
                        calls.add("commit" + aggregatedCommitInfo);
                        return Collections.emptyList();
                    }

                    @Override
                    public List<Object> restoreCommit(List<Object> aggregatedCommitInfo) {
                        calls.add("restoreCommit" + aggregatedCommitInfo);
                        return Collections.emptyList();
                    }

                    @Override
                    public Object combine(List<Object> commitInfos) {
                        return null;
                    }

                    @Override
                    public void abort(List<Object> aggregatedCommitInfo) {}

                    @Override
                    public void close() {}
                });
        MultiTableSinkAggregatedCommitter committer =
                new MultiTableSinkAggregatedCommitter(aggCommitters);
        Map<String, Object> info = new HashMap<>();
        info.put("table1", "xid-1");
        List<MultiTableAggregatedCommitInfo> infos =
                Collections.singletonList(new MultiTableAggregatedCommitInfo(info));

        committer.restoreCommit(infos);
        committer.commit(infos);

        Assertions.assertIterableEquals(
                Arrays.asList("restoreCommit[xid-1]", "commit[xid-1]"), calls);
    }
}
