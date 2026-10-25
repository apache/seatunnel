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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class MultiTableSinkCommitterTest {

    @Test
    void testRouteByTableIdentifierForCommitAndAbort() throws IOException {
        String table1 = "catalog.db.table1";
        String table2 = "catalog.db.table2";

        RecordingSinkCommitter table1Committer = new RecordingSinkCommitter();
        RecordingSinkCommitter table2Committer = new RecordingSinkCommitter();

        Map<String, SinkCommitter<?>> sinkCommitters = new HashMap<>();
        sinkCommitters.put(table1, table1Committer);
        sinkCommitters.put(table2, table2Committer);

        MultiTableSinkCommitter multiTableSinkCommitter =
                new MultiTableSinkCommitter(sinkCommitters);

        MultiTableCommitInfo commitInfo1 = new MultiTableCommitInfo(new ConcurrentHashMap<>());
        commitInfo1.getCommitInfo().put(SinkIdentifier.of(table1, 0), "t1-c0");
        commitInfo1.getCommitInfo().put(SinkIdentifier.of(table2, 0), "t2-c0");

        MultiTableCommitInfo commitInfo2 = new MultiTableCommitInfo(new ConcurrentHashMap<>());
        commitInfo2.getCommitInfo().put(SinkIdentifier.of(table1, 1), "t1-c1");
        commitInfo2.getCommitInfo().put(SinkIdentifier.of(table2, 1), "t2-c1");

        List<MultiTableCommitInfo> allCommitInfos = Arrays.asList(commitInfo1, commitInfo2);

        multiTableSinkCommitter.commit(allCommitInfos);
        Assertions.assertIterableEquals(Arrays.asList("t1-c0", "t1-c1"), table1Committer.committed);
        Assertions.assertIterableEquals(Arrays.asList("t2-c0", "t2-c1"), table2Committer.committed);

        multiTableSinkCommitter.abort(allCommitInfos);
        Assertions.assertIterableEquals(Arrays.asList("t1-c0", "t1-c1"), table1Committer.aborted);
        Assertions.assertIterableEquals(Arrays.asList("t2-c0", "t2-c1"), table2Committer.aborted);
    }

    private static class RecordingSinkCommitter implements SinkCommitter<Object> {

        private List<Object> committed = Collections.emptyList();
        private List<Object> aborted = Collections.emptyList();

        @Override
        public List<Object> commit(List<Object> commitInfos) {
            this.committed = commitInfos;
            return Collections.emptyList();
        }

        @Override
        public void abort(List<Object> commitInfos) {
            this.aborted = commitInfos;
        }
    }
}
